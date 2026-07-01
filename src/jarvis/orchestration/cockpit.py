from __future__ import annotations

import base64
import hashlib
import json
import pathlib
import re
from dataclasses import dataclass
from typing import Any

import httpx

from jarvis.capabilities import (
    FORGE_BRANCH_PUSH,
    FORGE_PR_COMMENT,
    FORGE_PR_CREATE,
    WORKER_JOB_START,
    WORKER_SESSION_CREATE,
    WORKER_SESSION_TURN,
)
from jarvis.config import WorkerConfig
from jarvis.ids import utc_now
from jarvis.orchestration.models import Artifact, OrchestrationRun, WorkerProfile, WorkerSessionLink
from jarvis.orchestration.reports import build_run_report
from jarvis.orchestration.store import OrchestrationStore
from jarvis.orchestration.supervisor import SyncSummary, sync_run_jobs, sync_run_sessions
from jarvis.orchestration.workers import WorkerRegistry
from jarvis.worker_session_contract import ACTIVE_SESSION_STATUSES

API_VERSION = "v1"
SCHEMA_VERSION = 1

SESSION_REF_PREFIX = "sessref_"
CURSOR_PREFIX = "evt_"
MAX_PAGE_LIMIT = 500
PRIVATE_PUBLIC_KEYS = {
    "api_key",
    "apikey",
    "authorization",
    "base_url",
    "codex_thread_id",
    "control_envelope",
    "cwd",
    "env",
    "environment",
    "execution_envelope",
    "headers",
    "metadata",
    "password",
    "provider_pid",
    "provider_session_id",
    "raw",
    "secret",
    "token",
    "token_env",
}
PUBLIC_EVENT_DATA_KEYS = {
    "branch",
    "checkpoint_id",
    "command",
    "commit_sha",
    "content",
    "decision",
    "delta",
    "detail",
    "exit_code",
    "id",
    "label",
    "message_id",
    "options",
    "payload",
    "provider",
    "question",
    "questions",
    "request_id",
    "request_kind",
    "run_id",
    "status",
    "summary",
    "text",
    "title",
    "turn_id",
    "url",
}


class CockpitError(RuntimeError):
    def __init__(self, code: str, message: str, *, recoverable: bool = False, status: int = 400) -> None:
        self.code = code
        self.message = message
        self.recoverable = recoverable
        self.status = status
        super().__init__(message)

    def body(self) -> dict[str, Any]:
        return {"ok": False, "error": {"code": self.code, "message": self.message, "recoverable": self.recoverable}}


@dataclass(frozen=True)
class SessionRef:
    worker_id: str
    session_id: str


class IdempotencyStore:
    def __init__(self, root: str) -> None:
        self.root = pathlib.Path(root).expanduser() / "idempotency"
        self.root.mkdir(parents=True, exist_ok=True)

    def get(self, scope: str, key: str, body: dict[str, Any]) -> dict[str, Any] | None:
        if not key:
            return None
        path = self._path(scope, key)
        if not path.exists():
            return None
        try:
            record = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise CockpitError("internal_error", f"could not read idempotency record: {exc}", status=500) from exc
        if record.get("fingerprint") != _body_fingerprint(body):
            raise CockpitError("idempotency_conflict", "idempotency key was reused with a different request body", status=409)
        response = dict(record.get("response") or {})
        response["idempotent"] = True
        return response

    def save(self, scope: str, key: str, body: dict[str, Any], response: dict[str, Any]) -> None:
        if not key:
            return
        path = self._path(scope, key)
        record = {"fingerprint": _body_fingerprint(body), "response": response}
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(record, indent=2, sort_keys=True))
        tmp.replace(path)

    def _path(self, scope: str, key: str) -> pathlib.Path:
        digest = hashlib.sha256(f"{scope}\0{key}".encode("utf-8")).hexdigest()
        return self.root / f"{digest}.json"


def make_session_ref(worker_id: str, session_id: str) -> str:
    raw = f"{worker_id}\0{session_id}".encode("utf-8")
    encoded = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return f"{SESSION_REF_PREFIX}{encoded}"


def parse_session_ref(value: str) -> SessionRef:
    text = str(value or "")
    if not text.startswith(SESSION_REF_PREFIX):
        raise CockpitError("not_found", "session not found", status=404)
    encoded = text[len(SESSION_REF_PREFIX):]
    padding = "=" * (-len(encoded) % 4)
    try:
        decoded = base64.urlsafe_b64decode(f"{encoded}{padding}".encode("ascii")).decode("utf-8")
    except Exception as exc:  # noqa: BLE001 - malformed public ids are just not found
        raise CockpitError("not_found", "session not found", status=404) from exc
    if "\0" not in decoded:
        raise CockpitError("not_found", "session not found", status=404)
    worker_id, session_id = decoded.split("\0", 1)
    if not worker_id or not session_id:
        raise CockpitError("not_found", "session not found", status=404)
    return SessionRef(worker_id=worker_id, session_id=session_id)


def cockpit_catalog() -> dict[str, Any]:
    return {
        "api_version": API_VERSION,
        "schema_version": SCHEMA_VERSION,
        "engines": [
            _engine_catalog("codex", "Codex", "OpenAI Codex provider session"),
            _engine_catalog("claude", "Claude", "Claude provider session"),
        ],
        "capabilities": [
            {"capability": "code.edit", "display_name": "Edit code", "maps_to": [WORKER_SESSION_CREATE, WORKER_SESSION_TURN]},
            {"capability": "shell.run", "display_name": "Run shell commands", "maps_to": [WORKER_JOB_START]},
            {"capability": "browser.use", "display_name": "Use browser", "maps_to": ["worker.browser"]},
            {"capability": "git.branch", "display_name": "Create branches", "maps_to": [FORGE_BRANCH_PUSH]},
            {"capability": "github.pr.create", "display_name": "Create pull requests", "maps_to": [FORGE_PR_CREATE]},
            {"capability": "github.pr.comment", "display_name": "Comment on pull requests", "maps_to": [FORGE_PR_COMMENT]},
        ],
        "work_sources": ["manual", "github", "linear", "voice", "whatsapp"],
        "engine_strategies": ["single", "parallel", "review_panel"],
        "branch_strategies": ["auto", "use_existing", "create", "none"],
        "landing_policies": ["branch_only", "draft_pr", "ready_pr", "confirm_before_pr"],
        "request_kinds": ["approval", "input"],
    }


def cockpit_snapshot(
    *,
    store: OrchestrationStore,
    worker_cfg: WorkerConfig,
    workers_path: str,
    sync_mode: str = "none",
    http_get: Any = httpx.get,
) -> dict[str, Any]:
    sync = sync_state(store=store, worker_cfg=worker_cfg, workers_path=workers_path, sync_mode=sync_mode, http_get=http_get)
    runs = store.list_runs()
    include_worker_state = sync["mode"] in {"fast", "probe"}
    workers = worker_profiles(worker_cfg=worker_cfg, workers_path=workers_path, probe=sync["mode"] == "probe", http_get=http_get)
    worker_by_id = {worker["worker_id"]: worker for worker in workers}
    sessions = aggregate_sessions(
        runs=runs,
        worker_cfg=worker_cfg,
        workers_path=workers_path,
        http_get=http_get,
        worker_by_id=worker_by_id,
        include_worker_state=include_worker_state,
    )
    requests = aggregate_requests(worker_cfg=worker_cfg, workers_path=workers_path, http_get=http_get) if include_worker_state else []
    checkpoints = aggregate_checkpoints(runs=runs, worker_cfg=worker_cfg, workers_path=workers_path, http_get=http_get) if include_worker_state else []
    artifacts = artifact_summaries(runs)
    return {
        "api_version": API_VERSION,
        "schema_version": SCHEMA_VERSION,
        "cursor": snapshot_cursor(runs, sessions, artifacts),
        "generated_at": utc_now(),
        "sync": sync,
        "runs": [run_summary(run, requests=requests, artifacts=artifacts) for run in runs],
        "sessions": [
            session_summary(session, requests=requests, checkpoints=checkpoints)
            for session in sorted(sessions.values(), key=lambda x: str(x.get("updated_at") or ""))
        ],
        "workers": workers,
        "artifacts": artifacts,
    }


def sync_state(
    *,
    store: OrchestrationStore,
    worker_cfg: WorkerConfig,
    workers_path: str,
    sync_mode: str,
    http_get: Any = httpx.get,
) -> dict[str, Any]:
    mode = sync_mode if sync_mode in {"none", "fast", "probe"} else "none"
    if mode == "none":
        return {"mode": mode, "status": "stale", "synced_at": "", "errors": []}
    job_summary = sync_run_jobs(store, worker_cfg=worker_cfg, workers_path=workers_path, get=http_get)
    session_summary_result = sync_run_sessions(store, worker_cfg=worker_cfg, workers_path=workers_path, get=http_get)
    summary = _merge_sync(job_summary, session_summary_result)
    return {
        "mode": mode,
        "status": "fresh" if not summary.errors else "partial",
        "synced_at": utc_now(),
        "errors": summary.errors or [],
    }


def worker_profiles(
    *,
    worker_cfg: WorkerConfig,
    workers_path: str,
    probe: bool = False,
    http_get: Any = httpx.get,
) -> list[dict[str, Any]]:
    registry = WorkerRegistry(worker_cfg, profiles_path=workers_path, http_get=http_get)
    return [project_worker_profile(profile) for profile in registry.profiles(probe=probe)]


def project_worker_profile(profile: WorkerProfile) -> dict[str, Any]:
    engines = [_engine_row(engine, default=(engine == profile.default_engine), worker_status=profile.status) for engine in profile.supported_engines]
    mapped_capabilities = _public_worker_capabilities(profile)
    return {
        "worker_id": profile.worker_id,
        "display_name": profile.display_name,
        "status": profile.status,
        "health": _worker_health(profile.status),
        "last_seen_at": utc_now() if profile.status == "online" else "",
        "capabilities": mapped_capabilities,
        "engines": engines,
        "capacity": {
            "max_sessions": profile.max_concurrent_jobs,
            "active_sessions": profile.current_jobs,
            "queued_sessions": 0,
        },
        "repositories": [],
        "public_metadata": {},
    }


def aggregate_sessions(
    *,
    runs: list[OrchestrationRun],
    worker_cfg: WorkerConfig,
    workers_path: str,
    http_get: Any = httpx.get,
    worker_by_id: dict[str, dict[str, Any]] | None = None,
    include_worker_state: bool = True,
) -> dict[str, dict[str, Any]]:
    sessions: dict[str, dict[str, Any]] = {}
    worker_by_id = worker_by_id or {}
    run_by_id = {run.run_id: run for run in runs}
    for run in runs:
        for link in run.sessions:
            ref = make_session_ref(link.worker_id, link.session_id)
            sessions[ref] = _session_from_link(link, run)
    if not include_worker_state:
        return sessions
    registry = WorkerRegistry(worker_cfg, profiles_path=workers_path)
    for profile in registry.profiles(probe=False):
        if profile.status == "offline":
            continue
        headers = worker_headers(worker_cfg, profile)
        try:
            response = http_get(f"{profile.base_url}/sessions", headers=headers, timeout=worker_cfg.request_timeout_s)
            if getattr(response, "status_code", 200) >= 400:
                continue
            for raw in response.json().get("sessions", []):
                if not isinstance(raw, dict):
                    continue
                ref = make_session_ref(profile.worker_id, str(raw.get("session_id") or ""))
                run = run_by_id.get(str(raw.get("run_id") or ""))
                sessions[ref] = _session_from_worker(raw, profile.worker_id, run=run)
        except Exception:  # noqa: BLE001 - aggregate views must remain inspectable
            continue
    return sessions


def aggregate_requests(*, worker_cfg: WorkerConfig, workers_path: str, http_get: Any = httpx.get) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    registry = WorkerRegistry(worker_cfg, profiles_path=workers_path)
    for profile in registry.profiles(probe=False):
        headers = worker_headers(worker_cfg, profile)
        try:
            response = http_get(f"{profile.base_url}/sessions/requests", headers=headers, timeout=worker_cfg.request_timeout_s)
            if getattr(response, "status_code", 200) >= 400:
                continue
            for raw in response.json().get("requests", []):
                if isinstance(raw, dict):
                    results.append(project_request(raw, profile.worker_id))
        except Exception:  # noqa: BLE001
            continue
    return results


def aggregate_checkpoints(
    *,
    runs: list[OrchestrationRun],
    worker_cfg: WorkerConfig,
    workers_path: str,
    http_get: Any = httpx.get,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    registry = WorkerRegistry(worker_cfg, profiles_path=workers_path)
    for run in runs:
        for link in run.sessions:
            profile = registry.get(link.worker_id, probe=False)
            if profile is None:
                continue
            headers = worker_headers(worker_cfg, profile)
            try:
                response = http_get(
                    f"{profile.base_url}/sessions/{link.session_id}/checkpoints",
                    headers=headers,
                    timeout=worker_cfg.request_timeout_s,
                )
                if getattr(response, "status_code", 200) >= 400:
                    continue
                for raw in response.json().get("checkpoints", []):
                    if isinstance(raw, dict):
                        item = project_checkpoint(raw, link.worker_id, link.session_id, run.run_id)
                        item["session_ref"] = make_session_ref(link.worker_id, link.session_id)
                        item["run_id"] = run.run_id
                        results.append(item)
            except Exception:  # noqa: BLE001
                continue
    return results


def run_summary(
    run: OrchestrationRun,
    *,
    requests: list[dict[str, Any]] | None = None,
    artifacts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    requests = requests or []
    artifacts = artifacts or []
    run_session_refs = {make_session_ref(session.worker_id, session.session_id) for session in run.sessions}
    run_requests = [request for request in requests if request.get("run_id") == run.run_id or request.get("session_ref") in run_session_refs]
    run_artifacts = [artifact for artifact in artifacts if artifact.get("run_id") == run.run_id]
    repo = _run_repo(run)
    branch = next((session.branch for session in run.sessions if session.branch), "")
    return {
        "run_id": run.run_id,
        "title": _title(run.objective),
        "objective": _redact(run.objective),
        "status": run.status,
        "phase": run.phase,
        "repo": repo,
        "branch": branch,
        "session_count": len(run.sessions),
        "active_session_count": sum(1 for session in run.sessions if session.status in ACTIVE_SESSION_STATUSES),
        "pending_input_count": sum(1 for request in run_requests if request.get("kind") == "input"),
        "pending_approval_count": sum(1 for request in run_requests if request.get("kind") == "approval"),
        "artifact_count": len(run_artifacts),
        "primary_artifact_ids": [artifact["artifact_id"] for artifact in run_artifacts if artifact.get("is_primary")],
        "latest_activity_at": run.updated_at,
        "latest_cursor": _run_cursor(run),
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "terminal_reason": _redact(run.terminal_reason) or None,
    }


def run_detail_projection(
    run: OrchestrationRun,
    *,
    requests: list[dict[str, Any]] | None = None,
    artifacts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    run_artifacts = [artifact for artifact in artifacts or [] if artifact.get("run_id") == run.run_id]
    return {
        **run_summary(run, requests=requests, artifacts=artifacts),
        "work_items": [
            {
                "source": link.item.source,
                "id": link.item.id,
                "kind": link.item.kind,
                "title": _redact(link.item.title),
                "url": _public_url(link.item.url),
                "role": link.role,
                "status": link.item.status,
                "priority": link.item.priority,
                "labels": list(link.item.labels),
            }
            for link in run.work_items
        ],
        "sessions": [session_summary(_session_from_link(session, run), requests=requests, checkpoints=[]) for session in run.sessions],
        "artifacts": run_artifacts,
    }


def session_summary(
    session: dict[str, Any],
    *,
    requests: list[dict[str, Any]] | None = None,
    checkpoints: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    requests = requests or []
    checkpoints = checkpoints or []
    ref = str(session["session_ref"])
    session_requests = [request for request in requests if request.get("session_ref") == ref]
    return {
        "session_ref": ref,
        "worker_id": session.get("worker_id", ""),
        "session_id": session.get("session_id", ""),
        "run_id": session.get("run_id", ""),
        "title": _redact(str(session.get("title") or "")),
        "provider": session.get("provider", ""),
        "engine": session.get("engine", ""),
        "status": session.get("status", ""),
        "repo": session.get("repo", ""),
        "branch": session.get("branch", ""),
        "cwd_label": cwd_label(str(session.get("cwd") or "")),
        "latest_event_cursor": session.get("latest_event_cursor", ""),
        "pending_input_count": sum(1 for request in session_requests if request.get("kind") == "input"),
        "pending_approval_count": sum(1 for request in session_requests if request.get("kind") == "approval"),
        "checkpoint_count": sum(1 for checkpoint in checkpoints if checkpoint.get("session_ref") == ref),
        "created_at": session.get("created_at", ""),
        "updated_at": session.get("updated_at", ""),
    }


def project_session_event(raw: dict[str, Any], *, worker_id: str, run_id: str = "", sequence: int = 0) -> dict[str, Any]:
    data = dict(raw.get("data") or {})
    session_id = str(raw.get("session_id") or "")
    turn_id = str(data.get("turn_id") or data.get("id") or "")
    return {
        "event_id": str(raw.get("event_id") or ""),
        "sequence": sequence,
        "session_ref": make_session_ref(worker_id, session_id),
        "run_id": run_id or str(data.get("run_id") or ""),
        "type": str(raw.get("type") or ""),
        "occurred_at": str(raw.get("time") or raw.get("occurred_at") or ""),
        "turn_id": turn_id,
        "message_id": _message_id(raw, data, turn_id),
        "data": public_event_data(data),
    }


def project_request(raw: dict[str, Any], worker_id: str) -> dict[str, Any]:
    event = dict(raw.get("event") or {})
    data = dict(event.get("data") or {})
    session_id = str(raw.get("session_id") or data.get("session_id") or event.get("session_id") or "")
    kind = str(raw.get("kind") or "")
    detail = str(data.get("detail") or data.get("command") or data.get("path") or data.get("prompt") or "")
    result = {
        "request_id": str(raw.get("request_id") or data.get("request_id") or data.get("id") or event.get("event_id") or ""),
        "session_ref": make_session_ref(worker_id, session_id),
        "run_id": str(data.get("run_id") or ""),
        "kind": kind,
        "status": str(raw.get("status") or "pending"),
        "title": str(data.get("title") or ("Approve action" if kind == "approval" else "Input needed")),
        "detail": _redact(detail),
        "created_at": str(event.get("time") or data.get("created_at") or ""),
        "expires_at": data.get("expires_at"),
        "payload": public_event_data(dict(data.get("payload") or data)),
    }
    if kind == "input":
        result["questions"] = data.get("questions") if isinstance(data.get("questions"), list) else [
            {"id": "response", "header": "Input", "question": str(data.get("question") or "Input needed"), "options": []}
        ]
    return result


def project_checkpoint(raw: dict[str, Any], worker_id: str, session_id: str, run_id: str) -> dict[str, Any]:
    checkpoint_id = str(raw.get("checkpoint_id") or raw.get("id") or "")
    return {
        "checkpoint_id": checkpoint_id,
        "session_ref": make_session_ref(worker_id, session_id),
        "run_id": run_id,
        "label": _redact(str(raw.get("label") or "")),
        "provider": _redact(str(raw.get("provider") or "")),
        "status": _redact(str(raw.get("status") or "")),
        "restored": bool(raw.get("restored", False)),
        "created_at": str(raw.get("created_at") or raw.get("time") or ""),
        "updated_at": str(raw.get("updated_at") or raw.get("time") or ""),
        "payload": public_event_data(dict(raw.get("payload") or {})),
    }


def artifact_summaries(runs: list[OrchestrationRun]) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for run in runs:
        for session in run.sessions:
            if session.branch:
                artifacts.append(
                    {
                        "artifact_id": artifact_id(run.run_id, "branch", session.branch),
                        "run_id": run.run_id,
                        "session_ref": make_session_ref(session.worker_id, session.session_id),
                        "kind": "branch",
                        "provider": "git",
                        "external_id": session.branch,
                        "is_primary": not any(a.type == "pull_request" for a in run.artifacts),
                        "visibility": "public-safe",
                        "title": session.branch,
                        "status": session.status,
                        "summary": "",
                        "url": "",
                        "branch": session.branch,
                        "commit_sha": "",
                        "created_at": run.created_at,
                        "updated_at": run.updated_at,
                        "metadata": {},
                    }
                )
        for artifact in run.artifacts:
            if not artifact.public:
                continue
            artifacts.append(project_artifact(artifact, run))
    return artifacts


def project_artifact(artifact: Artifact, run: OrchestrationRun) -> dict[str, Any]:
    kind = _artifact_kind(artifact.type)
    return {
        "artifact_id": artifact.id or artifact_id(run.run_id, kind, artifact.url or artifact.name),
        "run_id": run.run_id,
        "session_ref": "",
        "kind": kind,
        "provider": _artifact_provider(artifact.url),
        "external_id": artifact.id,
        "is_primary": kind in {"pull_request", "report"},
        "visibility": "public-safe",
        "title": artifact.name or artifact.url or artifact.id or kind,
        "status": artifact.status,
        "summary": "",
        "url": _public_url(artifact.url),
        "branch": "",
        "commit_sha": "",
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "metadata": {},
    }


def paged(items: list[dict[str, Any]], *, after: str = "", limit: int = 100) -> dict[str, Any]:
    if after:
        for idx, item in enumerate(items):
            if after in {str(item.get("event_id") or ""), str(item.get("artifact_id") or ""), str(item.get("cursor") or "")}:
                items = items[idx + 1 :]
                break
    page_limit = max(1, min(int(limit or 100), MAX_PAGE_LIMIT))
    page = items[:page_limit]
    cursor = ""
    if page:
        last = page[-1]
        cursor = str(last.get("event_id") or last.get("artifact_id") or last.get("cursor") or "")
    return {"items": page, "cursor": cursor, "has_more": len(items) > page_limit}


def worker_headers(worker_cfg: WorkerConfig, profile: WorkerProfile) -> dict[str, str]:
    import os

    token = os.environ.get(profile.token_env, "") if profile.token_env else ""
    if not token and profile.worker_id == "local-worker":
        token = worker_cfg.token.get_secret_value()
    return {"Authorization": f"Bearer {token}"} if token else {}


def snapshot_cursor(runs: list[OrchestrationRun], sessions: dict[str, dict[str, Any]], artifacts: list[dict[str, Any]]) -> str:
    basis = {
        "runs": [(run.run_id, run.updated_at, run.phase, run.status) for run in runs],
        "sessions": [(ref, item.get("updated_at"), item.get("latest_event_cursor")) for ref, item in sorted(sessions.items())],
        "artifacts": [(item.get("artifact_id"), item.get("updated_at")) for item in artifacts],
    }
    digest = hashlib.sha256(json.dumps(basis, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"{CURSOR_PREFIX}{digest}"


def artifact_id(run_id: str, kind: str, key: str) -> str:
    digest = hashlib.sha256(f"{run_id}\0{kind}\0{key}".encode("utf-8")).hexdigest()[:16]
    return f"artifact_{digest}"


def cwd_label(cwd: str) -> str:
    if not cwd:
        return ""
    return pathlib.Path(cwd).name or "workspace"


def _session_from_link(link: WorkerSessionLink, run: OrchestrationRun) -> dict[str, Any]:
    return {
        "session_ref": make_session_ref(link.worker_id, link.session_id),
        "worker_id": link.worker_id,
        "session_id": link.session_id,
        "run_id": run.run_id,
        "title": run.objective,
        "provider": link.provider,
        "engine": link.engine,
        "status": link.status,
        "repo": _run_repo(run),
        "branch": link.branch,
        "cwd": link.cwd,
        "latest_event_cursor": link.last_event_id,
        "created_at": run.created_at,
        "updated_at": run.updated_at,
    }


def _session_from_worker(raw: dict[str, Any], worker_id: str, *, run: OrchestrationRun | None) -> dict[str, Any]:
    session_id = str(raw.get("session_id") or "")
    return {
        "session_ref": make_session_ref(worker_id, session_id),
        "worker_id": worker_id,
        "session_id": session_id,
        "run_id": str(raw.get("run_id") or (run.run_id if run else "")),
        "title": str(raw.get("title") or (run.objective if run else "")),
        "provider": str(raw.get("provider") or ""),
        "engine": str(raw.get("engine") or raw.get("provider") or ""),
        "status": str(raw.get("status") or ""),
        "repo": str(raw.get("repo") or (_run_repo(run) if run else "")),
        "branch": str(raw.get("branch") or ""),
        "cwd": str(raw.get("cwd") or ""),
        "latest_event_cursor": str(raw.get("last_event_id") or ""),
        "created_at": str(raw.get("created_at") or (run.created_at if run else "")),
        "updated_at": str(raw.get("updated_at") or (run.updated_at if run else "")),
    }


def _engine_catalog(engine: str, display_name: str, description: str) -> dict[str, Any]:
    return {"engine": engine, "display_name": display_name, "description": description, "supports": _engine_supports(engine)}


def _engine_row(engine: str, *, default: bool, worker_status: str) -> dict[str, Any]:
    return {
        "engine": engine,
        "display_name": engine.capitalize(),
        "status": "available" if worker_status != "offline" else "unavailable",
        "default": default,
        "supports": _engine_supports(engine),
    }


def _engine_supports(engine: str) -> dict[str, bool]:
    if engine == "claude":
        return {
            "streaming": True,
            "resume": True,
            "interrupt": True,
            "approval_requests": False,
            "input_requests": False,
            "checkpoints": False,
        }
    return {
        "streaming": True,
        "resume": True,
        "interrupt": True,
        "approval_requests": True,
        "input_requests": True,
        "checkpoints": engine == "codex",
    }


def _public_worker_capabilities(profile: WorkerProfile) -> list[str]:
    caps = set(profile.capabilities)
    result: set[str] = set()
    if {"git", "codex", "claude", "python", "uv"} & caps or profile.supported_engines:
        result.add("code.edit")
    if "shell" in caps or "python" in caps or "uv" in caps:
        result.add("shell.run")
    if "browser" in caps:
        result.add("browser.use")
    if "git" in caps:
        result.add("git.branch")
    if FORGE_PR_CREATE in caps:
        result.add("github.pr.create")
    if FORGE_PR_COMMENT in caps:
        result.add("github.pr.comment")
    return sorted(result)


def _worker_health(status: str) -> str:
    if status == "online":
        return "healthy"
    if status == "degraded":
        return "degraded"
    if status == "offline":
        return "unhealthy"
    return "unknown"


def _run_repo(run: OrchestrationRun | None) -> str:
    if run is None:
        return ""
    return next((link.item.repo for link in run.work_items if link.item.repo), "")


def _run_cursor(run: OrchestrationRun) -> str:
    digest = hashlib.sha256(json.dumps(run.to_dict(), sort_keys=True).encode("utf-8")).hexdigest()[:16]
    return f"{CURSOR_PREFIX}{digest}"


def _title(objective: str) -> str:
    text = _redact(objective).strip()
    return text[:80] if text else "Untitled run"


def _message_id(raw: dict[str, Any], data: dict[str, Any], turn_id: str) -> str:
    message = data.get("message")
    explicit = data.get("message_id") or (message.get("id") if isinstance(message, dict) else "")
    if explicit:
        return str(explicit)
    if raw.get("type") == "assistant.delta" and turn_id:
        return f"msg_{turn_id}"
    return str(data.get("id") or "")


def _artifact_kind(value: str) -> str:
    text = str(value or "").lower()
    if text in {"pr", "pull_request"}:
        return "pull_request"
    if text in {"branch", "report", "verification", "log", "file", "url", "status_comment", "provider_evidence"}:
        return text
    return text or "url"


def _artifact_provider(url: str) -> str:
    if "github.com" in str(url or ""):
        return "github"
    if "linear.app" in str(url or ""):
        return "linear"
    return ""


def _public_url(value: str) -> str:
    text = str(value or "")
    if text.startswith(("https://github.com/", "https://linear.app/")):
        return text
    return ""


def _redact(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"/Users/[^\s)]+", "<local-path>", text)
    text = re.sub(r"\b(?:lin_api|ghp|github_pat|sk-[A-Za-z0-9])[A-Za-z0-9_\-]{12,}\b", "<redacted-token>", text)
    return text


def public_error_message(value: str) -> str:
    text = _redact(value)
    text = re.sub(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b", "<redacted-email>", text)
    return text[:300]


def public_event_data(data: dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): _public_value(value)
        for key, value in data.items()
        if _public_key(key) in PUBLIC_EVENT_DATA_KEYS and _public_value(value) not in ("", [], {})
    }


def _public_value(value: Any) -> Any:
    if isinstance(value, str):
        return _redact(value)
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int | float):
        return value
    if isinstance(value, list):
        return [item for item in (_public_value(item) for item in value) if item not in ("", [], {})]
    if isinstance(value, dict):
        return {
            str(key): item
            for key, raw in value.items()
            if _public_key(key) not in PRIVATE_PUBLIC_KEYS
            for item in [_public_value(raw)]
            if item not in ("", [], {})
        }
    return _redact(str(value))


def _public_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _merge_sync(left: SyncSummary, right: SyncSummary) -> SyncSummary:
    return SyncSummary(
        runs_seen=max(left.runs_seen, right.runs_seen),
        jobs_seen=left.jobs_seen,
        jobs_updated=left.jobs_updated,
        sessions_seen=right.sessions_seen,
        sessions_updated=right.sessions_updated,
        session_events_seen=right.session_events_seen,
        runs_completed=left.runs_completed + right.runs_completed,
        runs_failed=left.runs_failed + right.runs_failed,
        errors=[*(left.errors or []), *(right.errors or [])],
    )


def _body_fingerprint(body: dict[str, Any]) -> str:
    normalized = dict(body or {})
    normalized.pop("idempotency_key", None)
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def run_report_artifact(store: OrchestrationStore, run_id: str) -> dict[str, Any]:
    report = build_run_report(store, run_id)
    run = store.get(run_id)
    created_at = run.created_at if run is not None else ""
    updated_at = run.updated_at if run is not None else ""
    return {
        "artifact_id": artifact_id(run_id, "report", run_id),
        "run_id": run_id,
        "session_ref": "",
        "kind": "report",
        "provider": "jarvis",
        "external_id": run_id,
        "is_primary": False,
        "visibility": "public-safe",
        "title": f"Run report {run_id}",
        "status": str(report.get("phase") or ""),
        "summary": str(report.get("terminal_reason") or report.get("objective") or ""),
        "url": "",
        "branch": "",
        "commit_sha": "",
        "created_at": created_at,
        "updated_at": updated_at,
        "metadata": {"report": report},
    }
