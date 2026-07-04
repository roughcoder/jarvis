from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import pathlib
import re
import time
from dataclasses import dataclass
from typing import Any

import httpx

from jarvis.capabilities import (
    FORGE_BRANCH_PUSH,
    FORGE_PR_COMMENT,
    FORGE_PR_CREATE,
    WORKER_JOB_START,
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_CREATE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_RESTORE,
    WORKER_SESSION_STOP,
    WORKER_SESSION_TURN,
)
from jarvis.config import WorkerConfig
from jarvis.ids import utc_now
from jarvis.orchestration.models import Artifact, OrchestrationRun, WorkerProfile, WorkerSessionLink
from jarvis.orchestration.redaction import public_error_message
from jarvis.orchestration.redaction import public_url as _public_url
from jarvis.orchestration.redaction import redact as _redact
from jarvis.orchestration.reports import build_run_report
from jarvis.orchestration.store import OrchestrationStore
from jarvis.orchestration.supervisor import SyncSummary, sync_run_jobs, sync_run_sessions
from jarvis.orchestration.workers import WorkerRegistry
from jarvis.worker_session_contract import ACTIVE_SESSION_STATUSES

API_VERSION = "v1"
SCHEMA_VERSION = 1
IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60

SESSION_REF_PREFIX = "sessref_"
SESSION_REF_SIGNING_CONTEXT = b"jarvis-cockpit-session-ref-v1"
SESSION_REF_SIGNATURE_BYTES = 12
CURSOR_PREFIX = "evt_"
MAX_PAGE_LIMIT = 500
RUN_SUPPORTED_CONTROLS = ["archive"]
SESSION_CONTROL_ACTIONS = {
    "turn": WORKER_SESSION_TURN,
    "input": WORKER_SESSION_INPUT,
    "approval": WORKER_SESSION_APPROVE,
    "interrupt": WORKER_SESSION_INTERRUPT,
    "stop": WORKER_SESSION_STOP,
    "checkpoint_restore": WORKER_SESSION_RESTORE,
}
DEFAULT_SESSION_ALLOWED_ACTIONS = [WORKER_SESSION_TURN, WORKER_SESSION_INTERRUPT, WORKER_SESSION_STOP]
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
PRIVATE_PUBLIC_KEY_PATTERNS = (
    "apikey",
    "authorization",
    "credential",
    "password",
    "secret",
    "token",
)
LANDING_MODES = ["branch_only", "draft_pr", "ready_pr", "confirm_before_pr"]
WORK_SOURCES = ["manual", "github", "linear"]
ENGINE_STRATEGIES = ["single", "parallel"]
START_REQUIRED_FIELDS = {
    "manual": ["phrase or work_item.title", "repo (unless a default repo is configured)"],
    "github": ["repo (unless a default repo is configured)"],
    "linear": [],
}
# Worker/provider event names normalized to the canonical cockpit vocabulary
# before they reach clients. Keep this table in sync with docs/COCKPIT_API.md.
SESSION_EVENT_TYPE_ALIASES = {
    "provider.thread.ready": "provider.session.ready",
}
PHASE_STATE_REASONS = {
    "created": "Run created; no worker session dispatched yet",
    "claimed": "Work item claimed",
    "provisioned": "Worker session provisioned",
    "running": "Worker sessions active",
    "verifying": "Verification in progress",
    "landing": "Landing changes",
    "handoff": "Waiting for operator handoff",
    "blocked": "Run is blocked",
    "stalled": "Run has stalled",
    "needs_human": "Waiting for a human decision",
    "completed": "All worker sessions completed",
    "done": "Run completed",
    "failed": "Run failed",
    "cancelled": "Run cancelled",
}
BLOCKED_PHASES = {"blocked", "stalled", "needs_human"}
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
        except (OSError, json.JSONDecodeError):
            path.unlink(missing_ok=True)
            return None
        if not isinstance(record, dict):
            path.unlink(missing_ok=True)
            return None
        if _idempotency_expired(record):
            path.unlink(missing_ok=True)
            return None
        if record.get("fingerprint") != _body_fingerprint(body):
            raise CockpitError("idempotency_conflict", "idempotency key was reused with a different request body", status=409)
        response = dict(record.get("response") or {})
        response["idempotent"] = True
        return response

    def save(self, scope: str, key: str, body: dict[str, Any], response: dict[str, Any]) -> None:
        if not key:
            return
        self.prune()
        path = self._path(scope, key)
        record = {"created_at": time.time(), "fingerprint": _body_fingerprint(body), "response": response}
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(record, indent=2, sort_keys=True))
        tmp.replace(path)

    def prune(self, *, now: float | None = None) -> None:
        current = time.time() if now is None else now
        for path in self.root.glob("*.json"):
            try:
                record = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                path.unlink(missing_ok=True)
                continue
            if not isinstance(record, dict):
                path.unlink(missing_ok=True)
                continue
            if _idempotency_expired(record, now=current):
                path.unlink(missing_ok=True)

    def _path(self, scope: str, key: str) -> pathlib.Path:
        digest = hashlib.sha256(f"{scope}\0{key}".encode("utf-8")).hexdigest()
        return self.root / f"{digest}.json"


def make_session_ref(worker_id: str, session_id: str) -> str:
    raw = f"{worker_id}\0{session_id}".encode("utf-8")
    return f"{SESSION_REF_PREFIX}{_session_ref_digest(raw)}"


def valid_session_ref(value: str) -> bool:
    text = str(value or "")
    token = text[len(SESSION_REF_PREFIX):] if text.startswith(SESSION_REF_PREFIX) else ""
    return bool(token) and all(ch.isalnum() or ch in {"_", "-"} for ch in token)


def _session_ref_digest(raw: bytes) -> str:
    digest = hmac.new(SESSION_REF_SIGNING_CONTEXT, SESSION_REF_SIGNING_CONTEXT + b"\0" + raw, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest[:SESSION_REF_SIGNATURE_BYTES]).decode("ascii").rstrip("=")


CATALOG_ENGINE_LABELS = {
    "codex": ("Codex", "OpenAI Codex provider session"),
    "claude": ("Claude", "Claude provider session"),
}


def cockpit_catalog(*, start_defaults: dict[str, Any] | None = None, engines: list[str] | None = None) -> dict[str, Any]:
    defaults = {
        "source": "manual",
        "worker_id": "",
        "repo": "",
        "engine": "",
        "engine_strategy": "single",
        "landing_mode": "",
        **{key: value for key, value in (start_defaults or {}).items() if value},
    }
    engine_ids = list(engines) if engines else list(CATALOG_ENGINE_LABELS)
    engine_rows = [
        _engine_catalog(
            engine,
            *CATALOG_ENGINE_LABELS.get(engine, (engine.capitalize(), f"{engine.capitalize()} provider session")),
        )
        for engine in engine_ids
    ]
    return {
        "api_version": API_VERSION,
        "schema_version": SCHEMA_VERSION,
        "engines": engine_rows,
        "start_options": {
            "sources": list(WORK_SOURCES),
            "engines": engine_ids,
            "engine_strategies": list(ENGINE_STRATEGIES),
            "landing_modes": list(LANDING_MODES),
            "required_fields": {key: list(value) for key, value in START_REQUIRED_FIELDS.items()},
            "defaults": defaults,
        },
        "capabilities": [
            {"capability": "code.edit", "display_name": "Edit code", "maps_to": [WORKER_SESSION_CREATE, WORKER_SESSION_TURN]},
            {"capability": "shell.run", "display_name": "Run shell commands", "maps_to": [WORKER_JOB_START]},
            {"capability": "browser.use", "display_name": "Use browser", "maps_to": ["worker.browser"]},
            {"capability": "git.branch", "display_name": "Create branches", "maps_to": [FORGE_BRANCH_PUSH]},
            {"capability": "github.pr.create", "display_name": "Create pull requests", "maps_to": [FORGE_PR_CREATE]},
            {"capability": "github.pr.comment", "display_name": "Comment on pull requests", "maps_to": [FORGE_PR_COMMENT]},
            {"capability": "cockpit.archive", "display_name": "Archive cockpit runs and sessions", "maps_to": ["orchestration.runs.write"]},
        ],
        "work_sources": ["manual", "github", "linear"],
        "engine_strategies": ["single", "parallel"],
        "request_kinds": ["approval", "input"],
    }


def cockpit_snapshot(
    *,
    store: OrchestrationStore,
    worker_cfg: WorkerConfig,
    workers_path: str,
    sync_mode: str = "none",
    http_get: Any = httpx.get,
    default_repo: str = "",
) -> dict[str, Any]:
    sync = sync_state(store=store, worker_cfg=worker_cfg, workers_path=workers_path, sync_mode=sync_mode, http_get=http_get)
    all_runs = store.list_runs()
    archived_run_ids = {run.run_id for run in all_runs if run.archived_at}
    archived_session_refs = archived_session_refs_for_store(store, all_runs)
    runs = [run for run in all_runs if not run.archived_at]
    include_worker_state = sync["mode"] in {"fast", "probe"}
    workers = worker_profiles(
        worker_cfg=worker_cfg,
        workers_path=workers_path,
        probe=sync["mode"] == "probe",
        http_get=http_get,
        default_repo=default_repo,
    )
    worker_by_id = {worker["worker_id"]: worker for worker in workers}
    sessions = aggregate_sessions(
        runs=runs,
        worker_cfg=worker_cfg,
        workers_path=workers_path,
        http_get=http_get,
        worker_by_id=worker_by_id,
        include_worker_state=include_worker_state,
        archived_run_ids=archived_run_ids,
        archived_session_refs=archived_session_refs,
    )
    requests = aggregate_requests(worker_cfg=worker_cfg, workers_path=workers_path, http_get=http_get) if include_worker_state else []
    checkpoints = (
        aggregate_checkpoints(runs=runs, sessions=sessions, worker_cfg=worker_cfg, workers_path=workers_path, http_get=http_get)
        if include_worker_state
        else []
    )
    artifacts = artifact_summaries(runs)
    run_rows = [run_summary(run, requests=requests, artifacts=artifacts) for run in runs]
    session_rows = [
        session_summary(session, requests=requests, checkpoints=checkpoints)
        for session in sorted(sessions.values(), key=lambda x: str(x.get("updated_at") or ""))
    ]
    store.record_session_refs(_session_ref_index_rows(sessions.values()))
    return {
        "api_version": API_VERSION,
        "schema_version": SCHEMA_VERSION,
        "cursor": snapshot_cursor(
            {
                "sync": {"mode": sync["mode"], "status": sync["status"], "errors": sync["errors"]},
                "runs": run_rows,
                "sessions": session_rows,
                "workers": [_cursor_worker(worker) for worker in workers],
                "artifacts": artifacts,
                "requests": requests,
                "checkpoints": checkpoints,
            }
        ),
        "generated_at": utc_now(),
        "sync": sync,
        "runs": run_rows,
        "sessions": session_rows,
        "workers": workers,
        "artifacts": artifacts,
        "requests": requests,
        "checkpoints": checkpoints,
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
        "errors": [public_error_message(error) for error in summary.errors],
    }


def worker_profiles(
    *,
    worker_cfg: WorkerConfig,
    workers_path: str,
    probe: bool = False,
    http_get: Any = httpx.get,
    default_repo: str = "",
) -> list[dict[str, Any]]:
    registry = WorkerRegistry(worker_cfg, profiles_path=workers_path, http_get=http_get)
    return [project_worker_profile(profile, default_repo=default_repo) for profile in registry.profiles(probe=probe)]


def project_worker_profile(profile: WorkerProfile, *, default_repo: str = "") -> dict[str, Any]:
    engines = [
        _engine_row(
            engine,
            default=(engine == profile.default_engine),
            worker_status=profile.status,
            supports=profile.engine_supports.get(engine, {}),
        )
        for engine in profile.supported_engines
    ]
    mapped_capabilities = _public_worker_capabilities(profile)
    return {
        "worker_id": profile.worker_id,
        "display_name": profile.display_name,
        "status": profile.status,
        "health": _worker_health(profile.status),
        "last_seen_at": profile.last_seen_at,
        "capabilities": mapped_capabilities,
        "engines": engines,
        "capacity": {
            "max_sessions": profile.max_concurrent_jobs,
            "active_sessions": profile.current_jobs,
            "queued_sessions": 0,
        },
        "system": project_worker_system(profile.system),
        "repositories": _repository_rows(profile.repositories, profile.default_repo or default_repo),
        "public_metadata": {},
    }


def _repository_rows(raw_rows: list[dict[str, Any]], default_repo: str) -> list[dict[str, Any]]:
    rows = []
    for raw in raw_rows or []:
        repo = str(raw.get("repo") or raw.get("name") or "")
        if not repo:
            continue
        status = str(raw.get("status") or "ready")
        rows.append(
            {
                "repo": repo,
                "status": status,
                "default_branch": str(raw.get("default_branch") or ""),
                "is_default": _same_repo(repo, default_repo),
                "can_start_work": status == "ready",
            }
        )
    return rows


def _same_repo(repo: str, default_repo: str) -> bool:
    # The configured default may be "org/name" while workers publish the bare
    # checkout directory name; match on the trailing name in that case.
    if not repo or not default_repo:
        return False
    return repo == default_repo or repo.rsplit("/", 1)[-1] == default_repo.rsplit("/", 1)[-1]


def project_worker_system(system: Any) -> dict[str, Any]:
    if not isinstance(system, dict):
        system = {}
    disks = []
    for disk in system.get("disk") or []:
        if not isinstance(disk, dict):
            continue
        disks.append(
            {
                "mount": disk.get("mount"),
                "total_bytes": disk.get("total_bytes"),
                "available_bytes": disk.get("available_bytes"),
                "used_percent": disk.get("used_percent"),
            }
        )
    return {
        "hostname": system.get("hostname"),
        "platform": system.get("platform"),
        "arch": system.get("arch"),
        "os_name": system.get("os_name"),
        "os_version": system.get("os_version"),
        "cpu_model": system.get("cpu_model"),
        "cpu_cores_physical": system.get("cpu_cores_physical"),
        "cpu_cores_logical": system.get("cpu_cores_logical"),
        "memory_total_bytes": system.get("memory_total_bytes"),
        "memory_available_bytes": system.get("memory_available_bytes"),
        "memory_used_percent": system.get("memory_used_percent"),
        "load_average": system.get("load_average") or [None, None, None],
        "uptime_seconds": system.get("uptime_seconds"),
        "disk": disks,
        "checked_at": system.get("checked_at"),
    }


def aggregate_sessions(
    *,
    runs: list[OrchestrationRun],
    worker_cfg: WorkerConfig,
    workers_path: str,
    http_get: Any = httpx.get,
    worker_by_id: dict[str, dict[str, Any]] | None = None,
    include_worker_state: bool = True,
    archived_run_ids: set[str] | None = None,
    archived_session_refs: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    sessions: dict[str, dict[str, Any]] = {}
    worker_by_id = worker_by_id or {}
    archived_run_ids = archived_run_ids or set()
    archived_session_refs = archived_session_refs or set()
    run_by_id = {run.run_id: run for run in runs}
    for run in runs:
        if run.archived_at:
            continue
        for link in run.sessions:
            if link.archived_at:
                continue
            ref = make_session_ref(link.worker_id, link.session_id)
            sessions[ref] = _session_from_link(link, run)
    if not include_worker_state:
        return sessions
    registry = WorkerRegistry(worker_cfg, profiles_path=workers_path)
    for profile in registry.profiles(probe=False):
        projected_worker = worker_by_id.get(profile.worker_id, {})
        effective_status = str(projected_worker.get("status") or profile.status)
        if effective_status == "offline":
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
                if ref in archived_session_refs or str(raw.get("run_id") or "") in archived_run_ids:
                    continue
                run = run_by_id.get(str(raw.get("run_id") or ""))
                worker_row = _session_from_worker(raw, profile.worker_id, run=run)
                if ref in sessions:
                    stored = sessions[ref]
                    worker_row["latest_event_cursor"] = worker_row.get("latest_event_cursor") or stored.get("latest_event_cursor", "")
                    worker_row["archived_at"] = stored.get("archived_at") or worker_row.get("archived_at", "")
                    worker_row["allowed_actions"] = worker_row.get("allowed_actions") or stored.get("allowed_actions", [])
                sessions[ref] = worker_row
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
    sessions: dict[str, dict[str, Any]] | None = None,
    worker_cfg: WorkerConfig,
    workers_path: str,
    http_get: Any = httpx.get,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    registry = WorkerRegistry(worker_cfg, profiles_path=workers_path)
    rows: dict[str, dict[str, Any]] = {}
    for run in runs:
        for link in run.sessions:
            if not link.archived_at:
                rows[make_session_ref(link.worker_id, link.session_id)] = _session_from_link(link, run)
    rows.update(sessions or {})
    rows_by_worker: dict[str, list[dict[str, Any]]] = {}
    for row in rows.values():
        worker_id = str(row.get("worker_id") or "")
        session_id = str(row.get("session_id") or "")
        if not worker_id or not session_id or row.get("archived_at"):
            continue
        rows_by_worker.setdefault(worker_id, []).append(row)
    for worker_id, worker_rows in rows_by_worker.items():
        profile = registry.get(worker_id, probe=False)
        if profile is None:
            continue
        headers = worker_headers(worker_cfg, profile)
        bulk = _worker_bulk_checkpoints(profile.base_url, headers, worker_cfg.request_timeout_s, http_get)
        if bulk is not None:
            row_by_session = {str(row.get("session_id") or ""): row for row in worker_rows}
            for raw in bulk:
                session_id = str(raw.get("session_id") or "")
                row = row_by_session.get(session_id)
                if row is not None:
                    results.append(project_checkpoint(raw, worker_id, session_id, str(row.get("run_id") or "")))
            continue
        for row in worker_rows:
            session_id = str(row.get("session_id") or "")
            if not session_id:
                continue
            try:
                response = http_get(
                    f"{profile.base_url}/sessions/{session_id}/checkpoints",
                    headers=headers,
                    timeout=worker_cfg.request_timeout_s,
                )
                if getattr(response, "status_code", 200) >= 400:
                    continue
                for raw in response.json().get("checkpoints", []):
                    if isinstance(raw, dict):
                        results.append(project_checkpoint(raw, worker_id, session_id, str(row.get("run_id") or "")))
            except Exception:  # noqa: BLE001
                continue
    return results


def _worker_bulk_checkpoints(base_url: str, headers: dict[str, str], timeout: float, http_get: Any) -> list[dict[str, Any]] | None:
    try:
        response = http_get(
            f"{base_url}/sessions/checkpoints",
            headers=headers,
            timeout=timeout,
        )
        if getattr(response, "status_code", 200) >= 400:
            return None
        raw_items = response.json().get("checkpoints", [])
    except AssertionError:
        raise
    except Exception:  # noqa: BLE001 - workers may not support the bulk endpoint yet
        return None
    if not isinstance(raw_items, list):
        return None
    return [dict(item) for item in raw_items if isinstance(item, dict)]


def run_summary(
    run: OrchestrationRun,
    *,
    requests: list[dict[str, Any]] | None = None,
    artifacts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    requests = requests or []
    artifacts = artifacts or []
    visible_sessions = [session for session in run.sessions if not session.archived_at]
    run_session_refs = {make_session_ref(session.worker_id, session.session_id) for session in visible_sessions}
    run_requests = [
        request
        for request in requests
        if _request_visible_for_run(request, run.run_id, run_session_refs)
    ]
    run_artifacts = [artifact for artifact in artifacts if artifact.get("run_id") == run.run_id]
    repo = _run_repo(run)
    branch = next((session.branch for session in visible_sessions if session.branch), "")
    pending_input = sum(1 for request in run_requests if request.get("kind") == "input")
    pending_approval = sum(1 for request in run_requests if request.get("kind") == "approval")
    reason = _redact(run.terminal_reason)
    state_reason = reason or PHASE_STATE_REASONS.get(run.phase, "")
    waiting_on = []
    if pending_approval:
        waiting_on.append("approval")
    if pending_input:
        waiting_on.append("input")
    if run.phase == "needs_human":
        waiting_on.append("human")
    return {
        "authority": "jarvis",
        "supported_controls": RUN_SUPPORTED_CONTROLS,
        "run_id": run.run_id,
        "title": _title(run.objective),
        "objective": _redact(run.objective),
        "status": run.status,
        "phase": run.phase,
        "repo": repo,
        "branch": branch,
        "session_count": len(visible_sessions),
        "active_session_count": sum(1 for session in visible_sessions if session.status in ACTIVE_SESSION_STATUSES),
        "pending_input_count": pending_input,
        "pending_approval_count": pending_approval,
        "artifact_count": len(run_artifacts),
        "primary_artifact_ids": [artifact["artifact_id"] for artifact in run_artifacts if artifact.get("is_primary")],
        "latest_activity_at": run.updated_at,
        "latest_cursor": _run_cursor(run),
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "terminal_reason": reason or None,
        "state_reason": state_reason or None,
        "blocked_reason": state_reason if run.phase in BLOCKED_PHASES else None,
        "waiting_on": waiting_on,
        "last_error": reason if run.phase == "failed" else None,
        "archived_at": run.archived_at or None,
    }


def _request_visible_for_run(request: dict[str, Any], run_id: str, visible_session_refs: set[str]) -> bool:
    session_ref = str(request.get("session_ref") or "")
    if session_ref and session_ref not in visible_session_refs:
        return False
    return request.get("run_id") == run_id or session_ref in visible_session_refs


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
        "sessions": [session_summary(_session_from_link(session, run), requests=requests, checkpoints=[]) for session in run.sessions if not session.archived_at],
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
    pending_input = sum(1 for request in session_requests if request.get("kind") == "input")
    pending_approval = sum(1 for request in session_requests if request.get("kind") == "approval")
    waiting_on = []
    if pending_approval:
        waiting_on.append("approval")
    if pending_input:
        waiting_on.append("input")
    return {
        "authority": "jarvis",
        "supported_controls": _session_supported_controls(session),
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
        "pending_input_count": pending_input,
        "pending_approval_count": pending_approval,
        "waiting_on": waiting_on,
        "checkpoint_count": sum(1 for checkpoint in checkpoints if checkpoint.get("session_ref") == ref),
        "created_at": session.get("created_at", ""),
        "updated_at": session.get("updated_at", ""),
        "archived_at": session.get("archived_at") or None,
    }


def canonical_event_type(value: Any) -> str:
    text = str(value or "")
    return SESSION_EVENT_TYPE_ALIASES.get(text, text)


def project_session_event(raw: dict[str, Any], *, worker_id: str, run_id: str = "", sequence: int = 0) -> dict[str, Any]:
    data = dict(raw.get("data") or {})
    session_id = str(raw.get("session_id") or "")
    turn_id = str(data.get("turn_id") or data.get("id") or "")
    return {
        "event_id": str(raw.get("event_id") or ""),
        "sequence": sequence,
        "session_ref": make_session_ref(worker_id, session_id),
        "run_id": run_id or str(data.get("run_id") or ""),
        "type": canonical_event_type(raw.get("type")),
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
        "title": _redact(str(data.get("title") or ("Approve action" if kind == "approval" else "Input needed"))),
        "detail": _redact(detail),
        "created_at": str(event.get("time") or data.get("created_at") or ""),
        "expires_at": data.get("expires_at"),
        "payload": public_event_data(dict(data.get("payload") or data)),
    }
    if kind == "input":
        result["questions"] = _public_questions(data.get("questions")) or [
            {"id": "response", "header": "Input", "question": _redact(str(data.get("question") or "Input needed")), "options": []}
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


def artifact_summaries(runs: list[OrchestrationRun], *, include_archived: bool = False) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for run in runs:
        if run.archived_at and not include_archived:
            continue
        for session in run.sessions:
            if session.archived_at and not include_archived:
                continue
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


def archived_session_refs_for_runs(runs: list[OrchestrationRun]) -> set[str]:
    return {
        make_session_ref(session.worker_id, session.session_id)
        for run in runs
        for session in run.sessions
        if run.archived_at or session.archived_at
    }


def archived_session_refs_for_store(store: OrchestrationStore, runs: list[OrchestrationRun] | None = None) -> set[str]:
    refs = archived_session_refs_for_runs(runs if runs is not None else store.list_runs())
    refs.update(
        make_session_ref(item["worker_id"], item["session_id"])
        for item in store.archived_worker_sessions().values()
        if item.get("worker_id") and item.get("session_id")
    )
    return refs


def project_artifact(artifact: Artifact, run: OrchestrationRun) -> dict[str, Any]:
    kind = _artifact_kind(artifact.type)
    public_url = _public_url(artifact.url)
    title = _redact(artifact.name) or public_url or _redact(artifact.id) or kind
    row = {
        "artifact_id": artifact.id or artifact_id(run.run_id, kind, artifact.url or artifact.name),
        "run_id": run.run_id,
        "session_ref": "",
        "kind": kind,
        "provider": _artifact_provider(artifact.url),
        "external_id": _redact(artifact.id),
        "is_primary": kind in {"pull_request", "report"},
        "visibility": "public-safe",
        "title": title,
        "status": artifact.status,
        "summary": _redact(artifact.summary),
        "url": public_url,
        "branch": "",
        "commit_sha": "",
        "created_at": run.created_at,
        "updated_at": run.updated_at,
        "metadata": {},
    }
    if kind == "verification":
        row["command"] = _redact(artifact.command)
        row["started_at"] = artifact.started_at
        row["completed_at"] = artifact.completed_at
    return row


def paged(items: list[dict[str, Any]], *, after: str = "", limit: int = 100) -> dict[str, Any]:
    if after:
        for idx, item in enumerate(items):
            if after in {str(item.get("event_id") or ""), str(item.get("artifact_id") or ""), str(item.get("cursor") or "")}:
                items = items[idx + 1 :]
                break
        else:
            raise CockpitError("stale_cursor", "unknown pagination cursor; clear the cursor and refetch from the first page", recoverable=True, status=400)
    page_limit = max(1, min(int(limit or 100), MAX_PAGE_LIMIT))
    page = items[:page_limit]
    cursor = ""
    if page:
        last = page[-1]
        cursor = str(last.get("event_id") or last.get("artifact_id") or last.get("cursor") or "")
    return {"items": page, "cursor": cursor, "has_more": len(items) > page_limit}


def worker_headers(worker_cfg: WorkerConfig, profile: WorkerProfile) -> dict[str, str]:
    token = os.environ.get(profile.token_env, "") if profile.token_env else ""
    if not token and profile.worker_id == "local-worker":
        token = worker_cfg.token.get_secret_value()
    return {"Authorization": f"Bearer {token}"} if token else {}


def snapshot_cursor(public_projection: dict[str, Any]) -> str:
    digest = hashlib.sha256(json.dumps(public_projection, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]
    return f"{CURSOR_PREFIX}{digest}"


def _cursor_worker(worker: dict[str, Any]) -> dict[str, Any]:
    row = {key: value for key, value in worker.items() if key != "last_seen_at"}
    system = row.get("system")
    if isinstance(system, dict):
        row["system"] = {key: value for key, value in system.items() if key != "checked_at"}
    return row


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
        "archived_at": link.archived_at,
        "allowed_actions": list(link.allowed_actions),
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
        "archived_at": "",
        "allowed_actions": _allowed_actions_from_worker_session(raw),
    }


def _allowed_actions_from_worker_session(raw: dict[str, Any]) -> list[str]:
    direct = raw.get("allowed_actions")
    if isinstance(direct, list):
        return [str(item) for item in direct if item]
    metadata = raw.get("metadata")
    if not isinstance(metadata, dict):
        return []
    envelope = metadata.get("execution_envelope")
    if isinstance(envelope, dict) and isinstance(envelope.get("allowed_actions"), list):
        return [str(item) for item in envelope["allowed_actions"] if item]
    metadata_actions = metadata.get("allowed_actions")
    if isinstance(metadata_actions, list):
        return [str(item) for item in metadata_actions if item]
    return []


def _session_supported_controls(session: dict[str, Any]) -> list[str]:
    allowed_actions = set(session.get("allowed_actions") or DEFAULT_SESSION_ALLOWED_ACTIONS)
    controls = [control for control, action in SESSION_CONTROL_ACTIONS.items() if action in allowed_actions]
    controls.append("archive")
    return controls


def _engine_catalog(engine: str, display_name: str, description: str) -> dict[str, Any]:
    return {"engine": engine, "display_name": display_name, "description": description, "supports": _empty_engine_supports()}


def _engine_row(engine: str, *, default: bool, worker_status: str, supports: dict[str, bool]) -> dict[str, Any]:
    return {
        "engine": engine,
        "display_name": engine.capitalize(),
        "status": "available" if worker_status != "offline" else "unavailable",
        "default": default,
        "supports": {**_empty_engine_supports(), **{str(key): bool(value) for key, value in supports.items()}},
    }


def _empty_engine_supports() -> dict[str, bool]:
    return {
        "streaming": False,
        "resume": False,
        "interrupt": False,
        "approval_requests": False,
        "input_requests": False,
        "checkpoints": False,
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


def _session_ref_index_rows(rows: Any) -> list[dict[str, str]]:
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        result.append(
            {
                "session_ref": str(row.get("session_ref") or ""),
                "worker_id": str(row.get("worker_id") or ""),
                "session_id": str(row.get("session_id") or ""),
            }
        )
    return result


def public_event_data(data: dict[str, Any]) -> dict[str, Any]:
    return {
        str(key): _public_event_value(key, value)
        for key, value in data.items()
        if _public_key(key) in PUBLIC_EVENT_DATA_KEYS and _public_event_value(key, value) not in ("", [], {})
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
            if not _private_public_key(key)
            for item in [_public_value(raw)]
            if item not in ("", [], {})
        }
    return _redact(str(value))


def _public_questions(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    questions: list[dict[str, Any]] = []
    for raw in value:
        if not isinstance(raw, dict):
            continue
        question = {
            "id": _redact(str(raw.get("id") or "")),
            "header": _redact(str(raw.get("header") or "")),
            "question": _redact(str(raw.get("question") or "")),
            "options": _public_value(raw.get("options") if isinstance(raw.get("options"), list) else []),
        }
        questions.append({key: item for key, item in question.items() if item not in ("", [], {})})
    return questions


def _public_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _private_public_key(value: Any) -> bool:
    key = _public_key(value)
    normalized = re.sub(r"[^a-z0-9]", "", key)
    return key in PRIVATE_PUBLIC_KEYS or any(pattern in normalized for pattern in PRIVATE_PUBLIC_KEY_PATTERNS)


def _public_event_value(key: Any, value: Any) -> Any:
    if _public_key(key) == "url":
        return _public_url(str(value or ""))
    return _public_value(value)


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


def _idempotency_expired(record: dict[str, Any], *, now: float | None = None) -> bool:
    try:
        created_at = float(record.get("created_at") or 0)
    except (TypeError, ValueError):
        return True
    current = time.time() if now is None else now
    return created_at <= 0 or current - created_at > IDEMPOTENCY_TTL_SECONDS


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
