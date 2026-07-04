from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

from jarvis.config import WorkerConfig
from jarvis.orchestration.models import OrchestrationRun
from jarvis.orchestration.store import OrchestrationStore
from jarvis.worker_session_contract import ACTIVE_SESSION_STATUSES, FAILED_SESSION_STATUSES, SUCCESS_SESSION_STATUSES
from jarvis.orchestration.workers import WorkerProfile, WorkerRegistry, local_worker_display_name

TERMINAL_JOB_STATUSES = {"done", "error", "interrupted"}


@dataclass
class SyncSummary:
    runs_seen: int = 0
    jobs_seen: int = 0
    jobs_updated: int = 0
    sessions_seen: int = 0
    sessions_updated: int = 0
    session_events_seen: int = 0
    runs_completed: int = 0
    runs_failed: int = 0
    errors: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "runs_seen": self.runs_seen,
            "jobs_seen": self.jobs_seen,
            "jobs_updated": self.jobs_updated,
            "sessions_seen": self.sessions_seen,
            "sessions_updated": self.sessions_updated,
            "session_events_seen": self.session_events_seen,
            "runs_completed": self.runs_completed,
            "runs_failed": self.runs_failed,
            "errors": self.errors or [],
        }


def sync_run_jobs(
    store: OrchestrationStore,
    *,
    worker_cfg: WorkerConfig,
    workers_path: str = "",
    run_id: str = "",
    get: Callable[..., Any] | None = None,
) -> SyncSummary:
    """Refresh run graph job state from worker daemon job records."""

    http_get = get or httpx.get
    registry = WorkerRegistry(worker_cfg, profiles_path=workers_path)
    runs = _runs_to_sync(store, run_id)
    summary = SyncSummary(errors=[])
    summary.runs_seen = len(runs)
    for run in runs:
        for link in run.jobs:
            summary.jobs_seen += 1
            profile = _profile_for_job(registry, worker_cfg, link.worker_id)
            if profile is None:
                _record_sync_error(store, run.run_id, link.job_id, f"worker {link.worker_id!r} is not configured")
                summary.errors.append(f"{run.run_id}:{link.job_id}: worker not configured")
                continue
            headers = _headers_for_worker(worker_cfg, profile)
            try:
                response = http_get(
                    f"{profile.base_url}/jobs/{link.job_id}",
                    headers=headers,
                    timeout=worker_cfg.request_timeout_s,
                )
                status_code = getattr(response, "status_code", 200)
                if status_code >= 400:
                    raise RuntimeError(_response_error(response, status_code))
                data = response.json()
            except Exception as exc:  # noqa: BLE001 - sync must not make inspection unusable
                _record_sync_error(store, run.run_id, link.job_id, str(exc))
                summary.errors.append(f"{run.run_id}:{link.job_id}: {exc}")
                continue
            before = link.to_dict()
            store.update_job(
                run.run_id,
                link.job_id,
                status=str(data.get("status") or link.status),
                session_id=str(data.get("session_id") or link.session_id or ""),
                session_name=str(data.get("session_name") or link.session_name or ""),
                branch=str(data.get("branch") or link.branch or ""),
                cwd=str(data.get("cwd") or link.cwd or ""),
            )
            reloaded = store.get(run.run_id)
            if reloaded is not None:
                updated = next((x for x in reloaded.jobs if x.job_id == link.job_id), link)
                if updated.to_dict() != before:
                    summary.jobs_updated += 1
                run = reloaded
        final = _final_phase(run)
        if final == "completed":
            store.set_phase(run.run_id, "completed", "All worker jobs completed")
            summary.runs_completed += 1
        elif final == "failed":
            store.set_phase(run.run_id, "failed", "At least one worker job failed or was interrupted")
            summary.runs_failed += 1
    return summary


def sync_run_sessions(
    store: OrchestrationStore,
    *,
    worker_cfg: WorkerConfig,
    workers_path: str = "",
    run_id: str = "",
    get: Callable[..., Any] | None = None,
) -> SyncSummary:
    """Refresh run graph session state from worker daemon session records."""

    http_get = get or httpx.get
    registry = WorkerRegistry(worker_cfg, profiles_path=workers_path)
    runs = _session_runs_to_sync(store, run_id)
    summary = SyncSummary(errors=[])
    summary.runs_seen = len(runs)
    for run in runs:
        for link in run.sessions:
            if link.archived_at:
                continue
            summary.sessions_seen += 1
            profile = _profile_for_job(registry, worker_cfg, link.worker_id)
            if profile is None:
                _record_sync_error(store, run.run_id, link.session_id, f"worker {link.worker_id!r} is not configured")
                summary.errors.append(f"{run.run_id}:{link.session_id}: worker not configured")
                continue
            headers = _headers_for_worker(worker_cfg, profile)
            try:
                response = http_get(
                    f"{profile.base_url}/sessions/{link.session_id}",
                    headers=headers,
                    timeout=worker_cfg.request_timeout_s,
                )
                status_code = getattr(response, "status_code", 200)
                if status_code >= 400:
                    raise RuntimeError(_response_error(response, status_code))
                data = response.json()
                events_response = http_get(
                    f"{profile.base_url}/sessions/{link.session_id}/events",
                    headers=headers,
                    params={"after": link.last_event_id} if link.last_event_id else {},
                    timeout=worker_cfg.request_timeout_s,
                )
                event_status = getattr(events_response, "status_code", 200)
                if event_status >= 400:
                    raise RuntimeError(_response_error(events_response, event_status))
                event_data = events_response.json()
            except Exception as exc:  # noqa: BLE001 - sync must not make inspection unusable
                _record_sync_error(store, run.run_id, link.session_id, str(exc))
                summary.errors.append(f"{run.run_id}:{link.session_id}: {exc}")
                continue
            events = [event for event in event_data.get("events") or [] if isinstance(event, dict)]
            last_event_id = str(events[-1].get("event_id") or link.last_event_id) if events else link.last_event_id
            if events:
                persist_session_events(store, run.run_id, link.session_id, events)
            before = link.to_dict()
            store.update_session(
                run.run_id,
                link.session_id,
                worker_id=link.worker_id,
                status=str(data.get("status") or link.status),
                provider=str(data.get("provider") or link.provider or ""),
                engine=str(data.get("engine") or link.engine or ""),
                branch=str(data.get("branch") or link.branch or ""),
                cwd=str(data.get("cwd") or link.cwd or ""),
                last_event_id=last_event_id,
            )
            summary.session_events_seen += len(events)
            reloaded = store.get(run.run_id)
            if reloaded is not None:
                updated = next((x for x in reloaded.sessions if x.worker_id == link.worker_id and x.session_id == link.session_id), link)
                if updated.to_dict() != before:
                    summary.sessions_updated += 1
                run = reloaded
        final = final_session_phase(run)
        if final == "completed":
            store.set_phase(run.run_id, "completed", "All worker sessions completed")
            summary.runs_completed += 1
        elif final == "failed":
            store.set_phase(run.run_id, "failed", "At least one worker session failed, stopped, or was interrupted")
            summary.runs_failed += 1
    return summary


def _runs_to_sync(store: OrchestrationStore, run_id: str) -> list[OrchestrationRun]:
    if run_id:
        run = store.get(run_id)
        return [] if run is None or run.archived_at else [run]
    return [run for run in store.list_runs() if not run.archived_at and run.status != "terminal" and run.jobs]


def _session_runs_to_sync(store: OrchestrationStore, run_id: str) -> list[OrchestrationRun]:
    if run_id:
        run = store.get(run_id)
        return [] if run is None or run.archived_at else [run]
    return [run for run in store.list_runs() if not run.archived_at and run.status != "terminal" and any(not session.archived_at for session in run.sessions)]


def _profile_for_job(registry: WorkerRegistry, worker_cfg: WorkerConfig, worker_id: str) -> WorkerProfile | None:
    profile = registry.get(worker_id, probe=False)
    if profile is not None:
        return profile
    if not worker_id or worker_id == "local-worker":
        return WorkerProfile(
            worker_id="local-worker",
            display_name=local_worker_display_name(),
            base_url=worker_cfg.base_url,
            token_set=bool(worker_cfg.token.get_secret_value()),
            agent=worker_cfg.agent,
        )
    return None


def _headers_for_worker(worker_cfg: WorkerConfig, profile: WorkerProfile) -> dict[str, str]:
    token = os.environ.get(profile.token_env, "") if profile.token_env else ""
    if not token and profile.worker_id == "local-worker":
        token = worker_cfg.token.get_secret_value()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _response_error(response: Any, status_code: int) -> str:
    try:
        body = response.json()
    except ValueError:
        body = {}
    if isinstance(body, dict) and body.get("error"):
        return str(body["error"])
    return getattr(response, "text", "") or f"worker request failed with HTTP {status_code}"


def persist_session_events(store: OrchestrationStore, run_id: str, session_id: str, events: list[dict[str, Any]]) -> None:
    """Append worker session events to the run's local event log.

    This is what makes the run timeline (and the cockpit's session.event SSE
    stream) durable: the worker's `/events` cursor only returns each event once,
    so discarding events here would lose them. Dedup by event_id so overlapping
    syncs, dispatch responses, and cockpit session writes never double-append."""
    existing = {
        str(event.data.get("event_id") or "")
        for event in store.events(run_id)
        if isinstance(event.data, dict) and event.data.get("event_id")
    }
    for raw in events:
        event_id = str(raw.get("event_id") or "")
        if event_id and event_id in existing:
            continue
        data = raw.get("data") if isinstance(raw.get("data"), dict) else {}
        store.append_event(
            run_id,
            str(raw.get("type") or "session.event"),
            "",
            {
                "session_id": session_id,
                "event_id": event_id,
                "turn_id": str(data.get("turn_id") or ""),
                "message_id": str(data.get("message_id") or ""),
                "time": str(raw.get("time") or ""),
                "data": dict(data),
            },
        )
        if event_id:
            existing.add(event_id)


def _record_sync_error(store: OrchestrationStore, run_id: str, job_id: str, error: str) -> None:
    store.append_event(
        run_id,
        "job_sync_failed",
        f"Could not sync worker job {job_id}: {error}",
        {"job_id": job_id, "error": error},
    )


def _final_phase(run: OrchestrationRun) -> str:
    if not run.jobs or run.status == "terminal":
        return ""
    if run.sessions:
        return ""
    statuses = _effective_terminal_statuses(run)
    if not statuses.issubset(TERMINAL_JOB_STATUSES):
        return ""
    if statuses == {"done"}:
        return "completed"
    return "failed"


def final_session_phase(run: OrchestrationRun) -> str:
    visible_sessions = [session for session in run.sessions if not session.archived_at]
    if not visible_sessions or run.status == "terminal":
        return ""
    if any(job.status not in TERMINAL_JOB_STATUSES for job in run.jobs):
        return ""
    statuses = {session.status for session in visible_sessions}
    if statuses & ACTIVE_SESSION_STATUSES:
        return ""
    if statuses <= SUCCESS_SESSION_STATUSES:
        return "completed"
    if statuses & FAILED_SESSION_STATUSES:
        return "failed"
    return ""


def _effective_terminal_statuses(run: OrchestrationRun) -> set[str]:
    statuses: list[str] = []
    for idx, job in enumerate(run.jobs):
        if job.status in {"error", "interrupted"} and _superseded_by_successful_resume(job, run.jobs[idx + 1 :]):
            continue
        statuses.append(job.status)
    return set(statuses)


def _superseded_by_successful_resume(job, later_jobs) -> bool:  # noqa: ANN001
    if not job.session_id and not job.cwd:
        return False
    for later in later_jobs:
        if later.status != "done" or later.worker_id != job.worker_id:
            continue
        if job.session_id and later.session_id == job.session_id:
            return True
        if job.cwd and later.cwd == job.cwd:
            return True
    return False
