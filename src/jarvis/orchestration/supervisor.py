from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, TypeVar

from jarvis.config import WorkerConfig
from jarvis.orchestration.models import OrchestrationRun, WorkerJobLink, WorkerSessionLink
from jarvis.orchestration.redaction import public_error_message
from jarvis.orchestration.store import OrchestrationStore
from jarvis.worker_session_contract import ACTIVE_SESSION_STATUSES, FAILED_SESSION_STATUSES, SUCCESS_SESSION_STATUSES
from jarvis.orchestration.workers import (
    WorkerProfile,
    WorkerRegistry,
    local_worker_display_name,
    worker_auth_headers,
    worker_http_get,
)

TERMINAL_JOB_STATUSES = {"done", "error", "interrupted"}
RECONCILIATION_MAX_CONCURRENCY = 4
SESSION_EVENT_SYNC_LIMIT = 500

_Target = TypeVar("_Target")
_Observation = TypeVar("_Observation")


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


@dataclass(frozen=True)
class _JobSyncTarget:
    run_id: str
    link: WorkerJobLink
    profile: WorkerProfile | None
    headers: dict[str, str]
    skipped: bool = False


@dataclass(frozen=True)
class _JobObservation:
    target: _JobSyncTarget
    data: dict[str, Any] | None = None
    error: str = ""


@dataclass(frozen=True)
class _SessionSyncTarget:
    run_id: str
    link: WorkerSessionLink
    profile: WorkerProfile | None
    headers: dict[str, str]
    skipped: bool = False


@dataclass(frozen=True)
class _SessionObservation:
    target: _SessionSyncTarget
    data: dict[str, Any] | None = None
    events: tuple[dict[str, Any], ...] = ()
    error: str = ""


def sync_run_jobs(
    store: OrchestrationStore,
    *,
    worker_cfg: WorkerConfig,
    workers_path: str = "",
    run_id: str = "",
    get: Callable[..., Any] | None = None,
    timeout_s: float | None = None,
    should_sync_worker: Callable[[WorkerProfile], bool] | None = None,
) -> SyncSummary:
    """Refresh run graph job state from worker daemon job records."""

    http_get = get or worker_http_get
    timeout = worker_cfg.request_timeout_s if timeout_s is None else timeout_s
    runs = _runs_to_sync(store, run_id)
    summary = SyncSummary(errors=[])
    summary.runs_seen = len(runs)
    profiles = WorkerRegistry(worker_cfg, profiles_path=workers_path).profiles(probe=False)
    targets: list[_JobSyncTarget] = []
    for run in runs:
        for link in run.jobs:
            summary.jobs_seen += 1
            profile = _profile_for_job(profiles, worker_cfg, link.worker_id)
            targets.append(
                _JobSyncTarget(
                    run_id=run.run_id,
                    link=link,
                    profile=profile,
                    headers=_headers_for_worker(worker_cfg, profile) if profile is not None else {},
                    skipped=profile is not None and should_sync_worker is not None and not should_sync_worker(profile),
                )
            )

    observations = _bounded_observe(
        targets,
        lambda target: _observe_job(target, http_get=http_get, timeout=timeout),
    )
    for observation in observations:
        target = observation.target
        link = target.link
        if target.skipped:
            continue
        if target.profile is None:
            _record_sync_error(
                store,
                target.run_id,
                link.job_id,
                f"worker {link.worker_id!r} is not configured",
            )
            summary.errors.append(f"{target.run_id}:{link.job_id}: worker not configured")
            continue
        if observation.error:
            error = public_error_message(observation.error)
            _record_sync_error(store, target.run_id, link.job_id, error)
            summary.errors.append(f"{target.run_id}:{link.job_id}: {error}")
            continue
        data = observation.data or {}
        before = link.to_dict()
        updated_run = store.update_job_if_unchanged(
            target.run_id,
            link.job_id,
            expected=link,
            status=str(data.get("status") or link.status),
            session_id=str(data.get("session_id") or link.session_id or ""),
            session_name=str(data.get("session_name") or link.session_name or ""),
            branch=str(data.get("branch") or link.branch or ""),
            cwd=str(data.get("cwd") or link.cwd or ""),
        )
        if updated_run is not None:
            updated = next((x for x in updated_run.jobs if x.job_id == link.job_id), link)
            if updated.to_dict() != before:
                summary.jobs_updated += 1

    for original in runs:
        run = store.get(original.run_id) or original
        final = _final_phase(run)
        if final == "completed":
            finalized = store.set_phase_if_execution_unchanged(
                run.run_id,
                "completed",
                "All worker jobs completed",
                **_execution_fence(run),
            )
            if finalized is not None:
                summary.runs_completed += 1
        elif final == "failed":
            finalized = store.set_phase_if_execution_unchanged(
                run.run_id,
                "failed",
                "At least one worker job failed or was interrupted",
                **_execution_fence(run),
            )
            if finalized is not None:
                summary.runs_failed += 1
    return summary


def sync_run_sessions(
    store: OrchestrationStore,
    *,
    worker_cfg: WorkerConfig,
    workers_path: str = "",
    run_id: str = "",
    get: Callable[..., Any] | None = None,
    timeout_s: float | None = None,
    should_sync_worker: Callable[[WorkerProfile], bool] | None = None,
) -> SyncSummary:
    """Refresh run graph session state from worker daemon session records."""

    http_get = get or worker_http_get
    timeout = worker_cfg.request_timeout_s if timeout_s is None else timeout_s
    runs = _session_runs_to_sync(store, run_id)
    summary = SyncSummary(errors=[])
    summary.runs_seen = len(runs)
    profiles = WorkerRegistry(worker_cfg, profiles_path=workers_path).profiles(probe=False)
    targets: list[_SessionSyncTarget] = []
    for run in runs:
        for link in run.sessions:
            if link.archived_at:
                continue
            summary.sessions_seen += 1
            profile = _profile_for_job(profiles, worker_cfg, link.worker_id)
            targets.append(
                _SessionSyncTarget(
                    run_id=run.run_id,
                    link=link,
                    profile=profile,
                    headers=_headers_for_worker(worker_cfg, profile) if profile is not None else {},
                    skipped=profile is not None and should_sync_worker is not None and not should_sync_worker(profile),
                )
            )

    observations = _bounded_observe(
        targets,
        lambda target: _observe_session(target, http_get=http_get, timeout=timeout),
    )
    runs_with_more_events: set[str] = set()
    for observation in observations:
        target = observation.target
        link = target.link
        if target.skipped:
            continue
        if target.profile is None:
            _record_sync_error(
                store,
                target.run_id,
                link.session_id,
                f"worker {link.worker_id!r} is not configured",
            )
            summary.errors.append(f"{target.run_id}:{link.session_id}: worker not configured")
            continue
        if observation.error:
            error = public_error_message(observation.error)
            _record_sync_error(store, target.run_id, link.session_id, error)
            summary.errors.append(f"{target.run_id}:{link.session_id}: {error}")
            continue
        data = observation.data or {}
        events = list(observation.events)
        last_event_id = str(events[-1].get("event_id") or link.last_event_id) if events else link.last_event_id
        observed_status = str(data.get("status") or link.status)
        before = link.to_dict()
        metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}
        updated_run = store.update_session_if_unchanged(
            target.run_id,
            link.session_id,
            worker_id=link.worker_id,
            expected=link,
            events=events,
            status=str(data.get("status") or link.status),
            # Persisted so terminal rows keep their reason after the
            # worker goes offline; cleared ("") when a new turn starts.
            ended_reason=str(data.get("ended_reason") or metadata.get("ended_reason") or ""),
            provider=str(data.get("provider") or link.provider or ""),
            engine=str(data.get("engine") or link.engine or ""),
            branch=str(data.get("branch") or link.branch or ""),
            cwd=str(data.get("cwd") or link.cwd or ""),
            last_event_id=last_event_id,
        )
        if updated_run is None:
            if events or observed_status in (SUCCESS_SESSION_STATUSES | FAILED_SESSION_STATUSES):
                runs_with_more_events.add(target.run_id)
            continue
        summary.session_events_seen += len(events)
        updated = next(
            (
                item
                for item in updated_run.sessions
                if item.worker_id == link.worker_id and item.session_id == link.session_id
            ),
            link,
        )
        if updated.to_dict() != before:
            summary.sessions_updated += 1
        if len(events) == SESSION_EVENT_SYNC_LIMIT and observed_status in (
            SUCCESS_SESSION_STATUSES | FAILED_SESSION_STATUSES
        ):
            runs_with_more_events.add(target.run_id)

    for original in runs:
        run = store.get(original.run_id) or original
        if run.run_id in runs_with_more_events:
            continue
        final = final_session_phase(run)
        if final == "completed":
            finalized = store.set_phase_if_execution_unchanged(
                run.run_id,
                "completed",
                "All worker sessions completed",
                **_execution_fence(run),
            )
            if finalized is not None:
                summary.runs_completed += 1
        elif final == "failed":
            finalized = store.set_phase_if_execution_unchanged(
                run.run_id,
                "failed",
                "At least one worker session failed, stopped, or was interrupted",
                **_execution_fence(run),
            )
            if finalized is not None:
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


def _bounded_observe(
    targets: list[_Target],
    observe: Callable[[_Target], _Observation],
) -> list[_Observation]:
    if not targets:
        return []
    with ThreadPoolExecutor(
        max_workers=min(RECONCILIATION_MAX_CONCURRENCY, len(targets)),
        thread_name_prefix="jarvis-reconcile",
    ) as executor:
        return list(executor.map(observe, targets))


def _observe_job(
    target: _JobSyncTarget,
    *,
    http_get: Callable[..., Any],
    timeout: float,
) -> _JobObservation:
    if target.skipped or target.profile is None:
        return _JobObservation(target=target)
    try:
        response = http_get(
            f"{target.profile.base_url}/jobs/{target.link.job_id}",
            headers=target.headers,
            timeout=timeout,
        )
        status_code = getattr(response, "status_code", 200)
        if status_code >= 400:
            raise RuntimeError(_response_error(response, status_code))
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError("worker returned an invalid job response")
        return _JobObservation(target=target, data=data)
    except Exception as exc:  # noqa: BLE001 - sync must not make inspection unusable
        return _JobObservation(target=target, error=str(exc))


def _observe_session(
    target: _SessionSyncTarget,
    *,
    http_get: Callable[..., Any],
    timeout: float,
) -> _SessionObservation:
    if target.skipped or target.profile is None:
        return _SessionObservation(target=target)
    try:
        response = http_get(
            f"{target.profile.base_url}/sessions/{target.link.session_id}",
            headers=target.headers,
            timeout=timeout,
        )
        status_code = getattr(response, "status_code", 200)
        if status_code >= 400:
            raise RuntimeError(_response_error(response, status_code))
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError("worker returned an invalid session response")
        events_response = http_get(
            f"{target.profile.base_url}/sessions/{target.link.session_id}/events",
            headers=target.headers,
            params={
                **({"after": target.link.last_event_id} if target.link.last_event_id else {}),
                "limit": SESSION_EVENT_SYNC_LIMIT,
            },
            timeout=timeout,
        )
        event_status = getattr(events_response, "status_code", 200)
        if event_status >= 400:
            raise RuntimeError(_response_error(events_response, event_status))
        event_data = events_response.json()
        if not isinstance(event_data, dict):
            raise RuntimeError("worker returned an invalid session events response")
        events = tuple(event for event in event_data.get("events") or [] if isinstance(event, dict))
        return _SessionObservation(target=target, data=data, events=events)
    except Exception as exc:  # noqa: BLE001 - sync must not make inspection unusable
        return _SessionObservation(target=target, error=str(exc))


def _profile_for_job(
    profiles: list[WorkerProfile],
    worker_cfg: WorkerConfig,
    worker_id: str,
) -> WorkerProfile | None:
    profile = next(
        (candidate for candidate in profiles if not worker_id or candidate.worker_id == worker_id),
        None,
    )
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
    return worker_auth_headers(worker_cfg, profile)


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
    store.persist_worker_session_events(run_id, session_id, events)


def _record_sync_error(store: OrchestrationStore, run_id: str, job_id: str, error: str) -> None:
    error = public_error_message(error)
    store.append_event_if_run_visible(
        run_id,
        "job_sync_failed",
        f"Could not sync worker job {job_id}: {error}",
        {"job_id": job_id, "error": error},
    )


def _execution_fence(run: OrchestrationRun) -> dict[str, object]:
    return {
        "expected_phase": run.phase,
        "expected_status": run.status,
        "expected_jobs": tuple((job.worker_id, job.job_id, job.status) for job in run.jobs),
        "expected_sessions": tuple(
            (session.worker_id, session.session_id, session.status, session.archived_at)
            for session in run.sessions
        ),
    }


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
