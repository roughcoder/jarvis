from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

from jarvis.config import WorkerConfig
from jarvis.orchestration.models import OrchestrationRun, WorkerProfile
from jarvis.orchestration.store import OrchestrationStore
from jarvis.orchestration.workers import WorkerRegistry

TERMINAL_JOB_STATUSES = {"done", "error", "interrupted"}


@dataclass
class SyncSummary:
    runs_seen: int = 0
    jobs_seen: int = 0
    jobs_updated: int = 0
    runs_completed: int = 0
    runs_failed: int = 0
    errors: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "runs_seen": self.runs_seen,
            "jobs_seen": self.jobs_seen,
            "jobs_updated": self.jobs_updated,
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


def _runs_to_sync(store: OrchestrationStore, run_id: str) -> list[OrchestrationRun]:
    if run_id:
        run = store.get(run_id)
        return [] if run is None else [run]
    return [run for run in store.list_runs() if run.status != "terminal" and run.jobs]


def _profile_for_job(registry: WorkerRegistry, worker_cfg: WorkerConfig, worker_id: str) -> WorkerProfile | None:
    profile = registry.get(worker_id, probe=False)
    if profile is not None:
        return profile
    if not worker_id or worker_id == "local-worker":
        return WorkerProfile(
            worker_id="local-worker",
            display_name="Local worker",
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
    statuses = {job.status for job in run.jobs}
    if not statuses.issubset(TERMINAL_JOB_STATUSES):
        return ""
    if statuses == {"done"}:
        return "completed"
    return "failed"
