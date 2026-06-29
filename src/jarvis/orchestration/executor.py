from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any

import httpx

from jarvis.config import WorkerConfig
from jarvis.orchestration.envelope import build_execution_envelope
from jarvis.orchestration.models import ExecutionEnvelope, WorkCommand, WorkItem, WorkerJobLink
from jarvis.orchestration.store import OrchestrationStore
from jarvis.orchestration.workers import WorkerProfile


def start_worker_job(
    envelope: ExecutionEnvelope,
    *,
    worker_cfg: WorkerConfig,
    worker: WorkerProfile | None = None,
    store: OrchestrationStore | None = None,
    post: Callable[..., Any] | None = None,
) -> WorkerJobLink:
    post = post or httpx.post
    base_url = worker.base_url if worker and worker.base_url else worker_cfg.base_url
    headers = {}
    token = os.environ.get(worker.token_env, "") if worker and worker.token_env else ""
    if not token and (worker is None or worker.worker_id == "local-worker"):
        token = worker_cfg.token.get_secret_value()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    args = {
        "agent": envelope.engine,
        "prompt": envelope.prompt,
        "name": envelope.branch_name.rsplit("/", 1)[-1] or envelope.run_id,
        "execution_envelope": envelope.to_dict(),
    }
    if envelope.repo:
        args["repo"] = envelope.repo
    response = post(
        f"{base_url}/run",
        json={"action": "code", "args": args},
        headers=headers,
        timeout=worker_cfg.request_timeout_s,
    )
    response.raise_for_status()
    body = response.json()
    if not body.get("ok"):
        raise RuntimeError(body.get("error") or "worker rejected job")
    link = WorkerJobLink(
        worker_id=envelope.worker_id,
        job_id=body["job_id"],
        status=body.get("status", "running"),
        engine=envelope.engine,
        branch=body.get("branch") or "",
    )
    if store is not None:
        store.link_job(envelope.run_id, link)
    return link


def create_run_and_envelope(
    *,
    store: OrchestrationStore,
    command: WorkCommand,
    items: list[WorkItem],
    worker: WorkerProfile,
    landing_mode: str = "draft_pr",
) -> ExecutionEnvelope:
    objective = items[0].title if items else command.filters.get("text", command.operation)
    run = store.create_run(str(objective), work_items=items)
    store.set_phase(run.run_id, "claimed", "Work item claimed locally by Jarvis")
    envelope = build_execution_envelope(
        run_id=run.run_id,
        command=command,
        items=items,
        worker_id=worker.worker_id,
        landing_mode=landing_mode,
        engine=worker.agent,
    )
    store.append_event(run.run_id, "execution_envelope_created", "Execution envelope created", envelope.to_dict())
    return envelope
