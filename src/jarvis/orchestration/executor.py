from __future__ import annotations

import os
from dataclasses import replace
from collections.abc import Callable
from typing import Any

import httpx

from jarvis.config import WorkerConfig
from jarvis.ids import new_id
from jarvis.orchestration.envelope import build_execution_envelope
from jarvis.orchestration.models import ExecutionEnvelope, WorkCommand, WorkItem, WorkerJobLink, WorkerSessionLink
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
    if worker is None:
        base_url = worker_cfg.base_url
    elif worker.base_url:
        base_url = worker.base_url
    elif worker.worker_id == "local-worker":
        base_url = worker_cfg.base_url
    else:
        raise RuntimeError(f"worker {worker.worker_id} has no base_url; refusing to route to local worker")
    headers = {}
    token = os.environ.get(worker.token_env, "") if worker and worker.token_env else ""
    if not token and (worker is None or worker.worker_id == "local-worker"):
        token = worker_cfg.token.get_secret_value()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    args = {
        "agent": envelope.engine,
        "prompt": envelope.prompt,
        "name": _job_name(envelope),
        "session_name": envelope.session_name,
        "resume_session": envelope.resume_session,
        "execution_envelope": envelope.to_dict(),
    }
    if envelope.session_id:
        args["session_id"] = envelope.session_id
    if envelope.cwd:
        args["cwd"] = envelope.cwd
    if envelope.branch_name:
        args["branch"] = envelope.branch_name
    if envelope.repo:
        args["repo"] = envelope.repo
    response = post(
        f"{base_url}/run",
        json={"action": "code", "args": args},
        headers=headers,
        timeout=worker_cfg.request_timeout_s,
    )
    try:
        body = response.json()
    except ValueError:
        body = {}
    status_code = getattr(response, "status_code", 200)
    if status_code >= 400:
        error = body.get("error") if isinstance(body, dict) else ""
        if not error:
            error = getattr(response, "text", "") or f"worker request failed with HTTP {status_code}"
        raise RuntimeError(error)
    response.raise_for_status()
    if not body.get("ok"):
        raise RuntimeError(body.get("error") or "worker rejected job")
    link = WorkerJobLink(
        worker_id=envelope.worker_id,
        job_id=body["job_id"],
        status=body.get("status", "running"),
        engine=envelope.engine,
        session_id=body.get("session_id") or envelope.session_id,
        session_name=body.get("session_name") or envelope.session_name,
        branch=body.get("branch") or "",
        cwd=body.get("cwd") or envelope.cwd,
    )
    if store is not None:
        store.link_job(envelope.run_id, link)
    return link


def start_worker_session(
    envelope: ExecutionEnvelope,
    *,
    worker_cfg: WorkerConfig,
    worker: WorkerProfile | None = None,
    store: OrchestrationStore | None = None,
    post: Callable[..., Any] | None = None,
) -> WorkerSessionLink:
    post = post or httpx.post
    base_url, headers = _worker_endpoint(worker_cfg, worker)
    if envelope.resume_session and envelope.session_id:
        session = {
            "session_id": envelope.session_id,
            "status": "running",
            "provider": envelope.engine,
            "engine": envelope.engine,
            "branch": envelope.branch_name,
            "cwd": envelope.cwd,
        }
    else:
        create_response = post(
            f"{base_url}/sessions",
            json={
                "run_id": envelope.run_id,
                "provider": envelope.engine,
                "engine": envelope.engine,
                "repo": envelope.repo,
                "branch": envelope.branch_name,
                "cwd": envelope.cwd,
                "title": envelope.session_name or _job_name(envelope),
                "metadata": {
                    "execution_envelope": envelope.to_dict(),
                    "allowed_actions": envelope.allowed_actions,
                    "landing": envelope.landing.to_dict(),
                    "verification": envelope.verification.to_dict(),
                },
            },
            headers=headers,
            timeout=worker_cfg.request_timeout_s,
        )
        create_body = _json_body(create_response)
        _raise_worker_error(create_response, create_body)
        if not create_body.get("ok"):
            raise RuntimeError(create_body.get("error") or "worker rejected session")
        session = create_body["session"]
    turn_id = new_id("turn")
    turn_response = post(
        f"{base_url}/sessions/{session['session_id']}/turns",
        json={
            "turn_id": turn_id,
            "prompt": envelope.prompt,
            "metadata": {
                "session_name": envelope.session_name,
                "resume_session": envelope.resume_session,
                "execution_envelope": envelope.to_dict(),
            },
            "idempotency_key": f"{envelope.run_id}:{turn_id}",
        },
        headers=headers,
        timeout=worker_cfg.request_timeout_s,
    )
    turn_body = _json_body(turn_response)
    _raise_worker_error(turn_response, turn_body)
    if not turn_body.get("ok"):
        raise RuntimeError(turn_body.get("error") or "worker rejected session turn")
    current = turn_body.get("session") or session
    events = turn_body.get("events") or []
    link = WorkerSessionLink(
        worker_id=envelope.worker_id,
        session_id=current["session_id"],
        status=current.get("status", "running"),
        provider=current.get("provider") or envelope.engine,
        engine=current.get("engine") or envelope.engine,
        branch=current.get("branch") or envelope.branch_name,
        cwd=current.get("cwd") or envelope.cwd,
        last_event_id=str(events[-1].get("event_id") or "") if events else "",
    )
    if store is not None:
        store.link_session(envelope.run_id, link)
    return link


def start_worker_ensemble(
    envelope: ExecutionEnvelope,
    *,
    engines: list[str],
    worker_cfg: WorkerConfig,
    worker: WorkerProfile | None = None,
    store: OrchestrationStore | None = None,
    post: Callable[..., Any] | None = None,
) -> list[WorkerSessionLink]:
    links: list[WorkerSessionLink] = []
    for engine in engines:
        engine_envelope = replace(
            envelope,
            engine=engine,
            engine_strategy="ensemble",
            session_id="" if engine != envelope.engine else envelope.session_id,
            session_name=f"{envelope.session_name}-{engine}" if envelope.session_name else "",
            branch_name=f"{envelope.branch_name}-{engine}" if envelope.branch_name else "",
        )
        link = start_worker_session(
            engine_envelope,
            worker_cfg=worker_cfg,
            worker=worker,
            store=store,
            post=post,
        )
        links.append(link)
    if store is not None:
        store.append_event(
            envelope.run_id,
            "ensemble_sessions_started",
            f"Started {len(links)} worker session(s) for ensemble.",
            {"session_ids": [x.session_id for x in links], "engines": engines},
        )
    return links


def _job_name(envelope: ExecutionEnvelope) -> str:
    name = envelope.branch_name.rsplit("/", 1)[-1] if envelope.branch_name else envelope.run_id
    if name.startswith("jarvis-"):
        return name
    return f"jarvis-{name}"


def _worker_endpoint(worker_cfg: WorkerConfig, worker: WorkerProfile | None) -> tuple[str, dict[str, str]]:
    if worker is None:
        base_url = worker_cfg.base_url
    elif worker.base_url:
        base_url = worker.base_url
    elif worker.worker_id == "local-worker":
        base_url = worker_cfg.base_url
    else:
        raise RuntimeError(f"worker {worker.worker_id} has no base_url; refusing to route to local worker")
    token = os.environ.get(worker.token_env, "") if worker and worker.token_env else ""
    if not token and (worker is None or worker.worker_id == "local-worker"):
        token = worker_cfg.token.get_secret_value()
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return base_url, headers


def _json_body(response: Any) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _raise_worker_error(response: Any, body: dict[str, Any]) -> None:
    status_code = getattr(response, "status_code", 200)
    if status_code >= 400:
        error = body.get("error") if isinstance(body, dict) else ""
        if not error:
            error = getattr(response, "text", "") or f"worker request failed with HTTP {status_code}"
        raise RuntimeError(error)
    response.raise_for_status()


def create_run_and_envelope(
    *,
    store: OrchestrationStore,
    command: WorkCommand,
    items: list[WorkItem],
    worker: WorkerProfile,
    landing_mode: str = "draft_pr",
    engine: str = "",
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
        engine=engine or worker.default_engine or worker.agent,
        engine_strategy=command.engine_strategy,
    )
    store.append_event(run.run_id, "execution_envelope_created", "Execution envelope created", envelope.to_dict())
    return envelope
