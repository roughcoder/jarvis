from __future__ import annotations

import os
from dataclasses import replace
from collections.abc import Callable
from typing import Any

import httpx

from jarvis.capabilities import WORKER_SESSION_STOP
from jarvis.config import WorkerConfig
from jarvis.orchestration.envelope import build_execution_envelope
from jarvis.orchestration.models import ExecutionEnvelope, WorkCommand, WorkItem, WorkerJobLink, WorkerSessionLink
from jarvis.orchestration.store import OrchestrationStore
from jarvis.orchestration.workers import WorkerProfile
from jarvis.worker_session_contract import SESSION_RUNNING, SESSION_STOPPED


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
    get: Callable[..., Any] | None = None,
    created_session_ids: set[str] | None = None,
) -> WorkerSessionLink:
    post = post or httpx.post
    get = get or httpx.get
    base_url, headers = _worker_endpoint(worker_cfg, worker)
    created_by_dispatch = False
    if envelope.resume_session and envelope.session_id:
        session = {
            "session_id": envelope.session_id,
            "status": SESSION_RUNNING,
            "provider": envelope.engine,
            "engine": envelope.engine,
            "branch": envelope.branch_name,
            "cwd": envelope.cwd,
        }
    else:
        session_id = _worker_session_id(envelope)
        existing = _linked_session(store, envelope.run_id, envelope.worker_id, session_id)
        if existing is not None:
            session = existing
        else:
            create_response = post(
                f"{base_url}/sessions",
                json={
                    "session_id": session_id,
                    "run_id": envelope.run_id,
                    "provider": envelope.engine,
                    "engine": envelope.engine,
                    "repo": envelope.repo,
                    "branch": envelope.branch_name,
                    "cwd": envelope.cwd,
                    "title": envelope.session_name or _job_name(envelope),
                    "metadata": {
                        "execution_envelope": envelope.to_dict(),
                        "provision_workspace": bool(envelope.repo and not envelope.cwd and not envelope.resume_session),
                        "allowed_actions": envelope.allowed_actions,
                        "landing": envelope.landing.to_dict(),
                        "verification": envelope.verification.to_dict(),
                    },
                },
                headers=headers,
                timeout=worker_cfg.request_timeout_s,
            )
            create_body = _json_body(create_response)
            if _duplicate_session_response(create_response, create_body):
                session = _fetch_existing_session(
                    session_id,
                    base_url=base_url,
                    headers=headers,
                    timeout=worker_cfg.request_timeout_s,
                    get=get,
                )
                create_body = {"event": None}
            else:
                _raise_worker_error(create_response, create_body)
                if not create_body.get("ok"):
                    raise RuntimeError(create_body.get("error") or "worker rejected session")
                session = create_body["session"]
                created_by_dispatch = True
                if created_session_ids is not None:
                    created_session_ids.add(str(session.get("session_id") or session_id))
            if store is not None:
                store.link_session(envelope.run_id, _session_link_from_body(envelope, session, create_body.get("event")))
    turn_id = _turn_id(envelope)
    idempotency_key = _turn_idempotency_key(envelope)
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
            "idempotency_key": idempotency_key,
        },
        headers=headers,
        timeout=worker_cfg.request_timeout_s,
    )
    try:
        turn_body = _json_body(turn_response)
        _raise_worker_error(turn_response, turn_body)
        if not turn_body.get("ok"):
            raise RuntimeError(turn_body.get("error") or "worker rejected session turn")
    except Exception:
        if created_by_dispatch:
            _stop_created_session_after_turn_rejection(
                session,
                envelope=envelope,
                worker_cfg=worker_cfg,
                worker=worker,
                post=post,
                store=store,
            )
        raise
    current = turn_body.get("session") or session
    events = turn_body.get("events") or []
    link = WorkerSessionLink(
        worker_id=envelope.worker_id,
        session_id=current["session_id"],
        status=current.get("status", SESSION_RUNNING),
        provider=current.get("provider") or envelope.engine,
        engine=current.get("engine") or envelope.engine,
        branch=current.get("branch") or envelope.branch_name,
        cwd=current.get("cwd") or envelope.cwd,
        last_event_id=str(events[-1].get("event_id") or "") if events else "",
        allowed_actions=list(envelope.allowed_actions),
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
    get: Callable[..., Any] | None = None,
) -> list[WorkerSessionLink]:
    links: list[WorkerSessionLink] = []
    created_session_ids: set[str] = set()
    try:
        for engine in engines:
            engine_envelope = replace(
                envelope,
                engine=engine,
                engine_strategy="ensemble",
                dispatch_id=f"{_dispatch_id(envelope)}-{engine}",
                allowed_actions=sorted({*envelope.allowed_actions, WORKER_SESSION_STOP}),
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
                get=get,
                created_session_ids=created_session_ids,
            )
            links.append(link)
    except Exception:
        _stop_started_sessions(
            links,
            envelope=envelope,
            worker_cfg=worker_cfg,
            worker=worker,
            post=post,
            store=store,
            created_session_ids=created_session_ids,
        )
        raise
    if store is not None:
        store.append_event(
            envelope.run_id,
            "ensemble_sessions_started",
            f"Started {len(links)} worker session(s) for ensemble.",
            {"session_ids": [x.session_id for x in links], "engines": engines},
        )
    return links


def _stop_started_sessions(
    links: list[WorkerSessionLink],
    *,
    envelope: ExecutionEnvelope,
    worker_cfg: WorkerConfig,
    worker: WorkerProfile | None,
    post: Callable[..., Any] | None,
    store: OrchestrationStore | None,
    created_session_ids: set[str] | None = None,
) -> None:
    if not links:
        return
    http_post = post or httpx.post
    base_url, headers = _worker_endpoint(worker_cfg, worker)
    control_envelope = envelope.to_dict()
    control_envelope["allowed_actions"] = sorted({*control_envelope.get("allowed_actions", []), WORKER_SESSION_STOP})
    for link in reversed(links):
        if created_session_ids is not None and link.session_id not in created_session_ids:
            continue
        try:
            response = http_post(
                f"{base_url}/sessions/{link.session_id}/stop",
                json={"metadata": {"execution_envelope": control_envelope}},
                headers=headers,
                timeout=worker_cfg.request_timeout_s,
            )
            body = _json_body(response)
            _raise_worker_error(response, body)
            if not body.get("ok"):
                raise RuntimeError(body.get("error") or "worker rejected rollback stop")
        except Exception:  # noqa: BLE001 - preserve original ensemble dispatch failure
            if store is not None:
                store.append_event(
                    envelope.run_id,
                    "session_rollback_stop_failed",
                    f"Could not stop worker session {link.session_id} after ensemble dispatch failure",
                    {"session_id": link.session_id},
                )
            pass
        else:
            if store is not None:
                try:
                    store.update_session(envelope.run_id, link.session_id, worker_id=link.worker_id, status=SESSION_STOPPED)
                except Exception:  # noqa: BLE001 - best-effort rollback marker
                    pass


def _stop_created_session_after_turn_rejection(
    session: dict[str, Any],
    *,
    envelope: ExecutionEnvelope,
    worker_cfg: WorkerConfig,
    worker: WorkerProfile | None,
    post: Callable[..., Any] | None,
    store: OrchestrationStore | None,
) -> None:
    session_id = str(session.get("session_id") or "").strip()
    if not session_id:
        return
    control_envelope = envelope.to_dict()
    control_envelope["allowed_actions"] = sorted({*control_envelope.get("allowed_actions", []), WORKER_SESSION_STOP})
    http_post = post or httpx.post
    base_url, headers = _worker_endpoint(worker_cfg, worker)
    try:
        response = http_post(
            f"{base_url}/sessions/{session_id}/stop",
            json={"metadata": {"execution_envelope": control_envelope}},
            headers=headers,
            timeout=worker_cfg.request_timeout_s,
        )
        body = _json_body(response)
        _raise_worker_error(response, body)
        if not body.get("ok"):
            raise RuntimeError(body.get("error") or "worker rejected turn-failure cleanup stop")
    except Exception as exc:  # noqa: BLE001 - preserve the original turn rejection
        if store is not None:
            store.append_event(
                envelope.run_id,
                "session_turn_rejection_stop_failed",
                f"Could not stop worker session {session_id} after turn rejection",
                {"session_id": session_id, "error": str(exc)},
            )
        return
    if store is not None:
        try:
            store.update_session(envelope.run_id, session_id, worker_id=envelope.worker_id, status=SESSION_STOPPED)
        except Exception:  # noqa: BLE001 - cleanup already succeeded at worker boundary
            pass


def _linked_session(store: OrchestrationStore | None, run_id: str, worker_id: str, session_id: str) -> dict[str, Any] | None:
    if store is None:
        return None
    run = store.get(run_id)
    if run is None:
        return None
    link = next((session for session in run.sessions if session.worker_id == worker_id and session.session_id == session_id), None)
    if link is None:
        return None
    return {
        "session_id": link.session_id,
        "status": link.status,
        "provider": link.provider,
        "engine": link.engine,
        "branch": link.branch,
        "cwd": link.cwd,
    }


def _duplicate_session_response(response: Any, body: dict[str, Any]) -> bool:
    status_code = getattr(response, "status_code", 200)
    error = str(body.get("error") or "").lower()
    return status_code in {400, 409} and "already exists" in error


def _fetch_existing_session(
    session_id: str,
    *,
    base_url: str,
    headers: dict[str, str],
    timeout: float,
    get: Callable[..., Any],
) -> dict[str, Any]:
    response = get(f"{base_url}/sessions/{session_id}", headers=headers, timeout=timeout)
    body = _json_body(response)
    _raise_worker_error(response, body)
    if not body.get("session_id"):
        raise RuntimeError(f"worker duplicate session {session_id!r} could not be fetched")
    return body


def _session_link_from_body(
    envelope: ExecutionEnvelope,
    session: dict[str, Any],
    event: dict[str, Any] | None = None,
) -> WorkerSessionLink:
    return WorkerSessionLink(
        worker_id=envelope.worker_id,
        session_id=session["session_id"],
        status=session.get("status", SESSION_RUNNING),
        provider=session.get("provider") or envelope.engine,
        engine=session.get("engine") or envelope.engine,
        branch=session.get("branch") or envelope.branch_name,
        cwd=session.get("cwd") or envelope.cwd,
        last_event_id=str((event or {}).get("event_id") or ""),
        allowed_actions=list(envelope.allowed_actions),
    )


def _job_name(envelope: ExecutionEnvelope) -> str:
    name = envelope.branch_name.rsplit("/", 1)[-1] if envelope.branch_name else envelope.run_id
    if name.startswith("jarvis-"):
        return name
    return f"jarvis-{name}"


def _dispatch_id(envelope: ExecutionEnvelope) -> str:
    return envelope.dispatch_id or f"dispatch_{envelope.run_id}_{envelope.engine}"


def _turn_id(envelope: ExecutionEnvelope) -> str:
    dispatch_id = _dispatch_id(envelope)
    clean = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in dispatch_id)
    return f"turn_{clean}"


def _turn_idempotency_key(envelope: ExecutionEnvelope) -> str:
    return f"{envelope.run_id}:{_dispatch_id(envelope)}:turn"


def _worker_session_id(envelope: ExecutionEnvelope) -> str:
    if envelope.session_id:
        return envelope.session_id
    clean = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in _dispatch_id(envelope))
    return f"sess_{clean}"


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
    extra_allowed_actions: list[str] | None = None,
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
    if extra_allowed_actions:
        envelope.allowed_actions = [*envelope.allowed_actions, *[x for x in extra_allowed_actions if x not in envelope.allowed_actions]]
    store.append_event(run.run_id, "execution_envelope_created", "Execution envelope created", envelope.to_dict())
    return envelope
