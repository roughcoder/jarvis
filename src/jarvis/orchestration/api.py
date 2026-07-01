from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

import httpx
from aiohttp import web

from jarvis.capabilities import (
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_RESTORE,
    WORKER_SESSION_STOP,
    WORKER_SESSION_TURN,
)
from jarvis.config import Config, insecure_bind
from jarvis.orchestration.cockpit import (
    API_VERSION,
    SCHEMA_VERSION,
    CockpitError,
    IdempotencyStore,
    aggregate_checkpoints,
    aggregate_requests,
    aggregate_sessions,
    artifact_summaries,
    cockpit_catalog,
    cockpit_snapshot,
    make_session_ref,
    paged,
    parse_session_ref,
    project_request,
    project_session_event,
    run_report_artifact,
    run_summary,
    session_summary,
    snapshot_cursor,
    sync_state,
    worker_headers,
    worker_profiles,
)
from jarvis.brain.capabilities import resolve_capabilities
from jarvis.orchestration.authority import allowed
from jarvis.orchestration.intent import parse_work_command
from jarvis.orchestration.models import WorkCommand, WorkItem
from jarvis.orchestration.service import (
    MissingAuthorityError,
    MissingWorkRepoError,
    NoEligibleWorkerError,
    OrchestrationService,
    ResumeRunError,
    StartedWork,
    WorkAlreadyOwnedError,
    WorkerDispatchError,
)
from jarvis.orchestration.sources import GitHubWorkSource, LinearWorkSource, WorkSource
from jarvis.orchestration.store import OrchestrationStore
from jarvis.orchestration.workers import WorkerRegistry

HttpGet = Callable[..., Any]
HttpPost = Callable[..., Any]


def make_app(
    cfg: Config,
    *,
    http_get: HttpGet | None = None,
    http_post: HttpPost | None = None,
    source_factory: Callable[[str, Any], WorkSource] | None = None,
) -> web.Application:
    get = http_get or httpx.get
    post = http_post or httpx.post
    store = OrchestrationStore(cfg.orchestration.workspace)
    idempotency = IdempotencyStore(cfg.orchestration.workspace)
    source_factory = source_factory or _work_source
    app = web.Application(middlewares=[_error_middleware])

    def authorised(request: web.Request) -> bool:
        token = cfg.orchestration.api_token.get_secret_value()
        if not token:
            return True
        return request.headers.get("Authorization", "") == f"Bearer {token}"

    def require_auth(request: web.Request) -> None:
        if not authorised(request):
            raise CockpitError("unauthorized", "unauthorized", status=401)

    async def health(request: web.Request) -> web.Response:
        require_auth(request)
        return web.json_response({"ok": True, "api_version": API_VERSION, "schema_version": SCHEMA_VERSION})

    async def catalog(request: web.Request) -> web.Response:
        require_auth(request)
        return web.json_response(cockpit_catalog())

    async def snapshot(request: web.Request) -> web.Response:
        require_auth(request)
        mode = str(request.query.get("sync") or "none")
        return web.json_response(
            cockpit_snapshot(
                store=store,
                worker_cfg=cfg.worker,
                workers_path=cfg.orchestration.workers_path,
                sync_mode=mode,
                http_get=get,
            )
        )

    async def events(request: web.Request) -> web.StreamResponse:
        require_auth(request)
        mode = str(request.query.get("sync") or "none")
        client_cursor = str(request.query.get("after") or request.headers.get("Last-Event-ID") or "")
        body = cockpit_snapshot(
            store=store,
            worker_cfg=cfg.worker,
            workers_path=cfg.orchestration.workers_path,
            sync_mode=mode,
            http_get=get,
        )
        cursor = body["cursor"]
        response = web.StreamResponse(status=200, headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
        await response.prepare(request)
        if client_cursor != cursor:
            await _write_sse(response, "snapshot", cursor, {"cursor": cursor, "type": "snapshot", "payload": body})
        try:
            while True:
                await asyncio.sleep(1)
                next_body = cockpit_snapshot(
                    store=store,
                    worker_cfg=cfg.worker,
                    workers_path=cfg.orchestration.workers_path,
                    sync_mode="none",
                    http_get=get,
                )
                next_cursor = next_body["cursor"]
                if next_cursor != cursor:
                    cursor = next_cursor
                    await _write_sse(response, "snapshot", cursor, {"cursor": cursor, "type": "snapshot", "payload": next_body})
                else:
                    await response.write(b": heartbeat\n\n")
        except (asyncio.CancelledError, ConnectionResetError, RuntimeError):
            return response

    async def workers(request: web.Request) -> web.Response:
        require_auth(request)
        probe = str(request.query.get("sync") or request.query.get("probe") or "none") == "probe" or request.query.get("probe") == "true"
        return web.json_response(
            {
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "workers": worker_profiles(worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, probe=probe, http_get=get),
            }
        )

    async def worker_detail(request: web.Request) -> web.Response:
        require_auth(request)
        worker_id = request.match_info["worker_id"]
        profiles = worker_profiles(worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, probe=True, http_get=get)
        worker = next((item for item in profiles if item.get("worker_id") == worker_id), None)
        if worker is None:
            raise CockpitError("not_found", "worker not found", status=404)
        return web.json_response(worker)

    async def runs(request: web.Request) -> web.Response:
        require_auth(request)
        if str(request.query.get("sync") or "none") in {"fast", "probe"}:
            sync_state(store=store, worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, sync_mode=str(request.query["sync"]), http_get=get)
        run_items = store.list_runs()
        requests = aggregate_requests(worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, http_get=get)
        artifacts = artifact_summaries(run_items)
        return web.json_response({"runs": [run_summary(run, requests=requests, artifacts=artifacts) for run in run_items]})

    async def run_detail(request: web.Request) -> web.Response:
        require_auth(request)
        run = _run_or_404(store, request.match_info["run_id"])
        requests = aggregate_requests(worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, http_get=get)
        artifacts = artifact_summaries([run])
        return web.json_response({"run": run.to_dict(), "summary": run_summary(run, requests=requests, artifacts=artifacts)})

    async def run_events(request: web.Request) -> web.Response:
        require_auth(request)
        run = _run_or_404(store, request.match_info["run_id"])
        events = [_project_run_event(event, idx + 1) for idx, event in enumerate(store.events(run.run_id))]
        return web.json_response(paged(events, after=str(request.query.get("after") or ""), limit=_limit(request)))

    async def run_artifacts(request: web.Request) -> web.Response:
        require_auth(request)
        run = _run_or_404(store, request.match_info["run_id"])
        items = [*artifact_summaries([run]), run_report_artifact(store, run.run_id)]
        return web.json_response(paged(items, after=str(request.query.get("after") or ""), limit=_limit(request)))

    async def sessions(request: web.Request) -> web.Response:
        require_auth(request)
        runs_list = store.list_runs()
        session_rows = aggregate_sessions(runs=runs_list, worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, http_get=get)
        requests = aggregate_requests(worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, http_get=get)
        checkpoints = aggregate_checkpoints(runs=runs_list, worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, http_get=get)
        return web.json_response({"sessions": [session_summary(row, requests=requests, checkpoints=checkpoints) for row in session_rows.values()]})

    async def session_detail(request: web.Request) -> web.Response:
        require_auth(request)
        ref = parse_session_ref(request.match_info["session_ref"])
        raw = _worker_get_json(cfg, ref.worker_id, f"/sessions/{ref.session_id}", get=get)
        row = _worker_session_row(raw, ref.worker_id)
        requests = [_request_with_run(request_item, row.get("run_id", "")) for request_item in _worker_session_requests(cfg, ref, get=get)]
        checkpoints = _worker_session_checkpoints(cfg, ref, run_id=str(row.get("run_id") or ""), get=get)
        return web.json_response({"session": session_summary(row, requests=requests, checkpoints=checkpoints), "raw": _public_session_detail(raw)})

    async def session_events(request: web.Request) -> web.Response:
        require_auth(request)
        ref = parse_session_ref(request.match_info["session_ref"])
        run_id = _session_run_id(cfg, ref, get=get)
        raw = _worker_get_json(
            cfg,
            ref.worker_id,
            f"/sessions/{ref.session_id}/events",
            params={"after": str(request.query.get("after") or ""), "limit": _limit(request)},
            get=get,
        )
        events = [
            project_session_event(event, worker_id=ref.worker_id, run_id=run_id, sequence=idx + 1)
            for idx, event in enumerate(raw.get("events", []))
            if isinstance(event, dict)
        ]
        return web.json_response(paged(events, limit=_limit(request)))

    async def session_requests(request: web.Request) -> web.Response:
        require_auth(request)
        ref = parse_session_ref(request.match_info["session_ref"])
        return web.json_response({"requests": _worker_session_requests(cfg, ref, get=get)})

    async def session_checkpoints(request: web.Request) -> web.Response:
        require_auth(request)
        ref = parse_session_ref(request.match_info["session_ref"])
        return web.json_response({"checkpoints": _worker_session_checkpoints(cfg, ref, run_id=_session_run_id(cfg, ref, get=get), get=get)})

    async def work_start(request: web.Request) -> web.Response:
        require_auth(request)
        body = await _json_body(request)
        cached = idempotency.get("work/start", str(body.get("idempotency_key") or ""), body)
        if cached is not None:
            return web.json_response(cached)
        command, manual_item = _command_from_body(body, start=True)
        service = _service(cfg, source_factory, manual_item=manual_item)
        try:
            result = service.next_work(command, start=True)
        except (MissingAuthorityError, NoEligibleWorkerError, WorkAlreadyOwnedError, MissingWorkRepoError, WorkerDispatchError) as exc:
            raise _service_error(exc) from exc
        if result is None or not isinstance(result, StartedWork):
            raise CockpitError("not_found", "no eligible work item found", recoverable=True, status=404)
        response_body = _started_work_packet(store, result)
        idempotency.save("work/start", str(body.get("idempotency_key") or ""), body, response_body)
        return web.json_response(response_body)

    async def work_resume(request: web.Request) -> web.Response:
        require_auth(request)
        body = await _json_body(request)
        cached = idempotency.get("work/resume", str(body.get("idempotency_key") or ""), body)
        if cached is not None:
            return web.json_response(cached)
        service = _service(cfg, source_factory)
        try:
            result = service.resume_run(str(body.get("run_id") or "latest"), prompt=str(body.get("prompt") or ""))
        except (MissingAuthorityError, NoEligibleWorkerError, ResumeRunError, WorkerDispatchError) as exc:
            raise _service_error(exc) from exc
        response_body = _started_work_packet(store, result)
        idempotency.save("work/resume", str(body.get("idempotency_key") or ""), body, response_body)
        return web.json_response(response_body)

    async def session_write(request: web.Request) -> web.Response:
        require_auth(request)
        ref = parse_session_ref(request.match_info["session_ref"])
        action = request.match_info.get("action", "restore_checkpoint")
        body = await _json_body(request)
        scope = f"sessions/{ref.worker_id}/{ref.session_id}/{action}"
        cached = idempotency.get(scope, str(body.get("idempotency_key") or ""), body)
        if cached is not None:
            return web.json_response(cached)
        required = _required_session_action(action)
        _require_capability(cfg, required)
        proxied = dict(body)
        proxied["allowed_actions"] = sorted(set(proxied.get("allowed_actions") or []) | {required})
        path = f"/sessions/{ref.session_id}/{'checkpoints/restore' if action == 'restore_checkpoint' else action}"
        raw = _worker_post_json(cfg, ref.worker_id, path, proxied, post=post)
        response_body = _session_write_packet(cfg, ref, raw, get=get)
        idempotency.save(scope, str(body.get("idempotency_key") or ""), body, response_body)
        return web.json_response(response_body)

    app.add_routes([
        web.get("/v1/health", health),
        web.get("/v1/cockpit/catalog", catalog),
        web.get("/v1/cockpit/snapshot", snapshot),
        web.get("/v1/cockpit/events", events),
        web.get("/v1/workers", workers),
        web.get("/v1/workers/{worker_id}", worker_detail),
        web.get("/v1/runs", runs),
        web.get("/v1/runs/{run_id}/events", run_events),
        web.get("/v1/runs/{run_id}/artifacts", run_artifacts),
        web.get("/v1/runs/{run_id}", run_detail),
        web.get("/v1/sessions", sessions),
        web.get("/v1/sessions/{session_ref}/events", session_events),
        web.get("/v1/sessions/{session_ref}/requests", session_requests),
        web.get("/v1/sessions/{session_ref}/checkpoints", session_checkpoints),
        web.post("/v1/sessions/{session_ref}/checkpoints/restore", session_write, name="restore_checkpoint"),
        web.post("/v1/sessions/{session_ref}/{action:turns|input|approval|interrupt|stop}", session_write),
        web.get("/v1/sessions/{session_ref}", session_detail),
        web.post("/v1/work/start", work_start),
        web.post("/v1/work/resume", work_resume),
    ])
    return app


@web.middleware
async def _error_middleware(request: web.Request, handler):  # noqa: ANN001
    try:
        return await handler(request)
    except CockpitError as exc:
        return web.json_response(exc.body(), status=exc.status)
    except web.HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        return web.json_response(
            {"ok": False, "error": {"code": "internal_error", "message": str(exc), "recoverable": False}},
            status=500,
        )


async def _write_sse(response: web.StreamResponse, event: str, cursor: str, data: dict[str, Any]) -> None:
    payload = json.dumps(data, sort_keys=True)
    await response.write(f"id: {cursor}\nevent: {event}\ndata: {payload}\n\n".encode("utf-8"))


def _work_source(name: str, cfg: Config | None = None) -> WorkSource:
    if name == "linear":
        api_key = cfg.linear.api_key.get_secret_value() if cfg is not None else None
        return LinearWorkSource(api_key)
    if name == "manual":
        return _ManualWorkSource(None)
    return GitHubWorkSource()


class _ManualWorkSource:
    def __init__(self, item: WorkItem | None) -> None:
        self.item = item

    def list(self, *, repo: str = "", filters: dict | None = None, limit: int = 10) -> list[WorkItem]:
        return [self.item] if self.item is not None else []

    def next(self, *, repo: str = "", filters: dict | None = None) -> WorkItem | None:
        return self.item

    def claim(self, item: WorkItem) -> bool:
        return False

    def link_pr(self, item: WorkItem, artifact) -> bool:  # noqa: ANN001
        return False

    def comment(self, item: WorkItem, body: str) -> bool:
        return False


def _service(cfg: Config, source_factory: Callable[[str, Any], WorkSource], *, manual_item: WorkItem | None = None) -> OrchestrationService:
    def factory(name: str, inner_cfg: Any = None) -> WorkSource:
        if name == "manual":
            return _ManualWorkSource(manual_item)
        return source_factory(name, inner_cfg)

    return OrchestrationService(cfg=cfg, capabilities=resolve_capabilities(cfg.capabilities), source_factory=factory)


def _command_from_body(body: dict[str, Any], *, start: bool) -> tuple[WorkCommand, WorkItem | None]:
    if isinstance(body.get("command"), dict):
        command = WorkCommand.from_dict(dict(body["command"]))
    else:
        command = parse_work_command(str(body.get("phrase") or "next work"))
    if body.get("source"):
        command.source = str(body["source"])
    if body.get("repo"):
        command.filters["repo"] = str(body["repo"])
    if body.get("worker_id"):
        command.target_worker_id = str(body["worker_id"])
    if body.get("engine"):
        command.target_engine_id = str(body["engine"])
    if body.get("engine_strategy"):
        strategy = str(body["engine_strategy"])
        command.engine_strategy = "ensemble" if strategy == "parallel" else strategy
    command.start = start
    manual_item = None
    if command.source == "manual" or isinstance(body.get("work_item"), dict):
        raw = dict(body.get("work_item") or {})
        manual_item = WorkItem(
            source="manual",
            id=str(raw.get("id") or body.get("idempotency_key") or "manual"),
            title=str(raw.get("title") or body.get("title") or body.get("phrase") or "Manual cockpit work"),
            body=str(raw.get("body") or body.get("prompt") or ""),
            repo=str(raw.get("repo") or body.get("repo") or ""),
            kind=str(raw.get("kind") or "manual"),
        )
        command.source = "manual"
        command.operation = "start_next_work"
    return command, manual_item


def _started_work_packet(store: OrchestrationStore, result: StartedWork) -> dict[str, Any]:
    run = store.get(result.envelope.run_id)
    runs = [run] if run is not None else []
    sessions = []
    for session in result.sessions or [result.session]:
        row = {
            "session_ref": make_session_ref(session.worker_id, session.session_id),
            "worker_id": session.worker_id,
            "session_id": session.session_id,
            "run_id": result.envelope.run_id,
            "title": result.item.title,
            "provider": session.provider,
            "engine": session.engine,
            "status": session.status,
            "repo": result.item.repo,
            "branch": session.branch,
            "cwd": session.cwd,
            "latest_event_cursor": session.last_event_id,
            "created_at": run.created_at if run else "",
            "updated_at": run.updated_at if run else "",
        }
        sessions.append(session_summary(row))
    artifacts = artifact_summaries(runs)
    return {
        "ok": True,
        "cursor": snapshot_cursor(runs, {item["session_ref"]: item for item in sessions}, artifacts),
        "run": run_summary(run, artifacts=artifacts) if run else {},
        "session": sessions[0] if sessions else {},
        "events": [],
        "requests": [],
        "artifacts": artifacts,
    }


def _session_write_packet(cfg: Config, ref, raw: dict[str, Any], *, get: HttpGet) -> dict[str, Any]:  # noqa: ANN001
    session_raw = _worker_get_json(cfg, ref.worker_id, f"/sessions/{ref.session_id}", get=get)
    row = _worker_session_row(session_raw, ref.worker_id)
    requests = _worker_session_requests(cfg, ref, get=get)
    checkpoints = _worker_session_checkpoints(cfg, ref, run_id=str(row.get("run_id") or ""), get=get)
    events = []
    if isinstance(raw.get("events"), list):
        events = [project_session_event(event, worker_id=ref.worker_id, run_id=str(row.get("run_id") or ""), sequence=idx + 1) for idx, event in enumerate(raw["events"])]
    elif isinstance(raw.get("event"), dict):
        events = [project_session_event(raw["event"], worker_id=ref.worker_id, run_id=str(row.get("run_id") or ""), sequence=1)]
    artifacts = []
    return {
        "ok": bool(raw.get("ok", True)),
        "cursor": snapshot_cursor([], {row["session_ref"]: row}, artifacts),
        "run": {},
        "session": session_summary(row, requests=requests, checkpoints=checkpoints),
        "events": events,
        "requests": requests,
        "artifacts": artifacts,
    }


def _service_error(exc: Exception) -> CockpitError:
    if isinstance(exc, MissingAuthorityError):
        return CockpitError("forbidden", f"missing authority: {', '.join(exc.actions)}", status=403)
    if isinstance(exc, NoEligibleWorkerError):
        return CockpitError("worker_unavailable", str(exc) or "no eligible worker found", recoverable=True, status=409)
    if isinstance(exc, WorkAlreadyOwnedError):
        return CockpitError("validation_failed", str(exc), recoverable=True, status=409)
    if isinstance(exc, MissingWorkRepoError):
        return CockpitError("validation_failed", str(exc), recoverable=True, status=400)
    if isinstance(exc, ResumeRunError):
        message = str(exc)
        code = "session_active" if "active worker session" in message else "validation_failed"
        return CockpitError(code, message, recoverable=True, status=409)
    if isinstance(exc, WorkerDispatchError):
        return CockpitError("provider_unavailable", str(exc), recoverable=True, status=502)
    return CockpitError("internal_error", str(exc), status=500)


async def _json_body(request: web.Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception as exc:
        raise CockpitError("validation_failed", "request body must be JSON", status=400) from exc
    if not isinstance(body, dict):
        raise CockpitError("validation_failed", "request body must be an object", status=400)
    return body


def _run_or_404(store: OrchestrationStore, run_id: str):
    run = store.get(run_id)
    if run is None:
        raise CockpitError("not_found", "run not found", status=404)
    return run


def _project_run_event(event, sequence: int) -> dict[str, Any]:  # noqa: ANN001
    return {
        "event_id": f"{event.run_id}:{sequence}",
        "sequence": sequence,
        "run_id": event.run_id,
        "type": event.type,
        "occurred_at": event.time,
        "message": event.message,
        "data": event.data,
        "cursor": f"{event.run_id}:{sequence}",
    }


def _worker_profile(cfg: Config, worker_id: str):
    registry = WorkerRegistry(cfg.worker, profiles_path=cfg.orchestration.workers_path)
    profile = registry.get(worker_id, probe=False)
    if profile is None:
        raise CockpitError("not_found", "worker not found", status=404)
    return profile


def _worker_get_json(
    cfg: Config,
    worker_id: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    get: HttpGet,
) -> dict[str, Any]:
    profile = _worker_profile(cfg, worker_id)
    response = get(f"{profile.base_url}{path}", headers=worker_headers(cfg.worker, profile), params=params or {}, timeout=cfg.worker.request_timeout_s)
    status = getattr(response, "status_code", 200)
    if status == 404:
        raise CockpitError("not_found", "worker resource not found", status=404)
    if status == 401:
        raise CockpitError("unauthorized", "worker unauthorized", status=401)
    if status >= 400:
        raise CockpitError("worker_unavailable", _response_error(response) or "worker request failed", recoverable=True, status=502)
    data = response.json()
    if not isinstance(data, dict):
        raise CockpitError("internal_error", "worker returned invalid JSON", status=502)
    return data


def _worker_post_json(cfg: Config, worker_id: str, path: str, body: dict[str, Any], *, post: HttpPost) -> dict[str, Any]:
    profile = _worker_profile(cfg, worker_id)
    response = post(f"{profile.base_url}{path}", headers=worker_headers(cfg.worker, profile), json=body, timeout=cfg.worker.request_timeout_s)
    status = getattr(response, "status_code", 200)
    data = response.json() if hasattr(response, "json") else {}
    if status >= 400 or (isinstance(data, dict) and data.get("ok") is False):
        message = str(data.get("error") or "worker write failed") if isinstance(data, dict) else "worker write failed"
        if status == 401:
            raise CockpitError("unauthorized", message, status=401)
        if status == 403:
            raise CockpitError("forbidden", message, status=403)
        if status == 404:
            raise CockpitError("not_found", message, status=404)
        raise CockpitError(_worker_error_code(message), message, recoverable=status in {400, 409}, status=409 if status == 409 else status)
    return data if isinstance(data, dict) else {}


def _worker_session_row(raw: dict[str, Any], worker_id: str) -> dict[str, Any]:
    session_id = str(raw.get("session_id") or "")
    return {
        "session_ref": make_session_ref(worker_id, session_id),
        "worker_id": worker_id,
        "session_id": session_id,
        "run_id": str(raw.get("run_id") or ""),
        "title": str(raw.get("title") or ""),
        "provider": str(raw.get("provider") or ""),
        "engine": str(raw.get("engine") or raw.get("provider") or ""),
        "status": str(raw.get("status") or ""),
        "repo": str(raw.get("repo") or ""),
        "branch": str(raw.get("branch") or ""),
        "cwd": str(raw.get("cwd") or ""),
        "latest_event_cursor": "",
        "created_at": str(raw.get("created_at") or ""),
        "updated_at": str(raw.get("updated_at") or ""),
    }


def _public_session_detail(raw: dict[str, Any]) -> dict[str, Any]:
    data = dict(raw)
    data.pop("cwd", None)
    data.pop("metadata", None)
    return data


def _worker_session_requests(cfg: Config, ref, *, get: HttpGet) -> list[dict[str, Any]]:  # noqa: ANN001
    raw = _worker_get_json(cfg, ref.worker_id, f"/sessions/{ref.session_id}/requests", get=get)
    return [project_request(item, ref.worker_id) for item in raw.get("requests", []) if isinstance(item, dict)]


def _worker_session_checkpoints(cfg: Config, ref, *, run_id: str, get: HttpGet) -> list[dict[str, Any]]:  # noqa: ANN001
    raw = _worker_get_json(cfg, ref.worker_id, f"/sessions/{ref.session_id}/checkpoints", get=get)
    result = []
    for item in raw.get("checkpoints", []):
        if isinstance(item, dict):
            next_item = dict(item)
            next_item["session_ref"] = make_session_ref(ref.worker_id, ref.session_id)
            next_item["run_id"] = run_id
            result.append(next_item)
    return result


def _request_with_run(item: dict[str, Any], run_id: str) -> dict[str, Any]:
    if not item.get("run_id"):
        item = dict(item)
        item["run_id"] = run_id
    return item


def _session_run_id(cfg: Config, ref, *, get: HttpGet) -> str:  # noqa: ANN001
    raw = _worker_get_json(cfg, ref.worker_id, f"/sessions/{ref.session_id}", get=get)
    return str(raw.get("run_id") or "")


def _required_session_action(action: str) -> str:
    return {
        "turns": WORKER_SESSION_TURN,
        "input": WORKER_SESSION_INPUT,
        "approval": WORKER_SESSION_APPROVE,
        "interrupt": WORKER_SESSION_INTERRUPT,
        "stop": WORKER_SESSION_STOP,
        "restore_checkpoint": WORKER_SESSION_RESTORE,
    }[action]


def _require_capability(cfg: Config, action: str) -> None:
    capabilities = resolve_capabilities(cfg.capabilities)
    if not allowed(action, capabilities, public_write_mode=cfg.orchestration.landing_mode):
        raise CockpitError("forbidden", f"missing authority: {action}", status=403)


def _limit(request: web.Request) -> int:
    try:
        return max(1, min(int(request.query.get("limit") or 100), 500))
    except ValueError:
        return 100


def _response_error(response: Any) -> str:
    try:
        data = response.json()
    except Exception:
        return str(getattr(response, "text", "") or "")
    if isinstance(data, dict):
        return str(data.get("error") or data.get("message") or "")
    return ""


def _worker_error_code(message: str) -> str:
    text = message.lower()
    if "active turn" in text:
        return "session_active"
    if "does not accept new turns" in text:
        return "session_terminal"
    if "checkpoint" in text and "no such" in text:
        return "checkpoint_not_found"
    if "not pending" in text:
        return "request_not_pending"
    return "provider_unavailable"


async def serve(cfg: Config) -> None:
    bind = cfg.orchestration.api_bind_host or cfg.orchestration.api_host
    token_set = bool(cfg.orchestration.api_token.get_secret_value())
    if insecure_bind(bind, token_set, cfg.orchestration.api_allow_insecure):
        print(
            f"\n✗ Refusing to start: cockpit API is bound to {bind!r} with no "
            "ORCHESTRATION_API_TOKEN.\n"
            "  Set ORCHESTRATION_API_TOKEN, or ORCHESTRATION_API_ALLOW_INSECURE=true to override.\n"
        )
        return
    app = make_app(cfg)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, bind, cfg.orchestration.api_port)
    await site.start()
    print(f"Jarvis cockpit API listening on http://{bind}:{cfg.orchestration.api_port}/v1")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    import contextlib
    import signal

    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        await runner.cleanup()
