from __future__ import annotations

import asyncio
import contextlib
import json
from dataclasses import dataclass
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
from jarvis.ids import utc_now
from jarvis.orchestration.cockpit import (
    API_VERSION,
    MAX_PAGE_LIMIT,
    SCHEMA_VERSION,
    CockpitError,
    IdempotencyStore,
    SessionRef,
    aggregate_checkpoints,
    aggregate_requests,
    aggregate_sessions,
    archived_session_refs_for_store,
    artifact_summaries,
    cockpit_catalog,
    cockpit_snapshot,
    configure_session_ref_secret,
    make_session_ref,
    paged,
    project_checkpoint,
    project_request,
    project_session_event,
    public_error_message,
    public_event_data,
    _session_from_link,
    run_detail_projection,
    run_report_artifact,
    run_summary,
    session_summary,
    snapshot_cursor,
    sync_state,
    valid_session_ref,
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
from jarvis.worker_session_contract import ACTIVE_SESSION_STATUSES, FAILED_SESSION_STATUSES, SUCCESS_SESSION_STATUSES

HttpGet = Callable[..., Any]
HttpPost = Callable[..., Any]


@dataclass(frozen=True)
class CockpitAppContext:
    cfg: Config
    get: HttpGet
    post: HttpPost
    store: OrchestrationStore
    idempotency: IdempotencyStore
    idempotency_locks: dict[str, asyncio.Lock]
    source_factory: Callable[[str, Any], WorkSource]

    def require_auth(self, request: web.Request) -> None:
        token = self.cfg.orchestration.api_token.get_secret_value()
        if token and request.headers.get("Authorization", "") != f"Bearer {token}":
            raise CockpitError("unauthorized", "unauthorized", status=401)

    def service(self, *, manual_item: WorkItem | None = None) -> OrchestrationService:
        return _service(self.cfg, self.source_factory, manual_item=manual_item)


def make_app(
    cfg: Config,
    *,
    http_get: HttpGet | None = None,
    http_post: HttpPost | None = None,
    source_factory: Callable[[str, Any], WorkSource] | None = None,
) -> web.Application:
    configure_session_ref_secret(cfg.orchestration.session_ref_secret.get_secret_value())
    ctx = CockpitAppContext(
        cfg=cfg,
        get=http_get or httpx.get,
        post=http_post or httpx.post,
        store=OrchestrationStore(cfg.orchestration.workspace),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        source_factory=source_factory or _work_source,
    )
    app = web.Application(middlewares=[_error_middleware])
    reads = CockpitReadHandlers(ctx)
    writes = CockpitWriteHandlers(ctx)
    sse = SseHandlers(ctx)
    app.add_routes([
        web.get("/v1/health", reads.health),
        web.get("/v1/cockpit/catalog", reads.catalog),
        web.get("/v1/cockpit/snapshot", reads.snapshot),
        web.get("/v1/cockpit/events", sse.events),
        web.get("/v1/workers", reads.workers),
        web.get("/v1/workers/{worker_id}", reads.worker_detail),
        web.get("/v1/runs", reads.runs),
        web.get("/v1/runs/{run_id}/events", reads.run_events),
        web.get("/v1/runs/{run_id}/artifacts", reads.run_artifacts),
        web.get("/v1/runs/{run_id}", reads.run_detail),
        web.get("/v1/sessions", reads.sessions),
        web.get("/v1/sessions/{session_ref}/events", reads.session_events),
        web.get("/v1/sessions/{session_ref}/requests", reads.session_requests),
        web.get("/v1/sessions/{session_ref}/checkpoints", reads.session_checkpoints),
        web.post("/v1/runs/{run_id}/archive", writes.run_archive),
        web.post("/v1/sessions/{session_ref}/archive", writes.session_archive),
        web.post("/v1/sessions/{session_ref}/checkpoints/restore", writes.session_write, name="restore_checkpoint"),
        web.post("/v1/sessions/{session_ref}/{action:turns|input|approval|interrupt|stop}", writes.session_write),
        web.get("/v1/sessions/{session_ref}", reads.session_detail),
        web.post("/v1/work/start", writes.work_start),
        web.post("/v1/work/resume", writes.work_resume),
    ])
    return app


class CockpitReadHandlers:
    def __init__(self, ctx: CockpitAppContext) -> None:
        self.ctx = ctx

    async def health(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        return web.json_response({"ok": True, "api_version": API_VERSION, "schema_version": SCHEMA_VERSION})

    async def catalog(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        return web.json_response(cockpit_catalog())

    async def snapshot(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        mode = str(request.query.get("sync") or "none")
        return web.json_response(await asyncio.to_thread(_cockpit_snapshot, self.ctx, mode))

    async def workers(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        probe = str(request.query.get("sync") or request.query.get("probe") or "none") == "probe" or request.query.get("probe") == "true"
        return web.json_response(
            {
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "workers": await asyncio.to_thread(
                    worker_profiles,
                    worker_cfg=self.ctx.cfg.worker,
                    workers_path=self.ctx.cfg.orchestration.workers_path,
                    probe=probe,
                    http_get=self.ctx.get,
                ),
            }
        )

    async def worker_detail(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        worker_id = request.match_info["worker_id"]
        profiles = await asyncio.to_thread(
            worker_profiles,
            worker_cfg=self.ctx.cfg.worker,
            workers_path=self.ctx.cfg.orchestration.workers_path,
            probe=True,
            http_get=self.ctx.get,
        )
        worker = next((item for item in profiles if item.get("worker_id") == worker_id), None)
        if worker is None:
            raise CockpitError("not_found", "worker not found", status=404)
        return web.json_response(worker)

    async def runs(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        mode = str(request.query.get("sync") or "none")
        if mode in {"fast", "probe"}:
            await asyncio.to_thread(
                sync_state,
                store=self.ctx.store,
                worker_cfg=self.ctx.cfg.worker,
                workers_path=self.ctx.cfg.orchestration.workers_path,
                sync_mode=mode,
                http_get=self.ctx.get,
            )
        run_items = await asyncio.to_thread(_visible_runs, self.ctx.store)
        requests = (
            await asyncio.to_thread(
                aggregate_requests,
                worker_cfg=self.ctx.cfg.worker,
                workers_path=self.ctx.cfg.orchestration.workers_path,
                http_get=self.ctx.get,
            )
            if mode in {"fast", "probe"}
            else []
        )
        artifacts = artifact_summaries(run_items)
        return web.json_response({"runs": [run_summary(run, requests=requests, artifacts=artifacts) for run in run_items]})

    async def run_detail(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        run = await asyncio.to_thread(_run_or_404, self.ctx.store, request.match_info["run_id"])
        requests = await asyncio.to_thread(
            aggregate_requests,
            worker_cfg=self.ctx.cfg.worker,
            workers_path=self.ctx.cfg.orchestration.workers_path,
            http_get=self.ctx.get,
        )
        artifacts = artifact_summaries([run])
        detail = run_detail_projection(run, requests=requests, artifacts=artifacts)
        return web.json_response({"run": detail, "summary": run_summary(run, requests=requests, artifacts=artifacts)})

    async def run_events(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        run = await asyncio.to_thread(_run_or_404, self.ctx.store, request.match_info["run_id"])
        raw_events = await asyncio.to_thread(self.ctx.store.events, run.run_id)
        events = [_project_run_event(event, idx + 1) for idx, event in enumerate(raw_events)]
        return web.json_response(paged(events, after=str(request.query.get("after") or ""), limit=_limit(request)))

    async def run_artifacts(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        run = await asyncio.to_thread(_run_or_404, self.ctx.store, request.match_info["run_id"])
        report_artifact = await asyncio.to_thread(run_report_artifact, self.ctx.store, run.run_id)
        items = [*artifact_summaries([run]), report_artifact]
        return web.json_response(paged(items, after=str(request.query.get("after") or ""), limit=_limit(request)))

    async def sessions(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        mode = _sync_mode(request)
        include_worker_state = mode in {"fast", "probe"}
        all_runs = await asyncio.to_thread(self.ctx.store.list_runs)
        runs_list = [run for run in all_runs if not run.archived_at]
        workers = await asyncio.to_thread(
            worker_profiles,
            worker_cfg=self.ctx.cfg.worker,
            workers_path=self.ctx.cfg.orchestration.workers_path,
            probe=mode == "probe",
            http_get=self.ctx.get,
        )
        session_rows = await asyncio.to_thread(
            aggregate_sessions,
            runs=runs_list,
            worker_cfg=self.ctx.cfg.worker,
            workers_path=self.ctx.cfg.orchestration.workers_path,
            http_get=self.ctx.get,
            worker_by_id={worker["worker_id"]: worker for worker in workers},
            include_worker_state=include_worker_state,
            archived_run_ids={run.run_id for run in all_runs if run.archived_at},
            archived_session_refs=archived_session_refs_for_store(self.ctx.store, all_runs),
        )
        requests = []
        checkpoints = []
        if include_worker_state:
            requests = await asyncio.to_thread(
                aggregate_requests,
                worker_cfg=self.ctx.cfg.worker,
                workers_path=self.ctx.cfg.orchestration.workers_path,
                http_get=self.ctx.get,
            )
            checkpoints = await asyncio.to_thread(
                aggregate_checkpoints,
                runs=runs_list,
                worker_cfg=self.ctx.cfg.worker,
                workers_path=self.ctx.cfg.orchestration.workers_path,
                http_get=self.ctx.get,
            )
        return web.json_response({"sessions": [session_summary(row, requests=requests, checkpoints=checkpoints) for row in session_rows.values()]})

    async def session_detail(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.cfg, self.ctx.store, request.match_info["session_ref"], get=self.ctx.get)
        raw = await asyncio.to_thread(_worker_get_json, self.ctx.cfg, ref.worker_id, f"/sessions/{ref.session_id}", get=self.ctx.get)
        row = _worker_session_row(raw, ref.worker_id)
        requests = [
            _request_with_run(request_item, row.get("run_id", ""))
            for request_item in await asyncio.to_thread(_worker_session_requests, self.ctx.cfg, ref, get=self.ctx.get)
        ]
        checkpoints = await asyncio.to_thread(_worker_session_checkpoints, self.ctx.cfg, ref, run_id=str(row.get("run_id") or ""), get=self.ctx.get)
        return web.json_response({"session": session_summary(row, requests=requests, checkpoints=checkpoints), "raw": _public_session_detail(raw)})

    async def session_events(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.cfg, self.ctx.store, request.match_info["session_ref"], get=self.ctx.get)
        run_id = await asyncio.to_thread(_session_run_id, self.ctx.cfg, ref, get=self.ctx.get)
        raw = await asyncio.to_thread(
            _worker_get_json,
            self.ctx.cfg,
            ref.worker_id,
            f"/sessions/{ref.session_id}/events",
            get=self.ctx.get,
        )
        events = [
            project_session_event(event, worker_id=ref.worker_id, run_id=run_id, sequence=idx + 1)
            for idx, event in enumerate(raw.get("events", []))
            if isinstance(event, dict)
        ]
        return web.json_response(paged(events, after=str(request.query.get("after") or ""), limit=_limit(request)))

    async def session_requests(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.cfg, self.ctx.store, request.match_info["session_ref"], get=self.ctx.get)
        run_id = await asyncio.to_thread(_session_run_id, self.ctx.cfg, ref, get=self.ctx.get)
        requests = [
            _request_with_run(request_item, run_id)
            for request_item in await asyncio.to_thread(_worker_session_requests, self.ctx.cfg, ref, get=self.ctx.get)
        ]
        return web.json_response({"requests": requests})

    async def session_checkpoints(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.cfg, self.ctx.store, request.match_info["session_ref"], get=self.ctx.get)
        run_id = await asyncio.to_thread(_session_run_id, self.ctx.cfg, ref, get=self.ctx.get)
        checkpoints = await asyncio.to_thread(_worker_session_checkpoints, self.ctx.cfg, ref, run_id=run_id, get=self.ctx.get)
        return web.json_response({"checkpoints": checkpoints})


class CockpitWriteHandlers:
    def __init__(self, ctx: CockpitAppContext) -> None:
        self.ctx = ctx

    async def work_start(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        body = await _json_body(request)
        _reject_attachments(body)
        async with _idempotency_scope(self.ctx, "work/start", str(body.get("idempotency_key") or "")):
            cached = self.ctx.idempotency.get("work/start", str(body.get("idempotency_key") or ""), body)
            if cached is not None:
                return web.json_response(cached)
            command, manual_item = _command_from_body(body, start=True)
            service = self.ctx.service(manual_item=manual_item)
            try:
                result = await asyncio.to_thread(service.next_work, command, start=True)
            except (MissingAuthorityError, NoEligibleWorkerError, WorkAlreadyOwnedError, MissingWorkRepoError, WorkerDispatchError) as exc:
                raise _service_error(exc) from exc
            if result is None or not isinstance(result, StartedWork):
                raise CockpitError("not_found", "no eligible work item found", recoverable=True, status=404)
            response_body = _started_work_packet(self.ctx.store, result)
            self.ctx.idempotency.save("work/start", str(body.get("idempotency_key") or ""), body, response_body)
        return web.json_response(response_body)

    async def work_resume(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        body = await _json_body(request)
        async with _idempotency_scope(self.ctx, "work/resume", str(body.get("idempotency_key") or "")):
            cached = self.ctx.idempotency.get("work/resume", str(body.get("idempotency_key") or ""), body)
            if cached is not None:
                return web.json_response(cached)
            service = self.ctx.service()
            try:
                result = await asyncio.to_thread(service.resume_run, str(body.get("run_id") or "latest"), prompt=str(body.get("prompt") or ""))
            except (MissingAuthorityError, NoEligibleWorkerError, ResumeRunError, WorkerDispatchError) as exc:
                raise _service_error(exc) from exc
            response_body = _started_work_packet(self.ctx.store, result)
            self.ctx.idempotency.save("work/resume", str(body.get("idempotency_key") or ""), body, response_body)
        return web.json_response(response_body)

    async def run_archive(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        body = await _json_body(request)
        run_id = request.match_info["run_id"]
        scope = f"runs/{run_id}/archive"
        async with _idempotency_scope(self.ctx, scope, str(body.get("idempotency_key") or "")):
            cached = self.ctx.idempotency.get(scope, str(body.get("idempotency_key") or ""), body)
            if cached is not None:
                return web.json_response(cached)
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            run = await asyncio.to_thread(_archive_run, self.ctx.store, run_id)
            response_body = _archive_run_packet(run)
            self.ctx.idempotency.save(scope, str(body.get("idempotency_key") or ""), body, response_body)
        return web.json_response(response_body)

    async def session_archive(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        body = await _json_body(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.cfg, self.ctx.store, request.match_info["session_ref"], get=self.ctx.get)
        scope = f"sessions/{ref.worker_id}/{ref.session_id}/archive"
        async with _idempotency_scope(self.ctx, scope, str(body.get("idempotency_key") or "")):
            cached = self.ctx.idempotency.get(scope, str(body.get("idempotency_key") or ""), body)
            if cached is not None:
                return web.json_response(cached)
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            run = await asyncio.to_thread(_archive_session, self.ctx.store, ref)
            response_body = _archive_session_packet(run, ref)
            self.ctx.idempotency.save(scope, str(body.get("idempotency_key") or ""), body, response_body)
        return web.json_response(response_body)

    async def session_write(self, request: web.Request) -> web.Response:
        self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.cfg, self.ctx.store, request.match_info["session_ref"], get=self.ctx.get)
        action = request.match_info.get("action", "restore_checkpoint")
        body = await _json_body(request)
        if action == "turns":
            _reject_attachments(body)
        scope = f"sessions/{ref.worker_id}/{ref.session_id}/{action}"
        async with _idempotency_scope(self.ctx, scope, str(body.get("idempotency_key") or "")):
            cached = self.ctx.idempotency.get(scope, str(body.get("idempotency_key") or ""), body)
            if cached is not None:
                return web.json_response(cached)
            required = _required_session_action(action)
            _require_capability(self.ctx.cfg, required)
            proxied = _worker_control_body(body, required)
            path = f"/sessions/{ref.session_id}/{'checkpoints/restore' if action == 'restore_checkpoint' else action}"
            raw = await asyncio.to_thread(_worker_post_json, self.ctx.cfg, ref.worker_id, path, proxied, post=self.ctx.post)
            response_body = await asyncio.to_thread(_session_write_packet, self.ctx.cfg, self.ctx.store, ref, raw, get=self.ctx.get)
            self.ctx.idempotency.save(scope, str(body.get("idempotency_key") or ""), body, response_body)
        return web.json_response(response_body)


class SseHandlers:
    def __init__(self, ctx: CockpitAppContext) -> None:
        self.ctx = ctx

    async def events(self, request: web.Request) -> web.StreamResponse:
        self.ctx.require_auth(request)
        mode = str(request.query.get("sync") or "none")
        client_cursor = str(request.query.get("after") or request.headers.get("Last-Event-ID") or "")
        body = await asyncio.to_thread(_cockpit_snapshot, self.ctx, mode)
        cursor = body["cursor"]
        response = web.StreamResponse(status=200, headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
        await response.prepare(request)
        if client_cursor != cursor:
            await _write_sse(response, "snapshot", cursor, _sse_envelope(cursor, "snapshot", body))
        try:
            while True:
                await asyncio.sleep(1)
                next_body = await asyncio.to_thread(_cockpit_snapshot, self.ctx, "none")
                next_cursor = next_body["cursor"]
                if next_cursor != cursor:
                    cursor = next_cursor
                    await _write_sse(response, "snapshot", cursor, _sse_envelope(cursor, "snapshot", next_body))
                else:
                    await response.write(b": heartbeat\n\n")
        except (asyncio.CancelledError, ConnectionResetError, RuntimeError):
            return response


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
            {"ok": False, "error": {"code": "internal_error", "message": public_error_message(str(exc) or "internal server error"), "recoverable": False}},
            status=500,
        )


async def _write_sse(response: web.StreamResponse, event: str, cursor: str, data: dict[str, Any]) -> None:
    payload = json.dumps(data, sort_keys=True)
    await response.write(f"id: {cursor}\nevent: {event}\ndata: {payload}\n\n".encode("utf-8"))


def _sse_envelope(cursor: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"cursor": cursor, "occurred_at": utc_now(), "type": event_type, "payload": payload}


def _work_source(name: str, cfg: Config | None = None) -> WorkSource:
    if name == "github":
        return GitHubWorkSource()
    if name == "linear":
        api_key = cfg.linear.api_key.get_secret_value() if cfg is not None else None
        return LinearWorkSource(api_key)
    if name == "manual":
        return _ManualWorkSource(None)
    raise CockpitError("validation_failed", f"unsupported work source: {name}", recoverable=True, status=400)


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
    elif command.engine_strategy == "parallel":
        command.engine_strategy = "ensemble"
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
    run_row = run_summary(run, artifacts=artifacts) if run else {}
    return {
        "ok": True,
        "cursor": snapshot_cursor({"run": run_row, "sessions": sessions, "artifacts": artifacts}),
        "run": run_row,
        "session": sessions[0] if sessions else {},
        "events": [],
        "requests": [],
        "artifacts": artifacts,
    }


@contextlib.asynccontextmanager
async def _idempotency_scope(ctx: CockpitAppContext, scope: str, key: str):  # noqa: ANN202
    if not key:
        yield
        return
    lock_key = f"{scope}\0{key}"
    lock = ctx.idempotency_locks.setdefault(lock_key, asyncio.Lock())
    async with lock:
        yield


def _session_write_packet(cfg: Config, store: OrchestrationStore, ref, raw: dict[str, Any], *, get: HttpGet) -> dict[str, Any]:  # noqa: ANN001
    row = _fallback_session_row(store, ref, raw)
    requests: list[dict[str, Any]] = []
    checkpoints: list[dict[str, Any]] = []
    with contextlib.suppress(CockpitError):
        session_raw = _worker_get_json(cfg, ref.worker_id, f"/sessions/{ref.session_id}", get=get)
        row.update(_worker_session_row(session_raw, ref.worker_id))
    with contextlib.suppress(CockpitError):
        requests = _worker_session_requests(cfg, ref, get=get)
    with contextlib.suppress(CockpitError):
        checkpoints = _worker_session_checkpoints(cfg, ref, run_id=str(row.get("run_id") or ""), get=get)
    events: list[dict[str, Any]] = []
    if isinstance(raw.get("events"), list):
        events = [project_session_event(event, worker_id=ref.worker_id, run_id=str(row.get("run_id") or ""), sequence=idx + 1) for idx, event in enumerate(raw["events"])]
    elif isinstance(raw.get("event"), dict):
        events = [project_session_event(raw["event"], worker_id=ref.worker_id, run_id=str(row.get("run_id") or ""), sequence=1)]
    _persist_session_write(store, ref, row, events)
    artifacts = []
    session_row = session_summary(row, requests=requests, checkpoints=checkpoints)
    return {
        "ok": bool(raw.get("ok", True)),
        "cursor": snapshot_cursor({"session": session_row, "events": events, "requests": requests, "artifacts": artifacts}),
        "run": {},
        "session": session_row,
        "events": events,
        "requests": requests,
        "artifacts": artifacts,
    }


def _fallback_session_row(store: OrchestrationStore, ref: SessionRef, raw: dict[str, Any]) -> dict[str, Any]:
    session_raw = raw.get("session") if isinstance(raw.get("session"), dict) else {}
    for run in store.list_runs():
        for link in run.sessions:
            if link.worker_id == ref.worker_id and link.session_id == ref.session_id:
                return {
                    "session_ref": make_session_ref(link.worker_id, link.session_id),
                    "worker_id": link.worker_id,
                    "session_id": link.session_id,
                    "run_id": run.run_id,
                    "title": run.objective,
                    "provider": str(session_raw.get("provider") or link.provider),
                    "engine": str(session_raw.get("engine") or link.engine),
                    "status": str(session_raw.get("status") or link.status),
                    "repo": next((item.item.repo for item in run.work_items if item.item.repo), ""),
                    "branch": str(session_raw.get("branch") or link.branch),
                    "cwd": str(session_raw.get("cwd") or link.cwd),
                    "latest_event_cursor": str(session_raw.get("last_event_id") or link.last_event_id),
                    "created_at": run.created_at,
                    "updated_at": run.updated_at,
                    "archived_at": link.archived_at,
                }
    session_id = str(session_raw.get("session_id") or ref.session_id)
    return {
        "session_ref": make_session_ref(ref.worker_id, session_id),
        "worker_id": ref.worker_id,
        "session_id": session_id,
        "run_id": str(session_raw.get("run_id") or ""),
        "title": str(session_raw.get("title") or ""),
        "provider": str(session_raw.get("provider") or ""),
        "engine": str(session_raw.get("engine") or session_raw.get("provider") or ""),
        "status": str(session_raw.get("status") or ""),
        "repo": str(session_raw.get("repo") or ""),
        "branch": str(session_raw.get("branch") or ""),
        "cwd": str(session_raw.get("cwd") or ""),
        "latest_event_cursor": str(session_raw.get("last_event_id") or ""),
        "created_at": str(session_raw.get("created_at") or ""),
        "updated_at": str(session_raw.get("updated_at") or ""),
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
        return CockpitError("provider_unavailable", public_error_message(str(exc)), recoverable=True, status=502)
    return CockpitError("internal_error", str(exc), status=500)


async def _json_body(request: web.Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception as exc:
        raise CockpitError("validation_failed", "request body must be JSON", status=400) from exc
    if not isinstance(body, dict):
        raise CockpitError("validation_failed", "request body must be an object", status=400)
    return body


def _reject_attachments(body: dict[str, Any]) -> None:
    attachments = body.get("attachments")
    if attachments in (None, []):
        return
    raise CockpitError("validation_failed", "turn attachments are not supported by Jarvis cockpit API v1", recoverable=True, status=400)


def _run_or_404(store: OrchestrationStore, run_id: str):
    run = store.get(run_id)
    if run is None:
        raise CockpitError("not_found", "run not found", status=404)
    return run


def _visible_runs(store: OrchestrationStore):
    return [run for run in store.list_runs() if not run.archived_at]


def _archive_run(store: OrchestrationStore, run_id: str):
    try:
        return store.archive_run(run_id)
    except KeyError as exc:
        raise CockpitError("not_found", "run not found", status=404) from exc


def _archive_session(store: OrchestrationStore, ref: SessionRef):
    archived = store.archive_worker_session(ref.worker_id, ref.session_id)
    for run in store.list_runs():
        for session in run.sessions:
            if session.worker_id == ref.worker_id and session.session_id == ref.session_id:
                return store.archive_session(run.run_id, ref.session_id)
    return archived


def _archive_run_packet(run) -> dict[str, Any]:  # noqa: ANN001
    run_row = run_summary(run)
    return {
        "ok": True,
        "cursor": snapshot_cursor({"run": run_row, "archived": True}),
        "run": run_row,
        "session": {},
        "events": [],
        "requests": [],
        "artifacts": [],
    }


def _archive_session_packet(run, ref: SessionRef) -> dict[str, Any]:  # noqa: ANN001
    if hasattr(run, "sessions"):
        session = next((item for item in run.sessions if item.worker_id == ref.worker_id and item.session_id == ref.session_id), None)
        session_row = session_summary(_session_from_link(session, run)) if session is not None else {}
        run_row = run_summary(run)
    else:
        session_row = session_summary(
            {
                "session_ref": make_session_ref(ref.worker_id, ref.session_id),
                "worker_id": ref.worker_id,
                "session_id": ref.session_id,
                "status": "archived",
                "archived_at": str(run.get("archived_at") or utc_now()) if isinstance(run, dict) else utc_now(),
            }
        )
        run_row = {}
    return {
        "ok": True,
        "cursor": snapshot_cursor({"run": run_row, "session": session_row, "archived": True}),
        "run": run_row,
        "session": session_row,
        "events": [],
        "requests": [],
        "artifacts": [],
    }


def _cockpit_snapshot(ctx: CockpitAppContext, mode: str) -> dict[str, Any]:
    return cockpit_snapshot(
        store=ctx.store,
        worker_cfg=ctx.cfg.worker,
        workers_path=ctx.cfg.orchestration.workers_path,
        sync_mode=mode,
        http_get=ctx.get,
    )


def _sync_mode(request: web.Request) -> str:
    mode = str(request.query.get("sync") or "none")
    return mode if mode in {"none", "fast", "probe"} else "none"


def _resolve_session_ref(cfg: Config, store: OrchestrationStore, session_ref: str, *, get: HttpGet) -> SessionRef:
    if not valid_session_ref(session_ref):
        raise CockpitError("not_found", "session not found", status=404)
    for run in store.list_runs():
        for link in run.sessions:
            if make_session_ref(link.worker_id, link.session_id) == session_ref:
                return SessionRef(worker_id=link.worker_id, session_id=link.session_id)
    for item in store.archived_worker_sessions().values():
        worker_id = str(item.get("worker_id") or "")
        session_id = str(item.get("session_id") or "")
        if worker_id and session_id and make_session_ref(worker_id, session_id) == session_ref:
            return SessionRef(worker_id=worker_id, session_id=session_id)
    registry = WorkerRegistry(cfg.worker, profiles_path=cfg.orchestration.workers_path)
    for profile in registry.profiles(probe=False):
        if not profile.base_url:
            continue
        try:
            response = get(f"{profile.base_url}/sessions", headers=worker_headers(cfg.worker, profile), timeout=cfg.worker.request_timeout_s)
            if getattr(response, "status_code", 200) >= 400:
                continue
            sessions = response.json().get("sessions", [])
        except Exception:  # noqa: BLE001 - unknown refs should not expose worker failure details
            continue
        for raw in sessions:
            if not isinstance(raw, dict):
                continue
            session_id = str(raw.get("session_id") or "")
            if session_id and make_session_ref(profile.worker_id, session_id) == session_ref:
                return SessionRef(worker_id=profile.worker_id, session_id=session_id)
    raise CockpitError("not_found", "session not found", status=404)


def _persist_session_write(store: OrchestrationStore, ref: SessionRef, row: dict[str, Any], events: list[dict[str, Any]]) -> None:
    run_id = str(row.get("run_id") or "")
    if not run_id:
        return
    try:
        store.update_session(
            run_id,
            ref.session_id,
            status=str(row.get("status") or ""),
            provider=str(row.get("provider") or ""),
            engine=str(row.get("engine") or ""),
            branch=str(row.get("branch") or ""),
            last_event_id=_latest_event_id(events),
        )
    except KeyError:
        return
    for event in events:
        store.append_event(
            run_id,
            str(event.get("type") or "session.event"),
            "",
            {
                "session_id": ref.session_id,
                "event_id": str(event.get("event_id") or ""),
                "turn_id": str(event.get("turn_id") or ""),
                "message_id": str(event.get("message_id") or ""),
                "data": dict(event.get("data") or {}),
            },
        )
    _finalize_session_run_if_terminal(store, run_id)


def _latest_event_id(events: list[dict[str, Any]]) -> str | None:
    for event in reversed(events):
        event_id = str(event.get("event_id") or "")
        if event_id:
            return event_id
    return None


def _finalize_session_run_if_terminal(store: OrchestrationStore, run_id: str) -> None:
    run = store.get(run_id)
    if run is None or run.status == "terminal" or not run.sessions:
        return
    statuses = {session.status for session in run.sessions}
    if statuses & ACTIVE_SESSION_STATUSES:
        return
    if statuses <= SUCCESS_SESSION_STATUSES:
        store.set_phase(run_id, "completed", "All worker sessions completed")
        return
    if statuses & FAILED_SESSION_STATUSES:
        store.set_phase(run_id, "failed", "At least one worker session failed, stopped, or was interrupted")


def _worker_control_body(body: dict[str, Any], required: str) -> dict[str, Any]:
    proxied = {key: value for key, value in body.items() if key not in {"allowed_actions", "execution_envelope"}}
    metadata = dict(proxied.get("metadata") or {}) if isinstance(proxied.get("metadata"), dict) else {}
    metadata.pop("allowed_actions", None)
    metadata.pop("control_envelope", None)
    metadata.pop("execution_envelope", None)
    proxied["metadata"] = metadata
    proxied["allowed_actions"] = [required]
    return proxied


def _project_run_event(event, sequence: int) -> dict[str, Any]:  # noqa: ANN001
    return {
        "event_id": f"{event.run_id}:{sequence}",
        "sequence": sequence,
        "run_id": event.run_id,
        "type": event.type,
        "occurred_at": event.time,
        "message": public_error_message(event.message),
        "data": public_event_data(event.data),
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
    try:
        response = get(f"{profile.base_url}{path}", headers=worker_headers(cfg.worker, profile), params=params or {}, timeout=cfg.worker.request_timeout_s)
    except Exception as exc:  # noqa: BLE001 - exact session reads should return the public cockpit error contract
        message = public_error_message(str(exc) or "worker request failed")
        raise CockpitError("worker_unavailable", message, recoverable=True, status=502) from exc
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
    try:
        response = post(f"{profile.base_url}{path}", headers=worker_headers(cfg.worker, profile), json=body, timeout=cfg.worker.request_timeout_s)
    except Exception as exc:  # noqa: BLE001
        message = public_error_message(str(exc) or "worker write failed")
        raise CockpitError("worker_unavailable", message, recoverable=True, status=502) from exc
    status = getattr(response, "status_code", 200)
    try:
        data = response.json() if hasattr(response, "json") else {}
    except Exception:
        data = {}
    if status >= 400 or (isinstance(data, dict) and data.get("ok") is False):
        message = public_error_message(str(data.get("error") or data.get("message") or "")) if isinstance(data, dict) else ""
        message = message or _response_error(response) or "worker write failed"
        code = _worker_error_code(message)
        if status == 404 and code == "checkpoint_not_found":
            raise CockpitError(code, message, recoverable=True, status=409)
        if status == 401:
            raise CockpitError("unauthorized", message, status=401)
        if status == 403:
            raise CockpitError("forbidden", message, status=403)
        if status == 404:
            raise CockpitError("not_found", message, status=404)
        raise CockpitError(code, message, recoverable=status in {400, 409}, status=409 if status == 409 else status)
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
    return {
        key: value
        for key, value in public_event_data(
            {
                "id": raw.get("session_id"),
                "run_id": raw.get("run_id"),
                "title": raw.get("title"),
                "provider": raw.get("provider"),
                "status": raw.get("status"),
                "branch": raw.get("branch"),
                "message_id": raw.get("last_event_id"),
            }
        ).items()
        if value not in ("", [], {})
    }


def _worker_session_requests(cfg: Config, ref, *, get: HttpGet) -> list[dict[str, Any]]:  # noqa: ANN001
    raw = _worker_get_json(cfg, ref.worker_id, f"/sessions/{ref.session_id}/requests", get=get)
    return [project_request(item, ref.worker_id) for item in raw.get("requests", []) if isinstance(item, dict)]


def _worker_session_checkpoints(cfg: Config, ref, *, run_id: str, get: HttpGet) -> list[dict[str, Any]]:  # noqa: ANN001
    raw = _worker_get_json(cfg, ref.worker_id, f"/sessions/{ref.session_id}/checkpoints", get=get)
    result = []
    for item in raw.get("checkpoints", []):
        if isinstance(item, dict):
            result.append(project_checkpoint(item, ref.worker_id, ref.session_id, run_id))
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
        return max(1, min(int(request.query.get("limit") or 100), MAX_PAGE_LIMIT))
    except ValueError:
        return 100


def _response_error(response: Any) -> str:
    try:
        data = response.json()
    except Exception:
        return public_error_message(str(getattr(response, "text", "") or ""))
    if isinstance(data, dict):
        return public_error_message(str(data.get("error") or data.get("message") or ""))
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


async def serve(cfg: Config) -> int:
    bind = cfg.orchestration.api_bind_host or cfg.orchestration.api_host
    token_set = bool(cfg.orchestration.api_token.get_secret_value())
    if insecure_bind(bind, token_set, cfg.orchestration.api_allow_insecure):
        print(
            f"\n✗ Refusing to start: cockpit API is bound to {bind!r} with no "
            "ORCHESTRATION_API_TOKEN.\n"
            "  Set ORCHESTRATION_API_TOKEN, or ORCHESTRATION_API_ALLOW_INSECURE=true to override.\n"
        )
        return 1
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
    return 0
