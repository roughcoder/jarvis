from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable

import httpx
from aiohttp import web

from jarvis.brain.registry import ProjectEntry, RegistryStore
from jarvis.capabilities import (
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_RESTORE,
    WORKER_SESSION_STOP,
    WORKER_SESSION_TURN,
)
from jarvis.config import Config, insecure_bind
from jarvis.ids import new_id, utc_now
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
    canonical_event_type,
    cockpit_catalog,
    cockpit_snapshot,
    make_session_ref,
    paged,
    project_worker_profile,
    project_worker_system,
    project_checkpoint,
    project_request,
    project_session_event,
    public_error_message,
    public_event_data,
    _allowed_actions_from_worker_session,
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
    _cursor_worker,
)
from jarvis.brain.capabilities import resolve_capabilities
from jarvis.orchestration.authority import allowed
from jarvis.orchestration.intent import parse_work_command
from jarvis.orchestration.models import WorkCommand, WorkItem
from jarvis.orchestration.oauth import (
    OAuthTokenValidator,
    OAuthValidationError,
    auth_mode,
    oauth_is_configured,
    oauth_metadata,
    required_scopes,
)
from jarvis.orchestration.service import (
    MissingAuthorityError,
    MissingWorkRepoError,
    NoEligibleWorkerError,
    OrchestrationService,
    ResumeRunError,
    StartedWork,
    WorkAlreadyOwnedError,
    WorkerCapacityError,
    WorkerDispatchError,
)
from jarvis.orchestration.sources import GitHubWorkSource, LinearWorkSource, WorkSource
from jarvis.orchestration.store import OrchestrationStore
from jarvis.orchestration.supervisor import final_session_phase
from jarvis.orchestration.workers import WorkerRegistry
from jarvis.system_info import system_info_cached
from jarvis.users import HOUSE

HttpGet = Callable[..., Any]
HttpPost = Callable[..., Any]
CONFIG_KEY = web.AppKey("config", Config)
logger = logging.getLogger(__name__)
SSE_REFRESH_ERROR_LOG_INTERVAL_S = 60.0


@dataclass(frozen=True)
class CockpitAppContext:
    cfg: Config
    get: HttpGet
    post: HttpPost
    store: OrchestrationStore
    idempotency: IdempotencyStore
    idempotency_locks: dict[str, asyncio.Lock]
    idempotency_lock_refs: dict[str, int]
    source_factory: Callable[[str, Any], WorkSource]
    oauth_validator: OAuthTokenValidator | None = None

    async def require_auth(self, request: web.Request) -> None:
        orchestration = self.cfg.orchestration
        mode = auth_mode(str(orchestration.auth_mode))
        header = request.headers.get("Authorization", "")
        token = orchestration.api_token.get_secret_value()
        if mode in {"legacy", "hybrid"} and token and hmac.compare_digest(header, f"Bearer {token}"):
            # AUTH-ONLY V1: this propagation is for audit/introspection. Scopes
            # are enforced globally by validator config, and future consumers
            # of jarvis_user must bind it to sub or another IdP-controlled
            # claim before using it for authorization or memory ownership.
            request["auth"] = {
                "mode": "legacy",
                "subject": "legacy-token",
                "jarvis_user": "",
                "scopes": [],
            }
            return

        if mode in {"oauth", "hybrid"} and self.oauth_validator is not None:
            prefix = "Bearer "
            if header.startswith(prefix):
                try:
                    principal = await asyncio.to_thread(self.oauth_validator.validate, header[len(prefix) :])
                except OAuthValidationError as exc:
                    raise CockpitError("unauthorized", "unauthorized", status=401) from exc
                # AUTH-ONLY V1: this propagation is for audit/introspection.
                # Route-level scopes are intentionally not wired yet; global
                # scopes come from ORCHESTRATION_OAUTH_REQUIRED_SCOPES.
                request["auth"] = {
                    "mode": "oauth",
                    "subject": principal.subject,
                    "jarvis_user": principal.jarvis_user,
                    "scopes": sorted(principal.scopes),
                }
                return

        if not token and (mode == "legacy" or (mode == "hybrid" and self.oauth_validator is None)):
            # AUTH-ONLY V1: unauthenticated dev mode carries no usable identity.
            request["auth"] = {
                "mode": "none",
                "subject": "",
                "jarvis_user": "",
                "scopes": [],
            }
            return

        if token or self.oauth_validator is not None or mode == "oauth":
            raise CockpitError("unauthorized", "unauthorized", status=401)

    def service(self, *, manual_item: WorkItem | None = None) -> OrchestrationService:
        return _service(self.cfg, self.source_factory, manual_item=manual_item)


@dataclass(frozen=True)
class SseSubscription:
    subscription_id: int
    mode: str
    queue: asyncio.Queue[dict[str, Any] | None]
    snapshot: dict[str, Any]


class SseSnapshotHub:
    def __init__(self, ctx: CockpitAppContext) -> None:
        self.ctx = ctx
        self._subscribers: dict[int, SseSubscription] = {}
        self._snapshots: dict[str, dict[str, Any]] = {}
        self._event_counts: dict[str, dict[str, int]] = {}
        self._lock = asyncio.Lock()
        self._next_id = 0
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._next_refresh_error_log_at = 0.0

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="cockpit-sse-snapshot-hub")

    async def stop(self) -> None:
        self._stopping.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def subscribe(self, mode: str) -> SseSubscription:
        snapshot = await self._snapshot(mode)
        async with self._lock:
            self._next_id += 1
            subscription = SseSubscription(
                subscription_id=self._next_id,
                mode=mode,
                queue=asyncio.Queue(maxsize=8),
                snapshot=snapshot,
            )
            self._subscribers[subscription.subscription_id] = subscription
            return subscription

    async def unsubscribe(self, subscription: SseSubscription) -> None:
        async with self._lock:
            self._subscribers.pop(subscription.subscription_id, None)
            if not any(existing.mode == subscription.mode for existing in self._subscribers.values()):
                # Nobody is listening: drop the event baseline so a future
                # subscriber doesn't get the idle period replayed as frames.
                self._event_counts.pop(subscription.mode, None)

    async def _snapshot(self, mode: str) -> dict[str, Any]:
        cached = self._snapshots.get(mode)
        body = cached if cached is not None else await asyncio.to_thread(_cockpit_snapshot, self.ctx, mode)
        if mode not in self._event_counts:
            # Baseline per-run event counts at subscribe time so events that
            # land between now and the first refresh tick are emitted, not
            # silently absorbed into the first baseline.
            counts = await asyncio.to_thread(self._prime_event_counts, body)
            async with self._lock:
                self._event_counts.setdefault(mode, counts)
        async with self._lock:
            return self._snapshots.setdefault(mode, body)

    async def _run(self) -> None:
        refresh_interval = max(0.1, float(self.ctx.cfg.orchestration.sse_refresh_interval_s))
        heartbeat_interval = max(1.0, float(self.ctx.cfg.orchestration.sse_heartbeat_interval_s))
        heartbeat_at = asyncio.get_running_loop().time() + heartbeat_interval
        while not self._stopping.is_set():
            await asyncio.sleep(refresh_interval)
            try:
                modes = await self._active_modes()
                for mode in modes:
                    body = await asyncio.to_thread(_cockpit_snapshot, self.ctx, mode)
                    previous = self._snapshots.get(mode)
                    self._snapshots[mode] = body
                    baselined = mode in self._event_counts
                    if not baselined:
                        # First tick for this mode: baseline the per-run event
                        # counts so history is not replayed as live frames.
                        self._event_counts[mode] = await asyncio.to_thread(self._prime_event_counts, body)
                    if previous is None or previous.get("cursor") != body.get("cursor"):
                        deltas = _snapshot_delta_events(previous, body)
                        frames = (
                            await asyncio.to_thread(self._collect_session_event_frames, mode, body) if baselined else []
                        )
                        if deltas is not None and frames:
                            deltas = [*deltas, *frames]
                        await self._broadcast(
                            mode,
                            {
                                "body": body,
                                "prev_cursor": str(previous.get("cursor") or "") if previous else "",
                                "events": deltas,
                            },
                        )
            except asyncio.CancelledError:
                raise
            except Exception:
                now = asyncio.get_running_loop().time()
                if now >= self._next_refresh_error_log_at:
                    logger.exception("cockpit SSE snapshot refresh failed")
                    self._next_refresh_error_log_at = now + SSE_REFRESH_ERROR_LOG_INTERVAL_S
            now = asyncio.get_running_loop().time()
            if now >= heartbeat_at:
                try:
                    await self._broadcast_heartbeat()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("cockpit SSE heartbeat broadcast failed")
                heartbeat_at = now + heartbeat_interval

    def _prime_event_counts(self, body: dict[str, Any]) -> dict[str, int]:
        return {
            run_id: len(self.ctx.store.events(run_id))
            for row in body.get("runs") or []
            if isinstance(row, dict)
            for run_id in [str(row.get("run_id") or "")]
            if run_id
        }

    def _collect_session_event_frames(self, mode: str, body: dict[str, Any]) -> list[dict[str, Any]]:
        """Newly appended store events for visible runs, projected as
        session.event SSE frames. Counts advance per broadcast, so events that
        land between broadcasts are delivered on the next one."""
        counts = self._event_counts.get(mode, {})
        cursor = str(body.get("cursor") or "")
        occurred_at = utc_now()
        frames: list[dict[str, Any]] = []
        new_counts: dict[str, int] = {}
        for row in body.get("runs") or []:
            run_id = str(row.get("run_id") or "") if isinstance(row, dict) else ""
            if not run_id:
                continue
            events = self.ctx.store.events(run_id)
            start = counts.get(run_id, 0)
            new_counts[run_id] = len(events)
            if len(events) <= start:
                continue
            run = self.ctx.store.get(run_id)
            worker_by_session = {link.session_id: link.worker_id for link in (run.sessions if run else [])}
            for sequence, event in enumerate(events[start:], start=start + 1):
                data = event.data if isinstance(event.data, dict) else {}
                session_id = str(data.get("session_id") or "")
                worker_id = worker_by_session.get(session_id, "")
                # Only worker-originated SessionEvents carry an event_id;
                # internal store records (dispatch/sync bookkeeping) that
                # happen to mention a session are not per-turn timeline
                # entries and must not be streamed as such.
                if not session_id or not worker_id or not str(data.get("event_id") or ""):
                    continue
                projected = project_session_event(
                    {
                        "event_id": data.get("event_id"),
                        "session_id": session_id,
                        "type": event.type,
                        "time": data.get("time") or event.time,
                        "data": dict(data.get("data") or {}),
                    },
                    worker_id=worker_id,
                    run_id=run_id,
                    sequence=sequence,
                )
                frames.append(
                    {
                        "cursor": cursor,
                        "occurred_at": occurred_at,
                        "type": "session.event",
                        "run_id": run_id,
                        "session_ref": projected["session_ref"],
                        "worker_id": worker_id,
                        "payload": projected,
                    }
                )
        self._event_counts[mode] = new_counts
        return frames

    async def _active_modes(self) -> list[str]:
        async with self._lock:
            return sorted({subscription.mode for subscription in self._subscribers.values()})

    async def _broadcast(self, mode: str, body: dict[str, Any]) -> None:
        async with self._lock:
            subscribers = [subscription for subscription in self._subscribers.values() if subscription.mode == mode]
        for subscription in subscribers:
            _queue_latest(subscription.queue, body)

    async def _broadcast_heartbeat(self) -> None:
        async with self._lock:
            subscribers = list(self._subscribers.values())
        for subscription in subscribers:
            _queue_latest(subscription.queue, None)


SSE_SNAPSHOT_HUB_KEY = web.AppKey("sse_snapshot_hub", SseSnapshotHub)


def _queue_latest(queue: asyncio.Queue[dict[str, Any] | None], item: dict[str, Any] | None) -> None:
    if queue.full():
        with contextlib.suppress(asyncio.QueueEmpty):
            queue.get_nowait()
    queue.put_nowait(item)


async def _sse_snapshot_hub_context(app: web.Application):  # noqa: ANN202
    hub = app[SSE_SNAPSHOT_HUB_KEY]
    await hub.start()
    try:
        yield
    finally:
        await hub.stop()


def make_app(
    cfg: Config,
    *,
    http_get: HttpGet | None = None,
    http_post: HttpPost | None = None,
    source_factory: Callable[[str, Any], WorkSource] | None = None,
) -> web.Application:
    orchestration = cfg.orchestration
    validator = (
        OAuthTokenValidator(
            issuer=str(orchestration.oauth_issuer),
            audience=str(orchestration.oauth_audience),
            jwks_url=str(orchestration.oauth_jwks_url),
            scopes=required_scopes(str(orchestration.oauth_required_scopes)),
            jarvis_user_claim=str(orchestration.oauth_jarvis_user_claim),
            default_alg=str(orchestration.oauth_default_alg),
            jwks_ttl_s=float(orchestration.oauth_jwks_ttl_s),
            jwks_min_refresh_s=float(orchestration.oauth_jwks_min_refresh_s),
            http_get=http_get or httpx.get,
        )
        if auth_mode(str(orchestration.auth_mode)) in {"oauth", "hybrid"} and oauth_is_configured(orchestration)
        else None
    )
    ctx = CockpitAppContext(
        cfg=cfg,
        get=http_get or httpx.get,
        post=http_post or httpx.post,
        store=OrchestrationStore(cfg.orchestration.workspace),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=source_factory or _work_source,
        oauth_validator=validator,
    )
    _rebuild_session_ref_index_from_store(ctx.store)
    app = web.Application(middlewares=[_cors_middleware, _error_middleware])
    app[CONFIG_KEY] = cfg
    reads = CockpitReadHandlers(ctx)
    writes = CockpitWriteHandlers(ctx)
    sse = SseHandlers(ctx)
    hub = SseSnapshotHub(ctx)
    app[SSE_SNAPSHOT_HUB_KEY] = hub
    app.cleanup_ctx.append(_sse_snapshot_hub_context)
    app.add_routes([
        web.get("/v1/auth/metadata", reads.auth_metadata),
        web.get("/v1/health", reads.health),
        web.get("/v1/cockpit/catalog", reads.catalog),
        web.get("/v1/cockpit/snapshot", reads.snapshot),
        web.get("/v1/cockpit/events", sse.events),
        web.get("/v1/projects", reads.projects),
        web.get("/v1/projects/{project_id}", reads.project_detail),
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
        web.post("/v1/work/validate", writes.work_validate),
    ])
    return app


class CockpitReadHandlers:
    def __init__(self, ctx: CockpitAppContext) -> None:
        self.ctx = ctx

    async def auth_metadata(self, _request: web.Request) -> web.Response:
        return web.json_response(oauth_metadata(self.ctx.cfg.orchestration), headers={"Cache-Control": "no-store"})

    async def health(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        system = await asyncio.to_thread(system_info_cached)
        return web.json_response(
            {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "system": project_worker_system(system),
            }
        )

    async def catalog(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        defaults, engines = await asyncio.to_thread(_catalog_context, self.ctx.cfg)
        return web.json_response(cockpit_catalog(start_defaults=defaults, engines=engines or None))

    async def snapshot(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        mode = _sync_mode(request)
        return web.json_response(await asyncio.to_thread(_cockpit_snapshot, self.ctx, mode))

    async def workers(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        probe = _worker_probe(request)
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
                    default_repo=self.ctx.cfg.orchestration.default_repo,
                ),
            }
        )

    async def worker_detail(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        worker_id = request.match_info["worker_id"]
        probe = _worker_probe(request)
        profile = await asyncio.to_thread(
            WorkerRegistry(self.ctx.cfg.worker, profiles_path=self.ctx.cfg.orchestration.workers_path, http_get=self.ctx.get).get,
            worker_id,
            probe=probe,
        )
        if profile is None:
            raise CockpitError("not_found", "worker not found", status=404)
        return web.json_response(project_worker_profile(profile, default_repo=self.ctx.cfg.orchestration.default_repo))

    async def projects(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester_id = _cockpit_requester_id(self.ctx.cfg)
        include_archived = _include_archived(request)
        projects = (
            await asyncio.to_thread(_visible_projects, self.ctx.cfg, requester_id, include_archived=include_archived)
            if requester_id
            else []
        )
        return web.json_response(
            {
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "projects": [project.as_dict() for project in projects],
            }
        )

    async def project_detail(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester_id = _cockpit_requester_id(self.ctx.cfg)
        project = (
            await asyncio.to_thread(_visible_project_or_404, self.ctx.cfg, request.match_info["project_id"], requester_id)
            if requester_id
            else None
        )
        if project is None:
            raise CockpitError("not_found", "project not found", status=404)
        return web.json_response(
            {
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "project": project.as_dict(),
            }
        )

    async def runs(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
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
        await self.ctx.require_auth(request)
        run = await asyncio.to_thread(_run_or_404, self.ctx.store, request.match_info["run_id"])
        requests = await asyncio.to_thread(
            aggregate_requests,
            worker_cfg=self.ctx.cfg.worker,
            workers_path=self.ctx.cfg.orchestration.workers_path,
            http_get=self.ctx.get,
        )
        artifacts = artifact_summaries([run], include_archived=True)
        detail = run_detail_projection(run, requests=requests, artifacts=artifacts)
        return web.json_response({"run": detail, "summary": run_summary(run, requests=requests, artifacts=artifacts)})

    async def run_events(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        run = await asyncio.to_thread(_run_or_404, self.ctx.store, request.match_info["run_id"])
        raw_events = await asyncio.to_thread(self.ctx.store.events, run.run_id)
        events = [_project_run_event(event, idx + 1) for idx, event in enumerate(raw_events)]
        return web.json_response(paged(events, after=str(request.query.get("after") or ""), limit=_limit(request)))

    async def run_artifacts(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        run = await asyncio.to_thread(_run_or_404, self.ctx.store, request.match_info["run_id"])
        report_artifact = await asyncio.to_thread(run_report_artifact, self.ctx.store, run.run_id)
        items = [*artifact_summaries([run], include_archived=True), report_artifact]
        return web.json_response(paged(items, after=str(request.query.get("after") or ""), limit=_limit(request)))

    async def sessions(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
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
            default_repo=self.ctx.cfg.orchestration.default_repo,
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
                sessions=session_rows,
                worker_cfg=self.ctx.cfg.worker,
                workers_path=self.ctx.cfg.orchestration.workers_path,
                http_get=self.ctx.get,
            )
        self.ctx.store.record_session_refs(_session_ref_index_rows(session_rows.values()))
        return web.json_response({"sessions": [session_summary(row, requests=requests, checkpoints=checkpoints) for row in session_rows.values()]})

    async def session_detail(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
        raw: dict[str, Any] = {}
        has_stored_projection = await asyncio.to_thread(_has_stored_session_projection, self.ctx.store, ref)
        row = await asyncio.to_thread(_fallback_session_row, self.ctx.store, ref, {})
        try:
            raw = await asyncio.to_thread(_worker_get_json, self.ctx.cfg, ref.worker_id, f"/sessions/{ref.session_id}", get=self.ctx.get)
            row = _overlay_session_row(row, _worker_session_row(raw, ref.worker_id))
        except CockpitError as exc:
            if exc.code == "not_found" and not has_stored_projection:
                raise
        requests: list[dict[str, Any]] = []
        checkpoints: list[dict[str, Any]] = []
        with contextlib.suppress(CockpitError):
            requests = [
                _request_with_run(request_item, row.get("run_id", ""))
                for request_item in await asyncio.to_thread(_worker_session_requests, self.ctx.cfg, ref, get=self.ctx.get)
            ]
        with contextlib.suppress(CockpitError):
            checkpoints = await asyncio.to_thread(_worker_session_checkpoints, self.ctx.cfg, ref, run_id=str(row.get("run_id") or ""), get=self.ctx.get)
        return web.json_response({"session": session_summary(row, requests=requests, checkpoints=checkpoints), "raw": _public_session_detail(raw)})

    async def session_events(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
        run_id = await asyncio.to_thread(_session_run_id_from_store, self.ctx.store, ref)
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
        await self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
        run_id = await asyncio.to_thread(_session_run_id_from_store, self.ctx.store, ref)
        requests = [
            _request_with_run(request_item, run_id)
            for request_item in await asyncio.to_thread(_worker_session_requests, self.ctx.cfg, ref, get=self.ctx.get)
        ]
        return web.json_response({"requests": requests})

    async def session_checkpoints(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
        run_id = await asyncio.to_thread(_session_run_id_from_store, self.ctx.store, ref)
        checkpoints = await asyncio.to_thread(_worker_session_checkpoints, self.ctx.cfg, ref, run_id=run_id, get=self.ctx.get)
        return web.json_response({"checkpoints": checkpoints})


class CockpitWriteHandlers:
    def __init__(self, ctx: CockpitAppContext) -> None:
        self.ctx = ctx

    async def work_start(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
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
                error = _service_error(exc)
                if isinstance(exc, (MissingWorkRepoError, WorkerDispatchError)):
                    self.ctx.idempotency.save("work/start", str(body.get("idempotency_key") or ""), body, error.body())
                raise error from exc
            if result is None or not isinstance(result, StartedWork):
                raise CockpitError("not_found", "no eligible work item found", recoverable=True, status=404)
            response_body = _started_work_packet(self.ctx.store, result)
            self.ctx.idempotency.save("work/start", str(body.get("idempotency_key") or ""), body, response_body)
        return web.json_response(response_body)

    async def work_validate(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        _reject_attachments(body)
        command, manual_item = _command_from_body(body, start=False)
        service = self.ctx.service(manual_item=manual_item)
        validation = await asyncio.to_thread(service.validate_work, command, manual_item=manual_item)
        return web.json_response(
            {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "validation": validation,
            }
        )

    async def work_resume(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
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
        await self.ctx.require_auth(request)
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
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
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
        await self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
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
        await self.ctx.require_auth(request)
        mode = _sync_mode(request)
        filters = {
            key: str(request.query.get(key) or "")
            for key in ("run_id", "session_ref", "worker_id")
            if request.query.get(key)
        }
        client_cursor = str(request.query.get("after") or request.headers.get("Last-Event-ID") or "")
        hub = request.app[SSE_SNAPSHOT_HUB_KEY]
        subscription = await hub.subscribe(mode)
        body = subscription.snapshot
        cursor = body["cursor"]
        response = web.StreamResponse(status=200, headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
        _apply_cors_headers(request, response)
        await response.prepare(request)
        if client_cursor != cursor:
            await _write_sse(response, "snapshot", cursor, _sse_envelope(cursor, "snapshot", body))
        try:
            while True:
                event = await subscription.queue.get()
                if event is None:
                    await response.write(b": heartbeat\n\n")
                    continue
                next_body = event["body"]
                next_cursor = next_body["cursor"]
                if next_cursor == cursor:
                    await response.write(b": heartbeat\n\n")
                    continue
                deltas = event.get("events")
                if deltas and event.get("prev_cursor") == cursor:
                    # The client is exactly one tick behind: send granular events
                    # instead of re-sending the whole snapshot.
                    matched = [delta for delta in deltas if _frame_matches(delta, filters)]
                    for delta in matched:
                        await _write_sse(response, delta["type"], next_cursor, delta)
                    if not matched:
                        # Everything this tick was filtered out; keep the
                        # connection's cursor moving so the next tick can still
                        # go granular.
                        await response.write(b": heartbeat\n\n")
                else:
                    await _write_sse(response, "snapshot", next_cursor, _sse_envelope(next_cursor, "snapshot", next_body))
                cursor = next_cursor
        except asyncio.CancelledError:
            raise
        except (ConnectionResetError, RuntimeError):
            return response
        finally:
            await hub.unsubscribe(subscription)


@web.middleware
async def _cors_middleware(request: web.Request, handler):  # noqa: ANN001
    if request.method == "OPTIONS":
        match_info = await request.app.router.resolve(request)
        if isinstance(match_info.http_exception, web.HTTPNotFound):
            response = web.Response(status=404)
        else:
            response = web.Response(status=204)
    else:
        response = await handler(request)
    _apply_cors_headers(request, response)
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


def _apply_cors_headers(request: web.Request, response: web.StreamResponse) -> None:
    if response.prepared:
        return
    origin = _allowed_cors_origin(request)
    if not origin:
        return
    response.headers["Access-Control-Allow-Origin"] = origin
    response.headers["Vary"] = _append_vary(response.headers.get("Vary", ""), "Origin")
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Authorization,Content-Type,Last-Event-ID"
    response.headers["Access-Control-Expose-Headers"] = "Content-Type"
    response.headers["Access-Control-Max-Age"] = "600"


def _append_vary(existing: str, value: str) -> str:
    values = [item.strip() for item in existing.split(",") if item.strip()]
    if not any(item.lower() == value.lower() for item in values):
        values.append(value)
    return ", ".join(values)


def _allowed_cors_origin(request: web.Request) -> str:
    origin = request.headers.get("Origin", "")
    if not origin:
        return ""
    cfg = request.app[CONFIG_KEY]
    allowed = [item.strip() for item in cfg.orchestration.api_cors_origins.split(",") if item.strip()]
    if "*" in allowed:
        return origin
    return origin if origin in allowed else ""


async def _write_sse(response: web.StreamResponse, event: str, cursor: str, data: dict[str, Any]) -> None:
    payload = json.dumps(data, sort_keys=True)
    await response.write(f"id: {cursor}\nevent: {event}\ndata: {payload}\n\n".encode("utf-8"))


def _sse_envelope(cursor: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {"cursor": cursor, "occurred_at": utc_now(), "type": event_type, "payload": payload}


_SNAPSHOT_DELTA_SPECS = (
    ("runs", "run_id", "run.updated"),
    ("sessions", "session_ref", "session.updated"),
    ("workers", "worker_id", "worker.updated"),
    ("artifacts", "artifact_id", "artifact.upserted"),
    ("requests", "request_id", "request.updated"),
    ("checkpoints", "checkpoint_id", "checkpoint.updated"),
)


def _snapshot_delta_events(previous: dict[str, Any] | None, current: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Project the difference between two snapshot ticks as granular cockpit
    events. Returns None when a snapshot must be sent instead: no baseline, a
    run/session/worker/checkpoint disappeared (archive/restore), or nothing
    row-level explains the cursor change."""
    if not previous:
        return None
    cursor = str(current.get("cursor") or "")
    occurred_at = utc_now()
    events: list[dict[str, Any]] = []
    for key, id_key, event_type in _SNAPSHOT_DELTA_SPECS:
        prev_rows = {str(row.get(id_key) or ""): row for row in previous.get(key) or [] if isinstance(row, dict)}
        curr_rows = {str(row.get(id_key) or ""): row for row in current.get(key) or [] if isinstance(row, dict)}
        removed = set(prev_rows) - set(curr_rows)
        if removed:
            if key == "artifacts":
                events.extend(
                    {"cursor": cursor, "occurred_at": occurred_at, "type": "artifact.removed", "artifact_id": artifact_id, "payload": {"artifact_id": artifact_id}}
                    for artifact_id in sorted(removed)
                )
            elif key == "requests":
                # A pending request leaving the projection means it is no longer
                # pending; the final decision lives on the session's requests.
                for request_id in sorted(removed):
                    stale = prev_rows[request_id]
                    envelope = {
                        "cursor": cursor,
                        "occurred_at": occurred_at,
                        "type": "request.updated",
                        "request_id": request_id,
                        "payload": {"request_id": request_id, "status": "closed", "session_ref": str(stale.get("session_ref") or "")},
                    }
                    if stale.get("run_id"):
                        envelope["run_id"] = str(stale.get("run_id"))
                    events.append(envelope)
            else:
                return None
        for row_id, row in curr_rows.items():
            if _delta_row(key, prev_rows.get(row_id)) == _delta_row(key, row):
                continue
            envelope = {"cursor": cursor, "occurred_at": occurred_at, "type": event_type, "payload": row}
            if id_key != "run_id" and row.get("run_id"):
                envelope["run_id"] = str(row.get("run_id"))
            if id_key != "session_ref" and row.get("session_ref"):
                envelope["session_ref"] = str(row.get("session_ref"))
            envelope[id_key] = row_id
            events.append(envelope)
    return events or None


def _frame_matches(frame: dict[str, Any], filters: dict[str, str]) -> bool:
    """Strict AND matching: a filtered stream only carries frames that
    explicitly carry the requested id (in the envelope or its payload)."""
    if not filters:
        return True
    payload = frame.get("payload") if isinstance(frame.get("payload"), dict) else {}
    for key, value in filters.items():
        if str(frame.get(key) or payload.get(key) or "") != value:
            return False
    return True


def _delta_row(key: str, row: dict[str, Any] | None) -> dict[str, Any] | None:
    # Worker rows carry per-refresh timestamps that the snapshot cursor already
    # ignores; ignore them here too so workers don't emit no-op updates.
    if row is None or key != "workers":
        return row
    return _cursor_worker(row)


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
            id=str(raw.get("id") or body.get("idempotency_key") or new_id("manual")),
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
            "allowed_actions": list(session.allowed_actions),
        }
        sessions.append(session_summary(row))
        store.record_session_refs(_session_ref_index_rows([row]))
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
    ctx.idempotency_lock_refs[lock_key] = ctx.idempotency_lock_refs.get(lock_key, 0) + 1
    try:
        async with lock:
            yield
    finally:
        remaining = ctx.idempotency_lock_refs.get(lock_key, 1) - 1
        if remaining <= 0:
            ctx.idempotency_lock_refs.pop(lock_key, None)
            if ctx.idempotency_locks.get(lock_key) is lock:
                ctx.idempotency_locks.pop(lock_key, None)
        else:
            ctx.idempotency_lock_refs[lock_key] = remaining


def _session_write_packet(cfg: Config, store: OrchestrationStore, ref, raw: dict[str, Any], *, get: HttpGet) -> dict[str, Any]:  # noqa: ANN001
    row = _fallback_session_row(store, ref, raw)
    requests: list[dict[str, Any]] = []
    checkpoints: list[dict[str, Any]] = []
    with contextlib.suppress(CockpitError):
        session_raw = _worker_get_json(cfg, ref.worker_id, f"/sessions/{ref.session_id}", get=get)
        row = _overlay_session_row(row, _worker_session_row(session_raw, ref.worker_id))
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
                    "allowed_actions": list(link.allowed_actions),
                }
    session_id = str(session_raw.get("session_id") or ref.session_id)
    archived = store.archived_worker_sessions().get(f"{ref.worker_id}\0{session_id}", {})
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
        "archived_at": archived.get("archived_at") or "",
        "allowed_actions": _allowed_actions_from_worker_session(session_raw),
    }


def _has_stored_session_projection(store: OrchestrationStore, ref: SessionRef) -> bool:
    for run in store.list_runs():
        if any(link.worker_id == ref.worker_id and link.session_id == ref.session_id for link in run.sessions):
            return True
    return f"{ref.worker_id}\0{ref.session_id}" in store.archived_worker_sessions()


def _service_error(exc: Exception) -> CockpitError:
    if isinstance(exc, MissingAuthorityError):
        return CockpitError("forbidden", f"missing authority: {', '.join(exc.actions)}", status=403)
    if isinstance(exc, WorkerCapacityError):
        return CockpitError("worker_capacity_exceeded", str(exc) or "all eligible workers are at capacity", recoverable=True, status=409)
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
    return store.archive_cockpit_session(ref.worker_id, ref.session_id)


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
        default_repo=ctx.cfg.orchestration.default_repo,
    )


def _catalog_context(cfg: Config) -> tuple[dict[str, Any], list[str]]:
    registry = WorkerRegistry(cfg.worker, profiles_path=cfg.orchestration.workers_path)
    profiles = registry.profiles(probe=False)
    worker = profiles[0] if profiles else None
    engines: list[str] = []
    for profile in profiles:
        for engine in profile.supported_engines:
            if engine and engine not in engines:
                engines.append(engine)
    defaults = {
        "worker_id": worker.worker_id if worker else "",
        "engine": (worker.default_engine or worker.agent) if worker else "",
        "repo": (worker.default_repo if worker else "") or cfg.orchestration.default_repo,
        "landing_mode": cfg.orchestration.landing_mode,
    }
    return defaults, engines


def _registry_store(cfg: Config) -> RegistryStore:
    return RegistryStore(cfg.registry.path)


def _cockpit_requester_id(cfg: Config) -> str:
    identity = cfg.capabilities.identity.strip()
    return "" if identity == HOUSE else identity


def _visible_projects(cfg: Config, requester_id: str, *, include_archived: bool) -> list[ProjectEntry]:
    return _registry_store(cfg).list_projects(requester_id, include_archived=include_archived)


def _visible_project_or_404(cfg: Config, project_id: str, requester_id: str) -> ProjectEntry | None:
    return _registry_store(cfg).get_visible_project(project_id, requester_id)


def _include_archived(request: web.Request) -> bool:
    value = request.query.get("include_archived")
    if value is not None and value not in {"true", "false", "1", "0"}:
        raise CockpitError("validation_failed", "include_archived must be true or false", recoverable=True, status=400)
    return value in {"true", "1"}


def _sync_mode(request: web.Request) -> str:
    mode = str(request.query.get("sync") or "none")
    return mode if mode in {"none", "fast", "probe"} else "none"


def _worker_probe(request: web.Request) -> bool:
    sync = request.query.get("sync")
    probe = request.query.get("probe")
    if sync not in (None, "", "none", "fast", "probe"):
        raise CockpitError("validation_failed", "sync must be one of none, fast, or probe", recoverable=True, status=400)
    if probe is not None and probe not in {"true", "false", "1", "0"}:
        raise CockpitError("validation_failed", "probe must be true or false", recoverable=True, status=400)
    return sync == "probe" or probe in {"true", "1"}


def _resolve_session_ref(store: OrchestrationStore, session_ref: str) -> SessionRef:
    if not valid_session_ref(session_ref):
        raise CockpitError("not_found", "session not found", status=404)
    record = store.resolve_session_ref(session_ref)
    if record is None:
        _rebuild_session_ref_index_from_store(store)
        record = store.resolve_session_ref(session_ref)
    if record is not None:
        return SessionRef(worker_id=record["worker_id"], session_id=record["session_id"])
    raise CockpitError("not_found", "session not found", status=404)


def _rebuild_session_ref_index_from_store(store: OrchestrationStore) -> None:
    rows: list[dict[str, str]] = []
    for run in store.list_runs():
        for link in run.sessions:
            rows.append({"session_ref": make_session_ref(link.worker_id, link.session_id), "worker_id": link.worker_id, "session_id": link.session_id})
    rows.extend(
        {"session_ref": make_session_ref(str(item.get("worker_id") or ""), str(item.get("session_id") or "")), "worker_id": str(item.get("worker_id") or ""), "session_id": str(item.get("session_id") or "")}
        for item in store.archived_worker_sessions().values()
    )
    store.record_session_refs(rows)


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


def _persist_session_write(store: OrchestrationStore, ref: SessionRef, row: dict[str, Any], events: list[dict[str, Any]]) -> None:
    run_id = str(row.get("run_id") or "")
    if not run_id:
        return
    try:
        store.update_session(
            run_id,
            ref.session_id,
            worker_id=ref.worker_id,
            status=str(row.get("status") or ""),
            provider=str(row.get("provider") or ""),
            engine=str(row.get("engine") or ""),
            branch=str(row.get("branch") or ""),
            last_event_id=_latest_event_id(events),
            allowed_actions=list(row.get("allowed_actions") or []),
        )
    except KeyError:
        return
    existing_event_ids = {
        str(existing.data.get("event_id") or "")
        for existing in store.events(run_id)
        if isinstance(existing.data, dict) and existing.data.get("event_id")
    }
    for event in events:
        event_id = str(event.get("event_id") or "")
        if event_id and event_id in existing_event_ids:
            continue
        store.append_event(
            run_id,
            str(event.get("type") or "session.event"),
            "",
            {
                "session_id": ref.session_id,
                "event_id": event_id,
                "turn_id": str(event.get("turn_id") or ""),
                "message_id": str(event.get("message_id") or ""),
                "data": dict(event.get("data") or {}),
            },
        )
        if event_id:
            existing_event_ids.add(event_id)
    _finalize_session_run_if_terminal(store, run_id)


def _latest_event_id(events: list[dict[str, Any]]) -> str | None:
    for event in reversed(events):
        event_id = str(event.get("event_id") or "")
        if event_id:
            return event_id
    return None


def _finalize_session_run_if_terminal(store: OrchestrationStore, run_id: str) -> None:
    run = store.get(run_id)
    if run is None:
        return
    final = final_session_phase(run)
    if final == "completed":
        store.set_phase(run_id, "completed", "All worker sessions completed")
        return
    if final == "failed":
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
        "type": canonical_event_type(event.type),
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
        raise CockpitError("worker_unavailable", "worker authentication failed", recoverable=True, status=502)
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
        if status < 400:
            raise CockpitError("worker_unavailable", "worker returned invalid JSON", recoverable=True, status=502) from None
    if not isinstance(data, dict):
        if status < 400:
            raise CockpitError("worker_unavailable", "worker returned invalid JSON", recoverable=True, status=502)
        data = {}
    if status >= 400 or (isinstance(data, dict) and data.get("ok") is False):
        if status >= 500:
            raise CockpitError("worker_unavailable", _response_error(response) or "worker write failed", recoverable=True, status=502)
        message = public_error_message(str(data.get("error") or data.get("message") or "")) if isinstance(data, dict) else ""
        message = message or _response_error(response) or "worker write failed"
        code = _worker_error_code(message)
        if status == 404 and code == "checkpoint_not_found":
            raise CockpitError(code, message, recoverable=True, status=409)
        if code == "request_not_pending":
            raise CockpitError(code, message, recoverable=True, status=409)
        if code in {"session_active", "session_terminal", "checkpoint_not_found"}:
            raise CockpitError(code, message, recoverable=True, status=409)
        if status == 401:
            raise CockpitError("worker_unavailable", message or "worker authentication failed", recoverable=True, status=502)
        if status == 403:
            raise CockpitError("forbidden", message, status=403)
        if status == 404:
            raise CockpitError("not_found", message, status=404)
        if status in {400, 422}:
            raise CockpitError("validation_failed", message, recoverable=True, status=400)
        raise CockpitError("worker_unavailable", message, recoverable=True, status=502)
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
        "allowed_actions": _allowed_actions_from_worker_session(raw),
    }


def _overlay_session_row(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    row = dict(base)
    for key, value in overlay.items():
        if key in {"session_ref", "worker_id", "session_id"} or value not in ("", [], {}):
            row[key] = value
    return row


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
        if value not in (None, "", [], {})
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


def _session_run_id_from_store(store: OrchestrationStore, ref: SessionRef) -> str:
    for run in store.list_runs():
        if any(link.worker_id == ref.worker_id and link.session_id == ref.session_id for link in run.sessions):
            return run.run_id
    return ""


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
    if "not pending" in text or ("no pending" in text and "request" in text):
        return "request_not_pending"
    return "provider_unavailable"


async def serve(cfg: Config) -> int:
    bind = cfg.orchestration.api_bind_host or cfg.orchestration.api_host
    token_set = bool(cfg.orchestration.api_token.get_secret_value()) or (
        auth_mode(str(cfg.orchestration.auth_mode)) in {"oauth", "hybrid"}
        and oauth_is_configured(cfg.orchestration)
    )
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
