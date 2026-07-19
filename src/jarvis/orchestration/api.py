from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import socket
import hmac
import json
import logging
import threading
from functools import partial
from pathlib import Path
from dataclasses import dataclass, field, replace
from typing import Any, Awaitable, Callable

import httpx
from aiohttp import web

from jarvis.brain.facade import (
    BrainProjectClient,
    ConclusionRecord,
    CurationOutbox,
    MemoryClient,
    ProjectEntry,
    ProjectOperationError,
    RegistryStore,
    UnsupportedMemoryOperation,
)
from jarvis.capabilities import (
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_RESTORE,
    WORKER_SESSION_STOP,
    WORKER_SESSION_TURN,
)
from jarvis.config import Config, insecure_bind
from jarvis.connectors.cockpit import (
    CockpitConnector,
    CockpitThread,
    CockpitThreadIndex,
    ProviderTurnError,
    THREAD_INDEX_FILENAME,
    execute_orchestrator_tool,
    is_conversation_workspace,
    make_child_terminal_notifier,
    schedule_cold_task_drain,
    workspace_public,
)
from jarvis.ids import new_id, utc_now
from jarvis.mcp.status import (
    MCP_TOKENS_MANAGE_CAPABILITY,
    cockpit_mcp_status,
    cockpit_mcp_tools,
    token_record_public,
)
from jarvis.mcp_server.tokens import MCPTokenError, MCPTokenStore
from jarvis.runtime_info import runtime_info
from jarvis.orchestration.cockpit import (
    API_VERSION,
    MAX_PAGE_LIMIT,
    SCHEMA_VERSION,
    CockpitError,
    IdempotencyStore,
    SessionRef,
    WorkerReadDiagnostic,
    aggregate_checkpoints,
    aggregate_requests,
    aggregate_sessions,
    archived_session_refs_for_store,
    artifact_summaries,
    build_session_row,
    canonical_event_type,
    cockpit_catalog,
    cockpit_snapshot,
    deleted_session_refs_for_store,
    make_session_ref,
    paged,
    project_worker_profile,
    project_worker_system,
    _worktree_inventory as _cockpit_worktree_inventory,
    project_checkpoint,
    project_request,
    project_session_event,
    public_error_message,
    public_event_data,
    _allowed_actions_from_worker_session,
    _ended_reason_from_worker_session,
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
from jarvis.brain.facade import (
    RequestContext,
    Resolution,
    can_admin_project,
    can_edit_project,
    can_query_memory_peer,
    context_for_resolution,
    load_users,
    resolve_capabilities,
)
from jarvis.orchestration.authority import allowed
from jarvis.orchestration.activity import ProjectActivityLog, StaleCursorError
from jarvis.orchestration.intent import parse_work_command
from jarvis.orchestration.orchestrator_grants import (
    OrchestratorGrantError,
    resolve_orchestrator_grant,
)
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
from jarvis.orchestration.store import OrchestrationStore, RunArchivedError
from jarvis.orchestration.supervisor import final_session_phase, sync_run_jobs, sync_run_sessions
from jarvis.orchestration.workers import WorkerRegistry, worker_http_get
from jarvis.worker_session_contract import validate_model
from jarvis.system_info import system_info_cached
from jarvis.users import HOUSE

HttpGet = Callable[..., Any]
HttpPost = Callable[..., Any]
HttpDelete = Callable[..., Any]
CONFIG_KEY = web.AppKey("config", Config)
logger = logging.getLogger(__name__)
SSE_REFRESH_ERROR_LOG_INTERVAL_S = 60.0
MCP_TOKEN_STORE_LOCK = threading.Lock()
THREAD_QUEUE_DRAIN_LOCK = threading.Lock()
THREAD_QUEUE_DRAINS: set[tuple[str, str]] = set()


@dataclass(frozen=True)
class ThreadTurnState:
    operational_state: str
    diagnostic_reason: str = ""
    turn_id: str = ""
    started_at: str = ""


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
    # Live project-conversation operational state, keyed (project_id, thread_id).
    # Process-local state may describe an active attempt, but it never owns or
    # terminates the durable conversation. A restart safely returns open
    # conversations to idle. Tuples remain accepted for v1 compatibility tests.
    thread_turn_states: dict[
        tuple[str, str],
        ThreadTurnState | tuple[str, str],
    ] = field(default_factory=dict)
    # Legacy v1 status remains turn-terminal until the next turn starts, while
    # the durable conversation operational state returns to idle immediately.
    # Kept separate so compatibility state cannot make the conversation itself
    # look terminal or degraded.
    thread_turn_legacy_states: dict[tuple[str, str], tuple[str, str]] = field(default_factory=dict)
    worker_state_cache: dict[str, dict[str, Any]] = field(default_factory=dict)
    worker_state_cache_lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)
    worker_state_refresh_modes: set[str] = field(default_factory=set, repr=False, compare=False)
    delete: HttpDelete = httpx.delete

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
        return _service(
            self.cfg,
            self.source_factory,
            manual_item=manual_item,
            thread_child_terminal_notifier=_make_thread_child_terminal_notifier(self.cfg),
            thread_children_promoter=_make_thread_children_promoter(self.cfg),
        )


@dataclass(frozen=True)
class SseSubscription:
    subscription_id: int
    mode: str
    queue: asyncio.Queue[dict[str, Any] | None]
    snapshot: dict[str, Any]


class _HubWorkerSync:
    """Bound the hub's worker pulls without changing interactive call budgets."""

    def __init__(self, hub: SseSnapshotHub, *, respect_backoff: bool = True) -> None:
        self.hub = hub
        self.respect_backoff = respect_backoff
        registry = WorkerRegistry(hub.ctx.cfg.worker, profiles_path=hub.ctx.cfg.orchestration.workers_path)
        self.profiles = registry.profiles(probe=False)
        self._worker_by_base_url = {
            profile.base_url.rstrip("/"): profile.worker_id
            for profile in self.profiles
            if profile.base_url
        }

    def should_sync(self, profile) -> bool:  # noqa: ANN001
        return not self.respect_backoff or self.hub._worker_backoff_until.get(profile.worker_id, 0) <= self.hub._tick

    def is_backed_off(self, profile) -> bool:  # noqa: ANN001
        return self.respect_backoff and self.hub._worker_backoff_until.get(profile.worker_id, 0) > self.hub._tick

    def online_workers(self) -> frozenset[str]:
        return frozenset(
            profile.worker_id
            for profile in self.profiles
            if profile.status != "offline" and self.should_sync(profile)
        )

    def get(self, url: str, *args: Any, **kwargs: Any) -> Any:
        worker_id = self._worker_id(url)
        try:
            response = self.hub.ctx.get(url, *args, **kwargs)
        except Exception:
            self._failed(worker_id)
            raise
        # A missing optional endpoint is not an offline worker. Back off only
        # transport failures and server-side availability failures.
        if getattr(response, "status_code", 200) >= 500:
            self._failed(worker_id)
        elif worker_id:
            self.hub._worker_backoff_until.pop(worker_id, None)
        return response

    def _worker_id(self, url: str) -> str:
        normalized = url.rstrip("/")
        for base_url, worker_id in self._worker_by_base_url.items():
            if normalized == base_url or normalized.startswith(f"{base_url}/"):
                return worker_id
        return ""

    def _failed(self, worker_id: str) -> None:
        if not worker_id:
            return
        backoff = max(1, int(self.hub.ctx.cfg.orchestration.sse_sync_backoff_ticks))
        self.hub._worker_backoff_until[worker_id] = self.hub._tick + backoff


class SseSnapshotHub:
    def __init__(self, ctx: CockpitAppContext) -> None:
        self.ctx = ctx
        self._subscribers: dict[int, SseSubscription] = {}
        self._snapshots: dict[str, dict[str, Any]] = {}
        self._snapshot_stamps: dict[str, tuple[int, frozenset[str], str]] = {}
        self._worker_states = ctx.worker_state_cache
        self._event_counts: dict[str, dict[str, int]] = {}
        self._worker_backoff_until: dict[str, int] = {}
        self._tick = 0
        self._lock = asyncio.Lock()
        self._next_id = 0
        self._task: asyncio.Task[None] | None = None
        self._stopping = asyncio.Event()
        self._wake = asyncio.Event()
        self._dirty: set[tuple[str, str, str]] = set()
        self._next_notify_sync_at = 0.0
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

    def notify(self, *, worker_id: str, session_id: str = "", job_id: str = "") -> None:
        """Record a cheap hint from an authenticated worker and wake the hub."""
        self._dirty.add((worker_id, session_id, job_id))
        self._wake.set()

    def _take_dirty(self) -> set[tuple[str, str, str]]:
        dirty = self._dirty
        self._dirty = set()
        return dirty

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

    async def seed_snapshot(self, mode: str, body: dict[str, Any]) -> None:
        """Let a REST snapshot establish the baseline for a follow-up SSE stream.

        Do this only while the mode has no active subscribers. Mutating the hub
        baseline underneath live streams would make the next refresh compare
        against the wrong previous snapshot and could suppress deltas.
        """

        async with self._lock:
            if any(existing.mode == mode for existing in self._subscribers.values()):
                return
            self._snapshots[mode] = body
            self._snapshot_stamps[mode] = self._snapshot_stamp()

    async def _snapshot(self, mode: str) -> dict[str, Any]:
        cached = self._snapshots.get(mode)
        body = cached if cached is not None else await _shared_snapshot_body(self.ctx, self, mode)
        if mode not in self._event_counts:
            # Baseline per-run event counts at subscribe time so events that
            # land between now and the first refresh tick are emitted, not
            # silently absorbed into the first baseline.
            counts = await asyncio.to_thread(self._prime_event_counts, body)
            async with self._lock:
                self._event_counts.setdefault(mode, counts)
        async with self._lock:
            snapshot = self._snapshots.setdefault(mode, body)
            self._snapshot_stamps.setdefault(mode, self._snapshot_stamp())
            return snapshot

    async def _run(self) -> None:
        refresh_interval = max(0.1, float(self.ctx.cfg.orchestration.sse_refresh_interval_s))
        heartbeat_interval = max(1.0, float(self.ctx.cfg.orchestration.sse_heartbeat_interval_s))
        heartbeat_at = asyncio.get_running_loop().time() + heartbeat_interval
        while not self._stopping.is_set():
            woke_for_notify = False
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=refresh_interval)
                woke_for_notify = True
            except TimeoutError:
                pass
            self._wake.clear()
            dirty: set[tuple[str, str, str]] = set()
            if woke_for_notify:
                now = asyncio.get_running_loop().time()
                delay = self._next_notify_sync_at - now
                if delay > 0:
                    await asyncio.sleep(delay)
                notify_interval = max(
                    0.0,
                    float(self.ctx.cfg.orchestration.sse_notify_min_interval_s),
                )
                self._next_notify_sync_at = asyncio.get_running_loop().time() + notify_interval
                self._wake.clear()
                dirty = self._take_dirty()
            try:
                modes = await self._active_modes()
                for mode in modes:
                    self._tick += 1
                    worker_sync = _HubWorkerSync(self) if mode != "none" else None
                    run_snapshot = None
                    if worker_sync is not None and dirty:
                        dirty_run_snapshot = await asyncio.to_thread(self.ctx.store.list_runs)
                        sync = await asyncio.to_thread(
                            _hub_sync_dirty,
                            self.ctx,
                            mode,
                            worker_sync,
                            dirty,
                            dirty_run_snapshot,
                        )
                        run_snapshot = await asyncio.to_thread(self.ctx.store.list_runs)
                        dirty_worker_ids = {worker_id for worker_id, _session_id, _job_id in dirty}
                        worker_state = await asyncio.to_thread(
                            _refresh_worker_state,
                            self.ctx,
                            mode,
                            worker_sync,
                            run_snapshot,
                            dirty_worker_ids if mode in self._worker_states else None,
                        )
                    else:
                        sync = (
                            await asyncio.to_thread(
                                _hub_sync_state,
                                self.ctx,
                                mode,
                                worker_sync,
                            )
                            if worker_sync is not None
                            else None
                        )
                        if worker_sync is not None:
                            run_snapshot = await asyncio.to_thread(self.ctx.store.list_runs)
                        worker_state = (
                            await asyncio.to_thread(
                                _refresh_worker_state,
                                self.ctx,
                                mode,
                                worker_sync,
                                run_snapshot,
                            )
                            if worker_sync is not None
                            else None
                        )
                    stamp = self._snapshot_stamp(worker_sync, worker_state)
                    force_refresh = self._tick % max(1, int(self.ctx.cfg.orchestration.sse_forced_refresh_ticks)) == 0
                    if not force_refresh and self._snapshot_stamps.get(mode) == stamp:
                        continue
                    if worker_sync is None:
                        body = await asyncio.to_thread(_cockpit_snapshot, self.ctx, mode)
                    else:
                        body = await asyncio.to_thread(
                            _cockpit_snapshot,
                            self.ctx,
                            mode,
                            sync=sync,
                            sync_timeout_s=float(self.ctx.cfg.orchestration.sse_sync_timeout_s),
                            should_sync_worker=worker_sync.should_sync,
                            http_get=worker_sync.get,
                            worker_state=worker_state,
                            all_runs=run_snapshot,
                        )
                    previous = self._snapshots.get(mode)
                    self._snapshots[mode] = body
                    self._snapshot_stamps[mode] = stamp
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

    def _snapshot_stamp(
        self,
        worker_sync: _HubWorkerSync | None = None,
        worker_state: dict[str, Any] | None = None,
    ) -> tuple[int, frozenset[str], str]:
        online_workers = worker_sync.online_workers() if worker_sync is not None else _online_worker_ids(self.ctx)
        worker_token = snapshot_cursor(worker_state) if worker_state is not None else ""
        return self.ctx.store.generation, online_workers, worker_token

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
                if event.type == "child_terminal":
                    frames.append(
                        {
                            "cursor": cursor,
                            "occurred_at": occurred_at,
                            "type": "run.event",
                            "run_id": run_id,
                            "payload": _project_run_event(event, sequence),
                        }
                    )
                    continue
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
    http_delete: HttpDelete | None = None,
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
        get=http_get or worker_http_get,
        post=http_post or httpx.post,
        delete=http_delete or httpx.delete,
        store=OrchestrationStore(
            cfg.orchestration.workspace,
            thread_child_terminal_notifier=_make_thread_child_terminal_notifier(cfg),
            thread_children_promoter=_make_thread_children_promoter(cfg),
        ),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=source_factory or _work_source,
        oauth_validator=validator,
    )
    _rebuild_session_ref_index_from_store(ctx.store)
    # Attachments travel base64-encoded (~4/3 of the decoded limit) on turn bodies.
    attachment_budget = int(cfg.orchestration.turn_attachment_max_count) * ((int(cfg.orchestration.turn_attachment_max_bytes) * 4) // 3 + 1024)
    app = web.Application(
        middlewares=[_cors_middleware, _error_middleware],
        client_max_size=max(1024 * 1024, int(cfg.registry.max_upload_bytes) + 1024 * 1024, attachment_budget + 1024 * 1024),
    )
    app[CONFIG_KEY] = cfg
    reads = CockpitReadHandlers(ctx)
    writes = CockpitWriteHandlers(ctx)
    sse = SseHandlers(ctx)
    hub = SseSnapshotHub(ctx)
    app[SSE_SNAPSHOT_HUB_KEY] = hub
    app.cleanup_ctx.append(_sse_snapshot_hub_context)

    async def worker_notify(request: web.Request) -> web.Response:
        try:
            body = await request.json()
        except (json.JSONDecodeError, ValueError):
            raise CockpitError("bad_request", "invalid JSON body", status=400) from None
        if not isinstance(body, dict):
            raise CockpitError("bad_request", "notification body must be an object", status=400)
        worker_id = str(body.get("worker_id") or "").strip()
        session_id = str(body.get("session_id") or "").strip()
        job_id = str(body.get("job_id") or "").strip()
        kind = str(body.get("kind") or "").strip()
        if not worker_id or not kind or not (session_id or job_id):
            raise CockpitError("bad_request", "worker_id, kind, and session_id or job_id are required", status=400)
        if len(kind) > 128 or len(session_id) > 256 or len(job_id) > 256:
            raise CockpitError("bad_request", "notification fields are too long", status=400)
        header = request.headers.get("Authorization", "")
        token = header.removeprefix("Bearer ") if header.startswith("Bearer ") else ""
        registry = WorkerRegistry(ctx.cfg.worker, profiles_path=ctx.cfg.orchestration.workers_path)
        profile = await asyncio.to_thread(registry.authenticate_token, token)
        if profile is None:
            raise CockpitError("unauthorized", "unauthorized", status=401)
        if profile.worker_id != worker_id:
            raise CockpitError("forbidden", "worker identity does not match token", status=403)
        hub.notify(worker_id=worker_id, session_id=session_id, job_id=job_id)
        if session_id:
            index = CockpitThreadIndex(Path(ctx.cfg.orchestration.workspace) / THREAD_INDEX_FILENAME)
            for thread in index._threads().values():  # noqa: SLF001 - worker notification re-arms durable queues.
                if (
                    str(thread.workspace.get("worker_id") or thread.worker_id or "") == worker_id
                    and str(thread.workspace.get("session_id") or "") == session_id
                    and thread.queued_turns
                ):
                    _start_thread_queue_drain(ctx, thread.project_id, thread.thread_id)
        return web.json_response({"ok": True, "accepted": True})

    app.add_routes([
        web.get("/v1/auth/metadata", reads.auth_metadata),
        web.get("/v1/runtime", reads.runtime),
        web.get("/v1/health", reads.health),
        web.get("/v1/capabilities", reads.capabilities),
        web.get("/v1/cockpit/catalog", reads.catalog),
        web.get("/v1/cockpit/snapshot", reads.snapshot),
        web.get("/v1/cockpit/events", sse.events),
        web.post("/v1/worker/notify", worker_notify),
        web.get("/v1/mcp/status", reads.mcp_status),
        web.get("/v1/mcp/tools", reads.mcp_tools),
        web.get("/v1/mcp/tokens", reads.mcp_token_list),
        web.post("/v1/mcp/tokens", writes.mcp_token_issue),
        web.post(
            "/v1/orchestrator-tools/{project_id}/{thread_id}/{tool_name}",
            writes.orchestrator_tool,
        ),
        web.delete("/v1/mcp/tokens/{token_id}", writes.mcp_token_revoke),
        web.get("/v1/projects", reads.projects),
        web.post("/v1/projects", writes.project_create),
        web.get("/v1/projects/{project_id}/threads", reads.project_threads),
        web.get("/v1/projects/{project_id}/threads/{thread_id}", reads.project_thread_detail),
        web.get("/v1/projects/{project_id}/files", reads.project_files),
        web.get("/v1/projects/{project_id}/memory", reads.project_memory),
        web.get("/v1/projects/{project_id}/permissions", reads.project_permissions),
        web.get("/v1/projects/{project_id}/activity", reads.project_activity),
        web.get("/v1/projects/{project_id}", reads.project_detail),
        web.get("/v1/workers", reads.workers),
        web.post("/v1/workers/{worker_id}/worktrees/prune", writes.worker_worktrees_prune),
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
        web.delete("/v1/runs/{run_id}", writes.run_delete),
        web.post("/v1/sessions/{session_ref}/archive", writes.session_archive),
        web.post("/v1/sessions/{session_ref}/unarchive", writes.session_unarchive),
        web.delete("/v1/sessions/{session_ref}", writes.session_delete),
        web.post("/v1/sessions/{session_ref}/checkpoints/restore", writes.session_write, name="restore_checkpoint"),
        web.post("/v1/sessions/{session_ref}/{action:turns|input|approval|interrupt|stop}", writes.session_write),
        web.get("/v1/sessions/{session_ref}", reads.session_detail),
        web.patch("/v1/projects/{project_id}", writes.project_update),
        web.patch("/v1/projects/{project_id}/visibility", writes.project_visibility),
        web.post("/v1/projects/{project_id}/members", writes.project_member_add),
        web.delete("/v1/projects/{project_id}/members/{member_id}", writes.project_member_remove),
        web.post("/v1/projects/{project_id}/archive", writes.project_archive),
        web.post("/v1/projects/{project_id}/unarchive", writes.project_unarchive),
        web.delete("/v1/projects/{project_id}", writes.project_delete),
        web.post("/v1/projects/{project_id}/findings", writes.project_finding),
        web.post("/v1/projects/{project_id}/decisions", writes.project_decision),
        web.post("/v1/projects/{project_id}/memory/forget", writes.project_memory_forget),
        web.post("/v1/projects/{project_id}/memory/correct", writes.project_memory_correct),
        web.post("/v1/projects/{project_id}/files", writes.project_file_upload),
        web.delete("/v1/projects/{project_id}/files/{doc_id}", writes.project_file_retract),
        web.post("/v1/projects/{project_id}/threads", writes.project_thread_open),
        web.patch("/v1/projects/{project_id}/threads/{thread_id}", writes.project_thread_rename),
        web.post("/v1/projects/{project_id}/threads/{thread_id}/turns", writes.project_thread_turn),
        web.post(
            "/v1/projects/{project_id}/threads/{thread_id}/{action:input|approval|interrupt}",
            writes.project_thread_control,
        ),
        web.post("/v1/projects/{project_id}/threads/{thread_id}/archive", writes.project_thread_archive),
        web.post("/v1/projects/{project_id}/threads/{thread_id}/unarchive", writes.project_thread_unarchive),
        web.delete("/v1/projects/{project_id}/threads/{thread_id}", writes.project_thread_delete),
        web.post("/v1/projects/{project_id}/threads/{thread_id}/rename", writes.project_thread_rename),
        web.post("/v1/work/start", writes.work_start),
        web.post("/v1/work/resume", writes.work_resume),
        web.post("/v1/work/validate", writes.work_validate),
        web.post("/v1/runs/{run_id}/rename", writes.run_rename),
        web.post("/v1/sessions/{session_ref}/close", writes.session_close),
        web.post("/v1/sessions/{session_ref}/rename", writes.session_rename),
    ])
    _rearm_thread_queues(ctx)
    return app


class CockpitReadHandlers:
    def __init__(self, ctx: CockpitAppContext) -> None:
        self.ctx = ctx

    async def auth_metadata(self, _request: web.Request) -> web.Response:
        return web.json_response(oauth_metadata(self.ctx.cfg.orchestration), headers={"Cache-Control": "no-store"})

    async def runtime(self, _request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "runtime": runtime_info()}, headers={"Cache-Control": "no-store"})

    async def health(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        system = await asyncio.to_thread(system_info_cached)
        return web.json_response(
            {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "runtime": runtime_info(),
                "system": project_worker_system(system),
            }
        )

    async def catalog(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        defaults, engines, engine_supports = await asyncio.to_thread(_catalog_context, self.ctx.cfg)
        return web.json_response(
            cockpit_catalog(
                start_defaults=defaults,
                engines=engines or None,
                engine_supports=engine_supports,
                orchestrator_model=self.ctx.cfg.orchestration.orchestrator_model,
            )
        )

    async def capabilities(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_401(request, self.ctx.cfg)
        auth = request.get("auth", {})
        features = await asyncio.to_thread(_feature_availability, self.ctx.cfg)
        return web.json_response(
            {
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "principal": {
                    "identity": requester.identity,
                    "scope": requester.scope,
                    "auth_mode": str(auth.get("mode") or ""),
                },
                "capabilities": _cockpit_advertised_capabilities(requester, self.ctx.cfg),
                "routes": _route_templates(request.app),
                "features": features,
            }
        )

    async def snapshot(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        mode = _sync_mode(request)
        hub = request.app[SSE_SNAPSHOT_HUB_KEY]
        body = await _shared_snapshot_body(self.ctx, hub, mode, respect_backoff=False)
        await hub.seed_snapshot(mode, body)
        return web.json_response(body)

    async def mcp_status(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        return web.json_response(await asyncio.to_thread(cockpit_mcp_status, self.ctx.cfg))

    async def mcp_tools(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        server = str(request.query.get("server") or "")
        return web.json_response(
            await asyncio.to_thread(cockpit_mcp_tools, self.ctx.cfg, server=server)
        )

    async def mcp_token_list(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        _require_capability(self.ctx.cfg, MCP_TOKENS_MANAGE_CAPABILITY)
        include_revoked = str(request.query.get("include_revoked") or "").lower() in {
            "1",
            "true",
            "yes",
        }
        try:
            records = await asyncio.to_thread(
                MCPTokenStore(self.ctx.cfg.mcp_serve.token_store_path).list,
                include_revoked=include_revoked,
            )
        except MCPTokenError as exc:
            raise CockpitError("internal_error", public_error_message(str(exc)), status=500) from exc
        return web.json_response(
            {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "tokens": [token_record_public(record) for record in records],
            }
        )

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
        registry = await asyncio.to_thread(_registry_store, self.ctx.cfg)
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        include_archived = _include_archived(request)
        projects = (
            await asyncio.to_thread(_visible_projects, registry, requester, include_archived=include_archived)
            if requester is not None
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
        registry = await asyncio.to_thread(_registry_store, self.ctx.cfg)
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        project = (
            await asyncio.to_thread(_visible_project_or_404, registry, request.match_info["project_id"], requester)
            if requester is not None
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

    async def project_permissions(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        registry = await asyncio.to_thread(_registry_store, self.ctx.cfg)
        project = await asyncio.to_thread(
            _visible_project_or_404,
            registry,
            request.match_info["project_id"],
            requester,
        )
        if project is None:
            raise CockpitError("not_found", "project not found", status=404)
        edit = can_edit_project(requester, project)
        admin = can_admin_project(requester, project)
        return web.json_response(
            {
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "project_id": project.id,
                "role": "owner" if admin.allowed else "member" if edit.allowed else "viewer",
                "permissions": {
                    "can_update": edit.allowed,
                    "can_manage_repos": edit.allowed,
                    "can_create_thread": edit.allowed,
                    "can_archive_thread": edit.allowed,
                    "can_archive": admin.allowed,
                    "can_delete": admin.allowed,
                    "can_manage_members": admin.allowed,
                    "can_set_visibility": admin.allowed,
                },
            }
        )

    async def project_memory(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        registry = await asyncio.to_thread(_registry_store, self.ctx.cfg)
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        project = (
            await asyncio.to_thread(_visible_project_or_404, registry, request.match_info["project_id"], requester)
            if requester is not None
            else None
        )
        if project is None:
            raise CockpitError("not_found", "project not found", status=404)
        memory = MemoryClient(self.ctx.cfg.memory)
        representation, conclusions = await asyncio.to_thread(_project_memory, memory, project)
        return web.json_response(
            {
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "project_id": project.id,
                "peer_id": project.peer_id,
                "representation": representation,
                "conclusions": conclusions,
            }
        )

    async def project_activity(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        registry = await asyncio.to_thread(_registry_store, self.ctx.cfg)
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        route_project_id = request.match_info["project_id"]
        registry_project = registry.get_project(route_project_id)
        project = (
            await asyncio.to_thread(_visible_project_or_404, registry, route_project_id, requester)
            if requester is not None
            else None
        )
        if project is None:
            if (
                requester is None
                or registry_project is not None
                or not await asyncio.to_thread(_project_activity_log(self.ctx).visible_after_delete, route_project_id, requester.identity)
            ):
                raise CockpitError("not_found", "project not found", status=404)
            activity_project_id = route_project_id
        else:
            activity_project_id = project.id
        try:
            activity, next_cursor = await asyncio.to_thread(
                _project_activity_log(self.ctx).list,
                activity_project_id,
                limit=_limit(request),
                cursor=str(request.query.get("cursor") or ""),
                activity_type=str(request.query.get("type") or ""),
            )
        except StaleCursorError as exc:
            raise CockpitError("stale_cursor", str(exc), recoverable=True, status=400) from exc
        return web.json_response(
            {
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "project_id": activity_project_id,
                "activity": activity,
                "next_cursor": next_cursor,
            }
        )

    async def project_files(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        result = await _project_brain_write(
            self.ctx,
            requester,
            "project.file.list",
            {
                "project_id": request.match_info["project_id"],
                "include_retracted": str(request.query.get("include_retracted") or "").lower()
                in {"1", "true", "yes"},
            },
        )
        return web.json_response(
            {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                **result,
            }
        )

    async def project_threads(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        registry = await asyncio.to_thread(_registry_store, self.ctx.cfg)
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        include_archived = _include_archived(request)
        project = (
            await asyncio.to_thread(_visible_project_or_404, registry, request.match_info["project_id"], requester)
            if requester is not None
            else None
        )
        if project is None:
            raise CockpitError("not_found", "project not found", status=404)
        connector = _cockpit_connector(self.ctx)
        threads = await asyncio.to_thread(connector.list_threads, project, include_archived=include_archived)
        return web.json_response(
            {
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "project_id": project.id,
                "threads": [_thread_projection(thread, self.ctx) for thread in threads],
            }
        )

    async def project_thread_detail(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        project = await _project_for_member_route(self.ctx, request, requester)
        connector = _cockpit_connector(self.ctx)
        thread = await asyncio.to_thread(connector.index.get_with_messages, project.id, request.match_info["thread_id"])
        if thread is None:
            raise CockpitError("not_found", "thread not found", status=404)
        execution = await asyncio.to_thread(_thread_execution_projection, thread, self.ctx)
        _start_thread_queue_drain(self.ctx, project.id, thread.thread_id)
        return web.json_response(
            {
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "project_id": project.id,
                "thread": _thread_detail_projection(thread, self.ctx, execution=execution),
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
        deleted = await asyncio.to_thread(self.ctx.store.deleted_run, request.match_info["run_id"])
        if deleted is not None:
            row = _deleted_run_row(deleted)
            return web.json_response({"run": row, "summary": row, "deleted": True})
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
            archived_session_refs=archived_session_refs_for_store(self.ctx.store, all_runs) | deleted_session_refs_for_store(self.ctx.store),
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
        deleted = await asyncio.to_thread(self.ctx.store.deleted_worker_session, ref.worker_id, ref.session_id)
        if deleted is not None:
            return web.json_response({"session": _deleted_session_row(ref, deleted), "raw": {}, "deleted": True})
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

    async def project_create(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_401(request, self.ctx.cfg)
        body = await _json_body(request)
        scope = "projects/create"

        async def produce() -> dict[str, Any]:
            result = await _project_brain_write(self.ctx, requester, "project.create", body)
            project_id = _project_id_from_result(result, str(body.get("id") or ""))
            if project_id:
                await _record_project_activity(
                    self.ctx,
                    project_id,
                    "project.created",
                    requester,
                    f"Created project {project_id}",
                    {"project_id": project_id, "name": body.get("name", "")},
                )
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_update(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        body = await _json_body(request)
        body["project_id"] = request.match_info["project_id"]
        scope = f"projects/{body['project_id']}/update"
        op_payload = {key: value for key, value in body.items() if key != "idempotency_key"}

        async def produce() -> dict[str, Any]:
            result = await _project_brain_write(self.ctx, requester, "project.update", op_payload)
            project_id = _project_id_from_result(result, body["project_id"])
            await _record_project_activity(
                self.ctx,
                project_id,
                "project.updated",
                requester,
                f"Updated project {project_id}",
                {
                    "project_id": project_id,
                    "fields": sorted(key for key in body if key not in {"idempotency_key", "project_id"}),
                    "changes": {key: value for key, value in body.items() if key not in {"idempotency_key", "project_id"}},
                },
            )
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_visibility(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        body = await _json_body(request)
        fingerprint_body = {**body, "project_id": request.match_info["project_id"]}
        payload = {
            "project_id": fingerprint_body["project_id"],
            "visibility": fingerprint_body.get("visibility"),
        }
        scope = f"projects/{payload['project_id']}/visibility"

        async def produce() -> dict[str, Any]:
            result = await _project_brain_write(self.ctx, requester, "project.visibility.set", payload)
            project_id = _project_id_from_result(result, str(payload["project_id"]))
            await _record_project_activity(
                self.ctx,
                project_id,
                "project.visibility_changed",
                requester,
                f"Changed project visibility for {project_id}",
                {"project_id": project_id, "visibility": payload.get("visibility")},
            )
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_member_add(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        body = await _json_body(request)
        project = await _project_for_owner_route(self.ctx, request, requester)
        new_members = _members_from_body(body)
        members = _unique_member_list([*project.members, *new_members])
        fingerprint_body = {**body, "project_id": project.id}
        payload = {"project_id": project.id, "members": members}
        scope = f"projects/{project.id}/members/add"

        async def produce() -> dict[str, Any]:
            result = await _project_brain_write(self.ctx, requester, "project.members.set", payload)
            project_id = _project_id_from_result(result, project.id)
            await _record_project_activity(
                self.ctx,
                project_id,
                "project.members_changed",
                requester,
                f"Changed project members for {project_id}",
                {"project_id": project_id, "added": new_members, "members": members},
            )
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_member_remove(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        body = await _optional_json_body(request)
        project = await _project_for_owner_route(self.ctx, request, requester)
        member = request.match_info["member_id"]
        members = [item for item in project.members if item != member]
        fingerprint_body = {**body, "project_id": project.id, "member_id": member}
        payload = {"project_id": project.id, "members": members}
        scope = f"projects/{project.id}/members/remove"

        async def produce() -> dict[str, Any]:
            result = await _project_brain_write(self.ctx, requester, "project.members.set", payload)
            project_id = _project_id_from_result(result, project.id)
            await _record_project_activity(
                self.ctx,
                project_id,
                "project.members_changed",
                requester,
                f"Changed project members for {project_id}",
                {"project_id": project_id, "removed": member, "members": members},
            )
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_archive(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        body = await _optional_json_body(request)
        fingerprint_body = {**body, "project_id": request.match_info["project_id"], "archived": True}
        payload = {"project_id": fingerprint_body["project_id"], "archived": True}
        scope = f"projects/{payload['project_id']}/archive"

        async def produce() -> dict[str, Any]:
            result = await _project_brain_write(self.ctx, requester, "project.archive", payload)
            project_id = _project_id_from_result(result, str(payload["project_id"]))
            await _record_project_activity(self.ctx, project_id, "project.archived", requester, f"Archived project {project_id}", {"project_id": project_id})
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_unarchive(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        body = await _optional_json_body(request)
        fingerprint_body = {**body, "project_id": request.match_info["project_id"], "archived": False}
        payload = {"project_id": fingerprint_body["project_id"], "archived": False}
        scope = f"projects/{payload['project_id']}/unarchive"

        async def produce() -> dict[str, Any]:
            result = await _project_brain_write(self.ctx, requester, "project.archive", payload)
            project_id = _project_id_from_result(result, str(payload["project_id"]))
            await _record_project_activity(self.ctx, project_id, "project.unarchived", requester, f"Unarchived project {project_id}", {"project_id": project_id})
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_delete(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        body = await _optional_json_body(request)
        fingerprint_body = {**body, "project_id": request.match_info["project_id"]}
        payload = {"project_id": fingerprint_body["project_id"]}
        scope = f"projects/{payload['project_id']}/delete"

        async def produce() -> dict[str, Any]:
            visible_to: list[str] = []
            project_for_guard = None
            with contextlib.suppress(Exception):
                registry = await asyncio.to_thread(_registry_store, self.ctx.cfg)
                project = registry.get_project(str(payload["project_id"]))
                if project is not None:
                    project_for_guard = project
                    visible_to = sorted({project.owner, *project.members})
            if project_for_guard is not None:
                threads = await asyncio.to_thread(
                    _cockpit_connector(self.ctx).list_threads,
                    project_for_guard,
                    include_archived=True,
                )
                if threads:
                    raise CockpitError(
                        "project_not_empty",
                        "project has threads; delete or archive child work before deleting the project",
                        recoverable=True,
                        status=409,
                    )
            result = await _project_brain_write(self.ctx, requester, "project.delete", payload)
            project_id = _project_id_from_result(result, str(payload["project_id"]))
            await _record_project_activity(
                self.ctx,
                project_id,
                "project.deleted",
                requester,
                f"Deleted project {project_id}",
                {"project_id": project_id, "visible_to": visible_to},
            )
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_finding(self, request: web.Request) -> web.Response:
        return await self._project_artifact(request, artifact_type="finding")

    async def project_decision(self, request: web.Request) -> web.Response:
        return await self._project_artifact(request, artifact_type="decision")

    async def _project_artifact(self, request: web.Request, *, artifact_type: str) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        body = await _json_body(request)
        project = await _project_for_member_route(self.ctx, request, requester)
        content = str(body.get("content") or body.get(artifact_type) or "").strip()
        if not content:
            raise CockpitError("validation_failed", f"{artifact_type} content is required", status=400, recoverable=True)
        status = str(body.get("status") or ("accepted" if artifact_type == "decision" else "open")).strip()
        fingerprint_body = {**body, "project_id": project.id}
        scope = f"projects/{project.id}/{artifact_type}s"

        async def produce() -> dict[str, Any]:
            queued = _curation_outbox(self.ctx.cfg).enqueue_create(
                observed_id=project.peer_id,
                observer_id=requester.memory_peer,
                content=content,
                metadata={
                    "project_id": project.id,
                    "artifact_type": artifact_type,
                    "recorded_by": requester.memory_peer,
                    "source": "cockpit",
                    "channel": "cockpit",
                    "status": status,
                    "observed_at": str(body.get("observed_at") or utc_now()),
                },
            )
            result = {"project_id": project.id, "content_hash": queued.content_hash}
            await _record_project_activity(
                self.ctx,
                project.id,
                f"{artifact_type}.recorded",
                requester,
                f"Recorded project {artifact_type}",
                {"project_id": project.id, "content_hash": queued.content_hash, "status": status},
            )
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_memory_forget(self, request: web.Request) -> web.Response:
        return await self._project_memory_op(request, op="project.memory.forget")

    async def project_memory_correct(self, request: web.Request) -> web.Response:
        return await self._project_memory_op(request, op="project.memory.correct")

    async def _project_memory_op(self, request: web.Request, *, op: str) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        body = await _json_body(request)
        body["project_id"] = request.match_info["project_id"]
        body["channel"] = "cockpit"
        body["source"] = "cockpit"
        project_id = str(body["project_id"])
        scope = f"projects/{project_id}/memory/{'forget' if op.endswith('forget') else 'correct'}"
        op_payload = {key: value for key, value in body.items() if key != "idempotency_key"}

        async def produce() -> dict[str, Any]:
            result = await _project_brain_write(self.ctx, requester, op, op_payload)
            result_project_id = _project_id_from_result(result, project_id)
            activity_type = "memory.forgotten" if op.endswith("forget") else "memory.corrected"
            await _record_project_activity(
                self.ctx,
                result_project_id,
                activity_type,
                requester,
                "Forgot project memory" if op.endswith("forget") else "Corrected project memory",
                {"project_id": result_project_id, "conclusion_ids": body.get("conclusion_ids", [])},
            )
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_file_upload(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        payload = await _multipart_upload_payload(request)
        payload["project_id"] = request.match_info["project_id"]
        payload["channel"] = "cockpit"
        idempotency_key = str(request.headers.get("X-Idempotency-Key") or "")
        fingerprint_body = {**payload, "idempotency_key": idempotency_key}
        scope = f"projects/{payload['project_id']}/files/upload"

        async def produce() -> dict[str, Any]:
            result = await _project_brain_write(self.ctx, requester, "project.file.upload", payload)
            project_id = _project_id_from_result(result, str(payload["project_id"]))
            await _record_project_activity(
                self.ctx,
                project_id,
                "file.uploaded",
                requester,
                f"Uploaded file to project {project_id}",
                {"project_id": project_id, "doc_id": result.get("doc_id", ""), "filename": payload.get("filename", "")},
            )
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, idempotency_key, fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_file_retract(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        body = await _optional_json_body(request)
        fingerprint_body = {**body, "project_id": request.match_info["project_id"], "doc_id": request.match_info["doc_id"]}
        payload = {"project_id": fingerprint_body["project_id"], "doc_id": fingerprint_body["doc_id"]}
        scope = f"projects/{payload['project_id']}/files/retract"

        async def produce() -> dict[str, Any]:
            result = await _project_brain_write(self.ctx, requester, "project.file.retract", payload)
            project_id = _project_id_from_result(result, str(payload["project_id"]))
            await _record_project_activity(
                self.ctx,
                project_id,
                "file.retracted",
                requester,
                f"Retracted file from project {project_id}",
                {"project_id": project_id, "doc_id": payload["doc_id"]},
            )
            return _project_write_body(result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_thread_open(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        registry = await asyncio.to_thread(_registry_store, self.ctx.cfg)
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        project = (
            await asyncio.to_thread(_visible_project_or_404, registry, request.match_info["project_id"], requester)
            if requester is not None
            else None
        )
        if project is None or requester is None:
            raise CockpitError("not_found", "project not found", status=404)
        fingerprint_body = {**body, "project_id": project.id}
        scope = f"projects/{project.id}/threads/open"

        async def produce() -> dict[str, Any]:
            try:
                thread = await _cockpit_connector(self.ctx).open_thread(
                    project,
                    requester,
                    title=str(body.get("title") or ""),
                    parent_chat_id=str(body.get("parent_chat_id") or ""),
                    chat_type=str(body.get("chat_type") or "assistant"),
                    engine=str(body.get("engine") or ""),
                    model=str(body.get("model") or ""),
                    worker_id=str(body.get("worker_id") or ""),
                )
            except UnsupportedMemoryOperation as exc:
                raise CockpitError("memory_unavailable", str(exc), recoverable=True, status=503) from exc
            except ValueError as exc:
                raise CockpitError("validation_failed", public_error_message(str(exc)), recoverable=True, status=400) from exc
            except (TimeoutError, OSError, RuntimeError) as exc:
                raise CockpitError("memory_unavailable", public_error_message(str(exc)), recoverable=True, status=503) from exc
            await _record_project_activity(
                self.ctx,
                project.id,
                "thread.opened",
                requester,
                f"Opened project thread {thread.thread_id}",
                {"project_id": project.id, "thread_id": thread.thread_id},
            )
            return _project_write_body({"project_id": project.id, "thread": _thread_projection(thread, self.ctx)})

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def orchestrator_tool(self, request: web.Request) -> web.Response:
        header = request.headers.get("Authorization", "")
        token = header[len("Bearer ") :] if header.startswith("Bearer ") else ""
        try:
            grant = resolve_orchestrator_grant(self.ctx.cfg.orchestration, token)
        except OrchestratorGrantError as exc:
            raise CockpitError("unauthorized", str(exc), status=401) from exc
        project_id = str(request.match_info["project_id"])
        thread_id = str(request.match_info["thread_id"])
        tool_name = str(request.match_info["tool_name"])
        if grant.project_id != project_id or grant.thread_id != thread_id or tool_name not in grant.tools:
            raise CockpitError("forbidden", "orchestrator grant does not cover this tool", status=403)
        registry = await asyncio.to_thread(_registry_store, self.ctx.cfg)
        project = await asyncio.to_thread(
            _visible_project_or_404,
            registry,
            project_id,
            grant.requester,
        )
        if project is None:
            raise CockpitError("not_found", "project not found", status=404)
        connector = _cockpit_connector(self.ctx)
        thread = await asyncio.to_thread(connector.index.get, project_id, thread_id)
        if thread is None or thread.chat_type != "orchestrator":
            raise CockpitError("not_found", "orchestrator thread not found", status=404)
        if thread.archived_at:
            raise CockpitError("thread_archived", "thread is archived", recoverable=True, status=409)
        body = await _json_body(request)
        try:
            result = await execute_orchestrator_tool(
                self.ctx.cfg,
                project=project,
                thread=thread,
                requester=grant.requester,
                tool_name=tool_name,
                args=body,
            )
        except PermissionError as exc:
            raise CockpitError("forbidden", public_error_message(str(exc)), status=403) from exc
        except (RuntimeError, ValueError) as exc:
            raise CockpitError("tool_failed", public_error_message(str(exc)), recoverable=True, status=400) from exc
        return web.json_response({"ok": True, "result": result})

    async def project_thread_turn(self, request: web.Request) -> web.StreamResponse:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        attachments = _validate_attachments(body, self.ctx.cfg.orchestration)
        text = str(body.get("text") or body.get("message") or body.get("prompt") or "").strip()
        if not text:
            raise CockpitError("validation_failed", "turn text is required", recoverable=True, status=400)
        idempotency_key = str(body.get("idempotency_key") or "").strip()
        if not idempotency_key:
            raise CockpitError(
                "validation_failed",
                "idempotency_key is required",
                recoverable=True,
                status=400,
            )
        registry = await asyncio.to_thread(_registry_store, self.ctx.cfg)
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        project = (
            await asyncio.to_thread(_visible_project_or_404, registry, request.match_info["project_id"], requester)
            if requester is not None
            else None
        )
        if project is None or requester is None:
            raise CockpitError("not_found", "project not found", status=404)
        connector = _cockpit_connector(self.ctx)
        thread = await asyncio.to_thread(connector.index.get, project.id, request.match_info["thread_id"])
        if thread is None:
            raise CockpitError("not_found", "thread not found", status=404)
        if thread.archived_at:
            raise CockpitError("thread_archived", "thread is archived", recoverable=True, status=409)
        workspace_request = dict(body["workspace"]) if isinstance(body.get("workspace"), dict) else None
        requested_engine = str((workspace_request or {}).get("engine") or "").strip().lower()
        if requested_engine and thread.chat_type == "orchestrator" and requested_engine not in {"codex", "claude"}:
            raise CockpitError(
                "validation_failed",
                "orchestrator engine must be codex or claude",
                recoverable=True,
                status=400,
            )
        # A turn may carry a model for the engine it is about to run on; empty or
        # absent keeps the thread's current one. It rides in `workspace` so the
        # connector sees engine and model together and can respawn once.
        requested_model = str(body.get("model") or (workspace_request or {}).get("model") or "").strip()
        if requested_model:
            _, _, engine_supports = await asyncio.to_thread(_catalog_context, self.ctx.cfg)
            target_engine = requested_engine or thread.engine
            try:
                validate_model(requested_model, engine_supports.get(target_engine, {}).get("models"), target_engine)
            except ValueError as exc:
                raise CockpitError("validation_failed", str(exc), recoverable=True, status=400) from exc
            workspace_request = {**(workspace_request or {}), "model": requested_model}
        if idempotency_key:
            try:
                receipt = await asyncio.to_thread(
                    connector.index.foreground_turn_receipt,
                    project.id,
                    thread.thread_id,
                    text=text,
                    idempotency_key=idempotency_key,
                    workspace_request=workspace_request,
                    attachments=attachments or None,
                )
            except ValueError as exc:
                raise CockpitError("idempotency_conflict", str(exc), recoverable=True, status=409) from exc
            if receipt is not None:
                return await _write_thread_turn_receipt_response(request, self.ctx, thread, receipt)
        execution = await asyncio.to_thread(_thread_execution_projection, thread, self.ctx)
        if _thread_execution_is_active(execution):
            if attachments:
                raise CockpitError(
                    "queue_attachments_unsupported",
                    "attachments cannot be queued until durable attachment references are available",
                    recoverable=True,
                    status=409,
                )
            try:
                queued_thread, queued_turn, created = await asyncio.to_thread(
                    connector.index.enqueue_turn,
                    project.id,
                    thread.thread_id,
                    requester=requester,
                    text=text,
                    idempotency_key=idempotency_key,
                    workspace_request=workspace_request,
                    attachments=attachments or None,
                )
            except ValueError as exc:
                raise CockpitError("idempotency_conflict", str(exc), recoverable=True, status=409) from exc
            except OverflowError as exc:
                raise CockpitError("queue_full", str(exc), recoverable=True, status=409) from exc
            response = web.StreamResponse(
                status=200,
                headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"},
            )
            _apply_cors_headers(request, response)
            await response.prepare(request)
            cursor = new_id("threadturn")
            payload = {
                "project_id": project.id,
                "thread_id": thread.thread_id,
                "queue_id": str(queued_turn.get("queue_id") or ""),
                "idempotent": not created,
                "thread": _thread_projection(queued_thread, self.ctx, include_messages=True),
            }
            await _write_sse(
                response,
                "thread.turn.queued",
                cursor,
                _sse_envelope(cursor, "thread.turn.queued", payload),
            )
            await _write_sse(
                response,
                "thread.turn.done",
                cursor,
                _sse_envelope(cursor, "thread.turn.done", payload),
            )
            await response.write_eof()
            _start_thread_queue_drain(self.ctx, project.id, thread.thread_id)
            return response
        foreground_receipt: dict[str, Any] | None = None
        if idempotency_key:
            try:
                foreground_receipt, should_dispatch = await asyncio.to_thread(
                    connector.index.reserve_foreground_turn,
                    project.id,
                    thread.thread_id,
                    text=text,
                    idempotency_key=idempotency_key,
                    requester=requester,
                    dispatch_mode=(
                        "worker"
                        if thread.chat_type == "orchestrator" or bool(thread.workspace) or workspace_request is not None
                        else "brain"
                    ),
                    workspace_request=workspace_request,
                    attachments=attachments or None,
                )
            except ValueError as exc:
                raise CockpitError("idempotency_conflict", str(exc), recoverable=True, status=409) from exc
            if not should_dispatch:
                return await _write_thread_turn_receipt_response(
                    request,
                    self.ctx,
                    thread,
                    foreground_receipt,
                )
        response = web.StreamResponse(status=200, headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
        _apply_cors_headers(request, response)
        await response.prepare(request)
        cursor = new_id("threadturn")
        turn_id = str((foreground_receipt or {}).get("logical_turn_id") or new_id("turn"))
        turn_started_at = utc_now()
        await _write_sse(
            response,
            "thread.turn.started",
            cursor,
            _sse_envelope(
                cursor,
                "thread.turn.started",
                {
                    "project_id": project.id,
                    "thread_id": thread.thread_id,
                    "turn_id": turn_id,
                },
            ),
        )
        state_key = (project.id, thread.thread_id)
        self.ctx.thread_turn_states[state_key] = ThreadTurnState(
            operational_state="working",
            turn_id=turn_id,
            started_at=turn_started_at,
        )
        cold_sessions = []
        client_gone = False

        async def progress(update: dict[str, Any]) -> None:
            nonlocal client_gone
            if client_gone:
                return
            if update.get("type") != "text.delta":
                return
            delta = str(update.get("delta") or "")
            if not delta:
                return
            try:
                await _write_sse(
                    response,
                    "thread.delta",
                    cursor,
                    _sse_envelope(
                        cursor,
                        "thread.delta",
                        {
                            "project_id": project.id,
                            "thread_id": thread.thread_id,
                            "delta": delta,
                        },
                    ),
                )
            except (ConnectionResetError, OSError):
                client_gone = True

        def defer_cold_tasks(session) -> None:  # noqa: ANN001 - BrainSession stays behind the facade.
            cold_sessions.append(session)

        try:
            reply, updated, events = await connector.turn(
                project,
                thread,
                requester,
                text,
                attachments=attachments or None,
                workspace_request=workspace_request,
                progress=progress,
                cold_task_sink=defer_cold_tasks,
                logical_turn_id=str((foreground_receipt or {}).get("logical_turn_id") or turn_id),
                idempotency_key=idempotency_key,
            )
            if idempotency_key:
                await asyncio.to_thread(
                    connector.index.finish_foreground_turn,
                    project.id,
                    thread.thread_id,
                    idempotency_key,
                    status="accepted" if reply == "Workspace turn is running." else "completed",
                    reply=reply,
                )
        except (UnsupportedMemoryOperation, TimeoutError, OSError, RuntimeError) as exc:
            if idempotency_key:
                await asyncio.to_thread(
                    connector.index.fail_foreground_turn,
                    project.id,
                    thread.thread_id,
                    idempotency_key,
                )
            if _thread_turn_error_is_active(exc) and idempotency_key:
                await asyncio.to_thread(connector.index.release_execution_turn, project.id, thread.thread_id)
                if attachments:
                    return await _write_thread_turn_error(
                        response,
                        cursor,
                        project_id=project.id,
                        thread_id=thread.thread_id,
                        code="queue_attachments_unsupported",
                        message="attachments cannot be queued until durable attachment references are available",
                        recoverable=True,
                    )
                try:
                    queued_thread, queued_turn, created = await asyncio.to_thread(
                        connector.index.enqueue_turn,
                        project.id,
                        thread.thread_id,
                        requester=requester,
                        text=text,
                        idempotency_key=idempotency_key,
                        workspace_request=workspace_request,
                        attachments=attachments or None,
                    )
                except ValueError as conflict:
                    return await _write_thread_turn_error(
                        response,
                        cursor,
                        project_id=project.id,
                        thread_id=thread.thread_id,
                        code="idempotency_conflict",
                        message=str(conflict),
                        recoverable=True,
                    )
                payload = {
                    "project_id": project.id,
                    "thread_id": thread.thread_id,
                    "queue_id": str(queued_turn.get("queue_id") or ""),
                    "idempotent": not created,
                    "thread": _thread_projection(queued_thread, self.ctx, include_messages=True),
                }
                await _write_sse(
                    response,
                    "thread.turn.queued",
                    cursor,
                    _sse_envelope(cursor, "thread.turn.queued", payload),
                )
                await _write_sse(
                    response,
                    "thread.turn.done",
                    cursor,
                    _sse_envelope(cursor, "thread.turn.done", payload),
                )
                await response.write_eof()
                self.ctx.thread_turn_states.pop(state_key, None)
                self.ctx.thread_turn_legacy_states.pop(state_key, None)
                _start_thread_queue_drain(self.ctx, project.id, thread.thread_id)
                return response
            self.ctx.thread_turn_states[state_key] = ("degraded", "engine_error")
            try:
                return await _write_thread_turn_error(
                    response,
                    cursor,
                    project_id=project.id,
                    thread_id=thread.thread_id,
                    code="engine_error" if isinstance(exc, ProviderTurnError) else "memory_unavailable",
                    message=public_error_message(str(exc)),
                    recoverable=True,
                )
            finally:
                self.ctx.thread_turn_legacy_states[state_key] = ("failed", "engine_error")
                self.ctx.thread_turn_states.pop(state_key, None)
        except Exception as exc:  # noqa: BLE001 - preserve SSE contract after prepare.
            if idempotency_key:
                await asyncio.to_thread(
                    connector.index.fail_foreground_turn,
                    project.id,
                    thread.thread_id,
                    idempotency_key,
                )
            self.ctx.thread_turn_states[state_key] = ("degraded", "engine_error")
            try:
                return await _write_thread_turn_error(
                    response,
                    cursor,
                    project_id=project.id,
                    thread_id=thread.thread_id,
                    code="internal_error",
                    message=public_error_message(str(exc)),
                    recoverable=False,
                )
            finally:
                self.ctx.thread_turn_legacy_states[state_key] = ("failed", "engine_error")
                self.ctx.thread_turn_states.pop(state_key, None)
        self.ctx.thread_turn_states.pop(state_key, None)
        self.ctx.thread_turn_legacy_states.pop(state_key, None)
        payload = {
            "project_id": project.id,
            "thread": _thread_projection(updated, self.ctx, include_messages=True),
            "reply": reply,
        }
        try:
            if not client_gone:
                for event in events:
                    await _try_write_thread_event(response, cursor, event)
                await _write_sse(response, "thread.reply", cursor, _sse_envelope(cursor, "thread.reply", payload))
                await _write_sse(response, "thread.turn.done", cursor, _sse_envelope(cursor, "thread.turn.done", payload))
                await response.write_eof()
        except (ConnectionResetError, OSError):
            client_gone = True
        finally:
            for session in cold_sessions:
                schedule_cold_task_drain(session)
        _start_thread_queue_drain(self.ctx, project.id, thread.thread_id)
        return response

    async def project_thread_control(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        project = await _project_for_member_route(self.ctx, request, requester)
        connector = _cockpit_connector(self.ctx)
        thread_id = request.match_info["thread_id"]
        thread = await asyncio.to_thread(connector.index.get, project.id, thread_id)
        if thread is None:
            raise CockpitError("not_found", "thread not found", status=404)
        if thread.archived_at:
            raise CockpitError("thread_archived", "thread is archived", recoverable=True, status=409)
        worker_id = str(thread.workspace.get("worker_id") or thread.worker_id or "")
        session_id = str(thread.workspace.get("session_id") or "")
        if not worker_id or not session_id:
            raise CockpitError(
                "execution_unavailable",
                "thread has no attached worker execution",
                recoverable=True,
                status=409,
            )
        action = request.match_info["action"]
        required = _required_session_action(action)
        reference_key = "turn_id" if action == "interrupt" else "request_id"
        reference_id = str(body.get(reference_key) or "").strip()
        if not reference_id:
            raise CockpitError(
                "validation_failed",
                f"{reference_key} is required",
                recoverable=True,
                status=400,
            )
        scope = f"projects/{project.id}/threads/{thread_id}/{action}"
        fingerprint_body = {**body, "project_id": project.id, "thread_id": thread_id, "action": action}

        async def produce() -> dict[str, Any]:
            _require_capability(self.ctx.cfg, required)
            control_thread = thread
            if action == "interrupt":
                claimed = await asyncio.to_thread(
                    connector.claim_execution_interrupt,
                    project,
                    thread_id,
                )
                if claimed is None:
                    raise CockpitError("not_found", "thread not found", status=404)
                control_thread = claimed
            control_worker_id = str(
                control_thread.workspace.get("worker_id") or control_thread.worker_id or ""
            )
            control_session_id = str(control_thread.workspace.get("session_id") or "")
            if not control_worker_id or not control_session_id:
                raise CockpitError(
                    "execution_unavailable",
                    "thread has no attached worker execution",
                    recoverable=True,
                    status=409,
                )
            expected_generation = int(control_thread.workspace.get("session_generation") or 0)
            await asyncio.to_thread(
                _worker_post_json,
                self.ctx.cfg,
                control_worker_id,
                f"/sessions/{control_session_id}/{action}",
                _worker_control_body(body, required),
                post=self.ctx.post,
            )
            control = {"action": action, "accepted": True, reference_key: reference_id}
            execution = _thread_execution_projection(control_thread, self.ctx)
            if action == "interrupt":
                detached = await asyncio.to_thread(
                    connector.detach_interrupted_execution,
                    project,
                    thread_id,
                    expected_session_id=control_session_id,
                    expected_generation=expected_generation,
                )
                if detached is not None:
                    control_thread = detached
            return {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "project_id": project.id,
                "thread_id": thread_id,
                "control": control,
                "execution": execution,
            }

        response_body = await _idempotent_write_body(
            self.ctx,
            scope,
            str(body.get("idempotency_key") or ""),
            fingerprint_body,
            produce,
            requester=requester,
        )
        return web.json_response(response_body)

    async def project_thread_rename(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        title = " ".join(str(body.get("title") or body.get("name") or "").split())
        if not title:
            raise CockpitError("validation_failed", "title is required", recoverable=True, status=400)
        requester = _requester_or_404(request, self.ctx.cfg)
        project = await _project_for_member_route(self.ctx, request, requester)
        connector = _cockpit_connector(self.ctx)
        thread_id = request.match_info["thread_id"]
        scope = f"project_thread_rename/{project.id}/{thread_id}"
        fingerprint_body = {**body, "project_id": project.id, "thread_id": thread_id, "title": title}

        async def produce() -> dict[str, Any]:
            thread = await asyncio.to_thread(connector.rename_thread, project, thread_id, title)
            if thread is None:
                raise CockpitError("not_found", "thread not found", status=404)
            await _record_project_activity(
                self.ctx,
                project.id,
                "thread.renamed",
                requester,
                f"Renamed project thread {thread_id}",
                {"project_id": project.id, "thread_id": thread_id, "title": thread.title},
            )
            return {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "project_id": project.id,
                "thread": _thread_projection(thread, self.ctx),
            }

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_thread_archive(self, request: web.Request) -> web.Response:
        return await self._project_thread_archive(request, archived=True)

    async def project_thread_unarchive(self, request: web.Request) -> web.Response:
        return await self._project_thread_archive(request, archived=False)

    async def _project_thread_archive(self, request: web.Request, *, archived: bool) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        project = await _project_for_member_route(self.ctx, request, requester)
        connector = _cockpit_connector(self.ctx)
        thread_id = request.match_info["thread_id"]
        scope_name = "project_thread_archive" if archived else "project_thread_unarchive"
        scope = f"{scope_name}/{project.id}/{thread_id}"
        fingerprint_body = {**body, "project_id": project.id, "thread_id": thread_id, "archived": archived}

        async def produce() -> dict[str, Any]:
            existing = await asyncio.to_thread(connector.index.get, project.id, thread_id)
            if existing is None:
                raise CockpitError("not_found", "thread not found", status=404)
            if archived:
                thread = await asyncio.to_thread(
                    connector.archive_thread,
                    project,
                    thread_id,
                    by=requester.memory_peer,
                    reason=str(body.get("reason") or ""),
                )
                await asyncio.to_thread(self.ctx.store.promote_children, thread_id)
            else:
                thread = await asyncio.to_thread(connector.unarchive_thread, project, thread_id)
            if thread is None:
                raise CockpitError("not_found", "thread not found", status=404)
            activity_type = "thread.archived" if archived else "thread.unarchived"
            await _record_project_activity(
                self.ctx,
                project.id,
                activity_type,
                requester,
                f"{'Archived' if archived else 'Unarchived'} project thread {thread_id}",
                {"project_id": project.id, "thread_id": thread_id},
            )
            return {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "project_id": project.id,
                "thread": _thread_projection(thread, self.ctx),
            }

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def project_thread_delete(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _optional_json_body(request)
        requester = _requester_or_404(request, self.ctx.cfg)
        project = await _project_for_member_route(self.ctx, request, requester)
        thread_id = request.match_info["thread_id"]
        scope = f"projects/{project.id}/threads/{thread_id}/delete"
        fingerprint_body = {**body, "project_id": project.id, "thread_id": thread_id}

        async def produce() -> dict[str, Any]:
            connector = _cockpit_connector(self.ctx)
            existing = await asyncio.to_thread(connector.index.get, project.id, thread_id)
            memory_deleted = False
            notes: list[str] = []
            if existing is not None:
                try:
                    await asyncio.to_thread(MemoryClient(self.ctx.cfg.memory).delete_session, existing.session_id)
                    memory_deleted = True
                except UnsupportedMemoryOperation as exc:
                    notes.append(str(exc))
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        notes.append("memory session already absent")
                    else:
                        raise CockpitError("memory_unavailable", public_error_message(str(exc)), recoverable=True, status=503) from exc
                except Exception as exc:
                    raise CockpitError("memory_unavailable", public_error_message(str(exc)), recoverable=True, status=503) from exc
            thread, deleted = await asyncio.to_thread(connector.delete_thread, project, thread_id)
            if thread is None:
                raise CockpitError("not_found", "thread not found", status=404)
            if deleted:
                # R3: removing a node reparents its children up — promote child
                # runs and threads to root, same as archive; never orphan them.
                await asyncio.to_thread(self.ctx.store.promote_children, thread_id)
                await _record_project_activity(
                    self.ctx,
                    project.id,
                    "thread.deleted",
                    requester,
                    f"Deleted project thread {thread_id}",
                    {"project_id": project.id, "thread_id": thread_id},
                )
            summary = _reclamation_summary(
                records=1 if deleted else 0,
                events=len(thread.messages) if deleted else 0,
                memory_sessions=1 if memory_deleted else 0,
                notes=notes,
            )
            return {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "deleted": deleted,
                "project_id": project.id,
                "thread_id": thread_id,
                "reclamation": summary,
            }

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), fingerprint_body, produce, requester=requester)
        return web.json_response(response_body)

    async def work_start(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        attachments = _validate_attachments(body, self.ctx.cfg.orchestration)
        cacheable_error: dict[str, Any] | None = None

        async def produce() -> dict[str, Any]:
            command, manual_item = _command_from_body(body, start=True)
            service = self.ctx.service(manual_item=manual_item)
            try:
                next_work = partial(service.next_work, command, start=True)
                if attachments:
                    next_work = partial(next_work, attachments=attachments)
                parent_chat_id = str(body.get("parent_chat_id") or "")
                if parent_chat_id:
                    next_work = partial(next_work, parent_chat_id=parent_chat_id)
                result = await asyncio.to_thread(next_work)
            except (MissingAuthorityError, NoEligibleWorkerError, WorkAlreadyOwnedError, MissingWorkRepoError, WorkerDispatchError) as exc:
                nonlocal cacheable_error
                error = _service_error(exc)
                if isinstance(exc, (MissingWorkRepoError, WorkerDispatchError)):
                    cacheable_error = error.body()
                raise error from exc
            if result is None or not isinstance(result, StartedWork):
                raise CockpitError("not_found", "no eligible work item found", recoverable=True, status=404)
            response_body = _started_work_packet(self.ctx.store, result)
            await _record_work_dispatched_activity(self.ctx, request, body, result)
            return response_body

        response_body = await _idempotent_write_body(
            self.ctx,
            "work/start",
            str(body.get("idempotency_key") or ""),
            body,
            produce,
            cache_error_body=lambda _exc: cacheable_error,
        )
        return web.json_response(response_body)

    async def work_validate(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        _validate_attachments(body, self.ctx.cfg.orchestration)
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

    async def mcp_token_issue(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        _require_capability(self.ctx.cfg, MCP_TOKENS_MANAGE_CAPABILITY)
        body = await _json_body(request)
        scope = "mcp/tokens"
        key = str(body.get("idempotency_key") or "")
        issued_token_id: str | None = None

        async def produce() -> dict[str, Any]:
            nonlocal issued_token_id
            principal = str(body.get("principal") or "").strip()
            name = str(body.get("name") or "").strip()
            if not principal:
                raise CockpitError("validation_failed", "principal is required", status=400)
            if principal not in load_users(self.ctx.cfg.capabilities.users_dir):
                raise CockpitError("validation_failed", "unknown principal", status=400)
            try:
                token, record = await asyncio.to_thread(
                    _mcp_token_add,
                    self.ctx.cfg.mcp_serve.token_store_path,
                    principal=principal,
                    name=name,
                )
            except MCPTokenError as exc:
                raise CockpitError("internal_error", public_error_message(str(exc)), status=500) from exc
            logger.info(
                "mcp token issued token_id=%s principal=%s name=%s",
                record.token_id,
                record.principal,
                record.name,
            )
            issued_token_id = record.token_id
            return {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "token": token,
                "record": token_record_public(record),
            }

        def cache_response_body(response_body: dict[str, Any]) -> dict[str, Any]:
            idempotent_body = dict(response_body)
            idempotent_body["token"] = ""
            return idempotent_body

        async def revoke_after_save_error(exc: Exception) -> None:
            if issued_token_id:
                await asyncio.to_thread(
                    _mcp_token_revoke_after_failed_issue,
                    self.ctx.cfg.mcp_serve.token_store_path,
                    issued_token_id,
                )
            raise CockpitError(
                "internal_error",
                public_error_message(str(exc) or "idempotency save failed"),
                status=500,
            ) from exc

        response_body = await _idempotent_write_body(
            self.ctx,
            scope,
            key,
            body,
            produce,
            cache_response_body=cache_response_body,
            on_save_error=revoke_after_save_error,
        )
        return web.json_response(response_body)

    async def mcp_token_revoke(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        _require_capability(self.ctx.cfg, MCP_TOKENS_MANAGE_CAPABILITY)
        token_id = request.match_info["token_id"]
        try:
            record = await asyncio.to_thread(
                _mcp_token_revoke,
                self.ctx.cfg.mcp_serve.token_store_path,
                token_id,
            )
        except MCPTokenError as exc:
            message = str(exc)
            status = 409 if "ambiguous" in message.lower() else 404
            code = "conflict" if status == 409 else "not_found"
            raise CockpitError(code, public_error_message(message), status=status) from exc
        logger.info(
            "mcp token revoked token_id=%s principal=%s",
            record.token_id,
            record.principal,
        )
        return web.json_response(
            {
                "ok": True,
                "api_version": API_VERSION,
                "schema_version": SCHEMA_VERSION,
                "record": token_record_public(record),
            }
        )

    async def work_resume(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        scope = "work/resume"

        async def produce() -> dict[str, Any]:
            service = self.ctx.service()
            try:
                result = await asyncio.to_thread(service.resume_run, str(body.get("run_id") or "latest"), prompt=str(body.get("prompt") or ""))
            except (MissingAuthorityError, NoEligibleWorkerError, ResumeRunError, WorkerDispatchError) as exc:
                raise _service_error(exc) from exc
            return _started_work_packet(self.ctx.store, result)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce)
        return web.json_response(response_body)

    async def worker_worktrees_prune(self, request: web.Request) -> web.Response:
        """Sweep a worker's stale worktrees.

        The cockpit's "Prune stale worktrees" action had no endpoint on this
        tier at all, so it could only ever be a no-op — /v1/workers was
        read-only. This proxies the worker's own /worktrees/prune, which is the
        surface that knows the workspace-relative worktrees root.
        """
        await self.ctx.require_auth(request)
        body = await _optional_json_body(request)
        worker_id = request.match_info["worker_id"]
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        scope = f"workers/{worker_id}/worktrees/prune"

        async def produce() -> dict[str, Any]:
            # Same capability as run/session deletion — this reclaims the same
            # worktrees by another route, so it needs no new grant on the fleet.
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            return await asyncio.to_thread(_prune_worker_worktrees_packet, self.ctx, worker_id, body)

        response_body = await _idempotent_write_body(
            self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester
        )
        return web.json_response(response_body)

    async def run_archive(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        run_id = request.match_info["run_id"]
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        scope = f"runs/{run_id}/archive"

        async def produce() -> dict[str, Any]:
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            run = await asyncio.to_thread(_archive_run, self.ctx.store, run_id)
            return _archive_run_packet(run)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def run_delete(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _optional_json_body(request)
        run_id = request.match_info["run_id"]
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        scope = f"runs/{run_id}/delete"

        async def produce() -> dict[str, Any]:
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            return await asyncio.to_thread(_delete_run_packet, self.ctx, run_id)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def run_rename(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        run_id = request.match_info["run_id"]
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        scope = f"runs/{run_id}/rename"

        async def produce() -> dict[str, Any]:
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            try:
                run = await asyncio.to_thread(
                    self.ctx.store.rename_run,
                    run_id,
                    str(body.get("title") or body.get("name") or ""),
                )
            except KeyError as exc:
                raise CockpitError("not_found", "run not found", status=404) from exc
            except ValueError as exc:
                raise CockpitError("validation_failed", str(exc), recoverable=True, status=400) from exc
            return _rename_run_packet(run)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def session_archive(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        scope = f"sessions/{ref.worker_id}/{ref.session_id}/archive"

        async def produce() -> dict[str, Any]:
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            run = await asyncio.to_thread(_archive_session, self.ctx.store, ref)
            return _archive_session_packet(run, ref)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def session_unarchive(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        scope = f"sessions/{ref.worker_id}/{ref.session_id}/unarchive"

        async def produce() -> dict[str, Any]:
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            run = await asyncio.to_thread(_unarchive_session, self.ctx.store, ref)
            return _unarchive_session_packet(run, ref)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def session_delete(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _optional_json_body(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        scope = f"sessions/{ref.worker_id}/{ref.session_id}/delete"

        async def produce() -> dict[str, Any]:
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            return await asyncio.to_thread(_delete_session_packet, self.ctx, ref)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def session_rename(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        scope = f"sessions/{ref.worker_id}/{ref.session_id}/rename"

        async def produce() -> dict[str, Any]:
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            run_id = await asyncio.to_thread(_session_run_id_from_store, self.ctx.store, ref)
            if not run_id:
                raise CockpitError("not_found", "session run not found", status=404)
            try:
                run = await asyncio.to_thread(
                    self.ctx.store.rename_run,
                    run_id,
                    str(body.get("title") or body.get("name") or ""),
                )
            except ValueError as exc:
                raise CockpitError("validation_failed", str(exc), recoverable=True, status=400) from exc
            return _rename_session_packet(run, ref)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def session_close(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        body = await _json_body(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        scope = f"sessions/{ref.worker_id}/{ref.session_id}/close"

        async def produce() -> dict[str, Any]:
            _require_capability(self.ctx.cfg, WORKER_SESSION_STOP)
            _require_capability(self.ctx.cfg, "orchestration.runs.write")
            proxied = _worker_control_body(body, WORKER_SESSION_STOP)
            raw = await asyncio.to_thread(
                _worker_post_json,
                self.ctx.cfg,
                ref.worker_id,
                f"/sessions/{ref.session_id}/stop",
                proxied,
                post=self.ctx.post,
            )
            write_packet = await asyncio.to_thread(_session_write_packet, self.ctx.cfg, self.ctx.store, ref, raw, get=self.ctx.get)
            cleanup = await asyncio.to_thread(_cleanup_child_session_worktree, self.ctx.cfg, self.ctx.store, ref, body, post=self.ctx.post)
            archived = await asyncio.to_thread(_close_session, self.ctx.store, ref)
            return _close_session_packet(archived, ref, write_packet, cleanup)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
        return web.json_response(response_body)

    async def session_write(self, request: web.Request) -> web.Response:
        await self.ctx.require_auth(request)
        ref = await asyncio.to_thread(_resolve_session_ref, self.ctx.store, request.match_info["session_ref"])
        action = request.match_info.get("action", "restore_checkpoint")
        body = await _json_body(request)
        if action == "turns":
            attachments = _validate_attachments(body, self.ctx.cfg.orchestration)
            if attachments:
                body["attachments"] = attachments
        requester = _cockpit_requester_context(request, self.ctx.cfg)
        scope = f"sessions/{ref.worker_id}/{ref.session_id}/{action}"

        async def produce() -> dict[str, Any]:
            required = _required_session_action(action)
            _require_capability(self.ctx.cfg, required)
            proxied = _worker_control_body(body, required)
            path = f"/sessions/{ref.session_id}/{'checkpoints/restore' if action == 'restore_checkpoint' else action}"
            raw = await asyncio.to_thread(_worker_post_json, self.ctx.cfg, ref.worker_id, path, proxied, post=self.ctx.post)
            return await asyncio.to_thread(_session_write_packet, self.ctx.cfg, self.ctx.store, ref, raw, get=self.ctx.get)

        response_body = await _idempotent_write_body(self.ctx, scope, str(body.get("idempotency_key") or ""), body, produce, requester=requester)
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
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,PATCH,DELETE,OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Authorization,Content-Type,Last-Event-ID,X-Idempotency-Key"
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


async def _write_thread_turn_receipt_response(
    request: web.Request,
    ctx: CockpitAppContext,
    thread: CockpitThread,
    receipt: dict[str, Any],
) -> web.StreamResponse:
    status = str(receipt.get("status") or "dispatching")
    if status in {"uncertain", "retry_required"}:
        raise CockpitError(
            "turn_outcome_uncertain" if status == "uncertain" else "turn_retry_required",
            (
                str(receipt.get("recovery_reason") or "turn outcome requires operator review")
                + "; review the durable thread, then submit with a new idempotency_key to retry"
            ),
            recoverable=True,
            status=409,
        )
    response = web.StreamResponse(
        status=200,
        headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"},
    )
    _apply_cors_headers(request, response)
    await response.prepare(request)
    cursor = new_id("threadturn")
    if status in {"dispatching", "accepted"}:
        event_type = "thread.turn.in_progress" if status == "dispatching" else "thread.turn.accepted"
        payload = {
            "project_id": thread.project_id,
            "thread_id": thread.thread_id,
            "turn_id": str(receipt.get("logical_turn_id") or receipt.get("queue_id") or ""),
            "idempotent": True,
            "receipt_status": status,
            "thread": _thread_projection(thread, ctx, include_messages=True),
        }
        await _write_sse(response, event_type, cursor, _sse_envelope(cursor, event_type, payload))
        await response.write_eof()
        return response
    if receipt.get("queue_id") and status in {"queued", "claimed"}:
        event_type = "thread.turn.queued"
        payload = {
            "project_id": thread.project_id,
            "thread_id": thread.thread_id,
            "queue_id": str(receipt.get("queue_id") or ""),
            "idempotent": True,
            "receipt_status": status,
            "thread": _thread_projection(thread, ctx, include_messages=True),
        }
    else:
        event_type = "thread.turn.replayed"
        payload = {
            "project_id": thread.project_id,
            "thread_id": thread.thread_id,
            "turn_id": str(receipt.get("logical_turn_id") or ""),
            "reply": str(receipt.get("reply") or ""),
            "idempotent": True,
            "receipt_status": status,
            "thread": _thread_projection(thread, ctx, include_messages=True),
        }
    await _write_sse(response, event_type, cursor, _sse_envelope(cursor, event_type, payload))
    await _write_sse(
        response,
        "thread.turn.done",
        cursor,
        _sse_envelope(cursor, "thread.turn.done", payload),
    )
    await response.write_eof()
    return response


async def _write_thread_turn_error(
    response: web.StreamResponse,
    cursor: str,
    *,
    project_id: str,
    thread_id: str,
    code: str,
    message: str,
    recoverable: bool,
) -> web.StreamResponse:
    """Emit the `thread.turn.error` SSE frame and close the stream.

    Shared by project_thread_turn()'s two exception handlers, which only
    differ in the error code/message/recoverable flag.
    """
    await _write_sse(
        response,
        "thread.turn.error",
        cursor,
        _sse_envelope(
            cursor,
            "thread.turn.error",
            {
                "project_id": project_id,
                "thread_id": thread_id,
                "error": {
                    "code": code,
                    "message": message,
                    "recoverable": recoverable,
                },
            },
        ),
    )
    await response.write_eof()
    return response


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


def _service(
    cfg: Config,
    source_factory: Callable[[str, Any], WorkSource],
    *,
    manual_item: WorkItem | None = None,
    thread_child_terminal_notifier: Callable[[str, Any], bool] | None = None,
    thread_children_promoter: Callable[[str], object] | None = None,
) -> OrchestrationService:
    def factory(name: str, inner_cfg: Any = None) -> WorkSource:
        if name == "manual":
            return _ManualWorkSource(manual_item)
        return source_factory(name, inner_cfg)

    return OrchestrationService(
        cfg=cfg,
        capabilities=resolve_capabilities(cfg.capabilities),
        source_factory=factory,
        thread_child_terminal_notifier=thread_child_terminal_notifier,
        thread_children_promoter=thread_children_promoter,
    )


def _command_from_body(body: dict[str, Any], *, start: bool) -> tuple[WorkCommand, WorkItem | None]:
    if isinstance(body.get("command"), dict):
        command = WorkCommand.from_dict(dict(body["command"]))
    else:
        command = parse_work_command(str(body.get("phrase") or "next work"))
    if body.get("source"):
        command.source = str(body["source"])
    if body.get("repo"):
        command.filters["repo"] = str(body["repo"])
    project_id = _work_project_id(body)
    if project_id:
        command.filters["project_id"] = project_id
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
            "parent_chat_id": run.parent_chat_id if run else "",
            "project_id": result.envelope.project_id,
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
                return build_session_row(
                    session_ref=make_session_ref(link.worker_id, link.session_id),
                    worker_id=link.worker_id,
                    session_id=link.session_id,
                    run_id=run.run_id,
                    parent_chat_id=run.parent_chat_id or "",
                    project_id=link.project_id or run.project_id,
                    title=run.objective,
                    provider=str(session_raw.get("provider") or link.provider),
                    engine=str(session_raw.get("engine") or link.engine),
                    status=str(session_raw.get("status") or link.status),
                    ended_reason=_ended_reason_from_worker_session(session_raw) or link.ended_reason,
                    repo=next((item.item.repo for item in run.work_items if item.item.repo), ""),
                    branch=str(session_raw.get("branch") or link.branch),
                    cwd=str(session_raw.get("cwd") or link.cwd),
                    latest_event_cursor=str(session_raw.get("last_event_id") or link.last_event_id),
                    created_at=run.created_at,
                    updated_at=run.updated_at,
                    archived_at=link.archived_at,
                    allowed_actions=list(link.allowed_actions),
                )
    session_id = str(session_raw.get("session_id") or ref.session_id)
    archived = store.archived_worker_sessions().get(f"{ref.worker_id}\0{session_id}", {})
    return build_session_row(
        session_ref=make_session_ref(ref.worker_id, session_id),
        worker_id=ref.worker_id,
        session_id=session_id,
        run_id=str(session_raw.get("run_id") or ""),
        parent_chat_id=str(session_raw.get("parent_chat_id") or ""),
        project_id=str(session_raw.get("project_id") or ""),
        title=str(session_raw.get("title") or ""),
        provider=str(session_raw.get("provider") or ""),
        engine=str(session_raw.get("engine") or session_raw.get("provider") or ""),
        status=str(session_raw.get("status") or ""),
        ended_reason=_ended_reason_from_worker_session(session_raw),
        repo=str(session_raw.get("repo") or ""),
        branch=str(session_raw.get("branch") or ""),
        cwd=str(session_raw.get("cwd") or ""),
        latest_event_cursor=str(session_raw.get("last_event_id") or ""),
        created_at=str(session_raw.get("created_at") or ""),
        updated_at=str(session_raw.get("updated_at") or ""),
        archived_at=archived.get("archived_at") or "",
        allowed_actions=_allowed_actions_from_worker_session(session_raw),
    )


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


async def _optional_json_body(request: web.Request) -> dict[str, Any]:
    if not request.can_read_body:
        return {}
    try:
        return await _json_body(request)
    except CockpitError as exc:
        if exc.code == "validation_failed" and not (request.content_length or 0):
            return {}
        raise


async def _idempotent_write_body(
    ctx: CockpitAppContext,
    scope: str,
    key: str,
    fingerprint_body: dict[str, Any],
    producer: Callable[[], Any],
    *,
    requester: RequestContext | None = None,
    cache_response_body: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    cache_error_body: Callable[[Exception], dict[str, Any] | None] | None = None,
    on_save_error: Callable[[Exception], Awaitable[None]] | None = None,
) -> dict[str, Any]:
    if requester is not None:
        scope = f"{scope}/principal/{_idempotency_principal(requester)}"
    async with _idempotency_scope(ctx, scope, key):
        cached = ctx.idempotency.get(scope, key, fingerprint_body)
        if cached is not None:
            return cached
        try:
            response_body = await producer()
        except Exception as exc:
            if cache_error_body is not None:
                error_body = cache_error_body(exc)
                if error_body is not None:
                    ctx.idempotency.save(scope, key, fingerprint_body, error_body)
            raise
        cached_response_body = cache_response_body(response_body) if cache_response_body is not None else response_body
        try:
            ctx.idempotency.save(scope, key, fingerprint_body, cached_response_body)
        except Exception as exc:
            if on_save_error is not None:
                await on_save_error(exc)
            raise
        return response_body


def _idempotency_principal(requester: RequestContext) -> str:
    return "\0".join((requester.scope, requester.identity, requester.memory_peer, requester.channel))


def _reject_attachments(body: dict[str, Any]) -> None:
    attachments = body.get("attachments")
    if attachments in (None, []):
        return
    raise CockpitError("validation_failed", "turn attachments are not supported on this endpoint", recoverable=True, status=400)


ATTACHMENT_IMAGE_MIME_TYPES = {"image/png", "image/jpeg", "image/webp", "image/gif"}


def _validate_attachments(body: dict[str, Any], orchestration) -> list[dict[str, Any]]:  # noqa: ANN001
    attachments = body.get("attachments")
    if attachments in (None, []):
        return []
    if not isinstance(attachments, list):
        raise CockpitError("validation_failed", "attachments must be an array", recoverable=True, status=400)
    max_count = max(0, int(orchestration.turn_attachment_max_count))
    max_bytes = max(1, int(orchestration.turn_attachment_max_bytes))
    if len(attachments) > max_count:
        raise CockpitError(
            "validation_failed",
            f"too many attachments: {len(attachments)} exceeds the limit of {max_count} (ORCHESTRATION_TURN_ATTACHMENT_MAX_COUNT)",
            recoverable=True,
            status=400,
        )
    normalized: list[dict[str, Any]] = []
    for index, raw in enumerate(attachments):
        label = f"attachments[{index}]"
        if not isinstance(raw, dict):
            raise CockpitError("validation_failed", f"{label} must be an object", recoverable=True, status=400)
        kind = str(raw.get("kind") or "")
        if kind != "image":
            raise CockpitError("validation_failed", f'{label}: only kind "image" is supported', recoverable=True, status=400)
        mime_type = str(raw.get("mime_type") or "")
        if mime_type not in ATTACHMENT_IMAGE_MIME_TYPES:
            supported = ", ".join(sorted(ATTACHMENT_IMAGE_MIME_TYPES))
            raise CockpitError("validation_failed", f"{label}: mime_type must be one of {supported}", recoverable=True, status=400)
        data_url = str(raw.get("data_url") or "")
        prefix = f"data:{mime_type};base64,"
        if not data_url.startswith(prefix):
            raise CockpitError(
                "validation_failed",
                f'{label}: data_url must start with "data:{mime_type};base64,"',
                recoverable=True,
                status=400,
            )
        try:
            decoded_bytes = len(base64.b64decode(data_url[len(prefix) :], validate=True))
        except (binascii.Error, ValueError) as exc:
            raise CockpitError("validation_failed", f"{label}: data_url payload is not valid base64", recoverable=True, status=400) from exc
        if decoded_bytes > max_bytes:
            raise CockpitError(
                "validation_failed",
                f"{label} is {decoded_bytes} bytes decoded; the limit is {max_bytes} bytes (ORCHESTRATION_TURN_ATTACHMENT_MAX_BYTES)",
                recoverable=True,
                status=400,
            )
        normalized.append({"kind": "image", "mime_type": mime_type, "name": str(raw.get("name") or ""), "data_url": data_url})
    return normalized


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


def _close_session(store: OrchestrationStore, ref: SessionRef):
    return store.close_cockpit_session(ref.worker_id, ref.session_id)


def _unarchive_session(store: OrchestrationStore, ref: SessionRef):
    try:
        return store.unarchive_cockpit_session(ref.worker_id, ref.session_id)
    except KeyError as exc:
        raise CockpitError("not_found", "session not found", status=404) from exc
    except RunArchivedError as exc:
        raise CockpitError(
            "run_archived",
            f"run {exc.run_id} is archived; the session stays hidden until its run is restored",
            recoverable=True,
            status=409,
        ) from exc


def _archive_run_packet(run) -> dict[str, Any]:  # noqa: ANN001
    run_row = run_summary(run)
    reclamation = _reclamation_summary()
    return {
        "ok": True,
        "cursor": snapshot_cursor({"run": run_row, "archived": True, "reclamation": reclamation}),
        "run": run_row,
        "session": {},
        "events": [],
        "requests": [],
        "artifacts": [],
        "reclamation": reclamation,
    }


def _rename_run_packet(run) -> dict[str, Any]:  # noqa: ANN001
    run_row = run_summary(run)
    return {
        "ok": True,
        "cursor": snapshot_cursor({"run": run_row}),
        "run": run_row,
        "session": {},
        "events": [],
        "requests": [],
        "artifacts": [],
    }


def _rename_session_packet(run, ref: SessionRef) -> dict[str, Any]:  # noqa: ANN001
    session = next((item for item in run.sessions if item.worker_id == ref.worker_id and item.session_id == ref.session_id), None)
    session_row = session_summary(_session_from_link(session, run)) if session is not None else {}
    run_row = run_summary(run)
    return {
        "ok": True,
        "cursor": snapshot_cursor({"run": run_row, "session": session_row}),
        "run": run_row,
        "session": session_row,
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
    reclamation = _reclamation_summary()
    return {
        "ok": True,
        "cursor": snapshot_cursor({"run": run_row, "session": session_row, "archived": True, "reclamation": reclamation}),
        "run": run_row,
        "session": session_row,
        "events": [],
        "requests": [],
        "artifacts": [],
        "reclamation": reclamation,
    }


def _delete_session_packet(ctx: CockpitAppContext, ref: SessionRef) -> dict[str, Any]:
    if ctx.store.deleted_worker_session(ref.worker_id, ref.session_id) is not None:
        summary = _reclamation_summary()
        deleted = False
    else:
        worker_summary = _delete_worker_session(ctx.cfg, ref, delete=ctx.delete)
        store_summary = ctx.store.delete_cockpit_session(ref.worker_id, ref.session_id)
        summary = _reclamation_summary(
            records=int(worker_summary.get("records") or 0) + int(store_summary.get("records") or 0),
            events=int(worker_summary.get("events") or 0) + int(store_summary.get("events") or 0),
            worktrees=int(worker_summary.get("worktrees") or 0),
            bytes_reclaimed=int(worker_summary.get("bytes") or 0),
        )
        deleted = bool(store_summary.get("deleted"))
    session_row = session_summary(
        {
            "session_ref": make_session_ref(ref.worker_id, ref.session_id),
            "worker_id": ref.worker_id,
            "session_id": ref.session_id,
            "status": "deleted",
        }
    )
    return {
        "ok": True,
        "deleted": deleted,
        "cursor": snapshot_cursor({"session": session_row, "deleted": True, "reclamation": summary}),
        "run": {},
        "session": session_row,
        "events": [],
        "requests": [],
        "artifacts": [],
        "reclamation": summary,
    }


def _delete_run_packet(ctx: CockpitAppContext, run_id: str) -> dict[str, Any]:
    run = ctx.store.get(run_id)
    if run is None:
        if ctx.store.deleted_run(run_id) is None:
            raise CockpitError("not_found", "run not found", status=404)
        summary = _reclamation_summary()
        run_row = {"run_id": run_id, "status": "deleted"}
        return {
            "ok": True,
            "deleted": False,
            "cursor": snapshot_cursor({"run": run_row, "deleted": True, "reclamation": summary}),
            "run": run_row,
            "session": {},
            "events": [],
            "requests": [],
            "artifacts": [],
            "reclamation": summary,
        }
    if run.jobs:
        raise CockpitError("conflict", "run still has worker jobs; clean up jobs before deleting the run", recoverable=True, status=409)
    records = 0
    events = 0
    worktrees = 0
    bytes_reclaimed = 0
    for session in run.sessions:
        worker_summary = _delete_worker_session(
            ctx.cfg,
            SessionRef(worker_id=session.worker_id, session_id=session.session_id),
            delete=ctx.delete,
        )
        records += int(worker_summary.get("records") or 0)
        events += int(worker_summary.get("events") or 0)
        worktrees += int(worker_summary.get("worktrees") or 0)
        bytes_reclaimed += int(worker_summary.get("bytes") or 0)
    store_summary = ctx.store.delete_run(run.run_id)
    records += int(store_summary.get("records") or 0)
    events += int(store_summary.get("events") or 0)
    summary = _reclamation_summary(
        records=records,
        events=events,
        worktrees=worktrees,
        bytes_reclaimed=bytes_reclaimed,
    )
    run_row = {**run_summary(run), "status": "deleted"}
    return {
        "ok": True,
        "deleted": bool(store_summary.get("deleted")),
        "cursor": snapshot_cursor({"run": run_row, "deleted": True, "reclamation": summary}),
        "run": run_row,
        "session": {},
        "events": [],
        "requests": [],
        "artifacts": [],
        "reclamation": summary,
    }


def _deleted_run_row(deleted: dict[str, str]) -> dict[str, Any]:
    return {
        "authority": "jarvis",
        "supported_controls": [],
        "run_id": str(deleted.get("run_id") or ""),
        "title": "Deleted run",
        "objective": "",
        "status": "deleted",
        "phase": "deleted",
        "repo": "",
        "branch": "",
        "session_count": 0,
        "active_session_count": 0,
        "pending_input_count": 0,
        "pending_approval_count": 0,
        "artifact_count": 0,
        "primary_artifact_ids": [],
        "latest_activity_at": str(deleted.get("deleted_at") or ""),
        "latest_cursor": "",
        "created_at": "",
        "updated_at": str(deleted.get("deleted_at") or ""),
        "terminal_reason": None,
        "state_reason": "Run has been deleted",
        "blocked_reason": None,
        "waiting_on": [],
        "last_error": None,
        "archived_at": None,
        "deleted_at": str(deleted.get("deleted_at") or ""),
    }


def _deleted_session_row(ref: SessionRef, deleted: dict[str, str]) -> dict[str, Any]:
    return session_summary(
        {
            "session_ref": make_session_ref(ref.worker_id, ref.session_id),
            "worker_id": ref.worker_id,
            "session_id": ref.session_id,
            "status": "deleted",
            "updated_at": str(deleted.get("deleted_at") or ""),
        }
    ) | {"deleted_at": str(deleted.get("deleted_at") or "")}


def _prune_worker_worktrees_packet(ctx: CockpitAppContext, worker_id: str, body: dict[str, Any]) -> dict[str, Any]:
    """Ask one worker to sweep its stale worktrees and report what it reclaimed.

    `stale_ttl_s` is the worker's own configured threshold unless the caller
    overrides it; the worker owns the definition of stale because only it can
    see its live sessions and running jobs.
    """
    payload: dict[str, Any] = {"reason": str(body.get("reason") or "cockpit worktree prune")}
    if body.get("stale_ttl_s") is not None:
        try:
            payload["stale_ttl_s"] = float(body["stale_ttl_s"])
        except (TypeError, ValueError):
            raise CockpitError("validation_failed", "stale_ttl_s must be numeric", recoverable=True, status=400) from None
    if body.get("target"):
        payload["target"] = str(body["target"])
    raw = _worker_post_json(ctx.cfg, worker_id, "/worktrees/prune", payload, post=ctx.post)
    reclamation = _reclamation_summary(
        worktrees=int(raw.get("worktrees") or 0),
        bytes_reclaimed=int(raw.get("bytes") or 0),
        notes=[f"repo metadata pruned: {len(raw.get('repos_pruned') or [])}"],
    )
    inventory = _worker_worktree_inventory(ctx, worker_id)
    return {
        "ok": bool(raw.get("ok", True)),
        "worker_id": worker_id,
        "cursor": snapshot_cursor({"worker_id": worker_id, "reclamation": reclamation, "worktree_inventory": inventory}),
        "reclamation": reclamation,
        "pruned": [
            {"name": str(item.get("name") or ""), "bytes": int(item.get("bytes") or 0)}
            for item in raw.get("pruned") or []
            if isinstance(item, dict)
        ],
        "refused": [dict(item) for item in raw.get("refused") or [] if isinstance(item, dict)],
        "worktree_inventory": inventory,
    }


def _worker_worktree_inventory(ctx: CockpitAppContext, worker_id: str) -> dict[str, Any]:
    """Post-sweep recount, so the caller can show before/after without a second probe."""
    registry = WorkerRegistry(ctx.cfg.worker, profiles_path=ctx.cfg.orchestration.workers_path, http_get=ctx.get)
    profile = registry.get(worker_id, probe=True)
    return _cockpit_worktree_inventory(profile.worktree_inventory) if profile is not None else {}


def _delete_worker_session(cfg: Config, ref: SessionRef, *, delete: HttpDelete) -> dict[str, int]:
    try:
        raw = _worker_delete_json(cfg, ref.worker_id, f"/sessions/{ref.session_id}", delete=delete)
    except CockpitError as exc:
        if exc.code == "not_found":
            return {"records": 0, "events": 0, "worktrees": 0, "bytes": 0}
        raise
    reclamation = raw.get("reclamation") if isinstance(raw.get("reclamation"), dict) else {}
    return {
        "records": int(reclamation.get("records") or 0),
        "events": int(reclamation.get("events") or 0),
        "worktrees": int(reclamation.get("worktrees") or 0),
        "bytes": int(reclamation.get("bytes") or 0),
    }


def _reclamation_summary(
    *,
    records: int = 0,
    events: int = 0,
    worktrees: int = 0,
    bytes_reclaimed: int = 0,
    memory_sessions: int = 0,
    notes: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "records": max(0, int(records)),
        "events": max(0, int(events)),
        "worktrees": max(0, int(worktrees)),
        "bytes": max(0, int(bytes_reclaimed)),
        "memory_sessions": max(0, int(memory_sessions)),
        "notes": list(notes or []),
    }


def _unarchive_session_packet(run, ref: SessionRef) -> dict[str, Any]:  # noqa: ANN001
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
                "status": "",
                "archived_at": "",
            }
        )
        run_row = {}
    return {
        "ok": True,
        "cursor": snapshot_cursor({"run": run_row, "session": session_row, "archived": False}),
        "run": run_row,
        "session": session_row,
        "events": [],
        "requests": [],
        "artifacts": [],
    }


def _close_session_packet(run, ref: SessionRef, write_packet: dict[str, Any], cleanup: dict[str, Any]) -> dict[str, Any]:  # noqa: ANN001
    archived_packet = _archive_session_packet(run, ref)
    return {
        **archived_packet,
        "events": write_packet.get("events", []),
        "requests": write_packet.get("requests", []),
        "artifacts": write_packet.get("artifacts", []),
        "cleanup": cleanup,
        "cursor": snapshot_cursor(
            {
                "run": archived_packet.get("run", {}),
                "session": archived_packet.get("session", {}),
                "events": write_packet.get("events", []),
                "cleanup": cleanup,
                "closed": True,
            }
        ),
    }


def _hub_sync_state(ctx: CockpitAppContext, mode: str, worker_sync: _HubWorkerSync) -> dict[str, Any]:
    """Discover worker-side changes before deciding whether projection is needed."""

    return sync_state(
        store=ctx.store,
        worker_cfg=ctx.cfg.worker,
        workers_path=ctx.cfg.orchestration.workers_path,
        sync_mode=mode,
        http_get=worker_sync.get,
        timeout_s=float(ctx.cfg.orchestration.sse_sync_timeout_s),
        should_sync_worker=worker_sync.should_sync,
    )


def _hub_sync_dirty(
    ctx: CockpitAppContext,
    mode: str,
    worker_sync: _HubWorkerSync,
    dirty: set[tuple[str, str, str]],
    runs: list[Any] | None = None,
) -> dict[str, Any]:
    """Apply worker push hints without rediscovering every worker's state."""
    session_runs: set[str] = set()
    job_runs: set[str] = set()
    worker_ids = {worker_id for worker_id, _session_id, _job_id in dirty}
    for run in runs if runs is not None else ctx.store.list_runs():
        if run.archived_at:
            continue
        for worker_id, session_id, job_id in dirty:
            if session_id and any(
                link.worker_id == worker_id and link.session_id == session_id for link in run.sessions
            ):
                session_runs.add(run.run_id)
            if job_id and any(link.worker_id == worker_id and link.job_id == job_id for link in run.jobs):
                job_runs.add(run.run_id)

    errors: list[str] = []

    def should_sync(profile: Any) -> bool:
        return profile.worker_id in worker_ids and worker_sync.should_sync(profile)

    for run_id in sorted(session_runs):
        errors.extend(
            sync_run_sessions(
                ctx.store,
                worker_cfg=ctx.cfg.worker,
                workers_path=ctx.cfg.orchestration.workers_path,
                run_id=run_id,
                get=worker_sync.get,
                timeout_s=float(ctx.cfg.orchestration.sse_sync_timeout_s),
                should_sync_worker=should_sync,
            ).errors
            or []
        )
    for run_id in sorted(job_runs):
        errors.extend(
            sync_run_jobs(
                ctx.store,
                worker_cfg=ctx.cfg.worker,
                workers_path=ctx.cfg.orchestration.workers_path,
                run_id=run_id,
                get=worker_sync.get,
                timeout_s=float(ctx.cfg.orchestration.sse_sync_timeout_s),
                should_sync_worker=should_sync,
            ).errors
            or []
        )
    return {
        "mode": mode,
        "status": "fresh" if not errors else "partial",
        "synced_at": utc_now(),
        "errors": [public_error_message(error) for error in errors],
    }


async def _shared_snapshot_body(
    ctx: CockpitAppContext,
    hub: SseSnapshotHub,
    mode: str,
    *,
    respect_backoff: bool = True,
) -> dict[str, Any]:
    if mode not in {"fast", "probe"}:
        return await asyncio.to_thread(_cockpit_snapshot, ctx, mode)
    worker_sync = _HubWorkerSync(hub, respect_backoff=respect_backoff)
    sync = await asyncio.to_thread(_hub_sync_state, ctx, mode, worker_sync)
    all_runs = await asyncio.to_thread(ctx.store.list_runs)
    worker_state = await asyncio.to_thread(
        _refresh_worker_state,
        ctx,
        mode,
        worker_sync,
        all_runs,
    )
    return await asyncio.to_thread(
        _cockpit_snapshot,
        ctx,
        mode,
        sync=sync,
        sync_timeout_s=float(ctx.cfg.orchestration.sse_sync_timeout_s),
        should_sync_worker=worker_sync.should_sync,
        http_get=worker_sync.get,
        worker_state=worker_state,
        all_runs=all_runs,
    )


def _hub_worker_state(
    ctx: CockpitAppContext,
    mode: str,
    worker_sync: _HubWorkerSync,
    all_runs: list[Any] | None = None,
    worker_ids: set[str] | None = None,
    previous: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Fetch only the live worker portion used to decide if a projection changed.

    Run/event files are intentionally absent here: store generation makes their
    changes visible without reconstructing the rest of the cockpit snapshot.
    """

    all_runs = all_runs if all_runs is not None else ctx.store.list_runs()
    archived_run_ids = {run.run_id for run in all_runs if run.archived_at}
    archived_session_refs = archived_session_refs_for_store(ctx.store, all_runs) | deleted_session_refs_for_store(ctx.store)
    runs = [run for run in all_runs if not run.archived_at]
    registry = WorkerRegistry(
        ctx.cfg.worker,
        profiles_path=ctx.cfg.orchestration.workers_path,
        http_get=worker_sync.get,
    )
    previous = previous or {}
    profiles = registry.profiles(probe=False)
    if mode == "probe":
        profiles = [
            registry.get(profile.worker_id, probe=True) or profile
            if (worker_ids is None or profile.worker_id in worker_ids) and worker_sync.should_sync(profile)
            else profile
            for profile in profiles
        ]
    eligible_worker_ids = {
        profile.worker_id
        for profile in profiles
        if profile.status != "offline" and worker_sync.should_sync(profile)
    }
    workers = [
        project_worker_profile(profile, default_repo=ctx.cfg.orchestration.default_repo)
        for profile in profiles
    ]

    current_worker_ids = {profile.worker_id for profile in profiles}
    previous_worker_ids = _worker_state_ids(previous)
    refresh_worker_ids = (
        set(worker_ids)
        if worker_ids is not None
        else current_worker_ids | previous_worker_ids
    )
    attempted_worker_ids = {
        profile.worker_id
        for profile in profiles
        if profile.worker_id in refresh_worker_ids and profile.worker_id in eligible_worker_ids
    }
    unattempted_worker_ids = (refresh_worker_ids & current_worker_ids) - attempted_worker_ids
    backed_off_worker_ids = {
        profile.worker_id
        for profile in profiles
        if profile.status != "offline"
        and profile.worker_id in unattempted_worker_ids
        and worker_sync.is_backed_off(profile)
    }
    offline_worker_ids = {
        profile.worker_id
        for profile in profiles
        if profile.status == "offline" and profile.worker_id in unattempted_worker_ids
    }

    def should_sync(profile: Any) -> bool:
        return profile.worker_id in attempted_worker_ids

    session_diagnostics: list[WorkerReadDiagnostic] = []
    current_sessions = aggregate_sessions(
        runs=runs,
        worker_cfg=ctx.cfg.worker,
        workers_path=ctx.cfg.orchestration.workers_path,
        http_get=worker_sync.get,
        worker_by_id={worker["worker_id"]: worker for worker in workers},
        include_worker_state=True,
        archived_run_ids=archived_run_ids,
        archived_session_refs=archived_session_refs,
        timeout_s=float(ctx.cfg.orchestration.sse_sync_timeout_s),
        should_sync_worker=should_sync,
        diagnostics=session_diagnostics,
    )
    sessions = _reconcile_session_rows(
        previous.get("sessions") or {},
        current_sessions,
        refresh_worker_ids,
        _failed_workers(session_diagnostics, "sessions") | unattempted_worker_ids,
    )
    sessions = {
        ref: row
        for ref, row in sessions.items()
        if ref not in archived_session_refs and str(row.get("run_id") or "") not in archived_run_ids
    }
    request_diagnostics: list[WorkerReadDiagnostic] = []
    current_requests = aggregate_requests(
        worker_cfg=ctx.cfg.worker,
        workers_path=ctx.cfg.orchestration.workers_path,
        http_get=worker_sync.get,
        timeout_s=float(ctx.cfg.orchestration.sse_sync_timeout_s),
        should_sync_worker=should_sync,
        diagnostics=request_diagnostics,
    )
    requests = _reconcile_worker_rows(
        previous.get("requests") or [],
        current_requests,
        refresh_worker_ids,
        _failed_workers(request_diagnostics, "requests") | unattempted_worker_ids,
        sessions=sessions,
    )
    requests = [request for request in requests if str(request.get("session_ref") or "") in sessions]
    checkpoint_diagnostics: list[WorkerReadDiagnostic] = []
    current_checkpoints = aggregate_checkpoints(
        runs=runs,
        sessions=sessions,
        worker_cfg=ctx.cfg.worker,
        workers_path=ctx.cfg.orchestration.workers_path,
        http_get=worker_sync.get,
        timeout_s=float(ctx.cfg.orchestration.sse_sync_timeout_s),
        should_sync_worker=should_sync,
        diagnostics=checkpoint_diagnostics,
    )
    checkpoints = _reconcile_checkpoint_rows(
        previous.get("checkpoints") or [],
        current_checkpoints,
        refresh_worker_ids,
        checkpoint_diagnostics,
        sessions,
        unattempted_worker_ids=unattempted_worker_ids,
    )
    checkpoints = [
        checkpoint
        for checkpoint in checkpoints
        if str(checkpoint.get("session_ref") or "") in sessions
    ]
    diagnostics = [*session_diagnostics, *request_diagnostics, *checkpoint_diagnostics]
    projected_diagnostics = _reconcile_worker_diagnostics(
        [dict(item) for item in previous.get("diagnostics") or [] if isinstance(item, dict)],
        [_worker_read_diagnostic(item) for item in diagnostics],
        refresh_worker_ids,
        attempted_worker_ids,
        current_worker_ids,
    )
    projected_diagnostics = [
        item
        for item in projected_diagnostics
        if not str(item.get("session_id") or "")
        or make_session_ref(
            str(item.get("worker_id") or ""),
            str(item.get("session_id") or ""),
        ) not in archived_session_refs
    ]
    failed_worker_ids = {
        str(item.get("worker_id") or "")
        for item in projected_diagnostics
        if item.get("status") == "failure"
    }
    projected_diagnostics.extend(
        {
            "worker_id": worker_id,
            "resource": "worker_state",
            "status": "failure",
            "failure_kind": failure_kind,
            "status_code": 0,
            "error_type": "",
            "session_id": "",
        }
        for failure_kind, worker_ids_for_kind in (
            ("backoff", backed_off_worker_ids),
            ("offline", offline_worker_ids),
        )
        for worker_id in sorted(worker_ids_for_kind - failed_worker_ids)
    )
    current = {
        "workers": _reconcile_worker_rows(
            previous.get("workers") or [],
            workers,
            refresh_worker_ids,
            backed_off_worker_ids,
            include_new_for_failed=True,
        ),
        "sessions": sessions,
        "requests": requests,
        "checkpoints": checkpoints,
        "partial": any(item.get("status") == "failure" for item in projected_diagnostics),
        "diagnostics": projected_diagnostics,
    }
    return current


def _refresh_worker_state(
    ctx: CockpitAppContext,
    mode: str,
    worker_sync: _HubWorkerSync,
    all_runs: list[Any] | None = None,
    worker_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Refresh the process-wide last-known worker projection used by REST and SSE.

    Worker reads block for as long as an unresponsive worker takes to time out.
    Holding `worker_state_cache_lock` across them convoyed every refresh caller
    onto the lock and exhausted the default executor, starving *all* other
    `to_thread` work — conversation reads included — until the API stopped
    serving. So refreshes are single-flight per mode: a caller that finds one
    already running reuses the last-known projection instead of queueing another
    blocking read, and the lock is only held around the cache swap.
    """
    with ctx.worker_state_cache_lock:
        if mode in ctx.worker_state_refresh_modes:
            cached = ctx.worker_state_cache.get(mode)
            if cached is not None:
                return cached
        ctx.worker_state_refresh_modes.add(mode)
        previous = ctx.worker_state_cache.get(mode)
    try:
        state = _hub_worker_state(
            ctx,
            mode,
            worker_sync,
            all_runs,
            worker_ids,
            previous,
        )
    finally:
        with ctx.worker_state_cache_lock:
            ctx.worker_state_refresh_modes.discard(mode)
    with ctx.worker_state_cache_lock:
        ctx.worker_state_cache[mode] = state
        return state


def _failed_workers(diagnostics: list[WorkerReadDiagnostic], resource: str) -> set[str]:
    return {
        item.worker_id
        for item in diagnostics
        if item.resource == resource and item.status == "failure"
    }


def _worker_state_ids(state: dict[str, Any]) -> set[str]:
    ids = {
        str(row.get("worker_id") or "")
        for name in ("workers", "diagnostics")
        for row in state.get(name) or []
        if isinstance(row, dict)
    }
    ids.update(
        str(row.get("worker_id") or "")
        for row in (state.get("sessions") or {}).values()
        if isinstance(row, dict)
    )
    ids.discard("")
    return ids


def _reconcile_worker_diagnostics(
    previous: list[dict[str, Any]],
    current: list[dict[str, Any]],
    refresh_worker_ids: set[str],
    attempted_worker_ids: set[str],
    current_worker_ids: set[str],
) -> list[dict[str, Any]]:
    preserve_worker_ids = (refresh_worker_ids & current_worker_ids) - attempted_worker_ids
    retained = [
        item
        for item in previous
        if str(item.get("worker_id") or "") not in refresh_worker_ids
        or str(item.get("worker_id") or "") in preserve_worker_ids
    ]
    refreshed = [
        item
        for item in current
        if str(item.get("worker_id") or "") in attempted_worker_ids
    ]
    return [*retained, *refreshed]


def _reconcile_worker_rows(
    previous: list[dict[str, Any]],
    current: list[dict[str, Any]],
    target_worker_ids: set[str],
    failed_worker_ids: set[str],
    *,
    sessions: dict[str, dict[str, Any]] | None = None,
    include_new_for_failed: bool = False,
) -> list[dict[str, Any]]:
    def worker_id(row: dict[str, Any]) -> str:
        direct = str(row.get("worker_id") or "")
        if direct or sessions is None:
            return direct
        session = sessions.get(str(row.get("session_ref") or "")) or {}
        return str(session.get("worker_id") or "")

    retained = [
        row
        for row in previous
        if worker_id(row) not in target_worker_ids
        or worker_id(row) in failed_worker_ids
    ]
    previous_worker_ids = {worker_id(row) for row in previous}
    refreshed = [
        row
        for row in current
        if worker_id(row) in target_worker_ids
        and (
            worker_id(row) not in failed_worker_ids
            or (include_new_for_failed and worker_id(row) not in previous_worker_ids)
        )
    ]
    return [*retained, *refreshed]


def _reconcile_session_rows(
    previous: dict[str, dict[str, Any]],
    current: dict[str, dict[str, Any]],
    target_worker_ids: set[str],
    failed_worker_ids: set[str],
) -> dict[str, dict[str, Any]]:
    retained = {
        ref: row
        for ref, row in previous.items()
        if str(row.get("worker_id") or "") not in target_worker_ids
        or str(row.get("worker_id") or "") in failed_worker_ids
    }
    refreshed = {
        ref: row
        for ref, row in current.items()
        if str(row.get("worker_id") or "") in target_worker_ids
        and (
            str(row.get("worker_id") or "") not in failed_worker_ids
            or ref not in previous
        )
    }
    return {**retained, **refreshed}


def _reconcile_checkpoint_rows(
    previous: list[dict[str, Any]],
    current: list[dict[str, Any]],
    target_worker_ids: set[str],
    diagnostics: list[WorkerReadDiagnostic],
    sessions: dict[str, dict[str, Any]],
    *,
    unattempted_worker_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    failed_workers = _failed_workers(diagnostics, "checkpoints") | (unattempted_worker_ids or set())
    failed_sessions = {
        (item.worker_id, item.session_id)
        for item in diagnostics
        if item.resource == "session_checkpoints" and item.status == "failure" and item.session_id
    }

    def failed(row: dict[str, Any]) -> bool:
        session = sessions.get(str(row.get("session_ref") or "")) or {}
        worker_id = str(row.get("worker_id") or session.get("worker_id") or "")
        session_id = str(row.get("session_id") or session.get("session_id") or "")
        return worker_id in failed_workers or (worker_id, session_id) in failed_sessions

    def worker_id(row: dict[str, Any]) -> str:
        session = sessions.get(str(row.get("session_ref") or "")) or {}
        return str(row.get("worker_id") or session.get("worker_id") or "")

    retained = [
        row
        for row in previous
        if worker_id(row) not in target_worker_ids or failed(row)
    ]
    refreshed = [
        row
        for row in current
        if worker_id(row) in target_worker_ids and not failed(row)
    ]
    return [*retained, *refreshed]


def _worker_read_diagnostic(item: WorkerReadDiagnostic) -> dict[str, Any]:
    return {
        "worker_id": item.worker_id,
        "resource": item.resource,
        "status": item.status,
        "failure_kind": item.failure_kind,
        "status_code": item.status_code,
        "error_type": item.error_type,
        "session_id": item.session_id,
    }


def _merge_dirty_worker_state(
    previous: dict[str, Any],
    current: dict[str, Any],
    worker_ids: set[str],
) -> dict[str, Any]:
    """Replace cached live rows only for workers named by a push hint."""

    def merged_rows(name: str) -> list[dict[str, Any]]:
        old_rows = [
            row
            for row in previous.get(name) or []
            if str(row.get("worker_id") or "") not in worker_ids
        ]
        new_rows = [
            row
            for row in current.get(name) or []
            if str(row.get("worker_id") or "") in worker_ids
        ]
        return [*old_rows, *new_rows]

    old_sessions = {
        ref: row
        for ref, row in (previous.get("sessions") or {}).items()
        if str(row.get("worker_id") or "") not in worker_ids
    }
    new_sessions = {
        ref: row
        for ref, row in (current.get("sessions") or {}).items()
        if str(row.get("worker_id") or "") in worker_ids
    }
    return {
        "workers": merged_rows("workers"),
        "sessions": {**old_sessions, **new_sessions},
        "requests": merged_rows("requests"),
        "checkpoints": merged_rows("checkpoints"),
    }


def _online_worker_ids(ctx: CockpitAppContext) -> frozenset[str]:
    registry = WorkerRegistry(ctx.cfg.worker, profiles_path=ctx.cfg.orchestration.workers_path)
    return frozenset(profile.worker_id for profile in registry.profiles(probe=False) if profile.status != "offline")


def _cockpit_snapshot(
    ctx: CockpitAppContext,
    mode: str,
    *,
    sync: dict[str, Any] | None = None,
    sync_timeout_s: float | None = None,
    should_sync_worker: Callable[[Any], bool] | None = None,
    http_get: HttpGet | None = None,
    worker_state: dict[str, Any] | None = None,
    all_runs: list[Any] | None = None,
) -> dict[str, Any]:
    return cockpit_snapshot(
        store=ctx.store,
        worker_cfg=ctx.cfg.worker,
        workers_path=ctx.cfg.orchestration.workers_path,
        sync_mode=mode,
        http_get=http_get or ctx.get,
        default_repo=ctx.cfg.orchestration.default_repo,
        sync=sync,
        sync_timeout_s=sync_timeout_s,
        should_sync_worker=should_sync_worker,
        worker_state=worker_state,
        all_runs=all_runs,
    )


def _catalog_context(cfg: Config) -> tuple[dict[str, Any], list[str], dict[str, dict[str, Any]]]:
    registry = WorkerRegistry(cfg.worker, profiles_path=cfg.orchestration.workers_path)
    profiles = registry.profiles(probe=False)
    worker = profiles[0] if profiles else None
    engines: list[str] = []
    # Last-known provider capabilities, OR-ed across workers: engine supports
    # are provider-side, so any worker reporting a flag speaks for the engine.
    engine_supports: dict[str, dict[str, Any]] = {}
    # Model catalogs are per-worker config, not provider facts, so they union by
    # id and the first worker to report a default for an engine wins — the
    # catalog is a picker hint; the worker that runs the turn is the authority.
    engine_models: dict[str, list[dict[str, str]]] = {}
    engine_default_model: dict[str, str] = {}
    for profile in profiles:
        for engine in profile.supported_engines:
            if engine and engine not in engines:
                engines.append(engine)
        for engine, supports in profile.engine_supports.items():
            merged = engine_supports.setdefault(engine, {})
            for key, value in supports.items():
                merged[key] = bool(merged.get(key)) or bool(value)
        for engine, models in profile.engine_models.items():
            rows = engine_models.setdefault(engine, [])
            seen = {row["id"] for row in rows}
            # Copy: the merged catalog outlives this loop and must not alias the
            # registry's in-memory profiles.
            rows.extend(dict(row) for row in models if row.get("id") and row["id"] not in seen)
        for engine, default in profile.engine_default_model.items():
            engine_default_model.setdefault(engine, default)
    for engine in {*engine_supports, *engine_models, *engine_default_model}:
        entry = engine_supports.setdefault(engine, {})
        entry["models"] = list(engine_models.get(engine, []))
        entry["default_model"] = engine_default_model.get(engine, "")
    defaults = {
        "worker_id": worker.worker_id if worker else "",
        "engine": (worker.default_engine or worker.agent) if worker else "",
        "repo": (worker.default_repo if worker else "") or cfg.orchestration.default_repo,
        "landing_mode": cfg.orchestration.landing_mode,
    }
    return defaults, engines, engine_supports


def _route_templates(app: web.Application) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for route in app.router.routes():
        method = str(route.method)
        if method in {"HEAD", "OPTIONS"}:
            continue
        path = _route_path_template(route)
        if not path:
            continue
        key = (method, path)
        if key in seen:
            continue
        seen.add(key)
        rows.append({"method": method, "path": path})
    return rows


def _route_path_template(route: web.AbstractRoute) -> str:
    resource = route.resource
    canonical = getattr(resource, "canonical", "")
    if canonical:
        return str(canonical)
    info = resource.get_info() if resource is not None else {}
    return str(info.get("path") or info.get("formatter") or "")


def _feature_availability(cfg: Config) -> dict[str, Any]:
    project_write_available, project_write_reason = _project_write_available(cfg)
    workers_configured = _configured_worker_count(cfg)
    return {
        "project_writes": {
            "available": project_write_available,
            "reason": project_write_reason,
        },
        "mcp": {
            "available": bool(cfg.mcp.enabled and cfg.mcp.servers),
            "serve_configured": Path(cfg.mcp_serve.token_store_path).expanduser().exists(),
        },
        "worker_dispatch": {
            "available": workers_configured > 0,
            "workers_configured": workers_configured,
        },
    }


def _cockpit_advertised_capabilities(requester: RequestContext, cfg: Config) -> list[str]:
    enforced = resolve_capabilities(cfg.capabilities)
    return sorted(set(requester.capabilities) & enforced)


def _project_write_available(cfg: Config) -> tuple[bool, str]:
    if not cfg.brain.peer_token.get_secret_value():
        return False, "brain project operation credentials are not configured"
    if not cfg.intercom.brain_host or int(cfg.intercom.brain_port) <= 0:
        return False, "brain project operation endpoint is not configured"
    return True, ""


def _configured_worker_count(cfg: Config) -> int:
    registry = WorkerRegistry(cfg.worker, profiles_path=cfg.orchestration.workers_path)
    return registry.configured_profile_count()


def _registry_store(cfg: Config) -> RegistryStore:
    return RegistryStore(
        cfg.registry.path,
        memory=MemoryClient(cfg.memory),
        curation_outbox=_curation_outbox(cfg),
    )


def _legacy_cockpit_requester_id(cfg: Config) -> str:
    identity = cfg.capabilities.identity.strip()
    return "" if identity == HOUSE else identity


def _cockpit_requester_context(
    request: web.Request,
    cfg: Config,
) -> RequestContext | None:
    auth = request.get("auth", {})
    auth_mode_value = str(auth.get("mode") or "")
    if auth_mode_value == "oauth":
        # OAuth route authorization is anchored on the IdP-guaranteed subject.
        # `jarvis_user` is audit metadata until it is explicitly bound to `sub`.
        identity = str(auth.get("subject") or "").strip()
        users = load_users(cfg.capabilities.users_dir)
        user = users.get(identity)
        if user is None:
            return None
        resolution = Resolution(user.name, user.scope, "strong", user)
        return replace(context_for_resolution(cfg.capabilities, resolution), channel="cockpit")
    elif auth_mode_value in {"legacy", "none"}:
        identity = _legacy_cockpit_requester_id(cfg)
    else:
        identity = ""
    if not identity:
        return None
    return RequestContext(
        device_id=cfg.capabilities.device_id,
        identity=identity,
        scope="personal",
        capabilities=frozenset(resolve_capabilities(cfg.capabilities)),
        channel="cockpit",
        peer=identity,
    )


def _requester_or_401(request: web.Request, cfg: Config) -> RequestContext:
    requester = _cockpit_requester_context(request, cfg)
    if requester is None:
        raise CockpitError("unauthorized", "unauthorized", status=401)
    return requester


def _requester_or_404(request: web.Request, cfg: Config) -> RequestContext:
    requester = _cockpit_requester_context(request, cfg)
    if requester is None:
        raise CockpitError("not_found", "project not found", status=404)
    return requester


async def _project_brain_write(
    ctx: CockpitAppContext,
    requester: RequestContext,
    op: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    try:
        return await _project_brain_client(ctx).execute(requester, op, payload)
    except ProjectOperationError as exc:
        raise CockpitError(
            exc.code,
            str(exc),
            recoverable=exc.recoverable,
            status=exc.status,
        ) from exc
    except (TimeoutError, OSError, RuntimeError) as exc:
        raise CockpitError(
            "brain_unavailable",
            public_error_message(str(exc)),
            recoverable=True,
            status=502,
        ) from exc


def _project_brain_client(ctx: CockpitAppContext) -> BrainProjectClient:
    return BrainProjectClient(ctx.cfg)


def _project_write_response(result: dict[str, Any]) -> web.Response:
    return web.json_response(_project_write_body(result))


def _project_write_body(result: dict[str, Any]) -> dict[str, Any]:
    body = {
        "ok": True,
        "api_version": API_VERSION,
        "schema_version": SCHEMA_VERSION,
        **result,
    }
    if "result" not in result:
        body["result"] = result
    return body


def _project_activity_log(ctx: CockpitAppContext) -> ProjectActivityLog:
    return ProjectActivityLog(ctx.cfg.orchestration.workspace)


async def _record_project_activity(
    ctx: CockpitAppContext,
    project_id: str,
    activity_type: str,
    requester: RequestContext,
    summary: str,
    data: dict[str, Any] | None = None,
) -> None:
    try:
        await asyncio.to_thread(
            _project_activity_log(ctx).append,
            project_id,
            activity_type,
            _activity_actor(requester),
            summary,
            data or {},
        )
    except Exception as exc:  # noqa: BLE001 - activity logging is best-effort.
        logger.warning("project activity append failed for %s: %s", project_id, exc)


def _activity_actor(requester: RequestContext) -> dict[str, Any]:
    return {
        "identity": requester.identity,
        "peer": requester.peer or requester.identity,
        "scope": requester.scope,
        "device_id": requester.device_id,
        "channel": requester.channel,
    }


def _project_id_from_result(result: dict[str, Any], fallback: str = "") -> str:
    project = result.get("project")
    if isinstance(project, dict):
        return str(project.get("id") or fallback)
    return str(result.get("project_id") or fallback)


async def _record_work_dispatched_activity(
    ctx: CockpitAppContext,
    request: web.Request,
    body: dict[str, Any],
    result: StartedWork,
) -> None:
    project_id = _work_project_id(body)
    if not project_id:
        return
    requester = _cockpit_requester_context(request, ctx.cfg)
    if requester is None:
        return
    try:
        registry = await asyncio.to_thread(_registry_store, ctx.cfg)
        project = await asyncio.to_thread(_visible_project_or_404, registry, project_id, requester)
    except Exception as exc:  # noqa: BLE001 - work dispatch must not depend on activity lookup.
        logger.warning("project activity work linkage lookup failed for %s: %s", project_id, exc)
        return
    if project is None:
        return
    await _record_project_activity(
        ctx,
        project.id,
        "work.dispatched",
        requester,
        f"Dispatched work for project {project.id}",
        {
            "project_id": project.id,
            "run_id": result.envelope.run_id,
            "session_id": result.session.session_id,
            "worker_id": result.worker.worker_id,
            "work_source": result.item.source,
            "work_id": result.item.id,
            "title": result.item.title,
        },
    )


def _work_project_id(body: dict[str, Any]) -> str:
    project_id = str(body.get("project_id") or "").strip()
    if project_id:
        return project_id
    work_item = body.get("work_item")
    if isinstance(work_item, dict):
        return str(work_item.get("project_id") or "").strip()
    command = body.get("command")
    if isinstance(command, dict):
        filters = command.get("filters")
        if isinstance(filters, dict):
            return str(filters.get("project_id") or "").strip()
    return ""


async def _project_for_member_route(
    ctx: CockpitAppContext,
    request: web.Request,
    requester: RequestContext,
) -> ProjectEntry:
    registry = await asyncio.to_thread(_registry_store, ctx.cfg)
    project = await asyncio.to_thread(
        _visible_project_or_404,
        registry,
        request.match_info["project_id"],
        requester,
    )
    if project is None:
        raise CockpitError("not_found", "project not found", status=404)
    decision = can_edit_project(requester, project)
    if not decision.allowed:
        raise CockpitError("not_found", "project not found", status=404)
    return project


async def _project_for_owner_route(
    ctx: CockpitAppContext,
    request: web.Request,
    requester: RequestContext,
) -> ProjectEntry:
    registry = await asyncio.to_thread(_registry_store, ctx.cfg)
    project = await asyncio.to_thread(
        _visible_project_or_404,
        registry,
        request.match_info["project_id"],
        requester,
    )
    if project is None:
        raise CockpitError("not_found", "project not found", status=404)
    if not can_edit_project(requester, project).allowed:
        raise CockpitError("not_found", "project not found", status=404)
    if not can_admin_project(requester, project).allowed:
        raise CockpitError("forbidden", "project owner required", status=403)
    return project


def _curation_outbox(cfg: Config) -> CurationOutbox:
    return CurationOutbox(
        cfg.memory.curation_outbox_path,
        max_retries=cfg.memory.curation_outbox_max_retries,
        backoff_initial_s=cfg.memory.curation_outbox_backoff_initial_s,
        backoff_max_s=cfg.memory.curation_outbox_backoff_max_s,
    )


async def _multipart_upload_payload(request: web.Request) -> dict[str, Any]:
    if not request.content_type.startswith("multipart/"):
        raise CockpitError("validation_failed", "file upload must be multipart/form-data", status=400, recoverable=True)
    reader = await request.multipart()
    payload: dict[str, Any] = {}
    file_seen = False
    async for part in reader:
        if part.name == "file":
            data = await part.read(decode=False)
            if not data:
                raise CockpitError("validation_failed", "uploaded file is empty", status=400, recoverable=True)
            payload["filename"] = part.filename or "upload"
            payload["content_base64"] = base64.b64encode(data).decode("ascii")
            payload["mime_type"] = part.headers.get("Content-Type", "")
            file_seen = True
            continue
        if part.name:
            payload[part.name] = await part.text()
    if not file_seen:
        raise CockpitError("validation_failed", "multipart upload requires a file part", status=400, recoverable=True)
    return payload


def _members_from_body(body: dict[str, Any]) -> list[str]:
    if "members" in body:
        value = body["members"]
        if not isinstance(value, list):
            raise CockpitError("validation_failed", "members must be an array", status=400, recoverable=True)
        return [str(item).strip() for item in value if str(item).strip()]
    member = str(body.get("member") or body.get("member_id") or body.get("identity") or "").strip()
    if not member:
        raise CockpitError("validation_failed", "member is required", status=400, recoverable=True)
    return [member]


def _unique_member_list(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = value.strip()
        key = item.lower()
        if item and key not in seen:
            out.append(item)
            seen.add(key)
    return out


def _registry_projects(registry: RegistryStore) -> list[ProjectEntry]:
    return list(registry._projects.values())  # noqa: SLF001


def _visible_projects(
    registry: RegistryStore,
    requester: RequestContext,
    *,
    include_archived: bool,
) -> list[ProjectEntry]:
    projects = [
        project
        for project in _registry_projects(registry)
        if (include_archived or project.status != "archived")
        and _project_access_allowed(registry, requester, project)
    ]
    return sorted(projects, key=lambda project: project.name.lower())


def _visible_project_or_404(
    registry: RegistryStore,
    project_id: str,
    requester: RequestContext,
) -> ProjectEntry | None:
    project = registry.get_project(project_id)
    if project is None or not _project_access_allowed(registry, requester, project):
        return None
    return project


def _project_access_allowed(
    registry: RegistryStore,
    requester: RequestContext,
    project: ProjectEntry,
) -> bool:
    return can_query_memory_peer(requester, project.peer_id, registry=registry).allowed


def _project_memory(memory: Any, project: ProjectEntry) -> tuple[str, list[dict[str, str]]]:
    representation = _project_representation(memory, project.peer_id)
    conclusions = _project_memory_conclusions(memory, project)
    return representation, conclusions


def _project_representation(memory: Any, peer_id: str) -> str:
    representation = ""
    try:
        representation = str(memory.read_cached_representation(peer_id) or "")
    except Exception as exc:  # pragma: no cover - defensive best-effort boundary
        logger.debug("project memory cache read failed for %s: %s", peer_id, exc)
    try:
        live = memory.read_representation(peer_id)
    except (UnsupportedMemoryOperation, TimeoutError, OSError, RuntimeError) as exc:
        logger.debug("project memory live representation unavailable for %s: %s", peer_id, exc)
    except Exception as exc:  # pragma: no cover - defensive best-effort boundary
        logger.debug("project memory live representation failed for %s: %s", peer_id, exc)
    else:
        representation = str(live.representation or representation)
    return representation


def _project_memory_conclusions(memory: Any, project: ProjectEntry) -> list[dict[str, str]]:
    rows: list[ConclusionRecord] = []
    for artifact_type in ("finding", "decision"):
        try:
            rows.extend(
                memory.list_conclusions(
                    observed_id=project.peer_id,
                    level="explicit",
                    metadata={"project_id": project.id, "artifact_type": artifact_type},
                )
            )
        except (UnsupportedMemoryOperation, TimeoutError, OSError, RuntimeError) as exc:
            logger.debug("project memory conclusions unavailable for %s: %s", project.peer_id, exc)
            continue
        except Exception as exc:  # pragma: no cover - defensive best-effort boundary
            logger.debug("project memory conclusions failed for %s: %s", project.peer_id, exc)
            continue
    rows.sort(key=_conclusion_sort_key, reverse=True)
    return [_project_memory_conclusion(row) for row in rows[:20]]


def _conclusion_sort_key(conclusion: ConclusionRecord) -> tuple[str, str]:
    return (str(conclusion.metadata.get("observed_at") or ""), conclusion.id)


def _project_memory_conclusion(conclusion: ConclusionRecord) -> dict[str, str]:
    metadata = conclusion.metadata
    return {
        "id": conclusion.id,
        "content": conclusion.content,
        "artifact_type": str(metadata.get("artifact_type") or ""),
        "recorded_by": str(metadata.get("recorded_by") or ""),
        "observed_at": str(metadata.get("observed_at") or ""),
    }


def _cockpit_connector(ctx: CockpitAppContext) -> CockpitConnector:
    return CockpitConnector(ctx.cfg)


def _start_thread_queue_drain(ctx: CockpitAppContext, project_id: str, thread_id: str) -> None:
    key = (project_id, thread_id)
    index = CockpitThreadIndex(Path(ctx.cfg.orchestration.workspace) / THREAD_INDEX_FILENAME)
    if not index.has_queued_turns(project_id, thread_id):
        return
    with THREAD_QUEUE_DRAIN_LOCK:
        if key in THREAD_QUEUE_DRAINS:
            return
        THREAD_QUEUE_DRAINS.add(key)

    def run() -> None:
        try:
            project = RegistryStore(ctx.cfg.registry.path).get_project(project_id)
            if project is None:
                return

            async def drain_until_idle() -> None:
                connector = _cockpit_connector(ctx)
                deadline = asyncio.get_running_loop().time() + max(
                    1.0,
                    float(ctx.cfg.worker.job_timeout_s),
                )
                while index.has_queued_turns(project_id, thread_id):
                    await connector.drain_queued_turns(project, thread_id)
                    if not index.has_queued_turns(project_id, thread_id):
                        return
                    if asyncio.get_running_loop().time() >= deadline:
                        return
                    await asyncio.sleep(0.25)

            asyncio.run(drain_until_idle())
        except Exception:
            logger.exception("project thread queue drain failed for %s/%s", project_id, thread_id)
        finally:
            with THREAD_QUEUE_DRAIN_LOCK:
                THREAD_QUEUE_DRAINS.discard(key)

    threading.Thread(
        target=run,
        name=f"jarvis-thread-queue-{thread_id}",
        daemon=True,
    ).start()


def _rearm_thread_queues(ctx: CockpitAppContext) -> None:
    index = CockpitThreadIndex(Path(ctx.cfg.orchestration.workspace) / THREAD_INDEX_FILENAME)
    for thread in index._threads().values():  # noqa: SLF001 - startup recovery scans durable thread state.
        if thread.archived_at:
            continue
        index.recover_dispatching_turns(thread.project_id, thread.thread_id)
        # An execution lease cannot outlive the process that held it; releasing
        # it here is what lets queued turns drain after a restart.
        index.recover_orphaned_execution(thread.project_id, thread.thread_id)
        thread = index.get(thread.project_id, thread.thread_id) or thread
        if not thread.queued_turns:
            continue
        index.rearm_queued_turns(thread.project_id, thread.thread_id)
        _start_thread_queue_drain(ctx, thread.project_id, thread.thread_id)


def _make_thread_child_terminal_notifier(cfg: Config) -> Callable[[str, Any], bool]:
    return make_child_terminal_notifier(cfg)


def _make_thread_children_promoter(cfg: Config) -> Callable[[str], object]:
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / THREAD_INDEX_FILENAME)
    return index.promote_children


def _thread_projection(
    thread: CockpitThread,
    ctx: CockpitAppContext | None = None,
    *,
    include_messages: bool = False,
) -> dict[str, Any]:
    operational_state, diagnostic_reason = _thread_operational_state(thread, ctx)
    status, ended_reason = _thread_status(thread, ctx)
    lifecycle = "archived" if thread.archived_at else "open"
    data = {
        "conversation_id": thread.thread_id,
        "thread_id": thread.thread_id,
        "chat_id": thread.thread_id,
        "parent_chat_id": thread.parent_chat_id,
        "project_id": thread.project_id,
        "session_id": thread.session_id,
        "title": thread.title,
        "chat_type": thread.chat_type,
        "engine": thread.engine,
        "model": thread.model or (_thread_model(ctx) if thread.engine == "jarvis" else ""),
        "worker_id": thread.worker_id or None,
        "host": _BRAIN_HOSTNAME if thread.engine == "jarvis" else "",
        "lifecycle": lifecycle,
        "operational_state": operational_state,
        # Preserve the original v1 turn-derived contract while clients migrate
        # to lifecycle + operational_state for durable conversations.
        "status": status,
        "ended_reason": ended_reason or None,
        "diagnostic_reason": diagnostic_reason or None,
        "created_at": thread.created_at,
        "updated_at": thread.updated_at,
        "created_by": thread.created_by,
        "archived_at": thread.archived_at,
        "archived_by": thread.archived_by,
        "archive_reason": thread.archive_reason,
        "last_turn_at": thread.last_turn_at or None,
    }
    if is_conversation_workspace(thread.workspace):
        data["workspace"] = workspace_public(thread.workspace)
    if include_messages:
        data["messages"] = [dict(message) for message in thread.messages]
    return data


async def _try_write_thread_event(response: web.StreamResponse, cursor: str, event: dict[str, Any]) -> None:
    event_type = canonical_event_type(event.get("type"))
    if not event_type:
        return
    try:
        await _write_sse(response, event_type, cursor, _sse_envelope(cursor, event_type, dict(event)))
    except Exception as exc:  # noqa: BLE001 - thread tool events are best-effort.
        logger.debug("failed to write project-thread event %s: %s", event_type, exc)


_BRAIN_HOSTNAME = socket.gethostname()


def _thread_model(ctx: CockpitAppContext | None) -> str:
    if ctx is None:
        return ""
    gateway = ctx.cfg.gateway
    return str(gateway.voice_model or gateway.fast_model)


def _thread_operational_state(thread: CockpitThread, ctx: CockpitAppContext | None) -> tuple[str, str]:
    if thread.archived_at:
        return "archived", ""
    live = ctx.thread_turn_states.get((thread.project_id, thread.thread_id)) if ctx is not None else None
    if live is not None:
        status, reason, _turn_id, _started_at = _thread_turn_state_values(live)
        return {
            "created": "starting",
            "running": "working",
            "completed": "idle",
            "failed": "degraded",
        }.get(status, status), reason
    if str(thread.workspace.get("status") or "") == "failed":
        return "degraded", "engine_error"
    # The parent's own turn has ended, but a watch it registered is still
    # pending — the thread is genuinely waiting on its children, not idle.
    if thread.workspace.get("pending_child_watch_ids"):
        return "waiting_for_children", ""
    return "idle", ""


def _thread_status(thread: CockpitThread, ctx: CockpitAppContext | None) -> tuple[str, str]:
    """Return the legacy v1 turn-derived status contract."""
    live = ctx.thread_turn_states.get((thread.project_id, thread.thread_id)) if ctx is not None else None
    if live is not None:
        state, reason, _turn_id, _started_at = _thread_turn_state_values(live)
        if state in {"created", "running", "completed", "failed"}:
            return state, reason
        if state in {"starting", "working", "waiting_for_children", "waiting_for_input", "joining"}:
            return "running", ""
        if state in {"degraded", "blocked"}:
            return "failed", reason or "engine_error"
    legacy = (
        getattr(ctx, "thread_turn_legacy_states", {}).get((thread.project_id, thread.thread_id))
        if ctx is not None
        else None
    )
    if legacy is not None:
        return legacy
    if thread.workspace.get("pending_child_watch_ids"):
        return "running", ""
    if thread.last_turn_at or thread.messages:
        return "completed", "completed"
    return "created", ""


def _thread_turn_state_values(
    state: ThreadTurnState | tuple[str, str] | None,
) -> tuple[str, str, str, str]:
    if isinstance(state, ThreadTurnState):
        return (
            state.operational_state,
            state.diagnostic_reason,
            state.turn_id,
            state.started_at,
        )
    if state is not None:
        return state[0], state[1], "", ""
    return "", "", "", ""


def _thread_detail_projection(
    thread: CockpitThread,
    ctx: CockpitAppContext | None = None,
    *,
    execution: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = _thread_projection(thread, ctx)
    if execution is not None:
        row["execution"] = execution
    row["queued_turns"] = [
        {
            "queue_id": str(item.get("queue_id") or ""),
            "text": str(item.get("text") or ""),
            "queued_at": str(item.get("queued_at") or ""),
            "status": str(item.get("status") or "queued"),
        }
        for item in thread.queued_turns
        if str(item.get("status") or "queued") in {"queued", "claimed"}
    ]
    row["messages"] = [
        {
            "role": message.get("role", ""),
            "peer_id": message.get("peer_id", ""),
            "content": message.get("content", ""),
            "observed_at": message.get("observed_at", ""),
        }
        for message in thread.messages
    ]
    return row


def _thread_execution_projection(thread: CockpitThread, ctx: CockpitAppContext) -> dict[str, Any]:
    worker_id = str(thread.workspace.get("worker_id") or thread.worker_id or "")
    session_id = str(thread.workspace.get("session_id") or "")
    if not worker_id or not session_id:
        return _local_thread_execution_projection(thread, ctx)
    try:
        raw = _worker_get_json(
            ctx.cfg,
            worker_id,
            f"/sessions/{session_id}/execution-state",
            get=ctx.get,
            timeout_s=float(ctx.cfg.orchestration.sse_sync_timeout_s),
        )
        return _public_thread_execution(raw)
    except Exception as exc:  # noqa: BLE001 - execution diagnostics must not hide durable detail.
        message = exc.message if isinstance(exc, CockpitError) else str(exc)
        return _unavailable_thread_execution(public_error_message(message or "worker execution state unavailable"))


def _public_thread_execution(raw: dict[str, Any]) -> dict[str, Any]:
    if (
        not str(raw.get("session_id") or "")
        or not str(raw.get("status") or "")
        or raw.get("active_turn") is not None
        and not isinstance(raw.get("active_turn"), dict)
        or not isinstance(raw.get("pending_requests"), list)
        or not isinstance(raw.get("supported_controls"), list)
        or not isinstance(raw.get("supports"), dict)
    ):
        raise ValueError("worker returned invalid execution state")
    active = raw.get("active_turn") if isinstance(raw.get("active_turn"), dict) else None
    if active is not None and (
        not str(active.get("turn_id") or "")
        or not str(active.get("status") or "")
    ):
        raise ValueError("worker returned invalid active turn")
    pending = raw.get("pending_requests") if isinstance(raw.get("pending_requests"), list) else []
    controls = raw.get("supported_controls") if isinstance(raw.get("supported_controls"), list) else []
    return {
        "available": True,
        "status": str(raw.get("status") or ""),
        "active_turn": (
            {
                "turn_id": str(active.get("turn_id") or ""),
                "status": str(active.get("status") or ""),
                "started_at": str(active.get("started_at") or ""),
            }
            if active is not None
            else None
        ),
        "pending_requests": [
            _public_thread_pending_request(item)
            for item in pending
            if isinstance(item, dict)
            and str(item.get("kind") or "") in {"approval", "input"}
            and bool(str(item.get("request_id") or ""))
        ],
        "supported_controls": [
            str(item)
            for item in controls
            if str(item) in {"turn", "input", "approval", "interrupt", "stop"}
        ],
        "supports": {"steer": False, "queue": True},
        "diagnostic": None,
    }


def _thread_execution_is_active(execution: dict[str, Any]) -> bool:
    return isinstance(execution.get("active_turn"), dict) or str(execution.get("status") or "") in {
        "starting",
        "working",
        "running",
        "waiting_approval",
        "waiting_input",
        "interrupting",
    }


def _thread_turn_error_is_active(exc: BaseException) -> bool:
    code = str(getattr(exc, "code", "") or "")
    message = str(exc).lower()
    return code == "session_active" or "active turn" in message or "already has an active turn" in message


def _public_thread_pending_request(item: dict[str, Any]) -> dict[str, Any]:
    kind = str(item.get("kind") or "")
    result = {
        "request_id": str(item.get("request_id") or ""),
        "kind": kind,
        "status": str(item.get("status") or "pending"),
        "title": public_error_message(str(item.get("title") or "")),
        "detail": public_error_message(str(item.get("detail") or "")),
        "created_at": str(item.get("created_at") or ""),
    }
    if kind == "approval":
        request_kind = str(item.get("request_kind") or "")
        result["request_kind"] = (
            request_kind if request_kind in {"command", "file-read", "file-change"} else "command"
        )
    if kind == "input" and isinstance(item.get("questions"), list):
        result["questions"] = [
            {
                "id": public_error_message(str(question.get("id") or "")),
                "header": public_error_message(str(question.get("header") or "")),
                "question": public_error_message(str(question.get("question") or "")),
                "options": [
                    {
                        "label": public_error_message(str(option.get("label") or "")),
                        "description": public_error_message(str(option.get("description") or "")),
                    }
                    for option in question.get("options", [])
                    if isinstance(option, dict)
                ],
                "multi_select": bool(question.get("multi_select")),
            }
            for question in item["questions"]
            if isinstance(question, dict)
        ]
    return result


def _local_thread_execution_projection(
    thread: CockpitThread,
    ctx: CockpitAppContext,
) -> dict[str, Any]:
    state = ctx.thread_turn_states.get((thread.project_id, thread.thread_id))
    status, _reason, turn_id, started_at = _thread_turn_state_values(state)
    return {
        "available": True,
        "status": status or "idle",
        "active_turn": (
            {
                "turn_id": turn_id,
                "status": status,
                "started_at": started_at,
            }
            if turn_id
            else None
        ),
        "pending_requests": [],
        "supported_controls": ["turn"] if not thread.archived_at else [],
        "supports": {"steer": False, "queue": True},
        "diagnostic": None,
    }


def _unavailable_thread_execution(message: str) -> dict[str, Any]:
    return {
        "available": False,
        "status": "unavailable",
        "active_turn": None,
        "pending_requests": [],
        "supported_controls": [],
        "supports": {"steer": False, "queue": True},
        "diagnostic": {
            "code": "worker_unavailable",
            "message": message,
        },
    }


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


def _cleanup_child_session_worktree(
    cfg: Config,
    store: OrchestrationStore,
    ref: SessionRef,
    body: dict[str, Any],
    *,
    post: HttpPost,
) -> dict[str, Any]:
    """Narrow lifecycle hook for sibling lifecycle-cleanup work.

    The cleanup branch owns inventory/GC. This hook only asks the worker that owns
    the session to reclaim that session's worktree and records best-effort evidence.

    Worker/server.py has no per-session "/sessions/{id}/cleanup" route (that was
    a doomed 404 every close, so the worktree never actually got reclaimed here
    -- it just leaked until the separate GC pass caught it). The worker's real
    contract for this is POST /worktrees/prune with the worktree path as
    `target`; close only stops the session (see the /stop call above), it must
    not delete the session record, so DELETE /sessions/{id} is not used here.
    """
    run_id = _session_run_id_from_store(store, ref)
    target = _session_cwd_from_store(store, ref)
    if not target:
        # Nothing to prune (no recorded worktree cwd) -- report as a no-op
        # rather than guessing a path.
        result: dict[str, Any] = {"requested": False, "ok": True, "cleaned": []}
    else:
        payload = {
            "target": target,
            "stale_ttl_s": 0.0,
            "reason": str(body.get("reason") or "session closed"),
        }
        try:
            raw = _worker_post_json(cfg, ref.worker_id, "/worktrees/prune", payload, post=post)
        except Exception as exc:  # noqa: BLE001 - close/archive must not depend on cleanup availability
            result = {"requested": True, "ok": False, "error": public_error_message(str(exc))}
        else:
            result = {
                "requested": True,
                "ok": bool(raw.get("ok", True)),
                "cleaned": [
                    str(item.get("name") or item) if isinstance(item, dict) else str(item)
                    for item in raw.get("pruned") or []
                ],
            }
    if run_id:
        store.append_event(
            run_id,
            "session_cleanup_requested",
            "Requested worker session worktree cleanup",
            {"session_id": ref.session_id, "worker_id": ref.worker_id, "cleanup": result},
        )
    return result


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
    timeout_s: float | None = None,
) -> dict[str, Any]:
    profile = _worker_profile(cfg, worker_id)
    try:
        response = get(
            f"{profile.base_url}{path}",
            headers=worker_headers(cfg.worker, profile),
            params=params or {},
            timeout=cfg.worker.request_timeout_s if timeout_s is None else timeout_s,
        )
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


def _worker_delete_json(cfg: Config, worker_id: str, path: str, *, delete: HttpDelete) -> dict[str, Any]:
    profile = _worker_profile(cfg, worker_id)
    try:
        response = delete(f"{profile.base_url}{path}", headers=worker_headers(cfg.worker, profile), timeout=cfg.worker.request_timeout_s)
    except Exception as exc:  # noqa: BLE001
        message = public_error_message(str(exc) or "worker delete failed")
        raise CockpitError("worker_unavailable", message, recoverable=True, status=502) from exc
    status = getattr(response, "status_code", 200)
    try:
        data = response.json() if hasattr(response, "json") else {}
    except Exception:
        data = {}
        if status < 400:
            raise CockpitError("worker_unavailable", "worker returned invalid JSON", recoverable=True, status=502) from None
    if not isinstance(data, dict):
        data = {}
    if status == 404:
        raise CockpitError("not_found", "worker resource not found", status=404)
    if status == 409:
        raise CockpitError("conflict", public_error_message(str(data.get("error") or "worker refused delete")), recoverable=True, status=409)
    if status == 401:
        raise CockpitError("worker_unavailable", "worker authentication failed", recoverable=True, status=502)
    if status >= 400:
        raise CockpitError("worker_unavailable", _response_error(response) or "worker delete failed", recoverable=True, status=502)
    return data


def _worker_session_row(raw: dict[str, Any], worker_id: str) -> dict[str, Any]:
    session_id = str(raw.get("session_id") or "")
    return build_session_row(
        session_ref=make_session_ref(worker_id, session_id),
        worker_id=worker_id,
        session_id=session_id,
        run_id=str(raw.get("run_id") or ""),
        parent_chat_id=str(raw.get("parent_chat_id") or ""),
        project_id=str(raw.get("project_id") or ""),
        title=str(raw.get("title") or ""),
        provider=str(raw.get("provider") or ""),
        engine=str(raw.get("engine") or raw.get("provider") or ""),
        status=str(raw.get("status") or ""),
        ended_reason=_ended_reason_from_worker_session(raw),
        repo=str(raw.get("repo") or ""),
        branch=str(raw.get("branch") or ""),
        cwd=str(raw.get("cwd") or ""),
        latest_event_cursor="",
        created_at=str(raw.get("created_at") or ""),
        updated_at=str(raw.get("updated_at") or ""),
        allowed_actions=_allowed_actions_from_worker_session(raw),
        include_archived_at=False,
    )


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


def _session_cwd_from_store(store: OrchestrationStore, ref: SessionRef) -> str:
    for run in store.list_runs():
        for link in run.sessions:
            if link.worker_id == ref.worker_id and link.session_id == ref.session_id:
                return link.cwd
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


def _mcp_token_add(path: str, *, principal: str, name: str) -> tuple[str, Any]:
    with MCP_TOKEN_STORE_LOCK:
        return MCPTokenStore(path).add(principal=principal, name=name)


def _mcp_token_revoke(path: str, token_id: str):  # noqa: ANN202
    with MCP_TOKEN_STORE_LOCK:
        return MCPTokenStore(path).revoke(token_id)


def _mcp_token_revoke_after_failed_issue(path: str, token_id: str) -> None:
    try:
        _mcp_token_revoke(path, token_id)
    except MCPTokenError as exc:
        logger.error(
            "mcp token rollback failed token_id=%s error=%s",
            token_id,
            public_error_message(str(exc)),
        )


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
    import contextlib
    import faulthandler
    import signal as _signal

    # SIGUSR1 dumps every thread's Python stack to stderr (the launchd error
    # log) so a wedged process can be diagnosed without killing it.
    with contextlib.suppress(AttributeError, ValueError, RuntimeError):
        faulthandler.register(_signal.SIGUSR1, all_threads=True)
    app = make_app(cfg)
    runner = web.AppRunner(app, keepalive_timeout=15)
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
