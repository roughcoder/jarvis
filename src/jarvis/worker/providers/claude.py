from __future__ import annotations

import asyncio
import contextlib
import queue
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field, is_dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from jarvis.config import WorkerConfig
from jarvis.worker.authority import WorkerSessionAuthority
from jarvis.worker.providers.base import ProviderTurn
from jarvis.worker.sessions import SessionEvent, SessionManager, WorkerSession
from jarvis.worker.workspaces import is_worker_owned_path_for_config
from jarvis.worker_session_contract import (
    CANCELLED_SESSION_STATUSES,
    EVENT_APPROVAL_REQUESTED,
    EVENT_APPROVAL_RESOLVED,
    EVENT_ASSISTANT_MESSAGE,
    EVENT_INPUT_RECEIVED,
    EVENT_INPUT_REQUESTED,
    EVENT_PROVIDER_EVENT,
    EVENT_PROVIDER_LOG,
    EVENT_PROVIDER_PROCESS_STARTED,
    EVENT_PROVIDER_SESSION_READY,
    EVENT_PROVIDER_STARTED,
    EVENT_SESSION_INTERRUPTED,
    EVENT_SESSION_STOPPED,
    EVENT_TOOL_CALL,
    EVENT_TOOL_RESULT,
    EVENT_TURN_COMPLETED,
    EVENT_TURN_FAILED,
    REQUEST_KIND_APPROVAL,
    REQUEST_KIND_INPUT,
    SESSION_COMPLETED,
    SESSION_FAILED,
    SESSION_INTERRUPTED,
    SESSION_RUNNING,
    SESSION_STOPPED,
    SESSION_WAITING_APPROVAL,
    SESSION_WAITING_INPUT,
)

_RUNTIME_LOCK = threading.RLock()
_RUNTIMES: dict[str, _ClaudeSessionRuntime] = {}
_SDK: Any | None = None


class ClaudeProviderAdapter:
    provider = "claude"

    def capabilities(self) -> dict[str, Any]:
        return {
            "streaming": True,
            "resume": True,
            "interrupt": True,
            "approvals": True,
            "questions": True,
            "checkpoints": False,
            "rollback": False,
            "attachments": True,
            "runtime": "claude-agent-sdk in-process session runtime",
            "attached": True,
            "lifecycle": "one long-lived ClaudeSDKClient per worker session; turns are sent over streaming input",
            "backpressure": "provider thread owns the SDK asyncio loop and serializes turn ingestion",
            "timeouts": "turn timeout maps to turn.failed; approvals/questions deny after approval_timeout_s (0 = job timeout) with the turn clock paused while pending",
            "crash_recovery": "session/event history is durable; restarted workers resume from stored Claude session id",
            "event_ordering": "Claude SDK messages project to append-only SessionEvent order",
            "include_partial_messages": False,
        }

    def start_turn(
        self,
        *,
        session: WorkerSession,
        turn: ProviderTurn,
        sessions: SessionManager,
        worker_cfg: WorkerConfig,
    ) -> list[SessionEvent]:
        _load_sdk()
        authority = WorkerSessionAuthority.from_session(session, provider=self.provider)
        sessions.update_status(session.session_id, SESSION_RUNNING)
        claude_session_id = _claude_session_id(session)
        sessions.update_metadata(
            session.session_id,
            {
                "claude_session_id": claude_session_id,
                "provider_session_id": claude_session_id,
                "provider_runtime": "claude-agent-sdk",
            },
        )
        started = sessions.append_event(
            session.session_id,
            EVENT_PROVIDER_STARTED,
            {
                "turn_id": turn.turn_id,
                "idempotency_key": turn.idempotency_key,
                "provider": self.provider,
                "runtime": "claude-agent-sdk",
                "provider_session_id": claude_session_id,
            },
        )
        _runtime_for_session(
            session=session,
            sessions=sessions,
            worker_cfg=worker_cfg,
            authority=authority,
            claude_session_id=claude_session_id,
            turn=turn,
        )
        return [started]

    def resolve_approval(
        self,
        *,
        session: WorkerSession,
        request: dict[str, Any],
        sessions: SessionManager,
    ) -> SessionEvent:
        authority = WorkerSessionAuthority.from_session(session, provider=self.provider)
        if not authority.can_resolve_approval:
            raise RuntimeError("worker session missing required authority: worker.session.approve")
        request_id = _control_request_id(request)
        runtime = _runtime_for_existing_session(session.session_id)
        if runtime is None:
            raise RuntimeError(f"no pending claude approval request {request_id!r}")
        event = runtime.resolve_request(
            request_id,
            kind=REQUEST_KIND_APPROVAL,
            request=request,
            sessions=sessions,
        )
        if event is None:
            raise RuntimeError(f"no pending claude approval request {request_id!r}")
        return event

    def receive_input(
        self,
        *,
        session: WorkerSession,
        request: dict[str, Any],
        sessions: SessionManager,
    ) -> SessionEvent:
        authority = WorkerSessionAuthority.from_session(session, provider=self.provider)
        if not authority.can_receive_input:
            raise RuntimeError("worker session missing required authority: worker.session.input")
        request_id = _control_request_id(request)
        runtime = _runtime_for_existing_session(session.session_id)
        if runtime is None:
            raise RuntimeError(f"no pending claude input request {request_id!r}")
        event = runtime.resolve_request(
            request_id,
            kind=REQUEST_KIND_INPUT,
            request=request,
            sessions=sessions,
        )
        if event is None:
            raise RuntimeError(f"no pending claude input request {request_id!r}")
        return event

    def interrupt(self, *, session: WorkerSession, sessions: SessionManager) -> tuple[WorkerSession, SessionEvent]:
        runtime = _runtime_for_existing_session(session.session_id)
        if runtime is not None:
            runtime.stop()
        updated = sessions.update_status(session.session_id, SESSION_INTERRUPTED)
        event = sessions.append_event(updated.session_id, EVENT_SESSION_INTERRUPTED, {"status": SESSION_INTERRUPTED})
        return updated, event

    def stop(self, *, session: WorkerSession, sessions: SessionManager) -> tuple[WorkerSession, SessionEvent]:
        runtime = _runtime_for_existing_session(session.session_id)
        if runtime is not None:
            runtime.stop()
        updated = sessions.update_status(session.session_id, SESSION_STOPPED)
        event = sessions.append_event(updated.session_id, EVENT_SESSION_STOPPED, {"status": SESSION_STOPPED})
        return updated, event


@dataclass
class _PendingClaudeRequest:
    kind: str
    request_id: str
    tool_name: str
    tool_input: dict[str, Any]
    turn: ProviderTurn
    done: threading.Event = field(default_factory=threading.Event)
    response: dict[str, Any] = field(default_factory=dict)


@dataclass
class _QueuedTurn:
    turn: ProviderTurn


class _StopRuntime:
    pass


class _ClaudeSessionRuntime:
    def __init__(
        self,
        *,
        session: WorkerSession,
        sessions: SessionManager,
        worker_cfg: WorkerConfig,
        authority: WorkerSessionAuthority,
        claude_session_id: str,
    ) -> None:
        self.session_id = session.session_id
        self.sessions = sessions
        self.worker_cfg = worker_cfg
        self.authority = authority
        self.claude_session_id = claude_session_id
        self.cwd = _session_cwd(session, worker_cfg)
        self.model = str(session.metadata.get("model") or "")
        self.resume = bool(str(session.metadata.get("claude_session_started") or "").strip())
        self._queue: queue.Queue[_QueuedTurn | _StopRuntime] = queue.Queue()
        self._pending_lock = threading.RLock()
        self._pending: dict[str, _PendingClaudeRequest] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: Any | None = None
        self._close_requested = threading.Event()
        self._current_turn: ProviderTurn | None = None
        self._turn_deadline = 0.0
        self._turn_timeout: asyncio.Timeout | None = None
        self._process_started_recorded = False
        self._thread = threading.Thread(
            target=self._thread_main,
            name=f"jarvis-claude-sdk-{self.session_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    @property
    def alive(self) -> bool:
        return self._thread.is_alive() and not self._close_requested.is_set()

    def enqueue(self, turn: ProviderTurn) -> None:
        self._queue.put(_QueuedTurn(turn))

    def resolve_request(
        self,
        request_id: str,
        *,
        kind: str,
        request: dict[str, Any],
        sessions: SessionManager,
    ) -> SessionEvent | None:
        with self._pending_lock:
            pending = self._pending.get(request_id)
            if pending is None or pending.kind != kind:
                return None
            event_type = EVENT_APPROVAL_RESOLVED if kind == REQUEST_KIND_APPROVAL else EVENT_INPUT_RECEIVED
            waiting_status = SESSION_WAITING_APPROVAL if kind == REQUEST_KIND_APPROVAL else SESSION_WAITING_INPUT
            event = sessions.append_event(self.session_id, event_type, {**request, "request_id": request_id})
            pending.response = dict(request)
            self._pending.pop(request_id, None)
            pending.done.set()
        _restore_running_if_waiting(sessions, self.session_id, waiting_status, self.has_pending_requests)
        return event

    def has_pending_requests(self) -> bool:
        with self._pending_lock:
            return bool(self._pending)

    def interrupt(self) -> None:
        self._schedule_client_interrupt()

    def stop(self) -> None:
        self._close_requested.set()
        self._schedule_client_interrupt()
        self._resolve_pending_requests_on_shutdown("claude session stopped before request was resolved")
        self._queue.put(_StopRuntime())
        if self._thread.is_alive():
            self._thread.join(timeout=5)

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._amain())
        finally:
            with _RUNTIME_LOCK:
                if _RUNTIMES.get(self.session_id) is self:
                    _RUNTIMES.pop(self.session_id, None)

    async def _amain(self) -> None:
        self._loop = asyncio.get_running_loop()
        sdk = _load_sdk()
        options = sdk.ClaudeAgentOptions(
            cwd=self.cwd,
            model=self.model or None,
            permission_mode=self.authority.claude_permission_mode,
            session_id=None if self.resume else self.claude_session_id,
            resume=self.claude_session_id if self.resume else None,
            cli_path=self.worker_cfg.claude_bin or None,
            system_prompt={"type": "preset", "preset": "claude_code"},
            setting_sources=None,
            include_partial_messages=False,
            can_use_tool=self._can_use_tool,
            stderr=self._record_stderr,
        )
        client = sdk.ClaudeSDKClient(options=options)
        self._client = client
        try:
            await client.connect()
            while not self._close_requested.is_set():
                item = await asyncio.to_thread(self._queue.get)
                if isinstance(item, _StopRuntime):
                    return
                await self._run_turn(client, item.turn)
        except Exception as exc:  # noqa: BLE001 - provider failures must become session events
            turn = self._current_turn or self._pop_unstarted_turn()
            if turn is not None and not _session_cancelled(self.sessions, self.session_id):
                self.sessions.append_event_with_status(
                    self.session_id,
                    SESSION_FAILED,
                    EVENT_TURN_FAILED,
                    {"turn_id": turn.turn_id, "idempotency_key": turn.idempotency_key, "provider": "claude", "error": str(exc)},
                )
        finally:
            self._fail_pending_requests("claude session closed before request was resolved")
            with contextlib.suppress(Exception):
                await client.disconnect()

    async def _run_turn(self, client: Any, turn: ProviderTurn) -> None:
        self._current_turn = turn
        timeout_s = max(1.0, float(self.worker_cfg.job_timeout_s))
        self._turn_deadline = time.monotonic() + timeout_s
        if not self._process_started_recorded:
            self._process_started_recorded = True
            self._record_provider_process_started()
        try:
            if _session_cancelled(self.sessions, self.session_id):
                return
            await client.set_permission_mode(self.authority.claude_permission_mode)
            await client.query(_turn_query_input(turn))
            terminal_seen = False
            async with asyncio.timeout(timeout_s) as turn_timeout:
                self._turn_timeout = turn_timeout
                async for message in client.receive_response():
                    if _project_sdk_message(
                        message,
                        session_id=self.session_id,
                        turn=turn,
                        sessions=self.sessions,
                    ):
                        terminal_seen = True
            if terminal_seen:
                return
            if _session_cancelled(self.sessions, self.session_id):
                return
            self.sessions.append_event_with_status(
                self.session_id,
                SESSION_FAILED,
                EVENT_TURN_FAILED,
                {
                    "turn_id": turn.turn_id,
                    "idempotency_key": turn.idempotency_key,
                    "provider": "claude",
                    "error": "claude response ended without result",
                },
            )
        except TimeoutError:
            self._timeout_pending_requests(turn)
            if _session_cancelled(self.sessions, self.session_id):
                return
            self.sessions.append_event_with_status(
                self.session_id,
                SESSION_FAILED,
                EVENT_TURN_FAILED,
                {
                    "turn_id": turn.turn_id,
                    "idempotency_key": turn.idempotency_key,
                    "provider": "claude",
                    "error": "claude turn timed out",
                },
            )
            self._schedule_client_interrupt()
        except Exception as exc:  # noqa: BLE001 - provider failures must become session events
            if _session_cancelled(self.sessions, self.session_id):
                return
            self.sessions.append_event_with_status(
                self.session_id,
                SESSION_FAILED,
                EVENT_TURN_FAILED,
                {
                    "turn_id": turn.turn_id,
                    "idempotency_key": turn.idempotency_key,
                    "provider": "claude",
                    "error": str(exc),
                },
            )
        finally:
            self._current_turn = None
            self._turn_deadline = 0.0
            self._turn_timeout = None

    async def _can_use_tool(self, tool_name: str, tool_input: dict[str, Any], context: Any) -> Any:
        sdk = _load_sdk()
        tool_input = dict(tool_input or {})
        turn = self._current_turn
        if turn is None:
            return sdk.PermissionResultDeny(message="no active Jarvis turn for Claude tool request")
        if tool_name == "AskUserQuestion":
            if not self.authority.can_receive_input:
                return sdk.PermissionResultDeny(message="worker session missing required authority: worker.session.input")
            response = await self._await_control_request(
                kind=REQUEST_KIND_INPUT,
                tool_name=tool_name,
                tool_input=tool_input,
                context=context,
                turn=turn,
            )
            if response is None:
                return sdk.PermissionResultDeny(message="input request timed out")
            return sdk.PermissionResultAllow(updated_input=_updated_question_input(tool_input, response))
        denial = self.authority.claude_tool_denial(tool_name)
        if denial:
            return sdk.PermissionResultDeny(message=denial)
        if not self.authority.can_resolve_approval:
            return sdk.PermissionResultDeny(message="worker session missing required authority: worker.session.approve")
        response = await self._await_control_request(
            kind=REQUEST_KIND_APPROVAL,
            tool_name=tool_name,
            tool_input=tool_input,
            context=context,
            turn=turn,
        )
        if response is None:
            return sdk.PermissionResultDeny(message="approval timed out")
        if _approval_allowed(response):
            return sdk.PermissionResultAllow()
        return sdk.PermissionResultDeny(message=str(response.get("message") or "approval denied"))

    async def _await_control_request(
        self,
        *,
        kind: str,
        tool_name: str,
        tool_input: dict[str, Any],
        context: Any,
        turn: ProviderTurn,
    ) -> dict[str, Any] | None:
        request_id = str(getattr(context, "tool_use_id", "") or uuid.uuid4())
        pending = _PendingClaudeRequest(
            kind=kind,
            request_id=request_id,
            tool_name=tool_name,
            tool_input=tool_input,
            turn=turn,
        )
        event_type = EVENT_APPROVAL_REQUESTED if kind == REQUEST_KIND_APPROVAL else EVENT_INPUT_REQUESTED
        waiting_status = SESSION_WAITING_APPROVAL if kind == REQUEST_KIND_APPROVAL else SESSION_WAITING_INPUT
        data = {
            "turn_id": turn.turn_id,
            "idempotency_key": turn.idempotency_key,
            "provider": "claude",
            "request_id": request_id,
            "tool_name": tool_name,
            "input": tool_input,
            "title": str(getattr(context, "title", "") or ""),
            "display_name": str(getattr(context, "display_name", "") or ""),
            "description": str(getattr(context, "description", "") or ""),
            "decision_reason": str(getattr(context, "decision_reason", "") or ""),
            "blocked_path": str(getattr(context, "blocked_path", "") or ""),
        }
        with self._pending_lock:
            self._pending[request_id] = pending
        self.sessions.append_event(self.session_id, event_type, data)
        self.sessions.update_status(self.session_id, waiting_status)
        # A human answering is not turn compute: the wait gets its own budget
        # (approval_timeout_s; 0 = inherit job_timeout_s) and the turn clock
        # pauses while the request is pending.
        wait_s = max(0.1, float(self.worker_cfg.approval_timeout_s) or float(self.worker_cfg.job_timeout_s))
        remaining_s = max(0.1, self._turn_deadline - time.monotonic()) if self._turn_deadline else wait_s
        self._pause_turn_clock(wait_s)
        resolved = await asyncio.to_thread(pending.done.wait, wait_s)
        self._resume_turn_clock(remaining_s)
        if resolved:
            return dict(pending.response)
        with self._pending_lock:
            self._pending.pop(request_id, None)
        timeout_request = {
            "request_id": request_id,
            "decision": "denied",
            "message": "input request timed out" if kind == REQUEST_KIND_INPUT else "approval timed out",
            "provider_resolved": True,
        }
        event_type = EVENT_INPUT_RECEIVED if kind == REQUEST_KIND_INPUT else EVENT_APPROVAL_RESOLVED
        self.sessions.append_event(self.session_id, event_type, timeout_request)
        _restore_running_if_waiting(self.sessions, self.session_id, waiting_status, self.has_pending_requests)
        return None

    def _pause_turn_clock(self, wait_s: float) -> None:
        turn_timeout = self._turn_timeout
        loop = self._loop
        if turn_timeout is None or loop is None:
            return
        with contextlib.suppress(Exception):
            turn_timeout.reschedule(loop.time() + wait_s + 5.0)

    def _resume_turn_clock(self, remaining_s: float) -> None:
        self._turn_deadline = time.monotonic() + remaining_s
        turn_timeout = self._turn_timeout
        loop = self._loop
        if turn_timeout is None or loop is None:
            return
        with contextlib.suppress(Exception):
            turn_timeout.reschedule(loop.time() + remaining_s)

    def _timeout_pending_requests(self, turn: ProviderTurn) -> None:
        with self._pending_lock:
            pending_items = list(self._pending.values())
            self._pending.clear()
        for pending in pending_items:
            message = "input request timed out" if pending.kind == REQUEST_KIND_INPUT else "approval timed out"
            event_type = EVENT_INPUT_RECEIVED if pending.kind == REQUEST_KIND_INPUT else EVENT_APPROVAL_RESOLVED
            self.sessions.append_event(
                self.session_id,
                event_type,
                {
                    "turn_id": turn.turn_id,
                    "idempotency_key": turn.idempotency_key,
                    "provider": "claude",
                    "request_id": pending.request_id,
                    "decision": "denied",
                    "message": message,
                    "provider_resolved": True,
                },
            )
            pending.response = {"request_id": pending.request_id, "decision": "denied", "message": message}
            pending.done.set()

    def _fail_pending_requests(self, message: str) -> None:
        with self._pending_lock:
            pending_items = list(self._pending.values())
            self._pending.clear()
        for pending in pending_items:
            pending.response = {"request_id": pending.request_id, "decision": "denied", "message": message}
            pending.done.set()

    def _resolve_pending_requests_on_shutdown(self, message: str) -> None:
        with self._pending_lock:
            pending_items = list(self._pending.values())
            self._pending.clear()
        for pending in pending_items:
            event_type = EVENT_INPUT_RECEIVED if pending.kind == REQUEST_KIND_INPUT else EVENT_APPROVAL_RESOLVED
            waiting_status = SESSION_WAITING_INPUT if pending.kind == REQUEST_KIND_INPUT else SESSION_WAITING_APPROVAL
            self.sessions.append_event(
                self.session_id,
                event_type,
                {
                    "turn_id": pending.turn.turn_id,
                    "idempotency_key": pending.turn.idempotency_key,
                    "provider": "claude",
                    "request_id": pending.request_id,
                    "decision": "denied",
                    "message": message,
                    "provider_resolved": True,
                },
            )
            pending.response = {"request_id": pending.request_id, "decision": "denied", "message": message}
            pending.done.set()
            _restore_running_if_waiting(self.sessions, self.session_id, waiting_status, self.has_pending_requests)

    def _pop_unstarted_turn(self) -> ProviderTurn | None:
        try:
            while True:
                item = self._queue.get_nowait()
                if isinstance(item, _QueuedTurn):
                    return item.turn
        except queue.Empty:
            return None

    def _schedule_client_interrupt(self) -> None:
        loop = self._loop
        client = self._client
        if loop is None or client is None or loop.is_closed():
            return
        future = asyncio.run_coroutine_threadsafe(client.interrupt(), loop)
        with contextlib.suppress(Exception):
            future.result(timeout=2)

    def _record_stderr(self, text: str) -> None:
        turn = self._current_turn
        if turn is None:
            return
        _record_provider_log(self.session_id, turn, self.sessions, text)

    def _record_provider_process_started(self) -> None:
        turn = self._current_turn
        data = {
            "turn_id": turn.turn_id if turn else "",
            "idempotency_key": turn.idempotency_key if turn else "",
            "provider": "claude",
            "pid": "",
            "cwd": self.cwd,
            "resume": self.resume,
            "runtime": "claude-agent-sdk",
        }
        self.sessions.append_event(self.session_id, EVENT_PROVIDER_PROCESS_STARTED, data)


def _runtime_for_session(
    *,
    session: WorkerSession,
    sessions: SessionManager,
    worker_cfg: WorkerConfig,
    authority: WorkerSessionAuthority,
    claude_session_id: str,
    turn: ProviderTurn,
) -> _ClaudeSessionRuntime:
    with _RUNTIME_LOCK:
        runtime = _RUNTIMES.get(session.session_id)
        if runtime is not None and runtime.alive:
            runtime.authority = authority
            runtime.enqueue(turn)
            return runtime
        runtime = _ClaudeSessionRuntime(
            session=session,
            sessions=sessions,
            worker_cfg=worker_cfg,
            authority=authority,
            claude_session_id=claude_session_id,
        )
        _RUNTIMES[session.session_id] = runtime
        runtime.enqueue(turn)
        runtime.start()
        return runtime


def _turn_query_input(turn: ProviderTurn) -> Any:
    """Plain prompt string, or a streamed user message with image content blocks."""
    if not turn.attachments:
        return turn.prompt
    blocks: list[dict[str, Any]] = []
    if turn.prompt:
        blocks.append({"type": "text", "text": turn.prompt})
    for attachment in turn.attachments:
        data_url = str(attachment.get("data_url") or "")
        payload = data_url.split(",", 1)[1] if "," in data_url else ""
        if not payload:
            continue
        blocks.append(
            {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": str(attachment.get("mime_type") or "image/png"),
                    "data": payload,
                },
            }
        )

    async def _messages():  # noqa: ANN202
        yield {"type": "user", "message": {"role": "user", "content": blocks}, "parent_tool_use_id": None}

    return _messages()


def _runtime_for_existing_session(session_id: str) -> _ClaudeSessionRuntime | None:
    with _RUNTIME_LOCK:
        runtime = _RUNTIMES.get(session_id)
        if runtime is None or not runtime.alive:
            return None
        return runtime


def _load_sdk() -> Any:
    global _SDK
    if _SDK is not None:
        return _SDK
    try:
        from claude_agent_sdk import (  # type: ignore[import-not-found]
            AssistantMessage,
            ClaudeAgentOptions,
            ClaudeSDKClient,
            PermissionResultAllow,
            PermissionResultDeny,
            ResultMessage,
            ServerToolResultBlock,
            ServerToolUseBlock,
            SystemMessage,
            TextBlock,
            ToolResultBlock,
            ToolUseBlock,
            UserMessage,
        )
    except ModuleNotFoundError as exc:
        if exc.name == "claude_agent_sdk":
            raise RuntimeError(
                "Claude provider requires claude-agent-sdk; install jarvis[worker-claude] to start Claude worker sessions"
            ) from exc
        raise
    _SDK = SimpleNamespace(
        AssistantMessage=AssistantMessage,
        ClaudeAgentOptions=ClaudeAgentOptions,
        ClaudeSDKClient=ClaudeSDKClient,
        PermissionResultAllow=PermissionResultAllow,
        PermissionResultDeny=PermissionResultDeny,
        ResultMessage=ResultMessage,
        ServerToolResultBlock=ServerToolResultBlock,
        ServerToolUseBlock=ServerToolUseBlock,
        SystemMessage=SystemMessage,
        TextBlock=TextBlock,
        ToolResultBlock=ToolResultBlock,
        ToolUseBlock=ToolUseBlock,
        UserMessage=UserMessage,
    )
    return _SDK


def _project_sdk_message(
    message: Any,
    *,
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
) -> bool:
    if isinstance(message, dict):
        return _project_claude_message(message, session_id=session_id, turn=turn, sessions=sessions)
    sdk = _load_sdk()
    common = {"turn_id": turn.turn_id, "idempotency_key": turn.idempotency_key, "provider": "claude"}
    provider_session_id = str(getattr(message, "session_id", "") or "").strip()
    if provider_session_id:
        _record_provider_session_id(sessions, session_id, provider_session_id)
    if isinstance(message, sdk.SystemMessage):
        raw = _message_raw(message)
        provider_session_id = str(raw.get("session_id") or provider_session_id).strip()
        if provider_session_id:
            _record_provider_session_id(sessions, session_id, provider_session_id)
        if getattr(message, "subtype", "") == "init":
            sessions.append_event(
                session_id,
                EVENT_PROVIDER_SESSION_READY,
                {
                    **common,
                    "provider_session_id": provider_session_id,
                    "model": str(raw.get("model") or ""),
                    "cwd": str(raw.get("cwd") or ""),
                    "raw": raw,
                },
            )
        else:
            sessions.append_event(session_id, EVENT_PROVIDER_EVENT, {**common, "raw": raw})
    elif isinstance(message, sdk.AssistantMessage):
        for block in getattr(message, "content", []) or []:
            _project_content_block(block, session_id=session_id, turn=turn, sessions=sessions, common=common)
    elif isinstance(message, sdk.UserMessage):
        content = getattr(message, "content", None)
        if isinstance(content, list):
            for block in content:
                _project_content_block(block, session_id=session_id, turn=turn, sessions=sessions, common=common)
    elif isinstance(message, sdk.ResultMessage):
        raw = _message_raw(message)
        provider_session_id = str(raw.get("session_id") or provider_session_id).strip()
        if provider_session_id:
            _record_provider_session_id(sessions, session_id, provider_session_id)
        is_error = bool(getattr(message, "is_error", False))
        if _session_cancelled(sessions, session_id):
            return True
        sessions.append_event_with_status(
            session_id,
            SESSION_FAILED if is_error else SESSION_COMPLETED,
            EVENT_TURN_FAILED if is_error else EVENT_TURN_COMPLETED,
            {**common, "provider_status": str(getattr(message, "subtype", "") or ""), "raw": raw},
        )
        return True
    else:
        sessions.append_event(session_id, EVENT_PROVIDER_EVENT, {**common, "raw": _message_raw(message)})
    return False


def _project_content_block(
    block: Any,
    *,
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
    common: dict[str, Any],
) -> None:
    sdk = _load_sdk()
    if isinstance(block, sdk.TextBlock):
        text = str(getattr(block, "text", "") or "")
        if text:
            sessions.append_event(session_id, EVENT_ASSISTANT_MESSAGE, {**common, "text": text})
    elif isinstance(block, (sdk.ToolUseBlock, sdk.ServerToolUseBlock)):
        sessions.append_event(session_id, EVENT_TOOL_CALL, {**common, "item": _message_raw(block)})
    elif isinstance(block, (sdk.ToolResultBlock, sdk.ServerToolResultBlock)):
        sessions.append_event(session_id, EVENT_TOOL_RESULT, {**common, "item": _message_raw(block)})
    else:
        sessions.append_event(session_id, EVENT_PROVIDER_EVENT, {**common, "raw": _message_raw(block)})


def _project_claude_message(
    message: dict[str, Any],
    *,
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
) -> bool:
    message_type = str(message.get("type") or "")
    subtype = str(message.get("subtype") or "")
    common = {"turn_id": turn.turn_id, "idempotency_key": turn.idempotency_key, "provider": "claude"}
    provider_session_id = str(message.get("session_id") or "").strip()
    if provider_session_id:
        _record_provider_session_id(sessions, session_id, provider_session_id)
    if message_type == "system" and subtype == "init":
        sessions.append_event(
            session_id,
            EVENT_PROVIDER_SESSION_READY,
            {
                **common,
                "provider_session_id": provider_session_id,
                "model": str(message.get("model") or ""),
                "cwd": str(message.get("cwd") or ""),
                "raw": message,
            },
        )
    elif message_type == "assistant":
        for item in _content_items(message):
            item_type = str(item.get("type") or "")
            if item_type == "text":
                text = str(item.get("text") or "")
                if text:
                    sessions.append_event(session_id, EVENT_ASSISTANT_MESSAGE, {**common, "text": text})
            elif item_type in {"tool_use", "server_tool_use"}:
                sessions.append_event(session_id, EVENT_TOOL_CALL, {**common, "item": item})
            elif item_type in {"tool_result", "advisor_tool_result"}:
                sessions.append_event(session_id, EVENT_TOOL_RESULT, {**common, "item": item})
    elif message_type == "result":
        is_error = bool(message.get("is_error")) or subtype not in {"success", "done", "completed"}
        if _session_cancelled(sessions, session_id):
            return True
        event_type = EVENT_TURN_FAILED if is_error else EVENT_TURN_COMPLETED
        sessions.append_event_with_status(
            session_id,
            SESSION_FAILED if is_error else SESSION_COMPLETED,
            event_type,
            {**common, "provider_status": subtype or message_type, "raw": message},
        )
        return True
    elif message_type:
        sessions.append_event(session_id, EVENT_PROVIDER_EVENT, {**common, "raw": message})
    return False


def _content_items(message: dict[str, Any]) -> list[dict[str, Any]]:
    content = message.get("content")
    if content is None:
        content = dict(message.get("message") or {}).get("content")
    if isinstance(content, list):
        return [dict(item) for item in content if isinstance(item, dict)]
    return []


def _message_raw(message: Any) -> dict[str, Any]:
    if isinstance(message, dict):
        return dict(message)
    if is_dataclass(message):
        return asdict(message)
    data = getattr(message, "data", None)
    if isinstance(data, dict):
        return dict(data)
    attrs = getattr(message, "__dict__", None)
    if isinstance(attrs, dict):
        return dict(attrs)
    return {"repr": repr(message)}


def _record_provider_session_id(sessions: SessionManager, session_id: str, provider_session_id: str) -> None:
    sessions.update_metadata(
        session_id,
        {
            "claude_session_id": provider_session_id,
            "provider_session_id": provider_session_id,
            "claude_session_started": "true",
        },
    )


def _record_provider_log(session_id: str, turn: ProviderTurn, sessions: SessionManager, text: str) -> None:
    if not text:
        return
    recent = [
        event
        for event in sessions.events(session_id)
        if event.type == EVENT_PROVIDER_LOG and event.data.get("turn_id") == turn.turn_id
    ]
    if len(recent) >= 20:
        return
    sessions.append_event(
        session_id,
        EVENT_PROVIDER_LOG,
        {
            "turn_id": turn.turn_id,
            "idempotency_key": turn.idempotency_key,
            "provider": "claude",
            "text": text[:1000],
        },
    )


def _session_cwd(session: WorkerSession, worker_cfg: WorkerConfig) -> str:
    candidates = [
        session.cwd,
        str(session.metadata.get("provider_cwd") or ""),
        str(session.metadata.get("cwd") or ""),
    ]
    rejected: list[str] = []
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser().resolve(strict=False)
        if not is_worker_owned_path_for_config(path, worker_cfg):
            rejected.append(str(path))
            continue
        if path.is_dir():
            return str(path)
        rejected.append(str(path))
    if rejected:
        raise RuntimeError(f"worker session cwd is not a valid worker-owned directory: {', '.join(rejected)}")
    raise RuntimeError("worker session cwd is required for claude provider turns")


def _session_cancelled(sessions: SessionManager, session_id: str) -> bool:
    session = sessions.get(session_id)
    return session is not None and session.status in CANCELLED_SESSION_STATUSES


def _claude_session_id(session: WorkerSession) -> str:
    value = str(
        session.metadata.get("claude_session_id")
        or session.metadata.get("provider_session_id")
        or ""
    ).strip()
    if value:
        return value
    return str(uuid.uuid4())


def _control_request_id(request: dict[str, Any]) -> str:
    request_id = str(request.get("request_id") or request.get("id") or "").strip()
    if not request_id:
        raise RuntimeError("request_id is required")
    return request_id


def _approval_allowed(request: dict[str, Any]) -> bool:
    decision = str(request.get("decision") or "").strip().lower()
    return decision in {"approved", "approve", "allow", "allowed", "yes", "accept", "acceptforsession", "accept_for_session", "always"}


def _updated_question_input(tool_input: dict[str, Any], response: dict[str, Any]) -> dict[str, Any]:
    updated = dict(tool_input)
    text = str(response.get("text") or response.get("answer") or "")
    answers = response.get("answers")
    if isinstance(answers, dict):
        updated["answers"] = answers
    elif isinstance(answers, list):
        updated["answers"] = {"text": [str(item) for item in answers]}
    elif text:
        updated["answers"] = {key: text for key in _question_answer_keys(tool_input)}
    return updated


def _question_answer_keys(tool_input: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for input_field in ("questions", "items", "inputs"):
        values = tool_input.get(input_field)
        if not isinstance(values, list):
            continue
        for item in values:
            if not isinstance(item, dict):
                continue
            key = str(item.get("id") or item.get("name") or item.get("key") or "").strip()
            if key and key not in keys:
                keys.append(key)
    for input_field in ("question_id", "id", "name", "key"):
        key = str(tool_input.get(input_field) or "").strip()
        if key and key not in keys:
            keys.append(key)
    return keys or ["text"]


def _restore_running_if_waiting(
    sessions: SessionManager,
    session_id: str,
    waiting_status: str,
    has_pending_requests: Any,
) -> None:
    session = sessions.get(session_id)
    if session is None or session.status != waiting_status:
        return
    terminal_status = _terminal_status_from_events(sessions, session_id)
    if terminal_status is not None:
        sessions.update_status(session_id, terminal_status)
        return
    if not has_pending_requests():
        sessions.update_status(session_id, SESSION_RUNNING)


def _terminal_status_from_events(sessions: SessionManager, session_id: str) -> str | None:
    status_by_event = {
        EVENT_SESSION_INTERRUPTED: SESSION_INTERRUPTED,
        EVENT_SESSION_STOPPED: SESSION_STOPPED,
        EVENT_TURN_COMPLETED: SESSION_COMPLETED,
        EVENT_TURN_FAILED: SESSION_FAILED,
    }
    for event in reversed(sessions.events(session_id)):
        status = status_by_event.get(event.type)
        if status is not None:
            return status
    return None


def _terminate_provider_process(session: WorkerSession) -> None:
    runtime = _runtime_for_existing_session(session.session_id)
    if runtime is not None:
        runtime.interrupt()
