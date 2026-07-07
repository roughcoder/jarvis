from __future__ import annotations

import json
import queue
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from jarvis.config import WorkerConfig
from jarvis.worker.authority import WorkerSessionAuthority
from jarvis.worker.providers.base import ProviderTurn
from jarvis.worker.sessions import SessionEvent, SessionManager, WorkerSession
from jarvis.worker_session_contract import (
    CANCELLED_SESSION_STATUSES,
    EVENT_APPROVAL_REQUESTED,
    EVENT_APPROVAL_RESOLVED,
    EVENT_ARTIFACT_UPDATED,
    EVENT_ASSISTANT_DELTA,
    EVENT_ASSISTANT_MESSAGE,
    EVENT_INPUT_RECEIVED,
    EVENT_INPUT_REQUESTED,
    EVENT_PLAN_UPDATED,
    EVENT_PROVIDER_ERROR,
    EVENT_PROVIDER_LOG,
    EVENT_PROVIDER_PROCESS_STARTED,
    EVENT_PROVIDER_STARTED,
    EVENT_PROVIDER_THREAD_READY,
    EVENT_PROVIDER_TURN_STARTED,
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


class CodexProviderAdapter:
    provider = "codex"

    def capabilities(self) -> dict[str, Any]:
        return {
            "streaming": True,
            "resume": True,
            "interrupt": True,
            "approvals": True,
            "questions": True,
            "checkpoints": True,
            "rollback": False,
            "attachments": True,
            "runtime": "codex app-server JSON-RPC over stdio",
            "attached": True,
            "lifecycle": "spawn app-server process; initialize; thread/start or thread/resume; turn/start; ingest notifications",
            "event_ordering": "provider JSON-RPC notifications project to append-only SessionEvent order",
        }

    def start_turn(
        self,
        *,
        session: WorkerSession,
        turn: ProviderTurn,
        sessions: SessionManager,
        worker_cfg: WorkerConfig,
    ) -> list[SessionEvent]:
        authority = WorkerSessionAuthority.from_session(session, provider=self.provider)
        sessions.update_status(session.session_id, SESSION_RUNNING)
        started = sessions.append_event(
            session.session_id,
            EVENT_PROVIDER_STARTED,
            {
                "turn_id": turn.turn_id,
                "idempotency_key": turn.idempotency_key,
                "provider": self.provider,
                "runtime": "codex app-server",
            },
        )
        thread = threading.Thread(
            target=_run_codex_turn,
            args=(session.session_id, turn, sessions, worker_cfg, authority),
            name=f"jarvis-codex-{session.session_id}-{turn.turn_id}",
            daemon=True,
        )
        thread.start()
        return [started]

    def interrupt(self, *, session: WorkerSession, sessions: SessionManager) -> tuple[WorkerSession, SessionEvent]:
        _terminate_provider_process(session)
        updated = sessions.update_status(session.session_id, SESSION_INTERRUPTED)
        event = sessions.append_event(updated.session_id, EVENT_SESSION_INTERRUPTED, {"status": SESSION_INTERRUPTED})
        return updated, event

    def stop(self, *, session: WorkerSession, sessions: SessionManager) -> tuple[WorkerSession, SessionEvent]:
        _terminate_provider_process(session)
        updated = sessions.update_status(session.session_id, SESSION_STOPPED)
        event = sessions.append_event(updated.session_id, EVENT_SESSION_STOPPED, {"status": SESSION_STOPPED})
        return updated, event

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
        event: SessionEvent | None = None

        def record_resolution() -> None:
            nonlocal event
            event = sessions.append_event(session.session_id, EVENT_APPROVAL_RESOLVED, {**request, "request_id": request_id})
            _restore_running_if_waiting(sessions, session.session_id, SESSION_WAITING_APPROVAL)

        delivered = _deliver_pending_request(
            session.session_id,
            request_id,
            kind=REQUEST_KIND_APPROVAL,
            request=request,
            before_send=record_resolution,
        )
        if not delivered:
            raise RuntimeError(f"no pending codex approval request {request_id!r}")
        if event is None:
            raise RuntimeError(f"no pending codex approval request {request_id!r}")
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
        event: SessionEvent | None = None

        def record_input() -> None:
            nonlocal event
            event = sessions.append_event(session.session_id, EVENT_INPUT_RECEIVED, {**request, "request_id": request_id})
            _restore_running_if_waiting(sessions, session.session_id, SESSION_WAITING_INPUT)

        delivered = _deliver_pending_request(
            session.session_id,
            request_id,
            kind=REQUEST_KIND_INPUT,
            request=request,
            before_send=record_input,
        )
        if not delivered:
            raise RuntimeError(f"no pending codex input request {request_id!r}")
        if event is None:
            raise RuntimeError(f"no pending codex input request {request_id!r}")
        return event


@dataclass
class _PendingCodexRequest:
    kind: str
    process: subprocess.Popen[str]
    rpc_id: Any
    params: dict[str, Any] = field(default_factory=dict)


_PENDING_LOCK = threading.Lock()
_PENDING_REQUESTS: dict[tuple[str, str], _PendingCodexRequest] = {}
_PROCESS_LOCK = threading.Lock()
_PROVIDER_PROCESSES: dict[str, subprocess.Popen[str]] = {}


def _turn_input(turn: ProviderTurn) -> list[dict[str, Any]]:
    """turn/start UserInput items: the prompt text plus one app-server image
    item per attachment (data URLs go straight into the `url` variant)."""
    items: list[dict[str, Any]] = [{"type": "text", "text": turn.prompt}]
    for attachment in turn.attachments:
        url = str(attachment.get("data_url") or "")
        if url:
            items.append({"type": "image", "url": url})
    return items


def _run_codex_turn(
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
    worker_cfg: WorkerConfig,
    authority: WorkerSessionAuthority,
) -> None:
    process: subprocess.Popen[str] | None = None
    line_queue: queue.Queue[tuple[str, str]] | None = None
    rpc_id = 0
    try:
        session = sessions.get(session_id)
        if session is None:
            return
        if _session_cancelled(sessions, session_id):
            return
        cwd = _session_cwd(session, worker_cfg)
        if _session_cancelled(sessions, session_id):
            return
        process = subprocess.Popen(
            [worker_cfg.codex_bin, "app-server", "--stdio"],
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        _track_provider_process(session_id, process)
        if _session_cancelled(sessions, session_id):
            _terminate_provider_process(session)
            return
        line_queue = _start_line_readers(process)
        sessions.update_metadata(
            session_id,
            {
                "provider_pid": process.pid,
                "provider_runtime": "codex app-server",
                "provider_cwd": cwd,
            },
        )
        sessions.append_event(
            session_id,
            EVENT_PROVIDER_PROCESS_STARTED,
            {"turn_id": turn.turn_id, "provider": "codex", "pid": process.pid, "cwd": cwd},
        )

        rpc_id += 1
        init = _send_request(
            process,
            rpc_id,
            "initialize",
            {
                "clientInfo": {"name": "jarvis-worker", "version": "0.0.0"},
                "capabilities": {"experimentalApi": True},
            },
            session_id=session_id,
            turn=turn,
            sessions=sessions,
            line_queue=line_queue,
            timeout_s=30,
        )
        if "error" in init:
            raise RuntimeError(init["error"].get("message") or "codex initialize failed")
        _send_notification(process, "initialized")

        session = sessions.get(session_id) or session
        thread_id = str(session.metadata.get("codex_thread_id") or "").strip()
        if thread_id:
            rpc_id += 1
            thread_response = _send_request(
                process,
                rpc_id,
                "thread/resume",
                {
                    "threadId": thread_id,
                    "cwd": cwd,
                    "approvalPolicy": authority.codex_approval_policy,
                    "sandbox": authority.codex_sandbox,
                },
                session_id=session_id,
                turn=turn,
                sessions=sessions,
                line_queue=line_queue,
                timeout_s=30,
            )
        else:
            rpc_id += 1
            thread_response = _send_request(
                process,
                rpc_id,
                "thread/start",
                {
                    "cwd": cwd,
                    "approvalPolicy": authority.codex_approval_policy,
                    "sandbox": authority.codex_sandbox,
                },
                session_id=session_id,
                turn=turn,
                sessions=sessions,
                line_queue=line_queue,
                timeout_s=30,
            )
        if "error" in thread_response:
            raise RuntimeError(thread_response["error"].get("message") or "codex thread start/resume failed")
        thread = dict(thread_response.get("result", {}).get("thread") or {})
        thread_id = str(thread.get("id") or thread_id)
        if thread_id:
            sessions.update_metadata(
                session_id,
                {
                    "codex_thread_id": thread_id,
                    "codex_thread_path": str(thread.get("path") or ""),
                    "provider_session_id": str(thread.get("sessionId") or thread_id),
                },
            )
            sessions.append_event(
                session_id,
                EVENT_PROVIDER_THREAD_READY,
                {"turn_id": turn.turn_id, "provider": "codex", "thread_id": thread_id},
            )

        rpc_id += 1
        _send_json(
            process,
            {
                "jsonrpc": "2.0",
                "id": rpc_id,
                "method": "turn/start",
                "params": {
                    "threadId": thread_id,
                    "cwd": cwd,
                    "approvalPolicy": authority.codex_approval_policy,
                    "input": _turn_input(turn),
                },
            },
        )
        turn_started = _read_until_response(
            process,
            rpc_id,
            session_id=session_id,
            turn=turn,
            sessions=sessions,
            line_queue=line_queue,
            timeout_s=30,
        )
        if "error" in turn_started:
            raise RuntimeError(turn_started["error"].get("message") or "codex turn start failed")
        provider_turn_id = str(turn_started.get("result", {}).get("turn", {}).get("id") or "")
        if provider_turn_id:
            sessions.update_metadata(session_id, {"provider_turn_id": provider_turn_id})
        _read_until_turn_done(
            process,
            session_id=session_id,
            turn=turn,
            sessions=sessions,
            line_queue=line_queue,
            timeout_s=max(1.0, float(worker_cfg.job_timeout_s)),
        )
    except Exception as exc:  # noqa: BLE001 - provider failures must become session events
        if _session_cancelled(sessions, session_id):
            return
        sessions.update_status(session_id, SESSION_FAILED)
        sessions.append_event(
            session_id,
            EVENT_TURN_FAILED,
            {
                "turn_id": turn.turn_id,
                "idempotency_key": turn.idempotency_key,
                "provider": "codex",
                "error": str(exc),
            },
        )
    finally:
        if process is not None:
            _untrack_provider_process(session_id, process)
        if process is not None:
            _clear_pending_requests(session_id, process)
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        try:
            sessions.update_metadata(session_id, {"provider_pid": ""})
        except KeyError:
            pass


def _send_request(
    process: subprocess.Popen[str],
    request_id: int,
    method: str,
    params: dict[str, Any],
    *,
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
    line_queue: queue.Queue[tuple[str, str]],
    timeout_s: float,
) -> dict[str, Any]:
    _send_json(process, {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
    return _read_until_response(
        process,
        request_id,
        session_id=session_id,
        turn=turn,
        sessions=sessions,
        line_queue=line_queue,
        timeout_s=timeout_s,
    )


def _send_notification(process: subprocess.Popen[str], method: str, params: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        payload["params"] = params
    _send_json(process, payload)


def _send_json(process: subprocess.Popen[str], payload: dict[str, Any]) -> None:
    if process.stdin is None:
        raise RuntimeError("codex app-server stdin closed")
    process.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
    process.stdin.flush()


def _read_until_response(
    process: subprocess.Popen[str],
    request_id: int,
    *,
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
    line_queue: queue.Queue[tuple[str, str]],
    timeout_s: float,
) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        message = _read_message(
            process,
            line_queue=line_queue,
            timeout=0.2,
            session_id=session_id,
            turn=turn,
            sessions=sessions,
        )
        if message is None:
            if process.poll() is not None:
                raise RuntimeError(f"codex app-server exited with {process.returncode}")
            continue
        if message.get("id") == request_id:
            return message
        _project_jsonrpc_message(process, message, session_id=session_id, turn=turn, sessions=sessions)
    raise TimeoutError(f"codex app-server did not respond to request {request_id}")


def _read_until_turn_done(
    process: subprocess.Popen[str],
    *,
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
    line_queue: queue.Queue[tuple[str, str]],
    timeout_s: float,
) -> None:
    deadline = time.time() + timeout_s
    while True:
        if _has_pending_requests(session_id):
            deadline = time.time() + timeout_s
        elif time.time() >= deadline:
            break
        message = _read_message(
            process,
            line_queue=line_queue,
            timeout=0.5,
            session_id=session_id,
            turn=turn,
            sessions=sessions,
        )
        if message is None:
            if process.poll() is not None:
                raise RuntimeError(f"codex app-server exited with {process.returncode}")
            continue
        if _project_jsonrpc_message(process, message, session_id=session_id, turn=turn, sessions=sessions):
            return
    raise TimeoutError("codex turn timed out")


def _read_message(
    process: subprocess.Popen[str],
    *,
    line_queue: queue.Queue[tuple[str, str]],
    timeout: float,
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
) -> dict[str, Any] | None:
    try:
        source, line = line_queue.get(timeout=timeout)
    except queue.Empty:
        return None
    if source == "stderr":
        _record_provider_log(session_id, turn, sessions, line.strip())
        return None
    try:
        data = json.loads(line)
    except json.JSONDecodeError:
        _record_provider_log(session_id, turn, sessions, line.strip())
        return None
    if isinstance(data, dict):
        return data
    return None


def _start_line_readers(process: subprocess.Popen[str]) -> queue.Queue[tuple[str, str]]:
    line_queue: queue.Queue[tuple[str, str]] = queue.Queue()

    def read_stream(name: str, stream: Any) -> None:
        for line in stream:
            line_queue.put((name, line))

    for name, stream in (("stdout", process.stdout), ("stderr", process.stderr)):
        if stream is None:
            continue
        threading.Thread(
            target=read_stream,
            args=(name, stream),
            name=f"jarvis-codex-{name}-reader",
            daemon=True,
        ).start()
    return line_queue


def _project_jsonrpc_message(
    process: subprocess.Popen[str],
    message: dict[str, Any],
    *,
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
) -> bool:
    method = str(message.get("method") or "")
    params = dict(message.get("params") or {})
    common = {"turn_id": turn.turn_id, "idempotency_key": turn.idempotency_key, "provider": "codex"}
    if message.get("error"):
        sessions.append_event(session_id, EVENT_PROVIDER_ERROR, {**common, "error": message["error"]})
        return False
    if method == "item/agentMessage/delta":
        sessions.append_event(session_id, EVENT_ASSISTANT_DELTA, {**common, "text": str(params.get("delta") or "")})
    elif method == "item/completed":
        item = dict(params.get("item") or {})
        item_type = str(item.get("type") or "")
        if item_type == "agentMessage":
            sessions.append_event(session_id, EVENT_ASSISTANT_MESSAGE, {**common, "text": str(item.get("text") or "")})
        elif item_type == "commandExecution":
            sessions.append_event(session_id, EVENT_TOOL_RESULT, {**common, "item": item})
    elif method == "item/started":
        item = dict(params.get("item") or {})
        item_type = str(item.get("type") or "")
        if item_type and item_type not in {"userMessage", "reasoning"}:
            sessions.append_event(session_id, EVENT_TOOL_CALL, {**common, "item": item})
    elif method == "item/commandExecution/requestApproval":
        request_id = _message_request_id(message, params)
        _track_pending_request(
            session_id,
            request_id,
            kind=REQUEST_KIND_APPROVAL,
            process=process,
            rpc_id=message.get("id"),
            params=params,
        )
        sessions.append_event(session_id, EVENT_APPROVAL_REQUESTED, {**common, **params, "request_id": request_id})
        sessions.update_status(session_id, SESSION_WAITING_APPROVAL)
    elif method == "item/fileChange/requestApproval":
        request_id = _message_request_id(message, params)
        _track_pending_request(
            session_id,
            request_id,
            kind=REQUEST_KIND_APPROVAL,
            process=process,
            rpc_id=message.get("id"),
            params=params,
        )
        sessions.append_event(session_id, EVENT_APPROVAL_REQUESTED, {**common, **params, "request_id": request_id})
        sessions.update_status(session_id, SESSION_WAITING_APPROVAL)
    elif method == "item/tool/requestUserInput":
        request_id = _message_request_id(message, params)
        _track_pending_request(
            session_id,
            request_id,
            kind=REQUEST_KIND_INPUT,
            process=process,
            rpc_id=message.get("id"),
            params=params,
        )
        sessions.append_event(session_id, EVENT_INPUT_REQUESTED, {**common, **params, "request_id": request_id})
        sessions.update_status(session_id, SESSION_WAITING_INPUT)
    elif method == "serverRequest/resolved":
        request_id = _message_request_id(message, params)
        pending = _forget_pending_request(session_id, request_id, process=process)
        if pending is not None:
            event_type = EVENT_APPROVAL_RESOLVED if pending.kind == REQUEST_KIND_APPROVAL else EVENT_INPUT_RECEIVED
            sessions.append_event(
                session_id,
                event_type,
                {**common, **params, "request_id": request_id, "provider_resolved": True},
            )
            waiting_status = SESSION_WAITING_APPROVAL if pending.kind == REQUEST_KIND_APPROVAL else SESSION_WAITING_INPUT
            _restore_running_if_waiting(sessions, session_id, waiting_status)
    elif method == "turn/completed":
        if _session_cancelled(sessions, session_id):
            return True
        status = str(dict(params.get("turn") or {}).get("status") or "completed")
        event_type = EVENT_TURN_COMPLETED if status in {"completed", "done", "succeeded"} else EVENT_TURN_FAILED
        sessions.append_event_with_status(
            session_id,
            SESSION_COMPLETED if event_type == EVENT_TURN_COMPLETED else SESSION_FAILED,
            event_type,
            {**common, "provider_status": status, "raw": params},
        )
        return True
    elif method == "turn/started":
        sessions.append_event(session_id, EVENT_PROVIDER_TURN_STARTED, {**common, "raw": params})
    elif method == "turn/diff/updated":
        sessions.append_event(session_id, EVENT_ARTIFACT_UPDATED, {**common, "kind": "diff", "raw": params})
    elif method == "turn/plan/updated":
        sessions.append_event(session_id, EVENT_PLAN_UPDATED, {**common, "raw": params})
    return False


def _message_request_id(message: dict[str, Any], params: dict[str, Any]) -> str:
    return str(params.get("requestId") or params.get("request_id") or params.get("id") or message.get("id") or "").strip()


def _control_request_id(request: dict[str, Any]) -> str:
    request_id = str(request.get("request_id") or request.get("id") or "").strip()
    if not request_id:
        raise RuntimeError("request_id is required")
    return request_id


def _track_pending_request(
    session_id: str,
    request_id: str,
    *,
    kind: str,
    process: subprocess.Popen[str],
    rpc_id: Any,
    params: dict[str, Any] | None = None,
) -> None:
    if not request_id or rpc_id is None:
        return
    with _PENDING_LOCK:
        _PENDING_REQUESTS[(session_id, request_id)] = _PendingCodexRequest(
            kind=kind,
            process=process,
            rpc_id=rpc_id,
            params=dict(params or {}),
        )


def _deliver_pending_request(
    session_id: str,
    request_id: str,
    *,
    kind: str,
    request: dict[str, Any],
    before_send: Callable[[], None] | None = None,
) -> bool:
    with _PENDING_LOCK:
        key = (session_id, request_id)
        pending = _PENDING_REQUESTS.get(key)
        if pending is not None and pending.kind == kind:
            _PENDING_REQUESTS.pop(key, None)
    if pending is None or pending.kind != kind:
        return False
    result = _approval_result(request) if kind == REQUEST_KIND_APPROVAL else _input_result(request, pending.params)
    try:
        _send_json(pending.process, {"jsonrpc": "2.0", "id": pending.rpc_id, "result": result})
    except Exception:
        with _PENDING_LOCK:
            _PENDING_REQUESTS.setdefault(key, pending)
        raise
    if before_send is not None:
        before_send()
    return True


def _forget_pending_request(
    session_id: str,
    request_id: str,
    *,
    process: subprocess.Popen[str],
) -> _PendingCodexRequest | None:
    with _PENDING_LOCK:
        pending = _PENDING_REQUESTS.get((session_id, request_id))
        if pending is None or pending.process is not process:
            return None
        return _PENDING_REQUESTS.pop((session_id, request_id), None)


def _has_pending_requests(session_id: str) -> bool:
    with _PENDING_LOCK:
        return any(key[0] == session_id for key in _PENDING_REQUESTS)


def _restore_running_if_waiting(sessions: SessionManager, session_id: str, waiting_status: str) -> None:
    session = sessions.get(session_id)
    if session is None or session.status != waiting_status:
        return
    terminal_status = _terminal_status_from_events(sessions, session_id)
    if terminal_status is not None:
        sessions.update_status(session_id, terminal_status)
        return
    if not _has_pending_requests(session_id):
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


def _clear_pending_requests(session_id: str, process: subprocess.Popen[str]) -> None:
    with _PENDING_LOCK:
        for key, pending in list(_PENDING_REQUESTS.items()):
            if key[0] == session_id and pending.process is process:
                _PENDING_REQUESTS.pop(key, None)


def _approval_result(request: dict[str, Any]) -> dict[str, Any]:
    decision = str(request.get("decision") or "").strip().lower()
    if decision in {"approved", "approve", "allow", "allowed", "yes", "accept"}:
        mapped = "accept"
    elif decision in {"acceptforsession", "accept_for_session", "always"}:
        mapped = "acceptForSession"
    elif decision == "cancel":
        mapped = "cancel"
    else:
        mapped = "decline"
    return {"decision": mapped}


def _input_result(request: dict[str, Any], params: dict[str, Any] | None = None) -> dict[str, Any]:
    answers = request.get("answers")
    question_ids = _input_question_ids(params or {})
    text = str(request.get("text") or request.get("answer") or "")
    if isinstance(answers, dict):
        return {
            "answers": {
                question_id: _input_answer_payload(answers.get(question_id, text))
                for question_id in question_ids
            }
        }
    return {"answers": {question_id: {"answers": [text]} for question_id in question_ids}}


def _input_question_ids(params: dict[str, Any]) -> list[str]:
    result: list[str] = []
    for key in ("questions", "items", "inputs"):
        values = params.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            if isinstance(item, dict):
                question_id = str(item.get("id") or item.get("name") or item.get("key") or "").strip()
                if question_id and question_id not in result:
                    result.append(question_id)
    question_id = str(params.get("question_id") or params.get("id") or "").strip()
    if question_id and question_id not in result:
        result.append(question_id)
    return result or ["text"]


def _input_answer_payload(value: Any) -> dict[str, list[str]]:
    if isinstance(value, dict) and isinstance(value.get("answers"), list):
        return {"answers": [str(item) for item in value["answers"]]}
    if isinstance(value, list):
        return {"answers": [str(item) for item in value]}
    return {"answers": [str(value or "")]}


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
            "provider": "codex",
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
        if not _worker_owned_path(path, worker_cfg):
            rejected.append(str(path))
            continue
        if path.is_dir():
            return str(path)
        rejected.append(str(path))
    if rejected:
        raise RuntimeError(f"worker session cwd is not a valid worker-owned directory: {', '.join(rejected)}")
    raise RuntimeError("worker session cwd is required for codex provider turns")


def _worker_owned_path(path: Path, worker_cfg: WorkerConfig) -> bool:
    workspace = Path(worker_cfg.workspace).expanduser().resolve(strict=False)
    conversations = (
        Path(worker_cfg.conversation_workspace_root).expanduser().resolve(strict=False)
        if worker_cfg.conversation_workspace_root
        else (workspace / "conversations").resolve(strict=False)
    )
    roots = [
        (workspace / "runs").resolve(strict=False),
        (workspace / "worktrees").resolve(strict=False),
        conversations,
    ]
    return any(path.is_relative_to(root) for root in roots)


def _session_cancelled(sessions: SessionManager, session_id: str) -> bool:
    session = sessions.get(session_id)
    return session is not None and session.status in CANCELLED_SESSION_STATUSES


def _terminate_provider_process(session: WorkerSession) -> None:
    with _PROCESS_LOCK:
        process = _PROVIDER_PROCESSES.get(session.session_id)
    if process is None or process.poll() is not None:
        return
    try:
        process.terminate()
    except OSError:
        return


def _track_provider_process(session_id: str, process: subprocess.Popen[str]) -> None:
    with _PROCESS_LOCK:
        _PROVIDER_PROCESSES[session_id] = process


def _untrack_provider_process(session_id: str, process: subprocess.Popen[str]) -> None:
    with _PROCESS_LOCK:
        if _PROVIDER_PROCESSES.get(session_id) is process:
            _PROVIDER_PROCESSES.pop(session_id, None)
