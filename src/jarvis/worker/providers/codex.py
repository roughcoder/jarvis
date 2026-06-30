from __future__ import annotations

import json
import os
import queue
import signal
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from jarvis.config import WorkerConfig
from jarvis.worker.authority import WorkerSessionAuthority
from jarvis.worker.providers.base import ProviderTurn
from jarvis.worker.sessions import SessionEvent, SessionManager, WorkerSession


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
        authority = WorkerSessionAuthority.from_session(session)
        sessions.update_status(session.session_id, "running")
        started = sessions.append_event(
            session.session_id,
            "provider.started",
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
        updated = sessions.update_status(session.session_id, "interrupted")
        event = sessions.append_event(updated.session_id, "session.interrupted", {"status": "interrupted"})
        return updated, event

    def stop(self, *, session: WorkerSession, sessions: SessionManager) -> tuple[WorkerSession, SessionEvent]:
        _terminate_provider_process(session)
        updated = sessions.update_status(session.session_id, "stopped")
        event = sessions.append_event(updated.session_id, "session.stopped", {"status": "stopped"})
        return updated, event


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
        cwd = _session_cwd(session, worker_cfg)
        process = subprocess.Popen(
            [worker_cfg.codex_bin, "app-server", "--stdio"],
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
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
            "provider.process.started",
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
                "provider.thread.ready",
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
                    "input": [{"type": "text", "text": turn.prompt}],
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
        sessions.update_status(session_id, "failed")
        sessions.append_event(
            session_id,
            "turn.failed",
            {
                "turn_id": turn.turn_id,
                "idempotency_key": turn.idempotency_key,
                "provider": "codex",
                "error": str(exc),
            },
        )
    finally:
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
    while time.time() < deadline:
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
        sessions.append_event(session_id, "provider.error", {**common, "error": message["error"]})
        return False
    if method == "item/agentMessage/delta":
        sessions.append_event(session_id, "assistant.delta", {**common, "text": str(params.get("delta") or "")})
    elif method == "item/completed":
        item = dict(params.get("item") or {})
        item_type = str(item.get("type") or "")
        if item_type == "agentMessage":
            sessions.append_event(session_id, "assistant.message", {**common, "text": str(item.get("text") or "")})
        elif item_type == "commandExecution":
            sessions.append_event(session_id, "tool.result", {**common, "item": item})
    elif method == "item/started":
        item = dict(params.get("item") or {})
        item_type = str(item.get("type") or "")
        if item_type and item_type not in {"userMessage", "reasoning"}:
            sessions.append_event(session_id, "tool.call", {**common, "item": item})
    elif method == "item/commandExecution/requestApproval":
        sessions.append_event(session_id, "approval.requested", {**common, **params})
        _send_server_response(process, message, {"decision": "deny"})
    elif method == "item/fileChange/requestApproval":
        sessions.append_event(session_id, "approval.requested", {**common, **params})
        _send_server_response(process, message, {"decision": "deny"})
    elif method == "item/tool/requestUserInput":
        sessions.append_event(session_id, "input.requested", {**common, **params})
        _send_server_response(process, message, {"answers": {}})
    elif method == "turn/completed":
        status = str(dict(params.get("turn") or {}).get("status") or "completed")
        event_type = "turn.completed" if status in {"completed", "done", "succeeded"} else "turn.failed"
        sessions.update_status(session_id, "completed" if event_type == "turn.completed" else "failed")
        sessions.append_event(session_id, event_type, {**common, "provider_status": status, "raw": params})
        return True
    elif method == "turn/started":
        sessions.append_event(session_id, "provider.turn.started", {**common, "raw": params})
    elif method == "turn/diff/updated":
        sessions.append_event(session_id, "artifact.updated", {**common, "kind": "diff", "raw": params})
    elif method == "turn/plan/updated":
        sessions.append_event(session_id, "plan.updated", {**common, "raw": params})
    return False


def _send_server_response(
    process: subprocess.Popen[str],
    message: dict[str, Any],
    result: dict[str, Any],
) -> None:
    # Server-initiated JSON-RPC requests are recorded for Jarvis surfaces. Until
    # the approval/input bridge is implemented, deny or empty-answer them rather
    # than letting Codex wait indefinitely.
    request_id = message.get("id")
    if request_id is None:
        return
    _send_json(process, {"jsonrpc": "2.0", "id": request_id, "result": result})


def _record_provider_log(session_id: str, turn: ProviderTurn, sessions: SessionManager, text: str) -> None:
    if not text:
        return
    recent = [
        event
        for event in sessions.events(session_id)
        if event.type == "provider.log" and event.data.get("turn_id") == turn.turn_id
    ]
    if len(recent) >= 20:
        return
    sessions.append_event(
        session_id,
        "provider.log",
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
        str(Path(worker_cfg.workspace).expanduser()),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser().resolve(strict=False)
        if path.exists() and path.is_dir():
            return str(path)
    return str(Path(worker_cfg.workspace).expanduser().resolve(strict=False))


def _session_cancelled(sessions: SessionManager, session_id: str) -> bool:
    session = sessions.get(session_id)
    return session is not None and session.status in {"interrupted", "stopped"}


def _terminate_provider_process(session: WorkerSession) -> None:
    pid = str(session.metadata.get("provider_pid") or "").strip()
    if not pid:
        return
    try:
        os.kill(int(pid), signal.SIGTERM)
    except (OSError, ValueError):
        return
