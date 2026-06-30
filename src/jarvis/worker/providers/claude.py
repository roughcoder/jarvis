from __future__ import annotations

import json
import os
import queue
import signal
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from jarvis.config import WorkerConfig
from jarvis.worker.authority import WorkerSessionAuthority
from jarvis.worker.providers.base import ProviderTurn
from jarvis.worker.sessions import SessionEvent, SessionManager, WorkerSession


class ClaudeProviderAdapter:
    provider = "claude"

    def capabilities(self) -> dict[str, Any]:
        return {
            "streaming": True,
            "resume": True,
            "interrupt": True,
            "approvals": False,
            "questions": False,
            "checkpoints": False,
            "rollback": False,
            "runtime": "claude -p stream-json session runtime",
            "attached": True,
            "lifecycle": "spawn Claude stream-json process per turn; use --session-id/--resume; ingest JSONL events",
            "backpressure": "stdout/stderr reader threads feed bounded turn ingestion",
            "timeouts": "turn timeout maps to turn.failed",
            "crash_recovery": "session/event history is durable; active turn is marked failed on process error",
            "event_ordering": "Claude JSONL stream projects to append-only SessionEvent order",
            "sdk_sidecar_next": "replace subprocess runtime with @anthropic-ai/claude-agent-sdk sidecar for live prompt queue and permission callbacks",
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
        claude_session_id = _claude_session_id(session)
        sessions.update_metadata(
            session.session_id,
            {
                "claude_session_id": claude_session_id,
                "provider_session_id": claude_session_id,
                "provider_runtime": "claude stream-json",
            },
        )
        started = sessions.append_event(
            session.session_id,
            "provider.started",
            {
                "turn_id": turn.turn_id,
                "idempotency_key": turn.idempotency_key,
                "provider": self.provider,
                "runtime": "claude stream-json",
                "provider_session_id": claude_session_id,
            },
        )
        thread = threading.Thread(
            target=_run_claude_turn,
            args=(session.session_id, turn, sessions, worker_cfg, claude_session_id, authority),
            name=f"jarvis-claude-{session.session_id}-{turn.turn_id}",
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


def _run_claude_turn(
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
    worker_cfg: WorkerConfig,
    claude_session_id: str,
    authority: WorkerSessionAuthority,
) -> None:
    process: subprocess.Popen[str] | None = None
    try:
        session = sessions.get(session_id)
        if session is None:
            return
        cwd = _session_cwd(session, worker_cfg)
        resume = bool(str(session.metadata.get("claude_session_started") or "").strip())
        argv = [
            worker_cfg.claude_bin,
            "-p",
            "--verbose",
            "--output-format",
            "stream-json",
            "--include-partial-messages",
            "--input-format",
            "text",
            "--permission-mode",
            authority.claude_permission_mode,
        ]
        if resume:
            argv.extend(["--resume", claude_session_id])
        else:
            argv.extend(["--session-id", claude_session_id])
        argv.append(turn.prompt)
        process = subprocess.Popen(
            argv,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
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
                "provider_cwd": cwd,
                "provider_runtime": "claude stream-json",
            },
        )
        sessions.append_event(
            session_id,
            "provider.process.started",
            {"turn_id": turn.turn_id, "provider": "claude", "pid": process.pid, "cwd": cwd, "resume": resume},
        )
        _read_until_turn_done(
            process,
            line_queue=line_queue,
            session_id=session_id,
            turn=turn,
            sessions=sessions,
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
                "provider": "claude",
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


def _read_until_turn_done(
    process: subprocess.Popen[str],
    *,
    line_queue: queue.Queue[tuple[str, str]],
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
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
                if process.returncode == 0:
                    sessions.update_status(session_id, "completed")
                    sessions.append_event(
                        session_id,
                        "turn.completed",
                        {"turn_id": turn.turn_id, "provider": "claude", "provider_status": "exited"},
                    )
                    return
                raise RuntimeError(f"claude exited with {process.returncode}")
            continue
        if _project_claude_message(message, session_id=session_id, turn=turn, sessions=sessions):
            return
    raise TimeoutError("claude turn timed out")


def _read_message(
    process: subprocess.Popen[str],
    *,
    line_queue: queue.Queue[tuple[str, str]],
    timeout: float,
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
) -> dict[str, Any] | None:
    _ = process
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


def _project_claude_message(
    message: dict[str, Any],
    *,
    session_id: str,
    turn: ProviderTurn,
    sessions: SessionManager,
) -> bool:
    message_type = str(message.get("type") or "")
    subtype = str(message.get("subtype") or "")
    common = {"turn_id": turn.turn_id, "provider": "claude"}
    provider_session_id = str(message.get("session_id") or "").strip()
    if provider_session_id:
        sessions.update_metadata(
            session_id,
            {
                "claude_session_id": provider_session_id,
                "provider_session_id": provider_session_id,
                "claude_session_started": "true",
            },
        )
    if message_type == "system" and subtype == "init":
        sessions.append_event(
            session_id,
            "provider.session.ready",
            {
                **common,
                "provider_session_id": provider_session_id,
                "model": str(message.get("model") or ""),
                "cwd": str(message.get("cwd") or ""),
            },
        )
    elif message_type == "assistant":
        for item in _content_items(message):
            item_type = str(item.get("type") or "")
            if item_type == "text":
                text = str(item.get("text") or "")
                if text:
                    sessions.append_event(session_id, "assistant.message", {**common, "text": text})
            elif item_type == "tool_use":
                sessions.append_event(session_id, "tool.call", {**common, "item": item})
            elif item_type == "tool_result":
                sessions.append_event(session_id, "tool.result", {**common, "item": item})
    elif message_type == "result":
        is_error = subtype not in {"success", "done", "completed"}
        event_type = "turn.failed" if is_error else "turn.completed"
        sessions.update_status(session_id, "failed" if is_error else "completed")
        sessions.append_event(session_id, event_type, {**common, "provider_status": subtype or message_type, "raw": message})
        return True
    elif message_type:
        sessions.append_event(session_id, "provider.event", {**common, "raw": message})
    return False


def _content_items(message: dict[str, Any]) -> list[dict[str, Any]]:
    content = message.get("content")
    if content is None:
        content = dict(message.get("message") or {}).get("content")
    if isinstance(content, list):
        return [dict(item) for item in content if isinstance(item, dict)]
    return []


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
            name=f"jarvis-claude-{name}-reader",
            daemon=True,
        ).start()
    return line_queue


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
            "provider": "claude",
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


def _claude_session_id(session: WorkerSession) -> str:
    value = str(
        session.metadata.get("claude_session_id")
        or session.metadata.get("provider_session_id")
        or session.metadata.get("session_id")
        or ""
    ).strip()
    if value:
        return value
    return str(uuid.uuid4())


def _terminate_provider_process(session: WorkerSession) -> None:
    pid = str(session.metadata.get("provider_pid") or "").strip()
    if not pid:
        return
    try:
        os.kill(int(pid), signal.SIGTERM)
    except (OSError, ValueError):
        return
