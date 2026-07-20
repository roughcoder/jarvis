from __future__ import annotations

import json
import pathlib
import shutil
import threading
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

from jarvis.ids import new_id, utc_now
from jarvis.redaction import public_error_message
from jarvis.worker_session_contract import (
    ACTIVE_SESSION_STATUSES,
    CHECKPOINT_ID_KEY,
    EVENT_CHECKPOINT_CREATED,
    EVENT_CHECKPOINT_RESTORED,
    EVENT_SESSION_CREATED,
    EVENT_SESSION_INTERRUPTED,
    EVENT_SESSION_STOPPED,
    EVENT_TURN_COMPLETED,
    EVENT_TURN_FAILED,
    EVENT_TURN_STARTED,
    FAILED_SESSION_STATUSES,
    IDEMPOTENT_SESSION_EVENT_TYPES,
    SESSION_CREATED,
    SESSION_COMPLETED,
    SESSION_FAILED,
    SESSION_INTERRUPTED,
    SESSION_RUNNING,
    SESSION_STOPPED,
    SUCCESS_SESSION_STATUSES,
    TURN_RESUMABLE_SESSION_STATUSES,
    TURN_STARTABLE_SESSION_STATUSES,
    WORKER_ERROR_SESSION_ACTIVE,
    WORKER_ERROR_SESSION_TERMINAL,
    request_type as contract_request_type,
    resolved_request_type as contract_resolved_request_type,
)


class SessionTurnConflict(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)

# Why a session ended, keyed by the terminal status a transition lands on.
# "interrupted" defaults to a user-initiated interrupt; the daemon-restart
# sweep overrides it with "worker_lost" because only that code path knows.
ENDED_REASONS_BY_STATUS = {
    "completed": "completed",
    "done": "completed",
    "stopped": "stopped",
    "failed": "engine_error",
    "error": "engine_error",
    "interrupted": "interrupted_by_user",
}

PROVIDER_OWNED_METADATA_KEYS = {
    "ended_reason",
    "provider_pid",
    "provider_runtime",
    "provider_cwd",
    "provider_session_id",
    "codex_thread_id",
    "claude_session_id",
    "claude_session_started",
    "active_turn",
    "execution_pending_requests",
}

EXECUTION_EVENT_TAIL_BYTES = 1024 * 1024


@dataclass
class SessionEvent:
    event_id: str
    session_id: str
    type: str
    data: dict[str, Any] = field(default_factory=dict)
    time: str = field(default_factory=utc_now)

    @classmethod
    def create(cls, session_id: str, event_type: str, data: dict[str, Any] | None = None) -> SessionEvent:
        return cls(event_id=new_id("ev"), session_id=session_id, type=event_type, data=data or {})

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SessionEvent:
        if not _valid_id(str(data.get("event_id") or "")):
            raise ValueError(f"invalid event id {data.get('event_id')!r}")
        if not _valid_id(str(data.get("session_id") or "")):
            raise ValueError(f"invalid session id {data.get('session_id')!r}")
        if not _valid_event_type(str(data.get("type") or "")):
            raise ValueError(f"invalid event type {data.get('type')!r}")
        return cls(
            event_id=str(data.get("event_id") or ""),
            session_id=str(data.get("session_id") or ""),
            type=str(data.get("type") or ""),
            data=dict(data.get("data") or {}),
            time=str(data.get("time") or utc_now()),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WorkerSession:
    session_id: str
    provider: str
    engine: str
    status: str = SESSION_CREATED
    run_id: str = ""
    repo: str = ""
    branch: str = ""
    cwd: str = ""
    title: str = ""
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerSession:
        if not _valid_id(str(data.get("session_id") or "")):
            raise ValueError(f"invalid session id {data.get('session_id')!r}")
        return cls(
            session_id=str(data["session_id"]),
            provider=str(data.get("provider") or "codex"),
            engine=str(data.get("engine") or data.get("provider") or "codex"),
            status=str(data.get("status") or SESSION_CREATED),
            run_id=str(data.get("run_id") or ""),
            repo=str(data.get("repo") or ""),
            branch=str(data.get("branch") or ""),
            cwd=str(data.get("cwd") or ""),
            title=str(data.get("title") or ""),
            created_at=str(data.get("created_at") or utc_now()),
            updated_at=str(data.get("updated_at") or utc_now()),
            metadata=dict(data.get("metadata") or {}),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SessionManager:
    def __init__(self, store_dir: str, *, on_change: Callable[[str, str], None] | None = None) -> None:
        self.root = pathlib.Path(store_dir).expanduser()
        self.root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._on_change = on_change
        self._interrupt_stale_active_sessions()

    def create(self, data: dict[str, Any]) -> tuple[WorkerSession, SessionEvent]:
        with self._lock:
            provider = _clean_id(data.get("provider") or data.get("engine") or "codex")
            engine = _clean_id(data.get("engine") or provider)
            session_id = str(data.get("session_id") or new_id("sess"))
            if not _valid_id(session_id):
                raise ValueError(f"invalid session id {session_id!r}")
            if self.session_path(session_id).exists():
                raise ValueError(f"worker session already exists: {session_id}")
            session = WorkerSession(
                session_id=session_id,
                provider=provider,
                engine=engine,
                status=SESSION_CREATED,
                run_id=str(data.get("run_id") or ""),
                repo=str(data.get("repo") or ""),
                branch=str(data.get("branch") or ""),
                cwd=str(data.get("cwd") or ""),
                title=str(data.get("title") or data.get("name") or ""),
                metadata=_caller_metadata(data.get("metadata") or {}),
            )
            self.save(session)
            event = self.append_event(
                session.session_id,
                EVENT_SESSION_CREATED,
                {
                    "provider": session.provider,
                    "engine": session.engine,
                    "run_id": session.run_id,
                    "repo": session.repo,
                    "branch": session.branch,
                },
            )
            return session, event

    def get(self, session_id: str) -> WorkerSession | None:
        try:
            path = self.session_path(session_id)
        except ValueError:
            return None
        if not path.exists():
            matches = [x for x in self.root.glob(f"{session_id}*") if (x / "session.json").exists()]
            if len(matches) == 1:
                path = matches[0] / "session.json"
            else:
                return None
        try:
            return WorkerSession.from_dict(json.loads(path.read_text()))
        except (json.JSONDecodeError, ValueError, KeyError):
            return None

    def list(self) -> list[WorkerSession]:
        sessions: list[WorkerSession] = []
        for path in sorted(self.root.glob("*/session.json")):
            try:
                sessions.append(WorkerSession.from_dict(json.loads(path.read_text())))
            except (OSError, json.JSONDecodeError, ValueError, KeyError):
                continue
        return sorted(sessions, key=lambda x: x.updated_at)

    def update_status(self, session_id: str, status: str, *, ended_reason: str = "") -> WorkerSession:
        with self._lock:
            session = self.get(session_id)
            if session is None:
                raise KeyError(session_id)
            session.status = status
            if status not in ACTIVE_SESSION_STATUSES:
                session.metadata.pop("active_turn", None)
                session.metadata["execution_pending_requests"] = []
            _apply_ended_reason(session, status, override=ended_reason)
            session.updated_at = utc_now()
            self.save(session)
        self._changed(session_id, "session_status")
        return session

    def update_metadata(self, session_id: str, metadata: dict[str, Any]) -> WorkerSession:
        with self._lock:
            session = self.get(session_id)
            if session is None:
                raise KeyError(session_id)
            session.metadata.update(metadata)
            session.updated_at = utc_now()
            self.save(session)
            return session

    def update_workspace(
        self,
        session_id: str,
        *,
        cwd: str = "",
        branch: str = "",
        repo: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> WorkerSession:
        with self._lock:
            session = self.get(session_id)
            if session is None:
                raise KeyError(session_id)
            if cwd:
                session.cwd = cwd
            if branch:
                session.branch = branch
            if repo:
                session.repo = repo
            if metadata:
                session.metadata.update(metadata)
            session.updated_at = utc_now()
            self.save(session)
            return session

    def delete(self, session_id: str) -> None:
        with self._lock:
            directory = self.session_dir(session_id)
            if directory.exists():
                shutil.rmtree(directory)

    def append_event(self, session_id: str, event_type: str, data: dict[str, Any] | None = None) -> SessionEvent:
        with self._lock:
            session = self.get(session_id)
            if session is None:
                raise KeyError(session_id)
            if not _valid_event_type(event_type):
                raise ValueError(f"invalid event type {event_type!r}")
            existing = self._idempotent_event(session.session_id, event_type, data or {})
            if existing is not None:
                return existing
            event = SessionEvent.create(session.session_id, event_type, data)
            with self.events_path(session.session_id).open("a") as f:
                f.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")
            _update_execution_pending_requests(session, event)
            session.updated_at = event.time
            self.save(session)
        self._changed(session_id, "session_event")
        return event

    def append_event_with_status(
        self,
        session_id: str,
        status: str,
        event_type: str,
        data: dict[str, Any] | None = None,
    ) -> SessionEvent:
        with self._lock:
            session = self.get(session_id)
            if session is None:
                raise KeyError(session_id)
            if not _valid_event_type(event_type):
                raise ValueError(f"invalid event type {event_type!r}")
            existing = self._idempotent_event(session.session_id, event_type, data or {})
            session.status = status
            if status not in ACTIVE_SESSION_STATUSES:
                session.metadata.pop("active_turn", None)
                session.metadata["execution_pending_requests"] = []
            _apply_ended_reason(session, status)
            session.updated_at = utc_now()
            self.save(session)
            if existing is not None:
                event = existing
            else:
                event = SessionEvent.create(session.session_id, event_type, data)
                with self.events_path(session.session_id).open("a") as f:
                    f.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")
                session.updated_at = event.time
                self.save(session)
        self._changed(session_id, "session_event")
        return event

    def restore_running_if_waiting(self, session_id: str, waiting_status: str, *, has_pending_requests: bool) -> WorkerSession | None:
        changed = False
        with self._lock:
            session = self.get(session_id)
            if session is None or session.status != waiting_status:
                return session
            terminal_status = self._terminal_status_from_events(session.session_id)
            if terminal_status is not None:
                session.status = terminal_status
                session.metadata.pop("active_turn", None)
                session.metadata["execution_pending_requests"] = []
                _apply_ended_reason(session, terminal_status)
                session.updated_at = utc_now()
                self.save(session)
                changed = True
            elif not has_pending_requests:
                session.status = SESSION_RUNNING
                _apply_ended_reason(session, SESSION_RUNNING)
                session.updated_at = utc_now()
                self.save(session)
                changed = True
        if changed:
            self._changed(session_id, "session_status")
        return session

    def reserve_turn(self, session_id: str, data: dict[str, Any]) -> tuple[WorkerSession, SessionEvent, bool]:
        with self._lock:
            session = self.get(session_id)
            if session is None:
                raise KeyError(session_id)
            existing = self._idempotent_event(session.session_id, EVENT_TURN_STARTED, data)
            if existing is not None:
                return session, existing, False
            if session.status in (ACTIVE_SESSION_STATUSES - TURN_STARTABLE_SESSION_STATUSES):
                raise SessionTurnConflict(
                    WORKER_ERROR_SESSION_ACTIVE,
                    f"worker session {session.session_id} already has an active turn",
                )
            resume = bool(dict(data.get("metadata") or {}).get("resume_session"))
            if session.status not in TURN_STARTABLE_SESSION_STATUSES and not (
                resume and session.status in TURN_RESUMABLE_SESSION_STATUSES
            ):
                raise SessionTurnConflict(
                    WORKER_ERROR_SESSION_TERMINAL,
                    f"worker session {session.session_id} is {session.status} and does not accept new turns",
                )
            event = SessionEvent.create(session.session_id, EVENT_TURN_STARTED, data)
            with self.events_path(session.session_id).open("a") as f:
                f.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")
            session.status = SESSION_RUNNING
            session.metadata["active_turn"] = {
                "turn_id": str(data.get("turn_id") or ""),
                "started_at": event.time,
            }
            session.metadata.setdefault("execution_pending_requests", [])
            _apply_ended_reason(session, SESSION_RUNNING)
            session.updated_at = event.time
            self.save(session)
        self._changed(session_id, "session_event")
        return session, event, True

    def _changed(self, session_id: str, kind: str) -> None:
        if self._on_change is None:
            return
        try:
            self._on_change(session_id, kind)
        except Exception:  # noqa: BLE001 - change hints must never affect a worker session
            pass

    def _interrupt_stale_active_sessions(self) -> None:
        changed: list[str] = []
        with self._lock:
            for path in sorted(self.root.glob("*/session.json")):
                try:
                    session = WorkerSession.from_dict(json.loads(path.read_text()))
                except (OSError, json.JSONDecodeError, ValueError, KeyError):
                    continue
                if session.status not in (ACTIVE_SESSION_STATUSES - {SESSION_CREATED}):
                    continue
                event = SessionEvent.create(
                    session.session_id,
                    EVENT_SESSION_INTERRUPTED,
                    {"reason": "worker daemon restarted with active session state", "previous_status": session.status},
                )
                session.status = SESSION_INTERRUPTED
                session.metadata.pop("active_turn", None)
                session.metadata["execution_pending_requests"] = []
                _apply_ended_reason(session, SESSION_INTERRUPTED, override="worker_lost")
                session.updated_at = event.time
                self.save(session)
                with self.events_path(session.session_id).open("a") as f:
                    f.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")
                changed.append(session.session_id)
        for session_id in changed:
            self._changed(session_id, "session_event")

    def _terminal_status_from_events(self, session_id: str) -> str | None:
        status_by_event = {
            EVENT_SESSION_INTERRUPTED: SESSION_INTERRUPTED,
            EVENT_SESSION_STOPPED: SESSION_STOPPED,
            EVENT_TURN_COMPLETED: SESSION_COMPLETED,
            EVENT_TURN_FAILED: SESSION_FAILED,
        }
        for event in reversed(self.events(session_id)):
            status = status_by_event.get(event.type)
            if status is not None:
                return status
        return None

    def events(self, session_id: str, *, after: str = "", limit: int | None = None) -> list[SessionEvent]:
        try:
            path = self.events_path(session_id)
        except ValueError:
            return []
        if not path.exists():
            return []
        events: list[SessionEvent] = []
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                events.append(SessionEvent.from_dict(json.loads(line)))
            except (json.JSONDecodeError, ValueError):
                continue
        if after:
            for idx, event in enumerate(events):
                if event.event_id == after:
                    events = events[idx + 1 :]
                    break
        if limit is not None and limit >= 0:
            events = events[:limit]
        return events

    def pending_requests(self, session_id: str | None = None) -> list[dict[str, Any]]:
        sessions = [self.get(session_id)] if session_id else self.list()
        pending: list[dict[str, Any]] = []
        for session in [x for x in sessions if x is not None]:
            if session.status in FAILED_SESSION_STATUSES or session.status in SUCCESS_SESSION_STATUSES:
                continue
            stored = session.metadata.get("execution_pending_requests")
            if isinstance(stored, list):
                pending.extend(
                    _pending_request_from_execution_projection(session, item)
                    for item in stored
                    if isinstance(item, dict)
                )
                continue
            pending.extend(_pending_requests_from_events(session, self._execution_events(session.session_id)))
        return pending

    def execution_state(self, session_id: str) -> dict[str, Any] | None:
        """Project the current turn and requests from the durable event log."""
        session = self.get(session_id)
        if session is None:
            return None
        stored_active = session.metadata.get("active_turn")
        stored_pending = session.metadata.get("execution_pending_requests")
        needs_events = (
            session.status in ACTIVE_SESSION_STATUSES
            and session.status != SESSION_CREATED
            and (not isinstance(stored_active, dict) or not isinstance(stored_pending, list))
        )
        events = self._execution_events(session_id) if needs_events else []
        active_turn = (
            {
                "turn_id": str(stored_active.get("turn_id") or ""),
                "status": session.status,
                "started_at": str(stored_active.get("started_at") or ""),
            }
            if isinstance(stored_active, dict) and str(stored_active.get("turn_id") or "")
            else _active_turn_from_events(events, session.status)
        )
        if session.status not in ACTIVE_SESSION_STATUSES or session.status == SESSION_CREATED:
            active_turn = None

        return {
            "session_id": session.session_id,
            "status": session.status,
            "active_turn": active_turn,
            "pending_requests": _execution_pending_requests(session, events),
        }

    def _execution_events(self, session_id: str) -> list[SessionEvent]:
        """Read a bounded tail; providers pause after opening human requests."""
        try:
            path = self.events_path(session_id)
        except ValueError:
            return []
        if not path.exists():
            return []
        with path.open("rb") as stream:
            stream.seek(0, 2)
            size = stream.tell()
            start = max(0, size - EXECUTION_EVENT_TAIL_BYTES)
            stream.seek(start)
            payload = stream.read(EXECUTION_EVENT_TAIL_BYTES)
        if start > 0:
            newline = payload.find(b"\n")
            payload = payload[newline + 1 :] if newline >= 0 else b""
        events: list[SessionEvent] = []
        for line in payload.splitlines():
            if not line.strip():
                continue
            try:
                events.append(SessionEvent.from_dict(json.loads(line)))
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                continue
        return events

    def checkpoints(self, session_id: str) -> list[dict[str, Any]]:
        events = self.events(session_id)
        restored = {
            str(event.data.get(CHECKPOINT_ID_KEY) or "")
            for event in events
            if event.type == EVENT_CHECKPOINT_RESTORED and event.data.get(CHECKPOINT_ID_KEY)
        }
        checkpoints: list[dict[str, Any]] = []
        for event in events:
            if event.type != EVENT_CHECKPOINT_CREATED:
                continue
            checkpoint_id = str(event.data.get(CHECKPOINT_ID_KEY) or event.event_id)
            checkpoints.append(
                {
                    "session_id": session_id,
                    "checkpoint_id": checkpoint_id,
                    "label": str(event.data.get("label") or ""),
                    "provider": str(event.data.get("provider") or ""),
                    "event": event.to_dict(),
                    "restored": checkpoint_id in restored,
                }
            )
        return checkpoints

    def all_checkpoints(self) -> list[dict[str, Any]]:
        with self._lock:
            return [
                checkpoint
                for session in sorted(self.list(), key=lambda item: item.session_id)
                for checkpoint in self.checkpoints(session.session_id)
            ]

    def remove(self, session_id: str) -> bool:
        with self._lock:
            try:
                directory = self.session_dir(session_id)
            except ValueError:
                return False
            if not directory.exists():
                return False
            shutil.rmtree(directory)
            return True

    def save(self, session: WorkerSession) -> None:
        with self._lock:
            directory = self.session_dir(session.session_id)
            directory.mkdir(parents=True, exist_ok=True)
            path = self.session_path(session.session_id)
            tmp = path.with_name(f"session.{threading.get_ident()}.{new_id('tmp')}.json.tmp")
            tmp.write_text(json.dumps(session.to_dict(), indent=2, sort_keys=True))
            tmp.replace(path)

    def session_dir(self, session_id: str) -> pathlib.Path:
        if not _valid_id(session_id):
            raise ValueError(f"invalid session id {session_id!r}")
        root = self.root.resolve()
        path = (self.root / session_id).resolve(strict=False)
        if not path.is_relative_to(root):
            raise ValueError(f"session id escapes worker session store: {session_id!r}")
        return path

    def session_path(self, session_id: str) -> pathlib.Path:
        return self.session_dir(session_id) / "session.json"

    def events_path(self, session_id: str) -> pathlib.Path:
        return self.session_dir(session_id) / "events.jsonl"

    def _idempotent_event(
        self,
        session_id: str,
        event_type: str,
        data: dict[str, Any],
    ) -> SessionEvent | None:
        key = str(data.get("idempotency_key") or "").strip()
        if not key or event_type not in IDEMPOTENT_SESSION_EVENT_TYPES:
            return None
        for event in self.events(session_id):
            if event.type == event_type and str(event.data.get("idempotency_key") or "") == key:
                return event
        return None


def _apply_ended_reason(session: WorkerSession, status: str, *, override: str = "") -> None:
    if status in ACTIVE_SESSION_STATUSES:
        session.metadata.pop("ended_reason", None)
        return
    reason = override or ENDED_REASONS_BY_STATUS.get(status, "")
    if reason:
        session.metadata["ended_reason"] = reason


def _execution_request(item: dict[str, Any]) -> dict[str, Any]:
    event = item.get("event") if isinstance(item.get("event"), dict) else {}
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    nested_input = data.get("input") if isinstance(data.get("input"), dict) else {}
    kind = str(item.get("kind") or "")
    detail = str(
        data.get("detail")
        or data.get("description")
        or nested_input.get("description")
        or data.get("command")
        or nested_input.get("command")
        or data.get("path")
        or data.get("blocked_path")
        or nested_input.get("path")
        or data.get("prompt")
        or data.get("question")
        or ""
    )
    result = {
        "request_id": str(item.get("request_id") or ""),
        "kind": kind,
        "status": str(item.get("status") or "pending"),
        "title": public_error_message(
            str(data.get("title") or ("Approve action" if kind == "approval" else "Input needed"))
        ),
        "detail": public_error_message(detail),
        "created_at": str(event.get("time") or data.get("created_at") or ""),
    }
    if kind == "approval":
        result["request_kind"] = _execution_request_kind(data, nested_input)
    if kind == "input":
        result["questions"] = _execution_questions(nested_input or data, detail)
    return result


def _execution_questions(data: dict[str, Any], fallback: str) -> list[dict[str, Any]]:
    raw_questions = data.get("questions")
    questions: list[dict[str, Any]] = []
    if isinstance(raw_questions, list):
        for raw in raw_questions[:20]:
            if not isinstance(raw, dict):
                continue
            raw_options = raw.get("options") if isinstance(raw.get("options"), list) else []
            options = []
            for option in raw_options[:20]:
                if isinstance(option, dict):
                    options.append(
                        {
                            "label": public_error_message(
                                str(option.get("label") or option.get("value") or "Option")
                            ),
                            "description": public_error_message(str(option.get("description") or "")),
                        }
                    )
                else:
                    options.append(
                        {"label": public_error_message(str(option) or "Option"), "description": ""}
                    )
            questions.append(
                {
                    "id": public_error_message(str(raw.get("id") or "response")),
                    "header": public_error_message(str(raw.get("header") or "Input")),
                    "question": public_error_message(
                        str(raw.get("question") or fallback or "Input needed")
                    ),
                    "options": options,
                    "multi_select": bool(raw.get("multiSelect") or raw.get("multi_select")),
                }
            )
    if questions:
        return questions
    return [
        {
            "id": "response",
            "header": "Input",
            "question": public_error_message(fallback or "Input needed"),
            "options": [],
            "multi_select": False,
        }
    ]


def _execution_request_kind(data: dict[str, Any], nested_input: dict[str, Any]) -> str:
    explicit = str(data.get("request_kind") or nested_input.get("request_kind") or "")
    if explicit in {"command", "file-read", "file-change"}:
        return explicit
    tool_name = str(data.get("tool_name") or "").lower()
    if "read" in tool_name:
        return "file-read"
    if any(word in tool_name for word in ("edit", "write", "notebook")):
        return "file-change"
    return "command"


def _active_turn_from_events(events: list[SessionEvent], status: str) -> dict[str, str] | None:
    for event in reversed(events):
        turn_id = str(event.data.get("turn_id") or "")
        if turn_id:
            return {"turn_id": turn_id, "status": status, "started_at": event.time}
    return None


def _pending_requests_from_events(
    session: WorkerSession,
    events: list[SessionEvent],
) -> list[dict[str, Any]]:
    if session.status in FAILED_SESSION_STATUSES or session.status in SUCCESS_SESSION_STATUSES:
        return []
    pending: dict[tuple[str, str, str], dict[str, Any]] = {}
    for event in events:
        request_kind = contract_request_type(event.type)
        if request_kind:
            request_id = str(event.data.get("request_id") or event.data.get("id") or event.event_id)
            pending[(session.session_id, request_kind, request_id)] = {
                "session_id": session.session_id,
                "request_id": request_id,
                "kind": request_kind,
                "status": "pending",
                "event": event.to_dict(),
            }
            continue
        resolved_kind = contract_resolved_request_type(event.type)
        if resolved_kind:
            request_id = str(event.data.get("request_id") or event.data.get("id") or "")
            if request_id:
                pending.pop((session.session_id, resolved_kind, request_id), None)
    return list(pending.values())


def _pending_request_from_execution_projection(
    session: WorkerSession,
    item: dict[str, Any],
) -> dict[str, Any]:
    request_id = str(item.get("request_id") or "")
    kind = str(item.get("kind") or "")
    created_at = str(item.get("created_at") or "")
    data: dict[str, Any] = {
        "request_id": request_id,
        "title": str(item.get("title") or ""),
        "detail": str(item.get("detail") or ""),
        "created_at": created_at,
    }
    if kind == "approval" and item.get("request_kind"):
        data["request_kind"] = str(item["request_kind"])
    if kind == "input" and isinstance(item.get("questions"), list):
        data["questions"] = item["questions"]
    return {
        "session_id": session.session_id,
        "request_id": request_id,
        "kind": kind,
        "status": str(item.get("status") or "pending"),
        "event": {
            "event_id": request_id,
            "session_id": session.session_id,
            "type": f"{kind}.requested",
            "data": data,
            "time": created_at,
        },
    }


def _execution_pending_requests(
    session: WorkerSession,
    events: list[SessionEvent],
) -> list[dict[str, Any]]:
    stored = session.metadata.get("execution_pending_requests")
    if isinstance(stored, list):
        return [dict(item) for item in stored if isinstance(item, dict)]
    return [_execution_request(item) for item in _pending_requests_from_events(session, events)]


def _update_execution_pending_requests(session: WorkerSession, event: SessionEvent) -> None:
    request_kind = contract_request_type(event.type)
    resolved_kind = contract_resolved_request_type(event.type)
    if not request_kind and not resolved_kind:
        return
    pending = [
        dict(item)
        for item in session.metadata.get("execution_pending_requests", [])
        if isinstance(item, dict)
    ]
    request_id = str(event.data.get("request_id") or event.data.get("id") or event.event_id)
    pending = [
        item
        for item in pending
        if not (
            str(item.get("request_id") or "") == request_id
            and str(item.get("kind") or "") == (request_kind or resolved_kind)
        )
    ]
    if request_kind:
        pending.append(
            _execution_request(
                {
                    "session_id": session.session_id,
                    "request_id": request_id,
                    "kind": request_kind,
                    "status": "pending",
                    "event": event.to_dict(),
                }
            )
        )
    session.metadata["execution_pending_requests"] = pending


def _valid_id(value: str) -> bool:
    return bool(value) and all(ch.isalnum() or ch in {"_", "-"} for ch in value)


def _valid_event_type(value: str) -> bool:
    return bool(value) and all(ch.isalnum() or ch in {"_", ".", "-"} for ch in value)


def _clean_id(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if _valid_id(text) else "unknown"


def _caller_metadata(value: Any) -> dict[str, Any]:
    metadata = dict(value or {}) if isinstance(value, dict) else {}
    return {key: item for key, item in metadata.items() if key not in PROVIDER_OWNED_METADATA_KEYS}
