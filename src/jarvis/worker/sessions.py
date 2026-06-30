from __future__ import annotations

import json
import pathlib
from dataclasses import asdict, dataclass, field
from typing import Any

from jarvis.ids import new_id, utc_now


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
    status: str = "created"
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
            status=str(data.get("status") or "created"),
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
    def __init__(self, store_dir: str) -> None:
        self.root = pathlib.Path(store_dir).expanduser()
        self.root.mkdir(parents=True, exist_ok=True)

    def create(self, data: dict[str, Any]) -> tuple[WorkerSession, SessionEvent]:
        provider = _clean_id(data.get("provider") or data.get("engine") or "codex")
        engine = _clean_id(data.get("engine") or provider)
        session_id = str(data.get("session_id") or new_id("sess"))
        if not _valid_id(session_id):
            raise ValueError(f"invalid session id {session_id!r}")
        session = WorkerSession(
            session_id=session_id,
            provider=provider,
            engine=engine,
            status="created",
            run_id=str(data.get("run_id") or ""),
            repo=str(data.get("repo") or ""),
            branch=str(data.get("branch") or ""),
            cwd=str(data.get("cwd") or ""),
            title=str(data.get("title") or data.get("name") or ""),
            metadata=dict(data.get("metadata") or {}),
        )
        self.save(session)
        event = self.append_event(
            session.session_id,
            "session.created",
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
            except (OSError, json.JSONDecodeError, KeyError):
                continue
        return sorted(sessions, key=lambda x: x.updated_at)

    def update_status(self, session_id: str, status: str) -> WorkerSession:
        session = self.get(session_id)
        if session is None:
            raise KeyError(session_id)
        session.status = status
        session.updated_at = utc_now()
        self.save(session)
        return session

    def update_metadata(self, session_id: str, metadata: dict[str, Any]) -> WorkerSession:
        session = self.get(session_id)
        if session is None:
            raise KeyError(session_id)
        session.metadata.update(metadata)
        session.updated_at = utc_now()
        self.save(session)
        return session

    def append_event(self, session_id: str, event_type: str, data: dict[str, Any] | None = None) -> SessionEvent:
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
        session.updated_at = event.time
        self.save(session)
        return event

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
        pending: dict[tuple[str, str, str], dict[str, Any]] = {}
        for session in [x for x in sessions if x is not None]:
            for event in self.events(session.session_id):
                request_type = _request_type(event.type)
                if request_type:
                    request_id = str(event.data.get("request_id") or event.data.get("id") or event.event_id)
                    pending[(session.session_id, request_type, request_id)] = {
                        "session_id": session.session_id,
                        "request_id": request_id,
                        "kind": request_type,
                        "status": "pending",
                        "event": event.to_dict(),
                    }
                    continue
                resolved_type = _resolved_request_type(event.type)
                if resolved_type:
                    request_id = str(event.data.get("request_id") or event.data.get("id") or "")
                    if request_id:
                        pending.pop((session.session_id, resolved_type, request_id), None)
        return list(pending.values())

    def checkpoints(self, session_id: str) -> list[dict[str, Any]]:
        events = self.events(session_id)
        restored = {
            str(event.data.get("checkpoint_id") or "")
            for event in events
            if event.type == "checkpoint.restored" and event.data.get("checkpoint_id")
        }
        checkpoints: list[dict[str, Any]] = []
        for event in events:
            if event.type != "checkpoint.created":
                continue
            checkpoint_id = str(event.data.get("checkpoint_id") or event.event_id)
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

    def save(self, session: WorkerSession) -> None:
        directory = self.session_dir(session.session_id)
        directory.mkdir(parents=True, exist_ok=True)
        self.session_path(session.session_id).write_text(json.dumps(session.to_dict(), indent=2, sort_keys=True))

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
        if not key or event_type not in _IDEMPOTENT_EVENT_TYPES:
            return None
        for event in self.events(session_id):
            if event.type == event_type and str(event.data.get("idempotency_key") or "") == key:
                return event
        return None


def _valid_id(value: str) -> bool:
    return bool(value) and all(ch.isalnum() or ch in {"_", "-"} for ch in value)


def _valid_event_type(value: str) -> bool:
    return bool(value) and all(ch.isalnum() or ch in {"_", ".", "-"} for ch in value)


def _clean_id(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if _valid_id(text) else "unknown"


def _request_type(event_type: str) -> str:
    if event_type == "approval.requested":
        return "approval"
    if event_type == "input.requested":
        return "input"
    return ""


def _resolved_request_type(event_type: str) -> str:
    if event_type == "approval.resolved":
        return "approval"
    if event_type == "input.received":
        return "input"
    return ""


_IDEMPOTENT_EVENT_TYPES = {
    "turn.started",
    "provider.started",
    "turn.completed",
    "turn.failed",
}
