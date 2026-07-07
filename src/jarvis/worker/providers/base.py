from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from jarvis.config import WorkerConfig
from jarvis.worker.sessions import SessionEvent, SessionManager, WorkerSession
from jarvis.worker_session_contract import (
    EVENT_APPROVAL_RESOLVED,
    EVENT_CHECKPOINT_RESTORED,
    EVENT_INPUT_RECEIVED,
    EVENT_SESSION_INTERRUPTED,
    EVENT_SESSION_STOPPED,
    SESSION_INTERRUPTED,
    SESSION_STOPPED,
)


@dataclass
class ProviderTurn:
    turn_id: str
    prompt: str
    metadata: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str = ""
    # Image attachments: [{kind, mime_type, name, data_url}]. Full payloads are
    # request-scoped only; session events must carry summaries, never base64.
    attachments: list[dict[str, Any]] = field(default_factory=list)


class ProviderAdapter(Protocol):
    provider: str

    def capabilities(self) -> dict[str, Any]:
        """Return provider runtime capabilities for UI/debug surfaces."""

    def start_turn(
        self,
        *,
        session: WorkerSession,
        turn: ProviderTurn,
        sessions: SessionManager,
        worker_cfg: WorkerConfig,
    ) -> list[SessionEvent]:
        """Start or enqueue a provider turn and project canonical events."""

    def receive_input(
        self,
        *,
        session: WorkerSession,
        request: dict[str, Any],
        sessions: SessionManager,
    ) -> SessionEvent:
        return sessions.append_event(session.session_id, EVENT_INPUT_RECEIVED, request)

    def resolve_approval(
        self,
        *,
        session: WorkerSession,
        request: dict[str, Any],
        sessions: SessionManager,
    ) -> SessionEvent:
        return sessions.append_event(session.session_id, EVENT_APPROVAL_RESOLVED, request)

    def interrupt(self, *, session: WorkerSession, sessions: SessionManager) -> tuple[WorkerSession, SessionEvent]:
        updated = sessions.update_status(session.session_id, SESSION_INTERRUPTED)
        event = sessions.append_event(updated.session_id, EVENT_SESSION_INTERRUPTED, {"status": SESSION_INTERRUPTED})
        return updated, event

    def stop(self, *, session: WorkerSession, sessions: SessionManager) -> tuple[WorkerSession, SessionEvent]:
        updated = sessions.update_status(session.session_id, SESSION_STOPPED)
        event = sessions.append_event(updated.session_id, EVENT_SESSION_STOPPED, {"status": SESSION_STOPPED})
        return updated, event

    def restore_checkpoint(
        self,
        *,
        session: WorkerSession,
        request: dict[str, Any],
        sessions: SessionManager,
    ) -> SessionEvent:
        return sessions.append_event(session.session_id, EVENT_CHECKPOINT_RESTORED, request)
