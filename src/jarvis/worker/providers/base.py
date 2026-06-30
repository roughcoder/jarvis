from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from jarvis.config import WorkerConfig
from jarvis.worker.sessions import SessionEvent, SessionManager, WorkerSession


@dataclass
class ProviderTurn:
    turn_id: str
    prompt: str
    metadata: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str = ""


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
        return sessions.append_event(session.session_id, "input.received", request)

    def resolve_approval(
        self,
        *,
        session: WorkerSession,
        request: dict[str, Any],
        sessions: SessionManager,
    ) -> SessionEvent:
        return sessions.append_event(session.session_id, "approval.resolved", request)

    def interrupt(self, *, session: WorkerSession, sessions: SessionManager) -> tuple[WorkerSession, SessionEvent]:
        updated = sessions.update_status(session.session_id, "interrupted")
        event = sessions.append_event(updated.session_id, "session.interrupted", {"status": "interrupted"})
        return updated, event

    def stop(self, *, session: WorkerSession, sessions: SessionManager) -> tuple[WorkerSession, SessionEvent]:
        updated = sessions.update_status(session.session_id, "stopped")
        event = sessions.append_event(updated.session_id, "session.stopped", {"status": "stopped"})
        return updated, event
