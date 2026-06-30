from __future__ import annotations

from typing import Any

from jarvis.config import WorkerConfig
from jarvis.worker.providers.base import ProviderTurn
from jarvis.worker.sessions import SessionEvent, SessionManager, WorkerSession


class FakeProviderAdapter:
    provider = "fake"

    def capabilities(self) -> dict[str, Any]:
        return {
            "streaming": True,
            "resume": True,
            "interrupt": True,
            "approvals": True,
            "questions": True,
            "checkpoints": True,
            "rollback": False,
            "lifecycle": "in-process deterministic fake provider",
            "backpressure": "synchronous append; bounded by HTTP request",
            "event_ordering": "append order in events.jsonl",
        }

    def start_turn(
        self,
        *,
        session: WorkerSession,
        turn: ProviderTurn,
        sessions: SessionManager,
        worker_cfg: WorkerConfig,
    ) -> list[SessionEvent]:
        sessions.update_status(session.session_id, "running")
        common = {
            "turn_id": turn.turn_id,
            "idempotency_key": turn.idempotency_key,
            "provider": self.provider,
        }
        events = [
            sessions.append_event(
                session.session_id,
                "assistant.delta",
                {**common, "text": "Fake provider accepted the turn."},
            ),
            sessions.append_event(
                session.session_id,
                "assistant.message",
                {**common, "text": f"Fake provider completed: {turn.prompt}".strip()},
            ),
            sessions.append_event(
                session.session_id,
                "checkpoint.created",
                {**common, "checkpoint_id": f"ckpt_{turn.turn_id}", "label": "fake checkpoint"},
            ),
            sessions.append_event(session.session_id, "turn.completed", common),
        ]
        sessions.update_status(session.session_id, "completed")
        return events
