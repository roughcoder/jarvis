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
        lower_prompt = turn.prompt.lower()
        if "request approval" in lower_prompt:
            request_id = f"approval_{turn.turn_id}"
            event = sessions.append_event(
                session.session_id,
                "approval.requested",
                {
                    **common,
                    "request_id": request_id,
                    "action": "fake.tool.execute",
                    "prompt": "Approve fake provider action?",
                },
            )
            sessions.update_status(session.session_id, "waiting_approval")
            return [event]
        if "request input" in lower_prompt:
            request_id = f"input_{turn.turn_id}"
            event = sessions.append_event(
                session.session_id,
                "input.requested",
                {
                    **common,
                    "request_id": request_id,
                    "prompt": "Provide fake provider input.",
                },
            )
            sessions.update_status(session.session_id, "waiting_input")
            return [event]
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

    def resolve_approval(
        self,
        *,
        session: WorkerSession,
        request: dict[str, Any],
        sessions: SessionManager,
    ) -> SessionEvent:
        event = sessions.append_event(session.session_id, "approval.resolved", request)
        if request.get("decision") == "denied":
            sessions.append_event(
                session.session_id,
                "turn.failed",
                {
                    "request_id": request.get("request_id", ""),
                    "error": "approval denied",
                },
            )
            sessions.update_status(session.session_id, "blocked")
        else:
            sessions.update_status(session.session_id, "running")
        return event
