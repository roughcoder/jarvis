from __future__ import annotations

from typing import Any

from jarvis.config import WorkerConfig
from jarvis.worker.providers.base import ProviderTurn
from jarvis.worker.sessions import SessionEvent, SessionManager, WorkerSession
from jarvis.worker_session_contract import (
    CHECKPOINT_ID_KEY,
    EVENT_APPROVAL_REQUESTED,
    EVENT_APPROVAL_RESOLVED,
    EVENT_ASSISTANT_DELTA,
    EVENT_ASSISTANT_MESSAGE,
    EVENT_CHECKPOINT_CREATED,
    EVENT_INPUT_RECEIVED,
    EVENT_INPUT_REQUESTED,
    EVENT_TURN_COMPLETED,
    EVENT_TURN_FAILED,
    SESSION_BLOCKED,
    SESSION_COMPLETED,
    SESSION_RUNNING,
    SESSION_WAITING_APPROVAL,
    SESSION_WAITING_INPUT,
)


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
        sessions.update_status(session.session_id, SESSION_RUNNING)
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
                EVENT_APPROVAL_REQUESTED,
                {
                    **common,
                    "request_id": request_id,
                    "action": "fake.tool.execute",
                    "prompt": "Approve fake provider action?",
                },
            )
            sessions.update_status(session.session_id, SESSION_WAITING_APPROVAL)
            return [event]
        if "request input" in lower_prompt:
            request_id = f"input_{turn.turn_id}"
            event = sessions.append_event(
                session.session_id,
                EVENT_INPUT_REQUESTED,
                {
                    **common,
                    "request_id": request_id,
                    "prompt": "Provide fake provider input.",
                },
            )
            sessions.update_status(session.session_id, SESSION_WAITING_INPUT)
            return [event]
        events = [
            sessions.append_event(
                session.session_id,
                EVENT_ASSISTANT_DELTA,
                {**common, "text": "Fake provider accepted the turn."},
            ),
            sessions.append_event(
                session.session_id,
                EVENT_ASSISTANT_MESSAGE,
                {**common, "text": f"Fake provider completed: {turn.prompt}".strip()},
            ),
            sessions.append_event(
                session.session_id,
                EVENT_CHECKPOINT_CREATED,
                {**common, CHECKPOINT_ID_KEY: f"ckpt_{turn.turn_id}", "label": "fake checkpoint"},
            ),
            sessions.append_event(session.session_id, EVENT_TURN_COMPLETED, common),
        ]
        sessions.update_status(session.session_id, SESSION_COMPLETED)
        return events

    def resolve_approval(
        self,
        *,
        session: WorkerSession,
        request: dict[str, Any],
        sessions: SessionManager,
    ) -> SessionEvent:
        event = sessions.append_event(session.session_id, EVENT_APPROVAL_RESOLVED, request)
        if request.get("decision") == "denied":
            sessions.append_event(
                session.session_id,
                EVENT_TURN_FAILED,
                {
                    "request_id": request.get("request_id", ""),
                    "error": "approval denied",
                },
            )
            sessions.update_status(session.session_id, SESSION_BLOCKED)
        else:
            sessions.update_status(session.session_id, SESSION_RUNNING)
        return event

    def receive_input(
        self,
        *,
        session: WorkerSession,
        request: dict[str, Any],
        sessions: SessionManager,
    ) -> SessionEvent:
        event = sessions.append_event(session.session_id, EVENT_INPUT_RECEIVED, request)
        sessions.update_status(session.session_id, SESSION_COMPLETED)
        return event
