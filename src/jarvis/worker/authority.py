from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from jarvis.capabilities import WORKER_SESSION_TURN
from jarvis.worker.sessions import WorkerSession


@dataclass(frozen=True)
class WorkerSessionAuthority:
    allowed_actions: list[str]
    landing: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_session(cls, session: WorkerSession) -> WorkerSessionAuthority:
        metadata = session.metadata or {}
        envelope = metadata.get("execution_envelope")
        envelope_data = envelope if isinstance(envelope, dict) else {}
        allowed = _string_list(envelope_data.get("allowed_actions") or metadata.get("allowed_actions"))
        if WORKER_SESSION_TURN not in allowed:
            raise RuntimeError(f"worker session missing required authority: {WORKER_SESSION_TURN}")
        landing = envelope_data.get("landing") or metadata.get("landing") or {}
        return cls(allowed_actions=allowed, landing=dict(landing) if isinstance(landing, dict) else {})

    @property
    def codex_approval_policy(self) -> str:
        return "never"

    @property
    def codex_sandbox(self) -> str:
        return "workspace-write"

    @property
    def claude_permission_mode(self) -> str:
        return "dontAsk"


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item)]
