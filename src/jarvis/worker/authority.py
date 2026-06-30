from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from jarvis.capabilities import (
    FORGE_BRANCH_PUSH,
    FORGE_PR_CREATE,
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_TURN,
)
from jarvis.worker.sessions import WorkerSession


@dataclass(frozen=True)
class WorkerSessionAuthority:
    allowed_actions: list[str]
    landing: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_session(cls, session: WorkerSession, *, provider: str = "") -> WorkerSessionAuthority:
        metadata = session.metadata or {}
        envelope = metadata.get("execution_envelope")
        envelope_data = envelope if isinstance(envelope, dict) else {}
        allowed = _string_list(envelope_data.get("allowed_actions") or metadata.get("allowed_actions"))
        if WORKER_SESSION_TURN not in allowed:
            raise RuntimeError(f"worker session missing required authority: {WORKER_SESSION_TURN}")
        landing = envelope_data.get("landing") or metadata.get("landing") or {}
        authority = cls(allowed_actions=allowed, landing=dict(landing) if isinstance(landing, dict) else {})
        authority.validate(provider or session.provider)
        return authority

    def validate(self, provider: str) -> None:
        mode = self.landing_mode
        if self.landing.get("allow_merge") is True or mode in {"merge", "release"}:
            raise RuntimeError("worker session landing policy cannot merge or release")
        if mode in {"branch_only", "draft_pr", "ready_pr", "confirm_before_pr"} and FORGE_BRANCH_PUSH not in self.allowed:
            raise RuntimeError(f"worker session landing policy {mode!r} requires {FORGE_BRANCH_PUSH}")
        if mode in {"draft_pr", "ready_pr", "confirm_before_pr"} and FORGE_PR_CREATE not in self.allowed:
            raise RuntimeError(f"worker session landing policy {mode!r} requires {FORGE_PR_CREATE}")
        if provider == "claude" and self.codex_sandbox == "read-only":
            raise RuntimeError("claude provider cannot enforce read-only worker sessions")

    @property
    def allowed(self) -> set[str]:
        return set(self.allowed_actions)

    @property
    def landing_mode(self) -> str:
        return str(self.landing.get("mode") or "read_only")

    @property
    def codex_approval_policy(self) -> str:
        if WORKER_SESSION_APPROVE in self.allowed or WORKER_SESSION_INPUT in self.allowed:
            return "on-request"
        return "never"

    @property
    def codex_sandbox(self) -> str:
        if self.landing_mode in {"read_only", "inspect", "review"} or FORGE_BRANCH_PUSH not in self.allowed:
            return "read-only"
        return "workspace-write"

    @property
    def claude_permission_mode(self) -> str:
        if WORKER_SESSION_APPROVE in self.allowed or WORKER_SESSION_INPUT in self.allowed:
            return "default"
        return "dontAsk"

    @property
    def can_resolve_approval(self) -> bool:
        return WORKER_SESSION_APPROVE in self.allowed

    @property
    def can_receive_input(self) -> bool:
        return WORKER_SESSION_INPUT in self.allowed


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item)]
