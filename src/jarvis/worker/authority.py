from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from jarvis.capabilities import (
    FORGE_BRANCH_PUSH,
    FORGE_PR_CREATE,
    WORKER_SESSION_CREATE,
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_TURN,
)
from jarvis.worker.sessions import WorkerSession

REAL_SESSION_PROVIDERS = {"codex", "claude"}


@dataclass(frozen=True)
class WorkerSessionAuthority:
    allowed_actions: list[str]
    landing: dict[str, Any] = field(default_factory=dict)
    trusted_mcp_servers: list[str] = field(default_factory=list)

    @classmethod
    def from_metadata(
        cls,
        metadata: dict[str, Any],
        *,
        provider: str = "",
        require_turn: bool = False,
    ) -> WorkerSessionAuthority:
        envelope = metadata.get("execution_envelope")
        if provider in REAL_SESSION_PROVIDERS and not isinstance(envelope, dict):
            raise RuntimeError(f"worker session execution_envelope is required for {provider} provider")
        envelope_data = envelope if isinstance(envelope, dict) else None
        if envelope_data is not None:
            allowed = _string_list(envelope_data.get("allowed_actions"))
            landing_source = envelope_data.get("landing")
        else:
            allowed = _string_list(metadata.get("allowed_actions"))
            landing_source = metadata.get("landing")
        if require_turn and WORKER_SESSION_TURN not in allowed:
            raise RuntimeError(f"worker session missing required authority: {WORKER_SESSION_TURN}")
        landing = landing_source or {}
        authority = cls(
            allowed_actions=allowed,
            landing=dict(landing) if isinstance(landing, dict) else {},
            trusted_mcp_servers=_string_list(metadata.get("trusted_mcp_servers")),
        )
        authority.validate(provider)
        return authority

    @classmethod
    def from_session(cls, session: WorkerSession, *, provider: str = "") -> WorkerSessionAuthority:
        return cls.from_metadata(session.metadata or {}, provider=provider or session.provider, require_turn=True)

    @classmethod
    def for_session_create(cls, data: dict[str, Any]) -> WorkerSessionAuthority:
        metadata = dict(data.get("metadata") or {})
        authority = cls.from_metadata(metadata, provider=str(data.get("provider") or data.get("engine") or ""))
        authority.require(WORKER_SESSION_CREATE)
        return authority

    def validate(self, provider: str) -> None:
        mode = self.landing_mode
        if self.landing.get("allow_merge") is True or mode in {"merge", "release"}:
            raise RuntimeError("worker session landing policy cannot merge or release")
        if mode in {"branch_only", "draft_pr", "ready_pr", "confirm_before_pr"} and FORGE_BRANCH_PUSH not in self.allowed:
            raise RuntimeError(f"worker session landing policy {mode!r} requires {FORGE_BRANCH_PUSH}")
        if mode in {"draft_pr", "ready_pr", "confirm_before_pr"} and FORGE_PR_CREATE not in self.allowed:
            raise RuntimeError(f"worker session landing policy {mode!r} requires {FORGE_PR_CREATE}")

    def require(self, action: str) -> None:
        if action not in self.allowed:
            raise RuntimeError(f"worker session missing required authority: {action}")

    @property
    def allowed(self) -> set[str]:
        return set(self.allowed_actions)

    @property
    def landing_mode(self) -> str:
        return str(self.landing.get("mode") or "read_only")

    @property
    def codex_approval_policy(self) -> str:
        if WORKER_SESSION_APPROVE in self.allowed:
            return "on-request"
        return "never"

    @property
    def codex_sandbox(self) -> str:
        if self.landing_mode in {"read_only", "inspect", "review"} or FORGE_BRANCH_PUSH not in self.allowed:
            return "read-only"
        return "workspace-write"

    @property
    def codex_turn_sandbox_policy(self) -> dict[str, Any] | None:
        """Keep review sessions read-only without cutting them off from remote sources.

        Codex's legacy ``sandbox: "read-only"`` thread setting also defaults
        network access to false. Review agents still need read-only access to
        GitHub (for example, ``gh pr view`` and ``gh pr diff``), so override the
        turn with the structured policy supported by the app-server protocol.

        Workspace-write sessions retain their existing thread policy here. A
        structured workspace-write policy also needs an explicit writable-root
        contract, which is separate from this read-only review correction.
        """
        if self.codex_sandbox == "read-only":
            return {"type": "readOnly", "networkAccess": True}
        return None

    @property
    def claude_permission_mode(self) -> str:
        if self.codex_sandbox == "read-only":
            return "plan"
        if self.can_resolve_approval:
            return "default"
        return "dontAsk"

    def claude_tool_denial(self, tool_name: str) -> str:
        name = str(tool_name or "").strip()
        if self.codex_sandbox == "read-only" and not _claude_tool_is_read_only(
            name,
            trusted_mcp_servers=self.trusted_mcp_servers,
        ):
            return f"worker session is read-only; refusing Claude tool {name or '<unknown>'}"
        if self.codex_sandbox != "read-only" and not self.can_resolve_approval:
            return f"worker session lacks {WORKER_SESSION_APPROVE}; refusing Claude tool {name or '<unknown>'}"
        return ""

    def claude_tool_is_preapproved(self, tool_name: str) -> bool:
        return self.codex_sandbox == "read-only" and _claude_tool_is_read_only(
            str(tool_name or ""),
            trusted_mcp_servers=self.trusted_mcp_servers,
        )

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


def _claude_tool_is_read_only(tool_name: str, *, trusted_mcp_servers: list[str] | None = None) -> bool:
    name = tool_name.strip()
    if not name:
        return False
    if name.startswith("mcp__"):
        return any(name.startswith(f"mcp__{server}__") for server in trusted_mcp_servers or [])
    return name in {"AskUserQuestion", "Glob", "Grep", "Read", "WebFetch", "WebSearch"}
