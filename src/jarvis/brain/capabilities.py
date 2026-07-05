"""Capability gate — deny-by-default enforcement (Phase 3, review HIGH #1).

The wall that must exist *before* any tool that touches an account or the
filesystem. A request carries a set of granted capabilities (resolved from its
device profile); `require()` is called before any gated action and raises unless
the capability was explicitly granted. Nothing is allowed implicitly.

Capabilities are resolved from `profiles/<device>.md` front-matter when present,
else from a configured CSV default, else **empty** (everything denied).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from jarvis.config import CapabilityConfig
from jarvis.frontmatter import parse_front_matter
from jarvis.runtime import CapabilityError, RequestContext, require
from jarvis.users import User

if TYPE_CHECKING:
    from jarvis.brain.registry import RegistryStore

__all__ = [
    "CapabilityError",
    "RequestContext",
    "build_request_context",
    "context_for_resolution",
    "MemoryAccessDecision",
    "parse_profile_capabilities",
    "can_query_memory_peer",
    "can_write_memory_peer",
    "require",
    "resolve_capabilities",
]


@dataclass(frozen=True)
class MemoryAccessDecision:
    allowed: bool
    reason: str


def parse_profile_capabilities(text: str) -> set[str]:
    """Extract the capability set from a profile markdown's YAML front-matter.

    Supports inline (`capabilities: [a, b]`) and block (`capabilities:\n  - a`)
    forms. No front-matter / no capabilities key → empty set (deny-by-default).
    """
    value = parse_front_matter(text).get("capabilities")
    if isinstance(value, list):
        return {str(cap).strip() for cap in value if str(cap).strip()}
    if value:
        cap = str(value).strip()
        return {cap} if cap else set()
    return set()


# --- resolution ------------------------------------------------------------


def resolve_capabilities(cfg: CapabilityConfig) -> set[str]:
    """Capabilities for this device: profile file if present, else CSV default."""
    path = Path(cfg.profiles_dir) / f"{cfg.device_id}.md"
    if path.exists():
        return parse_profile_capabilities(path.read_text(encoding="utf-8"))
    return {c.strip() for c in cfg.default_capabilities.split(",") if c.strip()}


def build_request_context(cfg: CapabilityConfig) -> RequestContext:
    """Single-principal RequestContext from config (Phase 3a / single-process loop).
    The brain server uses `context_for_resolution` to build one per utterance from
    the resolved speaker instead (Phase 3d)."""
    return RequestContext(
        device_id=cfg.device_id,
        identity=cfg.identity,
        scope=cfg.scope,
        capabilities=frozenset(resolve_capabilities(cfg)),
    )


def context_for_resolution(cfg: CapabilityConfig, resolution) -> RequestContext:  # noqa: ANN001
    """Per-utterance RequestContext (Phase 3d): the device profile is the ceiling
    of what's allowed *here*; an identified user's own grants are added on top when
    in personal scope (their MCP servers etc.). Identity/scope/peer come from the
    resolution — that's what routes credentials + memory to the right principal.

    `resolution` is a `jarvis.brain.identity.Resolution` (kept duck-typed to avoid a
    circular import)."""
    caps = set(resolve_capabilities(cfg))
    user = getattr(resolution, "user", None)
    if user is not None and resolution.scope == "personal":
        caps |= set(user.capabilities)
    return RequestContext(
        device_id=cfg.device_id,
        identity=resolution.identity,
        scope=resolution.scope,
        capabilities=frozenset(caps),
        confidence=resolution.confidence,
        peer=getattr(user, "peer", "") if user is not None else "",
    )


# --- memory access matrix --------------------------------------------------


def can_query_memory_peer(
    ctx: RequestContext,
    peer_id: str,
    *,
    registry: "RegistryStore | None" = None,
    users: dict[str, User] | None = None,
    target: str | None = None,
) -> MemoryAccessDecision:
    """Shared read matrix for memory tools and API routes.

    Deny-by-default. The requester may read their own peer, target views they
    own, visible contacts, member projects, and guardian->minor principal peers.
    """
    requester = _requester_peer(ctx)
    peer = (peer_id or "").strip()
    if not requester or not peer:
        return MemoryAccessDecision(False, "missing requester or peer")
    if target and target != requester:
        return MemoryAccessDecision(False, "target view is not owned by requester")
    if peer == requester:
        return MemoryAccessDecision(True, "own peer")
    if peer.startswith("contact:"):
        contact_id = peer.split(":", 1)[1]
        if registry is not None and registry.get_visible_contact(contact_id, ctx.identity):
            return MemoryAccessDecision(True, "visible contact")
        return MemoryAccessDecision(False, "contact is not visible to requester")
    if peer.startswith("project:"):
        project_id = peer.split(":", 1)[1]
        if registry is not None and registry.get_visible_project(project_id, ctx.identity):
            return MemoryAccessDecision(True, "project member")
        return MemoryAccessDecision(False, "project is not visible to requester")
    if _guardian_can_read(ctx, peer, users or {}):
        return MemoryAccessDecision(True, "guardian read of minor peer")
    return MemoryAccessDecision(False, "memory access denied")


def can_write_memory_peer(
    ctx: RequestContext,
    peer_id: str,
    *,
    registry: "RegistryStore | None" = None,
) -> MemoryAccessDecision:
    """Shared curation write matrix.

    Guardian access is intentionally absent: the guardian rule is read-only.
    """
    requester = _requester_peer(ctx)
    peer = (peer_id or "").strip()
    if not requester or not peer:
        return MemoryAccessDecision(False, "missing requester or peer")
    if peer == requester:
        return MemoryAccessDecision(True, "own peer")
    if peer.startswith("contact:"):
        contact_id = peer.split(":", 1)[1]
        if registry is not None and registry.get_visible_contact(contact_id, ctx.identity):
            return MemoryAccessDecision(True, "visible contact")
        return MemoryAccessDecision(False, "contact is not writable by requester")
    if peer.startswith("project:"):
        project_id = peer.split(":", 1)[1]
        if registry is not None and registry.get_visible_project(project_id, ctx.identity):
            return MemoryAccessDecision(True, "project member")
        return MemoryAccessDecision(False, "project is not writable by requester")
    return MemoryAccessDecision(False, "memory write denied")


def _requester_peer(ctx: RequestContext) -> str:
    if ctx.scope != "personal" or not ctx.identity or ctx.identity == "house":
        return ""
    return ctx.memory_peer


def _guardian_can_read(ctx: RequestContext, peer_id: str, users: dict[str, User]) -> bool:
    requester = users.get(ctx.identity)
    if requester is None:
        return False
    requester_tier = requester.trust_tier.strip().lower()
    if requester_tier not in {"guardian", "adult", "parent"}:
        return False
    for user in users.values():
        if user.peer != peer_id:
            continue
        if user.trust_tier.strip().lower() != "minor":
            return False
        return ctx.identity in user.guardians
    return False
