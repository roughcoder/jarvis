"""Capability gate — deny-by-default enforcement (Phase 3, review HIGH #1).

The wall that must exist *before* any tool that touches an account or the
filesystem. A request carries a set of granted capabilities (resolved from its
device profile); `require()` is called before any gated action and raises unless
the capability was explicitly granted. Nothing is allowed implicitly.

Capabilities are resolved from `profiles/<device>.md` front-matter when present,
else from a configured CSV default, else **empty** (everything denied).
"""

from __future__ import annotations

import re
from pathlib import Path

from jarvis.config import CapabilityConfig
from jarvis.runtime import CapabilityError, RequestContext, require

__all__ = [
    "CapabilityError",
    "RequestContext",
    "build_request_context",
    "context_for_resolution",
    "parse_profile_capabilities",
    "require",
    "resolve_capabilities",
]


# --- profile parsing -------------------------------------------------------

_FRONT_MATTER = re.compile(r"^\s*---\s*\n(.*?)\n---\s*(?:\n|$)", re.DOTALL)
_INLINE_LIST = re.compile(r"^capabilities:\s*\[(.*?)\]", re.MULTILINE)
_BLOCK_KEY = re.compile(r"^capabilities:\s*$")
_BLOCK_ITEM = re.compile(r"^\s*-\s*(.+?)\s*$")


def parse_profile_capabilities(text: str) -> set[str]:
    """Extract the capability set from a profile markdown's YAML front-matter.

    Supports inline (`capabilities: [a, b]`) and block (`capabilities:\n  - a`)
    forms. No front-matter / no capabilities key → empty set (deny-by-default).
    """
    m = _FRONT_MATTER.match(text)
    if not m:
        return set()
    fm = m.group(1)

    inline = _INLINE_LIST.search(fm)
    if inline:
        return {c.strip().strip("'\"") for c in inline.group(1).split(",") if c.strip()}

    caps: set[str] = set()
    collecting = False
    for line in fm.splitlines():
        if _BLOCK_KEY.match(line):
            collecting = True
            continue
        if collecting:
            item = _BLOCK_ITEM.match(line)
            if item:
                caps.add(item.group(1).strip().strip("'\""))
            elif line.strip() and not line[0].isspace():
                break  # reached the next top-level key
    return caps


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
