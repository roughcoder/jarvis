"""Capability gate — deny-by-default enforcement (Phase 3, review HIGH #1).

The wall that must exist *before* any tool that touches an account or the
filesystem. A request carries a set of granted capabilities (resolved from its
device profile); `require()` is called before any gated action and raises unless
the capability was explicitly granted. Nothing is allowed implicitly.

Capabilities are resolved from `profiles/<device>.md` front-matter when present,
else from a configured CSV default, else **empty** (everything denied).
"""

from __future__ import annotations

import pathlib
import re

from jarvis.brain.context import RequestContext
from jarvis.config import CapabilityConfig


class CapabilityError(PermissionError):
    """Raised when a request lacks a required capability (deny-by-default)."""


def require(ctx: RequestContext, capability: str) -> None:
    """Gate a capability-bearing action. Raises CapabilityError if not granted."""
    if capability not in ctx.capabilities:
        raise CapabilityError(
            f"capability {capability!r} not granted "
            f"(identity={ctx.identity!r}, device={ctx.device_id!r})"
        )


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
    path = pathlib.Path(cfg.profiles_dir) / f"{cfg.device_id}.md"
    if path.exists():
        return parse_profile_capabilities(path.read_text(encoding="utf-8"))
    return {c.strip() for c in cfg.default_capabilities.split(",") if c.strip()}


def build_request_context(cfg: CapabilityConfig) -> RequestContext:
    """Single-principal RequestContext from config (Phase 3a). In W4 the brain
    server builds one per connection from the paired device's profile instead."""
    return RequestContext(
        device_id=cfg.device_id,
        identity=cfg.identity,
        scope=cfg.scope,
        capabilities=frozenset(resolve_capabilities(cfg)),
    )
