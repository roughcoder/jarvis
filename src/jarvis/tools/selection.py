"""Per-turn tool relevance prefilter (Phase 3 §9 — keep the voice prompt lean).

All tools stay registered + capability-gated; this only narrows what's OFFERED on
a given turn so the model isn't handed 100+ schemas every utterance (which taxes
TTFT and muddies selection). Built-in tools (web_search / files / worker — few)
are always offered. MCP tools are grouped by their `mcp.<server>` capability and a
server is included only when the utterance looks relevant to it: its name, any
configured `keywords`, or distinctive words derived from its tool names.

A miss just means that server isn't offered THAT turn — naming the app or using a
domain word ("issue", "meeting", "vault") brings it in. Deterministic and adds no
round-trip; an embedding-similarity scorer can later replace `_relevant` behind the
same interface.
"""

from __future__ import annotations

import math
import re
from collections.abc import Awaitable, Callable

from jarvis.tools.base import Tool

_MCP = "mcp."

# Words too generic to signal a server: CRUD verbs + filler that show up in many
# tool names. Kept out of the auto-derived keyword sets so a server only matches on
# its distinctive nouns ("issues", "meeting", "vault"), not "get"/"list"/"the".
_GENERIC = frozenset({
    "get", "list", "create", "update", "delete", "read", "write", "save", "add",
    "remove", "find", "search", "query", "fetch", "set", "make", "new", "all",
    "the", "and", "for", "with", "from", "your", "you", "this", "that", "what",
    "tool", "tools", "mcp", "info", "data", "item", "items", "name", "id",
    # common verbs/filler that aren't server signals (kept out to cut false hits
    # like "open" -> obsidian's backlog_open). The real fix is embedding similarity.
    "open", "close", "show", "tell", "give", "want", "need", "please", "help",
    "about", "into", "out", "view", "move", "toggle", "prepare",
})


def _tokens(text: str) -> set[str]:
    return {w for w in re.findall(r"[a-z0-9]+", (text or "").lower()) if len(w) >= 3} - _GENERIC


def server_keywords(server: str, tools: list[Tool], extra: set[str] | None = None) -> set[str]:
    """Trigger words for one MCP server: its name + configured extras + distinctive
    tokens from its tool names (descriptions are skipped — too noisy, they match
    everything)."""
    kw = {server.lower()} | _tokens(server)
    if extra:
        kw |= {e.lower() for e in extra}
    for t in tools:
        name = t.name.split("_", 1)[1] if "_" in t.name else t.name  # drop "<server>_" prefix
        kw |= _tokens(name.replace("_", " ").replace("-", " "))
    return kw - _GENERIC


def select_tools(
    tools: list[Tool],
    user_text: str,
    *,
    enabled: bool = True,
    extra_keywords: dict[str, set[str]] | None = None,
) -> list[Tool]:
    """Narrow `tools` (already capability-filtered) to those worth offering for
    `user_text`. Non-MCP tools always pass; MCP tools pass only if their server is
    relevant. Returns everything unchanged when disabled, when there's no utterance,
    or when there are no MCP tools to narrow."""
    if not enabled or not user_text:
        return tools
    mcp = [t for t in tools if t.required_capability.startswith(_MCP)]
    if not mcp:
        return tools
    builtins = [t for t in tools if not t.required_capability.startswith(_MCP)]

    by_server: dict[str, list[Tool]] = {}
    for t in mcp:
        by_server.setdefault(t.required_capability[len(_MCP):], []).append(t)

    utter = _tokens(user_text)
    chosen = list(builtins)
    for server, stools in by_server.items():
        extra = (extra_keywords or {}).get(server)
        if utter & server_keywords(server, stools, extra):
            chosen.extend(stools)
    return chosen


def _cosine(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    return dot / (na * nb) if na and nb else 0.0


class EmbeddingRelevance:
    """Semantic alternative to the keyword prefilter (WS8): score the utterance
    against each MCP server's tool-derived description by embedding cosine, and
    include servers above a threshold. Built-ins always pass. Server-doc embeddings
    are computed once and cached; only the utterance is embedded per turn. Any
    failure (no embeddings route, network error) falls back to the keyword matcher,
    so a turn is never blocked. `embed` is an async `list[str] -> list[vector]`."""

    def __init__(
        self,
        embed: Callable[[list[str]], Awaitable[list[list[float]]]],
        *,
        threshold: float,
        extra_keywords: dict[str, set[str]] | None = None,
    ) -> None:
        self._embed = embed
        self._threshold = threshold
        self._extra = extra_keywords or {}
        self._server_vecs: dict[str, list[float]] = {}

    def _server_doc(self, server: str, tools: list[Tool]) -> str:
        kw = server_keywords(server, tools, self._extra.get(server))
        return f"{server}: " + " ".join(sorted(kw))

    async def _ensure_server_vecs(self, by_server: dict[str, list[Tool]]) -> None:
        missing = [s for s in by_server if s not in self._server_vecs]
        if not missing:
            return
        docs = [self._server_doc(s, by_server[s]) for s in missing]
        vecs = await self._embed(docs)
        for s, v in zip(missing, vecs):
            self._server_vecs[s] = v

    async def select(self, tools: list[Tool], user_text: str) -> list[Tool]:
        if not user_text:
            return tools
        mcp = [t for t in tools if t.required_capability.startswith(_MCP)]
        if not mcp:
            return tools
        builtins = [t for t in tools if not t.required_capability.startswith(_MCP)]
        by_server: dict[str, list[Tool]] = {}
        for t in mcp:
            by_server.setdefault(t.required_capability[len(_MCP):], []).append(t)
        try:
            await self._ensure_server_vecs(by_server)
            uvec = (await self._embed([user_text]))[0]
        except Exception:  # noqa: BLE001 - never block a turn; fall back to keywords
            return select_tools(tools, user_text, extra_keywords=self._extra)
        chosen = list(builtins)
        for server, stools in by_server.items():
            if _cosine(uvec, self._server_vecs.get(server, [])) >= self._threshold:
                chosen.extend(stools)
        return chosen


def build_relevance(cfg, gateway):  # noqa: ANN001, ANN201 - cfg=Config, gateway=GatewayClient
    """An EmbeddingRelevance when TOOLS_RELEVANCE_MODE=embedding, else None (keyword
    prefilter). Shared across all per-context sessions (server docs are global)."""
    if cfg.tools.relevance_mode != "embedding":
        return None
    extra = {s.name: set(s.keywords) for s in cfg.mcp.servers if s.keywords}
    return EmbeddingRelevance(
        gateway.embed, threshold=cfg.tools.relevance_threshold, extra_keywords=extra
    )


def offered_servers(tools: list[Tool]) -> list[str]:
    """The distinct MCP servers represented in a tool list (for debug logging)."""
    seen = []
    for t in tools:
        if t.required_capability.startswith(_MCP):
            s = t.required_capability[len(_MCP):]
            if s not in seen:
                seen.append(s)
    return seen
