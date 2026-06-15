"""Embedding-based tool relevance + prompt-cache usage (Phase 3 WS8).

The embedding scorer is tested with a deterministic fake embed (no network): it
includes the semantically-near server and excludes the far one, embeds the
utterance once + caches server docs, and falls back to keywords on embed errors.
"""

from __future__ import annotations

import asyncio

from jarvis.brain.gateway_client import _usage_dict
from jarvis.tools.base import Tool
from jarvis.tools.selection import EmbeddingRelevance


def _tool(name: str, cap: str) -> Tool:
    return Tool(name, "d", {"type": "object", "properties": {}}, cap, lambda c, a: "", False)


def _fixture() -> list[Tool]:
    return [
        _tool("web_search", "web.search"),  # built-in, always offered
        _tool("linear_list_issues", "mcp.linear"),
        _tool("granola_get_meeting", "mcp.granola"),
    ]


# A toy embedding: vectors keyed by a word present in the text. "issue"->linear axis,
# "meeting"->granola axis. The utterance lands on whichever word it contains.
def _fake_embed_factory(calls: list):  # noqa: ANN202
    async def embed(texts: list[str]) -> list[list[float]]:
        calls.append(list(texts))
        out = []
        for t in texts:
            low = t.lower()
            out.append([1.0 if "issue" in low or "linear" in low else 0.0,
                        1.0 if "meeting" in low or "granola" in low else 0.0])
        return out

    return embed


def test_embedding_selects_near_server_only() -> None:
    calls: list = []
    rel = EmbeddingRelevance(_fake_embed_factory(calls), threshold=0.5)
    out = asyncio.run(rel.select(_fixture(), "any open issues for me"))
    names = {t.name for t in out}
    assert "web_search" in names  # built-in always
    assert "linear_list_issues" in names  # "issue" → linear axis
    assert "granola_get_meeting" not in names  # unrelated server excluded


def test_embedding_caches_server_docs() -> None:
    calls: list = []
    rel = EmbeddingRelevance(_fake_embed_factory(calls), threshold=0.5)
    asyncio.run(rel.select(_fixture(), "issues"))
    asyncio.run(rel.select(_fixture(), "issues again"))
    # server docs embedded once (first call), then only the utterance each turn
    server_doc_calls = [c for c in calls if any("linear" in x or "granola" in x for x in c)]
    assert len(server_doc_calls) == 1


def test_embedding_falls_back_to_keywords_on_error() -> None:
    async def boom(texts):  # noqa: ANN001, ANN202
        raise RuntimeError("no embeddings route")

    rel = EmbeddingRelevance(boom, threshold=0.5)
    out = asyncio.run(rel.select(_fixture(), "open linear issues"))
    # keyword fallback still resolves linear by name
    assert "linear_list_issues" in {t.name for t in out}


def test_usage_dict_normalises_cached_tokens() -> None:
    class _Details:
        cached_tokens = 512

    class _Usage:
        prompt_tokens = 2000
        completion_tokens = 40
        prompt_tokens_details = _Details()

    d = _usage_dict(_Usage())
    assert d["prompt_tokens"] == 2000
    assert d["cached_tokens"] == 512
    assert _usage_dict(None) == {}
