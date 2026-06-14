"""Integration: live web_search tool (needs TOOLS_WEBSEARCH_API_KEY).

Exercises the real provider end to end. Skips cleanly when unconfigured.
"""

from __future__ import annotations

import asyncio

import pytest

from jarvis.brain.context import RequestContext
from jarvis.config import load_config
from jarvis.tools.web_search import make_web_search_tool

pytestmark = pytest.mark.integration


def test_web_search_returns_results() -> None:
    cfg = load_config().tools
    if not cfg.websearch_api_key.get_secret_value():
        pytest.skip("TOOLS_WEBSEARCH_API_KEY not set")
    tool = make_web_search_tool(cfg)
    ctx = RequestContext("dev", "house", "house", frozenset({"web.search"}))
    out = asyncio.run(tool.handler(ctx, {"query": "current weather in Lisbon"}))
    assert isinstance(out, str) and out.strip()
    assert out != "No results."
