"""web_search tool — current information from the web (capability `web.search`).

Provider-configurable (default Tavily, an LLM-agent-oriented search API). The
handler is async with a hard timeout; the provider key lives brain-side only
(never on an intercom). `_format_tavily` is pure and unit-tested.
"""

from __future__ import annotations

from typing import Any

import httpx

from jarvis.brain.context import RequestContext
from jarvis.config import ToolsConfig
from jarvis.tools.base import Tool, ToolError

CAPABILITY = "web.search"


def make_web_search_tool(cfg: ToolsConfig) -> Tool:
    async def handler(ctx: RequestContext, args: dict[str, Any]) -> str:
        query = (args.get("query") or "").strip()
        if not query:
            return "error: empty query"
        key = cfg.websearch_api_key.get_secret_value()
        if not key:
            return "error: web search is not configured (no API key)"
        provider = cfg.websearch_provider.lower()
        if provider == "tavily":
            return await _tavily(query, key, cfg.websearch_max_results, cfg.timeout_s)
        raise ToolError(f"unsupported web search provider {provider!r}")

    return Tool(
        name="web_search",
        description=(
            "Search the web for current or factual information — news, weather, "
            "prices, events, or anything you might not know or that may have changed."
        ),
        parameters={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The search query."}
            },
            "required": ["query"],
        },
        required_capability=CAPABILITY,
        handler=handler,
    )


async def _tavily(query: str, key: str, max_results: int, timeout_s: float) -> str:
    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.post(
            "https://api.tavily.com/search",
            json={
                "api_key": key,
                "query": query,
                "max_results": max_results,
                "include_answer": True,
            },
        )
        r.raise_for_status()
        return _format_tavily(r.json())


def _format_tavily(data: dict[str, Any]) -> str:
    """Condense a Tavily response into compact text for the model."""
    parts: list[str] = []
    if data.get("answer"):
        parts.append(f"Answer: {data['answer']}")
    for item in (data.get("results") or [])[:8]:
        title = item.get("title", "")
        url = item.get("url", "")
        content = (item.get("content") or "").strip().replace("\n", " ")
        if len(content) > 300:
            content = content[:300] + "…"
        parts.append(f"- {title} ({url}): {content}")
    return "\n".join(parts) if parts else "No results."
