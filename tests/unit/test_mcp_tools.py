"""MCP tool layer — gating, registry wiring, and call routing (Phase 3 §6).

No MCP SDK needed: `make_mcp_tools` only reads `bridge.tools` and calls
`bridge.call`, so a duck-typed fake bridge exercises the whole tool layer. The
live SDK round-trip lives in test_mcp_client.py.
"""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.config import ToolsConfig
from jarvis.mcp.bridge import BridgedTool
from jarvis.tools import build_registry
from jarvis.tools.mcp import make_mcp_tools


class _FakeBridge:
    """Stands in for a connected MCPBridge: a tool list + a recording call()."""

    def __init__(self, tools: list[BridgedTool], *, fail: Exception | None = None) -> None:
        self.tools = tools
        self._fail = fail
        self.calls: list[tuple[str, dict]] = []

    async def call(self, offered_name: str, args: dict, *, user: str = "house") -> str:
        self.calls.append((offered_name, args))
        self.last_user = user
        if self._fail is not None:
            raise self._fail
        return f"ok:{offered_name}:{args}"


def _bt(server: str, tool: str, cap: str | None = None) -> BridgedTool:
    return BridgedTool(
        offered_name=f"{server}_{tool}",
        server=server,
        server_tool=tool,
        description=f"do {tool}",
        input_schema={"type": "object", "properties": {"x": {"type": "string"}}},
        required_capability=cap or f"mcp.{server}",
    )


def _ctx(*caps: str) -> RequestContext:
    return RequestContext("neil-mac", "neil", "personal", frozenset(caps))


def test_tools_built_with_capability_and_schema() -> None:
    bridge = _FakeBridge([_bt("gh", "list_prs")])
    (tool,) = make_mcp_tools(bridge)
    assert tool.name == "gh_list_prs"
    assert tool.required_capability == "mcp.gh"
    assert tool.announce is True  # bridged calls pulse while they run
    assert tool.parameters["type"] == "object"
    assert tool.description.startswith("[gh]")


def test_registry_offers_mcp_tools_only_when_capability_granted() -> None:
    mcp_tools = make_mcp_tools(_FakeBridge([_bt("gh", "list_prs"), _bt("granola", "search")]))
    reg = build_registry(ToolsConfig(_env_file=None), mcp=mcp_tools)

    # deny-by-default: no mcp caps => no mcp tools offered
    assert not {t.name for t in reg.available_for(_ctx())} & {"gh_list_prs", "granola_search"}
    # granting one server's capability reveals exactly that server's tools
    offered = {t.name for t in reg.available_for(_ctx("mcp.gh"))}
    assert "gh_list_prs" in offered
    assert "granola_search" not in offered  # different server, different capability


def test_handler_routes_through_bridge() -> None:
    bridge = _FakeBridge([_bt("gh", "list_prs")])
    (tool,) = make_mcp_tools(bridge)
    out = asyncio.run(tool.handler(_ctx("mcp.gh"), {"x": "1"}))
    assert out == "ok:gh_list_prs:{'x': '1'}"
    assert bridge.calls == [("gh_list_prs", {"x": "1"})]
    assert bridge.last_user == "neil"  # routed under the speaker's credentials


def test_handler_formats_errors_and_timeouts() -> None:
    err = make_mcp_tools(_FakeBridge([_bt("gh", "boom")], fail=RuntimeError("kaboom")))[0]
    assert asyncio.run(err.handler(_ctx("mcp.gh"), {})) == "error: kaboom"

    timed = make_mcp_tools(_FakeBridge([_bt("gh", "slow")], fail=TimeoutError()))[0]
    assert asyncio.run(timed.handler(_ctx("mcp.gh"), {})) == "error: gh_slow timed out"
