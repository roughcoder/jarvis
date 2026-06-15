"""MCP tools — the brain's gated view of bridged MCP servers (Phase 3 §6).

The thin layer over `jarvis.mcp.MCPBridge`, exactly analogous to `worker.py` over
the worker daemon: it turns each discovered MCP tool into a capability-gated
`Tool` whose handler routes back through the bridge. Registration is not a grant —
each tool carries the server's `mcp.<server>` capability, so a device only sees a
server's tools if its profile grants them (the firewall against tool sprawl). The
registry hard-bounds every call with `tools.timeout_s`; the bridge bounds it again
with `mcp.call_timeout_s`. Nothing here imports the MCP SDK — the bridge owns it.
"""

from __future__ import annotations

from typing import Any

from jarvis.brain.context import RequestContext
from jarvis.mcp.bridge import MCPBridge
from jarvis.tools.base import Tool


def make_mcp_tools(bridge: MCPBridge) -> list[Tool]:
    """Build a gated `Tool` for every tool the bridge has already discovered. Call
    after `bridge.start()`; an unstarted/empty bridge yields no tools."""

    def make_handler(offered_name: str):  # noqa: ANN202 - closure per tool
        async def handler(ctx: RequestContext, args: dict[str, Any]) -> str:
            try:
                # Route under the SPEAKER's credentials (the privacy wall, §5): an
                # OAuth server runs against this principal's own token, never another's.
                return await bridge.call(offered_name, args, user=ctx.memory_peer)
            except TimeoutError:
                return f"error: {offered_name} timed out"
            except Exception as exc:  # noqa: BLE001 - a tool error must not break the turn
                return f"error: {exc}"

        return handler

    tools: list[Tool] = []
    for bt in bridge.tools:
        desc = f"[{bt.server}] {bt.description}".strip() if bt.description else f"[{bt.server}] {bt.server_tool}"
        tools.append(
            Tool(
                bt.offered_name,
                desc,
                _safe_schema(bt.input_schema),
                bt.required_capability,
                make_handler(bt.offered_name),
                announce=True,  # bridged calls are remote/slow → pulse while they run
            )
        )
    return tools


def _safe_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """A JSON-Schema object the gateway will accept as function parameters. MCP
    servers should return an object schema; coerce anything odd into a minimal one."""
    if isinstance(schema, dict) and schema.get("type") == "object":
        return schema
    return {"type": "object", "properties": {}}
