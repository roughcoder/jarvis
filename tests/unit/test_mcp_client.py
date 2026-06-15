"""MCP client + bridge — live round-trip against a real stdio MCP server.

Isolation-first (the worker pattern): the subsystem is proven standalone here,
against an in-repo FastMCP echo server, before it's wrapped over the tool
boundary. Skips cleanly when the `mcp` SDK isn't installed.
"""

from __future__ import annotations

import asyncio
import pathlib
import sys

import pytest

pytest.importorskip("mcp")

from jarvis.config import MCPConfig, MCPServerSpec  # noqa: E402

_SERVER = str(pathlib.Path(__file__).parent / "mcp_echo_server.py")


def _spec(**kw) -> MCPServerSpec:  # noqa: ANN003
    return MCPServerSpec(name="echo", command=sys.executable, args=[_SERVER], **kw)


def test_client_connects_discovers_and_calls() -> None:
    from jarvis.mcp.client import MCPClient

    async def go():  # noqa: ANN202
        client = MCPClient(_spec(), call_timeout_s=10.0)
        tools = await asyncio.wait_for(client.connect(), 20.0)
        try:
            out = await client.call("echo", {"text": "hi"})
            return {t.name for t in tools}, out
        finally:
            await client.aclose()

    names, out = asyncio.run(go())
    assert {"echo", "add"} <= names
    assert "echo: hi" in out


def test_bridge_namespaces_gates_and_routes() -> None:
    from jarvis.mcp.bridge import MCPBridge

    cfg = MCPConfig(_env_file=None, enabled=True, servers=[_spec()])

    async def go():  # noqa: ANN202
        bridge = MCPBridge(cfg)
        tools = await bridge.start()
        try:
            res = await bridge.call("echo_echo", {"text": "yo"})
            return tools, res
        finally:
            await bridge.aclose()

    tools, res = asyncio.run(go())
    offered = {t.offered_name for t in tools}
    assert {"echo_echo", "echo_add"} <= offered  # namespaced <server>_<tool>
    assert all(t.required_capability == "mcp.echo" for t in tools)  # default cap
    assert "echo: yo" in res


def test_bridge_include_filter_and_disabled_noop() -> None:
    from jarvis.mcp.bridge import MCPBridge

    async def kept(spec: MCPServerSpec):  # noqa: ANN202
        bridge = MCPBridge(MCPConfig(_env_file=None, enabled=True, servers=[spec]))
        tools = await bridge.start()
        await bridge.aclose()
        return {t.server_tool for t in tools}

    # `include` is the per-server firewall: only the named tool survives.
    assert asyncio.run(kept(_spec(include=["add"]))) == {"add"}

    async def disabled():  # noqa: ANN202
        bridge = MCPBridge(MCPConfig(_env_file=None, enabled=False, servers=[_spec()]))
        tools = await bridge.start()
        await bridge.aclose()
        return tools

    assert asyncio.run(disabled()) == []  # disabled => no connection, no tools


def test_bridge_bad_server_is_skipped_not_fatal() -> None:
    from jarvis.mcp.bridge import MCPBridge

    good = _spec()
    bad = MCPServerSpec(name="nope", command="this-binary-does-not-exist", args=[])
    cfg = MCPConfig(
        _env_file=None, enabled=True, servers=[bad, good], connect_timeout_s=10.0
    )

    async def go():  # noqa: ANN202
        bridge = MCPBridge(cfg)
        tools = await bridge.start()
        connected = bridge.connected  # capture before aclose resets state
        await bridge.aclose()
        return connected, {t.server for t in tools}

    connected, servers = asyncio.run(go())
    assert connected == ["echo"]  # the bad server is skipped, the good one connects
    assert servers == {"echo"}
