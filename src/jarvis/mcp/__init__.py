"""MCP bridge package (Phase 3 §6) — the native MCP client.

Isolation-first, like the worker daemon: this package imports nothing from the
brain. It owns the MCP protocol (handshake, stdio/HTTP transports, tool
discovery + invocation); the brain reaches it only through `tools/mcp.py` (a thin
client over the registry boundary). The MCP SDK is an optional dependency
(`uv sync --extra mcp`); imports stay lazy so a brain with no servers configured
never needs it.
"""

from __future__ import annotations

from jarvis.mcp.bridge import BridgedTool, MCPBridge
from jarvis.mcp.client import MCPClient, MCPToolSpec

__all__ = ["BridgedTool", "MCPBridge", "MCPClient", "MCPToolSpec"]
