"""Jarvis MCP server lane.

This package exposes Jarvis brain powers as an MCP server while keeping the
same boundary-peer rules as the Cockpit API: authenticated principal in,
capability-gated brain/tool calls out.
"""

from __future__ import annotations

from jarvis.mcp_server.adapters import JarvisMCPService
from jarvis.mcp_server.tokens import MCPTokenRecord, MCPTokenStore

__all__ = ["JarvisMCPService", "MCPTokenRecord", "MCPTokenStore"]
