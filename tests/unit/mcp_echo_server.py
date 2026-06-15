"""A minimal stdio MCP server used only by the MCP client/bridge tests.

Run as a subprocess by `stdio_client` (command = python, args = [this file]); it
exposes two trivial tools so the tests can prove discovery + invocation over a
real MCP session — true isolation, no network, no external server. Not a test
module itself (no `test_` prefix → pytest won't collect it).
"""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

server = FastMCP("echo-test")


@server.tool()
def echo(text: str) -> str:
    """Echo the text back."""
    return f"echo: {text}"


@server.tool()
def add(a: int, b: int) -> int:
    """Add two integers."""
    return a + b


if __name__ == "__main__":
    server.run()  # stdio transport by default
