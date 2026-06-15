"""MCPClient — a persistent connection to ONE MCP server (Phase 3 §6).

Owns the MCP handshake and one live session over either transport: `stdio` (a
long-lived subprocess — the common local case, e.g. an `npx` server) or `http`
(streamable HTTP). It connects once, discovers the server's tools, and calls them
on the live session on demand.

The session lives inside a dedicated **runner task** that holds the SDK's
`async with` blocks open for the connection's whole lifetime. This is deliberate:
the SDK's transports use anyio cancel scopes that MUST be entered and exited on
the same task, and several servers' scopes would otherwise interleave on one task
and fail to close independently. Teardown is simply cancelling the runner task,
so enter/exit always happen in the same task. Tool *calls* come from any turn
task — the SDK's streams tolerate that; only scope enter/exit is task-bound.

Imports of the `mcp` SDK are lazy so the package costs nothing until a server is
actually configured.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from dataclasses import dataclass
from typing import Any

from jarvis.config import MCPServerSpec


@dataclass(frozen=True)
class MCPToolSpec:
    """A tool discovered on a server — enough to offer it to the model."""

    server: str
    name: str  # the server's own tool name (unqualified)
    description: str
    input_schema: dict[str, Any]


def _flatten(result: Any) -> str:
    """Reduce an MCP CallToolResult to the text the model should see. Text blocks
    are concatenated; non-text blocks (images, etc.) are noted but not inlined. An
    error result is surfaced as an `error:` string (fed back to the model, never
    raised through the turn)."""
    parts: list[str] = []
    for block in getattr(result, "content", None) or []:
        text = getattr(block, "text", None)
        if text is not None:
            parts.append(text)
        else:
            parts.append(f"[{getattr(block, 'type', 'non-text')} content]")
    out = "\n".join(p for p in parts if p).strip()
    if getattr(result, "isError", False):
        return f"error: {out or 'tool call failed'}"
    return out or "(no output)"


class MCPClient:
    def __init__(self, spec: MCPServerSpec, *, call_timeout_s: float, auth: Any = None) -> None:
        self._spec = spec
        self._call_timeout_s = call_timeout_s
        self._auth = auth  # httpx.Auth (OAuth provider) for http servers; None = none
        self._runner: asyncio.Task | None = None  # owns the session's lifetime
        self._ready: asyncio.Event | None = None  # set when connected OR failed
        self._error: BaseException | None = None
        self._session: Any = None  # mcp.ClientSession once connected
        self.tools: list[MCPToolSpec] = []

    @property
    def name(self) -> str:
        return self._spec.name

    async def connect(self) -> list[MCPToolSpec]:
        """Start the runner task, wait for it to connect + discover tools, and
        return them. Raises on failure (the bridge catches per-server so one bad
        server can't sink the others). Bound with a timeout at the caller — the
        bridge does; a timeout cancels connect(), and the bridge then aclose()s
        to stop the runner."""
        self._ready = asyncio.Event()
        self._error = None
        self._runner = asyncio.create_task(self._run())
        await self._ready.wait()
        if self._error is not None:
            await self.aclose()
            raise self._error
        return self.tools

    async def _run(self) -> None:
        """The runner task: open the transport + session, discover tools, then
        park until cancelled. All `async with` enter/exit happen here, in one
        task — the invariant the SDK's cancel scopes require."""
        from mcp import ClientSession

        try:
            async with self._transport_cm() as conn:
                read, write = conn[0], conn[1]  # stdio: 2-tuple, http: 3-tuple
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    listed = await session.list_tools()
                    self._session = session
                    self.tools = [
                        MCPToolSpec(
                            self._spec.name,
                            t.name,
                            t.description or "",
                            t.inputSchema or {"type": "object", "properties": {}},
                        )
                        for t in listed.tools
                    ]
                    self._ready.set()  # connect() may now return
                    await asyncio.Event().wait()  # park until aclose() cancels us
        except asyncio.CancelledError:
            raise  # normal teardown path
        except BaseException as exc:  # noqa: BLE001 - reported to connect()
            self._error = exc
        finally:
            self._session = None
            if self._ready is not None:
                self._ready.set()  # unblock connect() even on early failure

    def _transport_cm(self):  # noqa: ANN202 - an async context manager
        spec = self._spec
        if spec.transport == "http":
            from mcp.client.streamable_http import streamablehttp_client

            return streamablehttp_client(
                spec.url, headers=spec.headers or None, auth=self._auth
            )
        # stdio (default): spawn the server subprocess. Merge the parent env so a
        # local server (npx/node) keeps PATH etc.; spec.env layers on top.
        from mcp import StdioServerParameters
        from mcp.client.stdio import stdio_client

        params = StdioServerParameters(
            command=spec.command,
            args=list(spec.args),
            env={**os.environ, **spec.env} if spec.env else None,
        )
        return stdio_client(params)

    async def call(self, tool_name: str, args: dict[str, Any]) -> str:
        """Invoke a tool on the live session, hard-bounded by call_timeout_s.
        Raises TimeoutError on overrun (the hot path must never hang)."""
        if self._session is None:
            raise RuntimeError(f"mcp server {self._spec.name!r} not connected")
        result = await asyncio.wait_for(
            self._session.call_tool(tool_name, args or {}), self._call_timeout_s
        )
        return _flatten(result)

    async def aclose(self) -> None:
        """Stop the runner task; its cancellation unwinds the session + transport
        in the runner's own task, satisfying the cancel-scope invariant."""
        if self._runner is None:
            return
        self._runner.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await self._runner
        self._runner = None
        self._session = None
