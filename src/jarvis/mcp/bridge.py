"""MCPBridge — manages every configured MCP server, per principal (Phase 3 §6/§5).

Connects all servers at brain startup (cold path), aggregates + namespaces their
tools, and routes calls back to the right server **under the calling user's
credentials**. Two kinds of server:

- **stdio** (local: context7, obsidian) — a shared house resource, one connection.
- **http/OAuth** (Notion, Granola, …) — account-scoped, so one connection *per
  user* using that user's cached token (`.mcp-auth/<user>/`). Jules's request uses
  Jules's token; it never touches Neil's. A user with the capability but no token
  gets a "run `jarvis mcp login --user`" message, not someone else's account.

Connections key on `(principal, server)`. Best-effort: a server that fails to
connect is skipped, never fatal. The per-server `include` list + `max_tools_per_server`
cap are the sprawl firewall; the capability gate (tool layer) is the use firewall.
Imports nothing from the brain.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from jarvis.config import MCPConfig, MCPServerSpec
from jarvis.ids import utc_now
from jarvis.orchestration.redaction import public_error_message
from jarvis.mcp.client import MCPClient, MCPToolSpec

_NAME_OK = re.compile(r"[^a-zA-Z0-9_-]")
_SHARED = "house"  # stdio servers connect once under this principal


def _sanitize(name: str) -> str:
    return _NAME_OK.sub("_", name)[:64]


@dataclass(frozen=True)
class BridgedTool:
    offered_name: str
    server: str
    server_tool: str
    description: str
    input_schema: dict[str, Any]
    required_capability: str


class MCPBridge:
    def __init__(self, cfg: MCPConfig, principals: Iterable[str] = ("house",)) -> None:
        self._cfg = cfg
        # Principals to pre-connect OAuth servers for (house + known users).
        self._principals = list(dict.fromkeys(["house", *principals]))
        self._clients: dict[tuple[str, str], MCPClient] = {}  # (principal, server) -> client
        self._client_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._routes: dict[str, tuple[str, str]] = {}  # offered name -> (server, tool)
        self._spec: dict[str, MCPServerSpec] = {}  # server name -> spec
        self._errors: dict[str, str] = {}
        self._connected_at: dict[str, str] = {}
        self.tools: list[BridgedTool] = []

    @property
    def connected(self) -> list[str]:
        return sorted({server for _, server in self._clients})

    def status(self) -> dict[str, Any]:
        """Return a public-safe bridge snapshot for the cockpit sidecar."""
        connected = set(self.connected)
        tool_counts: dict[str, int] = {}
        for tool in self.tools:
            tool_counts[tool.server] = tool_counts.get(tool.server, 0) + 1
        return {
            "servers": [
                {
                    "name": spec.name,
                    "transport": spec.transport,
                    "connected": spec.name in connected,
                    "tool_count": tool_counts.get(spec.name, 0),
                    "error": public_error_message(self._errors.get(spec.name, "")),
                    "connected_at": self._connected_at.get(spec.name, ""),
                    "required_capability": spec.required_capability,
                }
                for spec in self._cfg.servers
            ],
            "tools": [
                {
                    "offered_name": tool.offered_name,
                    "server": tool.server,
                    "description": public_error_message(tool.description),
                    "required_capability": tool.required_capability,
                }
                for tool in self.tools
            ],
        }

    async def start(self) -> list[BridgedTool]:
        """Connect every configured server (best-effort) and return the bridged
        tools. Off the hot path — called once at brain startup."""
        if not self._cfg.enabled or not self._cfg.servers:
            return []
        from jarvis.mcp.auth import needs_oauth

        for spec in self._cfg.servers:
            self._spec[spec.name] = spec
            if needs_oauth(spec):
                # http/OAuth: one connection per principal that has a cached token.
                for principal in self._principals:
                    if self._has_token(spec, principal):
                        await self._connect(principal, spec, register=not self._registered(spec))
                if not self._registered(spec):
                    print(
                        f"  [mcp] {spec.name}: no authorized user yet — "
                        "run `jarvis mcp login --user <name>`"
                    )
            else:
                # stdio (or http with static headers): a shared house connection.
                await self._connect(_SHARED, spec, register=True)
        if self.tools:
            print(
                f"  [mcp] {len(self.tools)} tool(s) from {len(self.connected)} server(s): "
                + ", ".join(self.connected)
            )
        return self.tools

    def _registered(self, spec: MCPServerSpec) -> bool:
        # _routes maps offered_name -> (server, tool); match on the SERVER.
        return any(server == spec.name for server, _ in self._routes.values())

    def _has_token(self, spec: MCPServerSpec, principal: str) -> bool:
        from jarvis.mcp.auth import FileTokenStorage, auth_path

        return FileTokenStorage(auth_path(self._cfg, spec.name, principal)).has_tokens()

    def _auth_for(self, spec: MCPServerSpec, principal: str):  # noqa: ANN202
        from jarvis.mcp.auth import build_oauth_provider, needs_oauth

        if not needs_oauth(spec):
            return None
        provider, _, _ = build_oauth_provider(spec, self._cfg, interactive=False, user=principal)
        return provider

    async def _connect(self, principal: str, spec: MCPServerSpec, *, register: bool) -> None:
        client = MCPClient(
            spec, call_timeout_s=self._cfg.call_timeout_s, auth=self._auth_for(spec, principal)
        )
        try:
            discovered = await asyncio.wait_for(client.connect(), self._cfg.connect_timeout_s)
        except Exception as exc:  # noqa: BLE001 - one bad server/user must not be fatal
            from jarvis.mcp.auth import needs_oauth

            cause = _root_cause(exc)
            hint = " — run `jarvis mcp login --user " + principal + "`" if needs_oauth(spec) else ""
            print(f"  [mcp] {spec.name} ({principal}): connect failed ({cause}){hint}")
            self._errors[spec.name] = cause
            await _safe_close(client)
            return
        self._clients[(principal, spec.name)] = client
        self._errors.pop(spec.name, None)
        self._connected_at[spec.name] = utc_now()
        if register:
            for t in self._select(spec, discovered):
                self._add(spec, t)

    async def _connect_once(
        self, principal: str, spec: MCPServerSpec, *, register: bool
    ) -> MCPClient | None:
        key = (principal, spec.name)
        lock = self._client_locks.setdefault(key, asyncio.Lock())
        async with lock:
            client = self._clients.get(key)
            if client is not None:
                return client
            await self._connect(principal, spec, register=register)
            return self._clients.get(key)

    def _select(self, spec: MCPServerSpec, discovered: list[MCPToolSpec]) -> list[MCPToolSpec]:
        tools = discovered
        if spec.include:
            allow = set(spec.include)
            tools = [t for t in tools if t.name in allow]
        cap = self._cfg.max_tools_per_server
        if cap and len(tools) > cap:
            print(f"  [mcp] {spec.name}: capping {len(tools)} tools to {cap}")
            tools = tools[:cap]
        return tools

    def _add(self, spec: MCPServerSpec, t: MCPToolSpec) -> None:
        base = f"{spec.name}_{t.name}" if self._cfg.namespace else t.name
        offered = _sanitize(base)
        if offered in self._routes:
            offered = _sanitize(f"{spec.name}_{t.name}_{len(self._routes)}")
        self._routes[offered] = (spec.name, t.name)
        self.tools.append(
            BridgedTool(
                offered_name=offered,
                server=spec.name,
                server_tool=t.name,
                description=t.description,
                input_schema=t.input_schema,
                required_capability=spec.required_capability,
            )
        )

    def _principal_for(self, server: str, user: str) -> str:
        """stdio servers are shared (house); http servers run under the caller."""
        spec = self._spec.get(server)
        from jarvis.mcp.auth import needs_oauth

        return user if (spec and needs_oauth(spec)) else _SHARED

    async def call(self, offered_name: str, args: dict[str, Any], *, user: str = "house") -> str:
        """Route a call to the owning server under `user`'s credentials. Raises
        (Timeout/RuntimeError/SDK errors); the tool-layer handler formats failures."""
        route = self._routes.get(offered_name)
        if route is None:
            raise RuntimeError(f"unknown mcp tool {offered_name!r}")
        server, server_tool = route
        principal = self._principal_for(server, user)
        client = self._clients.get((principal, server))
        if client is None:
            # http server the user hasn't connected: lazy-connect if they have a
            # token, else tell them to log in (never fall back to another user).
            spec = self._spec[server]
            if self._has_token(spec, principal):
                client = await self._connect_once(principal, spec, register=False)
            if client is None:
                raise RuntimeError(
                    f"{user} isn't signed in to {server!r} — run "
                    f"`jarvis mcp login --user {user}`"
                )
        return await client.call(server_tool, args)

    async def aclose(self) -> None:
        for client in self._clients.values():
            await _safe_close(client)
        self._clients = {}
        self._client_locks = {}
        self._routes = {}
        self._spec = {}
        self._errors = {}
        self._connected_at = {}
        self.tools = []


def _root_cause(exc: BaseException) -> str:
    seen = 0
    while isinstance(exc, BaseExceptionGroup) and exc.exceptions and seen < 10:
        exc = exc.exceptions[0]
        seen += 1
    return str(exc) or type(exc).__name__


async def _safe_close(client: MCPClient) -> None:
    try:
        await client.aclose()
    except Exception as exc:  # noqa: BLE001 - shutdown must never raise
        print(f"  [mcp] {client.name}: close error ({exc})")
