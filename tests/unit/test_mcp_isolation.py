"""Per-user MCP credential isolation (Phase 3d §5) — the privacy wall.

Token files are keyed per `(user, server)`, and the bridge routes a call to the
caller's own connection — never another user's, and a missing one errors with a
login prompt rather than falling back. SDK-free (fake clients).
"""

from __future__ import annotations

import asyncio
import pathlib

import pytest

from jarvis.config import MCPConfig, MCPServerSpec
from jarvis.mcp.auth import auth_path
from jarvis.mcp.bridge import MCPBridge


def test_auth_path_is_per_user(tmp_path) -> None:  # noqa: ANN001
    cfg = MCPConfig(_env_file=None, auth_dir=str(tmp_path))
    assert auth_path(cfg, "notion", "neil") == pathlib.Path(tmp_path) / "neil" / "notion.json"
    assert auth_path(cfg, "notion", "jules") == pathlib.Path(tmp_path) / "jules" / "notion.json"
    assert auth_path(cfg, "notion") == pathlib.Path(tmp_path) / "house" / "notion.json"


class _FakeClient:
    def __init__(self, who: str) -> None:
        self.who = who

    async def call(self, tool: str, args: dict) -> str:
        return f"{self.who}:{tool}"

    async def aclose(self) -> None:
        pass


def _bridge_with(tmp_path) -> MCPBridge:  # noqa: ANN001
    cfg = MCPConfig(_env_file=None, enabled=True, auth_dir=str(tmp_path))
    b = MCPBridge(cfg, principals=["neil", "jules"])
    # An OAuth (http) server with each user's own live connection.
    b._spec["notion"] = MCPServerSpec(name="notion", transport="http", url="https://x/mcp")
    b._routes["notion_search"] = ("notion", "search")
    b._clients[("neil", "notion")] = _FakeClient("neil")
    b._clients[("jules", "notion")] = _FakeClient("jules")
    # A stdio server — shared house resource, not account-scoped.
    b._spec["ctx"] = MCPServerSpec(name="ctx", command="x")
    b._routes["ctx_q"] = ("ctx", "q")
    b._clients[("house", "ctx")] = _FakeClient("shared")
    return b


def test_oauth_call_runs_under_callers_own_credentials(tmp_path) -> None:  # noqa: ANN001
    b = _bridge_with(tmp_path)
    assert asyncio.run(b.call("notion_search", {}, user="neil")) == "neil:search"
    assert asyncio.run(b.call("notion_search", {}, user="jules")) == "jules:search"


def test_unauthed_user_errors_without_borrowing_another(tmp_path) -> None:  # noqa: ANN001
    b = _bridge_with(tmp_path)
    # house has no token + no client → must error, never reuse neil's/jules's client.
    with pytest.raises(RuntimeError, match="isn't signed in"):
        asyncio.run(b.call("notion_search", {}, user="house"))


def test_stdio_server_is_shared_regardless_of_user(tmp_path) -> None:  # noqa: ANN001
    b = _bridge_with(tmp_path)
    assert asyncio.run(b.call("ctx_q", {}, user="jules")) == "shared:q"
    assert asyncio.run(b.call("ctx_q", {}, user="neil")) == "shared:q"
