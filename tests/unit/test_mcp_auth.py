"""MCP OAuth onboarding — token storage, loopback callback, provider wiring.

The full browser round-trip needs a real server + a human, so it isn't unit
tested; what IS testable in isolation: the per-server token store, the localhost
callback that catches the redirect, and that the headless vs interactive provider
gets handlers wired correctly. Skips when the `mcp` SDK is absent.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

pytest.importorskip("mcp")

from jarvis.config import MCPConfig, MCPServerSpec  # noqa: E402
from jarvis.mcp.auth import (  # noqa: E402
    FileTokenStorage,
    LoopbackFlow,
    build_oauth_provider,
    needs_oauth,
    _wait_for_code,
)


def test_needs_oauth_rules() -> None:
    assert needs_oauth(MCPServerSpec(name="n", transport="http", url="https://x/mcp"))
    # static headers => the server authenticates without a browser
    assert not needs_oauth(
        MCPServerSpec(name="n", transport="http", url="https://x/mcp", headers={"Authorization": "Bearer k"})
    )
    assert not needs_oauth(MCPServerSpec(name="c", command="npx"))  # stdio


def test_file_token_storage_roundtrip(tmp_path) -> None:  # noqa: ANN001
    from mcp.shared.auth import OAuthToken

    store = FileTokenStorage(tmp_path / "notion.json")

    async def go():  # noqa: ANN202
        assert await store.get_tokens() is None  # empty => None
        assert store.has_tokens() is False
        await store.set_tokens(OAuthToken(access_token="tok-123", token_type="Bearer"))
        got = await store.get_tokens()
        return got

    got = asyncio.run(go())
    assert got.access_token == "tok-123"
    assert store.has_tokens() is True
    assert (tmp_path / "notion.json").exists()


def test_loopback_callback_captures_code() -> None:
    port = 41799

    async def go():  # noqa: ANN202
        waiter = asyncio.create_task(_wait_for_code(port))
        await asyncio.sleep(0.15)  # let the loopback server bind
        async with httpx.AsyncClient() as c:
            await c.get(f"http://localhost:{port}/callback?code=the-code&state=st-1")
        return await asyncio.wait_for(waiter, 5)

    code, state = asyncio.run(go())
    assert code == "the-code"
    assert state == "st-1"


def test_provider_handlers_wired_by_mode(tmp_path) -> None:  # noqa: ANN001
    cfg = MCPConfig(_env_file=None, auth_dir=str(tmp_path), oauth_redirect_port=41760)
    spec = MCPServerSpec(name="notion", transport="http", url="https://mcp.notion.com/mcp")

    # headless (brain): no interactive flow => never pops a browser
    _provider, storage, flow = build_oauth_provider(spec, cfg, interactive=False)
    assert flow is None
    assert isinstance(storage, FileTokenStorage)

    # interactive (jarvis mcp login): a loopback flow, not yet opened
    _provider, _storage, flow = build_oauth_provider(spec, cfg, interactive=True)
    assert isinstance(flow, LoopbackFlow)
    assert flow.opened is False
