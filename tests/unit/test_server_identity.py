"""Brain-server identity routing over a real WebSocket (Phase 3d §3/§5).

Offline + hermetic: a temp users/profiles set, no LLM/TTS needed — we only pair
and read the Welcome, which proves the device→identity→scope→capabilities pipeline
runs end to end through the real server. Two devices resolve to two principals.
"""

from __future__ import annotations

import asyncio

import pytest
import websockets

from jarvis.brain.server import BrainServer
from jarvis.config import BrainConfig, CapabilityConfig, MCPConfig, load_config
from jarvis.protocol.messages import Hello, Welcome, decode, encode


@pytest.fixture
def cfg(tmp_path):  # noqa: ANN001, ANN201
    profiles = tmp_path / "profiles"
    users = tmp_path / "users"
    profiles.mkdir()
    users.mkdir()
    (profiles / "local-mac.md").write_text("---\ncapabilities: [files.read, web.search]\n---\n")
    (profiles / "room-pi.md").write_text("---\ncapabilities: [web.search]\n---\n")
    (users / "neil.md").write_text(
        "---\ndevices: [local-mac]\ncapabilities: [mcp.notion]\nscope: personal\nhoncho_peer: neil\n---\n"
    )
    c = load_config()
    c.capabilities = CapabilityConfig(
        _env_file=None, device_id="local-mac", profiles_dir=str(profiles), users_dir=str(users)
    )
    c.mcp = MCPConfig(_env_file=None, enabled=False)  # don't connect MCP in this test
    c.brain = BrainConfig(_env_file=None)  # open pairing — ignore the real .env BRAIN_DEVICES
    return c


async def _welcome(server: BrainServer, device_id: str) -> Welcome:
    async with websockets.serve(server._handle, "localhost", 0) as srv:
        port = srv.sockets[0].getsockname()[1]
        async with websockets.connect(f"ws://localhost:{port}") as ws:
            await ws.send(encode(Hello(device_id=device_id)))
            return decode(await asyncio.wait_for(ws.recv(), 5))


def test_personal_device_resolves_to_its_owner(cfg) -> None:  # noqa: ANN001
    w = asyncio.run(_welcome(BrainServer(cfg), "local-mac"))
    assert isinstance(w, Welcome)
    assert w.identity == "neil"
    assert w.scope == "personal"
    assert "mcp.notion" in w.capabilities  # the owner's grant is added in personal scope
    assert "files.read" in w.capabilities  # plus the device profile's


def test_shared_device_resolves_to_house(cfg) -> None:  # noqa: ANN001
    w = asyncio.run(_welcome(BrainServer(cfg), "room-pi"))
    assert isinstance(w, Welcome)
    assert w.identity == "house"
    assert w.scope == "house"
    assert "mcp.notion" not in w.capabilities  # no personal grants for an unknown speaker
    assert w.capabilities == ["web.search"]
