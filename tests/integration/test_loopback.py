"""Integration: brain<->intercom loopback over a real WebSocket (Phase 3 W4).

Starts the brain server in-process and drives a text turn through it, asserting
reply audio streams back and the turn ends. Uses the TextIn path so no mic/STT is
needed, but it does a real LLM + TTS round-trip — skips without a gateway + TTS
key. This is the end-to-end proof that the protocol + server + BrainSession wire
together.
"""

from __future__ import annotations

import asyncio
import socket

import pytest
import websockets

from jarvis.brain.server import BrainServer
from jarvis.config import load_config
from jarvis.protocol.messages import (
    Hello,
    ReplyEnd,
    TextIn,
    Welcome,
    decode_binary_audio,
    decode,
    encode,
)

pytestmark = pytest.mark.integration


def _gateway_up(cfg) -> bool:
    try:
        with socket.create_connection((cfg.gateway.host, cfg.gateway.port), timeout=1.0):
            return True
    except OSError:
        return False


def test_text_turn_streams_reply_audio() -> None:
    cfg = load_config()
    if not _gateway_up(cfg):
        pytest.skip(f"gateway not reachable at {cfg.gateway.base_url}")
    if not cfg.tts.api_key.get_secret_value():
        pytest.skip("TTS_API_KEY not set")

    async def run() -> tuple[bool, bool]:
        server = BrainServer(cfg)
        async with websockets.serve(server._handle, "localhost", 0) as srv:
            port = srv.sockets[0].getsockname()[1]
            async with websockets.connect(f"ws://localhost:{port}") as ws:
                await ws.send(encode(Hello(device_id="test-mac")))
                welcome = decode(await ws.recv())
                assert isinstance(welcome, Welcome)
                await ws.send(encode(TextIn(turn_id="t1", text="Say hello in three words.")))
                got_audio = False
                ended = False
                async for raw in ws:
                    if isinstance(raw, bytes):
                        audio = decode_binary_audio(raw)
                        if audio is not None and audio.turn_id == "t1":
                            got_audio = True
                        continue
                    msg = decode(raw)
                    if isinstance(msg, ReplyEnd) and msg.turn_id == "t1":
                        ended = True
                        break
                return got_audio, ended

    got_audio, ended = asyncio.run(asyncio.wait_for(run(), timeout=60))
    assert ended
    assert got_audio
