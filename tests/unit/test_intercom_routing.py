"""Intercom message routing — the testable core of the proactive restructure.

The actual audio (tone + speech out of a speaker) is a human listen, but the queue
routing — pulling reply/proactive frames by turn id, spotting a proactive while idle —
is pure logic and pinned here.
"""

from __future__ import annotations

import asyncio

from jarvis.config import load_config
from jarvis.intercom.client import IntercomClient
from jarvis.protocol.messages import (
    DeviceRequest,
    DeviceResponse,
    Proactive,
    ReplyAudio,
    ReplyEnd,
    ReplyText,
    Transcript,
    decode,
)


class _Stub:  # audio/vad/wake aren't touched by the routing methods under test
    pass


class _Hardware:
    async def handle(self, action, args):  # noqa: ANN001
        assert action == "capture_photo"
        assert args == {"reason": "test"}
        return {"image_b64": "JPEG"}


class _WS:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, item: str) -> None:
        self.sent.append(item)


def _client() -> IntercomClient:
    return IntercomClient(
        load_config(), audio=_Stub(), vad=_Stub(), wake=_Stub(), hardware=_Hardware()
    )


def test_take_proactive_spots_a_proactive_else_none() -> None:
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    assert c._take_proactive(q) is None  # empty
    q.put_nowait(Proactive(text="tea's ready", turn_id="pa-1", open_mic=True))
    got = c._take_proactive(q)
    assert got is not None and got.text == "tea's ready" and got.open_mic is True
    q.put_nowait(ReplyText(turn_id="x", text="stray"))
    assert c._take_proactive(q) is None  # a stray non-proactive while idle is dropped


def test_reply_audio_yields_pcm_and_records_state() -> None:
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    for m in [
        Transcript(turn_id="t1", text="hello"),
        ReplyAudio.of("t1", b"\x01\x02"),
        ReplyAudio.of("zz", b"\xff"),  # other turn id → ignored
        ReplyText(turn_id="t1", text="hi there"),
        ReplyAudio.of("t1", b"\x03\x04"),
        ReplyEnd(
            turn_id="t1",
            ended=False,
            continue_listening=True,
            voice_mode="stay",
            close_reason="mode_enter",
        ),
    ]:
        q.put_nowait(m)

    async def go() -> list[bytes]:
        state = {"ended": False, "text": ""}
        chunks = [pcm async for pcm in c._reply_audio(q, "t1", state)]
        assert state["text"] == "hi there"
        assert state["ended"] is False
        assert state["continue_listening"] is True
        assert state["voice_mode"] == "stay"
        return chunks

    assert asyncio.run(go()) == [b"\x01\x02", b"\x03\x04"]  # only this turn's audio


def test_reply_audio_works_for_proactive_turn_id() -> None:
    # a proactive plays through the same path under its 'pa-' turn id
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    for m in [ReplyAudio.of("pa-9", b"tone"), ReplyAudio.of("pa-9", b"talk"), ReplyEnd(turn_id="pa-9")]:
        q.put_nowait(m)

    async def go() -> list[bytes]:
        return [pcm async for pcm in c._reply_audio(q, "pa-9", {"ended": False, "text": ""})]

    assert asyncio.run(go()) == [b"tone", b"talk"]


def test_device_request_returns_device_response() -> None:
    c = _client()
    ws = _WS()

    async def go() -> DeviceResponse:
        await c._handle_device_request(
            ws, DeviceRequest(request_id="r1", action="capture_photo", args={"reason": "test"})
        )
        assert ws.sent
        msg = decode(ws.sent[0])
        assert isinstance(msg, DeviceResponse)
        return msg

    got = asyncio.run(go())
    assert got.ok is True
    assert got.result["image_b64"] == "JPEG"
