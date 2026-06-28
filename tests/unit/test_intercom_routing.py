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
    ConversationIdle,
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


class _Mic:
    def __init__(self) -> None:
        self.drained = 0

    def drain(self) -> None:
        self.drained += 1


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


def test_stay_mode_can_play_queued_proactive_between_silence_windows() -> None:
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    ws = _WS()
    mic = _Mic()
    seen: list[str] = []
    q.put_nowait(Proactive(text="alarm", turn_id="pa-1", open_mic=True))

    async def fake_play(ws_arg, mic_arg, inbound_arg, pro):  # noqa: ANN001
        assert ws_arg is ws
        assert mic_arg is mic
        assert inbound_arg is q
        seen.append(pro.text)
        return {
            "ended": False,
            "text": "alarm",
            "continue_listening": True,
            "voice_mode": "stay",
            "close_reason": "",
        }

    c._play_proactive = fake_play  # type: ignore[method-assign]

    async def go() -> dict | None:
        return await c._play_queued_proactive(ws, mic, q)

    state = asyncio.run(go())
    assert state is not None
    assert state["voice_mode"] == "stay"
    assert state["continue_listening"] is True
    assert seen == ["alarm"]
    assert mic.drained == 1
    assert asyncio.run(go()) is None


def test_queued_proactive_returns_mode_exit_state() -> None:
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    ws = _WS()
    mic = _Mic()
    q.put_nowait(Proactive(text="notification", turn_id="pa-2", open_mic=True))

    async def fake_play(*_args):  # noqa: ANN002
        return {
            "ended": True,
            "text": "Okay, exiting stay mode.",
            "continue_listening": False,
            "voice_mode": "default",
            "close_reason": "mode_exit",
        }

    c._play_proactive = fake_play  # type: ignore[method-assign]

    async def go() -> dict | None:
        return await c._play_queued_proactive(ws, mic, q)

    state = asyncio.run(go())
    assert state is not None
    assert state["ended"] is True
    assert state["voice_mode"] == "default"
    assert state["continue_listening"] is False


def test_queued_proactive_preserves_stay_state_for_passive_playback() -> None:
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    ws = _WS()
    mic = _Mic()
    q.put_nowait(Proactive(text="notification", turn_id="pa-3", open_mic=True))
    active_state = {
        "ended": False,
        "text": "Working session",
        "continue_listening": True,
        "voice_mode": "stay",
        "close_reason": "stay_mode",
    }

    async def fake_play(*_args):  # noqa: ANN002
        return {
            "ended": False,
            "text": "notification",
            "continue_listening": False,
            "voice_mode": "default",
            "close_reason": "",
        }

    c._play_proactive = fake_play  # type: ignore[method-assign]

    async def go() -> dict | None:
        return await c._play_queued_proactive(ws, mic, q, active_state)

    state = asyncio.run(go())
    assert state == active_state
    assert state is not active_state
    assert mic.drained == 1


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


def test_interrupted_silence_sends_conversation_idle() -> None:
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    ws = _WS()
    mic = _Mic()

    async def fake_play(*_args):  # noqa: ANN002
        return True

    c._play_reply = fake_play  # type: ignore[method-assign]
    c._capture_utterance = lambda *_args, **_kwargs: b""  # type: ignore[method-assign]

    async def go() -> dict | None:
        return await c._converse(ws, mic, q, b"hello")

    assert asyncio.run(go()) is None
    sent = [decode(item) for item in ws.sent]
    assert any(isinstance(item, ConversationIdle) and item.reason == "timeout" for item in sent)


def test_interrupted_stay_mode_silence_keeps_listening_without_idle() -> None:
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    ws = _WS()
    mic = _Mic()
    captures = iter([b"", b"exit stay mode"])
    replies = 0

    async def fake_play(*args):  # noqa: ANN002
        nonlocal replies
        replies += 1
        state = args[4]
        if replies == 1:
            state["voice_mode"] = "stay"
            state["continue_listening"] = True
            return True
        state["ended"] = True
        state["voice_mode"] = "default"
        state["continue_listening"] = False
        state["close_reason"] = "mode_exit"
        return False

    c._play_reply = fake_play  # type: ignore[method-assign]
    c._capture_utterance = lambda *_args, **_kwargs: next(captures)  # type: ignore[method-assign]

    async def go() -> dict | None:
        return await c._converse(ws, mic, q, b"hello")

    state = asyncio.run(go())
    sent = [decode(item) for item in ws.sent]
    assert state is not None
    assert state["close_reason"] == "mode_exit"
    assert not any(isinstance(item, ConversationIdle) for item in sent)
    assert replies == 2


def test_interrupted_stay_mode_without_reply_end_preserves_active_mode() -> None:
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    ws = _WS()
    mic = _Mic()
    captures = iter([b"question", b"", b"exit stay mode"])
    replies = 0

    async def fake_play(*args):  # noqa: ANN002
        nonlocal replies
        replies += 1
        state = args[4]
        if replies == 1:
            state["voice_mode"] = "stay"
            state["continue_listening"] = True
            return False
        if replies == 2:
            return True
        state["ended"] = True
        state["voice_mode"] = "default"
        state["continue_listening"] = False
        state["close_reason"] = "mode_exit"
        return False

    c._play_reply = fake_play  # type: ignore[method-assign]
    c._capture_utterance = lambda *_args, **_kwargs: next(captures)  # type: ignore[method-assign]

    async def go() -> dict | None:
        return await c._converse(ws, mic, q, b"start stay")

    state = asyncio.run(go())
    sent = [decode(item) for item in ws.sent]
    assert state is not None
    assert state["close_reason"] == "mode_exit"
    assert not any(isinstance(item, ConversationIdle) for item in sent)
    assert replies == 3


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
