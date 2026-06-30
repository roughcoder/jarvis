"""Intercom message routing — the testable core of the proactive restructure.

The actual audio (tone + speech out of a speaker) is a human listen, but the queue
routing — pulling reply/proactive frames by turn id, spotting a proactive while idle —
is pure logic and pinned here.
"""

from __future__ import annotations

import asyncio

from jarvis.config import load_config
from jarvis.intercom.client import IntercomClient
from jarvis.intercom.metrics import SCHEMA_VERSION, IntercomReplyMetrics, summary
from jarvis.protocol.messages import (
    BinaryAudio,
    ConversationIdle,
    DeviceRequest,
    DeviceResponse,
    Proactive,
    REPLY_AUDIO_BINARY_V1,
    ReplyEnd,
    ReplyText,
    Transcript,
    Utterance,
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


class _Panel:
    def __init__(self, modes: list[str] | None = None) -> None:
        self.modes = modes or []
        self.published_modes: list[str] = []
        self.states: list[str] = []

    def start(self) -> None:
        return

    def stop(self) -> None:
        return

    def set(self, state: str) -> None:
        self.states.append(state)

    def set_voice_mode(self, voice_mode: str) -> None:
        self.published_modes.append(voice_mode)

    def take_voice_mode(self) -> str | None:
        return self.modes.pop(0) if self.modes else None


def _client(panel=None) -> IntercomClient:  # noqa: ANN001
    return IntercomClient(
        load_config(), audio=_Stub(), vad=_Stub(), wake=_Stub(), hardware=_Hardware(), panel=panel
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


def test_queued_passive_proactive_does_not_exit_selected_stay_mode() -> None:
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    ws = _WS()
    mic = _Mic()
    c._set_active_voice_mode("stay")
    q.put_nowait(Proactive(text="notification", turn_id="pa-4", open_mic=False))
    q.put_nowait(ReplyEnd(turn_id="pa-4"))
    active_state = {
        "ended": False,
        "text": "Working session",
        "continue_listening": True,
        "voice_mode": "stay",
        "close_reason": "stay_mode",
    }

    async def fake_play_stream(chunks, **_kwargs):  # noqa: ANN001
        return [chunk async for chunk in chunks]

    c._audio.play_stream = fake_play_stream  # type: ignore[attr-defined]

    async def go() -> dict | None:
        return await c._play_queued_proactive(ws, mic, q, active_state)

    state = asyncio.run(go())
    assert state == active_state
    assert c._active_voice_mode == "stay"
    assert mic.drained == 1


def test_reply_audio_yields_pcm_and_records_state() -> None:
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    for m in [
        Transcript(turn_id="t1", text="hello"),
        BinaryAudio(kind="reply_audio", turn_id="t1", pcm=b"\x01\x02"),
        BinaryAudio(kind="reply_audio", turn_id="zz", pcm=b"\xff"),  # other turn id
        ReplyText(turn_id="t1", text="hi there"),
        BinaryAudio(kind="reply_audio", turn_id="t1", pcm=b"\x03\x04"),
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
        metrics = IntercomReplyMetrics(turn_id="t1", device_id="pi")
        chunks = [pcm async for pcm in c._reply_audio(q, "t1", state, metrics)]
        assert metrics.data["schema_version"] == SCHEMA_VERSION
        assert state["text"] == "hi there"
        assert state["ended"] is False
        assert state["continue_listening"] is True
        assert state["voice_mode"] == "stay"
        audio = metrics.data["stages"]["reply_audio"]
        assert audio["chunks"] == 2
        assert audio["bytes"] == 4
        assert audio["protocol"] == REPLY_AUDIO_BINARY_V1
        assert audio["decode_ms"] == 0
        return chunks

    assert asyncio.run(go()) == [b"\x01\x02", b"\x03\x04"]  # only this turn's audio


def test_intercom_metric_summary_reports_playback_baseline() -> None:
    metrics = IntercomReplyMetrics(turn_id="t1", device_id="kitchen-pi")
    metrics.mark_utterance_sent(pcm_bytes=32000, frame_bytes=43000)
    metrics.record_audio_frame(
        protocol=REPLY_AUDIO_BINARY_V1,
        encoded_bytes=4,
        pcm_bytes=4,
    )
    metrics.data["stages"]["playback"] = {
        "ms": 500.0,
        "first_speech_ms": 420.0,
        "underruns": 2,
        "block_ms": 85.3,
    }
    metrics.data["total_ms"] = 900.0

    text = summary(metrics.data)
    assert "intercom/turn" in text
    assert "device=kitchen-pi" in text
    assert "first_frame=" in text
    assert "speech=420ms" in text
    assert "underruns=2" in text


def test_reply_audio_works_for_proactive_turn_id() -> None:
    # a proactive plays through the same path under its 'pa-' turn id
    c = _client()
    q: asyncio.Queue = asyncio.Queue()
    for m in [
        BinaryAudio(kind="reply_audio", turn_id="pa-9", pcm=b"tone"),
        BinaryAudio(kind="reply_audio", turn_id="pa-9", pcm=b"talk"),
        ReplyEnd(turn_id="pa-9"),
    ]:
        q.put_nowait(m)

    async def go() -> list[bytes]:
        return [pcm async for pcm in c._reply_audio(q, "pa-9", {"ended": False, "text": ""})]

    assert asyncio.run(go()) == [b"tone", b"talk"]


def test_passive_proactive_reply_end_preserves_selected_voice_mode() -> None:
    c = _client()
    c._set_active_voice_mode("stay")
    q: asyncio.Queue = asyncio.Queue()
    q.put_nowait(ReplyEnd(turn_id="pa-10"))
    state = {"ended": False, "text": "", "voice_mode": "stay", "continue_listening": True}

    async def go() -> list[bytes]:
        return [pcm async for pcm in c._reply_audio(q, "pa-10", state)]

    assert asyncio.run(go()) == []
    assert state["voice_mode"] == "stay"
    assert state["continue_listening"] is False
    assert c._active_voice_mode == "stay"


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


def test_panel_selected_voice_mode_is_sent_with_next_utterance() -> None:
    panel = _Panel(["stay"])
    c = _client(panel)
    q: asyncio.Queue = asyncio.Queue()
    ws = _WS()
    mic = _Mic()

    async def fake_play(*args):  # noqa: ANN002
        state = args[4]
        state["ended"] = True
        state["voice_mode"] = "stay"
        state["continue_listening"] = False
        return False

    c._play_reply = fake_play  # type: ignore[method-assign]

    async def go() -> dict | None:
        return await c._converse(ws, mic, q, b"hello")

    state = asyncio.run(go())
    sent = [decode(item) for item in ws.sent]
    utterances = [item for item in sent if isinstance(item, Utterance)]
    assert state is not None
    assert utterances and utterances[0].voice_mode == "stay"
    assert c._active_voice_mode == "stay"
    assert "stay" in panel.published_modes


def test_link_loss_uses_network_state_when_probe_fails() -> None:
    panel = _Panel()
    c = _client(panel)

    c._network_online = lambda: False  # type: ignore[method-assign]

    async def go() -> None:
        await c._set_link_lost_state()

    asyncio.run(go())

    assert panel.states == ["network"]


def test_link_loss_uses_disconnected_when_network_probe_passes() -> None:
    panel = _Panel()
    c = _client(panel)

    c._network_online = lambda: True  # type: ignore[method-assign]

    async def go() -> None:
        await c._set_link_lost_state()

    asyncio.run(go())

    assert panel.states == ["disconnected"]


def test_network_probe_can_be_disabled_for_lan_only_installs() -> None:
    c = _client()
    c._cfg.intercom.network_probe_host = "none"

    assert c._network_online() is True


def test_network_probe_failure_does_not_block_brain_reconnect() -> None:
    panel = _Panel()
    c = _client(panel)
    recovered = False

    c._network_online = lambda: False  # type: ignore[method-assign]

    async def fake_recover() -> None:
        nonlocal recovered
        recovered = True

    c._recover_network_if_needed = fake_recover  # type: ignore[method-assign]

    async def go() -> bool:
        return await c._ensure_network_ready()

    assert asyncio.run(go()) is False

    assert panel.states == ["network"]
    assert recovered is True


def test_failed_network_probe_keeps_network_state_during_brain_connect_attempt() -> None:
    panel = _Panel()
    c = _client(panel)

    c._network_online = lambda: False  # type: ignore[method-assign]

    async def fake_recover() -> None:
        return None

    c._recover_network_if_needed = fake_recover  # type: ignore[method-assign]

    async def go() -> None:
        await c._prepare_brain_connect_attempt()

    asyncio.run(go())

    assert panel.states == ["network"]


def test_network_probe_success_shows_connecting_before_brain_attempt() -> None:
    panel = _Panel()
    c = _client(panel)

    c._network_online = lambda: True  # type: ignore[method-assign]

    async def go() -> None:
        await c._prepare_brain_connect_attempt()

    asyncio.run(go())

    assert panel.states == ["connecting"]


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
