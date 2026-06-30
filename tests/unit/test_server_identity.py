"""Brain-server identity routing over a real WebSocket (Phase 3d §3/§5).

Offline + hermetic: a temp users/profiles set, no LLM/TTS needed — we only pair
and read the Welcome, which proves the device→identity→scope→capabilities pipeline
runs end to end through the real server. Two devices resolve to two principals.
"""

from __future__ import annotations

import asyncio

import pytest
import websockets

from jarvis.brain.server import BrainServer, BufferedAudioTurn
from jarvis.brain.tracing import TurnTrace
from jarvis.config import BrainConfig, CapabilityConfig, MCPConfig, load_config
from jarvis.protocol.messages import (
    AudioStart,
    BinaryAudio,
    Hello,
    ReplyEnd,
    ReplyText,
    TextIn,
    Welcome,
    decode_binary_audio,
    decode,
    encode,
    encode_uplink_audio_binary,
)
from jarvis.runtime import RequestContext


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


def test_audio_buffers_are_connection_local_even_with_same_turn_id() -> None:
    conn_a = {
        "audio_buffers": {
            "same-turn": {
                "sample_rate": 16000,
                "chunks": [],
                "frame_bytes": 0,
                "started_at": 1.0,
            }
        }
    }
    conn_b = {
        "audio_buffers": {
            "same-turn": {
                "sample_rate": 16000,
                "chunks": [],
                "frame_bytes": 0,
                "started_at": 1.0,
            }
        }
    }
    frame_a = encode_uplink_audio_binary("same-turn", 16000, b"a")
    frame_b = encode_uplink_audio_binary("same-turn", 16000, b"bb")

    BrainServer._buffer_audio_chunk(
        conn_a,
        BinaryAudio(kind="uplink_audio", turn_id="same-turn", pcm=b"a", sample_rate=16000),
        frame_bytes=len(frame_a),
    )
    BrainServer._buffer_audio_chunk(
        conn_b,
        BinaryAudio(kind="uplink_audio", turn_id="same-turn", pcm=b"bb", sample_rate=16000),
        frame_bytes=len(frame_b),
    )

    buffered_a = BrainServer._finish_audio_buffer(conn_a, "same-turn")
    buffered_b = BrainServer._finish_audio_buffer(conn_b, "same-turn")

    assert buffered_a is not None
    assert buffered_a.pcm == b"a"
    assert buffered_a.frame_bytes == len(frame_a)
    assert buffered_b is not None
    assert buffered_b.pcm == b"bb"
    assert buffered_b.frame_bytes == len(frame_b)


def test_audio_buffer_rejects_second_live_turn_on_same_connection() -> None:
    conn = {"audio_buffers": {}}

    assert BrainServer._start_audio_buffer(conn, AudioStart(turn_id="t1", sample_rate=16000))
    assert not BrainServer._start_audio_buffer(conn, AudioStart(turn_id="t2", sample_rate=16000))

    assert list(conn["audio_buffers"]) == ["t1"]


def test_audio_buffer_rejects_turn_that_exceeds_pcm_cap() -> None:
    async def go() -> bool:
        task = asyncio.create_task(asyncio.Event().wait())
        conn = {
            "audio_buffers": {
                "t1": {
                    "sample_rate": 16000,
                    "chunks": [],
                    "pcm_bytes": 0,
                    "frame_bytes": 0,
                    "max_pcm_bytes": 3,
                    "started_at": 1.0,
                    "streaming_stt_task": task,
                }
            }
        }

        first = BrainServer._buffer_audio_chunk(
            conn,
            BinaryAudio(kind="uplink_audio", turn_id="t1", pcm=b"aa", sample_rate=16000),
            frame_bytes=10,
        )
        second = BrainServer._buffer_audio_chunk(
            conn,
            BinaryAudio(kind="uplink_audio", turn_id="t1", pcm=b"bb", sample_rate=16000),
            frame_bytes=10,
        )
        await asyncio.sleep(0)

        assert first is True
        assert second is False
        assert "t1" not in conn["audio_buffers"]
        return task.cancelled()

    assert asyncio.run(go()) is True


def test_discard_audio_buffers_cancels_pending_streaming_stt() -> None:
    async def go() -> tuple[bool, dict]:
        task = asyncio.create_task(asyncio.Event().wait())
        conn = {
            "audio_buffers": {
                "t1": {"streaming_stt_task": task},
                "t2": {"streaming_stt_task": None},
            }
        }

        BrainServer._discard_audio_buffers(conn)
        await asyncio.sleep(0)
        return task.cancelled(), conn

    cancelled, conn = asyncio.run(go())

    assert cancelled is True
    assert conn["audio_buffers"] == {}


def test_audio_buffer_accepts_final_frame_at_local_capture_limit() -> None:
    sample_rate = 16000
    frame_bytes = 512 * 2
    local_limit_bytes = sample_rate * 2 * 30
    local_frames_at_limit = local_limit_bytes // frame_bytes + 1
    conn = {
        "audio_buffers": {
            "t1": {
                "sample_rate": sample_rate,
                "chunks": [],
                "pcm_bytes": 0,
                "frame_bytes": 0,
                "max_pcm_bytes": sample_rate * 2 * 31,
                "started_at": 1.0,
            }
        }
    }

    ok = True
    for _ in range(local_frames_at_limit):
        ok = BrainServer._buffer_audio_chunk(
            conn,
            BinaryAudio(
                kind="uplink_audio",
                turn_id="t1",
                pcm=b"x" * frame_bytes,
                sample_rate=sample_rate,
            ),
            frame_bytes=frame_bytes,
        )

    assert ok is True
    assert conn["audio_buffers"]["t1"]["pcm_bytes"] == local_frames_at_limit * frame_bytes

    too_much = BrainServer._buffer_audio_chunk(
        conn,
        BinaryAudio(
            kind="uplink_audio",
            turn_id="t1",
            pcm=b"x" * (sample_rate * 2),
            sample_rate=sample_rate,
        ),
        frame_bytes=sample_rate * 2,
    )
    assert too_much is False
    assert "t1" not in conn["audio_buffers"]


class _TurnTracer:
    def __init__(self) -> None:
        self.emitted: list[dict] = []

    def turn(self, *, room, speaker, channel="voice", device_id="", kind="turn"):  # noqa: ANN001
        return TurnTrace(
            room=room, speaker=speaker, channel=channel, device_id=device_id, kind=kind
        )

    def emit(self, trace: TurnTrace) -> None:
        trace.data["total_ms"] = round(trace.total_ms(), 1)
        self.emitted.append(dict(trace.data))


class _NoAlarmScheduler:
    def ringing_on(self, _device_id: str) -> bool:
        return False


class _CountingSTT:
    def __init__(self) -> None:
        self.calls: list[bytes] = []

    def transcribe(self, pcm: bytes, *, sample_rate: int = 16000) -> str:
        self.calls.append(pcm)
        return pcm.decode("utf-8")


def test_streaming_stt_reuses_exact_partial(cfg) -> None:  # noqa: ANN001
    async def go() -> tuple[str, dict, list[bytes]]:
        server = BrainServer(cfg)
        stt = _CountingSTT()
        server._stt = stt  # type: ignore[assignment]
        trace = TurnTrace(room="default", speaker="house")
        task = asyncio.create_task(server._transcribe_snapshot(b"hello", 16000))
        msg = BufferedAudioTurn(
            turn_id="t1",
            sample_rate=16000,
            pcm=b"hello",
            chunks=1,
            frame_bytes=5,
            stream_ms=10,
            streaming_stt_task=task,
            streaming_stt_pcm_bytes=5,
            streaming_stt_partial_runs=1,
        )

        text, _secs = await server._transcribe_buffered_audio(msg, trace)
        return text, trace.data, stt.calls

    text, data, calls = asyncio.run(go())

    assert text == "hello"
    assert calls == [b"hello"]
    assert data["stages"]["stt"]["streaming"] is True
    assert data["stages"]["stt"]["reused_partial"] is True
    assert data["stages"]["stt_stream"]["partial_runs"] == 1


def test_streaming_stt_falls_back_when_partial_is_stale(cfg) -> None:  # noqa: ANN001
    async def go() -> tuple[str, dict, list[bytes]]:
        server = BrainServer(cfg)
        stt = _CountingSTT()
        server._stt = stt  # type: ignore[assignment]
        trace = TurnTrace(room="default", speaker="house")
        task = asyncio.create_task(server._transcribe_snapshot(b"hel", 16000))
        await task
        msg = BufferedAudioTurn(
            turn_id="t1",
            sample_rate=16000,
            pcm=b"hello",
            chunks=1,
            frame_bytes=5,
            stream_ms=10,
            streaming_stt_task=task,
            streaming_stt_pcm_bytes=3,
            streaming_stt_partial_runs=1,
        )

        text, _secs = await server._transcribe_buffered_audio(msg, trace)
        return text, trace.data, stt.calls

    text, data, calls = asyncio.run(go())

    assert text == "hello"
    assert calls == [b"hel", b"hello"]
    assert data["stages"]["stt"]["streaming"] is True
    assert data["stages"]["stt"]["reused_partial"] is False
    assert data["stages"]["stt_stream"]["pcm_bytes"] == 3
    assert data["stages"]["stt_stream"]["stale"] is True


def test_streaming_stt_abandons_running_stale_snapshot_before_final_stt(cfg) -> None:  # noqa: ANN001
    async def go() -> tuple[bool, bool, str, dict, list[bytes]]:
        server = BrainServer(cfg)
        stt = _CountingSTT()
        server._stt = stt  # type: ignore[assignment]
        trace = TurnTrace(room="default", speaker="house")
        release_snapshot = asyncio.Event()

        async def stale_snapshot() -> dict:
            await release_snapshot.wait()
            return {
                "text": "hel",
                "ms": 12.0,
                "pcm_bytes": 3,
                "audio_s": 0.1,
                "chars": 3,
            }

        task = asyncio.create_task(stale_snapshot())
        msg = BufferedAudioTurn(
            turn_id="t1",
            sample_rate=16000,
            pcm=b"hello",
            chunks=1,
            frame_bytes=5,
            stream_ms=10,
            streaming_stt_task=task,
            streaming_stt_pcm_bytes=3,
            streaming_stt_partial_runs=1,
        )

        running = asyncio.create_task(server._transcribe_buffered_audio(msg, trace))
        await asyncio.sleep(0)
        final_started_before_snapshot_finished = bool(stt.calls)
        release_snapshot.set()
        text, _secs = await running
        snapshot_cancelled = task.cancelled()
        return final_started_before_snapshot_finished, snapshot_cancelled, text, trace.data, stt.calls

    final_started_early, snapshot_cancelled, text, data, calls = asyncio.run(go())

    assert final_started_early is True
    assert snapshot_cancelled is True
    assert text == "hello"
    assert calls == [b"hello"]
    assert "stt_stream" not in data["stages"]
    assert data["events"][0]["name"] == "stt_stream_abandoned"
    assert data["events"][0]["pcm_bytes"] == 3
    assert data["stages"]["stt"]["reused_partial"] is False


class _Contexts:
    def __init__(self, session) -> None:  # noqa: ANN001
        self._session = session

    def get(self, _ctx):  # noqa: ANN001
        return self._session


class _TurnWS:
    def __init__(self, *, fail_binary: bool = False) -> None:
        self.sent: list[str | bytes] = []
        self._fail_binary = fail_binary

    async def send(self, item: str | bytes) -> None:
        if self._fail_binary and isinstance(item, bytes):
            raise OSError("speaker link closed")
        self.sent.append(item)


class _FallbackTTS:
    async def synthesize_stream(self, text: str):  # noqa: ANN202
        yield text.encode()


class _FailingSession:
    def set_voice_mode(self, _mode: str) -> None:
        pass

    async def respond(self, _text, _trace, _result):  # noqa: ANN001
        raise RuntimeError("llm failed")
        if False:  # pragma: no cover - make this an async generator
            yield b""

    def finalize(self, _text, result, _trace=None) -> None:  # noqa: ANN001
        result.reply = result.raw
        result.ended = True


class _AudioSession:
    def set_voice_mode(self, _mode: str) -> None:
        pass

    async def respond(self, _text, _trace, result):  # noqa: ANN001
        result.raw = "This should be spoken."
        yield b"pcm"

    def finalize(self, _text, result, _trace=None) -> None:  # noqa: ANN001
        result.reply = result.raw
        result.ended = True


class _FinalizingAudioSession:
    def set_voice_mode(self, _mode: str) -> None:
        pass

    async def respond(self, _text, _trace, result):  # noqa: ANN001
        try:
            yield b"pcm"
        finally:
            result.raw = "Recovered streamed text."

    def finalize(self, _text, result, _trace=None) -> None:  # noqa: ANN001
        result.reply = result.raw
        result.ended = True


def _turn_server(cfg, session):  # noqa: ANN001
    server = BrainServer(cfg)
    server._scheduler = _NoAlarmScheduler()  # type: ignore[assignment]
    server._contexts = _Contexts(session)  # type: ignore[assignment]
    server._tts = _FallbackTTS()  # type: ignore[assignment]
    server._tracer = _TurnTracer()  # type: ignore[assignment]
    server._resolve = lambda *_args, **_kwargs: RequestContext(  # type: ignore[method-assign]
        "room-pi", "house", "house", frozenset(), channel="voice"
    )
    return server


def test_upstream_turn_error_speaks_fallback_audio(cfg) -> None:  # noqa: ANN001
    server = _turn_server(cfg, _FailingSession())
    ws = _TurnWS()
    conn = {
        "asserted": "",
        "base_asserted": "",
        "device_default": "house",
        "voice_mode": "default",
        "hardware": set(),
    }

    asyncio.run(server._do_turn(ws, "room-pi", "voice", conn, TextIn(turn_id="t1", text="hi")))

    audio = [decode_binary_audio(item) for item in ws.sent if isinstance(item, bytes)]
    text = [decode(item) for item in ws.sent if isinstance(item, str)]
    assert audio and audio[0] is not None
    reply = next(item for item in text if isinstance(item, ReplyText))
    assert "hit an error" in reply.text
    end = next(item for item in text if isinstance(item, ReplyEnd))
    assert end.ended is True
    assert server._tracer.emitted[-1]["reply_audio_chunks"] == 1


def test_downlink_audio_failure_keeps_text_reply_and_marks_missing_audio(cfg) -> None:  # noqa: ANN001
    server = _turn_server(cfg, _AudioSession())
    ws = _TurnWS(fail_binary=True)
    conn = {
        "asserted": "",
        "base_asserted": "",
        "device_default": "house",
        "voice_mode": "default",
        "hardware": set(),
    }

    asyncio.run(server._do_turn(ws, "room-pi", "voice", conn, TextIn(turn_id="t1", text="hi")))

    assert not any(isinstance(item, bytes) for item in ws.sent)
    text = [decode(item) for item in ws.sent]
    assert any(isinstance(item, ReplyText) and item.text == "This should be spoken." for item in text)
    trace = server._tracer.emitted[-1]
    assert trace["reply_audio_chunks"] == 0
    assert any(event["name"] == "reply_audio_missing" for event in trace["events"])
    assert any(event["name"] == "reply_audio_error" for event in trace["events"])


def test_downlink_failure_closes_reply_generator_before_finalize(cfg) -> None:  # noqa: ANN001
    server = _turn_server(cfg, _FinalizingAudioSession())
    ws = _TurnWS(fail_binary=True)
    conn = {
        "asserted": "",
        "base_asserted": "",
        "device_default": "house",
        "voice_mode": "default",
        "hardware": set(),
    }

    asyncio.run(server._do_turn(ws, "room-pi", "voice", conn, TextIn(turn_id="t1", text="hi")))

    text = [decode(item) for item in ws.sent]
    assert any(isinstance(item, ReplyText) and item.text == "Recovered streamed text." for item in text)
    trace = server._tracer.emitted[-1]
    assert trace["reply_audio_chunks"] == 0
    assert any(event["name"] == "reply_audio_error" for event in trace["events"])
