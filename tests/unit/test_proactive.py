"""Proactive voice delivery — tone generation + the frame builder (no real TTS)."""

from __future__ import annotations

import asyncio

from jarvis.brain.proactive import proactive_frames
from jarvis.brain.tones import make_tone
from jarvis.protocol.messages import Proactive, ReplyAudio, ReplyEnd, decode


class _FakeTTS:
    def __init__(self, chunks) -> None:
        self._chunks = chunks

    async def synthesize_stream(self, text: str):  # noqa: ANN202
        for c in self._chunks:
            yield c


def test_make_tone_generates_pcm() -> None:
    pcm = make_tone(16000, sound="chime", freq=880.0, seconds=0.5)
    assert isinstance(pcm, bytes) and len(pcm) > 1000  # ~0.5s of 16-bit PCM


def test_make_tone_bad_path_falls_back_to_generated() -> None:
    pcm = make_tone(16000, sound="/no/such/file.wav", freq=880.0)
    assert isinstance(pcm, bytes) and len(pcm) > 0  # never raises; generated fallback


def test_proactive_frames_header_audio_end() -> None:
    frames = asyncio.run(
        proactive_frames(
            _FakeTTS([b"\x01\x02", b"\x03\x04"]), 16000, "your tea is ready",
            turn_id="pa-1", kind="notification", open_mic=True, speak=True, tone=True,
        )
    )
    msgs = [decode(f) for f in frames]
    assert isinstance(msgs[0], Proactive)
    assert msgs[0].kind == "notification" and msgs[0].open_mic is True and msgs[0].turn_id == "pa-1"
    audio = [m for m in msgs if isinstance(m, ReplyAudio)]
    assert len(audio) == 3  # 1 tone + 2 speech chunks
    assert all(m.turn_id == "pa-1" for m in audio)
    assert isinstance(msgs[-1], ReplyEnd) and msgs[-1].turn_id == "pa-1"


def test_proactive_frames_tone_only_no_speech() -> None:
    # an alarm repeat: tone, no spoken label
    frames = asyncio.run(
        proactive_frames(_FakeTTS([b"x"]), 16000, "Alarm.", turn_id="pa-2", kind="alarm", speak=False, tone=True)
    )
    msgs = [decode(f) for f in frames]
    assert [type(m).__name__ for m in msgs] == ["Proactive", "ReplyAudio", "ReplyEnd"]
    assert msgs[0].kind == "alarm"


def test_proactive_frames_text_only_no_audio() -> None:
    frames = asyncio.run(
        proactive_frames(None, 16000, "hi", turn_id="pa-3", speak=False, tone=False)
    )
    msgs = [decode(f) for f in frames]
    assert [type(m).__name__ for m in msgs] == ["Proactive", "ReplyEnd"]
