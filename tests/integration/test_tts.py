"""Integration: real streaming TTS (formalises `jarvis say [--stop-after]`).

Two checks:
  - synthesis streams PCM (needs TTS_API_KEY + network, no speaker).
  - hard-stop cuts playback fast (needs an audio device; skips without one).
"""

from __future__ import annotations

import asyncio

import pytest

from jarvis.config import load_config

pytestmark = pytest.mark.integration


def _cfg_with_key():
    cfg = load_config()
    if not cfg.tts.api_key.get_secret_value():
        pytest.skip("TTS_API_KEY not set")
    return cfg


def test_tts_stream_yields_audio() -> None:
    cfg = _cfg_with_key()
    from jarvis.services.tts import InworldTTS

    tts = InworldTTS(cfg.tts)

    async def run() -> int:
        total = 0
        async for chunk in tts.synthesize_stream("Hello, this is a short test."):
            total += len(chunk)
        return total

    assert asyncio.run(run()) > 0


def test_tts_hard_stop_cuts_fast() -> None:
    cfg = _cfg_with_key()
    from jarvis.intercom.audio import AudioIO
    from jarvis.services.tts import InworldTTS

    try:
        audio = AudioIO(cfg.audio)
    except Exception as exc:  # noqa: BLE001 - no audio device in this environment
        pytest.skip(f"no audio device: {exc}")
    tts = InworldTTS(cfg.tts)

    async def run():
        play = asyncio.create_task(
            audio.play_stream(
                tts.synthesize_stream(
                    "One two three four five six seven eight nine ten eleven twelve."
                ),
                sample_rate=cfg.tts.sample_rate,
            )
        )
        await asyncio.sleep(1.0)
        audio.stop_playback()
        await play
        return audio.last_cut_latency_ms

    cut = asyncio.run(run())
    if cut is None:
        pytest.skip("no cut latency measured (playback ended before stop)")
    assert cut < 250  # spec target < 100ms; generous guard against regression
