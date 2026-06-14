"""Integration: local STT model loads and transcribes (formalises `jarvis listen`).

First run downloads the Faster-Whisper model, so this is opt-in. Silence in →
a string out (empty/near-empty); the point is the model loads and runs without
crashing on the configured device.
"""

from __future__ import annotations

import pytest

from jarvis.config import load_config

pytestmark = pytest.mark.integration


def test_stt_loads_and_transcribes_silence() -> None:
    cfg = load_config()
    from jarvis.stt import Transcriber

    stt = Transcriber(cfg.stt)
    stt.load()  # downloads the model on first run

    sr = cfg.audio.sample_rate
    pcm = bytes(2 * sr)  # ~1s of 16-bit mono silence
    out = stt.transcribe(pcm, sample_rate=sr)
    assert isinstance(out, str)
