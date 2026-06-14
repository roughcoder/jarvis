"""Endpointer — VAD endpointing logic (when has the user finished talking?).

Pure stateful logic: feed (frame, speech_prob) and it decides start/end. No
torch needed (only SileroVAD loads the model); we drive probabilities directly.
"""

from __future__ import annotations

from jarvis.intercom.vad import Endpointer

FRAME = bytes(1024)  # 512 int16 samples; content irrelevant to the logic


def _ep(**over) -> Endpointer:
    kw = dict(
        frame_ms=32.0,
        endpoint_silence_ms=900,
        speech_threshold=0.5,
        min_speech_ms=200,
        preroll_ms=300,
    )
    kw.update(over)
    return Endpointer(**kw)


def test_does_not_start_on_silence() -> None:
    ep = _ep()
    for _ in range(20):
        assert ep.feed(FRAME, 0.0) is False
    assert ep.started is False
    assert ep.audio == b""


def test_onset_captures_preroll_and_starts() -> None:
    ep = _ep()
    for _ in range(3):  # buffered as pre-roll so onset isn't clipped
        ep.feed(FRAME, 0.0)
    ep.feed(FRAME, 0.9)  # speech onset
    assert ep.started is True
    assert len(ep.audio) == 4 * len(FRAME)  # 3 pre-roll frames + the onset frame


def test_endpoints_after_min_speech_then_trailing_silence() -> None:
    ep = _ep()
    ep.feed(FRAME, 0.9)  # onset (speech_ms starts accumulating next frames)
    for _ in range(7):  # 7 * 32ms = 224ms > min_speech_ms(200)
        assert ep.feed(FRAME, 0.9) is False
    fired_at = None
    for i in range(1, 41):
        if ep.feed(FRAME, 0.1):  # trailing silence
            fired_at = i
            break
    assert fired_at is not None
    assert 27 <= fired_at <= 31  # ~900ms / 32ms ≈ 29 frames of silence


def test_no_endpoint_until_min_speech_met() -> None:
    # Lots of silence but barely any speech → never endpoints (avoids cutting on
    # a cough/throat-clear before the user actually speaks).
    ep = _ep(min_speech_ms=5000)
    ep.feed(FRAME, 0.9)  # onset
    ep.feed(FRAME, 0.9)  # only ~32ms of speech
    for _ in range(60):  # ~1.9s of silence, well past endpoint_silence_ms
        assert ep.feed(FRAME, 0.1) is False
