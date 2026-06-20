"""Generated alarm/notification tones as 16-bit PCM.

Brain-side (TTS lives here, so audio is synthesised here and streamed to the thin
intercom). A generated tone keeps the sound trivially changeable — tweak the
frequency in config, or point `ALARM_SOUND` at a file later. `sound="<path>.wav"`
loads a file instead of generating.
"""

from __future__ import annotations

import pathlib


def make_tone(sample_rate: int, *, sound: str = "chime", freq: float = 880.0, seconds: float = 0.8) -> bytes:
    """Return PCM for the alarm/notification sound. If `sound` is a readable file path,
    load it (raw 16-bit mono PCM or .wav); otherwise generate a tone. Never raises — a
    bad path falls back to a generated tone."""
    if sound and sound not in ("chime", "tone", "beep"):
        pcm = _load_file(sound, sample_rate)
        if pcm is not None:
            return pcm
    return _generate(sample_rate, freq, seconds, double=(sound != "tone"))


def _load_file(path: str, sample_rate: int) -> bytes | None:
    p = pathlib.Path(path)
    if not p.exists():
        return None
    try:
        if p.suffix.lower() == ".wav":
            import wave

            with wave.open(str(p), "rb") as w:
                return w.readframes(w.getnframes())
        return p.read_bytes()  # assume raw 16-bit PCM at the player's rate
    except Exception:  # noqa: BLE001 - bad file → generated fallback
        return None


def _generate(sample_rate: int, freq: float, seconds: float, *, double: bool) -> bytes:
    import numpy as np

    def beep(f: float, ms: int, amp: float = 0.5):  # noqa: ANN202
        t = np.linspace(0, ms / 1000, int(sample_rate * ms / 1000), False)
        tone = amp * np.sin(2 * np.pi * f * t)
        fade = max(1, int(sample_rate * 0.01))  # 10ms fades kill clicks
        env = np.ones_like(tone)
        env[:fade] = np.linspace(0, 1, fade)
        env[-fade:] = np.linspace(1, 0, fade)
        return tone * env

    ms = int(seconds * 1000)
    if double:  # two-tone "ding-dong" — friendlier than a flat beep
        half = ms // 2
        gap = np.zeros(int(sample_rate * 0.04))
        buf = np.concatenate([beep(freq, half), gap, beep(freq * 1.335, half)])
    else:
        buf = beep(freq, ms)
    return (np.clip(buf, -1, 1) * 32767).astype(np.int16).tobytes()
