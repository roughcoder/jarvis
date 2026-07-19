"""Generated alarm/notification tones as 16-bit PCM.

Brain-side (TTS lives here, so audio is synthesised here and streamed to the thin
intercom). A generated tone keeps the sound trivially changeable — tweak the
frequency in config, or point `ALARM_SOUND` at a file later. `sound="<path>.wav"`
loads a file instead of generating.
"""

from __future__ import annotations

import array
import math
import pathlib
import sys


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
    ms = int(seconds * 1000)
    if double:  # two-tone "ding-dong" — friendlier than a flat beep
        half = ms // 2
        samples = _beep_samples(sample_rate, freq, half)
        samples.extend([0] * int(sample_rate * 0.04))
        samples.extend(_beep_samples(sample_rate, freq * 1.335, half))
    else:
        samples = _beep_samples(sample_rate, freq, ms)
    return _pcm_bytes(samples)


def _beep_samples(sample_rate: int, freq: float, ms: int, amp: float = 0.5) -> array.array[int]:
    count = max(1, int(sample_rate * ms / 1000))
    fade = min(count // 2 or 1, max(1, int(sample_rate * 0.01)))  # 10ms fades kill clicks
    samples: array.array[int] = array.array("h")
    for index in range(count):
        env = 1.0
        if index < fade:
            env = index / fade
        elif index >= count - fade:
            env = (count - index - 1) / fade
        value = amp * math.sin(2 * math.pi * freq * index / sample_rate) * max(0.0, env)
        samples.append(int(max(-1.0, min(1.0, value)) * 32767))
    return samples


def _pcm_bytes(samples: array.array[int]) -> bytes:
    if sys.byteorder != "little":
        samples = array.array(samples.typecode, samples)
        samples.byteswap()
    return samples.tobytes()
