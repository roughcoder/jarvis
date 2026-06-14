"""VAD — Silero voice activity detection (spec §4, §5).

ONE instance drives two jobs depending on state (spec §5):
  - ACTIVE LISTENING: endpointing (detect when the user has FINISHED talking).
  - SPEAKING: barge-in (detect when the user STARTS talking over Jarvis).

Silero operates on 16kHz mono audio in fixed 512-sample (32ms) frames and
returns a speech probability [0,1] per frame. Keep streaming frames through the
same instance; call reset() between independent utterances.
"""

from __future__ import annotations

from jarvis.config import VADConfig

# Silero v5 requires exactly 512 samples per call at 16kHz.
FRAME_SAMPLES_16K = 512


class SileroVAD:
    def __init__(self, cfg: VADConfig) -> None:
        self._cfg = cfg
        self._model = None
        self._torch = None

    def load(self) -> None:
        if self._model is not None:
            return
        import torch
        from silero_vad import load_silero_vad

        self._torch = torch
        self._model = load_silero_vad()

    def reset(self) -> None:
        """Clear recurrent state before a new, independent utterance."""
        if self._model is not None:
            self._model.reset_states()

    def prob(self, frame_int16: bytes) -> float:
        """Speech probability for one 512-sample (16kHz, 16-bit mono) frame."""
        self.load()
        import numpy as np

        arr = np.frombuffer(frame_int16, dtype=np.int16)
        if len(arr) != FRAME_SAMPLES_16K:
            raise ValueError(
                f"Silero needs {FRAME_SAMPLES_16K} samples/frame, got {len(arr)}"
            )
        tensor = self._torch.from_numpy(arr.astype("float32") / 32768.0)
        return float(self._model(tensor, 16000).item())


class Endpointer:
    """Stateful endpointing: feed it (frame, speech_prob) and it tells you when
    the user has finished — first speech (with pre-roll so onset isn't clipped),
    then `endpoint_silence_ms` of trailing silence after `min_speech_ms` spoken.
    """

    def __init__(
        self,
        *,
        frame_ms: float,
        endpoint_silence_ms: int,
        speech_threshold: float,
        min_speech_ms: int,
        preroll_ms: int = 300,
    ) -> None:
        import collections

        self._frame_ms = frame_ms
        self._endpoint_silence_ms = endpoint_silence_ms
        self._threshold = speech_threshold
        self._min_speech_ms = min_speech_ms
        self._preroll: collections.deque = collections.deque(
            maxlen=max(1, int(preroll_ms / frame_ms))
        )
        self._captured = bytearray()
        self.started = False
        self._silence_ms = 0.0
        self._speech_ms = 0.0

    def feed(self, frame: bytes, prob: float) -> bool:
        """Returns True when the endpoint (end of utterance) is reached."""
        if not self.started:
            self._preroll.append(frame)
            if prob >= self._threshold:
                self.started = True
                self._captured.extend(b"".join(self._preroll))
            return False
        self._captured.extend(frame)
        if prob >= self._threshold:
            self._silence_ms = 0.0
            self._speech_ms += self._frame_ms
        else:
            self._silence_ms += self._frame_ms
        return (
            self._speech_ms >= self._min_speech_ms
            and self._silence_ms >= self._endpoint_silence_ms
        )

    @property
    def audio(self) -> bytes:
        return bytes(self._captured)
