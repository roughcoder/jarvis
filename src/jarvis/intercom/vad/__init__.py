"""VAD — voice activity detection (spec §4, §5).

ONE instance drives two jobs depending on state (spec §5):
  - ACTIVE LISTENING: endpointing (detect when the user has FINISHED talking).
  - SPEAKING: barge-in (detect when the user STARTS talking over Jarvis).

Backends return a speech probability [0,1] per frame. Keep streaming frames
through the same instance; call reset() between independent utterances.
"""

from __future__ import annotations

from jarvis.config import VADConfig

# Silero v5 requires exactly 512 samples per call at 16kHz.
FRAME_SAMPLES_16K = 512


class _EnergyVAD:
    """Fallback for test/dev environments without the optional webrtcvad wheel."""

    def is_speech(self, frame_int16: bytes, _sample_rate: int) -> bool:
        if not frame_int16:
            return False
        sample_count = len(frame_int16) // 2
        if sample_count == 0:
            return False
        total = 0
        for offset in range(0, len(frame_int16) - 1, 2):
            sample = int.from_bytes(frame_int16[offset : offset + 2], "little", signed=True)
            total += abs(sample)
        return (total / sample_count) > 500


class SileroVAD:
    def __init__(self, cfg: VADConfig) -> None:
        self._cfg = cfg
        self._model = None
        self._torch = None
        self._webrtc = None

    def load(self) -> None:
        if self._cfg.engine == "webrtc":
            if self._webrtc is not None:
                return
            try:
                import webrtcvad
            except ModuleNotFoundError:
                self._webrtc = _EnergyVAD()
            else:
                self._webrtc = webrtcvad.Vad(
                    max(0, min(3, int(self._cfg.webrtc_aggressiveness)))
                )
            return

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
        """Speech probability for one 16kHz, 16-bit mono frame."""
        self.load()
        if self._cfg.engine == "webrtc":
            return self._webrtc_prob(frame_int16)

        import numpy as np

        arr = np.frombuffer(frame_int16, dtype=np.int16)
        if len(arr) != FRAME_SAMPLES_16K:
            raise ValueError(
                f"Silero needs {FRAME_SAMPLES_16K} samples/frame, got {len(arr)}"
            )
        tensor = self._torch.from_numpy(arr.astype("float32") / 32768.0)
        return float(self._model(tensor, 16000).item())

    def _webrtc_prob(self, frame_int16: bytes) -> float:
        # WebRTC VAD accepts 10/20/30ms frames. Jarvis currently captures 32ms
        # frames for Silero/OpenWakeWord, so evaluate the first valid 30ms slice.
        sample_rate = 16000
        valid_lengths = (sample_rate * ms // 1000 * 2 for ms in (30, 20, 10))
        for byte_len in valid_lengths:
            if len(frame_int16) >= byte_len:
                is_speech = self._webrtc.is_speech(
                    frame_int16[:byte_len], sample_rate
                )
                return 1.0 if is_speech else 0.0
        raise ValueError("WebRTC VAD needs at least 10ms of 16kHz PCM audio")


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
