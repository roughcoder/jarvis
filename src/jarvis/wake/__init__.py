"""Wake word — always-on keyword spotting (spec §4, §5 Step 6).

Runs in the PASSIVE state: tiny, local, always-on. Two interchangeable engines
behind one interface (so the state machine never changes):

  - "openwakeword" (default): FOSS, no account. Pretrained "hey_jarvis".
  - "porcupine": Picovoice, best accuracy, needs an AccessKey. Built-in "jarvis".

Both consume the single always-open mic's 512-sample (32ms @16kHz) frames via
process(frame) -> bool.
"""

from __future__ import annotations

from jarvis.config import WakeConfig


class WakeWord:
    def __init__(self, cfg: WakeConfig) -> None:
        self._cfg = cfg
        self._impl = None

    def load(self) -> None:
        if self._impl is not None:
            return
        if self._cfg.engine == "porcupine":
            self._impl = _Porcupine(self._cfg)
        else:
            self._impl = _OpenWakeWord(self._cfg)
        self._impl.load()

    def process(self, frame_int16: bytes) -> bool:
        """True if the wake word is detected in this 512-sample frame."""
        self.load()
        return self._impl.process(frame_int16)

    def reset(self) -> None:
        """Clear internal audio/prediction buffers.

        Critical before reusing the detector for barge-in: otherwise its rolling
        buffer still holds the "Hey Jarvis" that started the turn and re-fires
        immediately. No-op for stateless engines.
        """
        if self._impl is not None:
            self._impl.reset()

    def delete(self) -> None:
        if self._impl is not None:
            self._impl.delete()
            self._impl = None


class _OpenWakeWord:
    """FOSS backend. Pretrained models (e.g. hey_jarvis) over ONNX runtime."""

    def __init__(self, cfg: WakeConfig) -> None:
        self._cfg = cfg
        self._model = None
        self._name: str | None = None

    def load(self) -> None:
        import openwakeword
        from openwakeword.model import Model

        # Downloads the shared feature models + pretrained wakewords on first use.
        try:
            openwakeword.utils.download_models()
        except Exception:
            pass
        self._model = Model(
            wakeword_models=[self._cfg.keyword], inference_framework="onnx"
        )
        # Resolve the actual key the model registered this wakeword under.
        self._name = next(iter(self._model.models.keys()))

    def process(self, frame_int16: bytes) -> bool:
        import numpy as np

        arr = np.frombuffer(frame_int16, dtype=np.int16)
        scores = self._model.predict(arr)
        return scores.get(self._name, 0.0) >= self._cfg.threshold

    def reset(self) -> None:
        if self._model is not None:
            try:
                self._model.reset()
            except Exception:
                pass
            try:
                self._model.preprocessor.reset()
            except Exception:
                pass

    def delete(self) -> None:
        self._model = None


class _Porcupine:
    """Picovoice backend. Built-in "jarvis" keyword (or a custom .ppn)."""

    def __init__(self, cfg: WakeConfig) -> None:
        self._cfg = cfg
        self._porcupine = None

    def load(self) -> None:
        import pvporcupine

        key = self._cfg.access_key.get_secret_value()
        if not key:
            raise RuntimeError(
                "WAKE_ACCESS_KEY is not set (get one at console.picovoice.ai)"
            )
        if self._cfg.keyword_path:
            self._porcupine = pvporcupine.create(
                access_key=key,
                keyword_paths=[self._cfg.keyword_path],
                sensitivities=[self._cfg.sensitivity],
            )
        else:
            self._porcupine = pvporcupine.create(
                access_key=key,
                keywords=[self._cfg.keyword],
                sensitivities=[self._cfg.sensitivity],
            )

    def process(self, frame_int16: bytes) -> bool:
        import struct

        n = self._porcupine.frame_length
        pcm = struct.unpack_from(f"{n}h", frame_int16)
        return self._porcupine.process(pcm) >= 0

    def reset(self) -> None:
        pass  # Porcupine is stateless per frame

    def delete(self) -> None:
        if self._porcupine is not None:
            self._porcupine.delete()
            self._porcupine = None
