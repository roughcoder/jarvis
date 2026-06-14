"""STT — local Faster-Whisper transcription (spec §4, Step 3).

Runs on `hive` only when active (gated behind push-to-key now, the wake word
later), never 24/7. English-only model. CTranslate2 has no Metal backend, so on
Apple Silicon this runs on CPU with int8 — fast enough for one speaker.
"""

from __future__ import annotations

from jarvis.config import STTConfig


class Transcriber:
    def __init__(self, cfg: STTConfig) -> None:
        self._cfg = cfg
        self._model = None

    def load(self) -> None:
        """Load the model once (downloads on first use, then HF-cached)."""
        if self._model is not None:
            return
        from faster_whisper import WhisperModel

        device = self._cfg.device
        if device == "auto":
            device = "cpu"  # CT2 whisper has no Metal/GPU path on macOS
        self._model = WhisperModel(
            self._cfg.model, device=device, compute_type=self._cfg.compute_type
        )

    def transcribe(self, pcm_int16: bytes, *, sample_rate: int = 16000) -> str:
        """Transcribe 16-bit mono PCM (assumed 16kHz) to text."""
        self.load()
        import numpy as np

        audio = np.frombuffer(pcm_int16, dtype=np.int16).astype(np.float32) / 32768.0
        segments, _info = self._model.transcribe(
            audio,
            language=self._cfg.language,
            vad_filter=False,
        )
        return "".join(seg.text for seg in segments).strip()
