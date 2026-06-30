"""Intercom-side latency metrics.

Brain traces measure STT/LLM/TTS where that work happens. These metrics measure
the edge-visible part of a voice turn on the intercom: time from utterance send
to first binary reply-audio frame and local playback buffering.
"""

from __future__ import annotations

import json
import pathlib
import time

from jarvis.config import TraceConfig


SCHEMA_VERSION = "jarvis.intercom.playback.v1"


class IntercomReplyMetrics:
    """Accumulate one intercom reply trace and emit it as JSONL."""

    def __init__(self, *, turn_id: str, device_id: str, kind: str = "turn") -> None:
        self._t0 = time.perf_counter()
        self.data: dict = {
            "ts": round(time.time(), 3),
            "kind": "intercom",
            "schema_version": SCHEMA_VERSION,
            "intercom_kind": kind,
            "turn_id": turn_id,
            "device_id": device_id,
            "stages": {},
            "events": [],
        }
        self._audio_chunks = 0
        self._audio_bytes = 0
        self._audio_encoded_bytes = 0
        self._decode_ms = 0.0
        self._first_audio_ms: float | None = None

    def mark_capture(
        self, *, capture_ms: float, audio_ms: float, pcm_bytes: int, streamed: bool
    ) -> None:
        self.data["stages"]["capture"] = {
            "ms": round(capture_ms, 1),
            "audio_ms": round(audio_ms, 1),
            "pcm_bytes": pcm_bytes,
            "streamed": streamed,
        }

    def mark_utterance_sent(
        self,
        *,
        pcm_bytes: int,
        frame_bytes: int,
        protocol: str,
        chunks: int = 1,
    ) -> None:
        self.data["stages"]["utterance_send"] = {
            "ms": 0.0,
            "pcm_bytes": pcm_bytes,
            "frame_bytes": frame_bytes,
            "protocol": protocol,
            "chunks": chunks,
        }

    def mark_proactive_received(self, *, text_chars: int) -> None:
        self.data["stages"]["proactive_start"] = {
            "ms": 0.0,
            "text_chars": text_chars,
        }

    def mark_transcript(self) -> None:
        self.data["stages"]["transcript"] = {"ms": self._elapsed_ms()}

    def mark_audio_frame_seen(self) -> None:
        if self._first_audio_ms is None:
            self._first_audio_ms = self._elapsed_ms()

    def record_audio_frame(
        self,
        *,
        protocol: str,
        encoded_bytes: int,
        pcm_bytes: int,
        decode_ms: float = 0.0,
    ) -> None:
        self.mark_audio_frame_seen()
        self._audio_chunks += 1
        self._audio_bytes += pcm_bytes
        self._audio_encoded_bytes += encoded_bytes
        self._decode_ms += decode_ms
        self.data["stages"]["reply_audio"] = {
            "ms": round(self._first_audio_ms, 1),
            "protocol": protocol,
            "chunks": self._audio_chunks,
            "bytes": self._audio_bytes,
            "encoded_bytes": self._audio_encoded_bytes,
            "decode_ms": round(self._decode_ms, 1),
            "decode_ms_avg": round(self._decode_ms / self._audio_chunks, 3),
        }

    @property
    def reply_audio_chunks(self) -> int:
        return self._audio_chunks

    def mark_missing_reply_audio(self, *, text_chars: int) -> None:
        self.data["events"].append(
            {
                "name": "reply_audio_missing",
                "ms": round(self._elapsed_ms(), 1),
                "text_chars": text_chars,
            }
        )

    def attach_playback(self, playback) -> None:  # noqa: ANN001 - PlaybackMetrics
        if playback is None:
            return
        self.data["stages"]["playback"] = playback.as_dict()
        self.data["total_ms"] = round(self._elapsed_ms(), 1)

    def emit(self, cfg: TraceConfig) -> None:
        self.data.setdefault("total_ms", round(self._elapsed_ms(), 1))
        if cfg.console:
            print("  " + summary(self.data))
        if not (cfg.enabled and cfg.path):
            return
        path = pathlib.Path(cfg.path)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(self.data, ensure_ascii=False) + "\n")
        except OSError:
            pass  # metrics must never break a turn

    def _elapsed_ms(self) -> float:
        return (time.perf_counter() - self._t0) * 1000


def summary(d: dict) -> str:
    stages = d.get("stages", {})
    utterance = stages.get("utterance_send", {})
    audio = stages.get("reply_audio", {})
    playback = stages.get("playback", {})
    parts = [
        f"⟐ intercom/{d.get('intercom_kind', '?')}",
        f"device={d.get('device_id', '?')}",
    ]
    if utterance:
        parts.append(
            f"uplink={utterance.get('protocol', '?')}/"
            f"{utterance.get('chunks', 0)}ch "
            f"{utterance.get('frame_bytes', 0)}B"
        )
    if audio:
        parts.append(
            f"first_frame={audio.get('ms', 0):.0f}ms "
            f"audio={audio.get('protocol', '?')} "
            f"decode={audio.get('decode_ms', 0):.1f}ms/{audio.get('chunks', 0)}ch"
        )
    if any(e.get("name") == "reply_audio_missing" for e in d.get("events", [])):
        parts.append("NO_REPLY_AUDIO")
    if playback:
        first_speech = playback.get("first_speech_ms")
        speech = f"{first_speech:.0f}ms" if first_speech is not None else "?"
        parts.append(
            f"speech={speech} "
            f"underruns={playback.get('underruns', 0)} "
            f"block={playback.get('block_ms', 0):.0f}ms"
        )
    parts.append(f"total={d.get('total_ms', 0):.0f}ms")
    return "  ".join(parts)
