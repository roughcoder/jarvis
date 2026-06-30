"""Per-turn pipeline tracing.

Each conversational turn produces one structured trace covering every stage —
STT, LLM, TTS, memory — with timings and metadata, so the latency breakdown
(the felt speed of Jarvis, spec §8) is visible. Traces are appended as JSON
lines to a file and optionally summarised to the console.

The non-LLM external calls (Inworld TTS) can't be seen in the LiteLLM gateway
logs, so this is where they're captured.
"""

from __future__ import annotations

import json
import pathlib
import threading
import time

from jarvis.config import TraceConfig


class TurnTrace:
    """Accumulates timings/metadata for one turn. Cheap; no I/O until emit."""

    def __init__(
        self,
        *,
        room: str,
        speaker: str,
        channel: str = "voice",
        device_id: str = "",
        kind: str = "turn",
    ) -> None:
        self._t0 = time.perf_counter()
        self._starts: dict[str, float] = {}
        # Wall-clock start so turn and cold-path (memory) traces can be lined up
        # on a timeline — e.g. to see if a slow follow-up turn overlapped a
        # cold-path/deriver burst (contention) vs ambient noise.
        self.data: dict = {
            "ts": round(time.time(), 3),
            "kind": kind,
            "room": room,
            "speaker": speaker,
            "channel": channel,
            "device_id": device_id,
            "stages": {},
            "events": [],
        }

    def start(self, stage: str) -> None:
        self._starts[stage] = time.perf_counter()

    def end(self, stage: str, **meta) -> None:
        t = self._starts.pop(stage, time.perf_counter())
        self.data["stages"][stage] = {"ms": round((time.perf_counter() - t) * 1000, 1), **meta}

    def stage(self, name: str, ms: float, **meta) -> None:
        self.data["stages"][name] = {"ms": round(ms, 1), **meta}

    def event(self, name: str, **meta) -> None:
        self.data["events"].append({"name": name, **meta})

    def set(self, **kv) -> None:
        self.data.update(kv)

    def total_ms(self) -> float:
        return (time.perf_counter() - self._t0) * 1000


class Tracer:
    def __init__(self, cfg: TraceConfig) -> None:
        self._cfg = cfg
        self._lock = threading.Lock()

    def turn(
        self,
        *,
        room: str,
        speaker: str,
        channel: str = "voice",
        device_id: str = "",
        kind: str = "turn",
    ) -> TurnTrace:
        return TurnTrace(
            room=room, speaker=speaker, channel=channel, device_id=device_id, kind=kind
        )

    def emit(self, trace: TurnTrace) -> None:
        trace.data["total_ms"] = round(trace.total_ms(), 1)
        if self._cfg.console:
            print("  " + _summary(trace.data))
        if self._cfg.enabled and self._cfg.path:
            path = pathlib.Path(self._cfg.path)
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with self._lock, path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(trace.data, ensure_ascii=False) + "\n")
            except OSError:
                pass  # tracing must never break a turn


def _summary(d: dict) -> str:
    """Compact one-line console summary of a turn trace."""
    s = d.get("stages", {})
    parts = [f"⟐ {d.get('kind', 'turn')}"]
    channel = d.get("channel")
    speaker = d.get("speaker")
    if channel or speaker:
        parts.append(f"{channel or '?'}:{speaker or '?'}")
    if "uplink" in s:
        uplink = s["uplink"]
        chunks = uplink.get("chunks")
        chunk_text = f"/{chunks}ch" if chunks is not None else ""
        parts.append(
            f"uplink[{uplink.get('protocol', '?')}]={uplink.get('audio_s', 0):.1f}s"
            f"/{uplink.get('ms', 0):.0f}ms{chunk_text}"
        )
    if "stt" in s:
        parts.append(f"stt={s['stt']['ms']:.0f}ms")
    if "llm" in s:
        llm = s["llm"]
        parts.append(f"llm[{llm.get('model', '?')}]={llm['ms']:.0f}ms")
    if "tts" in s:
        tts = s["tts"]
        ttfa = tts.get("ttfa_ms")
        parts.append(
            f"tts[{tts.get('voice', '?')}]={tts['ms']:.0f}ms"
            + (f"(ttfa {ttfa:.0f})" if ttfa is not None else "")
        )
    if "memory" in s:
        parts.append(f"mem={s['memory']['ms']:.0f}ms")
    if d.get("events"):
        names = ",".join(e["name"] for e in d["events"])
        parts.append(f"[{names}]")
    parts.append(f"total={d.get('total_ms', 0):.0f}ms")
    return "  ".join(parts)
