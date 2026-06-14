"""Per-turn tracing — accumulation, JSONL emit, and console summary.

Tracing must never break a turn (best-effort), and the trace shape is what
`jarvis traces` and the latency budgets read, so its structure is contract.
"""

from __future__ import annotations

import json

from jarvis.config import TraceConfig
from jarvis.brain.tracing import Tracer, TurnTrace, _summary


def test_turntrace_accumulates_stages_events_and_meta() -> None:
    t = TurnTrace(room="kitchen", speaker="neil")
    t.start("stt")
    t.end("stt", chars=10)
    t.stage("llm", 123.4, model="fast")
    t.event("barge_in")
    t.set(kind="turn")

    assert t.data["room"] == "kitchen"
    assert t.data["stages"]["stt"]["chars"] == 10
    assert t.data["stages"]["llm"]["ms"] == 123.4
    assert t.data["stages"]["llm"]["model"] == "fast"
    assert t.data["events"][0]["name"] == "barge_in"
    assert t.data["kind"] == "turn"
    assert t.total_ms() >= 0


def test_emit_writes_one_jsonl_line(tmp_path) -> None:
    cfg = TraceConfig(
        _env_file=None, path=str(tmp_path / "tr.jsonl"), console=False, enabled=True
    )
    tracer = Tracer(cfg)
    t = tracer.turn(room="r", speaker="s")
    t.stage("stt", 10.0)
    tracer.emit(t)

    lines = (tmp_path / "tr.jsonl").read_text().splitlines()
    assert len(lines) == 1
    d = json.loads(lines[0])
    assert d["room"] == "r"
    assert d["speaker"] == "s"
    assert "total_ms" in d


def test_emit_disabled_writes_nothing(tmp_path) -> None:
    cfg = TraceConfig(
        _env_file=None, path=str(tmp_path / "tr.jsonl"), console=False, enabled=False
    )
    tracer = Tracer(cfg)
    tracer.emit(tracer.turn(room="r", speaker="s"))
    assert not (tmp_path / "tr.jsonl").exists()


def test_summary_renders_stage_breakdown() -> None:
    d = {
        "kind": "turn",
        "stages": {
            "stt": {"ms": 800},
            "llm": {"ms": 1200, "model": "fast"},
            "tts": {"ms": 600, "ttfa_ms": 450, "voice": "Ashley"},
        },
        "events": [{"name": "barge_in"}],
        "total_ms": 2600,
    }
    s = _summary(d)
    assert "stt=800ms" in s
    assert "llm[fast]=1200ms" in s
    assert "tts[Ashley]=600ms" in s
    assert "barge_in" in s
    assert "total=2600ms" in s
