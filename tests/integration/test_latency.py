"""Integration: per-turn latency budgets (the "felt speed is the product" guard).

Reads the most recent real turn from the trace log and asserts its stage timings
are within generous budgets — a regression tripwire, not a tight SLA. Skips when
no traces exist yet (run `jarvis run` first). Budgets are deliberately loose;
tighten as the hardware/target settles.
"""

from __future__ import annotations

import json
import pathlib

import pytest

from jarvis.config import load_config

pytestmark = pytest.mark.integration

BUDGET_MS = {"stt": 3000, "llm": 5000, "tts": 3000, "total": 12000}


def test_recent_turn_within_latency_budget() -> None:
    cfg = load_config()
    path = pathlib.Path(cfg.trace.path)
    if not path.exists():
        pytest.skip(f"no traces at {path} — run `jarvis run` first")

    rows = [json.loads(ln) for ln in path.read_text().splitlines() if ln.strip()]
    turns = [
        r for r in rows
        if r.get("kind", "turn") == "turn" and "stt" in r.get("stages", {})
    ]
    if not turns:
        pytest.skip("no turn traces recorded yet")

    last = turns[-1]
    stages = last["stages"]
    assert stages["stt"]["ms"] < BUDGET_MS["stt"]
    if "llm" in stages:
        assert stages["llm"]["ms"] < BUDGET_MS["llm"]
    if "tts" in stages:
        assert stages["tts"]["ms"] < BUDGET_MS["tts"]
    assert last.get("total_ms", 0) < BUDGET_MS["total"]
