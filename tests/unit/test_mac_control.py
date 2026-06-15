"""Mac GUI control via the worker (Phase 3c) — gating + graceful no-peekaboo."""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.config import ToolsConfig, WorkerConfig
from jarvis.tools import build_registry
from jarvis.tools.worker import make_worker_tools
from jarvis.worker.actions import gui_doctor, run_peekaboo


def _ctx(*caps: str) -> RequestContext:
    return RequestContext("mac", "neil", "personal", frozenset(caps))


def test_gui_tools_gated_by_worker_gui() -> None:
    cfg = WorkerConfig(_env_file=None)
    tools = {t.name: t for t in make_worker_tools(cfg)}
    assert tools["see_screen"].required_capability == "worker.gui"
    assert tools["control_mac"].required_capability == "worker.gui"

    reg = build_registry(ToolsConfig(_env_file=None), worker=cfg)
    assert "control_mac" not in {t.name for t in reg.available_for(_ctx())}  # deny-by-default
    assert "control_mac" in {t.name for t in reg.available_for(_ctx("worker.gui"))}


def test_gui_doctor_reports_missing_binary() -> None:
    d = gui_doctor("peekaboo-does-not-exist")
    assert d["peekaboo_installed"] is False
    assert "install" in d["next_steps"].lower()


def test_run_peekaboo_absent_is_graceful() -> None:
    out = asyncio.run(run_peekaboo("peekaboo-does-not-exist", ["see"], 5.0))
    assert "isn't set up" in out
