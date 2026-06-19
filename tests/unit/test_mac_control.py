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


# GUI control is agent-only now: control_mac (act) + look_at_screen (read). The atomic
# peekaboo tools were removed so the model can't bypass the agent and flail.
_GUI = {"control_mac", "look_at_screen"}
_REMOVED = {"see_screen", "describe_screen", "list_apps", "launch_app", "click", "type_text", "press_keys"}


def test_gui_tools_gated_by_worker_gui() -> None:
    cfg = WorkerConfig(_env_file=None)
    tools = {t.name: t for t in make_worker_tools(cfg)}
    assert _GUI <= set(tools)
    assert not _REMOVED & set(tools)  # the direct-peekaboo tools are gone
    for name in _GUI:
        assert tools[name].required_capability == "worker.gui"

    reg = build_registry(ToolsConfig(_env_file=None), worker=cfg)
    assert not _GUI & {t.name for t in reg.available_for(_ctx())}  # deny-by-default
    assert _GUI <= {t.name for t in reg.available_for(_ctx("worker.gui"))}


def test_agent_task_authorisation_clause() -> None:
    from jarvis.tools.worker import _agent_task

    autonomous = _agent_task("leave the Discord group", True)
    assert autonomous.startswith("leave the Discord group")
    assert "authorised" in autonomous.lower() and "confirm" in autonomous.lower()
    # opt-out leaves the task untouched (agent may pause + ask; Jarvis relays it)
    assert _agent_task("leave the Discord group", False) == "leave the Discord group"


def test_gui_doctor_reports_missing_binary() -> None:
    d = gui_doctor("peekaboo-does-not-exist")
    assert d["peekaboo_installed"] is False
    assert "install" in d["next_steps"].lower()


def test_run_peekaboo_absent_is_graceful() -> None:
    out = asyncio.run(run_peekaboo("peekaboo-does-not-exist", ["see"], 5.0))
    assert "isn't set up" in out


def test_gui_guidance_only_when_worker_gui_granted() -> None:
    from jarvis.brain.session import _GUI_GUIDANCE, BrainSession
    from jarvis.config import load_config
    from jarvis.tools.base import ToolRegistry

    cfg = load_config()

    def prompt(*caps: str) -> str:
        s = BrainSession(
            cfg, _ctx(*caps), gateway=None, tts=None, memory=None, tracer=None, registry=ToolRegistry()
        )
        return s._system_prompt("")

    assert _GUI_GUIDANCE in prompt("worker.gui")  # operating manual present
    assert _GUI_GUIDANCE not in prompt("files.read")  # not for non-GUI contexts
