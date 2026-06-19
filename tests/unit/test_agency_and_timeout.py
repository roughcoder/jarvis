"""Agency persona + per-tool timeout (the 'do it manually' / empty-error fixes).

Two regressions from a real session:
- control_mac was killed at the global 8s tool guard; a bare asyncio timeout
  stringifies to '' so the model saw `error:` and gave up. Now slow tools carry
  their own budget and a timeout is reported legibly.
- the assistant kept deferring to manual ('call them'); the agency fragment is
  injected so it acts by default and only stops at a genuine blocker.
"""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.brain.session import _AGENCY, _BACKGROUND_GUIDANCE, BrainSession
from jarvis.config import WorkerConfig, load_config
from jarvis.tools.base import Tool, ToolError, ToolRegistry
from jarvis.tools.worker import make_worker_tools


def _ctx(*caps: str) -> RequestContext:
    return RequestContext("d", "house", "house", frozenset(caps))


# --- timeout legibility + per-tool override --------------------------------

def _slow_registry(timeout_s: float | None) -> ToolRegistry:
    async def slow(ctx, args):  # noqa: ANN001
        await asyncio.sleep(0.1)
        return "done"

    reg = ToolRegistry()
    reg.register(
        Tool("slow", "d", {"type": "object", "properties": {}}, "x", slow, timeout_s=timeout_s)
    )
    return reg


def test_timeout_raises_legible_message_not_empty() -> None:
    reg = _slow_registry(timeout_s=None)  # falls back to the (tiny) default guard

    async def go() -> str:
        try:
            await reg.execute(_ctx("x"), "slow", {}, timeout_s=0.01)
        except ToolError as exc:
            return str(exc)
        return "no error"

    msg = asyncio.run(go())
    assert "slow timed out after" in msg  # the bug was an EMPTY 'error:'


def test_per_tool_timeout_override_wins() -> None:
    reg = _slow_registry(timeout_s=1.0)  # tool needs ~0.1s; override gives it room
    # The default guard (0.01s) would cut it, but the tool's own budget takes over.
    out = asyncio.run(reg.execute(_ctx("x"), "slow", {}, timeout_s=0.01))
    assert out == "done"


def test_control_mac_carries_a_slow_budget() -> None:
    cfg = WorkerConfig(_env_file=None)
    tools = {t.name: t for t in make_worker_tools(cfg)}
    assert tools["control_mac"].timeout_s == cfg.peekaboo_agent_timeout_s + 15
    assert tools["control_mac"].timeout_s > 8.0  # past the hot-path guard
    assert tools["run_shell"].timeout_s == cfg.request_timeout_s + 5


# --- agency persona injection ----------------------------------------------

def _prompt(*caps: str) -> str:
    cfg = load_config()
    s = BrainSession(
        cfg, _ctx(*caps), gateway=None, tts=None, memory=None, tracer=None, registry=ToolRegistry()
    )
    return s._system_prompt("")


def test_agency_always_injected() -> None:
    assert _AGENCY in _prompt()  # present even with no capabilities
    assert _AGENCY in _prompt("worker.gui")


def test_background_guidance_gated_on_capability() -> None:
    assert _BACKGROUND_GUIDANCE in _prompt("background.run")
    assert _BACKGROUND_GUIDANCE not in _prompt("worker.gui")  # not without the grant
