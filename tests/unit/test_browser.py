"""Browser lane — gating, registration, doctor, and prompt guidance (no real Chrome).

The live host (nodriver + Chrome) is exercised by an integration test that self-skips
when the extra/binary is absent; these unit tests pin the wiring that must hold
regardless: worker.browser gates every browser tool (deny-by-default), the lane only
registers when enabled, and the guidance is injected only for granted contexts.
"""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.browser.doctor import browser_doctor
from jarvis.config import BrowserConfig, ToolsConfig, WorkerConfig, load_config
from jarvis.tools import build_registry
from jarvis.tools.base import ToolRegistry
from jarvis.tools.browser import make_browser_tools

_BROWSER = {"browser_open", "browser_snapshot", "browser_click", "browser_type", "browser_read"}


def _ctx(*caps: str) -> RequestContext:
    return RequestContext("d", "neil", "personal", frozenset(caps))


def test_browser_tools_gated_by_capability() -> None:
    tools = {t.name: t for t in make_browser_tools(WorkerConfig(_env_file=None), BrowserConfig(_env_file=None))}
    assert set(tools) == _BROWSER
    for t in tools.values():
        assert t.required_capability == "worker.browser"


def test_browser_registered_deny_by_default() -> None:
    reg = build_registry(
        ToolsConfig(_env_file=None), worker=WorkerConfig(_env_file=None), browser=BrowserConfig(_env_file=None)
    )
    assert not _BROWSER & {t.name for t in reg.available_for(_ctx())}  # deny-by-default
    assert _BROWSER <= {t.name for t in reg.available_for(_ctx("worker.browser"))}


def test_browser_disabled_registers_nothing() -> None:
    reg = build_registry(
        ToolsConfig(_env_file=None),
        worker=WorkerConfig(_env_file=None),
        browser=BrowserConfig(_env_file=None, enabled=False),
    )
    assert not _BROWSER & {t.name for t in reg.available_for(_ctx("worker.browser"))}


def test_browser_tool_handles_worker_down() -> None:
    # No worker listening → the tool reports it cleanly, never raises into the turn.
    tools = {
        t.name: t
        for t in make_browser_tools(
            WorkerConfig(_env_file=None, base_url="http://127.0.0.1:1"), BrowserConfig(_env_file=None)
        )
    }
    out = asyncio.run(tools["browser_open"].handler(_ctx("worker.browser"), {"url": "example.com"}))
    assert out.startswith("error:") and "unreachable" in out


def test_browser_doctor_shape() -> None:
    d = browser_doctor(BrowserConfig(_env_file=None))
    assert set(d) >= {"nodriver_installed", "chrome_path", "ready", "next_steps", "default_context"}


def test_browser_guidance_gated() -> None:
    from jarvis.brain.session import _BROWSER_GUIDANCE, BrainSession

    def prompt(*caps: str) -> str:
        s = BrainSession(
            load_config(), _ctx(*caps), gateway=None, tts=None, memory=None, tracer=None, registry=ToolRegistry()
        )
        return s._system_prompt("")

    assert _BROWSER_GUIDANCE in prompt("worker.browser")
    assert _BROWSER_GUIDANCE not in prompt("files.read")
