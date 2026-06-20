"""Alarm tools — gating + set/cancel/list against a real Scheduler (no brain)."""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.brain.scheduler import Scheduler
from jarvis.config import load_config
from jarvis.tools.alarm import make_alarm_tools

_NAMES = {"set_alarm", "cancel_alarm", "list_alarms"}


def _ctx(*caps: str, device: str = "mac") -> RequestContext:
    return RequestContext(device, "neil", "personal", frozenset(caps))


def _tools():  # noqa: ANN202
    sched = Scheduler()
    tools = {t.name: t for t in make_alarm_tools(sched, load_config())}
    return sched, tools


def test_alarm_tools_gated() -> None:
    _sched, tools = _tools()
    assert set(tools) == _NAMES
    for t in tools.values():
        assert t.required_capability == "alarms.set"


def test_set_timer_seconds_schedules_it() -> None:
    sched, tools = _tools()
    out = asyncio.run(tools["set_alarm"].handler(_ctx("alarms.set"), {"seconds": 5, "label": "tea"}))
    assert "tea" in out and "stop" in out.lower()
    assert len(sched.all()) == 1
    a = sched.all()[0]
    assert a.device_id == "mac" and a.label == "tea"


def test_set_alarm_requires_a_time() -> None:
    _sched, tools = _tools()
    out = asyncio.run(tools["set_alarm"].handler(_ctx("alarms.set"), {"label": "x"}))
    assert out.startswith("error")


def test_set_alarm_at_clock_time() -> None:
    sched, tools = _tools()
    asyncio.run(tools["set_alarm"].handler(_ctx("alarms.set"), {"at": "10:20", "label": "call"}))
    assert len(sched.all()) == 1


def test_cancel_and_list() -> None:
    sched, tools = _tools()
    asyncio.run(tools["set_alarm"].handler(_ctx("alarms.set"), {"minutes": 5, "label": "tea"}))
    listing = asyncio.run(tools["list_alarms"].handler(_ctx("alarms.set"), {}))
    assert "tea" in listing
    out = asyncio.run(tools["cancel_alarm"].handler(_ctx("alarms.set"), {"which": "tea"}))
    assert "tea" in out
    assert sched.all() == []
    # a device only sees/cancels its own
    assert "No alarms" in asyncio.run(tools["list_alarms"].handler(_ctx("alarms.set"), {}))
