"""BrainSession prompt assembly — the injected 'now' line (Phase 3 time/date).

Jarvis must know the current date/time from the prompt (no tool, no search), and
it must be the most-volatile, last part of the system prompt so the cache prefix
stays stable.
"""

from __future__ import annotations

from jarvis.brain.context import RequestContext
from jarvis.brain.session import BrainSession, _now_line
from jarvis.config import load_config

_DAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def test_now_line_has_weekday_and_clock() -> None:
    line = _now_line("")
    assert line.startswith("Right now it's ")
    assert any(d in line for d in _DAYS)
    assert ("am" in line) or ("pm" in line)


def test_now_line_handles_named_and_bad_timezone() -> None:
    assert "Right now it's" in _now_line("Europe/London")
    # an unknown tz must fall back to local, not crash
    assert "Right now it's" in _now_line("Not/AZone")


def _session() -> BrainSession:
    return BrainSession(
        load_config(),
        RequestContext("dev", "house", "house", frozenset()),
        gateway=None,
        tts=None,
        memory=None,
        tracer=None,
        registry=None,
    )


def test_system_prompt_injects_now_last() -> None:
    prompt = _session()._system_prompt("")
    assert "Right now it's" in prompt
    # most-volatile line goes last (keeps the cache prefix stable)
    assert prompt.rstrip().splitlines()[-1].startswith("Right now it's")


def test_system_prompt_includes_now_with_memory() -> None:
    prompt = _session()._system_prompt("likes tea")
    assert "likes tea" in prompt
    assert "Right now it's" in prompt
