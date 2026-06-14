"""BrainSession prompt assembly — the injected 'now' line (Phase 3 time/date).

Jarvis must know the current date/time from the prompt (no tool, no search), and
it must be the most-volatile, last part of the system prompt so the cache prefix
stays stable.
"""

from __future__ import annotations

from jarvis.brain.context import RequestContext
from jarvis.brain.session import BrainSession, TurnResult, _now_line
from jarvis.config import load_config


def _tool_turn(reply: str, job_id: str = "abc123") -> TurnResult:
    return TurnResult(
        reply=reply,
        tool_messages=[
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": "c1", "type": "function", "function": {"name": "start_coding_job", "arguments": "{}"}}
                ],
            },
            {"role": "tool", "tool_call_id": "c1", "content": f"Started a coding job (id {job_id})."},
        ],
    )

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


def test_history_carries_tool_calls_and_results() -> None:
    s = _session()
    s._remember("start a coding job", _tool_turn("Started the job."))
    hist = s._history
    assert hist[0] == {"role": "user", "content": "start a coding job"}
    assert any(m.get("role") == "assistant" and m.get("tool_calls") for m in hist)
    assert any(m.get("role") == "tool" and "abc123" in m["content"] for m in hist)
    assert hist[-1] == {"role": "assistant", "content": "Started the job."}


def test_history_trim_never_orphans_tool_messages() -> None:
    s = _session()
    s._cfg.persona.history_messages = 3  # force a trim mid tool-group
    s._remember("u1", _tool_turn("started"))  # 4 messages
    s._remember("u2", TurnResult(reply="hi there"))  # 2 messages
    # trimmed window must start on a user message (no orphaned tool/tool_calls)
    assert s._history[0]["role"] == "user"
    for i, m in enumerate(s._history):
        if m.get("role") == "tool":
            assert any(
                h.get("role") == "assistant" and h.get("tool_calls")
                for h in s._history[:i]
            )


def test_casual_turn_history_unchanged() -> None:
    s = _session()
    s._remember("hello", TurnResult(reply="Hi there."))
    assert s._history == [
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "Hi there."},
    ]
