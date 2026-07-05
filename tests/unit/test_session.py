"""BrainSession prompt assembly — the injected 'now' line (Phase 3 time/date).

Jarvis must know the current date/time from the prompt (no tool, no search), and
it must be the most-volatile, last part of the system prompt so the cache prefix
stays stable.
"""

from __future__ import annotations

import asyncio
from datetime import datetime

from jarvis.brain.dialog import _relative_date_map
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
    assert "Relative date map for memory recall" in line
    assert "tomorrow=" in line
    assert "in two days=" in line
    assert "next two weeks:" in line


def test_relative_date_map_covers_upcoming_weekdays() -> None:
    line = _relative_date_map(datetime(2026, 6, 28, 20, 47))
    assert "today=Sunday, 28 June 2026" in line
    assert "tomorrow=Monday, 29 June 2026" in line
    assert "in two days=Tuesday, 30 June 2026" in line
    assert "Friday=3 July" in line
    assert "Tuesday=7 July" in line


def test_now_line_handles_named_and_bad_timezone() -> None:
    assert "Right now it's" in _now_line("Europe/London")
    # an unknown tz must fall back to local, not crash
    assert "Right now it's" in _now_line("Not/AZone")


def _session(channel: str = "voice", cfg=None) -> BrainSession:  # noqa: ANN001
    cfg = cfg or load_config()
    return BrainSession(
        cfg,
        RequestContext("dev", "house", "house", frozenset(), channel=channel),
        gateway=None,
        tts=None,
        memory=None,
        tracer=None,
        registry=None,
    )


def test_system_prompt_mentions_camera_when_available() -> None:
    sess = BrainSession(
        load_config(),
        RequestContext(
            "pi", "alice", "personal", frozenset({"intercom.camera"}), channel="voice"
        ),
        gateway=None,
        tts=None,
        memory=None,
        tracer=None,
        registry=None,
    )

    prompt = sess._system_prompt("")

    assert "This intercom has a camera" in prompt
    assert "take_photo" in prompt


def test_system_prompt_mentions_pi_panel_when_display_available() -> None:
    sess = BrainSession(
        load_config(),
        RequestContext(
            "kitchen-pi", "house", "house", frozenset({"intercom.display"}), channel="voice"
        ),
        gateway=None,
        tts=None,
        memory=None,
        tracer=None,
        registry=None,
    )

    prompt = sess._system_prompt("")

    assert "PiPanel display" in prompt
    assert "control_pi_panel" in prompt
    assert "off' to hide" in prompt


def test_system_prompt_mentions_self_tools_when_available() -> None:
    sess = BrainSession(
        load_config(),
        RequestContext(
            "local-mac",
            "neil",
            "personal",
            frozenset({"self.inspect", "self.diagnostics", "worker.shell"}),
            channel="voice",
        ),
        gateway=None,
        tts=None,
        memory=None,
        tracer=None,
        registry=None,
    )

    prompt = sess._system_prompt("")

    assert "Device awareness" in prompt
    assert "describe_device" in prompt
    assert "run_self_diagnostics" in prompt
    assert "get_ip_address" in prompt
    assert "ping_host" in prompt
    assert "Terminal work" in prompt


def test_system_prompt_injects_now_last() -> None:
    prompt = _session()._system_prompt("")
    assert "Right now it's" in prompt
    # most-volatile line goes last (keeps the cache prefix stable)
    assert prompt.rstrip().splitlines()[-1].startswith("Right now it's")


def test_system_prompt_includes_now_with_memory() -> None:
    prompt = _session()._system_prompt("likes tea")
    assert "likes tea" in prompt
    assert "Right now it's" in prompt


def test_system_prompt_tells_voice_to_ground_relative_dates() -> None:
    prompt = _session("voice")._system_prompt("needs to bring the PE kit on Monday")
    assert "questions like 'tomorrow', 'in two days'" in prompt
    assert "'this Friday', or 'next Tuesday'" in prompt
    assert "remembered weekday or dated commitments" in prompt


def test_system_prompt_tells_memory_caps_to_honor_retractions() -> None:
    sess = BrainSession(
        load_config(),
        RequestContext("dev", "neil", "personal", frozenset({"memory.query"}), channel="voice"),
        gateway=None,
        tts=None,
        memory=None,
        tracer=None,
        registry=None,
    )

    prompt = sess._system_prompt("")

    assert "contradiction or retraction" in prompt
    assert "authoritative over any derived restatement" in prompt


def test_initial_model_is_channel_aware() -> None:
    cfg = load_config()
    fast, voice, strong = (
        cfg.gateway.fast_model,
        cfg.gateway.voice_model or cfg.gateway.fast_model,
        cfg.gateway.strong_model,
    )
    # voice: short -> voice route (defaults to fast), long -> strong
    assert _session("voice", cfg)._initial_model("hi") == voice
    assert _session("voice", cfg)._initial_model("x" * 200) == strong
    # messaging channels aren't TTS-bound -> strong from the start
    assert _session("whatsapp", cfg)._initial_model("hi") == strong
    assert _session("text", cfg)._initial_model("hi") == strong
    assert cfg.gateway.fast_model == fast


def test_voice_model_can_be_tuned_without_changing_fast_model() -> None:
    cfg = load_config()
    cfg.gateway.fast_model = "fast-route"
    cfg.gateway.voice_model = "voice-route"
    cfg.gateway.strong_model = "strong-route"

    assert _session("voice", cfg)._initial_model("hi") == "voice-route"
    assert _session("whatsapp", cfg)._initial_model("hi") == "strong-route"


def _drive(agen) -> None:  # noqa: ANN001
    async def go() -> None:
        async for _ in agen:
            pass
    asyncio.run(go())


def test_tool_loop_escalates_fast_to_strong_on_tool_use() -> None:
    import types

    from jarvis.tools.base import Tool, ToolRegistry

    cfg = load_config()
    fast, strong = cfg.gateway.fast_model, cfg.gateway.strong_model

    class _TC:
        def __init__(self) -> None:
            self.id = "c1"
            self.function = types.SimpleNamespace(name="ping", arguments="{}")

    class _Msg:
        def __init__(self, content="", tool_calls=None) -> None:  # noqa: ANN001
            self.content = content
            self.tool_calls = tool_calls

    class _Gateway:
        def __init__(self) -> None:
            self.models: list[str] = []
            self._script = [_Msg(tool_calls=[_TC()]), _Msg(content="done")]

        async def complete_with_tools(self, messages, *, model, tools=None, usage_out=None):  # noqa: ANN001
            self.models.append(model)
            return self._script.pop(0)

    async def _ping(ctx, args) -> str:  # noqa: ANN001
        return "ok"

    reg = ToolRegistry()
    reg.register(Tool(name="ping", description="", parameters={"type": "object", "properties": {}},
                      required_capability="ping.use", handler=_ping))
    gw = _Gateway()
    sess = BrainSession(
        cfg,
        RequestContext("dev", "neil", "personal", frozenset({"ping.use"}), channel="voice"),
        gateway=gw, tts=None, memory=None, tracer=None, registry=reg,
    )
    result = TurnResult(reply="", tool_messages=[])
    _drive(sess._run_tool_loop([], fast, None, [], result))
    # first call (deciding to use a tool) on fast; after the tool, escalated to strong
    assert gw.models == [fast, strong]
    assert result.raw == "done"


def test_system_prompt_format_is_channel_aware() -> None:
    # voice is heard → spoken rules (numbers as words, no markdown), with end-detection
    voice = _session("voice")._system_prompt("")
    assert "Write for the ear" in voice
    assert "messaging app" not in voice
    assert "Ending the conversation" in voice  # open-mic end-detect only on voice
    assert "Voice mode: default" in voice
    # whatsapp is read → written prose, and no open-mic end-detection
    wa = _session("whatsapp")._system_prompt("")
    assert "messaging app" in wa
    assert "Write for the ear" not in wa
    assert "Ending the conversation" not in wa


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
