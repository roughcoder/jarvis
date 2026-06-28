"""Voice modes — default short-task behavior plus persistent stay mode."""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.brain.server import BrainServer
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.brain.voice_modes import (
    DEFAULT_MODE,
    STAY_MODE,
    local_voice_action,
    parse_voice_control,
    strip_voice_controls,
)
from jarvis.config import load_config
from jarvis.protocol.messages import ReplyEnd, decode, encode


class _Gateway:
    def __init__(self) -> None:
        self.calls = 0

    async def complete(self, messages, *, model=None):  # noqa: ANN001
        self.calls += 1
        return "This should not be called."


class _TTS:
    async def synthesize_stream(self, text):  # noqa: ANN001
        yield text.encode()


def _session(mode: str = DEFAULT_MODE) -> BrainSession:
    sess = BrainSession(
        load_config(),
        RequestContext("dev", "house", "house", frozenset(), channel="voice"),
        gateway=_Gateway(),
        tts=_TTS(),
        memory=None,
        tracer=None,
        registry=None,
    )
    sess.set_voice_mode(mode)
    sess._fire_cold_path = lambda *_args: None  # type: ignore[method-assign]
    return sess


def test_parse_and_strip_voice_control_markers() -> None:
    raw = "Sure. [[VOICE_MODE:stay:mode_enter]] [[CONVERSATION:open:mode_enter]]"

    control = parse_voice_control(raw)

    assert control.mode == STAY_MODE
    assert control.conversation == "open"
    assert control.reason == "mode_enter"
    assert strip_voice_controls(raw) == "Sure."


def test_stay_mode_activation_is_pre_llm() -> None:
    sess = _session()
    result = TurnResult()

    async def go() -> list[bytes]:
        return [chunk async for chunk in sess.respond("stay with me", None, result)]

    chunks = asyncio.run(go())

    assert chunks == [b"Okay, I'll stay with you."]
    assert result.voice_mode == STAY_MODE
    assert result.continue_listening is True
    assert result.ended is False
    assert sess._gateway.calls == 0


def test_hard_exit_is_pre_llm_and_returns_default_mode() -> None:
    sess = _session(STAY_MODE)
    result = TurnResult()

    async def go() -> list[bytes]:
        return [chunk async for chunk in sess.respond("go to sleep", None, result)]

    chunks = asyncio.run(go())
    sess.finalize("go to sleep", result)

    assert chunks == [b"Okay, going to sleep."]
    assert result.voice_mode == DEFAULT_MODE
    assert result.ended is True
    assert result.continue_listening is False
    assert sess._gateway.calls == 0


def test_default_mode_closes_without_explicit_open_marker() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="It's seven o'clock.")

    sess.finalize("what time is it", result)

    assert result.reply == "It's seven o'clock."
    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "default_complete"


def test_default_mode_stays_open_on_explicit_open_marker() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="Let's break that down. [[CONVERSATION:open:followup_expected]]")

    sess.finalize("help me think through the move", result)

    assert result.reply == "Let's break that down."
    assert result.ended is False
    assert result.continue_listening is True
    assert result.voice_mode == DEFAULT_MODE


def test_stay_mode_keeps_listening_after_short_answer() -> None:
    sess = _session(STAY_MODE)
    result = TurnResult(raw="Yep, it's sunny.")

    sess.finalize("what's the weather", result)

    assert result.ended is False
    assert result.continue_listening is True
    assert result.voice_mode == STAY_MODE


def test_alarm_tools_force_voice_turn_closed() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw="Alarm set for seven.",
        tool_messages=[
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "c1",
                        "type": "function",
                        "function": {"name": "set_alarm", "arguments": "{}"},
                    }
                ],
            }
        ],
    )

    sess.finalize("set an alarm for seven", result)

    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "task_complete"


def test_alarm_tools_do_not_exit_stay_mode() -> None:
    sess = _session(STAY_MODE)
    result = TurnResult(
        raw="Alarm set for seven.",
        tool_messages=[
            {
                "role": "assistant",
                "tool_calls": [
                    {
                        "id": "c1",
                        "type": "function",
                        "function": {"name": "set_alarm", "arguments": "{}"},
                    }
                ],
            }
        ],
    )

    sess.finalize("set an alarm for seven", result)

    assert result.ended is False
    assert result.continue_listening is True
    assert result.voice_mode == STAY_MODE
    assert result.close_reason == "stay_mode"


def test_local_voice_action_ignores_requests() -> None:
    assert local_voice_action("bye, can you set a timer") is None


def test_reply_end_carries_voice_mode_metadata() -> None:
    msg = ReplyEnd(
        turn_id="t1",
        ended=False,
        continue_listening=True,
        voice_mode=STAY_MODE,
        close_reason="mode_enter",
    )

    back = decode(encode(msg))

    assert isinstance(back, ReplyEnd)
    assert back.model_dump() == msg.model_dump()


def test_voice_conversation_reset_clears_temporary_identity_and_mode() -> None:
    conn = {"asserted": "neil", "base_asserted": "", "voice_mode": STAY_MODE}

    BrainServer._reset_voice_conversation("voice", conn)

    assert conn["asserted"] == ""
    assert conn["voice_mode"] == DEFAULT_MODE
