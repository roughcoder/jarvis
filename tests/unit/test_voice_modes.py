"""Voice modes — default short-task behavior plus persistent stay mode."""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.brain.server import BrainServer
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.brain.tracing import TurnTrace
from jarvis.brain.turnloop import TurnLoop
from jarvis.brain.voice_modes import (
    DEFAULT_MODE,
    STAY_MODE,
    local_voice_action,
    parse_voice_control,
    strip_voice_controls,
)
from jarvis.config import load_config
from jarvis.protocol.messages import AudioStart, ReplyEnd, decode, encode


class _Gateway:
    def __init__(self) -> None:
        self.calls = 0

    async def complete(self, messages, *, model=None):  # noqa: ANN001
        self.calls += 1
        return "This should not be called."


class _TTS:
    async def synthesize_stream(self, text):  # noqa: ANN001
        yield text.encode()


def _session(mode: str = DEFAULT_MODE, *, conversation_mode: bool = True) -> BrainSession:
    cfg = load_config()
    cfg.vad.conversation_mode = conversation_mode
    sess = BrainSession(
        cfg,
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


def test_stay_mode_activation_is_unavailable_when_conversation_mode_disabled() -> None:
    sess = _session(conversation_mode=False)
    result = TurnResult()

    async def go() -> list[bytes]:
        return [chunk async for chunk in sess.respond("stay with me", None, result)]

    chunks = asyncio.run(go())

    assert chunks == [b"I can't stay with you while follow-up listening is off."]
    assert result.voice_mode == DEFAULT_MODE
    assert result.continue_listening is False
    assert result.ended is True
    assert result.close_reason == "conversation_disabled"
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


def test_polite_hard_exit_is_pre_llm_and_returns_default_mode() -> None:
    sess = _session(STAY_MODE)
    result = TurnResult()

    async def go() -> list[bytes]:
        return [chunk async for chunk in sess.respond("could you go to sleep please", None, result)]

    chunks = asyncio.run(go())
    sess.finalize("could you go to sleep please", result)

    assert chunks == [b"Okay, going to sleep."]
    assert result.voice_mode == DEFAULT_MODE
    assert result.ended is True
    assert result.continue_listening is False
    assert sess._gateway.calls == 0


def test_polite_stay_mode_activation_is_pre_llm() -> None:
    sess = _session()
    result = TurnResult()

    async def go() -> list[bytes]:
        return [chunk async for chunk in sess.respond("could you stay with me please", None, result)]

    chunks = asyncio.run(go())

    assert chunks == [b"Okay, I'll stay with you."]
    assert result.voice_mode == STAY_MODE
    assert result.continue_listening is True
    assert result.ended is False
    assert sess._gateway.calls == 0


def test_hard_exit_with_substantive_residue_goes_to_model() -> None:
    assert local_voice_action("can you stop the music please", STAY_MODE) is None


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


def test_default_mode_opens_when_reply_requests_followup_without_marker() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="I can't tell from that image. Could you try again with better lighting?")

    sess.finalize("what am I holding", result)

    assert result.ended is False
    assert result.continue_listening is True
    assert result.close_reason == "reply_followup_expected"
    assert result.voice_mode == DEFAULT_MODE


def test_default_mode_opens_when_reply_asks_conversational_followup_without_marker() -> None:
    sess = _session(DEFAULT_MODE)

    for raw in (
        "I'm running smoothly, thanks. How are you?",
        "I'm running smoothly, thanks. How's your day going?",
        "I'm good, thanks - what about you?",
        "That sounds useful. What are you working on?",
        "I can help with that. Would you like me to sketch a plan?",
    ):
        result = TurnResult(raw=raw)

        sess.finalize("how are you doing", result)

        assert result.ended is False
        assert result.continue_listening is True
        assert result.close_reason == "reply_followup_expected"
        assert result.voice_mode == DEFAULT_MODE


def test_default_mode_respects_closed_marker_when_reply_contains_followup_question() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw=(
            "I can't tell from that image. Could you try again with better lighting? "
            "[[CONVERSATION:closed:task_complete]]"
        )
    )

    sess.finalize("what am I holding", result)

    assert result.reply == "I can't tell from that image. Could you try again with better lighting?"
    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "task_complete"


def test_default_mode_respects_closed_marker_when_reply_contains_conversational_followup() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw="I'm running smoothly, thanks. How are you? [[CONVERSATION:closed:task_complete]]"
    )

    sess.finalize("how are you doing", result)

    assert result.reply == "I'm running smoothly, thanks. How are you?"
    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "task_complete"


def test_default_mode_opens_when_followup_reply_starts_with_tts_steering_tag() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="[say gently] Could you try again with better lighting?")

    sess.finalize("what am I holding", result)

    assert result.ended is False
    assert result.continue_listening is True
    assert result.close_reason == "reply_followup_expected"


def test_default_mode_ignores_generic_anything_else_question_on_closed_turn() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw="It's one fifteen. Anything else? [[CONVERSATION:closed:task_complete]]"
    )

    sess.finalize("what time is it", result)

    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "task_complete"


def test_default_mode_does_not_treat_need_answer_as_followup() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw="You need a Phillips screwdriver. [[CONVERSATION:closed:task_complete]]"
    )

    sess.finalize("what tool do I need", result)

    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "task_complete"


def test_default_mode_does_not_treat_better_lighting_statement_as_followup() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="Better lighting makes photos clearer.")

    sess.finalize("photo is hard to read", result)

    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "default_complete"


def test_default_mode_opens_for_exploratory_turn_without_marker() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="We should split it into packing, timing, and budget.")

    sess.finalize("help me think through the move", result)

    assert result.ended is False
    assert result.continue_listening is True
    assert result.close_reason == "brief_followup_expected"


def test_default_mode_respects_closed_marker_for_completed_explanation() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw="The sky looks blue because shorter blue wavelengths scatter more. "
        "[[CONVERSATION:closed:task_complete]]"
    )

    sess.finalize("why is the sky blue", result)

    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "task_complete"


def test_default_mode_followup_phrases_do_not_match_inside_completed_answer_words() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="That's when Docker starts. [[CONVERSATION:closed:task_complete]]")

    sess.finalize("when does Docker start", result)

    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "task_complete"


def test_default_mode_respects_closed_marker_when_followup_phrases_are_quoted() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw=(
            "'Could you' is a little more formal than 'can you'. "
            "[[CONVERSATION:closed:task_complete]]"
        )
    )

    sess.finalize("what is the difference between could you and can you", result)

    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "task_complete"


def test_default_mode_respects_closed_marker_when_answer_lists_question_text() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw=(
            "Useful planning questions include: What should success look like? "
            "[[CONVERSATION:closed:task_complete]]"
        )
    )

    sess.finalize("give me some questions for planning", result)

    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "task_complete"


def test_default_mode_soft_close_ends_plain_ack_turn() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="No problem. [[CONVERSATION:closed:task_complete]]")

    sess.finalize("thanks", result)

    assert result.reply == "No problem."
    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "user_closed"
    assert result.voice_mode == DEFAULT_MODE


def test_default_mode_soft_ack_yields_to_open_marker() -> None:
    # A bare 'thanks' is context-sensitive: the model's explicit open marker
    # (mid-flow judgement) outranks the soft close.
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="No problem. [[CONVERSATION:open:followup_expected]]")

    sess.finalize("thanks", result)

    assert result.ended is False
    assert result.continue_listening is True
    assert result.voice_mode == DEFAULT_MODE


def test_default_mode_soft_ack_yields_to_reply_question() -> None:
    # 'thanks' while Jarvis is asking the user something must not hang up.
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="Sure — what time should I book the table for?")

    sess.finalize("thanks", result)

    assert result.ended is False
    assert result.continue_listening is True


def test_signoff_closes_even_when_model_marks_open() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="No problem. [[CONVERSATION:open:followup_expected]]")

    sess.finalize("no thanks, that's all", result)

    assert result.reply == "No problem."
    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "user_closed"
    assert result.voice_mode == DEFAULT_MODE


def test_voice_turn_closes_when_conversation_mode_disabled() -> None:
    sess = _session(STAY_MODE, conversation_mode=False)
    result = TurnResult(raw="It's seven o'clock.")

    sess.finalize("what time is it", result)

    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "conversation_disabled"
    assert result.voice_mode == DEFAULT_MODE


def test_stay_mode_keeps_listening_after_short_answer() -> None:
    sess = _session(STAY_MODE)
    result = TurnResult(raw="Yep, it's sunny.")

    sess.finalize("what's the weather", result)

    assert result.ended is False
    assert result.continue_listening is True
    assert result.voice_mode == STAY_MODE


def test_stay_mode_ignores_generic_closed_marker() -> None:
    sess = _session(STAY_MODE)
    result = TurnResult(raw="Done. [[CONVERSATION:closed:task_complete]]")

    sess.finalize("what time is it", result)

    assert result.reply == "Done."
    assert result.ended is False
    assert result.continue_listening is True
    assert result.voice_mode == STAY_MODE
    assert result.close_reason == "stay_mode"


def test_stay_mode_keeps_listening_after_soft_acknowledgement() -> None:
    sess = _session(STAY_MODE)
    result = TurnResult(raw="No problem. [[CONVERSATION:open:stay_mode]]")

    sess.finalize("thanks", result)

    assert result.reply == "No problem."
    assert result.ended is False
    assert result.continue_listening is True
    assert result.voice_mode == STAY_MODE
    assert result.close_reason == "stay_mode"


def test_alarm_tool_close_yields_to_reply_question() -> None:
    # A successful set_alarm must not close the mic while the reply is asking
    # the user a question.
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="Alarm set for seven. Want it weekdays only?")
    result.tool_messages = [
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [{"id": "1", "function": {"name": "set_alarm", "arguments": "{}"}}],
        },
        {"role": "tool", "tool_call_id": "1", "content": "alarm set for 07:00"},
    ]

    sess.finalize("wake me at seven", result)

    assert result.ended is False
    assert result.continue_listening is True


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
            },
            {"role": "tool", "tool_call_id": "c1", "content": "Alarm set for seven."},
        ],
    )

    sess.finalize("set an alarm for seven", result)

    assert result.ended is True
    assert result.continue_listening is False
    assert result.close_reason == "task_complete"


def test_stay_mode_marker_overrides_task_complete_tool_backstop() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw=(
            "Alarm set for seven. "
            "[[VOICE_MODE:stay:mode_enter]] [[CONVERSATION:open:mode_enter]]"
        ),
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
            },
            {"role": "tool", "tool_call_id": "c1", "content": "Alarm set for seven."},
        ],
    )

    sess.finalize("stay with me and set an alarm for seven", result)

    assert result.reply == "Alarm set for seven."
    assert result.ended is False
    assert result.continue_listening is True
    assert result.voice_mode == STAY_MODE
    assert result.close_reason == "mode_enter"


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
            },
            {"role": "tool", "tool_call_id": "c1", "content": "Alarm set for seven."},
        ],
    )

    sess.finalize("set an alarm for seven", result)

    assert result.ended is False
    assert result.continue_listening is True
    assert result.voice_mode == STAY_MODE
    assert result.close_reason == "stay_mode"


def test_failed_alarm_tool_keeps_voice_turn_open_for_clarification() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw="What time should I set it for? [[CONVERSATION:open:clarification_needed]]",
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
            },
            {"role": "tool", "tool_call_id": "c1", "content": "error: tell me when"},
        ],
    )

    sess.finalize("set an alarm", result)

    assert result.ended is False
    assert result.continue_listening is True
    assert result.close_reason == "clarification_needed"


def test_failed_alarm_tool_opens_when_reply_asks_clarifying_question_without_marker() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(
        raw="What time should I set it for?",
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
            },
            {"role": "tool", "tool_call_id": "c1", "content": "error: tell me when"},
        ],
    )

    sess.finalize("set an alarm", result)

    assert result.ended is False
    assert result.continue_listening is True
    assert result.close_reason == "reply_followup_expected"


def test_failed_timer_tool_opens_for_common_duration_clarification_without_marker() -> None:
    sess = _session(DEFAULT_MODE)

    for raw in ("What should I set it for?", "For how long?"):
        result = TurnResult(raw=raw)

        sess.finalize("set a timer", result)

        assert result.ended is False
        assert result.continue_listening is True
        assert result.close_reason == "reply_followup_expected"


def test_default_mode_opens_for_followup_question_after_colon_or_dash() -> None:
    sess = _session(DEFAULT_MODE)

    for raw in (
        "I need one more detail: what time should I set it for?",
        "I need one more detail - what time should I set it for?",
    ):
        result = TurnResult(raw=raw)

        sess.finalize("set an alarm", result)

        assert result.ended is False
        assert result.continue_listening is True
        assert result.close_reason == "reply_followup_expected"


def test_voice_lifecycle_writes_trace_metadata() -> None:
    sess = _session(DEFAULT_MODE)
    result = TurnResult(raw="Could you try again?")
    trace = TurnTrace(room="default", speaker="alice", channel="voice", device_id="pi")

    sess.finalize("what am I holding", result, trace)

    assert trace.data["voice_mode_before"] == DEFAULT_MODE
    assert trace.data["voice_mode_after"] == DEFAULT_MODE
    assert trace.data["close_reason"] == "reply_followup_expected"
    assert trace.data["continue_listening"] is True
    assert trace.data["policy_decision"] == "reply_followup"
    assert trace.data["marker_seen"] is False
    assert trace.data["assistant_asked_followup"] is True


def test_local_voice_action_ignores_requests() -> None:
    assert local_voice_action("bye, can you set a timer") is None
    assert local_voice_action("stay with me and set an alarm for seven") is None
    assert local_voice_action("keep listening and turn on the lights") is None
    assert local_voice_action("please stay with me") is not None


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


def test_brain_buffers_audio_start_voice_mode() -> None:
    conn = {"audio_buffers": {}}

    conn["audio_buffers"]["t1"] = {
        "sample_rate": 16000,
        "voice_mode": STAY_MODE,
        "chunks": [],
        "frame_bytes": 0,
        "started_at": 1.0,
    }
    buffered = BrainServer._finish_audio_buffer(conn, "t1")

    assert buffered is not None
    assert buffered.voice_mode == STAY_MODE


def test_audio_start_voice_mode_round_trips() -> None:
    msg = decode(encode(AudioStart(turn_id="t1", sample_rate=16000, voice_mode=DEFAULT_MODE)))

    assert isinstance(msg, AudioStart)
    assert msg.voice_mode == DEFAULT_MODE


def test_voice_conversation_reset_clears_temporary_identity_and_mode() -> None:
    conn = {"asserted": "neil", "base_asserted": "", "voice_mode": STAY_MODE}

    BrainServer._reset_voice_conversation("voice", conn)

    assert conn["asserted"] == ""
    assert conn["voice_mode"] == DEFAULT_MODE


def test_voice_conversation_reset_preserves_paired_identity() -> None:
    conn = {"asserted": "jules", "base_asserted": "alice", "voice_mode": STAY_MODE}

    BrainServer._reset_voice_conversation("voice", conn)

    assert conn["asserted"] == "alice"
    assert conn["voice_mode"] == DEFAULT_MODE


def test_local_turnloop_reset_clears_temporary_identity_and_mode() -> None:
    loop = TurnLoop.__new__(TurnLoop)
    loop._asserted = "neil"
    loop._base_asserted = ""
    loop._voice_mode = STAY_MODE

    loop._reset_voice_conversation()

    assert loop._asserted == ""
    assert loop._voice_mode == DEFAULT_MODE


def test_local_cancelled_mode_exit_resets_identity_and_mode() -> None:
    loop = TurnLoop.__new__(TurnLoop)
    loop._asserted = "neil"
    loop._base_asserted = ""
    loop._voice_mode = STAY_MODE
    result = TurnResult(ended=True, voice_mode=DEFAULT_MODE, close_reason="mode_exit")

    loop._apply_cancelled_turn_result(result)

    assert loop._asserted == ""
    assert loop._voice_mode == DEFAULT_MODE


def test_local_cancelled_user_closed_preserves_identity_and_mode() -> None:
    loop = TurnLoop.__new__(TurnLoop)
    loop._asserted = "neil"
    loop._base_asserted = ""
    loop._voice_mode = STAY_MODE
    result = TurnResult(ended=True, voice_mode=DEFAULT_MODE, close_reason="user_closed")

    loop._apply_cancelled_turn_result(result)

    assert loop._asserted == "neil"
    assert loop._voice_mode == STAY_MODE


def test_alarm_ack_preserves_stay_mode() -> None:
    conn = {"voice_mode": STAY_MODE}

    end = BrainServer._alarm_ack_reply_end("t1", "voice", conn)

    assert end.ended is False
    assert end.continue_listening is True
    assert end.voice_mode == STAY_MODE
    assert end.close_reason == "alarm_ack"


def test_empty_voice_transcript_closes_and_resets_mode() -> None:
    end = BrainServer._empty_transcript_reply_end("t1", "voice")

    assert end.ended is True
    assert end.continue_listening is False
    assert end.voice_mode == DEFAULT_MODE
    assert end.close_reason == "empty_transcript"


def test_empty_text_transcript_remains_open_message_boundary() -> None:
    end = BrainServer._empty_transcript_reply_end("t1", "whatsapp")

    assert end.ended is False
    assert end.continue_listening is False
    assert end.voice_mode == DEFAULT_MODE
    assert end.close_reason == ""


def test_cancelled_partial_reply_preserves_connection_state() -> None:
    conn = {"asserted": "neil", "base_asserted": "", "voice_mode": STAY_MODE}
    result = TurnResult(ended=True, voice_mode=DEFAULT_MODE, close_reason="default_complete")

    BrainServer._apply_cancelled_turn_result("voice", conn, result)

    assert conn["asserted"] == "neil"
    assert conn["voice_mode"] == STAY_MODE


def test_cancelled_user_closed_preserves_connection_state() -> None:
    conn = {"asserted": "neil", "base_asserted": "", "voice_mode": STAY_MODE}
    result = TurnResult(ended=True, voice_mode=DEFAULT_MODE, close_reason="user_closed")

    BrainServer._apply_cancelled_turn_result("voice", conn, result)

    assert conn["asserted"] == "neil"
    assert conn["voice_mode"] == STAY_MODE


def test_cancelled_explicit_mode_exit_updates_connection_state() -> None:
    conn = {"asserted": "neil", "base_asserted": "", "voice_mode": STAY_MODE}
    result = TurnResult(ended=True, voice_mode=DEFAULT_MODE, close_reason="mode_exit")

    BrainServer._apply_cancelled_turn_result("voice", conn, result)

    assert conn["asserted"] == ""
    assert conn["voice_mode"] == DEFAULT_MODE


def test_cancelled_marker_parsed_by_finalize_updates_connection_state() -> None:
    sess = _session(STAY_MODE)
    conn = {"asserted": "neil", "base_asserted": "", "voice_mode": STAY_MODE}
    result = TurnResult(raw="Okay. [[VOICE_MODE:default:mode_exit]] [[CONVERSATION:closed:mode_exit]]")

    sess.finalize("exit stay mode", result)
    BrainServer._apply_cancelled_turn_result("voice", conn, result)

    assert result.close_reason == "mode_exit"
    assert conn["asserted"] == ""
    assert conn["voice_mode"] == DEFAULT_MODE


def test_turn_result_state_preserves_open_stay_connection() -> None:
    conn = {"asserted": "neil", "base_asserted": "", "voice_mode": DEFAULT_MODE}
    result = TurnResult(ended=False, voice_mode=STAY_MODE)

    BrainServer._apply_turn_result("voice", conn, result)

    assert conn["asserted"] == "neil"
    assert conn["voice_mode"] == STAY_MODE
