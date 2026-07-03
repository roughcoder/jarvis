"""Voice-mode policy for spoken Jarvis conversations.

Modes are a voice-only layer above ordinary turn state. Default mode closes
aggressively unless a turn explicitly asks to keep the mic open; stay mode keeps
the mic open until an explicit exit.
"""

from __future__ import annotations

from dataclasses import dataclass
import re

from jarvis.brain.dialog import _norm, _only_filler_remains, _REQUEST_CUE


DEFAULT_MODE = "default"
STAY_MODE = "stay"
KNOWN_MODES = frozenset({DEFAULT_MODE, STAY_MODE})


@dataclass(frozen=True)
class VoiceModeProfile:
    name: str
    listening_policy: str
    exit_policy: str
    identity_scope: str
    prompt_style: str


PROFILES = {
    DEFAULT_MODE: VoiceModeProfile(
        name=DEFAULT_MODE,
        listening_policy="explicit_or_brief_followup",
        exit_policy="default_closed",
        identity_scope="conversation",
        prompt_style="short_task",
    ),
    STAY_MODE: VoiceModeProfile(
        name=STAY_MODE,
        listening_policy="persistent",
        exit_policy="explicit_only",
        identity_scope="mode",
        prompt_style="open_conversation",
    ),
}

_MODE_CONTROL_RE = re.compile(
    r"\s*\[\[\s*(?P<kind>conversation|voice_mode)\s*:"
    r"\s*(?P<value>[a-z_ -]+?)\s*(?::\s*(?P<reason>[a-z_ -]+?)\s*)?\]\]\s*",
    re.IGNORECASE,
)
_ACTIVATE_STAY = re.compile(
    r"\b("
    r"(go|switch|put|come) (in|into|to) stay mode|"
    r"start stay mode|stay mode|stay with me|stick around|keep listening|"
    r"lets chat|let us chat|chat for a bit|hang around|hang out|"
    r"dont go to sleep yet|do not go to sleep yet"
    r")\b"
)
_EXIT_STAY = re.compile(
    r"\b("
    r"exit stay mode|leave stay mode|stop stay mode|default mode|"
    r"go back to default|back to default"
    r")\b"
)
# NOTE: intentionally a DIFFERENT list from dialog._SIGNOFF_ANCHOR. This one
# gates the pre-LLM local action (canned reply, no model call) so it holds only
# unmistakable commands; softer sign-offs ('no thanks') go to the LLM for a
# warm farewell and are caught post-turn by dialog's net.
_HARD_EXIT = re.compile(
    r"\b("
    r"stop listening|go to sleep|go to bed|goodbye|bye bye|bye|goodnight|"
    r"good night|that'?s enough|that is enough|that'?s all|that is all|"
    r"we'?re done|we are done|i'?m done|i am done"
    r")\b"
)
_SOFT_CLOSE = re.compile(
    r"^(thanks|thank you|cheers|ok|okay|cool|great|nice one|perfect|brilliant|"
    r"lovely|all good|no thanks|nothing else)[.! ]*$",
    re.IGNORECASE,
)
_LEADING_TTS_TAG = re.compile(r"^\s*\[[^\[\]]+\]\s*")
_FOLLOWUP_QUESTION_REPLY = re.compile(
    r"(?:^|[.!?,;:-]\s+)\b(?:could you|can you|would you|what time|which one|"
    r"who should|where should|"
    r"what should|when should|when do|where are|where is|for how long|"
    r"how long should|do you mean|did you mean)\b[^.?!]*\?",
    re.IGNORECASE,
)
_FOLLOWUP_FINAL_QUESTION_REPLY = re.compile(
    r"(?:^|[.!?,;:-]\s+)\b(?:"
    r"and you|"
    r"how (?:are|about|do|did|would|can|is|was|have) "
    r"(?:you|we|it|that|this|things|your)\b|"
    r"how's (?:your|the)\b|"
    r"what (?:about|are|were|do|did|would|should|can) (?:you|we|i)\b|"
    r"why do you\b|"
    r"are you\b|do you\b|did you\b|would you\b|could you\b|can you\b|"
    r"should we\b|shall we\b|is that\b|does that\b|will that\b|would that\b|"
    r"want me to\b|need me to\b|anything else\b"
    r")"
    r"[^.?!]*\?\s*$",
    re.IGNORECASE,
)
_FOLLOWUP_REQUEST_REPLY = re.compile(
    r"(?:^|[.!?,;:-]\s+)\b(?:please (?:tell|say|try|send|show)|try again|"
    r"one more time|tell me (?:which|what|who|where|when|how)|"
    r"send (?:me )?(?:another|a clearer))\b",
    re.IGNORECASE,
)
_EXPLORATORY_USER = re.compile(
    r"\b(help me|think through|walk me through|talk me through|plan|planning|"
    r"troubleshoot|debug|figure out|work out|explain|why|how should|how do i|"
    r"what should|what do you think|compare|decide|design|review|diagnose|"
    r"investigate|research|brainstorm|step by step)\b",
    re.IGNORECASE,
)
_TASK_COMPLETE_TOOLS = frozenset({"set_alarm", "cancel_alarm", "list_alarms"})


@dataclass(frozen=True)
class VoiceControl:
    conversation: str | None = None  # open | closed
    reason: str = ""
    mode: str | None = None


@dataclass(frozen=True)
class LocalVoiceAction:
    reply: str
    mode: str
    ended: bool
    continue_listening: bool
    reason: str


@dataclass(frozen=True)
class VoiceStateTransition:
    mode: str
    ended: bool
    continue_listening: bool
    reason: str
    reset_conversation: bool = False
    policy_decision: str = ""
    marker_seen: bool = False
    assistant_asked_followup: bool = False


def normalize_mode(mode: str | None) -> str:
    mode = (mode or DEFAULT_MODE).strip().lower().replace("-", "_")
    return mode if mode in KNOWN_MODES else DEFAULT_MODE


def strip_voice_controls(text: str) -> str:
    return _MODE_CONTROL_RE.sub(" ", text or "").strip()


def parse_voice_control(text: str) -> VoiceControl:
    conversation = None
    reason = ""
    mode = None
    for match in _MODE_CONTROL_RE.finditer(text or ""):
        kind = match.group("kind").lower()
        value = (match.group("value") or "").strip().lower().replace("-", "_")
        marker_reason = (match.group("reason") or "").strip().lower().replace("-", "_")
        if kind == "conversation" and value in {"open", "closed"}:
            conversation = value
            reason = marker_reason
        elif kind == "voice_mode" and value in KNOWN_MODES:
            mode = value
            reason = marker_reason or reason
    return VoiceControl(conversation=conversation, reason=reason, mode=mode)


def local_voice_action(user_text: str, active_mode: str = DEFAULT_MODE) -> LocalVoiceAction | None:
    """Return a pre-LLM action for unambiguous voice control, else None."""
    text = _norm(user_text)
    if not text or _REQUEST_CUE.search(text):
        return None
    if _EXIT_STAY.search(text):
        if not _is_pure_voice_control(text, _EXIT_STAY):
            return None
        return LocalVoiceAction(
            reply="Okay, exiting stay mode.",
            mode=DEFAULT_MODE,
            ended=True,
            continue_listening=False,
            reason="mode_exit",
        )
    if _HARD_EXIT.search(text):
        if not _is_pure_voice_control(text, _HARD_EXIT):
            return None
        reply = "Bye." if "bye" in text or "goodnight" in text or "good night" in text else "Okay, going to sleep."
        return LocalVoiceAction(
            reply=reply,
            mode=DEFAULT_MODE,
            ended=True,
            continue_listening=False,
            reason="user_closed",
        )
    if _ACTIVATE_STAY.search(text):
        if not _is_pure_voice_control(text, _ACTIVATE_STAY):
            return None
        return LocalVoiceAction(
            reply="Okay, I'll stay with you.",
            mode=STAY_MODE,
            ended=False,
            continue_listening=True,
            reason="mode_enter",
        )
    return None


def should_soft_close_default(user_text: str) -> bool:
    text = _norm(user_text)
    return bool(text and not _REQUEST_CUE.search(text) and _SOFT_CLOSE.match(text))


def tool_names(tool_messages: list) -> set[str]:
    names: set[str] = set()
    for msg in tool_messages or []:
        for call in msg.get("tool_calls") or []:
            fn = (call.get("function") or {}).get("name")
            if fn:
                names.add(str(fn))
    return names


def tool_result_text(tool_messages: list, tool_name: str) -> str:
    """Return the first result text for a named tool call, or empty if absent."""
    pending_ids: set[str] = set()
    for msg in tool_messages or []:
        for call in msg.get("tool_calls") or []:
            fn = (call.get("function") or {}).get("name")
            if fn == tool_name and call.get("id"):
                pending_ids.add(str(call["id"]))
        if msg.get("role") == "tool" and msg.get("tool_call_id") in pending_ids:
            return str(msg.get("content") or "")
    return ""


def tool_completes_successfully(tool_messages: list, tool_names_to_check: set[str]) -> bool:
    names = tool_names(tool_messages)
    for name in names & tool_names_to_check:
        result = tool_result_text(tool_messages, name).strip().lower()
        if result and not result.startswith("error"):
            return True
    return False


def tool_completes_voice_turn(tool_messages: list) -> bool:
    return tool_completes_successfully(tool_messages, _TASK_COMPLETE_TOOLS)


def assistant_requests_followup(reply: str) -> bool:
    text = strip_voice_controls(reply or "").strip()
    while True:
        stripped = _LEADING_TTS_TAG.sub("", text, count=1).strip()
        if stripped == text:
            break
        text = stripped
    return bool(
        text
        and (
            text.rstrip().endswith("?")  # the reply ENDS on a question to the user
            or _FOLLOWUP_QUESTION_REPLY.search(text)
            or _FOLLOWUP_FINAL_QUESTION_REPLY.search(text)
            or _FOLLOWUP_REQUEST_REPLY.search(text)
        )
    )


def user_expects_followup(user_text: str) -> bool:
    text = _norm(user_text)
    return bool(text and _EXPLORATORY_USER.search(text))


def classify_voice_turn(
    *,
    active_mode: str,
    raw_reply: str,
    user_text: str,
    tool_messages: list,
    explicit_close: bool,
) -> VoiceStateTransition:
    """Decide the post-turn voice lifecycle from one policy surface."""
    active_mode = normalize_mode(active_mode)
    control = parse_voice_control(raw_reply)
    requested_mode = normalize_mode(control.mode or active_mode)
    marker_seen = control.conversation is not None or control.mode is not None
    assistant_followup = assistant_requests_followup(raw_reply)
    # A bare ack ('thanks', 'ok', 'cool') closes only when nothing says the
    # exchange is still in flight: an explicit goodbye always closes, but a
    # soft ack yields to a reply that asks the user something and to the
    # model's own [[CONVERSATION:open:...]] judgement.
    soft_close = (
        should_soft_close_default(user_text)
        and not assistant_followup
        and control.conversation != "open"
    )
    default_user_closed = explicit_close or soft_close

    if active_mode == STAY_MODE and requested_mode == STAY_MODE:
        stay_exit = explicit_close or (
            control.conversation == "closed" and control.reason in {"mode_exit", "user_closed"}
        )
        if stay_exit:
            return VoiceStateTransition(
                mode=DEFAULT_MODE,
                ended=True,
                continue_listening=False,
                reason="user_closed" if explicit_close else control.reason,
                reset_conversation=True,
                policy_decision="stay_explicit_exit",
                marker_seen=marker_seen,
                assistant_asked_followup=assistant_followup,
            )
        return VoiceStateTransition(
            mode=STAY_MODE,
            ended=False,
            continue_listening=True,
            reason="stay_mode",
            policy_decision="stay_persistent",
            marker_seen=marker_seen,
            assistant_asked_followup=assistant_followup,
        )

    if requested_mode == STAY_MODE and control.conversation == "open" and not default_user_closed:
        return VoiceStateTransition(
            mode=STAY_MODE,
            ended=False,
            continue_listening=True,
            reason=control.reason or "mode_enter",
            policy_decision="mode_enter",
            marker_seen=marker_seen,
            assistant_asked_followup=assistant_followup,
        )

    # A completed alarm/timer command closes the turn — unless the reply asks
    # the user something (e.g. 'Alarm set — weekdays only?'), which must not
    # hang up on its own question.
    if tool_completes_voice_turn(tool_messages) and not assistant_followup:
        return VoiceStateTransition(
            mode=DEFAULT_MODE,
            ended=True,
            continue_listening=False,
            reason="task_complete",
            reset_conversation=True,
            policy_decision="tool_complete",
            marker_seen=marker_seen,
            assistant_asked_followup=assistant_followup,
        )

    if default_user_closed:
        return VoiceStateTransition(
            mode=DEFAULT_MODE,
            ended=True,
            continue_listening=False,
            reason="user_closed",
            reset_conversation=True,
            policy_decision="user_closed",
            marker_seen=marker_seen,
            assistant_asked_followup=assistant_followup,
        )

    if control.conversation == "open":
        return VoiceStateTransition(
            mode=requested_mode,
            ended=False,
            continue_listening=True,
            reason=control.reason or "followup_expected",
            policy_decision="marker_open",
            marker_seen=marker_seen,
            assistant_asked_followup=assistant_followup,
        )

    if control.conversation == "closed":
        return VoiceStateTransition(
            mode=DEFAULT_MODE,
            ended=True,
            continue_listening=False,
            reason=control.reason or "default_complete",
            reset_conversation=True,
            policy_decision="marker_closed",
            marker_seen=marker_seen,
            assistant_asked_followup=assistant_followup,
        )

    if assistant_followup:
        return VoiceStateTransition(
            mode=DEFAULT_MODE,
            ended=False,
            continue_listening=True,
            reason="reply_followup_expected",
            policy_decision="reply_followup",
            marker_seen=marker_seen,
            assistant_asked_followup=True,
        )

    if user_expects_followup(user_text):
        return VoiceStateTransition(
            mode=DEFAULT_MODE,
            ended=False,
            continue_listening=True,
            reason="brief_followup_expected",
            policy_decision="user_exploratory",
            marker_seen=marker_seen,
            assistant_asked_followup=assistant_followup,
        )

    return VoiceStateTransition(
        mode=DEFAULT_MODE,
        ended=True,
        continue_listening=False,
        reason=control.reason or "default_complete",
        reset_conversation=True,
        policy_decision="default_complete",
        marker_seen=marker_seen,
        assistant_asked_followup=assistant_followup,
    )


def voice_disabled_transition() -> VoiceStateTransition:
    return VoiceStateTransition(
        mode=DEFAULT_MODE,
        ended=True,
        continue_listening=False,
        reason="conversation_disabled",
        reset_conversation=True,
        policy_decision="conversation_disabled",
    )


def voice_result_transition(
    *,
    ended: bool,
    voice_mode: str,
    continue_listening: bool,
    close_reason: str,
) -> VoiceStateTransition:
    return VoiceStateTransition(
        mode=normalize_mode(voice_mode),
        ended=ended,
        continue_listening=continue_listening,
        reason=close_reason,
        reset_conversation=ended,
        policy_decision="result_reset" if ended else "result_open",
    )


def cancelled_voice_transition(
    *,
    voice_mode: str,
    close_reason: str,
) -> VoiceStateTransition | None:
    if close_reason not in {"mode_enter", "mode_exit"}:
        return None
    return VoiceStateTransition(
        mode=normalize_mode(voice_mode),
        ended=close_reason == "mode_exit",
        continue_listening=close_reason == "mode_enter",
        reason=close_reason,
        reset_conversation=close_reason == "mode_exit",
        policy_decision="cancelled_mode_transition",
    )


def alarm_ack_transition(active_mode: str) -> VoiceStateTransition:
    mode = normalize_mode(active_mode)
    if mode == STAY_MODE:
        return VoiceStateTransition(
            mode=STAY_MODE,
            ended=False,
            continue_listening=True,
            reason="alarm_ack",
            policy_decision="alarm_ack_stay",
        )
    return VoiceStateTransition(
        mode=DEFAULT_MODE,
        ended=True,
        continue_listening=False,
        reason="alarm_ack",
        reset_conversation=True,
        policy_decision="alarm_ack_close",
    )


def empty_transcript_transition(channel: str) -> VoiceStateTransition:
    if channel == "voice":
        return VoiceStateTransition(
            mode=DEFAULT_MODE,
            ended=True,
            continue_listening=False,
            reason="empty_transcript",
            reset_conversation=True,
            policy_decision="empty_transcript",
        )
    return VoiceStateTransition(
        mode=DEFAULT_MODE,
        ended=False,
        continue_listening=False,
        reason="",
        policy_decision="empty_message",
    )


def voice_mode_instruction(mode: str) -> str:
    mode = normalize_mode(mode)
    if mode == STAY_MODE:
        return (
            "Voice mode: stay. This is a spoken, persistent session. Keep the "
            "conversation open until the user explicitly exits stay mode or says "
            "a hard stop such as 'stop listening', 'go to sleep', or 'bye'. Do "
            "not close just because the answer was short or the user says thanks. "
            "Append [[CONVERSATION:open:stay_mode]] unless they explicitly exit; "
            "on exit append [[VOICE_MODE:default:mode_exit]] and "
            "[[CONVERSATION:closed:mode_exit]]."
        )
    return (
        "Voice mode: default. This is spoken household use, not chat. Prefer a "
        "short complete answer, then close the mic after completed commands "
        "(alarms, timers, reminders), time/weather/simple factual answers, and "
        "polite endings such as thanks, bye, or that's all. Keep listening briefly "
        "when the reply asks the user for clarification, asks them to try again, or "
        "when the user is clearly exploring, planning, troubleshooting, or asking "
        "a multi-step question. If unsure, prefer a brief follow-up listen rather "
        "than going straight to sleep. Append exactly one conversation marker at the end: "
        "[[CONVERSATION:closed:task_complete]] when the turn is complete, "
        "[[CONVERSATION:closed:user_closed]] when the user said goodbye or declined "
        "more help, or [[CONVERSATION:open:followup_expected]] when a real follow-up "
        "is expected. "
        "If the user asks for stay mode, append [[VOICE_MODE:stay:mode_enter]] "
        "and [[CONVERSATION:open:mode_enter]]."
    )


# Deliberately SMALLER than dialog's sign-off filler: a local action bypasses
# the LLM entirely (canned reply), so only unmistakable padding is ignored.
_LOCAL_CONTROL_FILLER = frozenset({"hey", "jarvis", "please", "ok", "okay", "and", "then"})


def _is_pure_voice_control(text: str, pattern: re.Pattern[str]) -> bool:
    return _only_filler_remains(text, pattern, _LOCAL_CONTROL_FILLER)
