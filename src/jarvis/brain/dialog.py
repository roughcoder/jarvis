"""Dialog text helpers — pure functions shared by the brain think-core.

Prompt fragments, conversation-end detection (the 3-layer hybrid), streaming
sentence segmentation, and TTS-steering extraction. No I/O, no state — extracted
here so both the single-process turn loop and the WebSocket BrainSession reuse
one copy (and the test suite pins them).
"""

from __future__ import annotations

import re

from jarvis.brain.conversation_policy import (
    REQUEST_CUE_RE,
    normalize_utterance,
    only_filler_remains,
)


def _date_label(dt) -> str:  # noqa: ANN001
    return dt.strftime("%A, %-d %B %Y")


def _relative_date_map(now) -> str:  # noqa: ANN001
    from datetime import timedelta

    tomorrow = now + timedelta(days=1)
    in_two_days = now + timedelta(days=2)
    upcoming = "; ".join(
        f"{(now + timedelta(days=days)).strftime('%A')}={(now + timedelta(days=days)).strftime('%-d %B')}"
        for days in range(1, 15)
    )
    return (
        "Relative date map for memory recall: "
        f"today={_date_label(now)}; tomorrow={_date_label(tomorrow)}; "
        f"in two days={_date_label(in_two_days)}; next two weeks: {upcoming}."
    )


def _now_line(tz_name: str) -> str:
    """A human 'right now' string injected so Jarvis knows the date/time without
    a tool or a search. `tz_name` is an IANA name; empty = host local time."""
    from datetime import datetime

    now = None
    if tz_name:
        try:
            from zoneinfo import ZoneInfo

            now = datetime.now(ZoneInfo(tz_name))
        except Exception:
            now = None
    if now is None:
        now = datetime.now().astimezone()
    tz = now.strftime("%Z") or "local time"
    # Keep the volatile temporal grounding last in the prompt. It lets the model
    # match "in two days" / "next Friday" to remembered dated commitments.
    return (
        f"Right now it's {_date_label(now)}, {now.strftime('%-I:%M %p').lower()} {tz}. "
        f"{_relative_date_map(now)}"
    )


# Technical format layer (always present). Personality comes from the soul
# (SOUL.md); what Jarvis knows about the user comes from memory.
_VOICE_FORMAT_BASE = (
    "Write for the ear, not the page: one or two short spoken sentences. Use "
    "contractions and natural phrasing. Speak dates and times the way a person "
    "would — 'today', 'tomorrow', 'this Saturday', 'in a few days', 'next week' "
    "for anything near now; give a full date only when it's far off or genuinely "
    "needed, and keep it light ('the seventeenth', not 'June the seventeenth, "
    "twenty twenty-six'). Write numbers as words ('twenty-three', not '23'). "
    "Use the current relative-date map to match questions like 'tomorrow', 'in "
    "two days', 'this Friday', or 'next Tuesday' against remembered weekday or "
    "dated commitments. Never use markdown, bullet points, headings, emoji, or "
    "special characters."
)
_VOICE_FORMAT_EXPRESSIVE = (
    _VOICE_FORMAT_BASE + "\n\n"
    "Let real feeling colour your delivery when the moment calls for it, using "
    "Inworld TTS-2 cues with a light touch:\n"
    "- Delivery/emotion: at most ONE instruction in [square brackets] at the "
    "very START of the reply — it sets mood, pace and tone for the whole line. "
    "Describe it concretely and match it to the words, e.g. [say warmly with a "
    "relaxed, conversational pace] or [say gently, a little concerned]. Never "
    "put a steering tag mid-sentence, never use more than one, never contradict "
    "the words.\n"
    "- Non-verbals may go INLINE where the sound happens: [laugh], [sigh], "
    "[breathe], [clear throat], [yawn].\n"
    "- For stress, capitalise a whole word ('I did NOT'), rarely.\n"
    "Most lines need no tags at all — reach for them only when feeling genuinely "
    "colours what you're saying."
)

# Messaging channels (WhatsApp, the text console) are read, not heard — so the spoken
# rules (one breath, numbers as words, TTS [cues]) are wrong here. Write like a person
# texting: concise, normal numerals/dates, light formatting, the odd emoji if it fits.
_MESSAGING_FORMAT = (
    "You're replying in a messaging app, so write to be READ, not spoken. Natural written "
    "English — friendly and concise (a few short lines, not an essay). Normal numerals and "
    "dates are fine ('23', 'Friday the 26th'), and you may use WhatsApp formatting (*bold*, "
    "_italic_, a short list) and the occasional emoji where it genuinely fits. Send links as "
    "plain URLs. No headings, no code fences, no TTS-style [cues]. "
    "Don't pad replies with filler like 'let me know if you need anything else'."
)
# Tool-use behaviour (act-don't-advise, live data via the browser) is NOT part
# of the format layer — it lives in _AGENCY and the capability-gated guidance
# blocks, so a context without those tools is never told to use them.

PROJECT_THREAD_TOOL_SURFACE_CONTRACT = (
    "Project-thread capability contract: this conversation is a project discussion "
    "surface, not a work session. Your real actions are exactly the function tools "
    "offered on this turn, such as project memory or project switching tools when "
    "they are present. Do not claim repo access, a worktree, code-review tools, test "
    "execution, shell access, browser access, or worker progress unless you actually "
    "called a tool that performed that action and you have its result. If the user "
    "asks you to review code, inspect a repository, run tests, or do other workspace "
    "work from this thread and no offered tool can do it, say plainly that you cannot "
    "do that from this conversation. Offer to dispatch it through the work session "
    "lane (`/v1/work/start`) or provision a workspace instead. Never narrate fake "
    "in-progress work, fake tool lists, fake progress, or fake findings."
)

def compose_spoken_prompt(
    soul: str, *, tz: str, expressive: bool = False, extra: str = ""
) -> str:
    """One composer for every spoken prompt outside BrainSession (heartbeat,
    the `jarvis chat` smoke test): soul + the spoken format rules + an optional
    task-specific block + the now-line, in the same order the session uses.
    Pure — callers read SOUL.md themselves (dialog does no I/O)."""
    fmt = _VOICE_FORMAT_EXPRESSIVE if expressive else _VOICE_FORMAT_BASE
    return "\n\n".join(p for p in (soul, fmt, extra, _now_line(tz)) if p)


# Conversation control: how the model signals the user is done so the loop can
# return to PASSIVE (wake word required again). One protocol — the
# [[CONVERSATION:...]] markers taught in voice_mode_instruction(); this section
# covers the farewell case. Markers are detected + stripped before TTS.
_END_INSTRUCTION = (
    "Ending the conversation: when the user clearly signals they're finished — "
    "a goodbye ('bye', 'goodnight', 'see you'), declining further help ('no "
    "thanks', \"no, that's good, thanks\", \"I'm good\", 'we're good'), or "
    "'that's all'/'stop'/'go to sleep' — reply with a short, warm farewell of a "
    "few words and NOTHING else, then append [[CONVERSATION:closed:user_closed]] "
    "as the very last characters. IMPORTANT: if your reply is itself a goodbye, "
    "you MUST append that closed marker. Do NOT habitually tack on 'is there "
    "anything else?', 'want to know more?', or similar filler — only ask a "
    "follow-up question when you genuinely need it to help (e.g. to clarify an "
    "ambiguous request). The mic is controlled by the conversation markers "
    "described below, so you never need to prompt the user to keep talking."
)
# Legacy alias: [[END]] / [END] (case-insensitive) still parses as a close and
# is stripped from the spoken reply, but the prompt no longer teaches it.
_END_RE = re.compile(r"\s*\[\[?\s*end\s*\]\]?\s*", re.IGNORECASE)

# --- Deterministic backstops (the model handles nuance; these guarantee the
# clear cases and never fire on a turn the user meant to continue) -----------

# Back-compat aliases for direct unit coverage of the low-level policy helpers.
_REQUEST_CUE = REQUEST_CUE_RE
_norm = normalize_utterance
_only_filler_remains = only_filler_remains

# A sign-off must contain one of these ANCHORS (matched on _norm()ed text) …
_SIGNOFF_ANCHOR = re.compile(
    r"\b("
    r"goodbye|bye bye|bye|good night|goodnight|see you|see ya|im off|"
    r"stop listening|stop|go to sleep|go to bed|go away|dismissed|"
    r"thats all|that is all|thats it|that is it|thats everything|"
    r"that is everything|im done|i am done|were done|we are done|"
    r"were finished|we are finished|nothing else|im all set|i am all set|"
    r"were good|we are good|im good|i am good"
    r")\b"
)
# … or be a decline of further help: a leading no + a specific closer phrase.
_DECLINE_LEAD = re.compile(r"^(no|nope|nah)\b")
_DECLINE_CLOSER = re.compile(
    r"\b(thanks|thank you|cheers|im good|im fine|im done|im all set|im set|"
    r"all good|all set|all done|were good|were done|thats all|thats it|"
    r"thats fine|thats everything|nothing else)\b"
)
# Phrases removed before the residue check (closers that legally accompany an
# anchor without changing its meaning). Longest-first so 'thank you' wins.
_SIGNOFF_REMOVABLE = re.compile(
    r"\b(thank you|thanks|cheers|im good|im fine|all good|all set|all done|"
    r"thats good|thats fine|thats great|thats everything|no worries|"
    r"no|nope|nah|yeah|yep|yes)\b"
)
# Single filler words allowed to remain around an anchor. ANY other residue
# word means the user said something substantive — never close on it.
# Pronouns are deliberately NOT filler: 'stop it' / 'stop that' target what
# Jarvis is doing (music, an action), not the conversation.
_SIGNOFF_FILLER = frozenset(
    "ok okay alright right well so um uh cool great perfect brilliant lovely "
    "nice good fine please now then there mate jarvis hey oh ah and for a an "
    "the thats is really very much indeed anyway all bye can could "
    "would you".split()
)
# Jarvis's OWN reply is a goodbye → end even if it forgot the [[END]] marker.
# Anchor + residue on the FINAL sentence only: the backstop fires when that
# sentence IS a farewell, not merely contains a farewell word ('The song is
# Bye Bye Bye by NSYNC' must not hang up).
_REPLY_FAREWELL = re.compile(
    r"\b(goodbye|bye bye|bye|good night|goodnight|night night|sleep well|"
    r"sweet dreams|take care|farewell|talk soon|see you|see ya|catch you)\b"
)
_REPLY_FAREWELL_FILLER = frozenset(
    "ok okay then for now soon later tonight tomorrow and have a good great "
    "lovely one day evening night sir mate all too enjoy rest well".split()
)
_REPLY_CONTINUE = re.compile(
    r"\?|anything else|let me know|give me a shout|what else|tell me|how about|"
    r"shall i|would you like|need anything",
    re.IGNORECASE,
)
_BRACKETED = re.compile(r"\[[^\]]*\]")  # TTS tags, [[END]], voice markers
_SENTENCE_SPLIT = re.compile(r"[.!?]+")


def _is_clear_signoff(text: str) -> bool:
    """True only for an unambiguous goodbye / decline of further help.

    Anchor + residue: the utterance must contain a closer anchor (a farewell,
    a done-phrase, or no + a decline closer), AND once the anchor and closer
    phrases are removed, everything left must be filler. Any substantive
    residue ('no, CANCEL IT, thanks') means the turn continues — this backstop
    must never fire on a turn the user meant to continue; unusual sign-offs
    fall through to the model's marker layer instead.
    """
    base = _norm(text)
    if not base:
        return False
    anchored = _SIGNOFF_ANCHOR.search(base) or (
        _DECLINE_LEAD.match(base) and _DECLINE_CLOSER.search(base)
    )
    if not anchored:
        return False
    residue = _SIGNOFF_ANCHOR.sub(" ", base)
    return _only_filler_remains(residue, _SIGNOFF_REMOVABLE, _SIGNOFF_FILLER)


def _is_reply_farewell(reply: str) -> bool:
    """True if Jarvis's reply ENDS on a pure goodbye with no continuation cue."""
    if _REPLY_CONTINUE.search(reply or ""):
        return False
    text = _BRACKETED.sub(" ", reply or "")
    sentences = [n for s in _SENTENCE_SPLIT.split(text) if (n := _norm(s))]
    if not sentences:
        return False
    last = sentences[-1]
    if not _REPLY_FAREWELL.search(last):
        return False
    residue = _REPLY_FAREWELL.sub(" ", last)
    return all(w in _REPLY_FAREWELL_FILLER for w in residue.split())


# --- Streaming: sentence segmentation + steering reuse ----------------------

# Inworld non-verbals stay inline; everything else leading is a steering
# directive that must be reused across sentences (its scope is the whole call).
_NONVERBALS = frozenset(
    {"laugh", "sigh", "breathe", "cough", "yawn", "clear throat", "gasp"}
)
_LEAD_BRACKET = re.compile(r"^\s*\[([^\]]+)\]\s*")


def _extract_steering(text: str) -> tuple[str, str]:
    """Pull a leading steering directive off the first sentence so it can be
    re-applied to later sentences. Returns (steering_tag_or_empty, remainder)."""
    m = _LEAD_BRACKET.match(text)
    if not m:
        return "", text
    if m.group(1).strip().lower() in _NONVERBALS:
        return "", text  # a sound, not a directive — leave it inline
    return f"[{m.group(1).strip()}]", text[m.end() :]


_ABBREV = frozenset(
    {"mr", "mrs", "ms", "dr", "prof", "st", "sr", "jr", "vs", "etc", "eg", "ie", "mt"}
)
_LAST_WORD = re.compile(r"([A-Za-z]+)$")


def _next_sentence(buf: str, min_len: int = 12, max_len: int = 180) -> tuple[str, str] | None:
    """Split off the first complete sentence from a streaming buffer, never
    breaking inside [...] or <...>. Returns (sentence, rest) or None if not yet."""
    depth_sq = depth_ang = 0
    for i, ch in enumerate(buf):
        if ch == "[":
            depth_sq += 1
        elif ch == "]":
            depth_sq = max(0, depth_sq - 1)
        elif ch == "<":
            depth_ang += 1
        elif ch == ">":
            depth_ang = max(0, depth_ang - 1)
        elif ch in ".!?" and depth_sq == 0 and depth_ang == 0:
            if i + 1 < len(buf) and buf[i + 1] in " \n" and i + 1 >= min_len:
                if ch == ".":  # don't split after an abbreviation or initial
                    m = _LAST_WORD.search(buf[:i])
                    w = m.group(1).lower() if m else ""
                    if w in _ABBREV or len(w) == 1:
                        continue
                return buf[: i + 1].strip(), buf[i + 2 :].lstrip()
    # Force-flush an over-long clause (rare for short replies) at a space.
    if len(buf) >= max_len and depth_sq == 0 and depth_ang == 0:
        cut = buf.rfind(" ", min_len)
        if cut != -1:
            return buf[:cut].strip(), buf[cut + 1 :].lstrip()
    return None
