"""Dialog text helpers — pure functions shared by the brain think-core.

Prompt fragments, conversation-end detection (the 3-layer hybrid), streaming
sentence segmentation, and TTS-steering extraction. No I/O, no state — extracted
here so both the single-process turn loop and the WebSocket BrainSession reuse
one copy (and the test suite pins them).
"""

from __future__ import annotations

import re

# Technical format layer (always present). Personality comes from the soul
# (SOUL.md); what Jarvis knows about the user comes from memory.
_VOICE_FORMAT_BASE = (
    "Write for the ear, not the page: one or two short spoken sentences. Use "
    "contractions and natural phrasing. Speak dates and times the way a person "
    "would — 'today', 'tomorrow', 'this Saturday', 'in a few days', 'next week' "
    "for anything near now; give a full date only when it's far off or genuinely "
    "needed, and keep it light ('the seventeenth', not 'June the seventeenth, "
    "twenty twenty-six'). Write numbers as words ('twenty-three', not '23'). "
    "Never use markdown, bullet points, headings, emoji, or special characters."
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
    "plain URLs. No headings, no code fences, no TTS-style [cues]."
)

# Conversation control: how the model signals the user is done so the loop can
# return to PASSIVE (wake word required again). Detected + stripped before TTS.
_END_INSTRUCTION = (
    "Ending the conversation: end only when the user clearly signals they're "
    "finished — a goodbye ('bye', 'goodnight', 'see you'), declining further help "
    "('no thanks', \"no, that's good, thanks\", \"I'm good\", 'we're good'), or "
    "'that's all'/'stop'/'go to sleep'. To end, give a short, warm farewell of a "
    "few words and NOTHING else, then put [[END]] as the very last characters. "
    "IMPORTANT: if your reply is itself a goodbye, you MUST include [[END]]. "
    "Otherwise, just answer naturally and stop. Do NOT habitually tack on 'is "
    "there anything else?', 'want to know more?', or similar filler — only ask a "
    "follow-up question when you genuinely need it to help (e.g. to clarify an "
    "ambiguous request). When you can't tell whether they're done, simply finish "
    "your reply normally — no farewell, no [[END]]; the mic stays open, so you "
    "never need to prompt them to keep talking."
)
# Matches [[END]] / [END] (case-insensitive). Stripped from the spoken reply.
_END_RE = re.compile(r"\s*\[\[?\s*end\s*\]\]?\s*", re.IGNORECASE)

# --- Deterministic backstops (the model handles nuance; these guarantee the
# clear cases and never fire on a turn the user meant to continue) -----------

# Any request/question word → never a sign-off.
_REQUEST_CUE = re.compile(
    r"\b(tell|what|whats|how|why|when|where|who|which|show|give|explain|"
    r"recommend|suggest|find|search|list|define|describe|help)\b"
)
# Short command / closer phrases (matched after stripping filler words).
_CLEAR_SIGNOFFS = frozenset(
    {
        "goodbye", "bye", "bye bye", "good night", "goodnight",
        "stop", "stop listening", "go to sleep", "go to bed", "go away",
        "dismissed", "that is all", "thats all", "that is it", "thats it",
        "im done", "i am done", "were done", "we are done", "nothing else",
    }
)
# Farewell / "we're finished" phrasing anywhere in the utterance.
_USER_FAREWELL = re.compile(
    r"\b(goodbye|good ?night|see you|see ya|were good|we are good|were done|"
    r"we are done|were finished|im off|that(s| is) (all|it|everything))\b"
)
# "no … <specific done-phrase>" — a decline of further help. Uses specific
# phrases (never bare 'good') so "no, that's a good idea" is NOT a sign-off.
_DECLINE_CLOSER = re.compile(
    r"^(no|nope|nah)\b.*\b(thanks|thank you|cheers|im good|im fine|im done|"
    r"im all set|im set|all good|all set|all done|good thanks|fine thanks|"
    r"great thanks|were good|were done|thats all|thats it|thats fine|"
    r"thats everything|nothing else)\b"
)
_SIGNOFF_LEAD = re.compile(
    r"^(no|nope|nah|yeah|yep|yes|ok|okay|alright|right|well|so|um|uh|cool|great|"
    r"thanks|thank you|cheers|jarvis)\s+"
)
_SIGNOFF_TRAIL = re.compile(
    r"\s+(please|thanks|thank you|cheers|now|then|mate|jarvis|ok|okay|here)$"
)
# Jarvis's OWN reply is a goodbye → end even if it forgot the [[END]] marker.
_REPLY_FAREWELL = re.compile(
    r"\b(goodbye|good ?night|see you|see ya|sleep well|take care|farewell|"
    r"talk soon|bye)\b",
    re.IGNORECASE,
)
_REPLY_CONTINUE = re.compile(
    r"\?|anything else|let me know|give me a shout|what else|tell me|how about|"
    r"shall i|would you like|need anything",
    re.IGNORECASE,
)


def _norm(text: str) -> str:
    t = text.lower().replace("'", "")
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _is_clear_signoff(text: str) -> bool:
    """True only for an unambiguous goodbye / decline of further help."""
    base = _norm(text)
    if _REQUEST_CUE.search(base):
        return False
    if _USER_FAREWELL.search(base) or _DECLINE_CLOSER.search(base):
        return True
    t = base
    while True:
        stripped = _SIGNOFF_TRAIL.sub("", _SIGNOFF_LEAD.sub("", t))
        if stripped == t:
            break
        t = stripped
    return t in _CLEAR_SIGNOFFS


def _is_reply_farewell(reply: str) -> bool:
    """True if Jarvis's reply is a goodbye with no continuation cue."""
    return bool(_REPLY_FAREWELL.search(reply)) and not _REPLY_CONTINUE.search(reply)


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
