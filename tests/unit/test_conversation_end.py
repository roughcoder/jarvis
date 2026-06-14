"""Conversation-end detection — the 3-layer hybrid (AGENTS.md).

The model's [[END]] marker handles nuance; these deterministic backstops
guarantee the clear cases and, critically, must NEVER fire on a turn the user
meant to continue. This is the gap AGENTS.md flagged ("add new failure phrases
to the test cases") — closed here. Add new real-world phrases as cases.
"""

from __future__ import annotations

import pytest

from jarvis.brain.dialog import _END_RE, _is_clear_signoff, _is_reply_farewell, _norm

# --- _is_clear_signoff: deterministic user sign-off net --------------------

CLEAR_SIGNOFFS = [
    "bye",
    "goodbye",
    "good night",
    "goodnight",
    "stop",
    "stop listening",
    "go to sleep",
    "that's all",
    "that's it",
    "we're done",
    "nothing else",
    "no thanks",
    "no, that's good, thanks",  # the exact example from the END instruction
    "okay, that's all then",
    "thanks, that's all",
]

NOT_SIGNOFFS = [
    "what's the weather",       # request cue
    "tell me a joke",           # request cue
    "how do I do that",         # request cue
    "can you help",             # request cue ("help")
    "no, that's a good idea",   # 'no' + 'good' but NOT a decline-closer
    "thanks",                   # bare ack — must ask, not end
    "ok",                       # bare ack
    "cool",                     # bare ack
    "great",                    # bare ack
]


@pytest.mark.parametrize("text", CLEAR_SIGNOFFS)
def test_clear_signoffs_end_the_conversation(text: str) -> None:
    assert _is_clear_signoff(text) is True


@pytest.mark.parametrize("text", NOT_SIGNOFFS)
def test_continuations_never_end_the_conversation(text: str) -> None:
    assert _is_clear_signoff(text) is False


# --- _is_reply_farewell: Jarvis-reply backstop -----------------------------

REPLY_FAREWELLS = [
    "Goodbye!",
    "Good night, sleep well.",
    "Take care.",
    "Bye for now.",
]

REPLY_CONTINUATIONS = [
    "Bye — anything else?",                 # farewell word but a continuation cue
    "Sure, what else can I do for you?",
    "Take care, and let me know if you need anything else.",
    "The weather is sunny today.",          # not a farewell at all
]


@pytest.mark.parametrize("reply", REPLY_FAREWELLS)
def test_reply_farewell_ends(reply: str) -> None:
    assert _is_reply_farewell(reply) is True


@pytest.mark.parametrize("reply", REPLY_CONTINUATIONS)
def test_reply_with_continuation_does_not_end(reply: str) -> None:
    assert _is_reply_farewell(reply) is False


# --- _END_RE: the [[END]] marker -------------------------------------------


def test_end_marker_detected_and_stripped() -> None:
    raw = "Goodnight. [[END]]"
    assert _END_RE.search(raw)
    assert _END_RE.sub(" ", raw).strip() == "Goodnight."


@pytest.mark.parametrize("marker", ["[[END]]", "[END]", "[[end]]", "[ end ]"])
def test_end_marker_variants(marker: str) -> None:
    assert _END_RE.search(f"bye {marker}")


def test_no_marker_leaves_text_untouched() -> None:
    raw = "There's plenty more to cover."
    assert _END_RE.search(raw) is None
    assert _END_RE.sub(" ", raw).strip() == raw


# --- _norm -----------------------------------------------------------------


def test_norm_lowercases_strips_punct_and_apostrophes() -> None:
    assert _norm("That's all!") == "thats all"
    assert _norm("  We're   DONE.  ") == "were done"
