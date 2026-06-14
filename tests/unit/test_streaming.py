"""Streaming helpers — sentence segmentation + steering reuse (turnloop).

These run on the LLM token stream so speech can start on sentence 1 while the
rest generates. Pure string logic; high regression risk during the restructure
(the streaming path moves to the intercom/brain seam).
"""

from __future__ import annotations

from jarvis.turnloop import _extract_steering, _next_sentence

# --- _next_sentence --------------------------------------------------------


def test_incomplete_buffer_returns_none() -> None:
    assert _next_sentence("Hello there") is None
    assert _next_sentence("Hi.") is None  # too short / no trailing space


def test_splits_on_first_sentence() -> None:
    sent, rest = _next_sentence("This is a sentence. Next one.")
    assert sent == "This is a sentence."
    assert rest == "Next one."


def test_never_splits_inside_brackets() -> None:
    # The '.' inside [stop. go] must not end the sentence (steering/non-verbals
    # contain punctuation); the split lands after "more here".
    sent, rest = _next_sentence("Padding here [stop. go] more here. End.")
    assert sent == "Padding here [stop. go] more here."
    assert rest == "End."


def test_does_not_split_after_abbreviation() -> None:
    sent, rest = _next_sentence("Well now then, Dr. Smith arrived. Next.")
    assert sent == "Well now then, Dr. Smith arrived."
    assert rest == "Next."


def test_force_flush_overlong_clause_without_terminator() -> None:
    # No sentence terminator but past max_len → flush at a word boundary.
    buf = ("alpha " * 40).strip()  # 239 chars, no '.', no trailing space
    result = _next_sentence(buf)
    assert result is not None
    sent, rest = result
    assert sent and rest  # split into a flushed clause + a remainder
    assert not sent.endswith(" ")  # cut cleanly at a word boundary


# --- _extract_steering -----------------------------------------------------


def test_leading_steering_directive_is_extracted() -> None:
    tag, rest = _extract_steering("[say warmly] Hello there")
    assert tag == "[say warmly]"
    assert rest == "Hello there"


def test_non_verbals_stay_inline() -> None:
    # [laugh] is a sound, not a directive — it must be left in place, not pulled
    # out for reuse across sentences.
    tag, rest = _extract_steering("[laugh] that is funny")
    assert tag == ""
    assert rest == "[laugh] that is funny"

    tag, rest = _extract_steering("[clear throat] ahem, now then")
    assert tag == ""
    assert rest.startswith("[clear throat]")


def test_plain_text_has_no_steering() -> None:
    tag, rest = _extract_steering("Just a plain reply.")
    assert tag == ""
    assert rest == "Just a plain reply."
