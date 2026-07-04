"""Shared utterance policy helpers for conversation lifecycle decisions."""

from __future__ import annotations

import re


# Any request/question word means a bare acknowledgement is not a soft close.
REQUEST_CUE_RE = re.compile(
    r"\b(tell|what|whats|how|why|when|where|who|which|show|give|explain|"
    r"recommend|suggest|find|search|list|define|describe|help|can you|could you)\b"
)


def normalize_utterance(text: str) -> str:
    """Lowercase, remove apostrophes/punctuation, and collapse whitespace."""
    t = (text or "").lower().replace("'", "")
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def only_filler_remains(text: str, remove: re.Pattern, filler: frozenset) -> bool:
    """True when removing pattern matches leaves only filler words."""
    return all(w in filler for w in remove.sub(" ", text).split())
