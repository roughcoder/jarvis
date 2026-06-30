from __future__ import annotations

import re


def slugify(text: str, max_words: int = 6) -> str:
    """A short, file- and speech-friendly handle from free text."""
    words = re.findall(r"[a-z0-9]+", text.lower())[:max_words]
    return "-".join(words) or "job"
