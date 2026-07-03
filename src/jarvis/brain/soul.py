"""Soul loading — the single reader for SOUL.md (who Jarvis is).

Every surface that composes a prompt (BrainSession, the heartbeat, the
`jarvis chat` smoke test) reads the personality through this one function, so
encoding/normalisation changes happen in exactly one place.
"""

from __future__ import annotations

import pathlib


def read_soul(path: str, fallback: str = "") -> str:
    p = pathlib.Path(path)
    return p.read_text(encoding="utf-8").strip() if p.exists() else fallback
