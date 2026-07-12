"""Voice-mode wire vocabulary shared by the brain and intercom edge.

These are the mode NAMES and normalizers that both tiers need to agree on
over the WebSocket protocol. The richer voice-mode policy (profiles,
transitions, end-detection heuristics) stays in `jarvis.brain.voice_modes`,
which imports and re-exports these names so existing brain-side importers are
unaffected.
"""

from __future__ import annotations

DEFAULT_MODE = "default"
STAY_MODE = "stay"
KNOWN_MODES = frozenset({DEFAULT_MODE, STAY_MODE})


def normalize_mode(mode: str | None) -> str:
    mode = (mode or DEFAULT_MODE).strip().lower().replace("-", "_")
    return mode if mode in KNOWN_MODES else DEFAULT_MODE


def normalize_and_validate_mode(value: str | None, allowed: frozenset[str] | tuple[str, ...]) -> str:
    """Normalise a raw voice-mode string and return it only if in `allowed`, else "".

    Shared by callers (e.g. intercom panels) that validate against their own
    allowed-mode set rather than always defaulting like `normalize_mode`.
    """
    mode = (value or "").strip().lower().replace("-", "_")
    return mode if mode in allowed else ""
