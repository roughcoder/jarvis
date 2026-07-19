"""Shared prompt/session constants for Honcho memory."""

from __future__ import annotations


_SESSION_ID = "voice"
_MEMORY_QUERY = (
    "Summarise everything important you know about the user — their name, "
    "preferences, and any facts or ongoing context — in a few concise sentences. "
    "If you know nothing about them yet, reply with an empty string."
)


def _turn_metadata(*, channel: str, device_id: str | None) -> dict[str, str]:
    metadata = {"channel": (channel or "voice").strip() or "voice"}
    if device_id:
        metadata["device_id"] = device_id
    return metadata
