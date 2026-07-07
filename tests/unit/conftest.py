"""Shared unit-test helpers."""

from __future__ import annotations

from jarvis.runtime import RequestContext
from jarvis.users import HOUSE


def request_context(
    *caps: str,
    device_id: str = "dev",
    identity: str = HOUSE,
    scope: str = HOUSE,
    channel: str = "voice",
    confidence: str = "strong",
    peer: str = "",
) -> RequestContext:
    return RequestContext(
        device_id,
        identity,
        scope,
        frozenset(caps),
        channel=channel,
        confidence=confidence,
        peer=peer,
    )

