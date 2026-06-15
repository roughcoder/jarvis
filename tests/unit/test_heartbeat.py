"""Heartbeat scheduler (Phase 3b §9) — the silent-completion sentinel + push."""

from __future__ import annotations

import asyncio

from jarvis.brain.heartbeat import HeartbeatScheduler, is_silent
from jarvis.config import HeartbeatConfig


def test_is_silent_recognises_sentinel_and_empty() -> None:
    assert is_silent("", "NO_REPLY")
    assert is_silent("   ", "NO_REPLY")
    assert is_silent("NO_REPLY", "NO_REPLY")
    assert is_silent("no_reply", "NO_REPLY")  # case-insensitive
    assert not is_silent("Your 3pm moved to 4pm.", "NO_REPLY")


def _sched(reply: str, sent: list) -> HeartbeatScheduler:
    async def think() -> str:
        return reply

    async def broadcast(text: str) -> None:
        sent.append(text)

    return HeartbeatScheduler(HeartbeatConfig(_env_file=None), think=think, broadcast=broadcast)


def test_tick_pushes_only_when_meaningful() -> None:
    sent: list = []
    assert asyncio.run(_sched("NO_REPLY", sent).tick()) is None
    assert sent == []  # silent => nothing pushed

    sent.clear()
    out = asyncio.run(_sched("  Your parcel arrived.  ", sent).tick())
    assert out == "Your parcel arrived."
    assert sent == ["Your parcel arrived."]  # meaningful => broadcast (trimmed)
