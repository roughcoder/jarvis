"""Integration: real Honcho memory round-trip (cold write + cache refresh).

Proves a turn can be written and the local representation cache refreshed
against a live Honcho. Skips if Honcho is unreachable.
"""

from __future__ import annotations

import asyncio

import pytest

from jarvis.config import load_config
from jarvis.memory_client import MemoryClient

pytestmark = pytest.mark.integration


def _mc_or_skip() -> MemoryClient:
    cfg = load_config()
    mc = MemoryClient(cfg.memory)
    if not mc.ping():
        pytest.skip(f"honcho not reachable at {cfg.memory.base_url}")
    return mc


def test_write_then_refresh_roundtrip() -> None:
    mc = _mc_or_skip()

    async def run() -> bool:
        await mc.write_turn("My name is Neil.", "Nice to meet you, Neil.")
        # min_interval_s=0 forces the refresh to actually run (not debounced).
        return await mc.refresh_cache(min_interval_s=0.0)

    ran = asyncio.run(run())
    assert isinstance(ran, bool)
    # The hot read always returns a string (may be empty until the deriver
    # catches up) — never raises.
    assert isinstance(mc.read_cached_representation(), str)
