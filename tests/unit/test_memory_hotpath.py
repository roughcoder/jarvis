"""Memory hot/cold boundary — the load-bearing Phase 2 readiness invariant.

Constraint #2: the hot path's only memory call is a LOCAL file read; it must
work even when the memory service is unreachable. The cold path must fail
*clean* at the boundary (a connection error, fast) — never hang, never silently
half-succeed. This is the PHASE2.md readiness snippet, automated.
"""

from __future__ import annotations

import json
import pathlib

import httpx
import pytest

from jarvis.config import MemoryConfig
from jarvis.memory_client import MemoryClient


def _client(tmp_path, **over):
    cfg = MemoryConfig(_env_file=None, cache_path=str(tmp_path / "rep.json"), **over)
    return MemoryClient(cfg), cfg


def test_hot_read_missing_cache_returns_empty(tmp_path) -> None:
    mc, _ = _client(tmp_path)
    assert mc.read_cached_representation() == ""


def test_hot_read_returns_cached_representation(tmp_path) -> None:
    mc, cfg = _client(tmp_path)
    pathlib.Path(cfg.cache_path).write_text(json.dumps({"representation": "likes tea"}))
    assert mc.read_cached_representation() == "likes tea"


def test_hot_read_malformed_cache_returns_empty(tmp_path) -> None:
    mc, cfg = _client(tmp_path)
    pathlib.Path(cfg.cache_path).write_text("{ not valid json")
    assert mc.read_cached_representation() == ""


def test_hot_read_works_with_dead_boundary(tmp_path) -> None:
    # Memory pointed at a dead host:port — the hot read still works because it
    # never touches the network. This is the readiness gate's "hot" half.
    mc, cfg = _client(tmp_path, host="localhost", port=1)
    pathlib.Path(cfg.cache_path).write_text(json.dumps({"representation": "offline ok"}))
    assert mc.read_cached_representation() == "offline ok"


def test_cold_write_fails_clean_at_dead_boundary(tmp_path) -> None:
    # The readiness gate's "cold" half: a write to a dead boundary raises a
    # connection error quickly, rather than hanging or pretending to succeed.
    mc, _ = _client(tmp_path, host="localhost", port=1, write_timeout_s=2.0)
    with pytest.raises((httpx.HTTPError, OSError)):
        mc._write_turn_sync("hello", "there")
