"""Per-`(device × user)` isolation (Phase 3d §9) — the structural privacy wall.

Memory caches are per-principal files, and the ContextStore hands each
`(device, identity)` its own session (own history). No network needed.
"""

from __future__ import annotations

import json
import types

from jarvis.brain.context import RequestContext
from jarvis.brain.contexts import ContextStore
from jarvis.brain.memory_client import MemoryClient
from jarvis.config import MemoryConfig


def test_memory_cache_is_per_user(tmp_path) -> None:  # noqa: ANN001
    cfg = MemoryConfig(_env_file=None, cache_path=str(tmp_path / "rep.json"), user_peer_id="user")
    mc = MemoryClient(cfg)
    # Each principal reads its own cache file; they never cross.
    (tmp_path / "rep-neil.json").write_text(json.dumps({"representation": "neil facts"}))
    (tmp_path / "rep-jules.json").write_text(json.dumps({"representation": "jules facts"}))
    assert mc.read_cached_representation("neil") == "neil facts"
    assert mc.read_cached_representation("jules") == "jules facts"
    assert mc.read_cached_representation("neil") != mc.read_cached_representation("jules")
    # The default principal keeps the original (single-principal) cache file.
    (tmp_path / "rep.json").write_text(json.dumps({"representation": "default"}))
    assert mc.read_cached_representation() == "default"
    assert mc.read_cached_representation("user") == "default"  # peer == default => base path


def test_memory_session_and_peer_per_user() -> None:
    cfg = MemoryConfig(_env_file=None, user_peer_id="user")
    mc = MemoryClient(cfg)
    assert mc._session_id(None) == "voice"
    assert mc._session_id("user") == "voice"  # default principal
    assert mc._session_id("jules") == "voice-jules"
    assert mc._peer("jules") == "jules"
    assert mc._peer(None) == "user"


def test_context_store_isolates_and_reuses_sessions() -> None:
    def make(ctx: RequestContext):  # noqa: ANN202 - a stand-in BrainSession
        s = types.SimpleNamespace(ctx=ctx, loaded=0)
        s.load_soul = lambda: setattr(s, "loaded", s.loaded + 1)
        return s

    store = ContextStore(make)
    neil = RequestContext("mac", "neil", "personal", frozenset(), peer="neil")
    jules = RequestContext("mac", "jules", "personal", frozenset(), peer="jules")

    s1 = store.get(neil)
    s2 = store.get(neil)  # same principal+device => reused
    s3 = store.get(jules)  # different principal => isolated session

    assert s1 is s2
    assert s1 is not s3
    assert len(store) == 2
    assert s1.loaded == 1  # soul loaded exactly once per session
    assert set(store.keys) == {("mac", "neil"), ("mac", "jules")}
