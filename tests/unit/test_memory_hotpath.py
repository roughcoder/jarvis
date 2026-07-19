"""Memory hot/cold boundary — the load-bearing Phase 2 readiness invariant.

Constraint #2: the hot path's only memory call is a LOCAL file read; it must
work even when the memory service is unreachable. The cold path must fail
*clean* at the boundary (a connection error, fast) — never hang, never silently
half-succeed. This is the PHASE2.md readiness snippet, automated.
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import time

import httpx
import pytest

from jarvis.brain.context import RequestContext
from jarvis.brain.memory_client import MemoryClient, QueueStatus, UnsupportedMemoryOperation, cache_key
from jarvis.brain.memory_outbox import CurationOutbox
from jarvis.brain.memory_client.v3 import HonchoV3MemoryClient
from jarvis.brain.session import BrainSession
from jarvis.config import MemoryConfig
from jarvis.config import load_config
from jarvis.tools.base import ToolRegistry


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


def test_cache_key_uses_readable_sanitised_peer_ids() -> None:
    assert cache_key("neil") == "neil"
    assert cache_key("project:jarvis") == "project-jarvis"
    assert cache_key("contact:klaus") == "contact-klaus"
    assert cache_key("voice:neil:mac") == "voice-neil-mac"


def test_cache_key_escapes_separator_collision_pairs() -> None:
    assert cache_key("voice:neil:mac") != cache_key("voice:neil/mac")
    assert cache_key("project:jarvis") != cache_key("project-jarvis")
    assert cache_key("voice:neil/mac") == "voice-neil_x2f_mac"
    assert cache_key("project-jarvis") == "project_x2d_jarvis"


def test_cache_path_keeps_default_peer_byte_compatible(tmp_path) -> None:
    mc, cfg = _client(tmp_path, user_peer_id="user")
    v3 = HonchoV3MemoryClient(
        MemoryConfig(
            _env_file=None,
            backend="v3",
            cache_path=str(tmp_path / "rep.json"),
            conclusion_sidecar_path=str(tmp_path / "sidecar.json"),
            user_peer_id="user",
        )
    )

    assert mc._cache_path(None) == pathlib.Path(cfg.cache_path)
    assert mc._cache_path("user") == pathlib.Path(cfg.cache_path)
    assert mc._cache_path("project:jarvis").name == "rep-project-jarvis.json"
    assert v3._cache_path(None) == pathlib.Path(cfg.cache_path)
    assert v3._cache_path("project:jarvis").name == "rep-project-jarvis.json"


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


class _ColdMemory:
    def __init__(
        self,
        statuses=None,  # noqa: ANN001
        error: Exception | None = None,
        refresh_errors: dict[str | None, Exception] | None = None,
    ) -> None:
        self.statuses = list(statuses or [QueueStatus()])
        self.error = error
        self.refresh_errors = dict(refresh_errors or {})
        self.calls: list[tuple] = []
        self.write_turn_kwargs: list[dict[str, str | None]] = []
        self.created: list[dict] = []

    def read_cached_representation(self, user=None):  # noqa: ANN001
        return ""

    async def write_turn(  # noqa: ANN001
        self,
        user_text,
        assistant_text,
        *,
        user=None,
        channel="voice",
        device_id=None,
    ) -> None:
        self.calls.append(("write_turn", user, user_text, assistant_text))
        self.write_turn_kwargs.append({"channel": channel, "device_id": device_id})

    def queue_status(self) -> QueueStatus:
        self.calls.append(("queue_status",))
        if self.error is not None:
            raise self.error
        if len(self.statuses) > 1:
            return self.statuses.pop(0)
        return self.statuses[0]

    async def refresh_cache(self, min_interval_s=0.0, *, user=None) -> bool:  # noqa: ANN001
        self.calls.append(("refresh_cache", user, min_interval_s))
        if user in self.refresh_errors:
            raise self.refresh_errors[user]
        return True

    def list_conclusions(self, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("list_conclusions", kwargs.get("observed_id"), kwargs.get("metadata")))
        return []

    def create_conclusion(self, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("create_conclusion", kwargs.get("observed_id"), kwargs.get("content")))
        self.created.append(kwargs)


class _Trace:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []
        self.stages: list[tuple[str, dict]] = []

    def event(self, name: str, **meta) -> None:  # noqa: ANN001
        self.events.append((name, meta))

    def stage(self, name: str, ms: float, **meta) -> None:  # noqa: ANN001
        self.stages.append((name, {"ms": ms, **meta}))


class _Tracer:
    def __init__(self) -> None:
        self.trace = _Trace()
        self.emitted: list[_Trace] = []

    def turn(self, **_kwargs) -> _Trace:  # noqa: ANN003
        return self.trace

    def emit(self, trace: _Trace) -> None:
        self.emitted.append(trace)


def _brain_with_memory(
    memory: _ColdMemory,
    tmp_path: pathlib.Path,
    *,
    timeout_s: float = 0.05,
    tracer=None,  # noqa: ANN001
    channel: str = "voice",
    device_id: str = "dev",
) -> BrainSession:
    cfg = load_config()
    cfg.memory.deriver_idle_timeout_s = timeout_s
    cfg.memory.refresh_interval_s = 30.0
    cfg.memory.curation_outbox_path = str(tmp_path / "outbox.jsonl")
    cfg.memory.curation_outbox_backoff_initial_s = 0
    cfg.memory.curation_outbox_backoff_max_s = 0
    ctx = RequestContext(device_id, "house", "house", frozenset(), channel=channel)
    return BrainSession(
        cfg,
        ctx,
        gateway=None,
        tts=None,
        memory=memory,
        tracer=tracer,
        registry=ToolRegistry(),
    )


def test_cold_path_waits_for_idle_before_refreshing_requested_peers(tmp_path) -> None:
    memory = _ColdMemory([QueueStatus()])
    session = _brain_with_memory(memory, tmp_path)

    asyncio.run(session._cold_path("hello", "there", refresh_peers=("project:jarvis",)))

    assert memory.calls == [
        ("write_turn", None, "hello", "there"),
        ("queue_status",),
        ("refresh_cache", None, 30.0),
        ("refresh_cache", "project:jarvis", 0.0),
    ]


def test_cold_path_forwards_channel_and_device_to_memory_write(tmp_path) -> None:
    memory = _ColdMemory([QueueStatus()])
    session = _brain_with_memory(memory, tmp_path, channel="whatsapp", device_id="whatsapp")

    asyncio.run(session._cold_path("hello", "there"))

    assert memory.write_turn_kwargs == [{"channel": "whatsapp", "device_id": "whatsapp"}]


def test_cold_path_flushes_curation_outbox_before_deriver_wait(tmp_path) -> None:
    memory = _ColdMemory([QueueStatus()])
    session = _brain_with_memory(memory, tmp_path)
    outbox = CurationOutbox(tmp_path / "outbox.jsonl")
    outbox.enqueue_create(
        observed_id="contact:klaus",
        observer_id="neil",
        content="Klaus is off Fridays.",
        metadata={"observed_at": "2026-07-04"},
    )

    asyncio.run(session._cold_path("hello", "there"))

    assert memory.calls[:4] == [
        ("write_turn", None, "hello", "there"),
        ("list_conclusions", "contact:klaus", None),
        ("create_conclusion", "contact:klaus", "Klaus is off Fridays."),
        ("queue_status",),
    ]
    assert [row["content"] for row in memory.created] == ["Klaus is off Fridays."]


def test_deriver_idle_wait_honours_bound_and_refreshes_on_busy_timeout(tmp_path) -> None:
    memory = _ColdMemory([QueueStatus(pending_work_units=1)])
    session = _brain_with_memory(memory, tmp_path, timeout_s=0.03)

    t0 = time.perf_counter()
    asyncio.run(session._cold_path("hello", "there"))

    assert (time.perf_counter() - t0) < 0.3
    assert ("refresh_cache", None, 30.0) in memory.calls


def test_deriver_idle_wait_errors_do_not_block_refresh(tmp_path) -> None:
    memory = _ColdMemory(error=RuntimeError("queue down"))
    session = _brain_with_memory(memory, tmp_path)

    asyncio.run(session._cold_path("hello", "there"))

    assert memory.calls == [
        ("write_turn", None, "hello", "there"),
        ("queue_status",),
        ("refresh_cache", None, 30.0),
    ]


def test_deriver_idle_wait_unsupported_queue_status_is_immediate_and_refreshes(tmp_path) -> None:
    memory = _ColdMemory(error=UnsupportedMemoryOperation("queue status unsupported"))
    session = _brain_with_memory(memory, tmp_path, timeout_s=5.0)

    t0 = time.perf_counter()
    asyncio.run(session._cold_path("hello", "there"))

    assert (time.perf_counter() - t0) < 2.0
    assert memory.calls == [
        ("write_turn", None, "hello", "there"),
        ("queue_status",),
        ("refresh_cache", None, 30.0),
    ]


def test_cold_path_continues_refreshing_peers_after_one_peer_fails(tmp_path) -> None:
    memory = _ColdMemory(refresh_errors={"project:jarvis": RuntimeError("refresh failed")})
    tracer = _Tracer()
    session = _brain_with_memory(memory, tmp_path, tracer=tracer)
    principal = session._memory_peer()

    asyncio.run(
        session._cold_path(
            "hello",
            "there",
            refresh_peers=("project:jarvis", "contact:klaus"),
        )
    )

    assert memory.calls == [
        ("write_turn", None, "hello", "there"),
        ("queue_status",),
        ("refresh_cache", None, 30.0),
        ("refresh_cache", "project:jarvis", 0.0),
        ("refresh_cache", "contact:klaus", 0.0),
    ]
    assert tracer.emitted == [tracer.trace]
    assert ("memory_refresh_peer_failed", {"peer": "project:jarvis", "error": "RuntimeError"}) in tracer.trace.events
    assert tracer.trace.stages[-1][0] == "memory"
    assert tracer.trace.stages[-1][1]["refreshed_peers"] == [principal, "contact:klaus"]
    assert tracer.trace.stages[-1][1]["failed_refresh_peers"] == [
        {"peer": "project:jarvis", "error": "RuntimeError"}
    ]
