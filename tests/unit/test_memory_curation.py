from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from jarvis.brain.memory_client import ConclusionRecord, RepresentationRecord
from jarvis.brain.memory_outbox import CurationOutbox
from jarvis.brain.memory_tools import make_memory_tools
from jarvis.brain.registry import ContactEntry, ProjectEntry, RegistryStore
from jarvis.config import MemoryConfig
from jarvis.runtime import RequestContext, ToolRegistry


def _ctx(*caps: str, identity: str = "neil", peer: str = "neil") -> RequestContext:
    return RequestContext(
        "dev",
        identity,
        "personal",
        frozenset(caps),
        channel="voice",
        peer=peer,
    )


def _memory_cfg(tmp_path: Path, **over: Any) -> MemoryConfig:
    values = {
        "backend": "v3",
        "cache_path": str(tmp_path / "cache.json"),
        "curation_outbox_path": str(tmp_path / "outbox.jsonl"),
        "tool_timeout_s": 0.05,
        "curation_outbox_backoff_initial_s": 0,
        "curation_outbox_backoff_max_s": 0,
    }
    values.update(over)
    return MemoryConfig(
        _env_file=None,
        **values,
    )


def _registry(tmp_path: Path) -> RegistryStore:
    store = RegistryStore(tmp_path / "registry.json")
    store.save_contact(
        ContactEntry(
            id="klaus",
            display_name="Klaus Schmidt",
            aliases=("Klaus",),
            owner="neil",
            visibility="shared",
            members=("neil", "jules"),
        )
    )
    store.save_project(
        ProjectEntry(
            id="jarvis",
            name="Jarvis",
            aliases=("the jarvis project",),
            owner="neil",
            members=("neil", "jules"),
            visibility="shared",
        )
    )
    return store


class FakeMemory:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.fail_create = False
        self.fail_after_create = False
        self.representations: dict[str, str] = {}
        self.query_matches: list[ConclusionRecord] = []

    def read_cached_representation(self, user: str | None = None) -> str:
        return self.representations.get(user or "neil", "")

    def create_conclusion(
        self,
        *,
        observed_id: str,
        content: str,
        observer_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ConclusionRecord:
        if self.fail_create:
            raise TimeoutError("memory down")
        record = {
            "observed_id": observed_id,
            "content": content,
            "observer_id": observer_id or "jarvis",
            "metadata": dict(metadata or {}),
        }
        self.created.append(record)
        conclusion = ConclusionRecord(
            id=f"c{len(self.created)}",
            content=content,
            observer_id=record["observer_id"],
            observed_id=observed_id,
            metadata=record["metadata"],
        )
        if self.fail_after_create:
            self.fail_after_create = False
            raise TimeoutError("ambiguous timeout")
        return conclusion

    def list_conclusions(
        self,
        *,
        observed_id: str | None = None,
        observer_id: str | None = None,
        session_id: str | None = None,
        level: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[ConclusionRecord]:
        rows = [
            ConclusionRecord(
                id=f"c{i}",
                content=row["content"],
                observer_id=row["observer_id"],
                observed_id=row["observed_id"],
                metadata=row["metadata"],
            )
            for i, row in enumerate(self.created, start=1)
        ]
        if observed_id:
            rows = [row for row in rows if row.observed_id == observed_id]
        if observer_id:
            rows = [row for row in rows if row.observer_id == observer_id]
        if level:
            rows = [row for row in rows if row.level == level]
        if metadata:
            rows = [
                row for row in rows
                if all(row.metadata.get(key) == value for key, value in metadata.items())
            ]
        return rows

    def query_conclusions(self, query: str, **kwargs: Any) -> list[ConclusionRecord]:
        return self.query_matches

    def delete_conclusion(self, conclusion_id: str) -> None:
        self.deleted.append(conclusion_id)

    def read_representation(self, peer_id: str, **kwargs: Any) -> RepresentationRecord:
        if peer_id == "dead":
            raise TimeoutError("down")
        return RepresentationRecord(peer_id=peer_id, representation=self.representations.get(peer_id, "live"))

    def dialectic_chat(self, peer_id: str, query: str, **kwargs: Any) -> str:
        if peer_id == "dead":
            raise TimeoutError("down")
        return f"answer: {query}"


def test_outbox_append_flush_retry_and_idempotency(tmp_path) -> None:
    backend = FakeMemory()
    backend.fail_after_create = True
    outbox = CurationOutbox(tmp_path / "outbox.jsonl", max_retries=2, backoff_initial_s=0)
    entry = outbox.enqueue_create(
        observed_id="contact:klaus",
        observer_id="neil",
        content="Klaus is off Fridays.",
        metadata={"recorded_by": "neil", "observed_at": "2026-07-04"},
    )

    result = outbox.flush_sync(backend)
    second = outbox.flush_sync(backend)

    assert result == {"delivered": 1, "failed": 0}
    assert second == {"delivered": 0, "failed": 0}
    assert len(backend.created) == 1
    assert backend.created[0]["metadata"]["content_hash"] == entry.content_hash
    events = [json.loads(line)["event"] for line in (tmp_path / "outbox.jsonl").read_text().splitlines()]
    assert events == ["queued", "attempt", "delivered"]


def test_outbox_retry_exhaustion_notifies_once_per_failed_entry(tmp_path) -> None:
    backend = FakeMemory()
    backend.fail_create = True
    outbox = CurationOutbox(tmp_path / "outbox.jsonl", max_retries=2, backoff_initial_s=0)
    outbox.enqueue_create(
        observed_id="contact:klaus",
        observer_id="neil",
        content="Klaus is off Fridays.",
        metadata={"observed_at": "2026-07-04"},
    )
    outbox.enqueue_create(
        observed_id="project:jarvis",
        observer_id="neil",
        content="Decision: use Honcho.",
        metadata={"observed_at": "2026-07-04"},
    )
    notifications: list[str] = []

    first = outbox.flush_sync(backend, notify=notifications.append)
    second = outbox.flush_sync(backend, notify=notifications.append)

    assert first == {"delivered": 0, "failed": 2}
    assert second == {"delivered": 0, "failed": 0}
    assert len(notifications) == 2


def test_outbox_cancellation_suppresses_pending_lines_and_delivery(tmp_path) -> None:
    backend = FakeMemory()
    outbox = CurationOutbox(tmp_path / "outbox.jsonl")
    outbox.enqueue_create(
        observed_id="contact:klaus",
        observer_id="neil",
        content="Klaus is off Fridays.",
        metadata={"observed_at": "2026-07-04"},
    )

    cancelled = outbox.cancel_pending(
        observed_id="contact:klaus",
        content="Klaus is off Fridays.",
    )
    result = outbox.flush_sync(backend)

    assert len(cancelled) == 1
    assert outbox.pending_entries() == []
    assert outbox.pending_lines(observed_id="contact:klaus") == []
    assert result == {"delivered": 0, "failed": 0}
    assert backend.created == []


def test_outbox_cancel_pending_matches_semantic_forget_query(tmp_path) -> None:
    outbox = CurationOutbox(tmp_path / "outbox.jsonl")
    outbox.enqueue_create(
        observed_id="contact:klaus",
        observer_id="neil",
        content="Klaus is off Fridays.",
        metadata={"observed_at": "2026-07-04"},
    )

    cancelled = outbox.cancel_pending(observed_id="contact:klaus", content="Fridays")

    assert len(cancelled) == 1
    assert outbox.pending_entries() == []


def test_outbox_pending_entries_uses_compacted_state_for_delivered_history(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    path = tmp_path / "outbox.jsonl"
    backend = FakeMemory()
    outbox = CurationOutbox(path)
    for index in range(25):
        outbox.enqueue_create(
            observed_id="contact:klaus",
            observer_id="neil",
            content=f"Klaus fact {index}.",
            metadata={"observed_at": "2026-07-04"},
        )
        assert outbox.flush_sync(backend) == {"delivered": 1, "failed": 0}

    original_read_text = Path.read_text

    def guarded_read_text(self: Path, *args: Any, **kwargs: Any) -> str:
        if self == path:
            raise AssertionError("pending lookup reparsed delivered journal")
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", guarded_read_text)

    assert CurationOutbox(path).pending_entries() == []


def test_outbox_pending_read_your_writes_lines_include_forgets(tmp_path) -> None:
    outbox = CurationOutbox(tmp_path / "outbox.jsonl")
    outbox.enqueue_create(
        observed_id="contact:klaus",
        observer_id="neil",
        content="Klaus is off Fridays.",
        metadata={"observed_at": "2026-07-04"},
    )
    outbox.enqueue_delete(conclusion_id="c1", observed_id="contact:klaus", content="old address")

    text = outbox.append_pending_lines("cached", observed_id="contact:klaus")

    assert "pending, not yet saved: Klaus is off Fridays." in text
    assert "pending, not yet saved: forget old address" in text


def test_memory_capabilities_make_tools_available_on_v3(tmp_path) -> None:
    cfg = _memory_cfg(tmp_path)
    registry = ToolRegistry()
    for tool in make_memory_tools(
        cfg,
        memory=FakeMemory(),
        outbox=CurationOutbox(cfg.curation_outbox_path),
        registry=_registry(tmp_path),
    ):
        registry.register(tool)

    available = {
        tool.name
        for tool in registry.available_for(_ctx("memory.query", "memory.curate"))
    }

    assert {
        "memory_search",
        "remember_contact",
        "forget_memory",
        "correct_memory",
        "add_finding",
        "record_decision",
    } <= available


def test_v2_backend_omits_curation_write_tools_and_enqueues_nothing(tmp_path) -> None:
    cfg = _memory_cfg(tmp_path, backend="v2")
    outbox = CurationOutbox(cfg.curation_outbox_path)

    tools = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=FakeMemory(),
            outbox=outbox,
            registry=_registry(tmp_path),
        )
    }

    assert set(tools) == {"memory_search"}
    assert outbox.pending_entries() == []


def test_curation_tool_queues_contact_conclusion_with_observed_at(tmp_path) -> None:
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
    tools = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=FakeMemory(),
            outbox=outbox,
            registry=_registry(tmp_path),
        )
    }

    result = asyncio.run(
        tools["remember_contact"].handler(
            _ctx("memory.curate"),
            {"contact": "Klaus", "fact": "Klaus is off Fridays."},
        )
    )

    assert result.startswith("Noted")
    entry = outbox.pending_entries()[0]
    assert entry.observed_id == "contact:klaus"
    assert entry.metadata["recorded_by"] == "neil"
    assert entry.metadata["source"] == "spoken"
    assert entry.metadata["channel"] == "voice"
    assert entry.metadata["observed_at"]


def test_project_finding_and_decision_payload_shape(tmp_path) -> None:
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
    tools = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=FakeMemory(),
            outbox=outbox,
            registry=_registry(tmp_path),
        )
    }
    ctx = _ctx("memory.curate")

    asyncio.run(tools["add_finding"].handler(ctx, {"project": "jarvis", "content": "Cache is fast."}))
    asyncio.run(tools["record_decision"].handler(ctx, {"project": "jarvis", "content": "Use Honcho v3."}))

    entries = outbox.pending_entries()
    assert entries[0].metadata["project_id"] == "jarvis"
    assert entries[0].metadata["artifact_type"] == "finding"
    assert entries[0].metadata["status"] == "open"
    assert entries[1].content == "Decision: Use Honcho v3."
    assert entries[1].metadata["artifact_type"] == "decision"
    assert entries[1].metadata["status"] == "accepted"


def test_weak_identity_contact_creation_requires_confirmation(tmp_path) -> None:
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
    tool = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=FakeMemory(),
            outbox=outbox,
            registry=_registry(tmp_path),
        )
    }["remember_contact"]

    result = asyncio.run(
        tool.handler(_ctx("memory.curate"), {"contact": "Zelda", "fact": "Off Fridays."})
    )

    assert result.startswith("confirmation required: create a contact")
    assert outbox.pending_entries() == []


def test_forget_and_correct_confirmation_roundtrip(tmp_path) -> None:
    backend = FakeMemory()
    backend.query_matches = [
        ConclusionRecord(
            id="c1",
            content="Klaus works Fridays.",
            observer_id="neil",
            observed_id="contact:klaus",
        )
    ]
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
    tools = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=backend,
            outbox=outbox,
            registry=_registry(tmp_path),
        )
    }
    ctx = _ctx("memory.curate")

    ask = asyncio.run(tools["correct_memory"].handler(ctx, {"target": "Klaus", "query": "Fridays", "replacement": "Klaus is off Fridays."}))
    done = asyncio.run(
        tools["correct_memory"].handler(
            ctx,
            {
                "target": "Klaus",
                "query": "Fridays",
                "replacement": "Klaus is off Fridays.",
                "confirm": True,
                "conclusion_ids": ["c1"],
            },
        )
    )

    assert "confirmation required" in ask and "c1: Klaus works Fridays." in ask
    assert done.startswith("Noted")
    pending = outbox.pending_entries()
    assert [entry.operation for entry in pending] == ["delete_conclusion", "create_conclusion"]


def test_forget_pending_memory_cancels_outbox_entry_before_flush(tmp_path) -> None:
    backend = FakeMemory()
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
    outbox.enqueue_create(
        observed_id="contact:klaus",
        observer_id="neil",
        content="Klaus is off Fridays.",
        metadata={"observed_at": "2026-07-04"},
    )
    tool = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=backend,
            outbox=outbox,
            registry=_registry(tmp_path),
        )
    }["forget_memory"]

    result = asyncio.run(
        tool.handler(
            _ctx("memory.curate"),
            {"target": "Klaus", "query": "Fridays"},
        )
    )
    flushed = outbox.flush_sync(backend)

    assert result == "Noted - cancelled pending memory."
    assert outbox.pending_lines(observed_id="contact:klaus") == []
    assert flushed == {"delivered": 0, "failed": 0}
    assert backend.created == []


def test_correct_pending_memory_replaces_outbox_entry_before_flush(tmp_path) -> None:
    backend = FakeMemory()
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
    outbox.enqueue_create(
        observed_id="contact:klaus",
        observer_id="neil",
        content="Klaus works Fridays.",
        metadata={"observed_at": "2026-07-04"},
    )
    tool = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=backend,
            outbox=outbox,
            registry=_registry(tmp_path),
        )
    }["correct_memory"]

    result = asyncio.run(
        tool.handler(
            _ctx("memory.curate"),
            {
                "target": "Klaus",
                "query": "Klaus works Fridays.",
                "replacement": "Klaus is off Fridays.",
            },
        )
    )
    pending = outbox.pending_entries(observed_id="contact:klaus")
    flushed = outbox.flush_sync(backend)

    assert result == "Noted - replaced pending memory."
    assert [entry.content for entry in pending] == ["Klaus is off Fridays."]
    assert flushed == {"delivered": 1, "failed": 0}
    assert [row["content"] for row in backend.created] == ["Klaus is off Fridays."]


def test_memory_search_degrades_to_cache_and_pending_lines_when_backend_dead(tmp_path) -> None:
    backend = FakeMemory()
    backend.representations["dead"] = "cached memory"
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
    outbox.enqueue_create(
        observed_id="dead",
        observer_id="neil",
        content="queued fact",
        metadata={"observed_at": "2026-07-04"},
    )
    tool = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=backend,
            outbox=outbox,
            registry=_registry(tmp_path),
        )
    }["memory_search"]

    result = asyncio.run(
        tool.handler(
            _ctx("memory.query", identity="dead", peer="dead"),
            {"target": "dead", "search_query": "what?"},
        )
    )

    assert "cached memory" in result
    assert "pending, not yet saved: queued fact" in result
    assert "memory is unreachable" in result


def test_memory_search_queries_live_when_backend_available(tmp_path) -> None:
    backend = FakeMemory()
    cfg = _memory_cfg(tmp_path)
    tool = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=backend,
            outbox=CurationOutbox(cfg.curation_outbox_path),
            registry=_registry(tmp_path),
        )
    }["memory_search"]

    result = asyncio.run(
        tool.handler(_ctx("memory.query"), {"target": "Klaus", "search_query": "Fridays?"})
    )

    assert result == "answer: Fridays?"
