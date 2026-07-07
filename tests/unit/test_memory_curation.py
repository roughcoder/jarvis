from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import httpx

from jarvis.brain.memory_client import ConclusionRecord, RepresentationRecord
from jarvis.brain.memory_client import encode_honcho_id
from jarvis.brain.memory_client.v3 import HonchoV3MemoryClient
from jarvis.brain.memory_outbox import CurationOutbox, RetractionIndex, retraction_index_path
from jarvis.brain.memory_tools import make_memory_tools
from jarvis.brain.registry import ContactEntry, ProjectEntry, RegistryStore
from jarvis.config import MemoryConfig
from jarvis.runtime import RequestContext, ToolRegistry
from conftest import request_context


def _ctx(*caps: str, identity: str = "neil", peer: str = "neil") -> RequestContext:
    return request_context(*caps, identity=identity, scope="personal", peer=peer)


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


def _json(request: httpx.Request) -> dict[str, Any]:
    if not request.content:
        return {}
    return json.loads(request.content.decode("utf-8"))


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


def _contact(**overrides: object) -> ContactEntry:
    values: dict[str, object] = {
        "id": "klaus",
        "display_name": "Klaus Schmidt",
        "owner": "neil",
        "members": ("neil",),
        "visibility": "shared",
    }
    values.update(overrides)
    return ContactEntry(**values)


class FakeMemory:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.fail_create = False
        self.fail_after_create = False
        self.representations: dict[str, str] = {}
        self.query_matches: list[ConclusionRecord] = []
        self.list_matches: list[ConclusionRecord] = []

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
            level=record["metadata"].get("level", "explicit"),
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
                level=row["metadata"].get("level", "explicit"),
                metadata=row["metadata"],
            )
            for i, row in enumerate(self.created, start=1)
        ]
        rows.extend(self.list_matches or self.query_matches)
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


def test_contact_merge_copies_loser_conclusions_to_survivor_through_outbox(tmp_path) -> None:
    sidecar_path = tmp_path / "sidecar.json"
    cfg = _memory_cfg(tmp_path, conclusion_sidecar_path=str(sidecar_path))
    loser_row = {
        "id": "loser-c1",
        "content": "Sarah J prefers email after six.",
        "observer_id": "neil",
        "observed_id": encode_honcho_id("contact:sarah-wa"),
        "level": "explicit",
    }
    survivor_row = {
        "id": "survivor-c1",
        "content": "Sarah Jones lives in Berlin.",
        "observer_id": "neil",
        "observed_id": encode_honcho_id("contact:sarah"),
        "level": "explicit",
    }
    rows = [loser_row, survivor_row]
    created_payloads: list[dict[str, Any]] = []
    sidecar_path.write_text(
        json.dumps(
            {
                cfg.workspace_id: {
                    "loser-c1": {
                        "recorded_by": "neil",
                        "observed_at": "2026-07-04",
                        "channel": "voice",
                        "content_hash": "sha256:loser-original",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/conclusions/list"):
            filters = _json(request).get("filters", {})
            matched = rows
            for key, value in filters.items():
                matched = [row for row in matched if row.get(key) == value]
            return httpx.Response(200, json={"items": matched})
        if request.url.path.endswith("/conclusions") and request.method == "POST":
            payload = _json(request)["conclusions"][0]
            created_payloads.append(payload)
            row = {
                "id": f"copy-{len(created_payloads)}",
                "content": payload["content"],
                "observer_id": payload["observer_id"],
                "observed_id": payload["observed_id"],
                "level": payload["level"],
            }
            rows.append(row)
            return httpx.Response(201, json=[row])
        return httpx.Response(201, json={"id": _json(request).get("id", "")})

    memory = HonchoV3MemoryClient(cfg, transport=httpx.MockTransport(handler))
    outbox = CurationOutbox(cfg.curation_outbox_path, backoff_initial_s=0)
    store = RegistryStore(tmp_path / "registry.json", memory=memory, curation_outbox=outbox)
    store.save_contact(_contact(id="sarah", display_name="Sarah Jones"))
    store.save_contact(_contact(id="sarah-wa", display_name="Sarah J"))

    survivor = store.merge_contacts("sarah", "sarah-wa")
    pending = outbox.pending_entries(observed_id=survivor.peer_id)
    flushed = outbox.flush_sync(memory)

    assert survivor.peer_id == "contact:sarah"
    assert [entry.content for entry in pending] == ["Sarah J prefers email after six."]
    assert flushed == {"delivered": 1, "failed": 0}
    survivor_rows = [
        row for row in rows
        if row["observed_id"] == encode_honcho_id("contact:sarah")
    ]
    assert [row["content"] for row in survivor_rows] == [
        "Sarah Jones lives in Berlin.",
        "Sarah J prefers email after six.",
    ]
    metadata = created_payloads[0]["metadata"]
    assert metadata["recorded_by"] == "neil"
    assert metadata["observed_at"] == "2026-07-04"
    assert metadata["channel"] == "voice"
    assert metadata["copied_from_peer_id"] == "contact:sarah-wa"
    assert metadata["copied_from_conclusion_id"] == "loser-c1"
    assert metadata["copied_from_content_hash"] == "sha256:loser-original"
    assert metadata["copied_reason"] == "contact_merge"
    assert metadata["content_hash"] != "sha256:loser-original"
    sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
    assert sidecar[cfg.workspace_id]["copy-1"]["copied_from_conclusion_id"] == "loser-c1"


def test_contact_merge_copy_delivery_failure_is_retried_by_outbox_without_breaking_merge(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/conclusions/list"):
            filters = _json(request).get("filters", {})
            if filters.get("observed_id") == encode_honcho_id("contact:sarah-wa"):
                return httpx.Response(
                    200,
                    json={
                        "items": [
                            {
                                "id": "loser-c1",
                                "content": "Sarah J prefers email after six.",
                                "observer_id": "neil",
                                "observed_id": encode_honcho_id("contact:sarah-wa"),
                                "level": "explicit",
                            }
                        ]
                    },
                )
            return httpx.Response(503, json={"error": "memory unavailable"})
        return httpx.Response(201, json={"id": _json(request).get("id", "")})

    cfg = _memory_cfg(
        tmp_path,
        conclusion_sidecar_path=str(tmp_path / "sidecar.json"),
    )
    Path(cfg.conclusion_sidecar_path).write_text(
        json.dumps(
            {
                cfg.workspace_id: {
                    "loser-c1": {
                        "recorded_by": "neil",
                        "observed_at": "2026-07-04",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    memory = HonchoV3MemoryClient(cfg, transport=httpx.MockTransport(handler))
    outbox = CurationOutbox(
        cfg.curation_outbox_path,
        max_retries=2,
        backoff_initial_s=0,
    )
    store = RegistryStore(tmp_path / "registry.json", memory=memory, curation_outbox=outbox)
    store.save_contact(_contact(id="sarah", display_name="Sarah Jones"))
    store.save_contact(_contact(id="sarah-wa", display_name="Sarah J"))

    survivor = store.merge_contacts("sarah", "sarah-wa")
    flush_result = outbox.flush_sync(memory)

    assert survivor.id == "sarah"
    assert store.get_contact("sarah-wa").status == "merged"
    assert flush_result == {"delivered": 0, "failed": 1}
    events = [json.loads(line)["event"] for line in Path(cfg.curation_outbox_path).read_text().splitlines()]
    assert events == ["queued", "attempt", "attempt", "failed"]


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


def test_retraction_short_claim_does_not_clear_unrelated_substring(tmp_path) -> None:
    index = RetractionIndex(tmp_path / "retractions.json")
    index.record(
        observed_id="contact:klaus",
        metadata={
            "recorded_by": "neil",
            "observed_at": "2026-07-04",
            "retracted_conclusion_id": "c1",
            "retracted_conclusion_level": "deductive",
            "retracted_content": "Fridays",
            "retraction_reason": "user_forget_request",
        },
    )

    cleared = index.clear_for_assertion(observed_id="contact:klaus", content="Jules likes Fridays.")

    assert cleared == []
    assert [row.retracted_content for row in index.active(observed_id="contact:klaus")] == ["Fridays"]


def test_retraction_substantial_restatement_still_clears(tmp_path) -> None:
    index = RetractionIndex(tmp_path / "retractions.json")
    index.record(
        observed_id="contact:klaus",
        metadata={
            "recorded_by": "neil",
            "observed_at": "2026-07-04",
            "retracted_conclusion_id": "c1",
            "retracted_conclusion_level": "deductive",
            "retracted_content": "Klaus keeps a vintage Leica camera in the study.",
            "retraction_reason": "user_forget_request",
        },
    )

    cleared = index.clear_for_assertion(
        observed_id="contact:klaus",
        content="Klaus keeps a vintage Leica camera in the study, not in the office.",
    )

    assert [row.retracted_conclusion_id for row in cleared] == ["c1"]
    assert index.active(observed_id="contact:klaus") == []


def test_retraction_index_active_reuses_unchanged_read_cache(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    path = tmp_path / "retractions.json"
    index = RetractionIndex(path)
    index.record(
        observed_id="contact:klaus",
        metadata={
            "recorded_by": "neil",
            "observed_at": "2026-07-04",
            "retracted_conclusion_id": "c1",
            "retracted_conclusion_level": "deductive",
            "retracted_content": "Klaus works Fridays.",
            "retraction_reason": "user_forget_request",
        },
    )
    original_read_text = Path.read_text
    reads = 0

    def counted_read_text(self: Path, *args: Any, **kwargs: Any) -> str:
        nonlocal reads
        if self == path:
            reads += 1
        return original_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counted_read_text)

    assert len(index.active(observed_id="contact:klaus")) == 1
    assert len(index.active(observed_id="contact:klaus")) == 1
    assert reads == 0


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
    assert entries[1].content == "Use Honcho v3."
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
    assert done == "Corrected."
    pending = outbox.pending_entries()
    assert [entry.operation for entry in pending] == ["delete_conclusion", "create_conclusion"]


def test_forget_confirmation_query_sends_self_scoped_semantic_filters(tmp_path) -> None:
    query_payloads: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/conclusions/query"):
            query_payloads.append(json.loads(request.content.decode("utf-8")))
            return httpx.Response(200, json=[])
        return httpx.Response(201, json={"id": "ok"})

    cfg = _memory_cfg(tmp_path, conclusion_sidecar_path=str(tmp_path / "sidecar.json"))
    memory = HonchoV3MemoryClient(cfg, transport=httpx.MockTransport(handler))
    tool = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=memory,
            outbox=CurationOutbox(cfg.curation_outbox_path),
            registry=_registry(tmp_path),
        )
    }["forget_memory"]

    asyncio.run(tool.handler(_ctx("memory.curate"), {"target": "Klaus", "query": "Fridays"}))

    assert query_payloads == [
        {
            "query": "Fridays",
            "filters": {
                "observed": encode_honcho_id("contact:klaus"),
                "observer": encode_honcho_id("contact:klaus"),
            },
            "top_k": 5,
        }
    ]


def test_forget_confirm_uses_id_stable_lookup_not_second_semantic_query(tmp_path) -> None:
    backend = FakeMemory()
    chosen = ConclusionRecord(
        id="c1",
        content="Klaus works Fridays.",
        observer_id="contact:klaus",
        observed_id="contact:klaus",
        level="explicit",
    )
    backend.query_matches = [chosen]
    backend.list_matches = [chosen]
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
    tool = {
        tool.name: tool
        for tool in make_memory_tools(
            cfg,
            memory=backend,
            outbox=outbox,
            registry=_registry(tmp_path),
        )
    }["forget_memory"]
    ctx = _ctx("memory.curate")

    ask = asyncio.run(tool.handler(ctx, {"target": "Klaus", "query": "Fridays"}))
    backend.query_matches = []
    done = asyncio.run(
        tool.handler(
            ctx,
            {
                "target": "Klaus",
                "query": "Fridays",
                "confirm": True,
                "conclusion_ids": ["c1"],
            },
        )
    )

    assert "c1: Klaus works Fridays." in ask
    assert done == "Forgotten."
    assert [entry.conclusion_id for entry in outbox.pending_entries(observed_id="contact:klaus")] == ["c1"]


def test_forget_derived_conclusion_queues_contradiction_retraction(tmp_path) -> None:
    backend = FakeMemory()
    backend.query_matches = [
        ConclusionRecord(
            id="c1",
            content="Klaus works Fridays.",
            observer_id="neil",
            observed_id="contact:klaus",
            level="deductive",
        )
    ]
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
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
            {
                "target": "Klaus",
                "query": "Fridays",
                "confirm": True,
                "conclusion_ids": ["c1"],
            },
        )
    )
    pending = outbox.pending_entries(observed_id="contact:klaus")
    flushed = outbox.flush_sync(backend)

    assert result == "Forgotten."
    assert [entry.operation for entry in pending] == ["delete_conclusion", "create_conclusion"]
    retraction = pending[1]
    assert retraction.metadata is not None
    assert retraction.metadata["level"] == "contradiction"
    assert retraction.metadata["source"] == "forget"
    assert retraction.metadata["recorded_by"] == "neil"
    assert retraction.metadata["observed_at"]
    assert retraction.metadata["retracted_conclusion_id"] == "c1"
    assert retraction.metadata["retracted_conclusion_level"] == "deductive"
    assert retraction.metadata["retracted_content"] == "Klaus works Fridays."
    assert "does not want it retained as current" in retraction.content
    active = RetractionIndex(retraction_index_path(cfg.curation_outbox_path)).active(observed_id="contact:klaus")
    assert len(active) == 1
    assert active[0].retracted_conclusion_id == "c1"
    assert active[0].retracted_content == "Klaus works Fridays."
    assert flushed == {"delivered": 2, "failed": 0}
    assert backend.deleted == ["c1"]
    assert backend.created[0]["metadata"]["level"] == "contradiction"


def test_forget_declared_conclusion_deletes_without_retraction(tmp_path) -> None:
    backend = FakeMemory()
    backend.query_matches = [
        ConclusionRecord(
            id="c1",
            content="Klaus works Fridays.",
            observer_id="neil",
            observed_id="contact:klaus",
            level="explicit",
        )
    ]
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
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
            {
                "target": "Klaus",
                "query": "Fridays",
                "confirm": True,
                "conclusion_ids": ["c1"],
            },
        )
    )
    pending = outbox.pending_entries(observed_id="contact:klaus")
    flushed = outbox.flush_sync(backend)

    assert result == "Forgotten."
    assert [entry.operation for entry in pending] == ["delete_conclusion"]
    assert RetractionIndex(retraction_index_path(cfg.curation_outbox_path)).active(
        observed_id="contact:klaus"
    ) == []
    assert flushed == {"delivered": 1, "failed": 0}
    assert backend.deleted == ["c1"]
    assert backend.created == []


def test_correct_derived_conclusion_retracts_then_writes_replacement(tmp_path) -> None:
    backend = FakeMemory()
    backend.query_matches = [
        ConclusionRecord(
            id="c1",
            content="Klaus works Fridays.",
            observer_id="neil",
            observed_id="contact:klaus",
            level="inductive",
        )
    ]
    cfg = _memory_cfg(tmp_path)
    outbox = CurationOutbox(cfg.curation_outbox_path)
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
                "query": "Fridays",
                "replacement": "Klaus is off Fridays.",
                "confirm": True,
                "conclusion_ids": ["c1"],
            },
        )
    )
    pending = outbox.pending_entries(observed_id="contact:klaus")

    assert result == "Corrected."
    assert [entry.operation for entry in pending] == [
        "delete_conclusion",
        "create_conclusion",
        "create_conclusion",
    ]
    assert pending[1].metadata is not None
    assert pending[1].metadata["level"] == "contradiction"
    assert pending[2].content == "Klaus is off Fridays."
    assert pending[2].metadata is not None
    assert pending[2].metadata.get("level") is None
    active = RetractionIndex(retraction_index_path(cfg.curation_outbox_path)).active(observed_id="contact:klaus")
    assert [row.retracted_content for row in active] == ["Klaus works Fridays."]


def test_correct_derived_conclusion_reassertion_clears_matching_retraction(tmp_path) -> None:
    backend = FakeMemory()
    backend.query_matches = [
        ConclusionRecord(
            id="c2",
            content="Klaus works Fridays.",
            observer_id="neil",
            observed_id="contact:klaus",
            level="deductive",
        )
    ]
    cfg = _memory_cfg(tmp_path)
    index = RetractionIndex(retraction_index_path(cfg.curation_outbox_path))
    index.record(
        observed_id="contact:klaus",
        metadata={
            "recorded_by": "neil",
            "observed_at": "2026-07-04",
            "retracted_conclusion_id": "c1",
            "retracted_conclusion_level": "deductive",
            "retracted_content": "Klaus works Fridays.",
            "retraction_reason": "user_forget_request",
        },
    )
    outbox = CurationOutbox(cfg.curation_outbox_path)
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
                "query": "Fridays",
                "replacement": "Klaus works Fridays.",
                "confirm": True,
                "conclusion_ids": ["c2"],
            },
        )
    )
    pending = outbox.pending_entries(observed_id="contact:klaus")

    assert result == "Corrected."
    assert [entry.operation for entry in pending] == ["delete_conclusion", "create_conclusion"]
    assert pending[1].content == "Klaus works Fridays."
    assert RetractionIndex(retraction_index_path(cfg.curation_outbox_path)).active(
        observed_id="contact:klaus"
    ) == []


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

    assert result == "Forgotten."
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

    assert result == "Corrected."
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

    assert "withdrawals below are authoritative" not in result
    assert "answer: Fridays?" in result


def test_memory_search_includes_active_retractions_for_peer(tmp_path) -> None:
    backend = FakeMemory()
    cfg = _memory_cfg(tmp_path)
    RetractionIndex(retraction_index_path(cfg.curation_outbox_path)).record(
        observed_id="contact:klaus",
        metadata={
            "recorded_by": "neil",
            "observed_at": "2026-07-05",
            "retracted_conclusion_id": "c1",
            "retracted_conclusion_level": "deductive",
            "retracted_content": "Klaus works Fridays.",
            "retraction_reason": "user_forget_request",
        },
    )
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
        tool.handler(_ctx("memory.query"), {"target": "Klaus", "search_query": "Does Klaus work Friday?"})
    )

    assert "withdrawals below are authoritative" in result
    assert "semantically equivalent restatements" in result
    assert "The user has retracted / withdrawn these memory claims" in result
    assert "Klaus works Fridays." in result
    assert "answer: Does Klaus work Friday?" in result
