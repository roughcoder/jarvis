from __future__ import annotations

import json
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("openai")

from jarvis.brain.capabilities import RequestContext
from jarvis.brain.memory_client import MemoryMessage, SessionPeer
from jarvis.brain.registry import ProjectEntry
from jarvis.config import Config
from jarvis.connectors.cockpit import CockpitConnector, CockpitThread, CockpitThreadIndex, orchestrator_session_id


class FakeMemory:
    def __init__(self) -> None:
        self.sessions: list[dict[str, Any]] = []
        self.messages: list[dict[str, Any]] = []
        self.operations: list[dict[str, Any]] = []

    def create_session(
        self,
        session_id: str,
        *,
        peers: list[SessionPeer] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        row = {
            "session_id": session_id,
            "peers": [peer.peer_id for peer in peers or []],
            "peer_configs": {
                peer.peer_id: {
                    "observe_me": peer.observe_me,
                    "observe_others": peer.observe_others,
                }
                for peer in peers or []
            },
            "messages_before_create": len(self.messages),
            "metadata": dict(metadata or {}),
        }
        self.sessions.append(row)
        self.operations.append({"kind": "create_session", **row})

    def create_messages(self, session_id: str, messages: list[MemoryMessage]) -> list[dict[str, Any]]:
        rows = [
            {
                "session_id": session_id,
                "peer_id": message.peer_id,
                "content": message.content,
                "metadata": dict(message.metadata),
            }
            for message in messages
        ]
        self.messages.extend(rows)
        self.operations.append({"kind": "create_messages", "session_id": session_id, "messages": rows})
        return rows


def _cfg(tmp_path: Path, monkeypatch) -> Config:  # noqa: ANN001
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                f"ORCHESTRATION_WORKSPACE={tmp_path / 'orchestration'}",
                f"REGISTRY_PATH={tmp_path / 'registry.json'}",
                f"MEMORY_CACHE_PATH={tmp_path / 'memory-cache.json'}",
                f"MEMORY_CURATION_OUTBOX_PATH={tmp_path / 'outbox.jsonl'}",
            ]
        )
    )
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env))
    return Config()


def test_connector_opens_honcho_thread_session_before_messages(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    memory = FakeMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=object(), tts=None, tracer=None)
    project = ProjectEntry(
        id="jarvis",
        name="Jarvis",
        owner="neil",
        members=("neil",),
        visibility="private",
    )
    requester = RequestContext(
        "dev",
        "neil",
        "personal",
        frozenset(),
        channel="cockpit",
        peer="neil",
    )

    import asyncio

    thread = asyncio.run(connector.open_thread(project, requester, title="Planning"))

    assert thread.session_id == orchestrator_session_id("jarvis", thread.thread_id)
    assert memory.sessions == [
        {
            "session_id": thread.session_id,
            "peers": ["project:jarvis", "neil", "jarvis"],
            "peer_configs": {
                "project:jarvis": {"observe_me": True, "observe_others": True},
                "neil": {"observe_me": True, "observe_others": True},
                "jarvis": {"observe_me": False, "observe_others": True},
            },
            "messages_before_create": 0,
            "metadata": {
                "kind": "cockpit_orchestrator",
                "chat_type": "assistant",
                "engine": "jarvis",
                "model": "",
                "project_id": "jarvis",
                "thread_id": thread.thread_id,
                "created_by": "neil",
                "parent_chat_id": "",
                "created_at": thread.created_at,
            },
        }
    ]
    assert memory.messages == []
    index = json.loads((tmp_path / "orchestration" / "cockpit-threads.json").read_text())
    assert index["threads"][thread.thread_id]["session_id"] == thread.session_id
    assert "messages" not in index["threads"][thread.thread_id]


def test_connector_adds_turn_author_to_thread_session_before_messages(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    memory = FakeMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=object(), tts=None, tracer=None)
    project = ProjectEntry(
        id="jarvis",
        name="Jarvis",
        owner="neil",
        members=("neil", "riley"),
        visibility="private",
    )
    opener = RequestContext(
        "dev",
        "neil",
        "personal",
        frozenset(),
        channel="cockpit",
        peer="neil",
    )

    import asyncio

    thread = asyncio.run(connector.open_thread(project, opener, title="Planning"))
    connector._persist_turn(
        thread.session_id,
        "riley",
        "riley-laptop",
        "Can I add the follow-up?",
        "Yes.",
    )

    assert [operation["kind"] for operation in memory.operations[-2:]] == [
        "create_session",
        "create_messages",
    ]
    membership = memory.operations[-2]
    assert membership["session_id"] == thread.session_id
    assert membership["peers"] == ["riley"]
    assert membership["peer_configs"]["riley"] == {"observe_me": True, "observe_others": True}
    message_write = memory.operations[-1]
    assert [message["peer_id"] for message in message_write["messages"]] == ["riley", "jarvis"]
    assert message_write["messages"][0]["metadata"]["channel"] == "cockpit"
    assert message_write["messages"][0]["metadata"]["device_id"] == "riley-laptop"


def test_connector_returns_before_cold_task_finishes_and_logs_failures(tmp_path, monkeypatch, caplog) -> None:  # noqa: ANN001
    class DeferredSession:
        def __init__(self, gate: asyncio.Event, *, fail: bool = False) -> None:
            self._gate = gate
            self._fail = fail
            self._tasks: set[asyncio.Task] = set()

        @property
        def pending_cold_tasks(self) -> tuple[asyncio.Task, ...]:
            return tuple(self._tasks)

        async def respond_text(self, _text, _trace, result, *, attachments=None, on_text=None):  # noqa: ANN001
            result.raw = "Ready."
            return result.raw

        def finalize(self, _text, result, _trace) -> None:  # noqa: ANN001
            result.reply = result.raw

            async def cold() -> None:
                await self._gate.wait()
                if self._fail:
                    raise RuntimeError("cold task failed")

            task = asyncio.create_task(cold())
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def verify() -> None:
        cfg = _cfg(tmp_path, monkeypatch)
        memory = FakeMemory()
        connector = CockpitConnector(cfg, memory=memory, gateway=object(), tts=None, tracer=None)
        project = ProjectEntry(id="jarvis", name="Jarvis", owner="neil", members=("neil",), visibility="private")
        requester = RequestContext("dev", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
        thread = await connector.open_thread(project, requester, title="Planning")
        gate = asyncio.Event()
        session = DeferredSession(gate, fail=True)
        monkeypatch.setattr(connector, "_make_session", lambda *_args, **_kwargs: session)

        reply, _updated, _events = await asyncio.wait_for(
            connector.turn(project, thread, requester, "Please answer."), timeout=0.1
        )

        assert reply == "Ready."
        assert session.pending_cold_tasks
        gate.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    import asyncio

    with caplog.at_level("ERROR", logger="jarvis.connectors.cockpit"):
        asyncio.run(verify())

    assert "cockpit cold task failed" in caplog.text


def test_thread_index_archive_round_trip_and_filtering(tmp_path) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    active = index.save(
        CockpitThread(
            thread_id="thread_active",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_active",
            title="Active",
            created_at="2026-07-05T09:00:00+00:00",
            updated_at="2026-07-05T09:00:00+00:00",
            created_by="neil",
        )
    )
    archived = index.save(
        CockpitThread(
            thread_id="thread_archived",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_archived",
            title="Archived",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )

    archived = index.set_archived("jarvis", archived.thread_id, archived=True, by="neil", reason="  done  ")
    assert archived is not None
    assert archived.archived_at
    assert archived.archived_by == "neil"
    assert archived.archive_reason == "done"
    assert [thread.thread_id for thread in index.list("jarvis")] == [active.thread_id]
    assert {thread.thread_id for thread in index.list("jarvis", include_archived=True)} == {
        active.thread_id,
        archived.thread_id,
    }

    archived_again = index.set_archived("jarvis", archived.thread_id, archived=True, by="riley", reason="new")
    assert archived_again is not None
    assert archived_again.archived_at == archived.archived_at
    assert archived_again.archived_by == "neil"
    assert archived_again.archive_reason == "done"

    restored = index.set_archived("jarvis", archived.thread_id, archived=False)
    assert restored is not None
    assert restored.archived_at == ""
    assert restored.archived_by == ""
    assert restored.archive_reason == ""
    assert {thread.thread_id for thread in index.list("jarvis")} == {active.thread_id, archived.thread_id}


def test_thread_index_archive_promotes_child_threads(tmp_path) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    parent = index.save(
        CockpitThread(
            thread_id="thread_parent",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_parent",
            title="Parent",
            created_at="2026-07-05T09:00:00+00:00",
            updated_at="2026-07-05T09:00:00+00:00",
            created_by="neil",
        )
    )
    child = index.save(
        CockpitThread(
            thread_id="thread_child",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_child",
            title="Child",
            created_at="2026-07-05T09:01:00+00:00",
            updated_at="2026-07-05T09:01:00+00:00",
            created_by="neil",
            parent_chat_id=parent.thread_id,
        )
    )

    index.set_archived("jarvis", parent.thread_id, archived=True, by="neil")

    promoted = index.get("jarvis", child.thread_id)
    assert promoted is not None
    assert promoted.parent_chat_id == ""
    assert promoted.archived_at == ""


def test_thread_index_append_turn_preserves_mid_turn_archive_fields(tmp_path) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    turn_snapshot = index.save(
        CockpitThread(
            thread_id="thread_archived",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_archived",
            title="Archived",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )
    archived = index.set_archived("jarvis", turn_snapshot.thread_id, archived=True, by="neil", reason="done")
    assert archived is not None

    updated = index.append_turn(
        turn_snapshot,
        user_peer_id="neil",
        user_text="one more note",
        assistant_peer_id="jarvis",
        assistant_text="noted",
    )

    assert updated.archived_at == archived.archived_at
    assert updated.archived_by == "neil"
    assert updated.archive_reason == "done"
    assert len(updated.messages) == 2
    assert index.get("jarvis", turn_snapshot.thread_id).messages == ()
    assert index.get_with_messages("jarvis", turn_snapshot.thread_id).messages == updated.messages


def test_thread_index_retains_full_local_history_for_detail_views(tmp_path) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    thread = index.save(
        CockpitThread(
            thread_id="thread_history",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_history",
            title="History",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )

    for turn in range(13):
        thread = index.append_turn(
            thread,
            user_peer_id="neil",
            user_text=f"user {turn}",
            assistant_peer_id="jarvis",
            assistant_text=f"assistant {turn}",
        )

    assert len(thread.messages) == 26
    assert thread.messages[0]["content"] == "user 0"
    assert thread.messages[-1]["content"] == "assistant 12"
    stored = json.loads((tmp_path / "threads.json").read_text())
    assert "messages" not in stored["threads"][thread.thread_id]
    assert index.get("jarvis", thread.thread_id).messages == ()
    assert len(index.get_with_messages("jarvis", thread.thread_id).messages) == 26
    limited = index.get_with_messages("jarvis", thread.thread_id, limit=2)
    assert limited is not None
    assert [message["content"] for message in limited.messages] == ["user 12", "assistant 12"]


def test_thread_index_migrates_legacy_embedded_messages_to_transcript_file(tmp_path) -> None:
    index_path = tmp_path / "threads.json"
    index_path.write_text(
        json.dumps(
            {
                "version": 1,
                "threads": {
                    "thread_legacy": {
                        "thread_id": "thread_legacy",
                        "project_id": "jarvis",
                        "session_id": "project:jarvis:orchestrator:thread_legacy",
                        "title": "Legacy",
                        "created_at": "2026-07-05T08:00:00+00:00",
                        "updated_at": "2026-07-05T08:00:00+00:00",
                        "created_by": "neil",
                        "messages": [
                            {
                                "role": "user",
                                "peer_id": "neil",
                                "content": "legacy question",
                                "observed_at": "2026-07-05T08:01:00+00:00",
                            }
                        ],
                    }
                },
            }
        )
    )
    index = CockpitThreadIndex(index_path)

    listed = index.list("jarvis")
    detail = index.get_with_messages("jarvis", "thread_legacy")
    compacted = json.loads(index_path.read_text())

    assert [thread.thread_id for thread in listed] == ["thread_legacy"]
    assert listed[0].messages == ()
    assert detail is not None
    assert [message["content"] for message in detail.messages] == ["legacy question"]
    assert "messages" not in compacted["threads"]["thread_legacy"]


def test_thread_index_appends_jsonl_and_round_trips_messages(tmp_path) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    thread = index.save(
        CockpitThread(
            thread_id="thread_jsonl",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_jsonl",
            title="JSONL",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )

    updated = index.append_turn(
        thread,
        user_peer_id="neil",
        user_text="question",
        assistant_peer_id="jarvis",
        assistant_text="answer",
    )
    path = index._transcript_path("jarvis", thread.thread_id)

    assert path.suffix == ".jsonl"
    assert [json.loads(line)["content"] for line in path.read_text().splitlines()] == ["question", "answer"]
    assert index.get_with_messages("jarvis", thread.thread_id) == updated


def test_thread_index_persists_seed_messages_when_creating_a_transcript(tmp_path) -> None:
    index_path = tmp_path / "threads.json"
    index = CockpitThreadIndex(index_path)
    thread = CockpitThread(
        thread_id="thread_seeded",
        project_id="jarvis",
        session_id="project:jarvis:orchestrator:thread_seeded",
        title="Seeded",
        created_at="2026-07-05T08:00:00+00:00",
        updated_at="2026-07-05T08:00:00+00:00",
        created_by="neil",
        messages=(
            {
                "role": "user",
                "peer_id": "neil",
                "content": "seed question",
                "observed_at": "2026-07-05T08:01:00+00:00",
            },
        ),
    )

    index.append_turn(
        thread,
        user_peer_id="neil",
        user_text="next question",
        assistant_peer_id="jarvis",
        assistant_text="next answer",
    )

    reloaded = CockpitThreadIndex(index_path).get_with_messages("jarvis", thread.thread_id)

    assert reloaded is not None
    assert [message["content"] for message in reloaded.messages] == [
        "seed question",
        "next question",
        "next answer",
    ]


def test_thread_index_migrates_legacy_json_transcript_once_after_interruption(tmp_path) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    thread = index.save(
        CockpitThread(
            thread_id="thread_legacy_file",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_legacy_file",
            title="Legacy file",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )
    legacy_path = index._legacy_transcript_path(thread.project_id, thread.thread_id)
    legacy_path.parent.mkdir(parents=True)
    legacy_payload = {
        "version": 1,
        "project_id": thread.project_id,
        "thread_id": thread.thread_id,
        "messages": [
            {
                "role": "user",
                "peer_id": "neil",
                "content": "legacy question",
                "observed_at": "2026-07-05T08:01:00+00:00",
            }
        ],
    }
    legacy_path.write_text(json.dumps(legacy_payload))

    migrated = index.get_with_messages(thread.project_id, thread.thread_id)
    jsonl_path = index._transcript_path(thread.project_id, thread.thread_id)

    assert migrated is not None
    assert [message["content"] for message in migrated.messages] == ["legacy question"]
    assert not legacy_path.exists()
    assert legacy_path.with_suffix(".json.bak").read_text() == json.dumps(legacy_payload)

    # Simulate a process interruption after the JSONL atomic rename but before
    # the original file was moved aside. The retry must not duplicate records.
    legacy_path.write_text(json.dumps(legacy_payload))
    retried = CockpitThreadIndex(tmp_path / "threads.json").get_with_messages(thread.project_id, thread.thread_id)

    assert retried is not None
    assert [message["content"] for message in retried.messages] == ["legacy question"]
    assert not legacy_path.exists()
    assert len(jsonl_path.read_text().splitlines()) == 1


def test_thread_index_cache_tail_reads_external_append_and_ignores_corrupt_trailing_line(tmp_path) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    thread = index.save(
        CockpitThread(
            thread_id="thread_tail",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_tail",
            title="Tail",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )
    index.append_turn(
        thread,
        user_peer_id="neil",
        user_text="first",
        assistant_peer_id="jarvis",
        assistant_text="first reply",
    )
    assert index.get_with_messages("jarvis", thread.thread_id) is not None  # Warm the cache.
    path = index._transcript_path("jarvis", thread.thread_id)
    with path.open("ab") as handle:
        handle.write(
            json.dumps(
                {
                    "role": "user",
                    "peer_id": "neil",
                    "content": "external append",
                    "observed_at": "2026-07-05T08:02:00+00:00",
                },
                sort_keys=True,
            ).encode()
            + b"\nnot-json\n{\"role\": \"user\""
        )

    detail = index.get_with_messages("jarvis", thread.thread_id)

    assert detail is not None
    assert [message["content"] for message in detail.messages] == ["first", "first reply", "external append"]
    recovered = index.append_turn(
        detail,
        user_peer_id="neil",
        user_text="after partial",
        assistant_peer_id="jarvis",
        assistant_text="recovered",
    )
    assert [message["content"] for message in recovered.messages] == [
        "first",
        "first reply",
        "external append",
        "after partial",
        "recovered",
    ]


def test_thread_index_appends_different_threads_without_global_transcript_serialization(tmp_path, monkeypatch) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    first = index.save(
        CockpitThread(
            thread_id="thread_one",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_one",
            title="One",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )
    second = index.save(
        CockpitThread(
            thread_id="thread_two",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_two",
            title="Two",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )
    original_append = index._append_thread_messages
    barrier = threading.Barrier(2)

    def append_after_both_threads_arrive(*args, **kwargs):  # noqa: ANN002, ANN003
        barrier.wait(timeout=2)
        return original_append(*args, **kwargs)

    monkeypatch.setattr(index, "_append_thread_messages", append_after_both_threads_arrive)
    failures: list[BaseException] = []

    def append(thread: CockpitThread) -> None:
        try:
            index.append_turn(
                thread,
                user_peer_id="neil",
                user_text=f"{thread.thread_id} question",
                assistant_peer_id="jarvis",
                assistant_text=f"{thread.thread_id} answer",
            )
        except BaseException as exc:  # noqa: BLE001 - report thread failures to the test.
            failures.append(exc)

    workers = [threading.Thread(target=append, args=(thread,)) for thread in (first, second)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=3)

    assert not failures
    assert not any(worker.is_alive() for worker in workers)
    assert [message["content"] for message in index.get_with_messages("jarvis", first.thread_id).messages] == [
        "thread_one question",
        "thread_one answer",
    ]
    assert [message["content"] for message in index.get_with_messages("jarvis", second.thread_id).messages] == [
        "thread_two question",
        "thread_two answer",
    ]


def test_thread_index_delete_waits_for_append_before_reclaiming_transcript(tmp_path, monkeypatch) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    thread = index.save(
        CockpitThread(
            thread_id="thread_delete_race",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_delete_race",
            title="Delete race",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )
    original_append = index._append_thread_messages
    append_started = threading.Event()
    allow_append = threading.Event()
    deleted = threading.Event()

    def hold_append(*args, **kwargs):  # noqa: ANN002, ANN003
        append_started.set()
        assert allow_append.wait(timeout=2)
        return original_append(*args, **kwargs)

    monkeypatch.setattr(index, "_append_thread_messages", hold_append)

    append_worker = threading.Thread(
        target=index.append_turn,
        args=(thread,),
        kwargs={
            "user_peer_id": "neil",
            "user_text": "question",
            "assistant_peer_id": "jarvis",
            "assistant_text": "answer",
        },
    )

    def delete() -> None:
        index.delete("jarvis", thread.thread_id)
        deleted.set()

    delete_worker = threading.Thread(target=delete)
    append_worker.start()
    assert append_started.wait(timeout=2)
    delete_worker.start()
    assert not deleted.wait(timeout=0.1)
    allow_append.set()
    append_worker.join(timeout=2)
    delete_worker.join(timeout=2)

    assert not append_worker.is_alive()
    assert not delete_worker.is_alive()
    assert deleted.is_set()
    assert index.get("jarvis", thread.thread_id) is None
    assert not index._transcript_path("jarvis", thread.thread_id).exists()


def test_thread_index_rejects_stale_turn_after_thread_deletion(tmp_path) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    thread = index.save(
        CockpitThread(
            thread_id="thread_deleted_turn",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_deleted_turn",
            title="Deleted turn",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )
    index.delete("jarvis", thread.thread_id)

    with pytest.raises(KeyError, match=thread.thread_id):
        index.append_turn(
            thread,
            user_peer_id="neil",
            user_text="late question",
            assistant_peer_id="jarvis",
            assistant_text="late answer",
        )
    with pytest.raises(KeyError, match=thread.thread_id):
        index.append_pending_turn(
            thread,
            user_peer_id="neil",
            user_text="late pending question",
            assistant_peer_id="jarvis",
        )

    assert index.get("jarvis", thread.thread_id) is None
    assert not index._transcript_path("jarvis", thread.thread_id).exists()


def test_thread_index_revalidates_stale_child_notification_after_deletion(tmp_path, monkeypatch) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    thread = index.save(
        CockpitThread(
            thread_id="thread_deleted_child",
            project_id="jarvis",
            session_id="project:jarvis:orchestrator:thread_deleted_child",
            title="Deleted child",
            created_at="2026-07-05T08:00:00+00:00",
            updated_at="2026-07-05T08:00:00+00:00",
            created_by="neil",
        )
    )
    index.delete("jarvis", thread.thread_id)
    original_threads = index._threads
    reads = 0

    def stale_then_current(**kwargs):  # noqa: ANN003
        nonlocal reads
        reads += 1
        if reads == 1:
            return {thread.thread_id: thread}
        return original_threads(**kwargs)

    monkeypatch.setattr(index, "_threads", stale_then_current)
    child = SimpleNamespace(
        run_id="run_child",
        objective="Child task",
        phase="completed",
        status="terminal",
        terminal_reason="done",
    )

    assert index.append_child_terminal_system_message(thread.thread_id, child) is False
    assert index.claim_ready_child_watch(thread.thread_id, {child.run_id}) is None
    index.finish_child_watch(thread.thread_id, "watch")
    index.renew_child_watch_claim(thread.thread_id, "watch")

    assert index.get("jarvis", thread.thread_id) is None
    assert not index._transcript_path("jarvis", thread.thread_id).exists()
