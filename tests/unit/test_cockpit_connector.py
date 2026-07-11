from __future__ import annotations

import json
from pathlib import Path
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
