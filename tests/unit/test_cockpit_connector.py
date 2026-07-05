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
from jarvis.connectors.cockpit import CockpitConnector, orchestrator_session_id


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
                "project_id": "jarvis",
                "thread_id": thread.thread_id,
                "created_by": "neil",
                "created_at": thread.created_at,
            },
        }
    ]
    assert memory.messages == []
    index = json.loads((tmp_path / "orchestration" / "cockpit-threads.json").read_text())
    assert index["threads"][thread.thread_id]["session_id"] == thread.session_id


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
