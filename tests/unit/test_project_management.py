from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from jarvis.brain.project_management import ProjectOperationError, ProjectOperationService, upload_session_id
from jarvis.brain.registry import RegistryStore
from jarvis.config import Config
from jarvis.runtime import RequestContext


class FakeMemory:
    def __init__(self) -> None:
        self.uploads: list[dict[str, Any]] = []
        self.deleted_sessions: list[str] = []

    def upload_file(self, session_id: str, *, peer_id: str, path: Path, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        assert path.exists()
        row = {"session_id": session_id, "peer_id": peer_id, "path": str(path), "metadata": dict(metadata or {})}
        self.uploads.append(row)
        return {"ok": True}

    def delete_session(self, session_id: str) -> None:
        self.deleted_sessions.append(session_id)


def _cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Config:
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                f"REGISTRY_PATH={tmp_path / 'jarvis-workspace' / 'registry' / 'registry.json'}",
                f"MEMORY_CACHE_PATH={tmp_path / 'cache.json'}",
                f"MEMORY_CURATION_OUTBOX_PATH={tmp_path / 'outbox.jsonl'}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env))
    return Config()


def _seed(store: RegistryStore) -> None:
    store.path.parent.mkdir(parents=True, exist_ok=True)
    store.path.write_text(
        json.dumps(
            {
                "version": 1,
                "projects": [
                    {
                        "id": "jarvis",
                        "name": "Jarvis",
                        "owner": "neil",
                        "members": ["neil", "jules"],
                        "visibility": "shared",
                        "status": "active",
                        "repos": [{"name": "runtime", "remote": "roughcoder/jarvis", "default": True}],
                        "links": {"jira": "", "urls": []},
                        "files_root": "projects/jarvis/files",
                    }
                ],
                "contacts": [],
            }
        ),
        encoding="utf-8",
    )
    store.load()


def _ctx(identity: str) -> RequestContext:
    return RequestContext("dev", identity, "personal", frozenset(), channel="cockpit", peer=identity)


def _service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[ProjectOperationService, RegistryStore, FakeMemory]:
    cfg = _cfg(tmp_path, monkeypatch)
    store = RegistryStore(cfg.registry.path)
    _seed(store)
    memory = FakeMemory()
    return ProjectOperationService(cfg, registry=store, memory=memory), store, memory


def test_project_role_gate_and_transactional_update(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, store, _memory = _service(tmp_path, monkeypatch)

    body = asyncio.run(
        service.execute(
            _ctx("jules"),
            "project.update",
            {"project_id": "jarvis", "name": "Jarvis Runtime", "status": "paused"},
        )
    )
    assert body["project"]["name"] == "Jarvis Runtime"
    assert store.get_project("jarvis").status == "paused"

    with pytest.raises(ProjectOperationError) as forbidden:
        asyncio.run(service.execute(_ctx("jules"), "project.visibility.set", {"project_id": "jarvis", "visibility": "private"}))
    assert forbidden.value.status == 403

    with pytest.raises(ProjectOperationError) as hidden:
        asyncio.run(service.execute(_ctx("alice"), "project.update", {"project_id": "jarvis", "name": "Nope"}))
    assert hidden.value.status == 404

    before = store.get_project("jarvis").as_dict()
    with pytest.raises(ProjectOperationError, match="owner-only"):
        asyncio.run(service.execute(_ctx("neil"), "project.update", {"project_id": "jarvis", "visibility": "private"}))
    assert store.get_project("jarvis").as_dict() == before

    with pytest.raises(ProjectOperationError, match="at most one default"):
        asyncio.run(
            service.execute(
                _ctx("neil"),
                "project.repos.set",
                {
                    "project_id": "jarvis",
                    "repos": [
                        {"name": "runtime", "remote": "roughcoder/jarvis", "default": True},
                        {"name": "infra", "remote": "roughcoder/infra", "default": True},
                    ],
                },
            )
        )
    assert store.get_project("jarvis").as_dict() == before


def test_project_create_owner_and_admin_ops(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, store, _memory = _service(tmp_path, monkeypatch)

    created = asyncio.run(service.execute(_ctx("jules"), "project.create", {"id": "bird-story", "name": "Bird Story"}))
    assert created["project"]["owner"] == "jules"
    assert created["project"]["members"] == ["jules"]

    members = asyncio.run(
        service.execute(_ctx("jules"), "project.members.set", {"project_id": "bird-story", "members": ["jules", "alice"]})
    )
    assert members["project"]["members"] == ["jules", "alice"]

    archived = asyncio.run(service.execute(_ctx("jules"), "project.archive", {"project_id": "bird-story", "archived": True}))
    assert archived["project"]["status"] == "archived"

    deleted = asyncio.run(service.execute(_ctx("jules"), "project.delete", {"project_id": "bird-story"}))
    assert deleted == {"deleted": True, "project_id": "bird-story"}
    assert store.get_project("bird-story") is None


def test_upload_writes_vault_first_then_ingests_and_retracts_session(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, _store, memory = _service(tmp_path, monkeypatch)

    result = asyncio.run(
        service.execute(
            _ctx("jules"),
            "project.file.upload",
            {
                "project_id": "jarvis",
                "filename": "Spec.md",
                "content_text": "# Spec\nBuild the cockpit.",
                "title": "Cockpit Spec",
                "artifact_type": "spec",
                "agent": "claude-code",
            },
        )
    )

    original = Path(result["original_path"])
    assert original.exists()
    assert original.read_text(encoding="utf-8") == "# Spec\nBuild the cockpit."
    assert result["session_id"] == upload_session_id("jarvis", result["doc_id"])
    assert memory.uploads[0]["session_id"] == result["session_id"]
    assert memory.uploads[0]["peer_id"] == "project:jarvis"
    metadata = memory.uploads[0]["metadata"]
    assert metadata["project_id"] == "jarvis"
    assert metadata["artifact_type"] == "spec"
    assert metadata["uploaded_by"] == "jules"
    assert metadata["source"] == "file"
    assert metadata["content_hash"].startswith("sha256:")
    assert metadata["original_path"] == str(original)
    assert metadata["agent"] == "claude-code"

    retracted = asyncio.run(
        service.execute(
            _ctx("jules"),
            "project.file.retract",
            {"project_id": "jarvis", "doc_id": result["doc_id"]},
        )
    )
    assert retracted["retracted"] is True
    assert memory.deleted_sessions == [result["session_id"]]
