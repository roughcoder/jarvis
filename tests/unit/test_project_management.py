from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from jarvis.brain import project_management as project_management_module
from jarvis.brain.project_management import ProjectOperationError, ProjectOperationService, upload_session_id
from jarvis.brain.registry import RegistryStore
from jarvis.config import Config
from jarvis.runtime import RequestContext


class FakeMemory:
    def __init__(self) -> None:
        self.uploads: list[dict[str, Any]] = []
        self.deleted_sessions: list[str] = []
        self.fail_upload: Exception | None = None

    def upload_file(self, session_id: str, *, peer_id: str, path: Path, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        if self.fail_upload is not None:
            raise self.fail_upload
        assert path.exists()
        row = {"session_id": session_id, "peer_id": peer_id, "path": str(path), "metadata": dict(metadata or {})}
        self.uploads.append(row)
        return {"ok": True}

    def delete_session(self, session_id: str) -> None:
        self.deleted_sessions.append(session_id)

    def query_conclusions(self, query: str, **kwargs: Any) -> list[Any]:
        return []

    def list_conclusions(self, **kwargs: Any) -> list[Any]:
        return []


def _cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Config:
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                f"REGISTRY_PATH={tmp_path / 'jarvis-workspace' / 'registry' / 'registry.json'}",
                f"REGISTRY_FILES_VAULT_ROOT={tmp_path / 'jarvis-workspace'}",
                f"REGISTRY_UPLOAD_STAGING_ROOT={tmp_path / 'jarvis-workspace' / 'uploads' / 'staging'}",
                f"REGISTRY_UPLOAD_MANIFEST_PATH={tmp_path / 'jarvis-workspace' / 'registry' / 'upload-manifest.json'}",
                f"MEMORY_CACHE_PATH={tmp_path / 'cache.json'}",
                f"MEMORY_CURATION_OUTBOX_PATH={tmp_path / 'outbox.jsonl'}",
                "MEMORY_BACKEND=v3",
                "BRAIN_PEER_TOKEN=peer-token",
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


def _ctx(identity: str, caps: frozenset[str] = frozenset()) -> RequestContext:
    return RequestContext("dev", identity, "personal", caps, channel="cockpit", peer=identity)


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


def test_archived_project_cannot_be_unarchived_by_member_update(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, store, _memory = _service(tmp_path, monkeypatch)
    asyncio.run(service.execute(_ctx("neil"), "project.archive", {"project_id": "jarvis", "archived": True}))

    with pytest.raises(ProjectOperationError, match="unarchived through the owner route"):
        asyncio.run(service.execute(_ctx("jules"), "project.update", {"project_id": "jarvis", "status": "active"}))

    assert store.get_project("jarvis").status == "archived"


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

    listed = asyncio.run(service.execute(_ctx("jules"), "project.file.list", {"project_id": "jarvis"}))
    assert listed["files"][0]["doc_id"] == result["doc_id"]
    assert listed["files"][0]["original_path"] == str(original)
    assert listed["files"][0]["ingestion"]["queued"] is True

    retracted = asyncio.run(
        service.execute(
            _ctx("jules"),
            "project.file.retract",
            {"project_id": "jarvis", "doc_id": result["doc_id"]},
        )
    )
    assert retracted["retracted"] is True
    assert memory.deleted_sessions == [result["session_id"]]
    assert original.exists()
    assert asyncio.run(service.execute(_ctx("jules"), "project.file.list", {"project_id": "jarvis"}))["files"] == []
    retracted_files = asyncio.run(
        service.execute(_ctx("jules"), "project.file.list", {"project_id": "jarvis", "include_retracted": True})
    )["files"]
    assert retracted_files[0]["retracted"] is True
    assert retracted_files[0]["doc_id"] == result["doc_id"]


def test_upload_ingestion_failure_returns_recoverable_result_with_vault_metadata(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, _store, memory = _service(tmp_path, monkeypatch)
    memory.fail_upload = TimeoutError("honcho timed out")

    result = asyncio.run(
        service.execute(
            _ctx("jules"),
            "project.file.upload",
            {"project_id": "jarvis", "filename": "Spec.md", "content_text": "# Spec"},
        )
    )

    assert Path(result["original_path"]).exists()
    assert result["doc_id"]
    assert result["content_hash"].startswith("sha256:")
    assert result["ingestion"] == {
        "queued": False,
        "code": "ingestion_failed",
        "error": "file ingestion failed",
        "recoverable": True,
    }
    assert result["file"]["ingestion"] == result["ingestion"]


def test_upload_rejects_path_outside_staging_root(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, _store, _memory = _service(tmp_path, monkeypatch)
    outside = tmp_path / "secret.env"
    outside.write_text("TOKEN=secret", encoding="utf-8")

    with pytest.raises(ProjectOperationError, match="upload staging root"):
        asyncio.run(service.execute(_ctx("jules"), "project.file.upload", {"project_id": "jarvis", "source_path": str(outside)}))


def test_upload_accepts_path_inside_staging_root(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, _store, memory = _service(tmp_path, monkeypatch)
    staging = Path(service.cfg.registry.upload_staging_root)
    staging.mkdir(parents=True)
    source = staging / "spec.md"
    source.write_text("# staged", encoding="utf-8")

    result = asyncio.run(service.execute(_ctx("jules"), "project.file.upload", {"project_id": "jarvis", "source_path": str(source)}))

    assert Path(result["original_path"]).read_text(encoding="utf-8") == "# staged"
    assert memory.uploads[0]["path"] == result["original_path"]


def test_upload_rejects_file_and_private_urls(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, _store, _memory = _service(tmp_path, monkeypatch)

    with pytest.raises(ProjectOperationError, match="http or https"):
        asyncio.run(service.execute(_ctx("jules"), "project.file.upload", {"project_id": "jarvis", "source_url": "file:///etc/passwd"}))

    with pytest.raises(ProjectOperationError, match="host is not allowed"):
        asyncio.run(service.execute(_ctx("jules"), "project.file.upload", {"project_id": "jarvis", "source_url": "http://127.0.0.1/x"}))


def test_upload_rejects_oversize_url_response(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, _store, _memory = _service(tmp_path, monkeypatch)
    service.cfg.registry.max_upload_bytes = 5

    class Response:
        status = 200
        headers: dict[str, str] = {}

        def getheader(self, name: str) -> str | None:
            return self.headers.get(name)

        def close(self) -> None:
            return None

        def read(self, _size: int) -> bytes:
            return b"abcdef"

    monkeypatch.setattr(
        project_management_module.socket,
        "getaddrinfo",
        lambda *args, **kwargs: [(None, None, None, None, ("93.184.216.34", 80))],
    )
    monkeypatch.setattr(project_management_module, "_open_pinned_upload_url", lambda _resolved: Response())

    with pytest.raises(ProjectOperationError, match="exceeds max size"):
        asyncio.run(
            service.execute(
                _ctx("jules"),
                "project.file.upload",
                {"project_id": "jarvis", "source_url": "https://example.com/large.md"},
            )
        )


def test_upload_rejects_dns_rebinding_to_private_address(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, _store, _memory = _service(tmp_path, monkeypatch)
    answers = [
        [(None, None, None, None, ("93.184.216.34", 80))],
        [(None, None, None, None, ("169.254.169.254", 80))],
    ]

    def fake_getaddrinfo(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        return answers.pop(0)

    monkeypatch.setattr(project_management_module.socket, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(
        project_management_module,
        "_open_pinned_upload_url",
        lambda _resolved: pytest.fail("rebound URL should be rejected before connect"),
    )

    with pytest.raises(ProjectOperationError, match="host is not allowed"):
        asyncio.run(
            service.execute(
                _ctx("jules"),
                "project.file.upload",
                {"project_id": "jarvis", "source_url": "https://example.com/spec.md"},
            )
        )


def test_files_root_is_relative_and_confined(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, store, _memory = _service(tmp_path, monkeypatch)

    with pytest.raises(ProjectOperationError, match="relative path"):
        asyncio.run(service.execute(_ctx("jules"), "project.update", {"project_id": "jarvis", "files_root": "/tmp/out"}))
    with pytest.raises(ProjectOperationError, match="relative path"):
        asyncio.run(service.execute(_ctx("jules"), "project.update", {"project_id": "jarvis", "files_root": "../out"}))

    result = asyncio.run(
        service.execute(_ctx("jules"), "project.update", {"project_id": "jarvis", "files_root": "projects/jarvis/docs"})
    )
    assert result["project"]["files_root"] == "projects/jarvis/docs"
    upload = asyncio.run(
        service.execute(_ctx("jules"), "project.file.upload", {"project_id": "jarvis", "filename": "a.txt", "content_text": "ok"})
    )
    assert Path(upload["original_path"]).resolve().is_relative_to(Path(service.cfg.registry.files_vault_root).resolve())
    assert store.get_project("jarvis").files_root == "projects/jarvis/docs"


def test_project_memory_forget_is_member_gated(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    service, _store, _memory = _service(tmp_path, monkeypatch)

    with pytest.raises(ProjectOperationError) as hidden:
        asyncio.run(
            service.execute(
                _ctx("alice", frozenset({"memory.curate"})),
                "project.memory.forget",
                {"project_id": "jarvis", "query": "old fact"},
            )
        )

    assert hidden.value.status == 404
