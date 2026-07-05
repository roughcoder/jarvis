from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from jarvis.brain.memory_client import RepresentationRecord
from jarvis.brain.memory_outbox import CurationOutbox
from jarvis.config import Config
from jarvis.mcp_server.adapters import JarvisMCPService, MCPAccessError
from jarvis.mcp_server.tokens import MCPTokenStore


class FakeMemory:
    def __init__(self) -> None:
        self.representations: dict[str, str] = {}
        self.reads: list[dict[str, Any]] = []
        self.chats: list[dict[str, Any]] = []

    def read_cached_representation(self, user: str | None = None) -> str:
        return self.representations.get(user or "", "cached")

    def read_representation(self, peer_id: str, **kwargs: Any) -> RepresentationRecord:
        self.reads.append({"peer_id": peer_id, **kwargs})
        return RepresentationRecord(
            peer_id=peer_id,
            representation=self.representations.get(peer_id, "live representation"),
        )

    def dialectic_chat(self, peer_id: str, query: str, **kwargs: Any) -> str:
        self.chats.append({"peer_id": peer_id, "query": query, **kwargs})
        return f"answer for {peer_id}: {query}"


def _cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Config:
    env = tmp_path / ".env"
    env.write_text(
        "\n".join(
            [
                f"CAPS_USERS_DIR={tmp_path / 'users'}",
                "CAPS_DEFAULT_CAPABILITIES=",
                f"REGISTRY_PATH={tmp_path / 'registry.json'}",
                "MEMORY_BACKEND=v3",
                f"MEMORY_CACHE_PATH={tmp_path / 'cache.json'}",
                f"MEMORY_CURATION_OUTBOX_PATH={tmp_path / 'outbox.jsonl'}",
                f"MCP_SERVE_TOKEN_STORE_PATH={tmp_path / 'tokens.json'}",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env))
    cfg = Config()
    _seed_users(cfg)
    _seed_registry(cfg)
    return cfg


def _seed_users(cfg: Config) -> None:
    users = Path(cfg.capabilities.users_dir)
    users.mkdir(parents=True, exist_ok=True)
    users.joinpath("neil.md").write_text(
        "---\nscope: personal\nhoncho_peer: neil\ncapabilities: [memory.query, memory.curate]\n---\n# Neil\n",
        encoding="utf-8",
    )
    users.joinpath("viewer.md").write_text(
        "---\nscope: personal\nhoncho_peer: viewer\ncapabilities: [memory.query]\n---\n# Viewer\n",
        encoding="utf-8",
    )
    users.joinpath("guest.md").write_text(
        "---\nscope: personal\nhoncho_peer: guest\ncapabilities: []\n---\n# Guest\n",
        encoding="utf-8",
    )


def _seed_registry(cfg: Config) -> None:
    path = Path(cfg.registry.path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "projects": [
                    {
                        "id": "jarvis",
                        "name": "Jarvis",
                        "aliases": ["the jarvis project"],
                        "owner": "neil",
                        "members": ["neil", "viewer"],
                        "visibility": "shared",
                        "status": "active",
                        "repos": [],
                        "links": {"jira": "", "urls": []},
                        "files_root": "",
                    },
                    {
                        "id": "private",
                        "name": "Private",
                        "owner": "alice",
                        "members": ["alice"],
                        "visibility": "private",
                        "status": "active",
                        "repos": [],
                        "links": {"jira": "", "urls": []},
                        "files_root": "",
                    },
                ],
                "contacts": [],
            }
        ),
        encoding="utf-8",
    )


def test_token_add_list_revoke_roundtrip(tmp_path) -> None:
    store = MCPTokenStore(tmp_path / "tokens.json")
    token, record = store.add(principal="neil", name="Claude Code")

    assert store.resolve(token).principal == "neil"
    listed = store.list()
    assert [(item.token_id, item.principal, item.name) for item in listed] == [
        (record.token_id, "neil", "Claude Code")
    ]
    assert token not in (tmp_path / "tokens.json").read_text(encoding="utf-8")

    revoked = store.revoke(record.token_id[:12])
    assert revoked.revoked
    assert store.resolve(token) is None
    assert store.list() == []
    assert store.list(include_revoked=True)[0].revoked


def test_token_principal_context_inherits_user_capabilities(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    token, _record = MCPTokenStore(cfg.mcp_serve.token_store_path).add(principal="neil")
    resolved = MCPTokenStore(cfg.mcp_serve.token_store_path).resolve(token)

    ctx = JarvisMCPService(cfg, memory=FakeMemory()).context_for_principal(resolved.principal)

    assert ctx.identity == "neil"
    assert ctx.memory_peer == "neil"
    assert ctx.channel == "mcp"
    assert {"memory.query", "memory.curate"} <= set(ctx.capabilities)


def test_unknown_principal_is_denied_by_default(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())

    with pytest.raises(MCPAccessError):
        service.context_for_principal("missing")


def test_project_list_is_membership_filtered(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    ctx = service.context_for_principal("viewer")

    body = asyncio.run(service.project_list(ctx))

    assert [project["id"] for project in body["projects"]] == ["jarvis"]


def test_project_get_is_membership_filtered(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    ctx = service.context_for_principal("viewer")

    body = asyncio.run(service.project_get(ctx, project_id="jarvis"))
    assert body["project"]["name"] == "Jarvis"

    with pytest.raises(MCPAccessError):
        asyncio.run(service.project_get(ctx, project_id="private"))


def test_memory_search_is_capability_and_access_gated(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    memory = FakeMemory()
    service = JarvisMCPService(cfg, memory=memory)
    viewer = service.context_for_principal("viewer")

    own = asyncio.run(service.memory_search(viewer, search_query="what do you know?"))
    assert own["result"] == "answer for viewer: what do you know?"

    with pytest.raises(MCPAccessError, match="project is not visible"):
        asyncio.run(service.memory_search(viewer, search_query="secret", target="project:private"))

    guest = service.context_for_principal("guest")
    with pytest.raises(MCPAccessError, match="capability"):
        asyncio.run(service.memory_search(guest, search_query="anything"))


def test_record_finding_carries_mcp_audit_metadata(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    ctx = service.context_for_principal("neil")

    body = asyncio.run(
        service.record_finding(
            ctx,
            project="Jarvis",
            content="The cache read stays local.",
            agent="claude-code",
            observed_at="2026-07-05",
        )
    )

    assert body["result"].startswith("Noted")
    entry = CurationOutbox(cfg.memory.curation_outbox_path).pending_entries()[0]
    assert entry.observed_id == "project:jarvis"
    assert entry.content == "The cache read stays local."
    assert entry.metadata["recorded_by"] == "neil"
    assert entry.metadata["channel"] == "mcp"
    assert entry.metadata["source"] == "mcp"
    assert entry.metadata["agent"] == "claude-code"
    assert entry.metadata["project_id"] == "jarvis"
    assert entry.metadata["artifact_type"] == "finding"
    assert entry.metadata["observed_at"] == "2026-07-05"


def test_upload_file_is_clear_not_available_stub(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    service = JarvisMCPService(cfg, memory=FakeMemory())
    ctx = service.context_for_principal("neil")

    body = asyncio.run(service.upload_file(ctx, project_id="jarvis", path="note.md"))

    assert "not yet available" in body["error"]


def test_service_does_not_construct_memory_client_for_registry_reads(tmp_path, monkeypatch) -> None:
    cfg = _cfg(tmp_path, monkeypatch)

    def fail_memory_client(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("MCP server should not construct memory for project_list")

    monkeypatch.setattr("jarvis.brain.memory_client.MemoryClient", fail_memory_client)
    service = JarvisMCPService(cfg)
    ctx = service.context_for_principal("viewer")

    body = asyncio.run(service.project_list(ctx))

    assert [project["id"] for project in body["projects"]] == ["jarvis"]
