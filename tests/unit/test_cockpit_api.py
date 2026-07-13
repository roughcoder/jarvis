from __future__ import annotations

import asyncio
import base64
import json
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("aiohttp")
pytest.importorskip("httpx")

import httpx  # noqa: E402
from aiohttp import web  # noqa: E402

from jarvis.brain.capabilities import RequestContext, can_query_memory_peer  # noqa: E402
from jarvis.brain.memory_client import ConclusionRecord, MemoryMessage, RepresentationRecord, SessionPeer  # noqa: E402
from jarvis.brain.memory_outbox import CurationOutbox  # noqa: E402
from jarvis.connectors.cockpit import (  # noqa: E402
    CockpitConnector,
    CockpitThread,
    CockpitThreadIndex,
    _continue_child_watch,
    _read_child_work_result_tool,
    _start_ready_child_watch,
    orchestrator_session_id,
)
from jarvis.config import Config, MCPServerSpec, WorkerConfig  # noqa: E402
import jarvis.orchestration.api as cockpit_api_module  # noqa: E402
import jarvis.orchestration.cockpit as cockpit_module  # noqa: E402
from jarvis.orchestration.api import CockpitAppContext, IdempotencyStore, SseSnapshotHub, _command_from_body, _idempotency_scope, make_app, serve  # noqa: E402
from jarvis.orchestration.cockpit import make_session_ref  # noqa: E402
from jarvis.mcp.status import mcp_status_path  # noqa: E402
from jarvis.mcp_server.tokens import MCPTokenStore  # noqa: E402
from jarvis.orchestration.models import Artifact, ExecutionEnvelope, WorkItem, WorkerJobLink, WorkerProfile, WorkerSessionLink  # noqa: E402
from jarvis.orchestration.orchestrator_grants import mint_orchestrator_grant  # noqa: E402
from jarvis.orchestration.oauth import OAuthTokenValidator, OAuthValidationError  # noqa: E402
from jarvis.orchestration.service import StartedWork  # noqa: E402
from jarvis.orchestration.store import OrchestrationStore  # noqa: E402
from jarvis.brain.registry import RegistryStore  # noqa: E402
from jarvis.worker.server import make_app as make_worker_app  # noqa: E402
from jarvis.worker.sessions import SessionManager  # noqa: E402
from jarvis.worker_session_contract import EVENT_CHECKPOINT_CREATED  # noqa: E402


class Response:
    def __init__(self, data: dict[str, Any], status_code: int = 200) -> None:
        self._data = data
        self.status_code = status_code
        self.text = json.dumps(data)

    def json(self) -> dict[str, Any]:
        return self._data

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(self.text)


class TextResponse:
    def __init__(self, text: str, status_code: int = 500) -> None:
        self.text = text
        self.status_code = status_code

    def json(self) -> dict[str, Any]:
        raise ValueError("not json")


class FakeProjectMemory:
    def __init__(
        self,
        *,
        cached: str = "",
        live: str = "",
        live_error: Exception | None = None,
        conclusion_error: Exception | None = None,
    ) -> None:
        self.cached = cached
        self.live = live
        self.live_error = live_error
        self.conclusion_error = conclusion_error
        self.conclusions: list[ConclusionRecord] = []
        self.cached_reads: list[str] = []
        self.live_reads: list[str] = []
        self.conclusion_filters: list[dict[str, Any]] = []
        self.sessions: list[dict[str, Any]] = []
        self.messages: list[dict[str, Any]] = []
        self.created_conclusions: list[dict[str, Any]] = []
        self.deleted_sessions: list[str] = []
        self.create_session_error: Exception | None = None
        self.create_messages_error: Exception | None = None
        self.delete_session_error: Exception | None = None

    def read_cached_representation(self, user: str | None = None) -> str:
        self.cached_reads.append(user or "")
        return self.cached

    def read_representation(self, peer_id: str) -> RepresentationRecord:
        self.live_reads.append(peer_id)
        if self.live_error is not None:
            raise self.live_error
        return RepresentationRecord(peer_id=peer_id, representation=self.live)

    def list_conclusions(self, **kwargs: Any) -> list[ConclusionRecord]:
        self.conclusion_filters.append(kwargs)
        if self.conclusion_error is not None:
            raise self.conclusion_error
        rows = list(self.conclusions)
        observed_id = kwargs.get("observed_id")
        level = kwargs.get("level")
        metadata = kwargs.get("metadata") or {}
        if observed_id:
            rows = [row for row in rows if row.observed_id == observed_id]
        if level:
            rows = [row for row in rows if row.level == level]
        for key, value in metadata.items():
            rows = [row for row in rows if row.metadata.get(key) == value]
        return rows

    def create_session(
        self,
        session_id: str,
        *,
        peers: list[SessionPeer] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if self.create_session_error is not None:
            raise self.create_session_error
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
            "metadata": dict(metadata or {}),
            "messages_at_create": len(self.messages),
        }
        self.sessions.append(row)
        return row

    def create_messages(self, session_id: str, messages: list[MemoryMessage]) -> list[dict[str, Any]]:
        if self.create_messages_error is not None:
            raise self.create_messages_error
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
        return rows

    def delete_session(self, session_id: str) -> None:
        if self.delete_session_error is not None:
            raise self.delete_session_error
        self.deleted_sessions.append(session_id)

    async def write_turn(self, user_text: str, assistant_text: str, *, user: str | None = None) -> None:
        self.messages.append(
            {
                "session_id": f"default:{user or ''}",
                "peer_id": user or "",
                "content": user_text,
                "metadata": {"assistant": assistant_text},
            }
        )

    async def refresh_cache(self, min_interval_s: float = 0.0, *, user: str | None = None) -> bool:
        return False

    def create_conclusion(
        self,
        *,
        observed_id: str,
        content: str,
        observer_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ConclusionRecord:
        row = {
            "observed_id": observed_id,
            "content": content,
            "observer_id": observer_id or "jarvis",
            "session_id": session_id,
            "metadata": dict(metadata or {}),
        }
        self.created_conclusions.append(row)
        return ConclusionRecord(
            id=f"cc{len(self.created_conclusions)}",
            observed_id=observed_id,
            observer_id=row["observer_id"],
            content=content,
            session_id=session_id,
            metadata=row["metadata"],
        )

    def queue_status(self) -> Any:
        return type("QueueStatus", (), {"idle": True})()


class FakeProjectBrainClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def execute(self, requester: RequestContext, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append({"identity": requester.identity, "op": op, "payload": dict(payload)})
        if payload.get("project_id") == "alice-private" and requester.identity != "alice":
            raise cockpit_api_module.ProjectOperationError("not_found", "project not found", status=404)
        if op in {"project.visibility.set", "project.members.set", "project.archive", "project.delete"} and requester.identity != "alice":
            raise cockpit_api_module.ProjectOperationError("forbidden", "project owner required", status=403)
        if op == "project.delete":
            return {"deleted": True, "project_id": payload["project_id"]}
        if op == "project.file.upload":
            return {
                "project_id": payload["project_id"],
                "doc_id": "upload-123",
                "session_id": "project:neil-shared:uploads:upload-123",
                "original_path": "/tmp/upload.md",
                "metadata": {"channel": payload.get("channel")},
                "ingestion": {"queued": True},
            }
        if op == "project.file.retract":
            return {
                "project_id": payload["project_id"],
                "doc_id": payload["doc_id"],
                "session_id": f"project:{payload['project_id']}:uploads:{payload['doc_id']}",
                "retracted": True,
            }
        if op == "project.file.list":
            return {"project_id": payload["project_id"], "files": [{"doc_id": "upload-123"}]}
        if op in {"project.memory.forget", "project.memory.correct"}:
            return {"project_id": payload["project_id"], "result": "Forgotten." if op.endswith("forget") else "Corrected."}
        return {
            "project": {
                "id": payload.get("project_id") or payload.get("id") or "new-project",
                "name": payload.get("name") or "Updated Project",
                "peer_id": "project:updated",
                "aliases": payload.get("aliases") or [],
                "owner": "alice",
                "members": payload.get("members") or ["alice", "neil"],
                "visibility": payload.get("visibility") or "shared",
                "status": "active",
                "repos": payload.get("repos") or [],
                "links": payload.get("links") or {"jira": "", "urls": []},
                "files_root": payload.get("files_root") or "projects/updated/files",
            }
        }


class _Fn:
    def __init__(self, name: str, arguments: str) -> None:
        self.name = name
        self.arguments = arguments


class _Call:
    def __init__(self, cid: str, name: str, arguments: str) -> None:
        self.id = cid
        self.function = _Fn(name, arguments)


class _Msg:
    def __init__(self, content: str = "", tool_calls: list[_Call] | None = None) -> None:
        self.content = content
        self.tool_calls = tool_calls


class FakeGateway:
    def __init__(self, scripted: list[_Msg | str]) -> None:
        self.scripted = scripted
        self.calls = 0
        self.messages: list[list[dict[str, Any]]] = []
        self.tools: list[list[dict[str, Any]] | None] = []

    async def complete(self, messages: list[dict[str, Any]], *, model: str | None = None) -> str:
        self.messages.append(messages)
        item = self.scripted[self.calls]
        self.calls += 1
        return item if isinstance(item, str) else item.content

    async def complete_with_tools(
        self,
        messages: list[dict[str, Any]],
        *,
        model: str | None = None,
        tools: list[dict[str, Any]] | None = None,
        usage_out: dict[str, Any] | None = None,
    ) -> _Msg:
        self.messages.append(messages)
        self.tools.append(tools)
        item = self.scripted[self.calls]
        self.calls += 1
        if isinstance(item, str):
            return _Msg(content=item)
        return item


async def _with_server(cfg: Config, fn: Callable[[str, httpx.AsyncClient], Any], *, http_get=None, http_post=None, http_delete=None) -> Any:  # noqa: ANN001
    runner = web.AppRunner(make_app(cfg, http_get=http_get, http_post=http_post, http_delete=http_delete))
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 0)
    await site.start()
    sockets = site._server.sockets  # type: ignore[union-attr, attr-defined]  # noqa: SLF001
    port = sockets[0].getsockname()[1]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            return await fn(f"http://localhost:{port}", client)
    finally:
        await runner.cleanup()


def _cfg(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    caps: str = "",
    token: str = "",
    cors_origins: str = "",
    identity: str = "house",
    auth_mode: str = "hybrid",
    oauth_issuer: str = "",
    oauth_audience: str = "",
    oauth_jwks_url: str = "",
    oauth_required_scopes: str = "",
    oauth_default_alg: str = "RS256",
    oauth_jwks_ttl_s: str = "300",
    oauth_jwks_min_refresh_s: str = "30",
    memory_backend: str = "v2",
    brain_peer_token: str = "",
    mcp_enabled: str = "false",
    mcp_servers: str = "[]",
    mcp_serve_token_store_path: str = "jarvis-workspace/.mcp-server/tokens.json",
    mcp_serve_auth_mode: str = "hybrid",
    mcp_serve_resource_url: str = "",
    mcp_serve_oauth_issuer: str = "",
    mcp_serve_oauth_jwks_url: str = "",
    mcp_serve_oauth_required_scopes: str = "",
    worker_token: str = "",
) -> Config:
    env = tmp_path / ".env"
    workspace = tmp_path / "orchestration"
    workers_path = workspace / "workers.json"
    registry_path = tmp_path / "registry.json"
    users_path = tmp_path / "users"
    env.write_text(
        "\n".join(
            [
                f"ORCHESTRATION_WORKSPACE={workspace}",
                f"ORCHESTRATION_WORKERS_PATH={workers_path}",
                "ORCHESTRATION_LANDING_MODE=branch_only",
                f"ORCHESTRATION_API_TOKEN={token}",
                f"ORCHESTRATION_API_CORS_ORIGINS={cors_origins}",
                f"REGISTRY_PATH={registry_path}",
                f"CAPS_IDENTITY={identity}",
                f"ORCHESTRATION_AUTH_MODE={auth_mode}",
                f"ORCHESTRATION_OAUTH_ISSUER={oauth_issuer}",
                f"ORCHESTRATION_OAUTH_AUDIENCE={oauth_audience}",
                f"ORCHESTRATION_OAUTH_JWKS_URL={oauth_jwks_url}",
                f"ORCHESTRATION_OAUTH_REQUIRED_SCOPES={oauth_required_scopes}",
                "ORCHESTRATION_OAUTH_JARVIS_USER_CLAIM=jarvis_user",
                f"ORCHESTRATION_OAUTH_DEFAULT_ALG={oauth_default_alg}",
                f"ORCHESTRATION_OAUTH_JWKS_TTL_S={oauth_jwks_ttl_s}",
                f"ORCHESTRATION_OAUTH_JWKS_MIN_REFRESH_S={oauth_jwks_min_refresh_s}",
                f"CAPS_DEFAULT_CAPABILITIES={caps}",
                f"CAPS_USERS_DIR={users_path}",
                f"MEMORY_BACKEND={memory_backend}",
                f"MEMORY_CACHE_PATH={tmp_path / 'memory-cache.json'}",
                f"MEMORY_CURATION_OUTBOX_PATH={tmp_path / 'curation-outbox.jsonl'}",
                f"BRAIN_PEER_TOKEN={brain_peer_token}",
                f"MCP_ENABLED={mcp_enabled}",
                f"MCP_SERVERS={mcp_servers}",
                f"MCP_SERVE_TOKEN_STORE_PATH={mcp_serve_token_store_path}",
                f"MCP_SERVE_AUTH_MODE={mcp_serve_auth_mode}",
                f"MCP_SERVE_RESOURCE_URL={mcp_serve_resource_url}",
                f"MCP_SERVE_OAUTH_ISSUER={mcp_serve_oauth_issuer}",
                f"MCP_SERVE_OAUTH_JWKS_URL={mcp_serve_oauth_jwks_url}",
                f"MCP_SERVE_OAUTH_REQUIRED_SCOPES={mcp_serve_oauth_required_scopes}",
                "WORKER_HOST=worker.test",
                "WORKER_PORT=8780",
                f"MACBOOK_WORKER_TOKEN={worker_token}",
                "WORKER_SUPPORTED_ENGINES=codex,claude",
            ]
        )
    )
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env))
    workspace.mkdir(parents=True, exist_ok=True)
    workers_path.write_text(
        json.dumps(
            {
                "workers": [
                    {
                        "worker_id": "macbook-worker",
                        "display_name": "MacBook Pro",
                        "base_url": "http://worker.test",
                        "token_env": "MACBOOK_WORKER_TOKEN" if worker_token else "",
                        "capabilities": ["git", "shell", "browser", "codex"],
                        "max_concurrent_jobs": 4,
                        "current_jobs": 1,
                        "status": "online",
                        "agent": "codex",
                        "supported_engines": ["codex", "claude"],
                        "repo_access": [{"repo": "roughcoder/jarvis", "accessible": True, "reason_code": "accessible"}],
                        "engine_supports": {
                            "codex": {
                                "streaming": True,
                                "resume": True,
                                "interrupt": True,
                                "approval_requests": True,
                                "input_requests": True,
                                "checkpoints": True,
                            },
                            "claude": {
                                "streaming": True,
                                "resume": True,
                                "interrupt": False,
                                "approval_requests": False,
                                "input_requests": False,
                                "checkpoints": False,
                            },
                        },
                    }
                ]
            }
        )
    )
    return Config()


def _set_worker_status(cfg: Config, status: str) -> None:
    workers_path = Path(cfg.orchestration.workers_path)
    data = json.loads(workers_path.read_text())
    data["workers"][0]["status"] = status
    workers_path.write_text(json.dumps(data))


def _seed_project_registry(cfg: Config) -> None:
    path = Path(cfg.registry.path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "projects": [
                    {
                        "id": "house-story",
                        "name": "House Story",
                        "aliases": ["story project"],
                        "owner": "jules",
                        "members": ["jules"],
                        "visibility": "household",
                        "status": "active",
                        "repos": [{"name": "runtime", "remote": "roughcoder/jarvis", "default": True}],
                        "links": {"jira": "", "urls": ["https://example.test/story"]},
                        "files_root": "projects/house-story/files",
                    },
                    {
                        "id": "neil-shared",
                        "name": "Neil Shared",
                        "aliases": ["shared project"],
                        "owner": "alice",
                        "members": ["alice", "neil"],
                        "visibility": "shared",
                        "status": "active",
                        "repos": [{"name": "notes", "remote": "roughcoder/notes"}],
                        "links": {"jira": "SHARED", "urls": []},
                        "files_root": "projects/neil-shared/files",
                    },
                    {
                        "id": "alice-private",
                        "name": "Alice Private",
                        "owner": "alice",
                        "members": ["alice"],
                        "visibility": "private",
                        "status": "active",
                        "repos": [],
                        "links": {"jira": "", "urls": []},
                        "files_root": "projects/alice-private/files",
                    },
                    {
                        "id": "old-project",
                        "name": "Old Project",
                        "owner": "neil",
                        "members": ["neil"],
                        "visibility": "private",
                        "status": "archived",
                        "repos": [],
                        "links": {"jira": "", "urls": []},
                        "files_root": "projects/old-project/files",
                    },
                ],
                "contacts": [],
            }
        )
    )


def _seed_user_profiles(cfg: Config, *names: str) -> None:
    users_dir = Path(cfg.capabilities.users_dir)
    users_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        users_dir.joinpath(f"{name}.md").write_text(
            f"---\nscope: personal\nhoncho_peer: {name}\n---\n\n# {name.title()}\n",
            encoding="utf-8",
        )


def _seed_user_profile(cfg: Config, name: str, *, capabilities: list[str] | None = None) -> None:
    users_dir = Path(cfg.capabilities.users_dir)
    users_dir.mkdir(parents=True, exist_ok=True)
    caps = f"\ncapabilities: {json.dumps(capabilities)}" if capabilities is not None else ""
    users_dir.joinpath(f"{name}.md").write_text(
        f"---\nscope: personal\nhoncho_peer: {name}{caps}\n---\n\n# {name.title()}\n",
        encoding="utf-8",
    )


def _conclusion(
    cid: str,
    *,
    project_id: str,
    artifact_type: str,
    content: str,
    observed_at: str,
    recorded_by: str = "neil",
    observed_id: str | None = None,
) -> ConclusionRecord:
    return ConclusionRecord(
        id=cid,
        content=content,
        observer_id="jarvis",
        observed_id=observed_id or f"project:{project_id}",
        metadata={
            "project_id": project_id,
            "artifact_type": artifact_type,
            "recorded_by": recorded_by,
            "observed_at": observed_at,
        },
    )


def _sse_events(text: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for block in text.strip().split("\n\n"):
        data = ""
        event = ""
        for line in block.splitlines():
            if line.startswith("event: "):
                event = line[len("event: ") :]
            elif line.startswith("data: "):
                data = line[len("data: ") :]
        if data:
            item = json.loads(data)
            item["_event"] = event
            events.append(item)
    return events


def test_mcp_status_uses_config_fallback_and_redacts_server_specs(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    cfg.mcp.enabled = True
    cfg.mcp.servers = [
        MCPServerSpec(
            name="vault",
            transport="http",
            url="https://secret.example.test/mcp?token=abc",
            headers={"Authorization": "Bearer secret-token"},
            command="/Users/neil/private/bin/mcp",
            args=["--secret", "abc"],
            env={"TOKEN": "secret"},
            capability="mcp.vault",
        )
    ]

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/mcp/status")
        assert response.status_code == 200
        data = response.json()
        assert data["source"] == "config"
        assert data["generated_at"] == ""
        assert data["stale"] is True
        assert data["servers"] == [
            {
                "name": "vault",
                "transport": "http",
                "connected": None,
                "tool_count": 0,
                "error": "",
                "connected_at": None,
                "required_capability": "mcp.vault",
            }
        ]
        raw = json.dumps(data)
        assert "secret.example" not in raw
        assert "Authorization" not in raw
        assert "/Users/neil" not in raw
        assert "token_store_path" not in raw
        assert data["serve"]["configured"] is False
        assert data["serve"]["auth_mode"] == "hybrid"
        assert data["serve"]["oauth"] == {
            "configured": False,
            "issuer": "",
            "resource": "http://localhost:8795",
            "metadata_url": "http://localhost:8795/.well-known/oauth-protected-resource",
        }
        assert data["serve"]["tokens"] == {"active": 0, "revoked": 0}
        assert data["serve"]["codex_wired"] is False
        assert data["serve"]["codex_wired_reason"]

    asyncio.run(_with_server(cfg, calls))


def test_mcp_status_and_tools_use_snapshot_with_server_filter(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    store = MCPTokenStore(cfg.mcp_serve.token_store_path)
    _token, record = store.add(principal="neil", name="Codex")
    store.revoke(record.token_id)
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    mcp_status_path(cfg).write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "servers": [
                    {
                        "name": "linear",
                        "transport": "http",
                        "connected": True,
                        "tool_count": 1,
                        "error": "",
                        "connected_at": "2026-07-06T09:59:59Z",
                        "required_capability": "mcp.linear",
                    },
                    {
                        "name": "local",
                        "transport": "stdio",
                        "connected": False,
                        "tool_count": 0,
                        "error": "failed at /Users/neil/secret with sk-test123456789012",
                        "required_capability": "mcp.local",
                    },
                ],
                "tools": [
                    {
                        "offered_name": "linear_search",
                        "server": "linear",
                        "description": "Search issues",
                        "required_capability": "mcp.linear",
                    },
                    {
                        "offered_name": "local_read",
                        "server": "local",
                        "description": "Read /Users/neil/private with sk-test123456789012",
                        "required_capability": "mcp.local",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        status = (await client.get(f"{base}/v1/mcp/status")).json()
        assert status["source"] == "snapshot"
        assert status["generated_at"] == generated_at
        assert status["stale"] is False
        assert status["servers"][0]["connected"] is True
        assert status["servers"][1]["error"] == "failed at <local-path> with <redacted-token>"
        assert status["serve"]["configured"] is True
        assert status["serve"]["auth_mode"] == "hybrid"
        assert status["serve"]["tokens"] == {"active": 0, "revoked": 1}

        tools = (await client.get(f"{base}/v1/mcp/tools")).json()
        assert tools["stale"] is False
        assert [tool["name"] for tool in tools["tools"]] == ["linear_search", "local_read"]
        filtered = (await client.get(f"{base}/v1/mcp/tools", params={"server": "linear"})).json()
        assert filtered["tools"] == [
            {
                "name": "linear_search",
                "server": "linear",
                "description": "Search issues",
                "required_capability": "mcp.linear",
            }
        ]
        local_only = (await client.get(f"{base}/v1/mcp/tools", params={"server": "local"})).json()
        assert local_only["tools"][0]["description"] == "Read <local-path> with <redacted-token>"

    asyncio.run(_with_server(cfg, calls))


def test_mcp_status_projects_mcp_serve_oauth_config(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        mcp_serve_auth_mode="oauth",
        mcp_serve_resource_url="https://jarvis.example",
        mcp_serve_oauth_issuer="https://cockpit.example",
        mcp_serve_oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        mcp_serve_oauth_required_scopes="mcp:use",
    )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/mcp/status")
        assert response.status_code == 200
        serve = response.json()["serve"]
        assert serve["auth_mode"] == "oauth"
        assert serve["oauth"] == {
            "configured": True,
            "issuer": "https://cockpit.example",
            "resource": "https://jarvis.example",
            "metadata_url": "https://jarvis.example/.well-known/oauth-protected-resource",
        }
        raw = json.dumps(serve)
        assert "jwks" not in raw.lower()
        assert "token_store_path" not in raw

    asyncio.run(_with_server(cfg, calls))


def test_mcp_status_oauth_projection_validates_runtime_urls(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        mcp_serve_auth_mode="hybrid",
        mcp_serve_resource_url="https://jarvis.example/mcp/serve",
        mcp_serve_oauth_issuer="http://cockpit.example",
        mcp_serve_oauth_jwks_url="https://cockpit.example/api/auth/jwks",
    )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/mcp/status")
        assert response.status_code == 200
        assert response.json()["serve"]["oauth"] == {
            "configured": False,
            "issuer": "http://cockpit.example",
            "resource": "https://jarvis.example/mcp/serve",
            "metadata_url": "https://jarvis.example/.well-known/oauth-protected-resource/mcp/serve",
        }

    asyncio.run(_with_server(cfg, calls))


def test_mcp_status_marks_old_snapshot_stale(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    old_generated_at = (datetime.now(UTC) - timedelta(hours=2)).replace(microsecond=0).isoformat()
    mcp_status_path(cfg).write_text(
        json.dumps({"generated_at": old_generated_at, "servers": [], "tools": []}),
        encoding="utf-8",
    )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        status = (await client.get(f"{base}/v1/mcp/status")).json()
        assert status["source"] == "snapshot"
        assert status["generated_at"] == old_generated_at
        assert status["stale"] is True
        tools = (await client.get(f"{base}/v1/mcp/tools")).json()
        assert tools["stale"] is True

    asyncio.run(_with_server(cfg, calls))


def test_mcp_token_lifecycle_over_http(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="mcp.tokens.manage")
    _seed_user_profiles(cfg, "neil")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        issued = await client.post(
            f"{base}/v1/mcp/tokens",
            json={"principal": "neil", "name": "Codex", "idempotency_key": "issue-1"},
        )
        assert issued.status_code == 200
        body = issued.json()
        assert body["ok"] is True
        assert body["token"].startswith("jv_mcp_")
        assert body["record"]["principal"] == "neil"
        assert set(body["record"]) == {"token_id", "principal", "name", "prefix", "created_at", "revoked_at"}

        replay = await client.post(
            f"{base}/v1/mcp/tokens",
            json={"principal": "neil", "name": "Codex", "idempotency_key": "issue-1"},
        )
        assert replay.status_code == 200
        assert replay.json()["token"] == ""
        assert replay.json()["idempotent"] is True

        listed = await client.get(f"{base}/v1/mcp/tokens")
        assert listed.status_code == 200
        records = listed.json()["tokens"]
        assert len(records) == 1
        assert records[0]["token_id"] == body["record"]["token_id"]
        assert "token_hash" not in records[0]
        assert body["token"] not in json.dumps(records)

        revoked = await client.delete(f"{base}/v1/mcp/tokens/{body['record']['token_id']}")
        assert revoked.status_code == 200
        assert revoked.json()["record"]["revoked_at"]

        active = (await client.get(f"{base}/v1/mcp/tokens")).json()
        assert active["tokens"] == []
        all_tokens = (await client.get(f"{base}/v1/mcp/tokens", params={"include_revoked": "true"})).json()
        assert all_tokens["tokens"][0]["revoked_at"]

    asyncio.run(_with_server(cfg, calls))


def test_mcp_token_issue_serializes_concurrent_writes(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="mcp.tokens.manage")
    _seed_user_profiles(cfg, "neil")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        responses = await asyncio.gather(
            *[
                client.post(f"{base}/v1/mcp/tokens", json={"principal": "neil", "name": f"client-{idx}"})
                for idx in range(12)
            ]
        )
        assert {response.status_code for response in responses} == {200}
        token_ids = {response.json()["record"]["token_id"] for response in responses}
        assert len(token_ids) == 12
        listed = (await client.get(f"{base}/v1/mcp/tokens")).json()["tokens"]
        assert {record["token_id"] for record in listed} == token_ids

    asyncio.run(_with_server(cfg, calls))


def test_mcp_token_issue_revokes_record_when_idempotency_save_fails(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="mcp.tokens.manage")
    _seed_user_profiles(cfg, "neil")
    original_save = cockpit_api_module.IdempotencyStore.save

    def fail_mcp_token_save(self, scope, key, body, response):  # noqa: ANN001
        if scope == "mcp/tokens":
            raise OSError("workspace full at /Users/neil/private")
        return original_save(self, scope, key, body, response)

    monkeypatch.setattr(cockpit_api_module.IdempotencyStore, "save", fail_mcp_token_save)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{base}/v1/mcp/tokens",
            json={"principal": "neil", "name": "Codex", "idempotency_key": "issue-fails"},
        )
        assert response.status_code == 500
        assert response.json()["error"]["code"] == "internal_error"
        assert response.json()["error"]["message"] == "workspace full at <local-path>"

    asyncio.run(_with_server(cfg, calls))
    records = MCPTokenStore(cfg.mcp_serve.token_store_path).list(include_revoked=True)
    assert len(records) == 1
    assert records[0].revoked


def test_mcp_token_errors_and_capability_gate(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="mcp.tokens.manage")
    _seed_user_profiles(cfg, "neil")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        unknown_principal = await client.post(
            f"{base}/v1/mcp/tokens",
            json={"principal": "alice", "name": "Codex"},
        )
        assert unknown_principal.status_code == 400
        assert unknown_principal.json()["error"]["code"] == "validation_failed"

        missing = await client.delete(f"{base}/v1/mcp/tokens/mcptok_missing")
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "not_found"

        token_store = Path(cfg.mcp_serve.token_store_path)
        token_store.parent.mkdir(parents=True, exist_ok=True)
        token_store.write_text("{not json", encoding="utf-8")
        corrupt = await client.post(
            f"{base}/v1/mcp/tokens",
            json={"principal": "neil", "name": "Codex"},
        )
        assert corrupt.status_code == 500
        assert corrupt.json()["error"]["code"] == "internal_error"

    asyncio.run(_with_server(cfg, calls))

    denied_root = tmp_path / "denied"
    denied_root.mkdir()
    denied = _cfg(denied_root, monkeypatch, caps="")
    _seed_user_profiles(denied, "neil")

    async def denied_calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/mcp/tokens")
        assert response.status_code == 403
        assert response.json()["error"]["message"] == "missing authority: mcp.tokens.manage"

    asyncio.run(_with_server(denied, denied_calls))


def _oauth_fixture(*, kid: str = "test-key", include_alg: bool = True) -> tuple[dict[str, Any], Callable[..., Response]]:
    jwt = pytest.importorskip("jwt")
    cryptography_rsa = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.rsa")
    cryptography_serialization = pytest.importorskip("cryptography.hazmat.primitives.serialization")

    private_key = cryptography_rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=cryptography_serialization.Encoding.PEM,
        format=cryptography_serialization.PrivateFormat.PKCS8,
        encryption_algorithm=cryptography_serialization.NoEncryption(),
    )
    public_jwk = json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(private_key.public_key()))
    public_jwk.update({"kid": kid, "use": "sig"})
    if include_alg:
        public_jwk["alg"] = "RS256"
    jwks = {"keys": [public_jwk]}

    def sign(
        *,
        issuer: str = "https://cockpit.example",
        audience: str = "jarvis-brain",
        subject: str = "user_123",
        jarvis_user: str = "neil",
        scope: str = "jarvis:read jarvis:operate",
        expires_delta: timedelta = timedelta(minutes=5),
        token_kid: str = kid,
        algorithm: str = "RS256",
        signing_key: Any = private_pem,
    ) -> str:
        now = datetime.now(UTC)
        claims = {
            "iss": issuer,
            "sub": subject,
            "aud": audience,
            "scope": scope,
            "exp": now + expires_delta,
            "iat": now,
            "jarvis_user": jarvis_user,
        }
        return jwt.encode(claims, signing_key, algorithm=algorithm, headers={"kid": token_kid})

    calls: dict[str, Any] = {"jwks": 0, "threads": []}

    def jwks_get(url: str, **_kwargs: Any) -> Response:
        if url == "https://cockpit.example/api/auth/jwks":
            calls["jwks"] += 1
            calls["threads"].append(threading.get_ident())
            return Response(jwks)
        return Response({})

    return {"sign": sign, "calls": calls, "jwks": jwks}, jwks_get


def _oauth_validator(http_get: Callable[..., Response], *, jwks_min_refresh_s: float = 30.0, jwks_ttl_s: float = 300.0) -> OAuthTokenValidator:
    return OAuthTokenValidator(
        issuer="https://cockpit.example",
        audience="jarvis-brain",
        jwks_url="https://cockpit.example/api/auth/jwks",
        scopes=("jarvis:read",),
        jarvis_user_claim="jarvis_user",
        default_alg="RS256",
        jwks_ttl_s=jwks_ttl_s,
        jwks_min_refresh_s=jwks_min_refresh_s,
        http_get=http_get,
    )


def _unsigned_jwt_with_kid(kid: str) -> str:
    def encode(data: dict[str, Any]) -> str:
        raw = json.dumps(data, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    return f"{encode({'alg': 'RS256', 'kid': kid})}.{encode({})}.{encode({'sig': 'invalid'})}"


def _seed_run(cfg: Config) -> tuple[OrchestrationStore, str]:
    store = OrchestrationStore(cfg.orchestration.workspace)
    item = WorkItem(
        source="github",
        id="#47",
        title="Build worker sessions",
        repo="roughcoder/jarvis",
        body="private implementation detail",
        source_internal_id="internal_47",
    )
    run = store.create_run("Expose live worker sessions", work_items=[item])
    store.link_session(
        run.run_id,
        WorkerSessionLink(
            worker_id="macbook-worker",
            session_id="sess_123",
            status="running",
            provider="codex",
            engine="codex",
            branch="jarvis/foo",
            cwd="/Users/example/private/jarvis",
            last_event_id="ev_2",
            allowed_actions=[
                "worker.session.turn",
                "worker.session.input",
                "worker.session.approve",
                "worker.session.interrupt",
                "worker.session.stop",
                "worker.session.restore",
            ],
        ),
    )
    store.append_event(
        run.run_id,
        "verification_started",
        "Running tests in /Users/example/private/jarvis",
        {
            "command": "pytest /Users/example/private/jarvis",
            "cwd": "/Users/example/private/jarvis",
            "token_env": "OPENAI_API_KEY",
        },
    )
    store.link_artifact(run.run_id, Artifact(type="pull_request", id="47", url="https://github.com/roughcoder/jarvis/pull/47", status="open"))
    return store, run.run_id


def _worker_system_health() -> dict[str, Any]:
    return {
        "hostname": "neil-laptop",
        "platform": "darwin",
        "arch": "arm64",
        "os_name": "macOS",
        "os_version": "15.5",
        "kernel_version": "24.5.0",
        "cpu_model": "Apple M4 Pro",
        "cpu_cores_physical": 12,
        "cpu_cores_logical": 12,
        "memory_total_bytes": 51539607552,
        "memory_available_bytes": 21474836480,
        "memory_used_bytes": 30064771072,
        "memory_used_percent": 58.3,
        "load_average": [2.12, 2.44, 2.19],
        "uptime_seconds": 384220,
        "disk": [
            {
                "mount": "/",
                "filesystem": "apfs",
                "total_bytes": 994662584320,
                "available_bytes": 420118257664,
                "used_bytes": 574544326656,
                "used_percent": 57.8,
            }
        ],
        "gpu": [{"name": "Apple M4 Pro", "memory_total_bytes": None}],
        "checked_at": "2026-07-02T23:35:00Z",
    }


def _fake_get(run_id: str):  # noqa: ANN202
    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/health"):
            return Response(
                {
                    "ok": True,
                    "agent": "codex",
                    "supported_engines": ["codex", "claude"],
                    "system": _worker_system_health(),
                    "worktree_inventory": {"count": 3, "disk_bytes": 2048, "stale_count": 1},
                }
            )
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response(
                {
                    "sessions": [
                        {
                            "session_id": "sess_123",
                            "run_id": run_id,
                            "provider": "codex",
                            "engine": "codex",
                            "status": "running",
                            "repo": "roughcoder/jarvis",
                            "branch": "jarvis/foo",
                            "cwd": "/Users/example/private/jarvis",
                            "title": "Codex implementation",
                            "created_at": "2026-07-01T11:00:00Z",
                            "updated_at": "2026-07-01T12:00:00Z",
                        },
                    ]
                }
            )
        if url.endswith("/sessions/sess_123"):
            return Response(
                {
                    "session_id": "sess_123",
                    "run_id": run_id,
                    "provider": "codex",
                    "engine": "codex",
                    "status": "running",
                    "repo": "roughcoder/jarvis",
                    "branch": "jarvis/foo",
                    "cwd": "/Users/example/private/jarvis",
                    "metadata": {"provider_pid": 1234},
                    "title": "Codex implementation",
                    "created_at": "2026-07-01T11:00:00Z",
                    "updated_at": "2026-07-01T12:00:00Z",
                }
            )
        if url.endswith("/sessions/sess_123/events"):
            return Response(
                {
                    "events": [
                        {
                            "event_id": "ev_1",
                            "session_id": "sess_123",
                            "type": "turn.started",
                            "time": "2026-07-01T11:00:00Z",
                            "data": {"turn_id": "turn_1"},
                        },
                        {
                            "event_id": "ev_2",
                            "session_id": "sess_123",
                            "type": "assistant.delta",
                            "time": "2026-07-01T11:00:01Z",
                            "data": {
                                "turn_id": "turn_1",
                                "delta": "hello",
                                "command": "cat /Users/example/private/secret.txt",
                                "cwd": "/Users/example/private/jarvis",
                                "token_env": "OPENAI_API_KEY",
                                "execution_envelope": {"allowed_actions": ["worker.session.turn"]},
                                "metadata": {"provider_pid": 1234},
                            },
                        },
                    ]
                }
            )
        if url.endswith("/sessions/sess_123/requests") or url.endswith("/sessions/requests"):
            return Response(
                {
                    "requests": [
                        {
                            "session_id": "sess_123",
                            "request_id": "req_approval",
                            "kind": "approval",
                            "status": "pending",
                            "event": {
                                "event_id": "ev_req",
                                "session_id": "sess_123",
                                "type": "approval.requested",
                                "time": "2026-07-01T11:01:00Z",
                                "data": {
                                    "run_id": run_id,
                                    "title": "Approve file edits",
                                    "detail": "/Users/example/private/file",
                                    "payload": {
                                        "request_kind": "file-change",
                                        "cwd": "/Users/example/private/jarvis",
                                        "token_env": "OPENAI_API_KEY",
                                        "access_token": "oauth_access_secret",
                                        "refresh-token": "oauth_refresh_secret",
                                        "client_secret": "oauth_client_secret",
                                        "Authorization": "Bearer oauth_authorization_secret",
                                        "credential": "oauth_credential_secret",
                                    },
                                },
                            },
                        },
                        {
                            "session_id": "sess_123",
                            "request_id": "req_input",
                            "kind": "input",
                            "status": "pending",
                            "event": {
                                "event_id": "ev_input",
                                "session_id": "sess_123",
                                "type": "input.requested",
                                "time": "2026-07-01T11:02:00Z",
                                "data": {
                                    "run_id": run_id,
                                    "title": "Input needed for http://localhost:8780/callback?token=secret",
                                    "question": "Use /workspace/private/jarvis?",
                                    "questions": [
                                        {
                                            "id": "response",
                                            "header": "Input",
                                            "question": "Continue with /home/jarvis/private and http://localhost:8780/callback?token=secret?",
                                            "options": [
                                                {"label": "Use /workspace/private", "value": "http://localhost:8780/logs?token=secret"},
                                                "Keep going from /tmp/private",
                                            ],
                                        }
                                    ],
                                },
                            },
                        }
                    ]
                }
            )
        if url.endswith("/sessions/checkpoints") or url.endswith("/sessions/sess_123/checkpoints"):
            return Response(
                {
                    "checkpoints": [
                        {
                            "session_id": "sess_123",
                            "checkpoint_id": "ckpt_1",
                            "label": "before tests",
                            "provider": "codex",
                            "restored": False,
                            "cwd": "/Users/example/private/jarvis",
                            "metadata": {"provider_pid": 1234},
                            "payload": {
                                "command": "pytest /Users/example/private/jarvis",
                                "token_env": "OPENAI_API_KEY",
                                "api-key": "provider_api_key_secret",
                                "clientSecret": "provider_client_secret",
                                "refresh_token": "provider_refresh_secret",
                            },
                        }
                    ]
                }
            )
        raise AssertionError(url)

    return get


def test_cockpit_catalog_snapshot_and_worker_projection(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)
    get = _fake_get(run_id)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        catalog = (await client.get(f"{base}/v1/cockpit/catalog")).json()
        stale_snapshot = (await client.get(f"{base}/v1/cockpit/snapshot")).json()
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "probe"})).json()
        workers = (await client.get(f"{base}/v1/workers", params={"sync": "probe"})).json()
        worker_detail = (await client.get(f"{base}/v1/workers/macbook-worker", params={"sync": "probe"})).json()

        assert catalog["api_version"] == "v1"
        assert "manual" in catalog["work_sources"]
        assert "voice" not in catalog["work_sources"]
        assert "whatsapp" not in catalog["work_sources"]
        assert "review_panel" not in catalog["engine_strategies"]
        assert stale_snapshot["sync"]["status"] == "stale"
        assert snapshot["schema_version"] == 1
        assert snapshot["sync"]["status"] == "fresh"
        assert snapshot["runs"][0]["run_id"] == run_id
        assert snapshot["runs"][0]["authority"] == "jarvis"
        assert "archive" in snapshot["runs"][0]["supported_controls"]
        assert snapshot["runs"][0]["pending_approval_count"] == 1
        assert snapshot["sessions"][0]["session_ref"].startswith("sessref_")
        assert snapshot["sessions"][0]["authority"] == "jarvis"
        assert snapshot["sessions"][0]["cwd_label"] == "jarvis"
        assert "/Users/" not in json.dumps(snapshot)
        assert workers["workers"][0]["capacity"]["max_sessions"] == 4
        assert workers["workers"][0]["engines"][0]["engine"] == "codex"
        assert workers["workers"][0]["engines"][0]["supports"]["checkpoints"] is True
        assert workers["workers"][0]["engines"][1]["engine"] == "claude"
        assert workers["workers"][0]["engines"][1]["supports"]["interrupt"] is False
        assert workers["workers"][0]["worktree_inventory"] == {"count": 3, "disk_bytes": 2048, "stale_count": 1}
        assert snapshot["workers"][0]["worktree_inventory"]["stale_count"] == 1
        assert snapshot["workers"][0]["system"]["cpu_model"] == "Apple M4 Pro"
        assert workers["workers"][0]["system"] == worker_detail["system"]
        assert workers["workers"][0]["system"]["disk"] == [
            {
                "mount": "/",
                "total_bytes": 994662584320,
                "available_bytes": 420118257664,
                "used_percent": 57.8,
            }
        ]
        assert "kernel_version" not in workers["workers"][0]["system"]
        assert "memory_used_bytes" not in workers["workers"][0]["system"]
        assert "filesystem" not in workers["workers"][0]["system"]["disk"][0]
        assert "gpu" not in workers["workers"][0]["system"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_manual_work_without_key_gets_distinct_ids() -> None:
    _command_a, item_a = _command_from_body({"source": "manual", "repo": "roughcoder/jarvis", "phrase": "task a"}, start=True)
    _command_b, item_b = _command_from_body({"source": "manual", "repo": "roughcoder/jarvis", "phrase": "task b"}, start=True)

    assert item_a is not None
    assert item_b is not None
    assert item_a.id.startswith("manual_")
    assert item_b.id.startswith("manual_")
    assert item_a.id != item_b.id


def test_cockpit_snapshot_none_does_not_poll_workers(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, _run_id = _seed_run(cfg)

    def no_worker_get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        raise AssertionError(f"sync=none should not call worker HTTP: {url}")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot")).json()

        assert snapshot["sync"]["status"] == "stale"
        assert snapshot["sessions"][0]["session_ref"].startswith("sessref_")

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=no_worker_get))


def test_cockpit_runs_none_does_not_poll_worker_requests(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)

    def no_worker_get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        raise AssertionError(f"sync=none run list should not call worker HTTP: {url}")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/runs")
        body = response.json()

        assert response.status_code == 200
        assert body["runs"][0]["run_id"] == run_id
        assert body["runs"][0]["pending_approval_count"] == 0

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=no_worker_get))


def test_cockpit_sessions_none_does_not_poll_workers(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)

    def no_worker_get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        raise AssertionError(f"sync=none session list should not call worker HTTP: {url}")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/sessions")
        body = response.json()

        assert response.status_code == 200
        assert body["sessions"][0]["run_id"] == run_id
        assert body["sessions"][0]["pending_approval_count"] == 0

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=no_worker_get))


def test_cockpit_snapshot_probe_uses_probed_worker_status_for_worker_sessions(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _set_worker_status(cfg, "offline")
    _store, run_id = _seed_run(cfg)

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        return _fake_get(run_id)(url, **kwargs)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "probe"})).json()

        assert snapshot["workers"][0]["status"] == "online"
        assert snapshot["sessions"]
        assert snapshot["sessions"][0]["session_id"] == "sess_123"
        assert snapshot["sessions"][0]["latest_event_cursor"] == "ev_2"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_archived_run_is_not_worker_synced(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    _store, run_id = _seed_run(cfg)
    calls_seen: list[str] = []

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        calls_seen.append(url)
        if "/sessions/sess_123" in url or "/jobs/" in url:
            raise AssertionError(f"archived runs should not sync linked worker resources: {url}")
        return _fake_get(run_id)(url, **kwargs)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        archive = await client.post(f"{base}/v1/runs/{run_id}/archive", json={"idempotency_key": "archive_sync_skip"})
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

        assert archive.status_code == 200
        assert snapshot["runs"] == []
        assert all("/sessions/sess_123" not in url for url in calls_seen)

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_snapshot_cursor_tracks_full_projection(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)
    state = {"pending": False}

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/requests") and not state["pending"]:
            return Response({"requests": []})
        if url.endswith("/sessions/sess_123/checkpoints") and not state["pending"]:
            return Response({"checkpoints": []})
        return _fake_get(run_id)(url, **kwargs)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()
        same = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()
        state["pending"] = True
        request_checkpoint_changed = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

        assert same["cursor"] == first["cursor"]
        assert request_checkpoint_changed["cursor"] != first["cursor"]
        assert request_checkpoint_changed["runs"][0]["pending_approval_count"] == 1
        assert request_checkpoint_changed["sessions"][0]["checkpoint_count"] == 1

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_sync_errors_are_redacted(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, _run_id = _seed_run(cfg)
    private_path = "/Users" + "/example/private/jarvis"

    from jarvis.orchestration.supervisor import SyncSummary

    monkeypatch.setattr("jarvis.orchestration.cockpit.sync_run_jobs", lambda *_args, **_kwargs: SyncSummary(errors=[f"failed in {private_path}"]))
    monkeypatch.setattr("jarvis.orchestration.cockpit.sync_run_sessions", lambda *_args, **_kwargs: SyncSummary())

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

        assert snapshot["sync"]["status"] == "partial"
        assert "/Users/" not in json.dumps(snapshot["sync"]["errors"])
        assert "<local-path>" in snapshot["sync"]["errors"][0]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))


def test_cockpit_snapshot_cursor_tracks_worker_projection(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, _run_id = _seed_run(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = (await client.get(f"{base}/v1/cockpit/snapshot")).json()
        _set_worker_status(cfg, "offline")
        changed = (await client.get(f"{base}/v1/cockpit/snapshot")).json()

        assert changed["cursor"] != first["cursor"]
        assert changed["workers"][0]["health"] == "unhealthy"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_worker_projection_tolerates_null_system(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    workers_path = Path(cfg.orchestration.workers_path)
    data = json.loads(workers_path.read_text())
    data["workers"][0]["system"] = None
    workers_path.write_text(json.dumps(data))

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/workers")
        body = response.json()

        assert response.status_code == 200
        assert body["workers"][0]["system"]["hostname"] is None
        assert body["workers"][0]["system"]["disk"] == []

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_snapshot_cursor_ignores_worker_checked_at(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, _run_id = _seed_run(cfg)
    workers_path = Path(cfg.orchestration.workers_path)
    data = json.loads(workers_path.read_text())
    data["workers"][0]["system"] = _worker_system_health()
    workers_path.write_text(json.dumps(data))

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = (await client.get(f"{base}/v1/cockpit/snapshot")).json()
        data["workers"][0]["system"]["checked_at"] = "2026-07-02T23:36:00Z"
        workers_path.write_text(json.dumps(data))
        same = (await client.get(f"{base}/v1/cockpit/snapshot")).json()

        assert first["workers"][0]["system"]["checked_at"] == "2026-07-02T23:35:00Z"
        assert same["workers"][0]["system"]["checked_at"] == "2026-07-02T23:36:00Z"
        assert same["cursor"] == first["cursor"]

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_snapshot_uses_stable_partial_sync_status(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)

    def degraded_get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/sess_123"):
            return Response({"error": "temporarily unavailable"}, status_code=503)
        return _fake_get(run_id)(url, **kwargs)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

        assert snapshot["sync"]["status"] == "partial"
        assert snapshot["sync"]["errors"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=degraded_get))


def test_cockpit_worker_health_uses_stable_unhealthy_status(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _set_worker_status(cfg, "offline")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        workers = (await client.get(f"{base}/v1/workers")).json()

        assert workers["workers"][0]["status"] == "offline"
        assert workers["workers"][0]["health"] == "unhealthy"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_session_detail_events_requests_and_checkpoints(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        detail = (await client.get(f"{base}/v1/sessions/{ref}")).json()
        events = (await client.get(f"{base}/v1/sessions/{ref}/events", params={"limit": 1})).json()
        requests = (await client.get(f"{base}/v1/sessions/{ref}/requests")).json()
        checkpoints = (await client.get(f"{base}/v1/sessions/{ref}/checkpoints")).json()

        assert detail["session"]["run_id"] == run_id
        assert detail["session"]["authority"] == "jarvis"
        assert "archive" in detail["session"]["supported_controls"]
        assert "checkpoint_restore" in detail["session"]["supported_controls"]
        assert "cwd" not in detail["raw"]
        assert "metadata" not in detail["raw"]
        assert "provider_pid" not in json.dumps(detail["raw"])
        assert events["items"][0]["sequence"] == 1
        assert events["has_more"] is True
        next_events = (await client.get(f"{base}/v1/sessions/{ref}/events", params={"after": "ev_1"})).json()["items"]
        assert next_events[0]["event_id"] == "ev_2"
        assert next_events[0]["sequence"] == 2
        all_events = (await client.get(f"{base}/v1/sessions/{ref}/events")).json()["items"]
        delta = [event for event in all_events if event["type"] == "assistant.delta"][0]
        assert delta["message_id"] == "msg_turn_1"
        assert "hello" in json.dumps(delta)
        assert "<local-path>" in json.dumps(delta)
        assert "OPENAI_API_KEY" not in json.dumps(all_events)
        assert "execution_envelope" not in json.dumps(all_events)
        assert "provider_pid" not in json.dumps(all_events)
        assert requests["requests"][0]["title"] == "Approve file edits"
        assert "<local-path>" in requests["requests"][0]["detail"]
        assert "<local-path>" in json.dumps(requests["requests"][1]["questions"])
        assert "localhost" not in json.dumps(requests)
        assert "token=secret" not in json.dumps(requests)
        assert "OPENAI_API_KEY" not in json.dumps(requests)
        assert "oauth_access_secret" not in json.dumps(requests)
        assert "oauth_refresh_secret" not in json.dumps(requests)
        assert "oauth_client_secret" not in json.dumps(requests)
        assert "oauth_authorization_secret" not in json.dumps(requests)
        assert "oauth_credential_secret" not in json.dumps(requests)
        assert "cwd" not in json.dumps(requests)
        assert checkpoints["checkpoints"][0]["session_ref"] == ref
        assert checkpoints["checkpoints"][0]["checkpoint_id"] == "ckpt_1"
        assert "<local-path>" in json.dumps(checkpoints)
        assert "provider_pid" not in json.dumps(checkpoints)
        assert "OPENAI_API_KEY" not in json.dumps(checkpoints)
        assert "provider_api_key_secret" not in json.dumps(checkpoints)
        assert "provider_client_secret" not in json.dumps(checkpoints)
        assert "provider_refresh_secret" not in json.dumps(checkpoints)

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_session_supported_controls_follow_allowed_actions(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    store = OrchestrationStore(cfg.orchestration.workspace)
    item = WorkItem(source="manual", id="manual_controls", title="Limited controls", repo="roughcoder/jarvis")
    run = store.create_run("Limited controls", work_items=[item])
    store.link_session(
        run.run_id,
        WorkerSessionLink(
            worker_id="macbook-worker",
            session_id="sess_limited",
            status="running",
            provider="codex",
            engine="codex",
            allowed_actions=["worker.session.turn", "worker.session.stop"],
        ),
    )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot")).json()

        assert snapshot["sessions"][0]["supported_controls"] == ["turn", "stop", "close", "archive", "unarchive", "rename"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run.run_id)))


def test_cockpit_session_detail_raw_projection_is_redacted(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/sess_123"):
            data = _fake_get(run_id)(url, **kwargs).json()
            data["title"] = "Continue in /home/jarvis/private with ghp_abcdefghijklmnopqrstuvwxyz"
            data["raw"] = {"provider_prompt": "secret"}
            return Response(data)
        return _fake_get(run_id)(url, **kwargs)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        detail = (await client.get(f"{base}/v1/sessions/{ref}")).json()
        text = json.dumps(detail["raw"])

        assert "/home/" not in text
        assert "ghp_abcdefghijklmnopqrstuvwxyz" not in text
        assert "provider_prompt" not in text
        assert "<local-path>" in text
        assert "<redacted-token>" in text

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_exact_session_requests_include_run_id(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/sess_123/requests"):
            return Response(
                {
                    "requests": [
                        {
                            "session_id": "sess_123",
                            "request_id": "req_without_run",
                            "kind": "approval",
                            "status": "pending",
                            "event": {
                                "event_id": "ev_req_without_run",
                                "session_id": "sess_123",
                                "type": "approval.requested",
                                "data": {"title": "Approve edits"},
                            },
                        }
                    ]
                }
            )
        return _fake_get(run_id)(url, **kwargs)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        requests = (await client.get(f"{base}/v1/sessions/{ref}/requests")).json()["requests"]

        assert requests[0]["run_id"] == run_id

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_run_events_and_artifact_pagination(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        detail = (await client.get(f"{base}/v1/runs/{run_id}")).json()
        events = (await client.get(f"{base}/v1/runs/{run_id}/events", params={"limit": 1})).json()
        artifacts = (await client.get(f"{base}/v1/runs/{run_id}/artifacts", params={"limit": 2})).json()
        all_artifacts = (await client.get(f"{base}/v1/runs/{run_id}/artifacts")).json()

        assert detail["run"]["run_id"] == run_id
        assert "private implementation detail" not in json.dumps(detail)
        assert "internal_47" not in json.dumps(detail)
        assert "/Users/" not in json.dumps(detail)
        assert events["items"][0]["type"] == "run_created"
        unknown_cursor = await client.get(f"{base}/v1/runs/{run_id}/events", params={"after": "evt_missing"})
        event_page = (await client.get(f"{base}/v1/runs/{run_id}/events")).json()
        assert "/Users/" not in json.dumps(event_page)
        assert "OPENAI_API_KEY" not in json.dumps(event_page)
        assert "cwd" not in json.dumps(event_page)
        assert events["has_more"] is True
        assert unknown_cursor.status_code == 400
        assert unknown_cursor.json()["error"]["code"] == "stale_cursor"
        assert unknown_cursor.json()["error"]["recoverable"] is True
        kinds = {item["kind"] for item in artifacts["items"]}
        report = [item for item in all_artifacts["items"] if item["kind"] == "report"][0]
        assert {"branch", "pull_request"}.issubset(kinds)
        assert report["created_at"]
        assert report["updated_at"]
        assert artifacts["has_more"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_sse_emits_snapshot_with_cursor(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        async with client.stream("GET", f"{base}/v1/cockpit/events", headers={"Last-Event-ID": "stale"}) as response:
            first = ""
            async for chunk in response.aiter_text():
                first += chunk
                if "\n\n" in first:
                    break

        assert "event: snapshot" in first
        assert "id: evt_" in first
        assert '"type": "snapshot"' in first
        assert '"occurred_at":' in first

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_sse_emits_snapshot_when_projection_cursor_changes(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    store, run_id = _seed_run(cfg)
    worker_calls: list[str] = []

    def no_worker_poll_get(url: str, **kwargs) -> Response:  # noqa: ANN001
        worker_calls.append(url)
        return _fake_get(run_id)(url, **kwargs)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        current = (await client.get(f"{base}/v1/cockpit/snapshot")).json()["cursor"]

        async def mutate_run() -> None:
            import asyncio

            await asyncio.sleep(0.1)
            run = store.get(run_id)
            assert run is not None
            run.phase = "verifying"
            store.save(run)

        import asyncio

        task = asyncio.create_task(mutate_run())
        seen = ""
        async with client.stream("GET", f"{base}/v1/cockpit/events", params={"after": current}) as response:
            async for chunk in response.aiter_text():
                seen += chunk
                if "event: run.updated" in seen:
                    break
        await task

        # A client exactly one tick behind receives granular events, not a snapshot.
        assert "event: run.updated" in seen
        assert "event: snapshot" not in seen
        assert '"occurred_at":' in seen
        assert f'"cursor": "{current}"' not in seen
        assert '"phase": "verifying"' in seen
        assert worker_calls == []

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=no_worker_poll_get))


def test_cockpit_sse_preserves_requested_sync_mode(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)
    state = {"pending": False}

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/requests"):
            if not state["pending"]:
                return Response({"requests": []})
            return Response(
                {
                    "requests": [
                        {
                            "session_id": "sess_123",
                            "request_id": "req_sse",
                            "kind": "approval",
                            "status": "pending",
                            "event": {"data": {"run_id": run_id, "title": "Approve SSE state"}},
                        }
                    ]
                }
            )
        return _fake_get(run_id)(url, **kwargs)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        current = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()["cursor"]

        async def mutate_worker_request() -> None:
            import asyncio

            await asyncio.sleep(0.1)
            state["pending"] = True

        import asyncio

        task = asyncio.create_task(mutate_worker_request())
        seen = ""
        async with client.stream("GET", f"{base}/v1/cockpit/events", params={"after": current, "sync": "fast"}) as response:
            async for chunk in response.aiter_text():
                seen += chunk
                if '"pending_approval_count": 1' in seen:
                    break
        await task

        # Granular updates only reach the stream because the hub kept polling in
        # the requested fast sync mode.
        assert "event: run.updated" in seen or "event: session.updated" in seen
        assert '"pending_approval_count": 1' in seen

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_sse_hub_fans_out_one_refresh_to_multiple_subscribers(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)
    state = {"pending": False}
    request_calls = {"count": 0}

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/requests"):
            request_calls["count"] += 1
            if state["pending"]:
                return Response(
                    {
                        "requests": [
                            {
                                "session_id": "sess_123",
                                "request_id": "req_fanout",
                                "kind": "approval",
                                "status": "pending",
                                "event": {"data": {"run_id": run_id, "title": "Approve fanout"}},
                            }
                        ]
                    }
                )
            return Response({"requests": []})
        return _fake_get(run_id)(url, **kwargs)

    ctx = CockpitAppContext(
        cfg=cfg,
        get=get,
        post=lambda *_args, **_kwargs: Response({}),
        store=OrchestrationStore(cfg.orchestration.workspace),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )

    async def run_hub() -> None:
        hub = SseSnapshotHub(ctx)
        await hub.start()
        try:
            first = await hub.subscribe("fast")
            second = await hub.subscribe("fast")
            assert request_calls["count"] == 1
            state["pending"] = True
            first_event = await asyncio.wait_for(first.queue.get(), timeout=2)
            second_event = await asyncio.wait_for(second.queue.get(), timeout=2)
            assert first_event is not None
            assert second_event is not None
            assert first_event["body"]["cursor"] == second_event["body"]["cursor"]
            assert request_calls["count"] == 2
        finally:
            await hub.stop()

    import asyncio

    asyncio.run(run_hub())


def test_cockpit_sse_hub_survives_snapshot_refresh_exception(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    cfg.orchestration.sse_refresh_interval_s = 0.1
    calls = {"count": 0}

    def snapshot(_ctx, _mode):  # noqa: ANN001
        calls["count"] += 1
        if calls["count"] == 1:
            return {"cursor": "evt_initial"}
        if calls["count"] == 2:
            raise OSError("bad run file")
        return {"cursor": "evt_recovered"}

    monkeypatch.setattr(cockpit_api_module, "_cockpit_snapshot", snapshot)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: Response({}),
        post=lambda *_args, **_kwargs: Response({}),
        store=OrchestrationStore(cfg.orchestration.workspace),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )

    async def run_hub() -> None:
        hub = SseSnapshotHub(ctx)
        await hub.start()
        try:
            subscription = await hub.subscribe("none")
            ctx.store.bump_generation()
            event = await asyncio.wait_for(subscription.queue.get(), timeout=1)
            assert event is not None
            assert event["body"] == {"cursor": "evt_recovered"}
            assert calls["count"] >= 3
        finally:
            await hub.stop()

    import asyncio

    asyncio.run(run_hub())


def test_cockpit_sse_hub_throttles_repeated_refresh_exception_logs(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    cfg.orchestration.sse_refresh_interval_s = 0.1
    calls = {"count": 0}
    logs = []

    def snapshot(_ctx, _mode):  # noqa: ANN001
        calls["count"] += 1
        if calls["count"] == 1:
            return {"cursor": "evt_initial"}
        raise OSError("bad run file")

    def log_exception(message: str) -> None:
        logs.append(message)

    monkeypatch.setattr(cockpit_api_module, "_cockpit_snapshot", snapshot)
    monkeypatch.setattr(cockpit_api_module.logger, "exception", log_exception)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: Response({}),
        post=lambda *_args, **_kwargs: Response({}),
        store=OrchestrationStore(cfg.orchestration.workspace),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )

    async def run_hub() -> None:
        hub = SseSnapshotHub(ctx)
        await hub.start()
        try:
            await hub.subscribe("none")
            ctx.store.bump_generation()
            await asyncio.sleep(0.35)
        finally:
            await hub.stop()

    import asyncio

    asyncio.run(run_hub())

    assert calls["count"] >= 3
    assert logs == ["cockpit SSE snapshot refresh failed"]


def test_cockpit_sse_hub_skips_snapshot_recompute_when_generation_is_unchanged(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    cfg.orchestration.sse_refresh_interval_s = 0.05
    calls = {"count": 0}

    def snapshot(_ctx, _mode):  # noqa: ANN001
        calls["count"] += 1
        return {"cursor": "evt_stable", "runs": []}

    monkeypatch.setattr(cockpit_api_module, "_cockpit_snapshot", snapshot)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: Response({}),
        post=lambda *_args, **_kwargs: Response({}),
        store=OrchestrationStore(cfg.orchestration.workspace),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )

    async def run_hub() -> None:
        hub = SseSnapshotHub(ctx)
        await hub.start()
        try:
            await hub.subscribe("none")
            await asyncio.sleep(0.2)
        finally:
            await hub.stop()

    import asyncio

    asyncio.run(run_hub())
    assert calls["count"] == 1


def test_cockpit_sse_hub_forces_refresh_for_external_store_write(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    cfg.orchestration.sse_refresh_interval_s = 0.05
    cfg.orchestration.sse_forced_refresh_ticks = 3
    store, run_id = _seed_run(cfg)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: Response({}),
        post=lambda *_args, **_kwargs: Response({}),
        store=store,
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )

    async def run_hub() -> dict[str, Any]:
        hub = SseSnapshotHub(ctx)
        await hub.start()
        try:
            subscription = await hub.subscribe("none")
            generation = store.generation
            external = store.get(run_id)
            assert external is not None
            external.phase = "verifying"
            # Deliberately bypass the Store API: a future external writer does
            # not advance the in-process generation signal.
            store.run_path(run_id).write_text(json.dumps(external.to_dict(), indent=2, sort_keys=True))
            assert store.generation == generation
            event = await asyncio.wait_for(subscription.queue.get(), timeout=1)
            assert event is not None
            return event["body"]
        finally:
            await hub.stop()

    import asyncio

    body = asyncio.run(run_hub())
    assert body["runs"][0]["phase"] == "verifying"


def test_cockpit_sse_hub_backs_off_failed_worker_syncs(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.api import _HubWorkerSync

    cfg = _cfg(tmp_path, monkeypatch)
    cfg.orchestration.sse_sync_backoff_ticks = 5
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("worker offline")),
        post=lambda *_args, **_kwargs: Response({}),
        store=OrchestrationStore(cfg.orchestration.workspace),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )
    hub = SseSnapshotHub(ctx)
    hub._tick = 1  # noqa: SLF001 - unit-test the hub's tick-local backoff contract
    sync = _HubWorkerSync(hub)
    profile = sync.profiles[0]

    with pytest.raises(OSError, match="worker offline"):
        sync.get(f"{profile.base_url}/sessions")

    assert hub._worker_backoff_until[profile.worker_id] == 6  # noqa: SLF001
    hub._tick = 5  # noqa: SLF001
    assert sync.should_sync(profile) is False
    hub._tick = 6  # noqa: SLF001
    assert sync.should_sync(profile) is True


def test_cockpit_sse_probe_respects_worker_backoff_before_profile_probe(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.api import _HubWorkerSync, _hub_worker_state

    cfg = _cfg(tmp_path, monkeypatch)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("backed-off worker was polled")),
        post=lambda *_args, **_kwargs: Response({}),
        store=OrchestrationStore(cfg.orchestration.workspace),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )
    hub = SseSnapshotHub(ctx)
    hub._tick = 2  # noqa: SLF001
    hub._worker_backoff_until["macbook-worker"] = 9  # noqa: SLF001

    state = _hub_worker_state(ctx, "probe", _HubWorkerSync(hub), [])

    assert state["workers"][0]["worker_id"] == "macbook-worker"


def test_cockpit_dirty_worker_state_preserves_cached_other_workers() -> None:
    from jarvis.orchestration.api import _merge_dirty_worker_state

    previous = {
        "workers": [{"worker_id": "dirty", "status": "online"}, {"worker_id": "steady", "status": "online"}],
        "sessions": {
            "dirty-old": {"worker_id": "dirty", "status": "running"},
            "steady-session": {"worker_id": "steady", "status": "running"},
        },
        "requests": [{"worker_id": "steady", "request_id": "keep"}],
        "checkpoints": [{"worker_id": "steady", "checkpoint_id": "keep"}],
    }
    current = {
        "workers": [{"worker_id": "dirty", "status": "busy"}, {"worker_id": "steady", "status": "unknown"}],
        "sessions": {"dirty-new": {"worker_id": "dirty", "status": "completed"}},
        "requests": [{"worker_id": "dirty", "request_id": "new"}],
        "checkpoints": [],
    }

    merged = _merge_dirty_worker_state(previous, current, {"dirty"})

    assert merged["workers"] == [
        {"worker_id": "steady", "status": "online"},
        {"worker_id": "dirty", "status": "busy"},
    ]
    assert set(merged["sessions"]) == {"steady-session", "dirty-new"}
    assert merged["requests"] == [
        {"worker_id": "steady", "request_id": "keep"},
        {"worker_id": "dirty", "request_id": "new"},
    ]
    assert merged["checkpoints"] == [{"worker_id": "steady", "checkpoint_id": "keep"}]


def test_cockpit_sse_hub_notify_wakes_early_and_targets_the_dirty_run(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.supervisor import SyncSummary

    cfg = _cfg(tmp_path, monkeypatch)
    cfg.orchestration.sse_refresh_interval_s = 5.0
    store, run_id = _seed_run(cfg)
    targeted: list[str] = []

    def sync_sessions(*_args, run_id: str = "", **_kwargs) -> SyncSummary:  # noqa: ANN001
        targeted.append(run_id)
        if run_id:
            run = store.get(run_id)
            assert run is not None
            run.phase = "verifying"
            store.save(run)
        return SyncSummary(errors=[])

    monkeypatch.setattr(cockpit_api_module, "sync_run_sessions", sync_sessions)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=_fake_get(run_id),
        post=lambda *_args, **_kwargs: Response({}),
        store=store,
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )

    async def run_hub() -> None:
        hub = SseSnapshotHub(ctx)
        await hub.start()
        try:
            subscription = await hub.subscribe("fast")
            expected = subscription.snapshot
            targeted.clear()  # Ignore the subscription's ordinary initial snapshot sync.
            hub.notify(worker_id="macbook-worker", session_id="sess_123")
            event = await asyncio.wait_for(subscription.queue.get(), timeout=1)
            assert targeted == [run_id]
            assert event is not None
            assert event["body"]["sessions"] == expected["sessions"]
            assert event["body"]["requests"] == expected["requests"]
            assert event["body"]["checkpoints"] == expected["checkpoints"]
        finally:
            await hub.stop()

    import asyncio

    asyncio.run(run_hub())


def test_cockpit_snapshot_serializes_cursor_projection_once(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration import cockpit as cockpit_module
    from jarvis.orchestration.cockpit import cockpit_snapshot

    cfg = _cfg(tmp_path, monkeypatch)
    store, _run_id = _seed_run(cfg)
    original_cursor = cockpit_module.snapshot_cursor
    projections = []

    def recording_cursor(projection):  # noqa: ANN001, ANN202
        projections.append(projection)
        return original_cursor(projection)

    monkeypatch.setattr(cockpit_module, "snapshot_cursor", recording_cursor)
    cockpit_snapshot(
        store=store,
        worker_cfg=cfg.worker,
        workers_path=cfg.orchestration.workers_path,
        sync_mode="none",
    )

    assert len(projections) == 1
    assert isinstance(projections[0], str)


def test_cockpit_health_includes_brain_system_projection(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    monkeypatch.setattr(cockpit_api_module, "system_info_cached", _worker_system_health)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/health")
        body = response.json()

        assert response.status_code == 200
        assert body["ok"] is True
        assert body["system"]["cpu_model"] == "Apple M4 Pro"
        assert body["system"]["disk"] == [
            {
                "mount": "/",
                "total_bytes": 994662584320,
                "available_bytes": 420118257664,
                "used_percent": 57.8,
            }
        ]
        assert "kernel_version" not in body["system"]
        assert "memory_used_bytes" not in body["system"]
        assert "filesystem" not in body["system"]["disk"][0]
        assert "gpu" not in body["system"]

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_auth_and_bad_session_ref_errors(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, token="secret")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        runtime = await client.get(f"{base}/v1/runtime")
        unauthorized = await client.get(f"{base}/v1/health")
        bad_ref = await client.get(f"{base}/v1/sessions/not-a-ref", headers={"Authorization": "Bearer secret"})

        assert runtime.status_code == 200
        assert runtime.json()["runtime"]["channel"] == "production"
        assert unauthorized.status_code == 401
        assert unauthorized.json()["error"]["code"] == "unauthorized"
        assert bad_ref.status_code == 404
        assert bad_ref.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_worker_notify_accepts_only_the_configured_worker_token(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, worker_token="worker-secret")
    _store, _run_id = _seed_run(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        body = {"worker_id": "macbook-worker", "session_id": "sess_123", "kind": "session_event"}
        valid = await client.post(f"{base}/v1/worker/notify", json=body, headers={"Authorization": "Bearer worker-secret"})
        unknown = await client.post(f"{base}/v1/worker/notify", json=body, headers={"Authorization": "Bearer unknown"})

        assert valid.status_code == 200
        assert valid.json() == {"ok": True, "accepted": True}
        assert unknown.status_code == 401
        assert unknown.json()["error"]["code"] == "unauthorized"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_capabilities_requires_auth_and_reports_legacy_principal(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    token_store = tmp_path / "mcp-server" / "tokens.json"
    token_store.parent.mkdir(parents=True)
    token_store.write_text("{}")
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        token="secret",
        identity="neil",
        caps="worker.job.start,worker.session.turn",
        brain_peer_token="brain-secret",
        mcp_enabled="true",
        mcp_servers=json.dumps([{"name": "notes", "transport": "http", "url": "http://localhost:9999/mcp"}]),
        mcp_serve_token_store_path=str(token_store),
    )
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        unauthorized = await client.get(f"{base}/v1/capabilities")
        response = await client.get(f"{base}/v1/capabilities", headers={"Authorization": "Bearer secret"})
        body = response.json()
        routes = {(route["method"], route["path"]) for route in body["routes"]}
        text = json.dumps(body)

        assert unauthorized.status_code == 401
        assert response.status_code == 200
        assert body["principal"] == {"identity": "neil", "scope": "personal", "auth_mode": "legacy"}
        assert body["capabilities"] == ["worker.job.start", "worker.session.turn"]
        assert ("GET", "/v1/capabilities") in routes
        assert ("GET", "/v1/projects/{project_id}/permissions") in routes
        assert ("GET", "/v1/workers/{worker_id}") in routes
        assert "neil-shared" not in text
        assert "sess_123" not in text
        assert "localhost" not in text
        assert "worker.test" not in text
        assert "brain-secret" not in text
        assert str(tmp_path) not in text
        assert body["features"] == {
            "project_writes": {"available": True, "reason": ""},
            "mcp": {"available": True, "serve_configured": True},
            "worker_dispatch": {"available": True, "workers_configured": 1},
        }

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_capabilities_reports_oauth_principal_and_unavailable_features(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg_root = tmp_path / "oauth"
    cfg_root.mkdir()
    cfg = _cfg(
        cfg_root,
        monkeypatch,
        identity="neil",
        caps="worker.session.turn",
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    _seed_user_profile(cfg, "jules", capabilities=["worker.job.start", "worker.session.turn"])
    Path(cfg.orchestration.workers_path).write_text(json.dumps({"workers": []}))
    token = fixture["sign"](subject="jules", jarvis_user="neil", scope="jarvis:read")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/capabilities", headers={"Authorization": f"Bearer {token}"})
        body = response.json()

        assert response.status_code == 200
        assert body["principal"] == {"identity": "jules", "scope": "personal", "auth_mode": "oauth"}
        assert body["capabilities"] == ["worker.session.turn"]
        assert body["features"]["project_writes"]["available"] is False
        assert body["features"]["project_writes"]["reason"]
        assert body["features"]["mcp"] == {"available": False, "serve_configured": False}
        assert body["features"]["worker_dispatch"] == {"available": False, "workers_configured": 0}

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))


def test_cockpit_capabilities_counts_default_worker_when_profiles_file_missing(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg_root = tmp_path / "default-worker"
    cfg_root.mkdir()
    cfg = _cfg(cfg_root, monkeypatch, token="secret", identity="neil")
    Path(cfg.orchestration.workers_path).unlink()

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/capabilities", headers={"Authorization": "Bearer secret"})
        body = response.json()

        assert response.status_code == 200
        assert body["features"]["worker_dispatch"] == {"available": True, "workers_configured": 1}

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_projects_list_is_membership_filtered(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/projects")
        body = response.json()

        assert response.status_code == 200
        assert body["api_version"] == "v1"
        assert [project["id"] for project in body["projects"]] == ["house-story", "neil-shared"]
        assert "alice-private" not in {project["id"] for project in body["projects"]}

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_project_detail_404s_when_not_visible(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        visible = await client.get(f"{base}/v1/projects/neil-shared")
        visible_memory = await client.get(f"{base}/v1/projects/neil-shared/memory")
        hidden = await client.get(f"{base}/v1/projects/alice-private")
        hidden_memory = await client.get(f"{base}/v1/projects/alice-private/memory")
        missing = await client.get(f"{base}/v1/projects/not-real")
        missing_memory = await client.get(f"{base}/v1/projects/not-real/memory")

        assert visible.status_code == 200
        assert visible_memory.status_code == 200
        project = visible.json()["project"]
        assert project == {
            "id": "neil-shared",
            "name": "Neil Shared",
            "peer_id": "project:neil-shared",
            "aliases": ["shared project"],
            "owner": "alice",
            "members": ["alice", "neil"],
            "visibility": "shared",
            "status": "active",
            "repos": [{"name": "notes", "remote": "roughcoder/notes"}],
            "links": {"jira": "SHARED", "urls": []},
            "files_root": "projects/neil-shared/files",
        }
        assert hidden.status_code == 404
        assert hidden.json()["error"]["code"] == "not_found"
        assert hidden_memory.status_code == 404
        assert hidden_memory.json()["error"]["code"] == "not_found"
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "not_found"
        assert missing_memory.status_code == 404
        assert missing_memory.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_project_permissions_project_effective_role(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        member = await client.get(f"{base}/v1/projects/neil-shared/permissions")
        non_member_private = await client.get(f"{base}/v1/projects/alice-private/permissions")
        non_member_household = await client.get(f"{base}/v1/projects/house-story/permissions")
        archived = await client.get(f"{base}/v1/projects/old-project/permissions")
        text = json.dumps({"member": member.json(), "archived": archived.json()})

        assert member.status_code == 200
        assert member.json() == {
            "api_version": "v1",
            "schema_version": 1,
            "project_id": "neil-shared",
            "role": "member",
            "permissions": {
                "can_update": True,
                "can_manage_repos": True,
                "can_create_thread": True,
                "can_archive_thread": True,
                "can_archive": False,
                "can_delete": False,
                "can_manage_members": False,
                "can_set_visibility": False,
            },
        }
        assert non_member_private.status_code == 404
        assert non_member_private.json()["error"]["code"] == "not_found"
        assert non_member_household.status_code == 200
        assert non_member_household.json() == {
            "api_version": "v1",
            "schema_version": 1,
            "project_id": "house-story",
            "role": "viewer",
            "permissions": {
                "can_update": False,
                "can_manage_repos": False,
                "can_create_thread": False,
                "can_archive_thread": False,
                "can_archive": False,
                "can_delete": False,
                "can_manage_members": False,
                "can_set_visibility": False,
            },
        }
        assert archived.status_code == 200
        assert archived.json()["role"] == "owner"
        archived_permissions = archived.json()["permissions"]
        assert all(archived_permissions.values())
        assert "localhost" not in text
        assert str(tmp_path) not in text
        assert "token" not in text.lower()

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_project_permissions_owner_gets_admin_actions(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg_root = tmp_path / "owner"
    cfg_root.mkdir()
    cfg = _cfg(cfg_root, monkeypatch, identity="alice")
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/projects/neil-shared/permissions")
        body = response.json()

        assert response.status_code == 200
        assert body["role"] == "owner"
        assert body["project_id"] == "neil-shared"
        permissions = body["permissions"]
        assert all(permissions.values())

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_project_writes_forward_to_brain_without_direct_registry_write(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    brain = FakeProjectBrainClient()
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        create = await client.post(f"{base}/v1/projects", json={"id": "new-project", "name": "New Project"})
        update = await client.patch(f"{base}/v1/projects/neil-shared", json={"name": "Renamed"})
        visibility = await client.patch(f"{base}/v1/projects/neil-shared/visibility", json={"visibility": "private"})
        hidden = await client.patch(f"{base}/v1/projects/alice-private", json={"name": "Nope"})

        assert create.status_code == 200
        assert update.status_code == 200
        assert update.json()["project"]["name"] == "Renamed"
        assert visibility.status_code == 403
        assert hidden.status_code == 404

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert [call["op"] for call in brain.calls] == [
        "project.create",
        "project.update",
        "project.visibility.set",
        "project.update",
    ]
    assert brain.calls[1]["payload"] == {"name": "Renamed", "project_id": "neil-shared"}
    assert cockpit_api_module._registry_store(cfg).get_project("neil-shared").name == "Neil Shared"  # noqa: SLF001


def test_cockpit_project_memory_routes_are_member_gated_and_attributed(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        finding = await client.post(
            f"{base}/v1/projects/neil-shared/findings",
            json={"content": "The notes repo owns project notes.", "observed_at": "2026-07-05"},
        )
        hidden = await client.post(
            f"{base}/v1/projects/alice-private/findings",
            json={"content": "No access."},
        )

        assert finding.status_code == 200
        assert finding.json()["content_hash"].startswith("sha256:")
        assert hidden.status_code == 404

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    entry = CurationOutbox(cfg.memory.curation_outbox_path).pending_entries()[0]
    assert entry.observed_id == "project:neil-shared"
    assert entry.content == "The notes repo owns project notes."
    assert entry.metadata["project_id"] == "neil-shared"
    assert entry.metadata["artifact_type"] == "finding"
    assert entry.metadata["recorded_by"] == "neil"
    assert entry.metadata["channel"] == "cockpit"
    assert entry.metadata["observed_at"] == "2026-07-05"


def test_cockpit_project_file_upload_uses_multipart_and_brain(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    brain = FakeProjectBrainClient()
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        upload = await client.post(
            f"{base}/v1/projects/neil-shared/files",
            files={"file": ("spec.md", b"# Spec", "text/markdown")},
            data={"title": "Spec", "artifact_type": "spec"},
        )

        assert upload.status_code == 200
        assert upload.json()["doc_id"] == "upload-123"

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert brain.calls[0]["op"] == "project.file.upload"
    payload = brain.calls[0]["payload"]
    assert payload["project_id"] == "neil-shared"
    assert payload["filename"] == "spec.md"
    assert base64.b64decode(payload["content_base64"]) == b"# Spec"
    assert payload["title"] == "Spec"
    assert payload["channel"] == "cockpit"


def test_cockpit_project_files_list_uses_brain_manifest_op(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    brain = FakeProjectBrainClient()
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        files = await client.get(f"{base}/v1/projects/neil-shared/files?include_retracted=true")

        assert files.status_code == 200
        assert files.json()["files"] == [{"doc_id": "upload-123"}]
        assert "result" not in files.json()

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert brain.calls[0]["op"] == "project.file.list"
    assert brain.calls[0]["payload"] == {"project_id": "neil-shared", "include_retracted": True}


def test_cockpit_forget_correct_forward_to_brain_memory_ops(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    brain = FakeProjectBrainClient()
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        forget = await client.post(
            f"{base}/v1/projects/neil-shared/memory/forget",
            json={"query": "old fact", "confirm": True, "conclusion_ids": ["c1"]},
        )
        correct = await client.post(
            f"{base}/v1/projects/neil-shared/memory/correct",
            json={"query": "wrong fact", "replacement": "right fact", "confirm": True, "conclusion_ids": ["c2"]},
        )

        assert forget.status_code == 200
        assert forget.json()["result"] == "Forgotten."
        assert correct.status_code == 200
        assert correct.json()["result"] == "Corrected."

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert [call["op"] for call in brain.calls] == ["project.memory.forget", "project.memory.correct"]
    assert brain.calls[0]["payload"]["project_id"] == "neil-shared"
    assert brain.calls[0]["payload"]["channel"] == "cockpit"
    assert brain.calls[1]["payload"]["source"] == "cockpit"


def test_cockpit_app_client_max_size_tracks_upload_limit(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    cfg.registry.max_upload_bytes = 3 * 1024 * 1024

    app = make_app(cfg)

    attachment_budget = cfg.orchestration.turn_attachment_max_count * ((cfg.orchestration.turn_attachment_max_bytes * 4) // 3 + 1024)
    assert app._client_max_size == max(4 * 1024 * 1024, attachment_budget + 1024 * 1024)  # noqa: SLF001


def test_cockpit_project_memory_returns_representation_and_conclusions(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory(cached="cached shared context", live="live shared context")
    memory.conclusions = [
        _conclusion(
            "c1",
            project_id="neil-shared",
            artifact_type="finding",
            content="Finding: the notes repo owns the cockpit notes.",
            observed_at="2026-07-04T10:00:00Z",
        ),
        _conclusion(
            "c2",
            project_id="neil-shared",
            artifact_type="decision",
            content="Decision: keep project memory behind the Jarvis API.",
            observed_at="2026-07-05T09:00:00Z",
            recorded_by="alice",
        ),
        _conclusion(
            "c3",
            project_id="neil-shared",
            artifact_type="note",
            content="Note: hidden from the findings/decisions surface.",
            observed_at="2026-07-06T09:00:00Z",
        ),
        _conclusion(
            "c4",
            project_id="other",
            artifact_type="decision",
            content="Decision: belongs to another project.",
            observed_at="2026-07-07T09:00:00Z",
        ),
    ]
    monkeypatch.setattr(cockpit_api_module, "MemoryClient", lambda _cfg: memory)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/projects/neil-shared/memory")
        body = response.json()

        assert response.status_code == 200
        assert body == {
            "api_version": "v1",
            "schema_version": 1,
            "project_id": "neil-shared",
            "peer_id": "project:neil-shared",
            "representation": "live shared context",
            "conclusions": [
                {
                    "id": "c2",
                    "content": "Decision: keep project memory behind the Jarvis API.",
                    "artifact_type": "decision",
                    "recorded_by": "alice",
                    "observed_at": "2026-07-05T09:00:00Z",
                },
                {
                    "id": "c1",
                    "content": "Finding: the notes repo owns the cockpit notes.",
                    "artifact_type": "finding",
                    "recorded_by": "neil",
                    "observed_at": "2026-07-04T10:00:00Z",
                },
            ],
        }
        assert memory.cached_reads == ["project:neil-shared"]
        assert memory.live_reads == ["project:neil-shared"]
        assert memory.conclusion_filters == [
            {
                "observed_id": "project:neil-shared",
                "level": "explicit",
                "metadata": {"project_id": "neil-shared", "artifact_type": "finding"},
            },
            {
                "observed_id": "project:neil-shared",
                "level": "explicit",
                "metadata": {"project_id": "neil-shared", "artifact_type": "decision"},
            },
        ]

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_project_memory_degrades_when_backend_is_v2_or_dead(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory(
        live_error=cockpit_api_module.UnsupportedMemoryOperation("v2 unsupported"),
        conclusion_error=RuntimeError("memory down"),
    )
    monkeypatch.setattr(cockpit_api_module, "MemoryClient", lambda _cfg: memory)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/projects/neil-shared/memory")
        body = response.json()

        assert response.status_code == 200
        assert body["project_id"] == "neil-shared"
        assert body["peer_id"] == "project:neil-shared"
        assert body["representation"] == ""
        assert body["conclusions"] == []

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_threads_open_and_list_are_membership_filtered(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway(["unused"]), tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Kickoff"})
        listed = await client.get(f"{base}/v1/projects/neil-shared/threads")
        hidden_list = await client.get(f"{base}/v1/projects/alice-private/threads")
        hidden_open = await client.post(f"{base}/v1/projects/alice-private/threads", json={})
        hidden_detail = await client.get(f"{base}/v1/projects/alice-private/threads/thread_1")
        hidden_post = await client.post(f"{base}/v1/projects/alice-private/threads/thread_1/turns", json={"text": "hi"})

        assert opened.status_code == 200
        thread = opened.json()["thread"]
        assert thread["project_id"] == "neil-shared"
        assert thread["session_id"] == orchestrator_session_id("neil-shared", thread["thread_id"])
        assert listed.status_code == 200
        assert [item["thread_id"] for item in listed.json()["threads"]] == [thread["thread_id"]]
        assert "messages" not in listed.json()["threads"][0]
        assert hidden_list.status_code == 404
        assert hidden_open.status_code == 404
        assert hidden_detail.status_code == 404
        assert hidden_post.status_code == 404

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert len(memory.sessions) == 1
    assert memory.sessions[0]["messages_at_create"] == 0
    assert memory.sessions[0]["peers"] == ["project:neil-shared", "neil", "jarvis"]
    assert memory.sessions[0]["metadata"]["kind"] == "cockpit_orchestrator"
    assert memory.messages == []


def test_cockpit_thread_detail_enriches_from_one_targeted_worker_execution_read(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    index.save(
        CockpitThread(
            thread_id="thread_active",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_active"),
            title="Active workspace conversation",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            engine="codex",
            worker_id="macbook-worker",
            workspace={"worker_id": "macbook-worker", "session_id": "conv_thread_active"},
        )
    )
    reads: list[str] = []

    def worker_get(url: str, **_kwargs: Any) -> Response:
        reads.append(url)
        assert url == "http://worker.test/sessions/conv_thread_active/execution-state"
        return Response(
            {
                "session_id": "conv_thread_active",
                "status": "waiting_input",
                "active_turn": {
                    "turn_id": "turn_active",
                    "status": "waiting_input",
                    "started_at": "2026-07-13T00:00:01Z",
                },
                "pending_requests": [
                    {
                        "request_id": "input_1",
                        "kind": "input",
                        "status": "pending",
                        "title": "Input needed",
                        "detail": "Choose a target.",
                        "created_at": "2026-07-13T00:00:02Z",
                        "questions": [
                            {
                                "id": "target",
                                "header": "Target",
                                "question": "Which target?",
                                "options": [{"label": "Tests", "description": "Run tests"}],
                                "multi_select": False,
                            }
                        ],
                    }
                ],
                "supported_controls": ["turn"],
                "supports": {"steer": False, "queue": False},
            }
        )

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        response = await client.get(f"{base}/v1/projects/neil-shared/threads/thread_active")
        assert response.status_code == 200
        return response.json()["thread"]

    thread = asyncio.run(_with_server(cfg, calls, http_get=worker_get))

    assert reads == ["http://worker.test/sessions/conv_thread_active/execution-state"]
    assert thread["execution"] == {
        "available": True,
        "status": "waiting_input",
        "active_turn": {
            "turn_id": "turn_active",
            "status": "waiting_input",
            "started_at": "2026-07-13T00:00:01Z",
        },
        "pending_requests": [
            {
                "request_id": "input_1",
                "kind": "input",
                "status": "pending",
                "title": "Input needed",
                "detail": "Choose a target.",
                "created_at": "2026-07-13T00:00:02Z",
                "questions": [
                    {
                        "id": "target",
                        "header": "Target",
                        "question": "Which target?",
                        "options": [{"label": "Tests", "description": "Run tests"}],
                        "multi_select": False,
                    }
                ],
            }
        ],
        "supported_controls": ["turn"],
        "supports": {"steer": False, "queue": False},
        "diagnostic": None,
    }


@pytest.mark.parametrize(
    "failure",
    [
        TimeoutError("timed out at /Users/neil/private with sk-abcdefghijklmnopqrstuvwxyz"),
        Response({"error": "unauthorized"}, status_code=401),
        Response({"error": "missing"}, status_code=404),
        Response({"error": "worker exploded"}, status_code=500),
        TextResponse("not json", status_code=200),
        Response({"status": "running", "pending_requests": "invalid"}),
        Response(
            {
                "session_id": "conv_thread_degraded",
                "status": "waiting_input",
                "active_turn": {"turn_id": "", "status": "waiting_input"},
                "pending_requests": [
                    {"request_id": "future_1", "kind": "future_request", "status": "pending"}
                ],
                "supported_controls": [],
                "supports": {"steer": False, "queue": False},
            }
        ),
    ],
)
def test_cockpit_thread_detail_degrades_worker_execution_failures_to_durable_detail(
    tmp_path,
    monkeypatch,
    failure,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    stored = index.save(
        CockpitThread(
            thread_id="thread_degraded",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_degraded"),
            title="Durable while worker is unavailable",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            engine="codex",
            worker_id="macbook-worker",
            workspace={"worker_id": "macbook-worker", "session_id": "conv_thread_degraded"},
        )
    )
    index.append_turn(
        stored,
        user_peer_id="neil",
        user_text="Keep this durable message.",
        assistant_peer_id="jarvis",
        assistant_text="Durable reply.",
    )

    def worker_get(_url: str, **kwargs: Any) -> Response:
        assert kwargs["timeout"] == cfg.orchestration.sse_sync_timeout_s
        if isinstance(failure, BaseException):
            raise failure
        return failure

    async def calls(base: str, client: httpx.AsyncClient) -> tuple[int, dict[str, Any]]:
        response = await client.get(f"{base}/v1/projects/neil-shared/threads/thread_degraded")
        return response.status_code, response.json()["thread"]

    status, thread = asyncio.run(_with_server(cfg, calls, http_get=worker_get))

    assert status == 200
    assert thread["title"] == "Durable while worker is unavailable"
    assert thread["messages"][0]["content"] == "Keep this durable message."
    assert thread["execution"] == {
        "available": False,
        "status": "unavailable",
        "active_turn": None,
        "pending_requests": [],
        "supported_controls": [],
        "supports": {"steer": False, "queue": False},
        "diagnostic": {
            "code": "worker_unavailable",
            "message": thread["execution"]["diagnostic"]["message"],
        },
    }
    assert "/Users/" not in json.dumps(thread["execution"])
    assert "sk-abcdefghijklmnopqrstuvwxyz" not in json.dumps(thread["execution"])


def test_cockpit_thread_execution_ignores_future_request_kinds_without_erasing_active_turn() -> None:
    execution = cockpit_api_module._public_thread_execution(  # noqa: SLF001
        {
            "session_id": "conv_future",
            "status": "running",
            "active_turn": {
                "turn_id": "turn_active",
                "status": "running",
                "started_at": "2026-07-13T00:00:00Z",
            },
            "pending_requests": [
                {"request_id": "future_1", "kind": "future_request", "status": "pending"}
            ],
            "supported_controls": ["turn"],
            "supports": {"steer": False, "queue": False},
        }
    )

    assert execution["active_turn"]["turn_id"] == "turn_active"
    assert execution["pending_requests"] == []


def test_cockpit_brain_thread_detail_exposes_addressable_local_active_turn(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    class BlockingGateway(FakeGateway):
        def __init__(self) -> None:
            super().__init__([])
            self.entered = asyncio.Event()
            self.release = asyncio.Event()

        async def stream_with_tools(self, messages, **_kwargs):  # noqa: ANN001
            self.messages.append(messages)
            self.entered.set()
            await self.release.wait()
            yield "Finished."

    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    gateway = BlockingGateway()
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=gateway,
        tts=None,
        tracer=None,
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        opened = (await client.post(f"{base}/v1/projects/neil-shared/threads", json={})).json()["thread"]
        turn = asyncio.create_task(
            client.post(
                f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}/turns",
                json={"text": "Hold this turn open."},
            )
        )
        await asyncio.wait_for(gateway.entered.wait(), timeout=2)
        detail = (
            await client.get(f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}")
        ).json()["thread"]
        gateway.release.set()
        await turn
        return detail

    thread = asyncio.run(_with_server(cfg, calls))

    assert thread["execution"]["status"] == "working"
    assert thread["execution"]["active_turn"]["turn_id"].startswith("turn_")
    assert thread["execution"]["active_turn"]["status"] == "working"
    assert thread["execution"]["active_turn"]["started_at"]
    assert thread["execution"]["pending_requests"] == []
    assert thread["execution"]["supported_controls"] == ["turn"]
    assert thread["execution"]["supports"] == {"steer": False, "queue": False}


def test_cockpit_thread_turn_streams_reply_and_writes_lane1_attribution(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory(cached="cached shared context", live="live shared context")
    gateway = FakeGateway(["The route should stream over SSE."])
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Plan"})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "What should we build first?"},
        )
        detail = await client.get(f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}")
        events = _sse_events(response.text)
        reply_events = [event for event in events if event["_event"] == "thread.reply"]
        done_events = [event for event in events if event["_event"] == "thread.turn.done"]

        assert response.status_code == 200
        assert response.headers["Content-Type"].startswith("text/event-stream")
        assert reply_events[0]["payload"]["reply"] == "The route should stream over SSE."
        assert done_events
        assert not any(event["_event"] in {"tool.call", "tool.result"} for event in events)
        assert detail.status_code == 200
        messages = detail.json()["thread"]["messages"]
        assert messages == [
            {
                "role": "user",
                "peer_id": "neil",
                "content": "What should we build first?",
                "observed_at": messages[0]["observed_at"],
            },
            {
                "role": "assistant",
                "peer_id": "jarvis",
                "content": "The route should stream over SSE.",
                "observed_at": messages[1]["observed_at"],
            },
        ]

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert [message["peer_id"] for message in memory.messages] == ["neil", "jarvis"]
    assert memory.messages[0]["content"] == "What should we build first?"
    assert memory.messages[1]["content"] == "The route should stream over SSE."
    assert memory.messages[0]["metadata"]["channel"] == "cockpit"
    assert memory.messages[0]["metadata"]["device_id"] == "local-mac"
    assert memory.messages[1]["metadata"]["channel"] == "cockpit"
    assert memory.messages[1]["metadata"]["device_id"] == "local-mac"
    assert memory.messages[0]["session_id"].startswith("project:neil-shared:orchestrator:")
    system_prompt = gateway.messages[0][0]["content"]
    assert "Project registry entry" in system_prompt
    assert "Live project representation:\nlive shared context" in system_prompt
    assert "Project-thread capability contract" in system_prompt


def test_cockpit_thread_turn_emits_deltas_before_final_reply(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    class StreamingGateway(FakeGateway):
        async def stream_with_tools(
            self,
            messages: list[dict[str, Any]],
            *,
            model: str | None = None,
            tools: list[dict[str, Any]] | None = None,
            usage_out: dict[str, Any] | None = None,
            tool_calls_out: list[dict[str, Any]] | None = None,
        ):
            self.messages.append(messages)
            self.tools.append(tools)
            for delta in self.scripted[self.calls]:
                yield delta
            self.calls += 1

    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    gateway = StreamingGateway([["First reply segment. ", "Second reply segment."]])
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Stream the response."},
        )
        events = _sse_events(response.text)
        names = [event["_event"] for event in events]
        deltas = [event["payload"]["delta"] for event in events if event["_event"] == "thread.delta"]
        legacy = [event for event in events if event["_event"] in {"thread.reply", "thread.turn.done"}]

        assert response.status_code == 200
        assert names == [
            "thread.turn.started",
            "thread.delta",
            "thread.delta",
            "thread.reply",
            "thread.turn.done",
        ]
        assert "".join(deltas) == "First reply segment. Second reply segment."
        assert legacy[0]["payload"]["reply"] == "".join(deltas)
        assert legacy[1]["payload"]["reply"] == "".join(deltas)

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_thread_turn_finishes_after_delta_client_disconnect(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    class StreamingGateway(FakeGateway):
        async def stream_with_tools(
            self,
            messages: list[dict[str, Any]],
            *,
            model: str | None = None,
            tools: list[dict[str, Any]] | None = None,
            usage_out: dict[str, Any] | None = None,
            tool_calls_out: list[dict[str, Any]] | None = None,
        ):
            self.messages.append(messages)
            self.tools.append(tools)
            for delta in self.scripted[self.calls]:
                yield delta
            self.calls += 1

    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    gateway = StreamingGateway([["First reply segment. ", "Second reply segment."]])
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)
    original_write_sse = cockpit_api_module._write_sse
    scheduled = []

    async def disconnected_delta(response, event, cursor, data):  # noqa: ANN001
        if event == "thread.delta":
            raise ConnectionResetError("client disconnected")
        await original_write_sse(response, event, cursor, data)

    monkeypatch.setattr(cockpit_api_module, "_write_sse", disconnected_delta)
    monkeypatch.setattr(cockpit_api_module, "schedule_cold_task_drain", scheduled.append)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Stream the response."},
        )
        detail = await client.get(f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}")

        assert response.status_code == 200
        assert detail.json()["thread"]["messages"][-1]["content"] == "First reply segment. Second reply segment."

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert len(scheduled) == 1


def test_cockpit_thread_turn_streams_tool_events_and_persists_detail_messages(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        caps="memory.curate",
        memory_backend="v3",
    )
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway(
        [
            _Msg(
                tool_calls=[
                    _Call(
                        "call_1",
                        "add_finding",
                        json.dumps(
                            {
                                "project": "Neil Shared",
                                "content": "Thread turns should expose tool calls.",
                                "token": "sk-abcdefghijklmnopqrstuvwxyz",
                            }
                        ),
                    )
                ]
            ),
            _Msg(content="Recorded that finding."),
        ]
    )
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Record the finding about thread tool events."},
        )
        events = _sse_events(response.text)
        tool_call = next(event for event in events if event["_event"] == "tool.call")
        tool_result = next(event for event in events if event["_event"] == "tool.result")
        done = next(event for event in events if event["_event"] == "thread.turn.done")

        assert response.status_code == 200
        assert tool_call["payload"]["type"] == "tool.call"
        assert tool_call["payload"]["session_ref"] == thread["session_id"]
        assert tool_call["payload"]["run_id"] == ""
        assert tool_call["payload"]["data"]["item"]["type"] == "tool_use"
        assert tool_call["payload"]["data"]["item"]["name"] == "add_finding"
        assert tool_call["payload"]["data"]["item"]["input"] == {
            "project": "Neil Shared",
            "content": "Thread turns should expose tool calls.",
        }
        assert "arguments" not in tool_call["payload"]["data"]
        assert "token" not in json.dumps(tool_call["payload"]["data"])
        assert tool_result["payload"]["type"] == "tool.result"
        assert tool_result["payload"]["data"]["item"]["type"] == "tool_result"
        assert tool_result["payload"]["data"]["item"]["name"] == "add_finding"
        assert "queued finding" in tool_result["payload"]["data"]["item"]["content"]
        detail_events = [
            message["event"]
            for message in done["payload"]["thread"]["messages"]
            if message.get("role") == "event"
        ]
        assert [event["type"] for event in detail_events] == ["tool.call", "tool.result"]
        assert detail_events[0]["data"]["item"]["name"] == "add_finding"

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    outbox_text = Path(cfg.memory.curation_outbox_path).read_text()
    assert "Thread turns should expose tool calls." in outbox_text
    assert [message["peer_id"] for message in memory.messages] == ["neil", "jarvis"]


def test_cockpit_thread_turn_declines_unavailable_code_review_instead_of_faking_progress(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway(["I'll start a code review of the runtime repo now. It's underway."])
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Please do a code review of the runtime repo."},
        )
        events = _sse_events(response.text)
        reply = next(event for event in events if event["_event"] == "thread.reply")["payload"]["reply"]

        assert response.status_code == 200
        assert "can't do that from this project conversation" in reply
        assert "/v1/work/start" in reply
        assert "underway" not in reply.lower()
        assert not any(event["_event"] in {"tool.call", "tool.result"} for event in events)

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert memory.messages[1]["content"].startswith("I can't do that from this project conversation")


@pytest.mark.parametrize(
    ("user_text", "model_reply"),
    [
        (
            "Should I run the tests before merging?",
            "Yes. Run the touched unit tests and ruff before merging.",
        ),
        (
            "how do I run the tests locally?",
            "Use uv run pytest for tests and uv run ruff check src/ for lint.",
        ),
        (
            "Can you review the code style conventions we agreed on?",
            "The convention is to keep diffs tight, use existing patterns, and verify with focused tests.",
        ),
    ],
)
def test_cockpit_thread_turn_preserves_advisory_replies_without_tool_calls(
    tmp_path,
    monkeypatch,
    user_text: str,
    model_reply: str,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway([model_reply])
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": user_text},
        )
        events = _sse_events(response.text)
        reply = next(event for event in events if event["_event"] == "thread.reply")["payload"]["reply"]

        assert response.status_code == 200
        assert reply == model_reply
        assert not any(event["_event"] in {"tool.call", "tool.result"} for event in events)

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert memory.messages[1]["content"] == model_reply


def test_cockpit_thread_turn_guards_fake_review_even_when_memory_tool_ran(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        caps="memory.curate",
        memory_backend="v3",
    )
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway(
        [
            _Msg(
                tool_calls=[
                    _Call(
                        "call_1",
                        "add_finding",
                        json.dumps({"project": "Neil Shared", "content": "Record the review request."}),
                    )
                ]
            ),
            _Msg(content="I've started a code review of the runtime repo. It's underway."),
        ]
    )
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Please review the runtime repo and remember that I asked."},
        )
        events = _sse_events(response.text)
        reply = next(event for event in events if event["_event"] == "thread.reply")["payload"]["reply"]

        assert response.status_code == 200
        assert "can't do that from this project conversation" in reply
        assert "underway" not in reply.lower()
        assert [event["_event"] for event in events if event["_event"] in {"tool.call", "tool.result"}] == [
            "tool.call",
            "tool.result",
        ]

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert memory.messages[1]["content"].startswith("I can't do that from this project conversation")


def test_cockpit_thread_turn_does_not_guard_truthful_completed_status_reply(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    # Regression: the guard used to also match backward-looking status words
    # ("done", "finished", "completed") and bare third-person mentions with no
    # first-person subject, so a truthful report of a *completed* child run
    # got clobbered into the canned workspace-offer reply.
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway(["The code review is done — run_abc finished."])
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Is the code review done yet?"},
        )
        events = _sse_events(response.text)
        reply = next(event for event in events if event["_event"] == "thread.reply")["payload"]["reply"]

        assert response.status_code == 200
        assert reply == "The code review is done — run_abc finished."

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_thread_turn_does_not_guard_advisory_answer(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    # Regression: "running the tests locally" (no first-person subject) used to
    # match the guard's bare status branch and clobber an advisory answer that
    # never claimed Jarvis itself was doing untooled work.
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway(["Run `pytest tests/unit` from the repo root for running the tests locally."])
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "How do I run the tests locally?"},
        )
        events = _sse_events(response.text)
        reply = next(event for event in events if event["_event"] == "thread.reply")["payload"]["reply"]

        assert response.status_code == 200
        assert reply == "Run `pytest tests/unit` from the repo root for running the tests locally."

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_thread_turn_labels_unescalated_chat_as_planning_only(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory(cached="cached shared context", live="live shared context")
    gateway = FakeGateway(["We need to escalate before I can inspect files."])
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Plan"})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Inspect the notes repo files and plan the tests."},
        )
        events = _sse_events(response.text)

        assert response.status_code == 200
        assert events[-1]["_event"] == "thread.turn.done"
        assert "workspace" not in events[-1]["payload"]["thread"]

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    system_prompt = gateway.messages[0][0]["content"]
    assert "Conversation workspace: planning-only" in system_prompt
    assert "do not claim to inspect repository files" in system_prompt


def test_cockpit_thread_escalates_to_workspace_without_losing_thread_history(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway(["Planning reply."])
    state: dict[str, Any] = {"posts": [], "worktrees": []}

    def worker_get(url: str, **_kwargs: Any) -> Response:
        if url.endswith("/health"):
            return Response(
                {
                    "ok": True,
                    "agent": "codex",
                    "default_engine": "codex",
                    "supported_engines": ["codex", "claude"],
                    "engine_supports": {"codex": {"streaming": True}},
                    "repositories": [{"repo": "notes", "status": "ready"}],
                }
            )
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response({"sessions": []})
        if "/sessions/" in url:
            return Response({"session_id": "conv_thread", "provider": "codex", "engine": "codex", "status": "created"})
        return Response({})

    def worker_post(url: str, json: dict[str, Any], **_kwargs: Any) -> Response:  # noqa: A002
        state["posts"].append({"url": url, "json": json})
        workspace = {
            "workspace_id": "neil-shared-thread",
            "conversation_id": "neil-shared-thread",
            "root": str(tmp_path / "worker" / "conversations" / "neil-shared-thread"),
            "root_label": "neil-shared-thread",
            "cwd_label": "neil-shared-thread",
            "status": "ready",
            "provision_phase": "running",
            "worktrees": list(state["worktrees"]),
            "created_at": "2026-07-07T00:00:00+00:00",
            "updated_at": "2026-07-07T00:00:00+00:00",
        }
        if url.endswith("/conversation-workspaces"):
            return Response({"ok": True, "workspace": workspace})
        if url.endswith("/worktrees"):
            state["worktrees"].append(
                {
                    "name": json["name"],
                    "repo": json["repo"],
                    "path": str(tmp_path / "worker" / "conversations" / "neil-shared-thread" / "repos" / json["name"]),
                    "path_label": json["name"],
                    "branch": "jarvis/neil-shared-thread-notes",
                    "base_ref": "",
                    "status": "ready",
                    "provision_phase": "running",
                }
            )
            workspace["worktrees"] = list(state["worktrees"])
            return Response({"ok": True, "workspace": workspace})
        if url.endswith("/sessions"):
            return Response({"ok": True, "session": {**json, "status": "created"}, "event": {"event_id": "ev_create"}})
        if url.endswith("/turns"):
            return Response({"ok": True, "session": {"session_id": "conv_thread", "status": "running"}, "events": []})
        return Response({"ok": False, "error": "unexpected worker post"}, status_code=400)

    connector = CockpitConnector(
        cfg,
        memory=memory,
        gateway=gateway,
        tts=None,
        tracer=None,
        worker_get=worker_get,
        worker_post=worker_post,
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Plan"})
        thread = opened.json()["thread"]
        first = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Plan the rollout."},
        )
        second = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Inspect the notes repo.", "workspace": {"repos": ["notes"]}},
        )
        listed = await client.get(f"{base}/v1/projects/neil-shared/threads")

        assert first.status_code == 200
        assert second.status_code == 200
        done = [event for event in _sse_events(second.text) if event["_event"] == "thread.turn.done"][-1]
        reply = [event for event in _sse_events(second.text) if event["_event"] == "thread.reply"][-1]
        assert reply["payload"]["reply"] == "Workspace turn is running."
        workspace = done["payload"]["thread"]["workspace"]
        assert workspace["worker_id"] == "macbook-worker"
        assert workspace["session_id"].startswith("conv_thread")
        assert workspace["worktrees"][0]["name"] == "notes"
        assert "root" not in workspace
        assert listed.json()["threads"][0]["workspace"]["cwd_label"] == "neil-shared-thread"

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert [message["content"] for message in memory.messages] == [
        "Plan the rollout.",
        "Planning reply.",
        "Inspect the notes repo.",
    ]
    stored = connector.index.list("neil-shared", include_archived=True)[0]
    with_messages = connector.index.get_with_messages("neil-shared", stored.thread_id)
    assert with_messages is not None
    assert len(with_messages.messages) == 4
    assert with_messages.messages[-1]["content"] == "[workspace turn pending]"
    turn_posts = [call for call in state["posts"] if call["url"].endswith("/turns")]
    assert turn_posts
    assert "Honcho session: project:neil-shared:orchestrator:" in turn_posts[0]["json"]["prompt"]


def test_cockpit_thread_workspace_turn_idempotency_key_differs_for_repeat_text(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    # Regression: the worker idempotency key used to be derived from
    # len(thread.messages), which is always 0 for the `thread` passed into
    # _workspace_turn (it comes back from an index round-trip that strips
    # messages). Sending the same text twice produced the same key, so the
    # worker's reserve_turn treated the second send as an idempotent replay
    # and never ran a new provider turn.
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway(["unused"])
    state: dict[str, Any] = {"posts": [], "worktrees": []}

    def worker_get(url: str, **_kwargs: Any) -> Response:
        if url.endswith("/health"):
            return Response(
                {
                    "ok": True,
                    "agent": "codex",
                    "default_engine": "codex",
                    "supported_engines": ["codex"],
                    "engine_supports": {"codex": {"streaming": True}},
                    "repositories": [{"repo": "notes", "status": "ready"}],
                }
            )
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response({"sessions": []})
        if "/sessions/" in url:
            return Response({"session_id": "conv_thread", "provider": "codex", "engine": "codex", "status": "created"})
        return Response({})

    def worker_post(url: str, json: dict[str, Any], **_kwargs: Any) -> Response:  # noqa: A002
        state["posts"].append({"url": url, "json": json})
        workspace = {
            "workspace_id": "neil-shared-thread",
            "conversation_id": "neil-shared-thread",
            "root": str(tmp_path / "worker" / "conversations" / "neil-shared-thread"),
            "root_label": "neil-shared-thread",
            "cwd_label": "neil-shared-thread",
            "status": "ready",
            "provision_phase": "running",
            "worktrees": list(state["worktrees"]),
            "created_at": "2026-07-07T00:00:00+00:00",
            "updated_at": "2026-07-07T00:00:00+00:00",
        }
        if url.endswith("/conversation-workspaces"):
            return Response({"ok": True, "workspace": workspace})
        if url.endswith("/worktrees"):
            state["worktrees"].append(
                {
                    "name": json["name"],
                    "repo": json["repo"],
                    "path": str(tmp_path / "worker" / "conversations" / "neil-shared-thread" / "repos" / json["name"]),
                    "path_label": json["name"],
                    "branch": "jarvis/neil-shared-thread-notes",
                    "base_ref": "",
                    "status": "ready",
                    "provision_phase": "running",
                }
            )
            workspace["worktrees"] = list(state["worktrees"])
            return Response({"ok": True, "workspace": workspace})
        if url.endswith("/sessions"):
            return Response({"ok": True, "session": {**json, "status": "created"}, "event": {"event_id": "ev_create"}})
        if url.endswith("/turns"):
            return Response({"ok": True, "session": {"session_id": "conv_thread", "status": "running"}, "events": []})
        return Response({"ok": False, "error": "unexpected worker post"}, status_code=400)

    connector = CockpitConnector(
        cfg,
        memory=memory,
        gateway=gateway,
        tts=None,
        tracer=None,
        worker_get=worker_get,
        worker_post=worker_post,
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Plan"})
        thread = opened.json()["thread"]
        first = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Same message.", "workspace": {"repos": ["notes"]}},
        )
        second = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Same message."},
        )

        assert first.status_code == 200
        assert second.status_code == 200

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    turn_posts = [call for call in state["posts"] if call["url"].endswith("/turns")]
    assert len(turn_posts) == 2
    keys = [call["json"]["idempotency_key"] for call in turn_posts]
    assert keys[0] != keys[1]
    # The second turn hit an already-ready workspace with no new repo request,
    # so it must not re-provision (no second conversation-workspaces/worktrees
    # round-trip).
    workspace_posts = [call for call in state["posts"] if call["url"].endswith("/conversation-workspaces")]
    worktree_posts = [call for call in state["posts"] if call["url"].endswith("/worktrees")]
    assert len(workspace_posts) == 1
    assert len(worktree_posts) == 1


def test_cockpit_thread_workspace_failure_marks_workspace_failed(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()

    def worker_get(url: str, **_kwargs: Any) -> Response:
        if url.endswith("/health"):
            return Response(
                {
                    "ok": True,
                    "agent": "codex",
                    "default_engine": "codex",
                    "supported_engines": ["codex"],
                    "engine_supports": {"codex": {"streaming": True}},
                    "repositories": [{"repo": "notes", "status": "ready"}],
                }
            )
        if url.endswith("/sessions"):
            return Response({"sessions": []})
        return Response({})

    def worker_post(url: str, json: dict[str, Any], **_kwargs: Any) -> Response:  # noqa: A002
        if url.endswith("/conversation-workspaces"):
            return Response(
                {
                    "ok": True,
                    "workspace": {
                        "workspace_id": "neil-shared-thread",
                        "conversation_id": "neil-shared-thread",
                        "root": str(tmp_path / "worker" / "conversations" / "neil-shared-thread"),
                        "root_label": "neil-shared-thread",
                        "cwd_label": "neil-shared-thread",
                        "status": "ready",
                        "provision_phase": "running",
                        "worktrees": [],
                    },
                }
            )
        if url.endswith("/worktrees"):
            return Response({"ok": False, "error": "clone failed"}, status_code=400)
        return Response({"ok": False, "error": "unexpected worker post"}, status_code=400)

    connector = CockpitConnector(
        cfg,
        memory=memory,
        gateway=FakeGateway(["unused"]),
        tts=None,
        tracer=None,
        worker_get=worker_get,
        worker_post=worker_post,
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Plan"})
        thread = opened.json()["thread"]
        failed = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Inspect the notes repo.", "workspace": {"repos": ["notes"]}},
        )
        detail = await client.get(f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}")
        return {"events": _sse_events(failed.text), "detail": detail.json()}

    import asyncio

    result = asyncio.run(_with_server(cfg, calls))

    errors = [event for event in result["events"] if event["_event"] == "thread.turn.error"]
    assert errors
    thread = result["detail"]["thread"]
    assert thread["lifecycle"] == "open"
    assert thread["operational_state"] == "degraded"
    assert thread["status"] == "failed"
    assert thread["diagnostic_reason"] == "engine_error"
    assert thread["ended_reason"] == "engine_error"
    assert thread["workspace"]["status"] == "failed"
    assert thread["workspace"]["provision_phase"] == "failed"


def test_cockpit_thread_turn_records_decision_only_through_lane2_tool(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        caps="memory.curate",
        memory_backend="v3",
    )
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway(
        [
            _Msg(
                tool_calls=[
                    _Call(
                        "call_1",
                        "record_decision",
                        json.dumps({"project": "Neil Shared", "content": "Use SSE for thread replies."}),
                    )
                ]
            ),
            _Msg(content="Queued that decision."),
        ]
    )
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Record the SSE decision."},
        )
        events = _sse_events(response.text)

        assert response.status_code == 200
        assert any(event["_event"] == "thread.reply" for event in events)

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert len(memory.created_conclusions) == 1
    conclusion = memory.created_conclusions[0]
    assert conclusion["observed_id"] == "project:neil-shared"
    assert conclusion["content"] == "Use SSE for thread replies."
    assert conclusion["metadata"]["artifact_type"] == "decision"
    assert conclusion["metadata"]["project_id"] == "neil-shared"
    assert conclusion["metadata"]["channel"] == "cockpit"
    assert [message["peer_id"] for message in memory.messages] == ["neil", "jarvis"]


def test_cockpit_thread_archive_hides_and_unarchive_restores_listing(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway(["unused"]), tts=None, tracer=None)
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_archive",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_archive"),
            title="Archive me",
            created_at="2026-07-05T09:00:00+00:00",
            updated_at="2026-07-05T09:00:00+00:00",
            created_by="neil",
        )
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        archived = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}/archive",
            json={"reason": "  done for now  ", "idempotency_key": "thread_archive_1"},
        )
        replay = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}/archive",
            json={"reason": "  done for now  ", "idempotency_key": "thread_archive_1"},
        )
        default = await client.get(f"{base}/v1/projects/neil-shared/threads")
        included = await client.get(f"{base}/v1/projects/neil-shared/threads?include_archived=true")
        invalid_include = await client.get(f"{base}/v1/projects/neil-shared/threads?include_archived=junk")
        unarchived = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}/unarchive",
            json={"idempotency_key": "thread_unarchive_1"},
        )
        restored = await client.get(f"{base}/v1/projects/neil-shared/threads")
        activity = await client.get(f"{base}/v1/projects/neil-shared/activity")

        assert archived.status_code == 200
        archived_thread = archived.json()["thread"]
        assert archived_thread["archived_at"]
        assert archived_thread["archived_by"] == "neil"
        assert archived_thread["archive_reason"] == "done for now"
        assert replay.status_code == 200
        assert replay.json()["thread"] == archived_thread
        assert replay.json()["idempotent"] is True
        assert default.json()["threads"] == []
        assert [item["thread_id"] for item in included.json()["threads"]] == [thread.thread_id]
        assert invalid_include.status_code == 400
        assert invalid_include.json()["error"]["code"] == "validation_failed"
        assert unarchived.status_code == 200
        assert unarchived.json()["thread"]["archived_at"] == ""
        assert unarchived.json()["thread"]["archived_by"] == ""
        assert unarchived.json()["thread"]["archive_reason"] == ""
        assert [item["thread_id"] for item in restored.json()["threads"]] == [thread.thread_id]
        activity_types = [item["type"] for item in activity.json()["activity"]]
        assert "thread.archived" in activity_types
        assert "thread.unarchived" in activity_types
        # The idempotent replay must not have re-emitted the archive event.
        assert activity_types.count("thread.archived") == 1

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_thread_delete_removes_index_and_memory_session_idempotently(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway(["unused"]), tts=None, tracer=None)
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_delete",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_delete"),
            title="Delete me",
            created_at="2026-07-05T09:00:00+00:00",
            updated_at="2026-07-05T09:00:00+00:00",
            created_by="neil",
        )
    )
    # Seed the transcript through the real storage path (messages live in the
    # per-thread transcript file, not inline on the index).
    thread = connector.index.append_turn(
        thread,
        user_peer_id="neil",
        user_text="hi",
        assistant_peer_id="jarvis",
        assistant_text="hello",
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)
    monkeypatch.setattr(cockpit_api_module, "MemoryClient", lambda _cfg: memory)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = await client.delete(f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}")
        second = await client.delete(f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}")
        listed = await client.get(f"{base}/v1/projects/neil-shared/threads?include_archived=true")

        assert first.status_code == 200
        assert first.json()["deleted"] is True
        assert first.json()["reclamation"]["records"] == 1
        assert first.json()["reclamation"]["events"] == 2
        assert first.json()["reclamation"]["memory_sessions"] == 1
        assert second.status_code == 200
        assert second.json()["deleted"] is False
        assert second.json()["reclamation"]["memory_sessions"] == 0
        assert listed.json()["threads"] == []

    import asyncio

    asyncio.run(_with_server(cfg, calls))
    assert memory.deleted_sessions == [thread.session_id]
    assert not (connector.index.transcripts_dir / "neil-shared" / "thread_delete.json").exists()


def test_cockpit_thread_delete_promotes_children_to_root(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway(["unused"]), tts=None, tracer=None)
    parent = connector.index.save(
        CockpitThread(
            thread_id="thread_parent_del",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_parent_del"),
            title="Parent",
            created_at="2026-07-05T09:00:00+00:00",
            updated_at="2026-07-05T09:00:00+00:00",
            created_by="neil",
        )
    )
    child_thread = connector.index.save(
        CockpitThread(
            thread_id="thread_child_del",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_child_del"),
            title="Child",
            created_at="2026-07-05T09:00:00+00:00",
            updated_at="2026-07-05T09:00:00+00:00",
            created_by="neil",
            parent_chat_id=parent.thread_id,
        )
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)
    monkeypatch.setattr(cockpit_api_module, "MemoryClient", lambda _cfg: memory)
    store = OrchestrationStore(cfg.orchestration.workspace, thread_children_promoter=connector.index.promote_children)
    child_run = store.create_run("Child work", parent_chat_id=parent.thread_id)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.delete(f"{base}/v1/projects/neil-shared/threads/{parent.thread_id}")
        assert response.status_code == 200
        assert response.json()["deleted"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls))
    promoted_thread = connector.index.get("neil-shared", child_thread.thread_id)
    assert promoted_thread is not None
    assert promoted_thread.parent_chat_id == ""
    reloaded_run = OrchestrationStore(cfg.orchestration.workspace).get(child_run.run_id)
    assert reloaded_run is not None
    assert reloaded_run.parent_chat_id is None


def test_cockpit_thread_delete_treats_missing_v3_memory_session_as_reclaimed(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil", memory_backend="v3")
    _seed_project_registry(cfg)
    request = httpx.Request("DELETE", "http://memory/v3/workspaces/ws/sessions/missing")
    memory = FakeProjectMemory()
    memory.delete_session_error = httpx.HTTPStatusError(
        "not found",
        request=request,
        response=httpx.Response(404, request=request),
    )
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway(["unused"]), tts=None, tracer=None)
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_missing_memory",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_missing_memory"),
            title="Delete missing memory",
            created_at="2026-07-05T09:00:00+00:00",
            updated_at="2026-07-05T09:00:00+00:00",
            created_by="neil",
            messages=(),
        )
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)
    monkeypatch.setattr(cockpit_api_module, "MemoryClient", lambda _cfg: memory)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.delete(f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}")
        listed = await client.get(f"{base}/v1/projects/neil-shared/threads?include_archived=true")

        assert response.status_code == 200
        assert response.json()["deleted"] is True
        assert response.json()["reclamation"]["memory_sessions"] == 0
        assert response.json()["reclamation"]["notes"] == ["memory session already absent"]
        assert listed.json()["threads"] == []

    import asyncio

    asyncio.run(_with_server(cfg, calls))
    assert memory.deleted_sessions == []


def test_cockpit_thread_archive_non_member_gets_404(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway(["unused"]), tts=None, tracer=None)
    connector.index.save(
        CockpitThread(
            thread_id="thread_private",
            project_id="alice-private",
            session_id=orchestrator_session_id("alice-private", "thread_private"),
            title="Private",
            created_at="2026-07-05T09:00:00+00:00",
            updated_at="2026-07-05T09:00:00+00:00",
            created_by="alice",
        )
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        archived = await client.post(
            f"{base}/v1/projects/alice-private/threads/thread_private/archive",
            json={"idempotency_key": "thread_archive_private"},
        )
        unarchived = await client.post(
            f"{base}/v1/projects/alice-private/threads/thread_private/unarchive",
            json={"idempotency_key": "thread_unarchive_private"},
        )

        assert archived.status_code == 404
        assert archived.json()["error"]["code"] == "not_found"
        assert unarchived.status_code == 404
        assert unarchived.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_thread_turn_on_archived_thread_returns_409(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway(["reply"]), tts=None, tracer=None)
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_archived",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_archived"),
            title="Archived",
            created_at="2026-07-05T09:00:00+00:00",
            updated_at="2026-07-05T09:00:00+00:00",
            created_by="neil",
            archived_at="2026-07-05T10:00:00+00:00",
            archived_by="neil",
            archive_reason="done",
        )
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}/turns",
            json={"text": "continue"},
        )

        assert response.status_code == 409
        assert response.json()["error"]["code"] == "thread_archived"
        assert response.json()["error"]["recoverable"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_thread_backend_gaps_degrade_without_500(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    memory.create_session_error = cockpit_api_module.UnsupportedMemoryOperation("v2 unsupported")
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway(["reply"]), tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        open_failed = await client.post(f"{base}/v1/projects/neil-shared/threads", json={})
        assert open_failed.status_code == 503
        assert open_failed.json()["error"]["code"] == "memory_unavailable"

        thread = connector.index.save(
            CockpitThread(
                thread_id="thread_existing",
                project_id="neil-shared",
                session_id=orchestrator_session_id("neil-shared", "thread_existing"),
                title="Existing",
                created_at="2026-07-05T09:00:00+00:00",
                updated_at="2026-07-05T09:00:00+00:00",
                created_by="neil",
            )
        )
        memory.create_session_error = None
        memory.create_messages_error = cockpit_api_module.UnsupportedMemoryOperation("v2 unsupported")
        # A memory failure during the turn's Lane 1 persist must not break the
        # turn (AGENTS.md: "tracing/memory must never break a turn") — the
        # gateway reply already happened, so the turn still completes.
        turn_ok = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}/turns",
            json={"text": "hi"},
        )
        events = _sse_events(turn_ok.text)

        assert turn_ok.status_code == 200
        assert events[-1]["_event"] == "thread.turn.done"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_project_activity_records_member_writes_and_paginates(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    brain = FakeProjectBrainClient()
    memory = FakeProjectMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway(["unused"]), tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        update = await client.patch(
            f"{base}/v1/projects/neil-shared",
            json={"name": "Spec at /Users/example/private and https://example.test/private"},
        )
        finding = await client.post(f"{base}/v1/projects/neil-shared/findings", json={"content": "Finding one."})
        decision = await client.post(f"{base}/v1/projects/neil-shared/decisions", json={"content": "Decision one."})
        forget = await client.post(f"{base}/v1/projects/neil-shared/memory/forget", json={"query": "old", "confirm": True})
        correct = await client.post(f"{base}/v1/projects/neil-shared/memory/correct", json={"query": "bad", "replacement": "good", "confirm": True})
        upload = await client.post(
            f"{base}/v1/projects/neil-shared/files",
            files={"file": ("spec.md", b"# Spec", "text/markdown")},
        )
        retract = await client.request(
            "DELETE",
            f"{base}/v1/projects/neil-shared/files/upload-123",
            json={},
        )
        thread = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Kickoff"})
        hidden = await client.get(f"{base}/v1/projects/alice-private/activity")
        first_page = (await client.get(f"{base}/v1/projects/neil-shared/activity", params={"limit": 2})).json()
        second_page = (await client.get(f"{base}/v1/projects/neil-shared/activity", params={"cursor": first_page["next_cursor"], "limit": 10})).json()
        filtered = (await client.get(f"{base}/v1/projects/neil-shared/activity", params={"type": "file.uploaded"})).json()
        stale = await client.get(f"{base}/v1/projects/neil-shared/activity", params={"cursor": "missing_cursor"})

        assert all(response.status_code == 200 for response in [update, finding, decision, forget, correct, upload, retract, thread])
        assert hidden.status_code == 404
        assert stale.status_code == 400
        assert stale.json()["error"]["code"] == "stale_cursor"
        assert [item["type"] for item in first_page["activity"]] == ["thread.opened", "file.retracted"]
        assert first_page["next_cursor"]
        assert {item["type"] for item in second_page["activity"]} == {
            "file.uploaded",
            "memory.corrected",
            "memory.forgotten",
            "decision.recorded",
            "finding.recorded",
            "project.updated",
        }
        assert [item["type"] for item in filtered["activity"]] == ["file.uploaded"]
        all_activity = first_page["activity"] + second_page["activity"]
        assert all(item["actor"]["identity"] == "neil" for item in all_activity)
        text = json.dumps(all_activity)
        assert "/Users/example/private" not in text
        assert "https://example.test/private" not in text
        assert "<local-path>" in text

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    activity_file = Path(cfg.orchestration.workspace) / "project-activity" / "neil-shared.jsonl"
    assert activity_file.exists()
    assert len(activity_file.read_text().splitlines()) == 8


def test_cockpit_project_activity_records_owner_writes(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="alice")
    _seed_project_registry(cfg)
    brain = FakeProjectBrainClient()
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        visibility = await client.patch(f"{base}/v1/projects/neil-shared/visibility", json={"visibility": "private"})
        add = await client.post(f"{base}/v1/projects/neil-shared/members", json={"member": "jules"})
        remove = await client.request("DELETE", f"{base}/v1/projects/neil-shared/members/neil", json={})
        archive = await client.post(f"{base}/v1/projects/neil-shared/archive", json={})
        unarchive = await client.post(f"{base}/v1/projects/neil-shared/unarchive", json={})
        delete = await client.request("DELETE", f"{base}/v1/projects/neil-shared", json={})
        activity = (await client.get(f"{base}/v1/projects/neil-shared/activity")).json()["activity"]

        assert all(response.status_code == 200 for response in [visibility, add, remove, archive, unarchive, delete])
        assert [item["type"] for item in activity] == [
            "project.deleted",
            "project.unarchived",
            "project.archived",
            "project.members_changed",
            "project.members_changed",
            "project.visibility_changed",
        ]

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_project_delete_blocks_when_threads_exist(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="alice")
    _seed_project_registry(cfg)
    brain = FakeProjectBrainClient()
    memory = FakeProjectMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway(["unused"]), tts=None, tracer=None)
    connector.index.save(
        CockpitThread(
            thread_id="thread_child",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_child"),
            title="Child work",
            created_at="2026-07-05T09:00:00+00:00",
            updated_at="2026-07-05T09:00:00+00:00",
            created_by="alice",
        )
    )
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.request("DELETE", f"{base}/v1/projects/neil-shared", json={})

        assert response.status_code == 409
        assert response.json()["error"]["code"] == "project_not_empty"

    import asyncio

    asyncio.run(_with_server(cfg, calls))
    assert [call["op"] for call in brain.calls if call["op"] == "project.delete"] == []


def test_cockpit_project_activity_remains_readable_after_delete(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="alice")
    _seed_project_registry(cfg)

    class DeletingProjectBrain(FakeProjectBrainClient):
        async def execute(self, requester: RequestContext, op: str, payload: dict[str, Any]) -> dict[str, Any]:
            result = await super().execute(requester, op, payload)
            if op == "project.delete":
                registry_path = Path(cfg.registry.path)
                registry = json.loads(registry_path.read_text())
                registry["projects"] = [project for project in registry["projects"] if project["id"] != payload["project_id"]]
                registry_path.write_text(json.dumps(registry))
            return result

    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: DeletingProjectBrain())

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        delete = await client.request("DELETE", f"{base}/v1/projects/neil-shared", json={})
        detail = await client.get(f"{base}/v1/projects/neil-shared")
        activity = await client.get(f"{base}/v1/projects/neil-shared/activity")

        assert delete.status_code == 200
        assert detail.status_code == 404
        assert activity.status_code == 200
        assert activity.json()["activity"][0]["type"] == "project.deleted"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_project_activity_append_failure_does_not_fail_write(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    brain = FakeProjectBrainClient()
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)

    def fail_append(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("jarvis.orchestration.activity.ProjectActivityLog.append", fail_append)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.patch(f"{base}/v1/projects/neil-shared", json={"name": "Still succeeds"})

        assert response.status_code == 200
        assert response.json()["project"]["name"] == "Still succeeds"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_project_idempotency_replay_conflict_and_no_key(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="alice")
    _seed_project_registry(cfg)
    brain = FakeProjectBrainClient()
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = await client.post(f"{base}/v1/projects", json={"id": "created-one", "name": "Created", "idempotency_key": "create-1"})
        replay = await client.post(f"{base}/v1/projects", json={"id": "created-one", "name": "Created", "idempotency_key": "create-1"})
        conflict = await client.post(f"{base}/v1/projects", json={"id": "created-one", "name": "Changed", "idempotency_key": "create-1"})
        archive = await client.post(f"{base}/v1/projects/neil-shared/archive", json={"idempotency_key": "archive-1"})
        archive_replay = await client.post(f"{base}/v1/projects/neil-shared/archive", json={"idempotency_key": "archive-1"})
        archive_conflict = await client.post(f"{base}/v1/projects/neil-shared/archive", json={"idempotency_key": "archive-1", "reason": "different body"})
        member = await client.post(f"{base}/v1/projects/neil-shared/members", json={"member": "jules", "idempotency_key": "member-1"})
        registry_path = Path(cfg.registry.path)
        registry = json.loads(registry_path.read_text())
        for project in registry["projects"]:
            if project["id"] == "neil-shared":
                project["members"].append("riley")
        registry_path.write_text(json.dumps(registry))
        member_replay = await client.post(f"{base}/v1/projects/neil-shared/members", json={"member": "jules", "idempotency_key": "member-1"})
        no_key_a = await client.post(f"{base}/v1/projects", json={"id": "no-key", "name": "No Key"})
        no_key_b = await client.post(f"{base}/v1/projects", json={"id": "no-key", "name": "No Key"})

        assert first.status_code == 200
        assert replay.status_code == 200
        assert replay.json()["idempotent"] is True
        assert replay.json()["project"] == first.json()["project"]
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "idempotency_conflict"
        assert archive.status_code == 200
        assert archive_replay.json()["idempotent"] is True
        assert archive_conflict.status_code == 409
        assert archive_conflict.json()["error"]["code"] == "idempotency_conflict"
        assert member.status_code == 200
        assert member_replay.status_code == 200
        assert member_replay.json()["idempotent"] is True
        assert no_key_a.status_code == 200
        assert no_key_b.status_code == 200

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert [call["op"] for call in brain.calls].count("project.create") == 3
    assert [call["op"] for call in brain.calls].count("project.archive") == 1
    assert [call["op"] for call in brain.calls].count("project.members.set") == 1
    activity_file = Path(cfg.orchestration.workspace) / "project-activity" / "created-one.jsonl"
    assert json.loads(activity_file.read_text().splitlines()[0])["type"] == "project.created"


def test_cockpit_project_idempotency_is_principal_scoped(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    _seed_project_registry(cfg)
    _seed_user_profiles(cfg, "alice", "neil")
    brain = FakeProjectBrainClient()
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        alice = {"Authorization": f"Bearer {fixture['sign'](subject='alice', jarvis_user='alice', scope='jarvis:read')}"}
        neil = {"Authorization": f"Bearer {fixture['sign'](subject='neil', jarvis_user='neil', scope='jarvis:read')}"}
        body = {"idempotency_key": "delete-private"}

        first = await client.request("DELETE", f"{base}/v1/projects/alice-private", headers=alice, json=body)
        replay = await client.request("DELETE", f"{base}/v1/projects/alice-private", headers=alice, json=body)
        cross_principal = await client.request("DELETE", f"{base}/v1/projects/alice-private", headers=neil, json=body)

        assert first.status_code == 200
        assert replay.status_code == 200
        assert replay.json()["idempotent"] is True
        assert cross_principal.status_code == 404
        assert cross_principal.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))

    assert [call["op"] for call in brain.calls].count("project.delete") == 2


def test_cockpit_run_archive_idempotency_is_principal_scoped(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        caps="orchestration.runs.write",
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    _seed_user_profiles(cfg, "alice", "neil")
    _store, run_id = _seed_run(cfg)

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if "jwks" in url:
            return jwks_get(url, **kwargs)
        return _fake_get(run_id)(url, **kwargs)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        alice = {"Authorization": f"Bearer {fixture['sign'](subject='alice', jarvis_user='alice', scope='jarvis:read')}"}
        neil = {"Authorization": f"Bearer {fixture['sign'](subject='neil', jarvis_user='neil', scope='jarvis:read')}"}
        body = {"idempotency_key": "archive-shared"}

        first = await client.post(f"{base}/v1/runs/{run_id}/archive", headers=alice, json=body)
        replay = await client.post(f"{base}/v1/runs/{run_id}/archive", headers=alice, json=body)
        cross_principal = await client.post(f"{base}/v1/runs/{run_id}/archive", headers=neil, json=body)

        assert first.status_code == 200
        assert replay.status_code == 200
        assert replay.json()["idempotent"] is True
        # A different principal reusing the same key must execute the action,
        # not be served the first principal's cached result.
        assert cross_principal.status_code == 200
        assert "idempotent" not in cross_principal.json()

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_file_upload_and_retract_idempotency(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    brain = FakeProjectBrainClient()
    monkeypatch.setattr(cockpit_api_module, "_project_brain_client", lambda _ctx: brain)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first_upload = await client.post(
            f"{base}/v1/projects/neil-shared/files",
            headers={"X-Idempotency-Key": "upload-1"},
            files={"file": ("spec.md", b"# Spec", "text/markdown")},
        )
        upload_replay = await client.post(
            f"{base}/v1/projects/neil-shared/files",
            headers={"X-Idempotency-Key": "upload-1"},
            files={"file": ("spec.md", b"# Spec", "text/markdown")},
        )
        upload_conflict = await client.post(
            f"{base}/v1/projects/neil-shared/files",
            headers={"X-Idempotency-Key": "upload-1"},
            files={"file": ("spec.md", b"# Changed", "text/markdown")},
        )
        retract = await client.request(
            "DELETE",
            f"{base}/v1/projects/neil-shared/files/upload-123",
            json={"idempotency_key": "retract-1"},
        )
        retract_replay = await client.request(
            "DELETE",
            f"{base}/v1/projects/neil-shared/files/upload-123",
            json={"idempotency_key": "retract-1"},
        )

        assert first_upload.status_code == 200
        assert upload_replay.json()["idempotent"] is True
        assert upload_replay.json()["doc_id"] == first_upload.json()["doc_id"]
        assert upload_conflict.status_code == 409
        assert retract.status_code == 200
        assert retract_replay.json()["idempotent"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert [call["op"] for call in brain.calls] == ["project.file.upload", "project.file.retract"]


def test_cockpit_thread_open_idempotency(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway(["unused"]), tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Plan", "idempotency_key": "thread-1"})
        replay = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Plan", "idempotency_key": "thread-1"})
        conflict = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Changed", "idempotency_key": "thread-1"})

        assert first.status_code == 200
        assert replay.status_code == 200
        assert replay.json()["idempotent"] is True
        assert replay.json()["thread"] == first.json()["thread"]
        assert conflict.status_code == 409

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert len(memory.sessions) == 1


def test_cockpit_findings_decisions_use_resource_write_envelope(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        finding = (await client.post(f"{base}/v1/projects/neil-shared/findings", json={"content": "Finding."})).json()
        decision = (await client.post(f"{base}/v1/projects/neil-shared/decisions", json={"content": "Decision."})).json()

        for body in (finding, decision):
            assert body["ok"] is True
            assert body["api_version"] == "v1"
            assert body["schema_version"] == 1
            assert body["project_id"] == "neil-shared"
            assert body["content_hash"].startswith("sha256:")
            assert body["result"] == {"project_id": "neil-shared", "content_hash": body["content_hash"]}

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_oauth_projects_use_subject_not_process_identity_or_jarvis_user(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    _seed_project_registry(cfg)
    _seed_user_profiles(cfg, "jules", "neil")
    token = fixture["sign"](subject="jules", jarvis_user="neil", scope="jarvis:read")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        headers = {"Authorization": f"Bearer {token}"}
        listing = await client.get(f"{base}/v1/projects", headers=headers)
        process_visible = await client.get(f"{base}/v1/projects/neil-shared", headers=headers)
        process_visible_memory = await client.get(f"{base}/v1/projects/neil-shared/memory", headers=headers)
        private = await client.get(f"{base}/v1/projects/alice-private", headers=headers)
        private_memory = await client.get(f"{base}/v1/projects/alice-private/memory", headers=headers)

        assert listing.status_code == 200
        assert [project["id"] for project in listing.json()["projects"]] == ["house-story"]
        assert process_visible.status_code == 404
        assert process_visible_memory.status_code == 404
        assert private.status_code == 404
        assert private_memory.status_code == 404

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))


def test_cockpit_oauth_household_principal_without_projects_sees_household_projects(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    _seed_project_registry(cfg)
    _seed_user_profiles(cfg, "riley")
    token = fixture["sign"](subject="riley", jarvis_user="neil", scope="jarvis:read")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        headers = {"Authorization": f"Bearer {token}"}
        listing = await client.get(f"{base}/v1/projects", headers=headers)
        household = await client.get(f"{base}/v1/projects/house-story", headers=headers)
        household_memory = await client.get(f"{base}/v1/projects/house-story/memory", headers=headers)
        shared = await client.get(f"{base}/v1/projects/neil-shared", headers=headers)
        shared_memory = await client.get(f"{base}/v1/projects/neil-shared/memory", headers=headers)
        private = await client.get(f"{base}/v1/projects/alice-private", headers=headers)

        assert listing.status_code == 200
        assert [project["id"] for project in listing.json()["projects"]] == ["house-story"]
        assert household.status_code == 200
        assert household.json()["project"]["id"] == "house-story"
        assert household_memory.status_code == 200
        assert shared.status_code == 404
        assert shared_memory.status_code == 404
        assert private.status_code == 404

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))


def test_cockpit_oauth_unmapped_subject_gets_no_project_visibility(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    _seed_project_registry(cfg)
    _seed_user_profiles(cfg, "neil")
    token = fixture["sign"](subject="idp-user-123", jarvis_user="neil", scope="jarvis:read")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        headers = {"Authorization": f"Bearer {token}"}
        listing = await client.get(f"{base}/v1/projects", headers=headers)
        detail = await client.get(f"{base}/v1/projects/house-story", headers=headers)
        memory = await client.get(f"{base}/v1/projects/house-story/memory", headers=headers)

        assert listing.status_code == 200
        assert listing.json()["projects"] == []
        assert detail.status_code == 404
        assert memory.status_code == 404

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))


def test_cockpit_project_access_matches_voice_memory_matrix(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)
    registry = cockpit_api_module._registry_store(cfg)  # noqa: SLF001
    requesters = (
        RequestContext(
            "dev",
            "jules",
            "personal",
            frozenset({"memory.query"}),
            channel="cockpit",
            peer="jules",
        ),
        RequestContext(
            "dev",
            "riley",
            "personal",
            frozenset({"memory.query"}),
            channel="cockpit",
            peer="riley",
        ),
    )

    for requester in requesters:
        for project_id in ("house-story", "neil-shared", "alice-private"):
            project = registry.get_project(project_id)
            assert project is not None
            api_allowed = cockpit_api_module._project_access_allowed(registry, requester, project)  # noqa: SLF001
            voice_allowed = can_query_memory_peer(requester, project.peer_id, registry=registry).allowed
            assert api_allowed == voice_allowed


def test_cockpit_projects_empty_registry_returns_empty_list(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        listing = await client.get(f"{base}/v1/projects")
        detail = await client.get(f"{base}/v1/projects/anything")

        assert listing.status_code == 200
        assert listing.json()["projects"] == []
        assert detail.status_code == 404
        assert detail.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_projects_archived_filtering(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        default = await client.get(f"{base}/v1/projects")
        included = await client.get(f"{base}/v1/projects?include_archived=true")
        detail = await client.get(f"{base}/v1/projects/old-project")

        assert "old-project" not in {project["id"] for project in default.json()["projects"]}
        assert "old-project" in {project["id"] for project in included.json()["projects"]}
        assert detail.status_code == 200
        assert detail.json()["project"]["status"] == "archived"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_projects_require_api_auth(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, token="secret", identity="neil")
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        listing = await client.get(f"{base}/v1/projects")
        detail = await client.get(f"{base}/v1/projects/neil-shared")
        memory = await client.get(f"{base}/v1/projects/neil-shared/memory")

        assert listing.status_code == 401
        assert listing.json()["error"]["code"] == "unauthorized"
        assert detail.status_code == 401
        assert detail.json()["error"]["code"] == "unauthorized"
        assert memory.status_code == 401
        assert memory.json()["error"]["code"] == "unauthorized"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_projects_without_requester_identity_are_empty(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        listing = await client.get(f"{base}/v1/projects")
        detail = await client.get(f"{base}/v1/projects/house-story")
        memory = await client.get(f"{base}/v1/projects/house-story/memory")

        assert listing.status_code == 200
        assert listing.json()["projects"] == []
        assert detail.status_code == 404
        assert detail.json()["error"]["code"] == "not_found"
        assert memory.status_code == 404
        assert memory.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_auth_metadata_is_public_and_secret_free(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        token="secret",
        auth_mode="hybrid",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read jarvis:operate",
    )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/auth/metadata")
        body = response.json()

        assert response.status_code == 200
        assert body == {
            "auth_mode": "hybrid",
            "issuer": "https://cockpit.example",
            "audience": "jarvis-brain",
            "jwks_url": "https://cockpit.example/api/auth/jwks",
            "required_scopes": ["jarvis:read", "jarvis:operate"],
            "jarvis_user_claim": "jarvis_user",
        }
        assert response.headers["Cache-Control"] == "no-store"
        assert "secret" not in response.text

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_oauth_jwt_allows_health_and_snapshot(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read jarvis:operate",
    )
    token = fixture["sign"]()

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        event_loop_thread = threading.get_ident()
        headers = {"Authorization": f"Bearer {token}"}
        health = await client.get(f"{base}/v1/health", headers=headers)
        snapshot = await client.get(f"{base}/v1/cockpit/snapshot", headers=headers)

        assert health.status_code == 200
        assert health.json()["runtime"]["channel"] == "production"
        assert snapshot.status_code == 200
        assert snapshot.json()["api_version"]
        assert fixture["calls"]["jwks"] == 1
        assert fixture["calls"]["threads"]
        assert all(thread_id != event_loop_thread for thread_id in fixture["calls"]["threads"])

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))


def test_cockpit_oauth_rejects_bad_jwt_claims(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read jarvis:operate",
    )
    invalid_tokens = [
        fixture["sign"](issuer="https://evil.example"),
        fixture["sign"](audience="other-brain"),
        fixture["sign"](scope="jarvis:read"),
        fixture["sign"](expires_delta=timedelta(minutes=-2)),
        fixture["sign"](jarvis_user=""),
        "not-a-jwt",
    ]

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        for token in invalid_tokens:
            response = await client.get(f"{base}/v1/health", headers={"Authorization": f"Bearer {token}"})
            assert response.status_code == 401
            assert response.json()["error"]["code"] == "unauthorized"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))


def test_cockpit_oauth_throttles_unknown_kid_jwks_refreshes(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
        oauth_jwks_min_refresh_s="30",
    )
    valid_token = fixture["sign"]()
    unknown_tokens = [fixture["sign"](token_kid=f"rotated-key-{idx}") for idx in range(8)]

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        valid = await client.get(f"{base}/v1/health", headers={"Authorization": f"Bearer {valid_token}"})
        assert valid.status_code == 200

        for token in unknown_tokens:
            response = await client.get(f"{base}/v1/health", headers={"Authorization": f"Bearer {token}"})
            assert response.status_code == 401
            assert response.json()["error"]["code"] == "unauthorized"

        # One initial JWKS load for the valid token plus one allowed unknown-kid
        # refresh. The rest are served from cache and rejected.
        assert fixture["calls"]["jwks"] == 2

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))


def test_cockpit_oauth_bounds_negative_kid_cache() -> None:
    fixture, jwks_get = _oauth_fixture()
    validator = _oauth_validator(jwks_get)
    validator.validate(fixture["sign"]())

    for idx in range(validator._NEG_KID_MAX + 100):  # noqa: SLF001
        with pytest.raises(OAuthValidationError):
            validator.validate(_unsigned_jwt_with_kid(f"random-kid-{idx}"))

    assert len(validator._negative_kids) <= validator._NEG_KID_MAX  # noqa: SLF001
    assert fixture["calls"]["jwks"] == 2


def test_cockpit_oauth_failed_unknown_kid_refresh_is_throttled_and_nonblocking() -> None:
    fixture, jwks_get = _oauth_fixture()
    failing = False
    fail_calls = {"jwks": 0}
    fetch_started = threading.Event()
    release_fetch = threading.Event()

    def slow_failing_get(url: str, **kwargs: Any) -> Response:
        if not failing:
            return jwks_get(url, **kwargs)
        fail_calls["jwks"] += 1
        fetch_started.set()
        release_fetch.wait(timeout=2)
        raise RuntimeError("jwks endpoint down")

    validator = _oauth_validator(slow_failing_get)
    valid_token = fixture["sign"]()
    validator.validate(valid_token)
    failing = True

    errors: list[Exception] = []

    def validate_unknown() -> None:
        try:
            validator.validate(_unsigned_jwt_with_kid("unknown-during-outage"))
        except OAuthValidationError as exc:
            errors.append(exc)

    thread = threading.Thread(target=validate_unknown)
    thread.start()
    assert fetch_started.wait(timeout=1)

    started_at = time.perf_counter()
    principal = validator.validate(valid_token)
    elapsed = time.perf_counter() - started_at

    release_fetch.set()
    thread.join(timeout=1)
    assert not thread.is_alive()
    assert errors
    assert principal.subject == "user_123"
    assert elapsed < 0.5

    for idx in range(5):
        with pytest.raises(OAuthValidationError):
            validator.validate(_unsigned_jwt_with_kid(f"unknown-after-outage-{idx}"))

    assert fail_calls["jwks"] == 1


def test_cockpit_oauth_refetches_jwks_after_ttl(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
        oauth_jwks_ttl_s="0.001",
    )
    token = fixture["sign"]()

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = await client.get(f"{base}/v1/health", headers={"Authorization": f"Bearer {token}"})
        await asyncio.sleep(0.01)
        second = await client.get(f"{base}/v1/health", headers={"Authorization": f"Bearer {token}"})

        assert first.status_code == 200
        assert second.status_code == 200
        assert fixture["calls"]["jwks"] == 2

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))


def test_cockpit_oauth_rejects_header_algorithm_confusion(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture(include_alg=False)
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    token = fixture["sign"](algorithm="HS256", signing_key="attacker-secret-value-with-enough-bytes")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/health", headers={"Authorization": f"Bearer {token}"})

        assert response.status_code == 401
        assert response.json()["error"]["code"] == "unauthorized"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))


def test_cockpit_oauth_requires_secure_issuer_and_jwks_urls(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    insecure_cfg = _cfg(
        tmp_path,
        monkeypatch,
        auth_mode="oauth",
        oauth_issuer="http://issuer.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    with pytest.raises(ValueError, match="OAuth issuer must use https://"):
        make_app(insecure_cfg)

    localhost_cfg = _cfg(
        tmp_path,
        monkeypatch,
        auth_mode="oauth",
        oauth_issuer="http://localhost:41760",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="http://127.0.0.1:41760/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    make_app(localhost_cfg)


def test_cockpit_hybrid_accepts_legacy_token_while_oauth_is_configured(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    _fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        token="secret",
        auth_mode="hybrid",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/health", headers={"Authorization": "Bearer secret"})
        assert response.status_code == 200

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=jwks_get))


def test_cockpit_cors_preflight_uses_configured_origins(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, token="secret", cors_origins="https://cockpit.example")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        allowed = await client.options(
            f"{base}/v1/cockpit/snapshot",
            headers={
                "Origin": "https://cockpit.example",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Authorization",
            },
        )
        denied = await client.options(
            f"{base}/v1/cockpit/snapshot",
            headers={
                "Origin": "https://evil.example",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Authorization",
            },
        )
        unknown = await client.options(
            f"{base}/v1/nope",
            headers={
                "Origin": "https://cockpit.example",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Authorization",
            },
        )
        async with client.stream(
            "GET",
            f"{base}/v1/cockpit/events",
            headers={"Origin": "https://cockpit.example", "Authorization": "Bearer secret"},
        ) as sse:
            first = ""
            async for chunk in sse.aiter_text():
                first += chunk
                if "\n\n" in first:
                    break

        assert allowed.status_code == 204
        assert allowed.headers["Access-Control-Allow-Origin"] == "https://cockpit.example"
        assert allowed.headers["Vary"] == "Origin"
        assert "Authorization" in allowed.headers["Access-Control-Allow-Headers"]
        assert "Access-Control-Allow-Origin" not in denied.headers
        assert unknown.status_code == 404
        assert sse.headers["Access-Control-Allow-Origin"] == "https://cockpit.example"
        assert "event: snapshot" in first

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_session_ref_rejects_tampering(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    ref = make_session_ref("macbook-worker", "sess_123")
    tampered = f"{ref[:-2]}{'A' if ref[-2] != 'A' else 'B'}{ref[-1]}"

    assert ref.startswith("sessref_")
    assert "macbook-worker" not in ref
    assert "sess_123" not in ref

    _store, run_id = _seed_run(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        resolved = await client.get(f"{base}/v1/sessions/{ref}")
        rejected = await client.get(f"{base}/v1/sessions/{tampered}")

        assert resolved.status_code == 200
        assert resolved.json()["session"]["session_ref"] == ref
        assert rejected.status_code == 404
        assert rejected.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_unknown_session_ref_does_not_sweep_workers(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, _run_id = _seed_run(cfg)
    unknown_ref = "sessref_unknown-but-url-safe"

    def no_worker_get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        raise AssertionError(f"unknown session_ref should not sweep workers: {url}")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/sessions/{unknown_ref}")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=no_worker_get))


def test_cockpit_workers_reject_invalid_probe_value(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)

    def no_worker_get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        raise AssertionError(f"invalid probe should fail before worker HTTP: {url}")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        list_response = await client.get(f"{base}/v1/workers", params={"probe": "probe"})
        detail_response = await client.get(f"{base}/v1/workers/macbook-worker", params={"probe": "probe"})

        assert list_response.status_code == 400
        assert detail_response.status_code == 400
        assert list_response.json()["error"]["code"] == "validation_failed"
        assert detail_response.json()["error"]["code"] == "validation_failed"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=no_worker_get))


def test_cockpit_run_events_filter_non_public_urls(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    store, run_id = _seed_run(cfg)
    store.link_artifact(run_id, Artifact(type="url", id="private", url="http://localhost:8780/logs?token=secret", status="open"))
    store.append_event(run_id, "worker_link", "log at http://localhost:8780/logs?token=secret", {"summary": "open /workspace/private/log and http://localhost:8780/logs?token=secret"})

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        events = (await client.get(f"{base}/v1/runs/{run_id}/events")).json()
        text = json.dumps(events)

        assert "localhost" not in text
        assert "token=secret" not in text
        assert "/workspace/" not in text
        assert "<redacted-url>" in text
        assert "<local-path>" in text

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_artifact_titles_and_urls_are_public_safe(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    store, run_id = _seed_run(cfg)
    store.link_artifact(run_id, Artifact(type="url", id="private", url="http://localhost:8780/logs?token=secret", status="open"))
    store.link_artifact(run_id, Artifact(type="url", id="github", url="https://github.com/roughcoder/jarvis/pull/49?code=secret#frag", status="open"))

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        artifacts = (await client.get(f"{base}/v1/runs/{run_id}/artifacts")).json()
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot")).json()
        text = json.dumps({"artifacts": artifacts, "snapshot": snapshot})

        assert "localhost" not in text
        assert "token=secret" not in text
        assert "code=secret" not in text
        assert "#frag" not in text
        assert "https://github.com/roughcoder/jarvis/pull/49" in text

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_worker_error_messages_are_redacted(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, _run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")
    private_path = "/Users" + "/example/private/jarvis"
    fake_token = "sk-" + "abcdefghijklmnopqrstuvwxyz"

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/sess_123"):
            return Response({"error": f"failed in {private_path} with {fake_token}"}, status_code=500)
        return Response({})

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/sessions/{ref}")
        body = response.json()

        assert response.status_code == 200, body
        assert body["session"]["session_ref"] == ref
        assert body["session"]["run_id"]
        assert body["raw"] == {}
        assert "/Users/" not in json.dumps(body)
        assert fake_token not in json.dumps(body)

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_work_start_redacts_dispatch_errors(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push")
    private_path = "/Users" + "/example/private/jarvis"

    def next_work(_self, _command, *, start: bool = False):  # noqa: ANN001, FBT001, FBT002
        from jarvis.orchestration.service import WorkerDispatchError

        raise WorkerDispatchError("run_private", RuntimeError(f"worker rejected cwd {private_path}"))

    monkeypatch.setattr("jarvis.orchestration.service.OrchestrationService.next_work", next_work)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{base}/v1/work/start",
            json={"idempotency_key": "dispatch_private", "source": "manual", "repo": "roughcoder/jarvis", "phrase": "start"},
        )
        body = response.json()

        assert response.status_code == 502, body
        assert body["error"]["code"] == "provider_unavailable"
        assert "/Users/" not in body["error"]["message"]
        assert "<local-path>" in body["error"]["message"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))


def test_cockpit_worker_connection_errors_are_public_worker_unavailable(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, _run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/sess_123"):
            raise httpx.ConnectError("connection refused at /Users/example/private/socket")
        return Response({})

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/sessions/{ref}")
        body = response.json()

        assert response.status_code == 200
        assert body["session"]["session_ref"] == ref
        assert body["session"]["status"] == "running"
        assert body["raw"] == {}
        assert "/Users/" not in json.dumps(body)

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_api_refuses_unsafe_bind_with_nonzero_status(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    cfg.orchestration.api_host = "0.0.0.0"

    import asyncio

    assert asyncio.run(serve(cfg)) == 1


def test_cockpit_api_bounds_idle_keepalive_connections(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    runner_options: dict[str, Any] = {}

    class FakeRunner:
        def __init__(self, _app: object, **kwargs: Any) -> None:
            runner_options.update(kwargs)

        async def setup(self) -> None:
            pass

        async def cleanup(self) -> None:
            pass

    class FakeSite:
        def __init__(self, _runner: FakeRunner, _bind: str, _port: int) -> None:
            pass

        async def start(self) -> None:
            pass

    class FakeEvent:
        def set(self) -> None:
            pass

        async def wait(self) -> None:
            pass

    class FakeLoop:
        def add_signal_handler(self, _signal: object, _callback: object) -> None:
            pass

    monkeypatch.setattr(cockpit_api_module, "make_app", lambda _cfg: object())
    monkeypatch.setattr(cockpit_api_module.web, "AppRunner", FakeRunner)
    monkeypatch.setattr(cockpit_api_module.web, "TCPSite", FakeSite)
    monkeypatch.setattr(cockpit_api_module.asyncio, "Event", FakeEvent)
    monkeypatch.setattr(cockpit_api_module.asyncio, "get_running_loop", FakeLoop)

    assert asyncio.run(serve(cfg)) == 0
    assert runner_options == {"keepalive_timeout": 15}


def test_cockpit_session_write_proxy_and_idempotency(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.turn")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")
    posts: list[dict[str, Any]] = []

    def post(url: str, **kwargs) -> Response:  # noqa: ANN001
        posts.append({"url": url, "json": kwargs.get("json")})
        assert kwargs["json"]["allowed_actions"] == ["worker.session.turn"]
        return Response(
            {
                "ok": True,
                "session": {"session_id": "sess_123", "status": "running"},
                "events": [
                    {
                        "event_id": "ev_turn",
                        "session_id": "sess_123",
                        "type": "turn.started",
                        "time": "2026-07-01T12:00:00Z",
                        "data": {"turn_id": "turn_ui", "idempotency_key": "t3_key"},
                    }
                ],
            }
        )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        body = {
            "idempotency_key": "t3_key",
            "prompt": "continue",
            "metadata": {
                "surface": "jarvis-cockpit",
                "allowed_actions": ["worker.session.stop"],
                "control_envelope": {"allowed_actions": ["worker.session.stop"]},
                "execution_envelope": {"allowed_actions": ["worker.session.stop"]},
            },
            "execution_envelope": {"allowed_actions": ["worker.session.stop"]},
            "allowed_actions": ["worker.session.stop"],
        }
        first = (await client.post(f"{base}/v1/sessions/{ref}/turns", json=body)).json()
        second = (await client.post(f"{base}/v1/sessions/{ref}/turns", json=body)).json()
        conflict = await client.post(f"{base}/v1/sessions/{ref}/turns", json={**body, "prompt": "different"})

        assert first["ok"] is True
        assert first["events"][0]["event_id"] == "ev_turn"
        assert second["idempotent"] is True
        assert len(posts) == 1
        assert posts[0]["json"]["allowed_actions"] == ["worker.session.turn"]
        assert "execution_envelope" not in posts[0]["json"]
        assert "allowed_actions" not in posts[0]["json"]["metadata"]
        assert "control_envelope" not in posts[0]["json"]["metadata"]
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "idempotency_conflict"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_post=post))


def test_cockpit_session_write_persists_result_for_store_only_snapshots(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.stop")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")
    state = {"status": "running"}

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/sess_123"):
            response = _fake_get(run_id)(url, **kwargs).json()
            response["status"] = state["status"]
            return Response(response)
        return _fake_get(run_id)(url, **kwargs)

    def post(url: str, **_kwargs) -> Response:  # noqa: ANN001
        assert url.endswith("/sessions/sess_123/stop")
        state["status"] = "stopped"
        return Response(
            {
                "ok": True,
                "session": {"session_id": "sess_123", "status": "stopped"},
                "event": {"event_id": "ev_stop", "session_id": "sess_123", "type": "session.stopped"},
            }
        )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/stop", json={"idempotency_key": "stop_store"})
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot")).json()

        assert response.status_code == 200
        assert response.json()["session"]["status"] == "stopped"
        assert snapshot["sessions"][0]["status"] == "stopped"
        assert snapshot["sessions"][0]["latest_event_cursor"] == "ev_stop"
        assert snapshot["runs"][0]["active_session_count"] == 0

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get, http_post=post))


def test_cockpit_session_write_returns_best_effort_packet_when_reconcile_reads_fail(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.stop")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")
    state = {"posted": False}

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if state["posted"] and url.endswith("/sessions/sess_123"):
            return Response({"error": "session unavailable"}, status_code=503)
        return _fake_get(run_id)(url, **kwargs)

    def post(url: str, **_kwargs) -> Response:  # noqa: ANN001
        assert url.endswith("/sessions/sess_123/stop")
        state["posted"] = True
        return Response(
            {
                "ok": True,
                "session": {"session_id": "sess_123", "status": "stopped"},
                "event": {"event_id": "ev_stop_best_effort", "session_id": "sess_123", "type": "session.stopped"},
            }
        )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/stop", json={"idempotency_key": "stop_best_effort"})
        replay = await client.post(f"{base}/v1/sessions/{ref}/stop", json={"idempotency_key": "stop_best_effort"})

        assert response.status_code == 200
        assert response.json()["session"]["status"] == "stopped"
        assert response.json()["events"][0]["event_id"] == "ev_stop_best_effort"
        assert replay.json()["idempotent"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get, http_post=post))


def test_cockpit_session_write_finalizes_run_when_last_session_is_terminal(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.stop")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")
    state = {"status": "running"}

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/sess_123"):
            data = _fake_get(run_id)(url, **kwargs).json()
            data["status"] = state["status"]
            return Response(data)
        return _fake_get(run_id)(url, **kwargs)

    def post(_url: str, **_kwargs) -> Response:  # noqa: ANN001
        state["status"] = "stopped"
        return Response({"ok": True, "session": {"session_id": "sess_123", "status": "stopped"}})

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/stop", json={"idempotency_key": "stop_terminal"})
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot")).json()

        assert response.status_code == 200
        assert snapshot["runs"][0]["status"] == "terminal"
        assert snapshot["runs"][0]["phase"] == "failed"
        assert snapshot["runs"][0]["terminal_reason"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get, http_post=post))


def test_cockpit_archive_run_hides_it_from_views(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    _store, run_id = _seed_run(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/runs/{run_id}/archive", json={"idempotency_key": "archive_run_1"})
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot")).json()
        runs = (await client.get(f"{base}/v1/runs")).json()
        sessions = (await client.get(f"{base}/v1/sessions")).json()
        detail = (await client.get(f"{base}/v1/runs/{run_id}")).json()
        artifacts = (await client.get(f"{base}/v1/runs/{run_id}/artifacts")).json()

        assert response.status_code == 200
        assert response.json()["run"]["archived_at"]
        assert snapshot["runs"] == []
        assert snapshot["sessions"] == []
        assert runs["runs"] == []
        assert sessions["sessions"] == []
        assert detail["summary"]["artifact_count"] >= 2
        assert detail["run"]["artifacts"]
        assert {"branch", "pull_request"}.issubset({item["kind"] for item in artifacts["items"]})

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_archive_session_hides_it_without_archiving_run(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/archive", json={"idempotency_key": "archive_session_1"})
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()
        detail = (await client.get(f"{base}/v1/sessions/{ref}")).json()

        assert response.status_code == 200
        assert response.json()["session"]["archived_at"]
        assert detail["session"]["archived_at"]
        assert snapshot["runs"][0]["run_id"] == run_id
        assert snapshot["runs"][0]["session_count"] == 0
        assert snapshot["runs"][0]["pending_approval_count"] == 0
        assert snapshot["runs"][0]["pending_input_count"] == 0
        assert snapshot["sessions"] == []
        assert all(artifact["kind"] != "branch" for artifact in snapshot["artifacts"])

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_archive_reclaims_nothing(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    _store, run_id = _seed_run(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/runs/{run_id}/archive", json={})

        assert response.status_code == 200
        assert response.json()["reclamation"] == {
            "records": 0,
            "events": 0,
            "worktrees": 0,
            "bytes": 0,
            "memory_sessions": 0,
            "notes": [],
        }
def test_cockpit_session_close_stops_cleans_up_and_archives(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.stop,orchestration.runs.write")
    store, run_id = _seed_run(cfg)
    parent = store.create_run("Parent orchestrator")
    child = store.get(run_id)
    assert child is not None
    child.parent_chat_id = parent.run_id
    child.parent_run_id = parent.run_id
    store.save(child)
    parent.child_chat_ids.append(run_id)
    parent.child_run_ids.append(run_id)
    store.save(parent)
    grandchild = store.create_run("Grandchild work", parent_chat_id=run_id)
    ref = make_session_ref("macbook-worker", "sess_123")
    state = {"status": "running"}
    posts: list[str] = []

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/sess_123"):
            data = _fake_get(run_id)(url, **kwargs).json()
            data["status"] = state["status"]
            return Response(data)
        return _fake_get(run_id)(url, **kwargs)

    def post(url: str, **_kwargs) -> Response:  # noqa: ANN001
        posts.append(url)
        if url.endswith("/sessions/sess_123/stop"):
            state["status"] = "stopped"
            return Response({"ok": True, "session": {"session_id": "sess_123", "status": "stopped"}})
        if url.endswith("/worktrees/prune"):
            return Response({"ok": True, "pruned": [{"name": "worktree"}], "bytes": 0, "refused": []})
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/close", json={"idempotency_key": "close_session_1"})
        replay = await client.post(f"{base}/v1/sessions/{ref}/close", json={"idempotency_key": "close_session_1"})
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot")).json()

        assert response.status_code == 200
        body = response.json()
        assert body["session"]["archived_at"]
        assert body["cleanup"] == {"requested": True, "ok": True, "cleaned": ["worktree"]}
        assert replay.json()["idempotent"] is True
        assert posts == ["http://worker.test/sessions/sess_123/stop", "http://worker.test/worktrees/prune"]
        run_rows = {row["run_id"]: row for row in snapshot["runs"]}
        assert run_rows[run_id]["session_count"] == 0
        assert snapshot["sessions"] == []

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get, http_post=post))
    reloaded = OrchestrationStore(cfg.orchestration.workspace)
    events = reloaded.events(run_id)
    closed = reloaded.get(run_id)
    reloaded_parent = reloaded.get(parent.run_id)
    reloaded_grandchild = reloaded.get(grandchild.run_id)
    assert closed is not None
    assert closed.parent_chat_id is None
    assert closed.child_chat_ids == []
    assert reloaded_parent is not None
    assert run_id not in reloaded_parent.child_chat_ids
    assert reloaded_grandchild is not None
    assert reloaded_grandchild.parent_chat_id is None
    assert any(event.type == "session_cleanup_requested" for event in events)


def test_cockpit_run_and_session_rename_update_chat_title(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        run_renamed = await client.post(f"{base}/v1/runs/{run_id}/rename", json={"title": "Parent title"})
        session_renamed = await client.post(f"{base}/v1/sessions/{ref}/rename", json={"title": "Child title"})
        detail = (await client.get(f"{base}/v1/runs/{run_id}")).json()

        assert run_renamed.status_code == 200
        assert run_renamed.json()["run"]["title"] == "Parent title"
        assert session_renamed.status_code == 200
        assert session_renamed.json()["run"]["title"] == "Child title"
        assert session_renamed.json()["session"]["title"] == "Child title"
        assert detail["run"]["title"] == "Child title"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_delete_session_prunes_worker_and_is_idempotent(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")
    deletes: list[str] = []

    def delete(url: str, **_kwargs) -> Response:  # noqa: ANN001
        deletes.append(url)
        if url.endswith("/sessions/sess_123"):
            return Response(
                {
                    "ok": True,
                    "deleted": True,
                    "reclamation": {"records": 1, "events": 3, "worktrees": 1, "bytes": 4096},
                }
            )
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = await client.delete(f"{base}/v1/sessions/{ref}")
        second = await client.delete(f"{base}/v1/sessions/{ref}")
        sessions = (await client.get(f"{base}/v1/sessions", params={"sync": "fast"})).json()
        detail = (await client.get(f"{base}/v1/sessions/{ref}")).json()

        assert first.status_code == 200
        assert first.json()["reclamation"]["records"] == 2
        assert first.json()["reclamation"]["events"] == 3
        assert first.json()["reclamation"]["worktrees"] == 1
        assert first.json()["reclamation"]["bytes"] == 4096
        assert second.status_code == 200
        assert second.json()["deleted"] is False
        assert sessions["sessions"] == []
        assert detail["deleted"] is True
        assert detail["session"]["status"] == "deleted"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_delete=delete))
    assert len(deletes) == 1


def test_cockpit_delete_empty_worker_only_session_marks_deleted(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    store = OrchestrationStore(cfg.orchestration.workspace)
    ref = make_session_ref("macbook-worker", "worker_only_empty")
    store.record_session_refs(
        [
            {
                "session_ref": ref,
                "worker_id": "macbook-worker",
                "session_id": "worker_only_empty",
            }
        ]
    )

    def delete(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/worker_only_empty"):
            return Response({"error": "not found"}, status_code=404)
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = await client.delete(f"{base}/v1/sessions/{ref}")
        second = await client.delete(f"{base}/v1/sessions/{ref}")
        detail = await client.get(f"{base}/v1/sessions/{ref}")

        assert first.status_code == 200
        assert first.json()["deleted"] is True
        assert first.json()["reclamation"]["records"] == 0
        assert first.json()["reclamation"]["events"] == 0
        assert first.json()["reclamation"]["worktrees"] == 0
        assert second.status_code == 200
        assert second.json()["deleted"] is False
        assert detail.json()["deleted"] is True
        assert detail.json()["session"]["status"] == "deleted"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("run_missing"), http_delete=delete))


def test_cockpit_delete_run_deletes_owned_sessions_and_records(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    _store, run_id = _seed_run(cfg)

    def delete(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/sess_123"):
            return Response({"ok": True, "reclamation": {"records": 1, "events": 2, "worktrees": 1, "bytes": 512}})
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = await client.delete(f"{base}/v1/runs/{run_id}")
        second = await client.delete(f"{base}/v1/runs/{run_id}")
        listed = (await client.get(f"{base}/v1/runs")).json()
        detail = (await client.get(f"{base}/v1/runs/{run_id}")).json()
        session_detail = (await client.get(f"{base}/v1/sessions/{make_session_ref('macbook-worker', 'sess_123')}")).json()

        assert first.status_code == 200
        assert first.json()["reclamation"]["records"] >= 3
        assert first.json()["reclamation"]["events"] >= 3
        assert first.json()["reclamation"]["worktrees"] == 1
        assert first.json()["reclamation"]["bytes"] == 512
        assert second.status_code == 200
        assert second.json()["deleted"] is False
        assert listed["runs"] == []
        assert detail["deleted"] is True
        assert detail["run"]["status"] == "deleted"
        assert session_detail["deleted"] is True
        assert session_detail["session"]["status"] == "deleted"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_delete=delete))


def test_cockpit_delete_run_refuses_job_backed_runs(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    store, run_id = _seed_run(cfg)
    store.link_job(
        run_id,
        WorkerJobLink(
            worker_id="macbook-worker",
            job_id="job_running",
            status="running",
            engine="codex",
            branch="jarvis/job",
            cwd="/Users/example/private/jarvis/.worktrees/job",
        ),
    )

    def delete(url: str, **_kwargs) -> Response:  # noqa: ANN001
        raise AssertionError(f"worker delete should not be called for job-backed runs: {url}")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.delete(f"{base}/v1/runs/{run_id}")
        listed = (await client.get(f"{base}/v1/runs")).json()

        assert response.status_code == 409
        assert response.json()["error"]["code"] == "conflict"
        assert "worker jobs" in response.json()["error"]["message"]
        assert listed["runs"][0]["run_id"] == run_id

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_delete=delete))


def test_cockpit_worker_only_sessions_include_checkpoint_counts(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    ref = make_session_ref("macbook-worker", "sess_worker_only")

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/health"):
            return Response({"ok": True, "agent": "codex", "supported_engines": ["codex"]})
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response(
                {
                    "sessions": [
                        {
                            "session_id": "sess_worker_only",
                            "provider": "codex",
                            "engine": "codex",
                            "status": "running",
                            "repo": "roughcoder/jarvis",
                            "title": "Worker-only session",
                            "metadata": {"execution_envelope": {"allowed_actions": ["worker.session.turn", "worker.session.restore"]}},
                        }
                    ]
                }
            )
        if url.endswith("/sessions/sess_worker_only/checkpoints"):
            return Response({"checkpoints": [{"checkpoint_id": "ckpt_worker", "label": "worker only"}]})
        if url.endswith("/sessions/checkpoints"):
            return Response({"error": "not found"}, status_code=404)
        if url.endswith("/sessions/requests"):
            return Response({"requests": []})
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

        assert snapshot["sessions"][0]["session_ref"] == ref
        assert snapshot["sessions"][0]["checkpoint_count"] == 1
        assert "checkpoint_restore" in snapshot["sessions"][0]["supported_controls"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_checkpoint_aggregation_uses_worker_bulk_endpoint(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    ref = make_session_ref("macbook-worker", "sess_worker_only")
    calls_seen: list[str] = []

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        calls_seen.append(url)
        if url.endswith("/health"):
            return Response({"ok": True, "agent": "codex", "supported_engines": ["codex"]})
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response(
                {
                    "sessions": [
                        {
                            "session_id": "sess_worker_only",
                            "provider": "codex",
                            "engine": "codex",
                            "status": "running",
                            "repo": "roughcoder/jarvis",
                            "title": "Worker-only session",
                            "metadata": {"execution_envelope": {"allowed_actions": ["worker.session.turn", "worker.session.restore"]}},
                        }
                    ]
                }
            )
        if url.endswith("/sessions/checkpoints"):
            return Response({"checkpoints": [{"session_id": "sess_worker_only", "checkpoint_id": "ckpt_bulk", "label": "bulk"}]})
        if url.endswith("/sessions/requests"):
            return Response({"requests": []})
        if url.endswith("/sessions/sess_worker_only/checkpoints"):
            raise AssertionError("bulk checkpoint response should avoid per-session checkpoint calls")
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

        assert snapshot["sessions"][0]["session_ref"] == ref
        assert snapshot["sessions"][0]["checkpoint_count"] == 1
        assert any(url.endswith("/sessions/checkpoints") for url in calls_seen)
        assert not any(url.endswith("/sessions/sess_worker_only/checkpoints") for url in calls_seen)

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_snapshot_uses_same_version_worker_bulk_checkpoints(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    worker_token = "worker-test-token"
    worker_cfg = WorkerConfig(_env_file=None, token=worker_token, workspace=str(tmp_path / "worker"))
    sessions = SessionManager(str(tmp_path / "worker" / "sessions"))
    for index in range(20):
        session_id = f"sess_{index:02d}"
        sessions.create({"session_id": session_id, "provider": "fake", "engine": "fake"})
        sessions.append_event(
            session_id,
            EVENT_CHECKPOINT_CREATED,
            {"checkpoint_id": f"ckpt_{index:02d}", "label": f"Checkpoint {index:02d}", "provider": "fake"},
        )
    cfg = _cfg(tmp_path, monkeypatch, worker_token=worker_token)

    async def run() -> tuple[dict[str, Any], list[str]]:
        worker_runner = web.AppRunner(make_worker_app(worker_cfg))
        await worker_runner.setup()
        worker_site = web.TCPSite(worker_runner, "localhost", 0)
        await worker_site.start()
        worker_socket = worker_site._server.sockets[0]  # type: ignore[union-attr]  # noqa: SLF001
        worker_base = f"http://localhost:{worker_socket.getsockname()[1]}"
        workers_path = Path(cfg.orchestration.workers_path)
        workers = json.loads(workers_path.read_text())
        workers["workers"][0]["base_url"] = worker_base
        workers_path.write_text(json.dumps(workers))
        calls_seen: list[str] = []
        try:
            with httpx.Client(timeout=10) as worker_client:
                def worker_get(url: str, **kwargs):  # noqa: ANN001, ANN202
                    calls_seen.append(url)
                    return worker_client.get(url, **kwargs)

                async def snapshot(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
                    response = await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})
                    assert response.status_code == 200
                    return response.json()

                body = await _with_server(cfg, snapshot, http_get=worker_get)
            return body, calls_seen
        finally:
            await worker_runner.cleanup()

    body, calls_seen = asyncio.run(run())

    assert len(body["sessions"]) == 20
    assert len(body["checkpoints"]) == 20
    assert sum(url.endswith("/sessions/checkpoints") for url in calls_seen) == 1
    assert not any("/sessions/sess_" in url and url.endswith("/checkpoints") for url in calls_seen)


@pytest.mark.parametrize("bulk_failure", ["timeout", "http_503", "invalid_payload"])
def test_cockpit_checkpoint_aggregation_does_not_fan_out_after_bulk_failure(  # noqa: ANN001
    tmp_path, monkeypatch, bulk_failure
) -> None:
    cfg = _cfg(tmp_path, monkeypatch)
    calls_seen: list[str] = []

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        calls_seen.append(url)
        if url.endswith("/health"):
            return Response({"ok": True, "agent": "codex", "supported_engines": ["codex"]})
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response(
                {
                    "sessions": [
                        {
                            "session_id": f"sess_{index}",
                            "provider": "codex",
                            "engine": "codex",
                            "status": "completed",
                            "title": f"Historical session {index}",
                        }
                        for index in range(20)
                    ]
                }
            )
        if url.endswith("/sessions/requests"):
            return Response({"requests": []})
        if url.endswith("/sessions/checkpoints"):
            if bulk_failure == "timeout":
                raise TimeoutError("worker checkpoint endpoint timed out")
            if bulk_failure == "http_503":
                return Response({}, status_code=503)
            return Response({"checkpoints": {}})
        if "/sessions/sess_" in url and url.endswith("/checkpoints"):
            raise AssertionError("bulk transport failure must not fan out per historical session")
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})

        assert response.status_code == 200
        assert len(response.json()["sessions"]) == 20
        assert sum(url.endswith("/sessions/checkpoints") for url in calls_seen) == 1
        assert not any("/sessions/sess_" in url and url.endswith("/checkpoints") for url in calls_seen)

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


@pytest.mark.parametrize(
    ("scenario", "expected_kind", "expected_status", "expected_error_type"),
    [
        ("timeout", "transport_error", 0, "TimeoutError"),
        ("http_503", "http_error", 503, ""),
        ("invalid_json", "invalid_payload", 0, ""),
        ("non_object", "invalid_payload", 0, ""),
        ("wrong_container", "invalid_payload", 0, ""),
        ("invalid_entry", "invalid_payload", 0, ""),
    ],
)
def test_worker_bulk_checkpoint_failure_diagnostics_are_bounded_and_redacted(  # noqa: ANN001
    monkeypatch, caplog, scenario, expected_kind, expected_status, expected_error_type
) -> None:
    cockpit_module._bulk_checkpoint_next_warning_at.clear()  # noqa: SLF001
    times = iter([0.0, 1.0, 61.0])
    monkeypatch.setattr(cockpit_module.time, "monotonic", lambda: next(times))

    def failed_get(*_args, **_kwargs):  # noqa: ANN001
        if scenario == "timeout":
            raise TimeoutError("secret-worker.example checkpoint request timed out")
        if scenario == "http_503":
            return Response({}, status_code=503)
        if scenario == "invalid_json":
            return TextResponse("not-json", status_code=200)
        if scenario == "non_object":
            return Response([])  # type: ignore[arg-type]
        if scenario == "wrong_container":
            return Response({"checkpoints": {}})
        return Response({"checkpoints": [{"checkpoint_id": "ok"}, "invalid"]})  # type: ignore[list-item]

    with caplog.at_level("WARNING", logger=cockpit_module.__name__):
        results = [
            cockpit_module._worker_bulk_checkpoints(  # noqa: SLF001
                "worker-safe-id", "https://private-worker.example", {"Authorization": "secret"}, 1.0, failed_get
            )
            for _ in range(3)
        ]

    records = [record for record in caplog.records if record.message.startswith("worker bulk checkpoints unavailable")]
    assert results == [[], [], []]
    assert len(records) == 2
    assert all(record.event == "worker_bulk_checkpoints_unavailable" for record in records)
    assert all(record.worker_id == "worker-safe-id" for record in records)
    assert all(record.failure_kind == expected_kind for record in records)
    assert all(record.status_code == expected_status for record in records)
    assert all(record.error_type == expected_error_type for record in records)
    assert all("private-worker.example" not in record.message for record in records)
    assert all("Authorization" not in record.message for record in records)


@pytest.mark.parametrize("status_code", [404, 405])
def test_worker_bulk_checkpoint_compatibility_fallback_is_explicit_and_silent(status_code, caplog) -> None:  # noqa: ANN001
    cockpit_module._bulk_checkpoint_next_warning_at.clear()  # noqa: SLF001

    with caplog.at_level("WARNING", logger=cockpit_module.__name__):
        result = cockpit_module._worker_bulk_checkpoints(  # noqa: SLF001
            "worker-safe-id",
            "https://private-worker.example",
            {},
            1.0,
            lambda *_args, **_kwargs: Response({}, status_code=status_code),
        )

    assert result is None
    assert not [record for record in caplog.records if record.message.startswith("worker bulk checkpoints unavailable")]


def test_cockpit_session_detail_returns_not_found_for_stale_worker_only_ref(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    ref = make_session_ref("macbook-worker", "sess_worker_only")

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/health"):
            return Response({"ok": True, "agent": "codex", "supported_engines": ["codex"]})
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response(
                {
                    "sessions": [
                        {
                            "session_id": "sess_worker_only",
                            "provider": "codex",
                            "engine": "codex",
                            "status": "running",
                            "repo": "roughcoder/jarvis",
                            "title": "Worker-only session",
                        }
                    ]
                }
            )
        if url.endswith("/sessions/requests") or url.endswith("/sessions/checkpoints"):
            return Response({"requests": [], "checkpoints": []})
        if url.endswith("/sessions/sess_worker_only"):
            return Response({"error": "gone"}, status_code=404)
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()
        assert snapshot["sessions"][0]["session_ref"] == ref

        response = await client.get(f"{base}/v1/sessions/{ref}")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_archive_worker_only_session_hides_it_from_worker_views(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    ref = make_session_ref("macbook-worker", "sess_worker_only")

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/health"):
            return Response({"ok": True, "agent": "codex", "supported_engines": ["codex"]})
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response(
                {
                    "sessions": [
                        {
                            "session_id": "sess_worker_only",
                            "provider": "codex",
                            "engine": "codex",
                            "status": "running",
                            "repo": "roughcoder/jarvis",
                            "branch": "jarvis/worker-only",
                            "title": "Worker-only session",
                            "created_at": "2026-07-01T11:00:00Z",
                            "updated_at": "2026-07-01T12:00:00Z",
                        }
                    ]
                }
            )
        if url.endswith("/sessions/requests"):
            return Response({"requests": []})
        if url.endswith("/sessions/checkpoints"):
            return Response({"checkpoints": []})
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        before = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()
        response = await client.post(f"{base}/v1/sessions/{ref}/archive", json={"idempotency_key": "archive_worker_only"})
        replay = await client.post(f"{base}/v1/sessions/{ref}/archive", json={"idempotency_key": "archive_worker_only"})
        after = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

        assert before["sessions"][0]["session_ref"] == ref
        assert response.status_code == 200
        assert response.json()["session"]["archived_at"]
        assert replay.json()["idempotent"] is True
        assert after["sessions"] == []

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_turn_attachments_validated_and_proxied(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.turn,worker.job.start,worker.session.create,forge.github.branch.push")
    cfg.orchestration.turn_attachment_max_count = 2
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")
    posts: list[dict[str, Any]] = []

    def post(url: str, **kwargs) -> Response:  # noqa: ANN001
        posts.append({"url": url, "json": kwargs.get("json")})
        return Response({"ok": True, "session": {"session_id": "sess_123", "status": "running"}, "events": []})

    attachment = {"kind": "image", "mime_type": "image/png", "name": "screenshot.png", "data_url": "data:image/png;base64,cG5n"}

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        turn = await client.post(f"{base}/v1/sessions/{ref}/turns", json={"idempotency_key": "turn_attach", "prompt": "see this", "attachments": [attachment]})
        assert turn.status_code == 200
        assert posts[-1]["json"]["attachments"] == [attachment]

        bad_mime = {**attachment, "mime_type": "image/tiff", "data_url": "data:image/tiff;base64,cG5n"}
        rejected_mime = await client.post(f"{base}/v1/sessions/{ref}/turns", json={"idempotency_key": "turn_tiff", "prompt": "x", "attachments": [bad_mime]})
        assert rejected_mime.status_code == 400
        assert rejected_mime.json()["error"]["code"] == "validation_failed"
        assert "mime_type" in rejected_mime.json()["error"]["message"]

        too_many = await client.post(
            f"{base}/v1/sessions/{ref}/turns",
            json={"idempotency_key": "turn_many", "prompt": "x", "attachments": [attachment, attachment, attachment]},
        )
        assert too_many.status_code == 400
        assert "ORCHESTRATION_TURN_ATTACHMENT_MAX_COUNT" in too_many.json()["error"]["message"]

        cfg.orchestration.turn_attachment_max_bytes = 2
        oversize = await client.post(f"{base}/v1/sessions/{ref}/turns", json={"idempotency_key": "turn_big", "prompt": "x", "attachments": [attachment]})
        assert oversize.status_code == 400
        assert "ORCHESTRATION_TURN_ATTACHMENT_MAX_BYTES" in oversize.json()["error"]["message"]
        assert oversize.json()["error"]["recoverable"] is True
        cfg.orchestration.turn_attachment_max_bytes = 5 * 1024 * 1024

        bad_start = await client.post(
            f"{base}/v1/work/start",
            json={"idempotency_key": "start_attach", "source": "manual", "repo": "roughcoder/jarvis", "phrase": "start", "attachments": [{"kind": "file"}]},
        )
        assert bad_start.status_code == 400
        assert bad_start.json()["error"]["code"] == "validation_failed"

        # Only the valid first turn reached the worker boundary.
        assert len(posts) == 1

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_post=post))


def test_cockpit_session_control_endpoints_proxy_with_action_capabilities(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    caps = ",".join(
        [
            "worker.session.input",
            "worker.session.approve",
            "worker.session.interrupt",
            "worker.session.stop",
            "worker.session.restore",
        ]
    )
    cfg = _cfg(tmp_path, monkeypatch, caps=caps)
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")
    calls_seen: list[tuple[str, str]] = []
    expected = {
        "input": ("worker.session.input", "/sessions/sess_123/input"),
        "approval": ("worker.session.approve", "/sessions/sess_123/approval"),
        "interrupt": ("worker.session.interrupt", "/sessions/sess_123/interrupt"),
        "stop": ("worker.session.stop", "/sessions/sess_123/stop"),
        "checkpoints/restore": ("worker.session.restore", "/sessions/sess_123/checkpoints/restore"),
    }

    def post(url: str, **kwargs) -> Response:  # noqa: ANN001
        action = kwargs["json"]["metadata"]["action"]
        required, path = expected[action]
        assert url.endswith(path)
        assert required in kwargs["json"]["allowed_actions"]
        calls_seen.append((action, required))
        return Response({"ok": True, "event": {"event_id": f"ev_{action}", "session_id": "sess_123", "type": f"{action}.accepted"}})

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        for action in expected:
            response = await client.post(
                f"{base}/v1/sessions/{ref}/{action}",
                json={"idempotency_key": f"key_{action}", "metadata": {"action": action}},
            )
            body = response.json()
            assert response.status_code == 200
            assert body["ok"] is True
            assert body["session"]["pending_approval_count"] == 1

        assert calls_seen == [(action, expected[action][0]) for action in expected]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_post=post))


def test_cockpit_session_write_rejects_missing_capability(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    def post(_url: str, **_kwargs) -> Response:  # noqa: ANN001
        raise AssertionError("worker write should not be called without local authority")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/stop", json={"idempotency_key": "stop_1"})
        body = response.json()

        assert response.status_code == 403
        assert body["ok"] is False
        assert body["error"]["code"] == "forbidden"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_post=post))


def test_cockpit_session_write_maps_worker_errors(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.restore")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    def post(_url: str, **_kwargs) -> Response:  # noqa: ANN001
        return Response({"ok": False, "error": "no such checkpoint: ckpt_missing"}, status_code=404)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{base}/v1/sessions/{ref}/checkpoints/restore",
            json={"idempotency_key": "restore_missing", "checkpoint_id": "ckpt_missing"},
        )
        body = response.json()

        assert response.status_code == 409
        assert body["ok"] is False
        assert body["error"]["code"] == "checkpoint_not_found"
        assert body["error"]["recoverable"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_post=post))


def test_cockpit_session_write_maps_no_pending_codex_request(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.approve")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    def post(_url: str, **_kwargs) -> Response:  # noqa: ANN001
        return Response({"ok": False, "error": "no pending codex approval request req_stale"}, status_code=400)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/approval", json={"idempotency_key": "approve_stale", "request_id": "req_stale"})
        body = response.json()

        assert response.status_code == 409
        assert body["error"]["code"] == "request_not_pending"
        assert body["error"]["recoverable"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_post=post))


def test_cockpit_session_write_maps_worker_auth_failure_to_worker_unavailable(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.stop")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    def post(_url: str, **_kwargs) -> Response:  # noqa: ANN001
        return Response({"error": "unauthorized"}, status_code=401)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/stop", json={"idempotency_key": "stop_worker_unauthorized"})
        body = response.json()

        assert response.status_code == 502
        assert body["error"]["code"] == "worker_unavailable"
        assert body["error"]["recoverable"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_post=post))


def test_cockpit_session_write_maps_non_json_worker_errors(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.stop")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    def post(_url: str, **_kwargs) -> TextResponse:  # noqa: ANN001
        return TextResponse("failed at /workspace/private/log with sk-abcdefghijklmnopqrstuvwxyz", status_code=502)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/stop", json={"idempotency_key": "stop_text_error"})
        body = response.json()

        assert response.status_code == 502
        assert body["error"]["code"] == "worker_unavailable"
        assert "/workspace/" not in body["error"]["message"]
        assert "sk-abcdefghijklmnopqrstuvwxyz" not in body["error"]["message"]
        assert "<local-path>" in body["error"]["message"]
        assert "<redacted-token>" in body["error"]["message"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_post=post))


def test_cockpit_session_write_rejects_invalid_success_worker_response(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.stop")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    def post(_url: str, **_kwargs) -> TextResponse:  # noqa: ANN001
        return TextResponse("not json", status_code=200)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/stop", json={"idempotency_key": "stop_invalid_success"})
        body = response.json()

        assert response.status_code == 502
        assert body["error"]["code"] == "worker_unavailable"
        assert body["error"]["recoverable"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id), http_post=post))


def test_cockpit_work_start_rejects_unknown_sources_without_github_fallback(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{base}/v1/work/start",
            json={
                "idempotency_key": "voice_1",
                "source": "voice",
                "repo": "roughcoder/jarvis",
                "phrase": "next work",
            },
        )
        body = response.json()

        assert response.status_code == 400
        assert body["ok"] is False
        assert body["error"]["code"] == "validation_failed"
        assert body["error"]["recoverable"] is True
        assert "voice" in body["error"]["message"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))


def test_cockpit_work_start_caches_side_effecting_failures(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push")
    body = {"idempotency_key": "missing_repo_once", "source": "manual", "phrase": "Start work without repo"}

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        first = await client.post(f"{base}/v1/work/start", json=body)
        second = await client.post(f"{base}/v1/work/start", json=body)
        runs = OrchestrationStore(cfg.orchestration.workspace).list_runs()

        assert first.status_code == 400
        assert first.json()["error"]["code"] == "validation_failed"
        assert second.status_code == 200
        assert second.json()["ok"] is False
        assert second.json()["idempotent"] is True
        assert len(runs) == 1
        assert runs[0].phase == "needs_human"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))


def test_cockpit_work_start_normalizes_nested_parallel_strategy(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push")
    store = OrchestrationStore(cfg.orchestration.workspace)
    item = WorkItem(source="manual", id="manual_parallel", title="Parallel start", repo="roughcoder/jarvis")
    run = store.create_run("Parallel start", work_items=[item])
    session = WorkerSessionLink(worker_id="macbook-worker", session_id="sess_parallel", status="running", provider="codex", engine="codex")
    store.link_session(run.run_id, session)
    strategies_seen: list[str] = []

    def next_work(_self, command, *, start: bool = False):  # noqa: ANN001, FBT001, FBT002
        strategies_seen.append(command.engine_strategy)
        return StartedWork(
            item=item,
            worker=WorkerProfile(worker_id="macbook-worker", display_name="MacBook Pro"),
            envelope=ExecutionEnvelope(run_id=run.run_id, repo=item.repo, prompt=item.title, worker_id="macbook-worker", session_id=session.session_id),
            session=session,
        )

    monkeypatch.setattr("jarvis.orchestration.service.OrchestrationService.next_work", next_work)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{base}/v1/work/start",
            json={
                "idempotency_key": "nested_parallel",
                "command": {"operation": "start_next_work", "source": "manual", "engine_strategy": "parallel"},
                "work_item": {"id": "manual_parallel", "title": "Parallel start", "repo": "roughcoder/jarvis"},
            },
        )

        assert response.status_code == 200
        assert strategies_seen == ["ensemble"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run.run_id)))


def test_cockpit_work_start_manual_dispatches_worker_session(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    caps = "worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push"
    cfg = _cfg(tmp_path, monkeypatch, caps=caps)
    post_calls: list[str] = []

    def executor_post(url: str, **kwargs) -> Response:  # noqa: ANN001
        post_calls.append(url)
        if url.endswith("/sessions"):
            session = {
                "session_id": kwargs["json"]["session_id"],
                "status": "created",
                "provider": kwargs["json"]["provider"],
                "engine": kwargs["json"]["engine"],
                "branch": "jarvis/manual",
                "cwd": "/Users/example/private/jarvis",
            }
            return Response({"ok": True, "session": session, "event": {"event_id": "ev_create"}})
        if url.endswith("/turns"):
            return Response({"ok": True, "events": [{"event_id": "ev_turn", "type": "turn.started", "session_id": "sess_manual"}]})
        raise AssertionError(url)

    monkeypatch.setattr("jarvis.orchestration.executor.httpx.post", executor_post)
    monkeypatch.setattr("jarvis.orchestration.workers.WorkerRegistry._probe", lambda _self, profile: profile)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{base}/v1/work/start",
            json={
                "idempotency_key": "manual_1",
                "source": "manual",
                "repo": "roughcoder/jarvis",
                "phrase": "Build a cockpit smoke",
                "worker_id": "macbook-worker",
                "engine": "codex",
            },
        )
        body = response.json()

        assert response.status_code == 200
        assert body["ok"] is True
        assert body["run"]["repo"] == "roughcoder/jarvis"
        assert body["session"]["session_ref"].startswith("sessref_")
        assert [url.rsplit("/", 1)[-1] for url in post_calls] == ["sessions", "turns"]
        # The synchronous create/provision and first-turn events from dispatch land in the run log.
        store = OrchestrationStore(cfg.orchestration.workspace)
        run_id = body["run"]["run_id"]
        persisted_ids = [e.data.get("event_id") for e in store.events(run_id) if isinstance(e.data, dict) and e.data.get("event_id")]
        assert persisted_ids == ["ev_create", "ev_turn"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))


def test_cockpit_work_start_links_child_to_parent_chat_in_snapshot(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    caps = "worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push"
    cfg = _cfg(tmp_path, monkeypatch, caps=caps)

    def executor_post(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions"):
            session = {
                "session_id": kwargs["json"]["session_id"],
                "status": "created",
                "provider": kwargs["json"]["provider"],
                "engine": kwargs["json"]["engine"],
                "branch": "jarvis/manual-child",
                "cwd": "/Users/example/private/jarvis",
            }
            return Response({"ok": True, "session": session})
        if url.endswith("/turns"):
            return Response({"ok": True, "events": []})
        raise AssertionError(url)

    monkeypatch.setattr("jarvis.orchestration.executor.httpx.post", executor_post)
    monkeypatch.setattr("jarvis.orchestration.workers.WorkerRegistry._probe", lambda _self, profile: profile)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{base}/v1/work/start",
            json={
                "idempotency_key": "manual_child_1",
                "source": "manual",
                "repo": "roughcoder/jarvis",
                "phrase": "Build a child task",
                "parent_chat_id": "thread_parent",
            },
        )
        body = response.json()
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot")).json()

        assert response.status_code == 200
        assert body["run"]["parent_chat_id"] == "thread_parent"
        assert body["session"]["parent_chat_id"] == "thread_parent"
        assert snapshot["runs"][0]["parent_chat_id"] == "thread_parent"
        assert snapshot["sessions"][0]["parent_chat_id"] == "thread_parent"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))


def test_cockpit_work_start_records_linked_project_activity_and_skips_unlinked(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    store = OrchestrationStore(cfg.orchestration.workspace)
    item = WorkItem(source="manual", id="manual_activity", title="Activity dispatch", repo="roughcoder/jarvis")
    run = store.create_run("Activity dispatch", work_items=[item])
    session = WorkerSessionLink(worker_id="macbook-worker", session_id="sess_activity", status="running", provider="codex", engine="codex")
    store.link_session(run.run_id, session)

    def next_work(_self, _command, *, start: bool = False):  # noqa: ANN001, FBT001, FBT002
        return StartedWork(
            item=item,
            worker=WorkerProfile(worker_id="macbook-worker", display_name="MacBook Pro"),
            envelope=ExecutionEnvelope(run_id=run.run_id, repo=item.repo, prompt=item.title, worker_id="macbook-worker", session_id=session.session_id),
            session=session,
        )

    monkeypatch.setattr("jarvis.orchestration.service.OrchestrationService.next_work", next_work)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        unlinked = await client.post(f"{base}/v1/work/start", json={"source": "manual", "repo": "roughcoder/jarvis", "phrase": "unlinked"})
        empty = (await client.get(f"{base}/v1/projects/neil-shared/activity", params={"type": "work.dispatched"})).json()
        linked = await client.post(
            f"{base}/v1/work/start",
            json={"source": "manual", "repo": "roughcoder/jarvis", "phrase": "linked", "project_id": "neil-shared"},
        )
        activity = (await client.get(f"{base}/v1/projects/neil-shared/activity", params={"type": "work.dispatched"})).json()["activity"]

        assert unlinked.status_code == 200
        assert linked.status_code == 200
        assert empty["activity"] == []
        assert [item["type"] for item in activity] == ["work.dispatched"]
        assert activity[0]["data"]["run_id"] == run.run_id
        assert activity[0]["data"]["session_id"] == "sess_activity"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run.run_id)))


def test_cockpit_work_start_projects_run_and_session_linkage_survives_reload(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.cockpit import aggregate_sessions, run_detail_projection, run_summary, session_summary

    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    created_run_ids: list[str] = []
    seen_project_ids: list[str] = []

    def next_work(_self, command, *, start: bool = False):  # noqa: ANN001, FBT001, FBT002
        project_id = str(command.filters.get("project_id") or "")
        seen_project_ids.append(project_id)
        engine = str(command.target_engine_id or "codex")
        item = WorkItem(
            source="manual",
            id=f"manual_{len(created_run_ids)}",
            title="Projected dispatch",
            repo=str(command.filters.get("repo") or "roughcoder/jarvis"),
        )
        store = OrchestrationStore(cfg.orchestration.workspace)
        run = store.create_run(item.title, work_items=[item], project_id=project_id, engine=engine)
        session = WorkerSessionLink(
            worker_id="macbook-worker",
            session_id=f"sess_projected_{len(created_run_ids)}",
            status="running",
            provider=engine,
            engine=engine,
            project_id=project_id,
            branch="jarvis/projected-dispatch",
        )
        store.link_session(run.run_id, session)
        created_run_ids.append(run.run_id)
        return StartedWork(
            item=item,
            worker=WorkerProfile(worker_id="macbook-worker", display_name="MacBook Pro"),
            envelope=ExecutionEnvelope(
                run_id=run.run_id,
                repo=item.repo,
                prompt=item.title,
                worker_id="macbook-worker",
                engine=engine,
                project_id=project_id,
                session_id=session.session_id,
            ),
            session=session,
        )

    monkeypatch.setattr("jarvis.orchestration.service.OrchestrationService.next_work", next_work)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        linked = await client.post(
            f"{base}/v1/work/start",
            json={
                "source": "manual",
                "repo": "roughcoder/jarvis",
                "phrase": "linked",
                "project_id": "neil-shared",
                "engine": "claude",
            },
        )
        legacy = await client.post(
            f"{base}/v1/work/start",
            json={"source": "manual", "repo": "roughcoder/jarvis", "phrase": "legacy"},
        )
        linked_body = linked.json()
        legacy_body = legacy.json()

        assert linked.status_code == 200, linked_body
        assert legacy.status_code == 200, legacy_body
        assert linked_body["run"]["project_id"] == "neil-shared"
        assert linked_body["run"]["engine"] == "claude"
        assert linked_body["session"]["project_id"] == "neil-shared"
        assert linked_body["session"]["engine"] == "claude"
        assert legacy_body["run"]["project_id"] is None
        assert legacy_body["run"]["engine"] == "codex"
        assert legacy_body["session"]["project_id"] is None
        assert legacy_body["session"]["engine"] == "codex"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))

    assert seen_project_ids == ["neil-shared", ""]
    reloaded = OrchestrationStore(cfg.orchestration.workspace)
    linked_run = reloaded.get(created_run_ids[0])
    legacy_run = reloaded.get(created_run_ids[1])
    assert linked_run is not None
    assert legacy_run is not None

    linked_sessions = aggregate_sessions(
        runs=[linked_run],
        worker_cfg=cfg.worker,
        workers_path=cfg.orchestration.workers_path,
        include_worker_state=False,
    )
    linked_session = session_summary(next(iter(linked_sessions.values())))
    linked_detail = run_detail_projection(linked_run)

    assert run_summary(linked_run)["project_id"] == "neil-shared"
    assert run_summary(linked_run)["engine"] == "claude"
    assert linked_detail["sessions"][0]["project_id"] == "neil-shared"
    assert linked_session["project_id"] == "neil-shared"
    assert linked_session["engine"] == "claude"
    assert run_summary(legacy_run)["project_id"] is None
    assert run_detail_projection(legacy_run)["sessions"][0]["project_id"] is None


def test_cockpit_work_start_idempotency_serializes_concurrent_dispatch(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push")
    store = OrchestrationStore(cfg.orchestration.workspace)
    item = WorkItem(source="manual", id="manual_concurrent", title="Concurrent start", repo="roughcoder/jarvis")
    run = store.create_run("Concurrent start", work_items=[item])
    session = WorkerSessionLink(worker_id="macbook-worker", session_id="sess_concurrent", status="running", provider="codex", engine="codex")
    store.link_session(run.run_id, session)
    calls_seen = {"count": 0}
    entered = threading.Event()

    def next_work(_self, _command, *, start: bool = False):  # noqa: ANN001, FBT001, FBT002
        calls_seen["count"] += 1
        entered.set()
        time.sleep(0.2)
        return StartedWork(
            item=item,
            worker=WorkerProfile(worker_id="macbook-worker", display_name="MacBook Pro"),
            envelope=ExecutionEnvelope(run_id=run.run_id, repo=item.repo, prompt=item.title, worker_id="macbook-worker", session_id=session.session_id),
            session=session,
        )

    monkeypatch.setattr("jarvis.orchestration.service.OrchestrationService.next_work", next_work)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        import asyncio

        body = {"idempotency_key": "concurrent_start", "source": "manual", "repo": "roughcoder/jarvis", "phrase": "start"}
        first = asyncio.create_task(client.post(f"{base}/v1/work/start", json=body))
        await asyncio.to_thread(entered.wait, 2)
        second = asyncio.create_task(client.post(f"{base}/v1/work/start", json=body))
        responses = await asyncio.gather(first, second)
        payloads = [response.json() for response in responses]

        assert all(response.status_code == 200 for response in responses)
        assert calls_seen["count"] == 1
        assert [payload.get("idempotent") for payload in payloads].count(True) == 1

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run.run_id)))


def test_cockpit_idempotency_scope_cleans_up_lock(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: Response({}),
        post=lambda *_args, **_kwargs: Response({}),
        store=OrchestrationStore(cfg.orchestration.workspace),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )

    async def run_scope() -> None:
        async with _idempotency_scope(ctx, "work.start", "same-key"):
            assert len(ctx.idempotency_locks) == 1
            assert len(ctx.idempotency_lock_refs) == 1

    import asyncio

    asyncio.run(run_scope())

    assert ctx.idempotency_locks == {}
    assert ctx.idempotency_lock_refs == {}


def test_cockpit_idempotency_store_treats_corrupt_or_expired_records_as_miss(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    store = IdempotencyStore(cfg.orchestration.workspace)
    body = {"idempotency_key": "key", "prompt": "continue"}

    corrupt_path = store._path("sessions/test/turns", "corrupt")  # noqa: SLF001
    corrupt_path.write_text("{not-json")
    assert store.get("sessions/test/turns", "corrupt", body) is None
    assert not corrupt_path.exists()

    expired_path = store._path("sessions/test/turns", "expired")  # noqa: SLF001
    expired_path.write_text(json.dumps({"created_at": 0, "fingerprint": "ignored", "response": {"ok": True}}))
    assert store.get("sessions/test/turns", "expired", body) is None
    assert not expired_path.exists()

    non_object_path = store._path("sessions/test/turns", "non-object")  # noqa: SLF001
    non_object_path.write_text("[]")
    assert store.get("sessions/test/turns", "non-object", body) is None
    assert not non_object_path.exists()


def test_cockpit_session_updates_are_keyed_by_worker_and_session(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    store = OrchestrationStore(cfg.orchestration.workspace)
    item = WorkItem(source="manual", id="manual_dup", title="Duplicate session ids", repo="roughcoder/jarvis")
    run = store.create_run("Duplicate session ids", work_items=[item])
    store.link_session(run.run_id, WorkerSessionLink(worker_id="worker-a", session_id="sess_dup", status="running"))
    store.link_session(run.run_id, WorkerSessionLink(worker_id="worker-b", session_id="sess_dup", status="running"))

    updated = store.update_session(run.run_id, "sess_dup", worker_id="worker-b", status="stopped")

    statuses = {(session.worker_id, session.session_id): session.status for session in updated.sessions}
    assert statuses[("worker-a", "sess_dup")] == "running"
    assert statuses[("worker-b", "sess_dup")] == "stopped"


def test_cockpit_session_archives_are_keyed_by_worker_and_session(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    store = OrchestrationStore(cfg.orchestration.workspace)
    item = WorkItem(source="manual", id="manual_archive_dup", title="Duplicate archive ids", repo="roughcoder/jarvis")
    run = store.create_run("Duplicate archive ids", work_items=[item])
    store.link_session(run.run_id, WorkerSessionLink(worker_id="worker-a", session_id="sess_dup", status="running"))
    store.link_session(run.run_id, WorkerSessionLink(worker_id="worker-b", session_id="sess_dup", status="running"))

    archived = store.archive_session(run.run_id, "sess_dup", worker_id="worker-b")

    archived_at = {(session.worker_id, session.session_id): session.archived_at for session in archived.sessions}
    assert archived_at[("worker-a", "sess_dup")] == ""
    assert archived_at[("worker-b", "sess_dup")]


def test_cockpit_work_resume_maps_active_session_error(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push")
    _store, run_id = _seed_run(cfg)

    def resume_run(_self, _run_id: str, *, prompt: str = ""):  # noqa: ANN001
        assert prompt == "continue"
        from jarvis.orchestration.service import ResumeRunError

        raise ResumeRunError("run already has active worker session sess_123")

    monkeypatch.setattr("jarvis.orchestration.service.OrchestrationService.resume_run", resume_run)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/work/resume", json={"idempotency_key": "resume_1", "run_id": run_id, "prompt": "continue"})
        body = response.json()

        assert response.status_code == 409
        assert body["ok"] is False
        assert body["error"]["code"] == "session_active"
        assert body["error"]["recoverable"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_catalog_exposes_start_options_and_defaults(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        return (await client.get(f"{base}/v1/cockpit/catalog")).json()

    import asyncio

    body = asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))
    options = body["start_options"]

    assert options["sources"] == ["manual", "github", "linear"]
    assert options["engine_strategies"] == ["single", "parallel"]
    assert options["landing_modes"] == ["branch_only", "draft_pr", "ready_pr", "confirm_before_pr"]
    assert "repo (unless a default repo is configured)" in options["required_fields"]["manual"]
    assert options["required_fields"]["linear"] == ["repo (unless a default repo is configured)"]
    assert options["defaults"]["worker_id"] == "macbook-worker"
    assert options["defaults"]["engine"] == "codex"
    assert options["defaults"]["landing_mode"] == "branch_only"
    assert options["defaults"]["source"] == "manual"


def test_cockpit_worker_repositories_projection_marks_default_repo() -> None:
    from jarvis.orchestration.cockpit import project_worker_profile

    profile = WorkerProfile(
        worker_id="macbook-worker",
        display_name="MacBook Pro",
        git_identity={
            "provider": "github",
            "connected": True,
            "authenticated": True,
            "auth_fresh": True,
            "login": "octocat",
            "detail": "gh user probe succeeded",
        },
        repo_access=[
            {
                "repo": "roughcoder/jarvis",
                "accessible": True,
                "public": False,
                "reason_code": "accessible",
                "reason": "Worker GitHub identity can read this repo.",
                "checked_at": 1751371200,
                "ttl_s": 300,
            }
        ],
        repositories=[
            {"repo": "jarvis", "default_branch": "main", "status": "ready"},
            {"repo": "polymarket", "default_branch": "develop", "status": "cloning"},
            {"name": "legacy-name-key"},
            {"no_repo": True},
        ],
    )

    projected = project_worker_profile(profile, default_repo="roughcoder/jarvis")
    rows = projected["repositories"]

    assert rows == [
        {"repo": "jarvis", "status": "ready", "default_branch": "main", "is_default": True, "can_start_work": True},
        {"repo": "polymarket", "status": "cloning", "default_branch": "develop", "is_default": False, "can_start_work": False},
        {"repo": "legacy-name-key", "status": "ready", "default_branch": "", "is_default": False, "can_start_work": True},
    ]
    assert projected["git_identity"]["login"] == "octocat"
    assert projected["git_identity"]["connected"] is True
    assert projected["repo_access"][0]["repo"] == "roughcoder/jarvis"
    assert projected["repo_access"][0]["reason_code"] == "accessible"


def test_cockpit_workers_probe_surfaces_health_published_repositories(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/health"):
            return Response(
                {
                    "ok": True,
                    "agent": "codex",
                    "supported_engines": ["codex", "claude"],
                    "repositories": [{"repo": "jarvis", "default_branch": "main", "status": "ready"}],
                    "diagnostics": {
                        "engines": [
                            {
                                "engine": "codex",
                                "installed": True,
                                "authenticated": True,
                                "version": "codex 1.2.3",
                                "detail": "~/.codex/auth.json present",
                            }
                        ],
                        "package_managers": [{"name": "uv", "available": True}],
                        "browser": {
                            "available": False,
                            "nodriver_installed": True,
                            "chrome_found": False,
                            "detail": "/Applications/Google Chrome.app missing",
                        },
                        "repositories": [{"repo": "jarvis", "default_branch": "main", "status": "ready"}],
                    },
                }
            )
        return _fake_get("")(url, **kwargs)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        return (await client.get(f"{base}/v1/workers", params={"probe": "true"})).json()

    import asyncio

    body = asyncio.run(_with_server(cfg, calls, http_get=get))
    worker = body["workers"][0]

    assert worker["repositories"] == [
        {"repo": "jarvis", "status": "ready", "default_branch": "main", "is_default": False, "can_start_work": True}
    ]
    assert worker["readiness"]["engines"][0]["installed"] is True
    assert worker["readiness"]["engines"][0]["authenticated"] is True
    assert worker["readiness"]["package_managers"] == [{"name": "uv", "available": True}]
    assert worker["readiness"]["browser"]["available"] is False
    assert "~/.codex" not in json.dumps(worker["readiness"])
    assert "/Users/neil" not in json.dumps(worker["readiness"])
    assert "/Applications/" not in json.dumps(worker["readiness"])


def test_cockpit_work_validate_reports_selection_without_creating_a_run(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    caps = "worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push"
    cfg = _cfg(tmp_path, monkeypatch, caps=caps)
    monkeypatch.setattr("jarvis.orchestration.workers.WorkerRegistry._probe", lambda _self, profile: profile)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        response = await client.post(
            f"{base}/v1/work/validate",
            json={"source": "manual", "repo": "roughcoder/jarvis", "phrase": "Build a cockpit smoke", "worker_id": "macbook-worker", "engine": "codex"},
        )
        assert response.status_code == 200
        return response.json()

    import asyncio

    body = asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))
    validation = body["validation"]

    assert body["ok"] is True
    assert validation["can_start"] is True
    assert validation["worker_id"] == "macbook-worker"
    assert validation["engine"] == "codex"
    assert validation["repo"] == "roughcoder/jarvis"
    assert validation["compatibility"]["repo"] == "roughcoder/jarvis"
    assert validation["compatibility"]["selected_worker_id"] == "macbook-worker"
    assert validation["compatibility"]["workers"] == [
        {
            "worker_id": "macbook-worker",
            "eligible": True,
            "reasons": ["selected"],
            "reason_codes": ["selected"],
            "repo_access": {
                "repo": "roughcoder/jarvis",
                "accessible": True,
                "public": False,
                "reason_code": "accessible",
                "checked_at": None,
                "cached": False,
            },
        }
    ]
    assert validation["compatibility"]["warnings"] == []
    assert validation["reason_codes"] == []
    assert validation["warnings"] == []
    assert validation["warning_codes"] == []
    assert validation["missing"] == []
    assert validation["missing_authority"] == []
    assert OrchestrationStore(cfg.orchestration.workspace).list_runs() == []


def test_cockpit_work_validate_reports_worker_repo_compatibility(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    caps = "worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push"
    cfg = _cfg(tmp_path, monkeypatch, caps=caps)
    workers_path = Path(cfg.orchestration.workers_path)
    workers_path.write_text(
        json.dumps(
            {
                "workers": [
                    {
                        "worker_id": "macbook-worker",
                        "display_name": "MacBook Pro",
                        "base_url": "http://worker-a.test",
                        "capabilities": ["git", "codex"],
                        "status": "online",
                        "supported_engines": ["codex"],
                        "repo_access": [{"repo": "roughcoder/jarvis", "accessible": True, "reason_code": "accessible"}],
                        "repositories": [{"repo": "jarvis", "status": "ready", "default_branch": "main"}],
                    },
                    {
                        "worker_id": "hive-worker",
                        "display_name": "Hive",
                        "base_url": "http://worker-b.test",
                        "capabilities": ["git", "codex"],
                        "status": "online",
                        "supported_engines": ["codex"],
                        "repo_access": [{"repo": "roughcoder/jarvis", "accessible": True, "reason_code": "accessible"}],
                        "repositories": [{"repo": "other", "status": "ready", "default_branch": "main"}],
                    },
                ]
            }
        )
    )
    monkeypatch.setattr("jarvis.orchestration.workers.WorkerRegistry._probe", lambda _self, profile: profile)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        response = await client.post(
            f"{base}/v1/work/validate",
            json={"source": "manual", "repo": "roughcoder/jarvis", "phrase": "Build a cockpit smoke"},
        )
        assert response.status_code == 200
        return response.json()

    import asyncio

    validation = asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))["validation"]

    assert validation["compatibility"] == {
        "repo": "roughcoder/jarvis",
            "selected_worker_id": "macbook-worker",
            "warnings": [],
            "workers": [
                {
                    "worker_id": "macbook-worker",
                    "eligible": True,
                    "reasons": ["selected"],
                    "reason_codes": ["selected"],
                    "repo_access": {
                        "repo": "roughcoder/jarvis",
                        "accessible": True,
                        "public": False,
                        "reason_code": "accessible",
                        "checked_at": None,
                        "cached": False,
                    },
                },
                {
                    "worker_id": "hive-worker",
                    "eligible": True,
                    "reasons": ["eligible", "repo not checked out"],
                    "reason_codes": ["eligible", "repo-not-warm"],
                    "repo_access": {
                        "repo": "roughcoder/jarvis",
                        "accessible": True,
                        "public": False,
                        "reason_code": "accessible",
                        "checked_at": None,
                        "cached": False,
                    },
                },
            ],
        }


def test_cockpit_work_validate_flags_missing_repo_without_side_effects(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    caps = "worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push"
    cfg = _cfg(tmp_path, monkeypatch, caps=caps)
    monkeypatch.setattr("jarvis.orchestration.workers.WorkerRegistry._probe", lambda _self, profile: profile)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        response = await client.post(f"{base}/v1/work/validate", json={"source": "manual", "phrase": "Start work without repo"})
        assert response.status_code == 200
        return response.json()

    import asyncio

    validation = asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))["validation"]

    assert validation["can_start"] is False
    assert validation["missing"] == ["repo"]
    assert any("no repo/default repo" in reason for reason in validation["reasons"])
    # Unlike /v1/work/start, validation must not create a needs_human run.
    assert OrchestrationStore(cfg.orchestration.workspace).list_runs() == []


def test_cockpit_work_validate_reports_missing_authority(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.job.start")
    monkeypatch.setattr("jarvis.orchestration.workers.WorkerRegistry._probe", lambda _self, profile: profile)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        response = await client.post(f"{base}/v1/work/validate", json={"source": "manual", "repo": "roughcoder/jarvis", "phrase": "x"})
        assert response.status_code == 200
        return response.json()

    import asyncio

    validation = asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))["validation"]

    assert validation["can_start"] is False
    assert "worker.session.create" in validation["missing_authority"]
    assert "forge.github.branch.push" in validation["missing_authority"]


def test_cockpit_run_summary_lifecycle_reason_fields(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.cockpit import run_summary
    from jarvis.orchestration.models import OrchestrationRun

    failed = OrchestrationRun(run_id="run_f", objective="Fail", phase="failed", terminal_reason="Worker dispatch failed: boom")
    blocked = OrchestrationRun(run_id="run_b", objective="Blocked", phase="needs_human", terminal_reason="Work item has no repo")
    running = OrchestrationRun(run_id="run_r", objective="Run", phase="running")

    failed_row = run_summary(failed)
    blocked_row = run_summary(blocked)
    running_row = run_summary(running)

    assert failed_row["last_error"] == "Worker dispatch failed: boom"
    assert failed_row["state_reason"] == "Worker dispatch failed: boom"
    assert failed_row["blocked_reason"] is None
    assert blocked_row["blocked_reason"] == "Work item has no repo"
    assert blocked_row["waiting_on"] == ["human"]
    assert blocked_row["last_error"] is None
    assert running_row["state_reason"] == "Worker sessions active"
    assert running_row["blocked_reason"] is None
    assert running_row["waiting_on"] == []


def test_cockpit_session_event_type_aliases_are_normalized() -> None:
    from jarvis.orchestration.cockpit import canonical_event_type, project_session_event

    event = project_session_event(
        {"event_id": "ev_1", "session_id": "sess_1", "type": "provider.thread.ready", "time": "2026-07-01T11:00:00Z", "data": {}},
        worker_id="macbook-worker",
        run_id="run_1",
        sequence=1,
    )

    assert event["type"] == "provider.session.ready"
    assert canonical_event_type("assistant.delta") == "assistant.delta"
    assert canonical_event_type("") == ""


def test_cockpit_sse_snapshot_delta_events(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.api import _snapshot_delta_events

    previous = {
        "cursor": "evt_a",
        "runs": [{"run_id": "run_1", "status": "active", "phase": "running"}],
        "sessions": [{"session_ref": "sessref_x", "run_id": "run_1", "status": "running"}],
        "workers": [{"worker_id": "w1", "status": "online", "last_seen_at": "old"}],
        "artifacts": [{"artifact_id": "artifact_1", "run_id": "run_1"}],
    }
    current = {
        "cursor": "evt_b",
        "runs": [{"run_id": "run_1", "status": "active", "phase": "verifying"}],
        "sessions": [{"session_ref": "sessref_x", "run_id": "run_1", "status": "running"}],
        "workers": [{"worker_id": "w1", "status": "online", "last_seen_at": "new"}],
        "artifacts": [{"artifact_id": "artifact_2", "run_id": "run_1"}],
    }

    events = _snapshot_delta_events(previous, current)

    assert events is not None
    types = sorted(event["type"] for event in events)
    assert types == ["artifact.removed", "artifact.upserted", "run.updated"]
    run_event = next(event for event in events if event["type"] == "run.updated")
    assert run_event["run_id"] == "run_1"
    assert run_event["cursor"] == "evt_b"
    assert run_event["payload"]["phase"] == "verifying"

    # No baseline or a disappearing run/session forces a snapshot instead.
    assert _snapshot_delta_events(None, current) is None
    assert _snapshot_delta_events(current, {"cursor": "evt_c", "runs": [], "sessions": [], "workers": [], "artifacts": []}) is None


def test_supervisor_sync_persists_session_events_once(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.supervisor import sync_run_sessions

    cfg = _cfg(tmp_path, monkeypatch)
    store = OrchestrationStore(cfg.orchestration.workspace)
    item = WorkItem(source="manual", id="manual_sync", title="Sync events", repo="roughcoder/jarvis")
    run = store.create_run("Sync events", work_items=[item])
    store.link_session(run.run_id, WorkerSessionLink(worker_id="macbook-worker", session_id="sess_123", status="running"))

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions/sess_123"):
            return Response({"session_id": "sess_123", "status": "running", "provider": "codex", "engine": "codex"})
        if url.endswith("/sessions/sess_123/events"):
            return Response(
                {
                    "events": [
                        {"event_id": "ev_1", "session_id": "sess_123", "type": "turn.started", "time": "t1", "data": {"turn_id": "turn_1"}},
                        {"event_id": "ev_2", "session_id": "sess_123", "type": "assistant.delta", "time": "t2", "data": {"turn_id": "turn_1", "delta": "hi"}},
                    ]
                }
            )
        raise AssertionError(url)

    sync_run_sessions(store, worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, run_id=run.run_id, get=get)
    sync_run_sessions(store, worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, run_id=run.run_id, get=get)

    persisted = [event for event in store.events(run.run_id) if isinstance(event.data, dict) and event.data.get("event_id")]
    assert [event.data["event_id"] for event in persisted] == ["ev_1", "ev_2"]
    assert persisted[1].type == "assistant.delta"
    assert persisted[1].data["data"]["delta"] == "hi"


def test_supervisor_sync_honors_hub_timeout_and_worker_skip(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.supervisor import sync_run_sessions

    cfg = _cfg(tmp_path, monkeypatch)
    store = OrchestrationStore(cfg.orchestration.workspace)
    item = WorkItem(source="manual", id="manual_sync_timeout", title="Sync timeout", repo="roughcoder/jarvis")
    run = store.create_run("Sync timeout", work_items=[item])
    store.link_session(run.run_id, WorkerSessionLink(worker_id="macbook-worker", session_id="sess_timeout", status="running"))
    calls: list[float] = []

    def get(url: str, **kwargs) -> Response:  # noqa: ANN001
        calls.append(kwargs["timeout"])
        if url.endswith("/sessions/sess_timeout"):
            return Response({"session_id": "sess_timeout", "status": "running"})
        if url.endswith("/sessions/sess_timeout/events"):
            return Response({"events": []})
        raise AssertionError(url)

    skipped = sync_run_sessions(
        store,
        worker_cfg=cfg.worker,
        workers_path=cfg.orchestration.workers_path,
        run_id=run.run_id,
        get=get,
        timeout_s=4.0,
        should_sync_worker=lambda _profile: False,
    )
    assert skipped.errors == []
    assert calls == []

    sync_run_sessions(
        store,
        worker_cfg=cfg.worker,
        workers_path=cfg.orchestration.workers_path,
        run_id=run.run_id,
        get=get,
        timeout_s=4.0,
    )
    assert calls == [4.0, 4.0]


def test_cockpit_snapshot_uses_precomputed_sync_state(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.cockpit import cockpit_snapshot

    cfg = _cfg(tmp_path, monkeypatch)
    store, _run_id = _seed_run(cfg)

    def fail_sync(**_kwargs):  # noqa: ANN001, ANN202
        raise AssertionError("precomputed sync must bypass sync_state")

    monkeypatch.setattr("jarvis.orchestration.cockpit.sync_state", fail_sync)
    snapshot = cockpit_snapshot(
        store=store,
        worker_cfg=cfg.worker,
        workers_path=cfg.orchestration.workers_path,
        sync_mode="fast",
        sync={"mode": "none", "status": "stale", "synced_at": "", "errors": []},
    )

    assert snapshot["sync"]["status"] == "stale"


def test_cockpit_snapshot_fast_includes_requests_and_checkpoints(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        return (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

    import asyncio

    body = asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))

    assert {request["request_id"] for request in body["requests"]} == {"req_approval", "req_input"}
    assert body["checkpoints"][0]["checkpoint_id"] == "ckpt_1"
    # Store-only snapshots stay worker-free: the arrays exist but are empty.
    empty = asyncio.run(_with_server(cfg, lambda base, client: client.get(f"{base}/v1/cockpit/snapshot"), http_get=_fake_get(run_id)))
    assert empty.json()["requests"] == []
    assert empty.json()["checkpoints"] == []


def test_cockpit_sse_delta_events_cover_requests_and_checkpoints(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.api import _snapshot_delta_events

    base = {"cursor": "evt_a", "runs": [], "sessions": [], "workers": [], "artifacts": []}
    previous = {
        **base,
        "requests": [{"request_id": "req_1", "session_ref": "sessref_x", "run_id": "run_1", "kind": "approval", "status": "pending"}],
        "checkpoints": [{"checkpoint_id": "ckpt_1", "session_ref": "sessref_x", "run_id": "run_1"}],
    }
    current = {
        **base,
        "cursor": "evt_b",
        "requests": [{"request_id": "req_2", "session_ref": "sessref_x", "run_id": "run_1", "kind": "input", "status": "pending"}],
        "checkpoints": [{"checkpoint_id": "ckpt_1", "session_ref": "sessref_x", "run_id": "run_1"}],
    }

    events = _snapshot_delta_events(previous, current)

    assert events is not None
    by_type = {}
    for event in events:
        by_type.setdefault(event["type"], []).append(event)
    closed = [event for event in by_type["request.updated"] if event["payload"].get("status") == "closed"]
    opened = [event for event in by_type["request.updated"] if event["payload"].get("status") == "pending"]
    assert closed[0]["request_id"] == "req_1"
    assert closed[0]["payload"]["session_ref"] == "sessref_x"
    assert opened[0]["request_id"] == "req_2"
    assert opened[0]["session_ref"] == "sessref_x"

    # A checkpoint disappearing (restore/cleanup) forces a snapshot instead.
    gone = {**current, "cursor": "evt_c", "checkpoints": []}
    assert _snapshot_delta_events(current, gone) is None


def test_cockpit_sse_hub_emits_session_event_frames_from_store(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    store, run_id = _seed_run(cfg)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: Response({}),
        post=lambda *_args, **_kwargs: Response({}),
        store=store,
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )
    hub = SseSnapshotHub(ctx)
    body = {"cursor": "evt_tick", "runs": [{"run_id": run_id}]}
    hub._event_counts["none"] = hub._prime_event_counts(body)  # noqa: SLF001

    store.append_event(
        run_id,
        "assistant.delta",
        "",
        {"session_id": "sess_123", "event_id": "ev_live", "turn_id": "turn_9", "message_id": "", "time": "t9", "data": {"turn_id": "turn_9", "delta": "hello"}},
    )
    frames = hub._collect_session_event_frames("none", body)  # noqa: SLF001

    assert len(frames) == 1
    frame = frames[0]
    assert frame["type"] == "session.event"
    assert frame["run_id"] == run_id
    assert frame["session_ref"].startswith("sessref_")
    assert frame["payload"]["type"] == "assistant.delta"
    assert frame["payload"]["event_id"] == "ev_live"
    assert frame["payload"]["data"]["delta"] == "hello"
    assert frame["worker_id"] == "macbook-worker"  # so ?worker_id= filters apply
    # Counts advanced: a second collect with no new events emits nothing.
    assert hub._collect_session_event_frames("none", body) == []  # noqa: SLF001

    # Internal bookkeeping records that mention a session (no worker event_id)
    # must not be streamed as per-turn timeline entries.
    store.append_event(run_id, "session_updated", "sync bookkeeping", {"session_id": "sess_123"})
    assert hub._collect_session_event_frames("none", body) == []  # noqa: SLF001


def test_cockpit_sse_frame_filters_match_envelope_or_payload() -> None:
    from jarvis.orchestration.api import _frame_matches

    run_frame = {"type": "run.updated", "run_id": "run_1", "payload": {"run_id": "run_1"}}
    session_frame = {"type": "session.updated", "run_id": "run_1", "session_ref": "sessref_x", "payload": {"worker_id": "w1", "session_ref": "sessref_x"}}

    assert _frame_matches(run_frame, {}) is True
    assert _frame_matches(run_frame, {"run_id": "run_1"}) is True
    assert _frame_matches(run_frame, {"run_id": "run_2"}) is False
    assert _frame_matches(session_frame, {"worker_id": "w1"}) is True
    assert _frame_matches(session_frame, {"session_ref": "sessref_x", "run_id": "run_1"}) is True
    # Frames that do not carry the filtered id are dropped, not passed through.
    assert _frame_matches(run_frame, {"worker_id": "w1"}) is False


def test_cockpit_work_start_maps_capacity_to_worker_capacity_exceeded(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    caps = "worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push"
    cfg = _cfg(tmp_path, monkeypatch, caps=caps)
    workers_path = Path(cfg.orchestration.workers_path)
    data = json.loads(workers_path.read_text())
    data["workers"][0]["current_jobs"] = 4  # max_concurrent_jobs is 4 -> no free slot
    workers_path.write_text(json.dumps(data))
    monkeypatch.setattr("jarvis.orchestration.workers.WorkerRegistry._probe", lambda _self, profile: profile)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{base}/v1/work/start",
            json={"idempotency_key": "capacity_1", "source": "manual", "repo": "roughcoder/jarvis", "phrase": "start"},
        )
        body = response.json()

        assert response.status_code == 409
        assert body["error"]["code"] == "worker_capacity_exceeded"
        assert body["error"]["recoverable"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))


def test_cockpit_work_validate_peeks_source_work_item(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    caps = "work.github.issues.read,worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push"
    cfg = _cfg(tmp_path, monkeypatch, caps=caps)
    monkeypatch.setattr("jarvis.orchestration.workers.WorkerRegistry._probe", lambda _self, profile: profile)

    class FakeSource:
        def __init__(self, item):  # noqa: ANN001
            self.item = item

        def list(self, *, repo: str = "", filters=None, limit: int = 10):  # noqa: ANN001
            return [self.item] if self.item else []

        def next(self, *, repo: str = "", filters=None):  # noqa: ANN001
            return self.item

    issue = WorkItem(source="github", id="#12", title="Fix flaky wake word test", repo="roughcoder/jarvis")

    import asyncio

    async def run_case(source) -> dict[str, Any]:  # noqa: ANN001
        runner = web.AppRunner(make_app(cfg, http_get=_fake_get(""), source_factory=lambda name, _cfg=None: source))
        await runner.setup()
        site = web.TCPSite(runner, "localhost", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr, attr-defined]  # noqa: SLF001
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(f"http://localhost:{port}/v1/work/validate", json={"source": "github", "phrase": "next work"})
                return response.json()
        finally:
            await runner.cleanup()

    found = asyncio.run(run_case(FakeSource(issue)))
    empty = asyncio.run(run_case(FakeSource(None)))

    assert found["validation"]["can_start"] is True
    assert found["validation"]["work_item"] == {"source": "github", "id": "#12", "title": "Fix flaky wake word test", "repo": "roughcoder/jarvis", "kind": "issue"}
    assert found["validation"]["repo"] == "roughcoder/jarvis"
    assert empty["validation"]["can_start"] is False
    assert "no eligible work item found in the source" in empty["validation"]["reasons"]
    assert OrchestrationStore(cfg.orchestration.workspace).list_runs() == []


def test_cockpit_verification_artifacts_project_first_class_fields() -> None:
    from jarvis.orchestration.cockpit import project_artifact
    from jarvis.orchestration.models import OrchestrationRun

    run = OrchestrationRun(run_id="run_v", objective="Verify")
    artifact = Artifact(
        type="verification",
        id="verify_1",
        status="passed",
        summary="187 passed",
        command="pytest -q",
        started_at="2026-07-04T11:55:00Z",
        completed_at="2026-07-04T12:00:00Z",
    )

    row = project_artifact(artifact, run)

    assert row["kind"] == "verification"
    assert row["summary"] == "187 passed"
    assert row["command"] == "pytest -q"
    assert row["started_at"] == "2026-07-04T11:55:00Z"
    assert row["completed_at"] == "2026-07-04T12:00:00Z"
    # Non-verification artifacts keep the base shape.
    assert "command" not in project_artifact(Artifact(type="branch", name="jarvis/foo"), run)


def test_cockpit_worker_last_seen_and_per_worker_default_repo(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.cockpit import project_worker_profile, worker_headers
    from jarvis.orchestration.workers import WorkerRegistry

    env = tmp_path / ".env"
    env.write_text("MACBOOK_WORKER_TOKEN=cockpit-token # remote worker\n")
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env))
    profile = WorkerProfile(
        worker_id="macbook-worker",
        display_name="MacBook Pro",
        token_env="MACBOOK_WORKER_TOKEN",
        default_repo="polymarket",
        repositories=[{"repo": "jarvis"}, {"repo": "polymarket"}],
    )
    rows = project_worker_profile(profile, default_repo="roughcoder/jarvis")["repositories"]
    assert [(row["repo"], row["is_default"]) for row in rows] == [("jarvis", False), ("polymarket", True)]
    assert worker_headers(WorkerConfig(_env_file=None), profile) == {"Authorization": "Bearer cockpit-token"}

    cfg = _cfg(tmp_path, monkeypatch)
    registry = WorkerRegistry(cfg.worker, profiles_path=cfg.orchestration.workers_path, http_get=_fake_get(""))
    probed = registry.profiles(probe=True)[0]
    assert probed.status == "online"
    assert probed.last_seen_at  # stamped at probe time, not synthesized at projection
    assert project_worker_profile(probed)["last_seen_at"] == probed.last_seen_at


def test_cockpit_sse_first_tick_delivers_session_event_after_subscribe(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    cfg.orchestration.sse_refresh_interval_s = 0.05
    store, run_id = _seed_run(cfg)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: Response({}),
        post=lambda *_args, **_kwargs: Response({}),
        store=store,
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )

    async def run_hub() -> dict[str, Any]:
        import asyncio

        hub = SseSnapshotHub(ctx)
        await hub.start()
        try:
            subscription = await hub.subscribe("none")
            # An event lands between subscribe and the first refresh tick.
            store.append_event(
                run_id,
                "assistant.message",
                "",
                {"session_id": "sess_123", "event_id": "ev_first", "turn_id": "turn_1", "message_id": "msg_1", "time": "t1", "data": {"text": "done"}},
            )
            run = store.get(run_id)
            assert run is not None
            run.phase = "verifying"
            store.save(run)
            return await asyncio.wait_for(subscription.queue.get(), timeout=2)
        finally:
            await hub.stop()

    import asyncio

    event = asyncio.run(run_hub())

    assert event is not None
    frames = [frame for frame in event["events"] or [] if frame["type"] == "session.event"]
    assert len(frames) == 1
    assert frames[0]["payload"]["event_id"] == "ev_first"


def test_cockpit_sse_delivers_child_terminal_parent_event(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    cfg.orchestration.sse_refresh_interval_s = 0.05
    store = OrchestrationStore(cfg.orchestration.workspace)
    parent = store.create_run("Parent orchestrator")
    child = store.create_run("Child work", parent_chat_id=parent.run_id)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: Response({}),
        post=lambda *_args, **_kwargs: Response({}),
        store=store,
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )

    async def run_hub() -> dict[str, Any]:
        import asyncio

        hub = SseSnapshotHub(ctx)
        await hub.start()
        try:
            subscription = await hub.subscribe("none")
            store.set_phase(child.run_id, "completed", "done")
            return await asyncio.wait_for(subscription.queue.get(), timeout=2)
        finally:
            await hub.stop()

    import asyncio

    event = asyncio.run(run_hub())
    frames = [frame for frame in event["events"] or [] if frame["type"] == "run.event"]

    assert len(frames) == 1
    assert frames[0]["run_id"] == parent.run_id
    assert frames[0]["payload"]["type"] == "child_terminal"
    assert frames[0]["payload"]["data"]["child_chat_id"] == child.run_id


def test_cockpit_work_start_capacity_uses_probed_worker_state(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    caps = "worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push"
    cfg = _cfg(tmp_path, monkeypatch, caps=caps)
    # The static profile claims free slots; only the live probe reveals a full worker.
    workers_path = Path(cfg.orchestration.workers_path)
    data = json.loads(workers_path.read_text())
    assert data["workers"][0]["current_jobs"] == 1

    def probe(_self, profile):  # noqa: ANN001
        profile.status = "online"
        profile.current_jobs = profile.max_concurrent_jobs
        profile.repo_access = [{"repo": "roughcoder/jarvis", "accessible": True, "reason_code": "accessible"}]
        return profile

    monkeypatch.setattr("jarvis.orchestration.workers.WorkerRegistry._probe", probe)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"{base}/v1/work/start",
            json={"idempotency_key": "probed_capacity", "source": "manual", "repo": "roughcoder/jarvis", "phrase": "start"},
        )

        assert response.status_code == 409
        assert response.json()["error"]["code"] == "worker_capacity_exceeded"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))


def test_cockpit_worker_last_seen_at_is_empty_without_probe(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.cockpit import worker_profiles

    cfg = _cfg(tmp_path, monkeypatch)
    rows = worker_profiles(worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, probe=False)

    # The static profile says online, but nothing has actually seen the worker.
    assert rows[0]["status"] == "online"
    assert rows[0]["last_seen_at"] == ""


def test_cockpit_snapshot_hides_requests_for_archived_sessions(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    store, run_id = _seed_run(cfg)
    store.archive_session(run_id, "sess_123", worker_id="macbook-worker")

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        return (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

    import asyncio

    body = asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))

    # The worker still reports pending requests for sess_123, but the session
    # is archived locally so they must not leak into the snapshot.
    assert body["requests"] == []
    assert all(session["session_id"] != "sess_123" for session in body["sessions"])


def test_cockpit_work_validate_reports_already_owned_items(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    caps = "worker.job.start,worker.session.create,worker.session.turn,forge.github.branch.push"
    cfg = _cfg(tmp_path, monkeypatch, caps=caps)
    monkeypatch.setattr("jarvis.orchestration.workers.WorkerRegistry._probe", lambda _self, profile: profile)
    store = OrchestrationStore(cfg.orchestration.workspace)
    owned = WorkItem(source="manual", id="manual_owned", title="Already running", repo="roughcoder/jarvis")
    run = store.create_run("Already running", work_items=[owned])

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        response = await client.post(
            f"{base}/v1/work/validate",
            json={
                "source": "manual",
                "repo": "roughcoder/jarvis",
                "work_item": {"id": "manual_owned", "title": "Already running", "repo": "roughcoder/jarvis"},
            },
        )
        assert response.status_code == 200
        return response.json()

    import asyncio

    validation = asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))["validation"]

    assert validation["can_start"] is False
    assert validation["owned_by_run_id"] == run.run_id
    assert any("already owned" in reason for reason in validation["reasons"])
    # Validation stayed read-only: the owning run is still the only one.
    assert [existing.run_id for existing in store.list_runs()] == [run.run_id]


def test_cockpit_unarchive_session_restores_it_to_views(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        archived = await client.post(f"{base}/v1/sessions/{ref}/archive", json={"idempotency_key": "arch_1"})
        hidden = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()
        restored = await client.post(f"{base}/v1/sessions/{ref}/unarchive", json={"idempotency_key": "unarch_1"})
        replay = await client.post(f"{base}/v1/sessions/{ref}/unarchive", json={"idempotency_key": "unarch_1"})
        visible = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()
        detail = (await client.get(f"{base}/v1/sessions/{ref}")).json()

        assert archived.status_code == 200
        assert hidden["sessions"] == []
        assert restored.status_code == 200
        assert restored.json()["ok"] is True
        assert restored.json()["session"]["archived_at"] is None
        assert replay.json()["idempotent"] is True
        assert visible["sessions"][0]["session_ref"] == ref
        assert visible["sessions"][0]["archived_at"] is None
        assert detail["session"]["archived_at"] is None

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_unarchive_unknown_session_returns_not_found(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_never_seen")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        response = await client.post(f"{base}/v1/sessions/{ref}/unarchive", json={"idempotency_key": "unarch_missing"})

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_worker_only_session_has_null_run_id_and_ended_reason(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    ref = make_session_ref("macbook-worker", "sess_worker_only")

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/health"):
            return Response({"ok": True, "agent": "codex", "supported_engines": ["codex"]})
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response(
                {
                    "sessions": [
                        {
                            "session_id": "sess_worker_only",
                            "provider": "codex",
                            "engine": "codex",
                            "status": "interrupted",
                            "repo": "roughcoder/jarvis",
                            "title": "Worker-only session",
                            "metadata": {"ended_reason": "worker_lost"},
                        }
                    ]
                }
            )
        if url.endswith("/sessions/requests"):
            return Response({"requests": []})
        if url.endswith("/sessions/checkpoints"):
            return Response({"checkpoints": []})
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

        row = snapshot["sessions"][0]
        assert row["session_ref"] == ref
        assert row["run_id"] is None
        assert row["ended_reason"] == "worker_lost"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_session_summary_derives_ended_reason_from_status(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.cockpit import session_summary as summary

    def row(status: str, ended_reason: str = "") -> dict:
        return {
            "session_ref": make_session_ref("macbook-worker", "sess_x"),
            "worker_id": "macbook-worker",
            "session_id": "sess_x",
            "status": status,
            "ended_reason": ended_reason,
        }

    assert summary(row("running"))["ended_reason"] is None
    assert summary(row("completed"))["ended_reason"] == "completed"
    assert summary(row("done"))["ended_reason"] == "completed"
    assert summary(row("stopped"))["ended_reason"] == "stopped"
    assert summary(row("failed"))["ended_reason"] == "engine_error"
    assert summary(row("interrupted"))["ended_reason"] is None
    assert summary(row("interrupted", "worker_lost"))["ended_reason"] == "worker_lost"
    assert summary(row("interrupted", "interrupted_by_user"))["ended_reason"] == "interrupted_by_user"


def test_cockpit_thread_turn_attachments_reach_gateway_without_persisting_payload(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway(["I can see the screenshot."])
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)
    attachment = {"kind": "image", "mime_type": "image/png", "name": "bug.png", "data_url": "data:image/png;base64,cG5n"}

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Bug hunt"})
        thread = opened.json()["thread"]
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Here is the bug", "attachments": [attachment]},
        )
        detail = await client.get(f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}")

        assert response.status_code == 200
        assert [event["_event"] for event in _sse_events(response.text)][-1] == "thread.turn.done"
        user_message = gateway.messages[-1][-1]
        assert user_message["role"] == "user"
        assert user_message["content"][0] == {"type": "text", "text": "Here is the bug"}
        assert user_message["content"][1] == {"type": "image_url", "image_url": {"url": attachment["data_url"]}}
        messages = detail.json()["thread"]["messages"]
        assert messages[0]["content"] == "Here is the bug\n[image attached: bug.png (image/png)]"
        assert "base64,cG5n" not in json.dumps(messages)

        rejected = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "bad", "attachments": [{"kind": "image", "mime_type": "image/tiff", "data_url": "data:image/tiff;base64,cG5n"}]},
        )
        assert rejected.status_code == 400
        assert rejected.json()["error"]["code"] == "validation_failed"

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert memory.messages[0]["content"] == "Here is the bug\n[image attached: bug.png (image/png)]"


def test_cockpit_opens_explicit_code_agent_orchestrator_thread(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway([]), tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = await client.post(
            f"{base}/v1/projects/neil-shared/threads",
            json={
                "title": "Review",
                "chat_type": "orchestrator",
                "engine": "codex",
                "model": "gpt-5.5",
                "worker_id": "brain-worker",
            },
        )

        assert opened.status_code == 200
        thread = opened.json()["thread"]
        assert thread["chat_type"] == "orchestrator"
        assert thread["engine"] == "codex"
        assert thread["model"] == "gpt-5.5"
        assert thread["worker_id"] == "brain-worker"
        assert thread["host"] == ""

    asyncio.run(_with_server(cfg, calls))


def test_orchestrator_turn_wait_pages_past_old_worker_events(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    requests: list[str] = []
    old_events = [
        {
            "event_id": f"event_{index}",
            "type": "assistant.delta",
            "data": {"turn_id": "turn_old", "text": "old"},
        }
        for index in range(500)
    ]

    def get_events(_worker_id: str, path: str) -> dict:
        requests.append(path)
        if "after=event_499" in path:
            return {
                "events": [
                    {
                        "event_id": "event_500",
                        "type": "assistant.message",
                        "data": {"turn_id": "turn_current", "text": "review complete"},
                    },
                    {
                        "event_id": "event_501",
                        "type": "turn.completed",
                        "data": {"turn_id": "turn_current"},
                    },
                ]
            }
        return {"events": old_events}

    monkeypatch.setattr(connector, "_get_worker_json", get_events)

    result = asyncio.run(
        connector._wait_for_orchestrator_turn(  # noqa: SLF001
            "worker_a",
            "session_a",
            "turn_current",
        )
    )

    assert result == "review complete"
    assert requests == [
        "/sessions/session_a/events?limit=500",
        "/sessions/session_a/events?limit=500&after=event_499",
    ]


def test_orchestrator_poll_failure_preserves_session_for_reconciliation(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    thread = CockpitThread(
        thread_id="thread_orchestrator_failure",
        project_id=project.id,
        session_id=orchestrator_session_id(project.id, "thread_orchestrator_failure"),
        title="Review",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="codex",
        worker_id="worker_a",
        workspace={
            "worker_id": "worker_a",
            "session_id": "orch_thread_orchestrator_failure",
            "provider_started": False,
            "status": "ready",
        },
    )
    connector.index.save(thread)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    posted: list[dict[str, Any]] = []

    async def ensure(*_args, **_kwargs):  # noqa: ANN002, ANN003
        return thread

    async def fail_wait(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise RuntimeError("transient worker poll failed")

    monkeypatch.setattr(connector, "_ensure_orchestrator_session", ensure)
    def post(_worker_id: str, _path: str, body: dict[str, Any]) -> dict[str, Any]:
        posted.append(body)
        return {"ok": True}

    monkeypatch.setattr(connector, "_post_worker_json", post)
    monkeypatch.setattr(connector, "_wait_for_orchestrator_turn", fail_wait)

    with pytest.raises(RuntimeError, match="transient worker poll failed"):
        asyncio.run(connector._orchestrator_turn(project, thread, requester, "review", progress=None))  # noqa: SLF001

    failed = connector.index.get(project.id, thread.thread_id)
    assert failed is not None
    assert failed.workspace["status"] == "failed"
    assert failed.workspace["provision_phase"] == "failed"
    assert failed.workspace["session_id"] == "orch_thread_orchestrator_failure"
    assert failed.workspace["provider_started"] is True
    assert "session_generation" not in failed.workspace
    assert posted[0]["runtime_context"]["orchestrator_mcp"]["timeout_s"] > cfg.worker.request_timeout_s + 5


def test_orchestrator_explicit_worker_does_not_fall_back(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None)
    thread = CockpitThread(
        thread_id="thread_strict_worker",
        project_id=project.id,
        session_id=orchestrator_session_id(project.id, "thread_strict_worker"),
        title="Review",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="claude",
        worker_id="requested-worker",
    )
    fallback = WorkerProfile(
        worker_id="fallback-worker",
        display_name="Fallback",
        base_url="http://fallback.test",
        status="online",
        supported_engines=["claude"],
        max_concurrent_jobs=2,
    )
    registry = type("Registry", (), {"choose": lambda *_args, **_kwargs: fallback})()
    monkeypatch.setattr(connector, "_registry", lambda: registry)

    with pytest.raises(RuntimeError, match="requested worker"):
        asyncio.run(connector._ensure_orchestrator_session(project, thread, RequestContext("mac", "neil", "personal", frozenset()), progress=None))  # noqa: SLF001


def test_failed_orchestrator_session_is_recreated_for_retry(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None)
    thread = CockpitThread(
        thread_id="thread_retry",
        project_id=project.id,
        session_id=orchestrator_session_id(project.id, "thread_retry"),
        title="Review",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="codex",
        worker_id="worker_a",
        workspace={
            "worker_id": "worker_a",
            "session_id": "orch_thread_retry",
            "provider_started": True,
            "status": "running",
        },
    )
    connector.index.save(thread)
    profile = WorkerProfile(
        worker_id="worker_a",
        display_name="Worker A",
        base_url="http://worker.test",
        status="online",
        supported_engines=["codex"],
        max_concurrent_jobs=2,
    )
    registry = type("Registry", (), {"choose": lambda *_args, **_kwargs: profile})()
    posts: list[str] = []

    def post(_worker_id: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        posts.append(path)
        if path == "/conversation-workspaces":
            return {"ok": True, "workspace": {"root": str(tmp_path / "conversation")}}
        assert path == "/sessions"
        return {"ok": True, "session": {"session_id": body["session_id"]}}

    monkeypatch.setattr(connector, "_registry", lambda: registry)
    monkeypatch.setattr(connector, "_get_worker_json", lambda *_args, **_kwargs: {"status": "failed"})
    monkeypatch.setattr(connector, "_post_worker_json", post)

    retried = asyncio.run(
        connector._ensure_orchestrator_session(  # noqa: SLF001
            project,
            thread,
            RequestContext("mac", "neil", "personal", frozenset()),
            progress=None,
        )
    )

    assert posts == ["/conversation-workspaces", "/sessions"]
    assert retried.workspace["session_id"] == "orch_thread-retry_1"
    assert retried.workspace["provider_started"] is False
    assert retried.workspace["status"] == "ready"


def test_cockpit_thread_projection_keeps_conversation_open_after_turn(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    gateway = FakeGateway(["Done thinking."])
    connector = CockpitConnector(cfg, memory=memory, gateway=gateway, tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = (await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Enrichment"})).json()["thread"]

        assert opened["engine"] == "jarvis"
        assert opened["model"] == str(cfg.gateway.voice_model or cfg.gateway.fast_model)
        assert opened["worker_id"] is None
        assert opened["host"]
        assert opened["conversation_id"] == opened["thread_id"]
        assert opened["lifecycle"] == "open"
        assert opened["operational_state"] == "idle"
        assert opened["status"] == "created"
        assert opened["ended_reason"] is None

        turn = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}/turns",
            json={"text": "Think about it"},
        )
        done = [event for event in _sse_events(turn.text) if event["_event"] == "thread.turn.done"][0]
        listed = (await client.get(f"{base}/v1/projects/neil-shared/threads")).json()["threads"]
        detail = (await client.get(f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}")).json()["thread"]

        assert done["payload"]["thread"]["lifecycle"] == "open"
        assert done["payload"]["thread"]["operational_state"] == "idle"
        assert done["payload"]["thread"]["status"] == "completed"
        assert done["payload"]["thread"]["ended_reason"] == "completed"
        assert listed[0]["engine"] == "jarvis"
        assert listed[0]["status"] == "completed"
        assert detail["status"] == "completed"
        assert detail["ended_reason"] == "completed"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_thread_operational_state_is_non_terminal() -> None:
    thread = CockpitThread(
        thread_id="thread_durable",
        project_id="jarvis",
        session_id="project:jarvis:orchestrator:thread_durable",
        title="Durable conversation",
        created_at="2026-07-12T12:00:00+00:00",
        updated_at="2026-07-12T12:00:00+00:00",
        created_by="operator",
    )

    assert cockpit_api_module._thread_operational_state(thread, None) == ("idle", "")  # noqa: SLF001
    assert cockpit_api_module._thread_status(thread, None) == ("created", "")  # noqa: SLF001
    archived = replace(thread, archived_at="2026-07-12T13:00:00+00:00")
    assert cockpit_api_module._thread_operational_state(archived, None) == ("archived", "")  # noqa: SLF001
    assert cockpit_api_module._thread_status(archived, None) == ("created", "")  # noqa: SLF001

    for legacy, expected in (
        ("created", "starting"),
        ("running", "working"),
        ("completed", "idle"),
        ("failed", "degraded"),
    ):
        ctx = SimpleNamespace(
            thread_turn_states={(thread.project_id, thread.thread_id): (legacy, "")}
        )
        assert cockpit_api_module._thread_operational_state(thread, ctx) == (expected, "")  # noqa: SLF001


def test_cockpit_thread_projection_clears_transient_degraded_state() -> None:
    thread = CockpitThread(
        thread_id="thread_recovered",
        project_id="jarvis",
        session_id="project:jarvis:orchestrator:thread_recovered",
        title="Recovered conversation",
        created_at="2026-07-12T12:00:00+00:00",
        updated_at="2026-07-12T12:00:00+00:00",
        created_by="operator",
        engine="codex",
        model="gpt-5.5",
        last_turn_at="2026-07-12T12:01:00+00:00",
    )
    state_key = (thread.project_id, thread.thread_id)
    ctx = SimpleNamespace(
        thread_turn_states={state_key: ("degraded", "engine_error")},
        thread_turn_legacy_states={},
    )

    assert cockpit_api_module._thread_operational_state(thread, ctx) == ("degraded", "engine_error")  # noqa: SLF001
    ctx.thread_turn_states.pop(state_key)

    projection = cockpit_api_module._thread_projection(thread, ctx)  # noqa: SLF001
    assert projection["operational_state"] == "idle"
    assert projection["diagnostic_reason"] is None
    assert projection["status"] == "completed"
    assert projection["ended_reason"] == "completed"


def test_cockpit_thread_projection_separates_legacy_failure_from_operational_state() -> None:
    thread = CockpitThread(
        thread_id="thread_retryable",
        project_id="jarvis",
        session_id="project:jarvis:orchestrator:thread_retryable",
        title="Retryable conversation",
        created_at="2026-07-12T12:00:00+00:00",
        updated_at="2026-07-12T12:01:00+00:00",
        created_by="operator",
        engine="codex",
        model="gpt-5.5",
        last_turn_at="2026-07-12T12:01:00+00:00",
    )
    state_key = (thread.project_id, thread.thread_id)
    ctx = SimpleNamespace(
        thread_turn_states={},
        thread_turn_legacy_states={state_key: ("failed", "engine_error")},
    )

    failed = cockpit_api_module._thread_projection(thread, ctx)  # noqa: SLF001
    assert failed["operational_state"] == "idle"
    assert failed["diagnostic_reason"] is None
    assert failed["status"] == "failed"
    assert failed["ended_reason"] == "engine_error"

    workspace_failed = replace(thread, workspace={"status": "failed"})
    ctx.thread_turn_legacy_states.clear()
    workspace_projection = cockpit_api_module._thread_projection(workspace_failed, ctx)  # noqa: SLF001
    assert workspace_projection["operational_state"] == "degraded"
    assert workspace_projection["diagnostic_reason"] == "engine_error"
    assert workspace_projection["status"] == "completed"
    assert workspace_projection["ended_reason"] == "completed"

    ctx.thread_turn_states[state_key] = ("working", "")
    assert cockpit_api_module._thread_status(thread, ctx) == ("running", "")  # noqa: SLF001

    ctx.thread_turn_states.pop(state_key)
    assert cockpit_api_module._thread_status(thread, ctx) == ("completed", "completed")  # noqa: SLF001


def test_cockpit_thread_turn_error_preserves_legacy_failure_without_degrading_conversation(  # noqa: ANN001
    tmp_path, monkeypatch
) -> None:
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        opened = (await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Retryable"})).json()[
            "thread"
        ]
        failed = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}/turns",
            json={"text": "Try once"},
        )
        detail = (await client.get(f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}"))
        return {"events": _sse_events(failed.text), "thread": detail.json()["thread"]}

    import asyncio

    result = asyncio.run(_with_server(cfg, calls))

    assert any(event["_event"] == "thread.turn.error" for event in result["events"])
    assert result["thread"]["operational_state"] == "idle"
    assert result["thread"]["diagnostic_reason"] is None
    assert result["thread"]["status"] == "failed"
    assert result["thread"]["ended_reason"] == "engine_error"


def test_cockpit_thread_rename_persists_and_records_activity(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    connector = CockpitConnector(cfg, memory=memory, gateway=FakeGateway([]), tts=None, tracer=None)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        opened = (await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Old name"})).json()["thread"]
        renamed = await client.patch(
            f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}",
            json={"title": "  New   name  ", "idempotency_key": "rename_1"},
        )
        listed = (await client.get(f"{base}/v1/projects/neil-shared/threads")).json()["threads"]
        detail = (await client.get(f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}")).json()["thread"]
        activity = (await client.get(f"{base}/v1/projects/neil-shared/activity")).json()
        missing_title = await client.patch(
            f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}",
            json={},
        )
        unknown = await client.patch(
            f"{base}/v1/projects/neil-shared/threads/thread_missing",
            json={"title": "X"},
        )

        assert renamed.status_code == 200
        assert renamed.json()["thread"]["title"] == "New name"
        assert listed[0]["title"] == "New name"
        assert detail["title"] == "New name"
        assert any(item.get("type") == "thread.renamed" for item in activity.get("activity", []))
        assert missing_title.status_code == 400
        assert unknown.status_code == 404

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_catalog_merges_worker_reported_engine_supports() -> None:
    from jarvis.orchestration.cockpit import cockpit_catalog

    catalog = cockpit_catalog(
        engines=["claude", "codex"],
        engine_supports={"claude": {"streaming": True, "attachments": True}},
    )

    rows = {row["engine"]: row for row in catalog["engines"]}
    assert rows["claude"]["supports"]["attachments"] is True
    assert rows["claude"]["supports"]["streaming"] is True
    assert rows["codex"]["supports"]["attachments"] is False


def test_cockpit_stored_session_link_keeps_ended_reason_offline(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    store, run_id = _seed_run(cfg)
    store.update_session(run_id, "sess_123", worker_id="macbook-worker", status="interrupted", ended_reason="worker_lost")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        # sync=none renders from the stored link only — no live worker call.
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "none"})).json()

        row = snapshot["sessions"][0]
        assert row["status"] == "interrupted"
        assert row["ended_reason"] == "worker_lost"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_unarchive_session_rejected_while_run_archived(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        archived_session = await client.post(f"{base}/v1/sessions/{ref}/archive", json={"idempotency_key": "arch_sess"})
        archived_run = await client.post(f"{base}/v1/runs/{run_id}/archive", json={"idempotency_key": "arch_run"})
        blocked = await client.post(f"{base}/v1/sessions/{ref}/unarchive", json={"idempotency_key": "unarch_blocked"})

        assert archived_session.status_code == 200
        assert archived_run.status_code == 200
        assert blocked.status_code == 409
        assert blocked.json()["error"]["code"] == "run_archived"
        assert blocked.json()["error"]["recoverable"] is True

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_child_watch_claims_exactly_once_after_every_expected_child_is_terminal(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    thread = CockpitThread(
        thread_id="thread_parent",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_parent",
        title="Review pull request",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
    )
    index.save(thread)
    requester = RequestContext(
        device_id="local-mac",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"orchestration.runs.read"}),
    )
    watch_id = index.register_child_watch(
        thread,
        ["run_a", "run_b"],
        requester=requester,
        continuation_instruction="Publish the reconciled review before reporting success.",
    )

    assert index.claim_ready_child_watch(thread.thread_id, {"run_a"}) is None
    claimed = index.claim_ready_child_watch(thread.thread_id, {"run_a", "run_b"})
    assert claimed is not None
    assert claimed["watch_id"] == watch_id
    assert claimed["requester"]["device_id"] == "local-mac"
    assert claimed["requester"]["capabilities"] == ["orchestration.runs.read"]
    assert claimed["continuation_instruction"] == "Publish the reconciled review before reporting success."
    assert index.claim_ready_child_watch(thread.thread_id, {"run_a", "run_b"}) is None


def test_child_watch_tool_enforces_optional_expected_count(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.connectors.cockpit import _watch_child_work_sessions_tool

    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    parent = CockpitThread(
        thread_id="thread_parent",
        project_id=project.id,
        session_id="project:neil-shared:orchestrator:thread_parent",
        title="Review pull request",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
    )
    child = OrchestrationStore(cfg.orchestration.workspace).create_run(
        "Claude review",
        parent_chat_id=parent.thread_id,
        project_id=project.id,
    )
    tool = _watch_child_work_sessions_tool(cfg, project, parent)
    requester = RequestContext(
        device_id="local-mac",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"orchestration.runs.read"}),
    )

    result = asyncio.run(
        tool.handler(
            requester,
            {"child_chat_ids": [child.run_id], "expected_count": 2},
        )
    )

    assert result == "error: expected 2 distinct child_chat_ids, received 1"


def test_pending_child_watch_accepts_revised_completion_instruction(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    parent = CockpitThread(
        thread_id="thread_parent",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_parent",
        title="Review pull request",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
    )
    index.save(parent)
    requester = RequestContext(
        device_id="local-mac",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"orchestration.runs.read"}),
    )

    first_watch_id = index.register_child_watch(parent, ["run_a", "run_b"], requester=requester)
    second_watch_id = index.register_child_watch(
        parent,
        ["run_a", "run_b"],
        requester=requester,
        continuation_instruction="Publish the reconciled review.",
    )
    claimed = index.claim_ready_child_watch(parent.thread_id, {"run_a", "run_b"})

    assert first_watch_id == second_watch_id
    assert claimed is not None
    assert claimed["continuation_instruction"] == "Publish the reconciled review."
    assert claimed["requester"]["capabilities"] == ["orchestration.runs.read"]


def test_pending_child_watch_rejects_instruction_update_from_different_requester(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    parent = CockpitThread(
        thread_id="thread_parent",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_parent",
        title="Review pull request",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
    )
    index.save(parent)
    privileged = RequestContext(
        device_id="local-mac",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"orchestration.runs.read", "forge.github.pr.comment"}),
    )
    read_only = RequestContext(
        device_id="shared-browser",
        identity="guest",
        scope="household",
        capabilities=frozenset({"orchestration.runs.read"}),
    )

    index.register_child_watch(
        parent,
        ["run_a", "run_b"],
        requester=privileged,
        continuation_instruction="Read and summarize the results.",
    )
    index.register_child_watch(
        parent,
        ["run_a", "run_b"],
        requester=read_only,
        continuation_instruction="Publish the review using the stored authority.",
    )
    claimed = index.claim_ready_child_watch(parent.thread_id, {"run_a", "run_b"})

    assert claimed is not None
    assert claimed["continuation_instruction"] == "Read and summarize the results."
    assert claimed["requester"]["identity"] == "neil"
    assert claimed["requester"]["capabilities"] == [
        "forge.github.pr.comment",
        "orchestration.runs.read",
    ]


def test_child_watch_continuation_reuses_exact_requester_authority(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    parent = CockpitThread(
        thread_id="thread_parent",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_parent",
        title="Review pull request",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
    )
    index.save(parent)
    requester = RequestContext(
        device_id="local-mac",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"orchestration.runs.read", "forge.github.pr.comment"}),
    )
    index.register_child_watch(
        parent,
        ["run_a", "run_b"],
        requester=requester,
        continuation_instruction="MUST publish one GitHub review before the resumed turn ends.",
    )
    watch = index.claim_ready_child_watch(parent.thread_id, {"run_a", "run_b"})
    assert watch is not None
    continued: list[tuple[RequestContext, str]] = []

    async def turn(_self, _project, _thread, resumed_requester, instruction):  # noqa: ANN001
        continued.append((resumed_requester, instruction))
        return "done", parent, []

    monkeypatch.setattr(CockpitConnector, "turn", turn)

    _continue_child_watch(cfg, parent.thread_id, watch)

    assert len(continued) == 1
    assert continued[0][0].device_id == "local-mac"
    assert continued[0][0].identity == "neil"
    assert continued[0][0].capabilities == requester.capabilities
    assert "MUST publish one GitHub review before the resumed turn ends." in continued[0][1]


def test_child_watch_continuation_waits_beyond_old_fixed_retry_window(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    parent = CockpitThread(
        thread_id="thread_parent",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_parent",
        title="Review pull request",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
    )
    index.save(parent)
    requester = RequestContext(
        device_id="local-mac",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"orchestration.runs.read"}),
        peer="neil",
    )
    index.register_child_watch(parent, ["run_a", "run_b"], requester=requester)
    watch = index.claim_ready_child_watch(parent.thread_id, {"run_a", "run_b"})
    assert watch is not None
    attempts = 0
    finished: list[str] = []
    original_finish = CockpitThreadIndex.finish_child_watch

    async def turn(_self, _project, _thread, _requester, _instruction):  # noqa: ANN001
        nonlocal attempts
        attempts += 1
        if attempts <= 120:
            raise RuntimeError("worker session already has an active turn")
        return "done", parent, []

    async def no_sleep(_seconds: float) -> None:
        return None

    def finish(self, parent_id: str, watch_id: str, *, error: str = "") -> None:  # noqa: ANN001
        finished.append(error)
        original_finish(self, parent_id, watch_id, error=error)

    monkeypatch.setattr(CockpitConnector, "turn", turn)
    monkeypatch.setattr("jarvis.connectors.cockpit.asyncio.sleep", no_sleep)
    monkeypatch.setattr(CockpitThreadIndex, "finish_child_watch", finish)

    _continue_child_watch(
        cfg,
        parent.thread_id,
        watch,
    )

    assert attempts == 121
    assert finished == [""]


def test_child_watch_continuation_stops_after_claim_completes(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    parent = CockpitThread(
        thread_id="thread_parent",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_parent",
        title="Review pull request",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
    )
    index.save(parent)
    requester = RequestContext("mac", "neil", "personal", frozenset({"orchestration.runs.read"}), peer="neil")
    index.register_child_watch(parent, ["run_a"], requester=requester)
    watch = index.claim_ready_child_watch(parent.thread_id, {"run_a"})
    assert watch is not None
    attempts = 0

    async def busy(*_args, **_kwargs):  # noqa: ANN002, ANN003
        nonlocal attempts
        attempts += 1
        raise RuntimeError("worker session already has an active turn")

    async def complete_during_sleep(_seconds: float) -> None:
        index.finish_child_watch(parent.thread_id, str(watch["watch_id"]))

    monkeypatch.setattr(CockpitConnector, "turn", busy)
    monkeypatch.setattr("jarvis.connectors.cockpit.asyncio.sleep", complete_during_sleep)

    _continue_child_watch(cfg, parent.thread_id, watch)

    assert attempts == 1


def test_read_child_work_result_is_parent_project_scoped_and_bounded(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    parent = CockpitThread(
        thread_id="thread_parent",
        project_id=project.id,
        session_id="project:neil-shared:orchestrator:thread_parent",
        title="Review pull request",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
    )
    store = OrchestrationStore(cfg.orchestration.workspace)
    child = store.create_run(
        "Codex review",
        parent_chat_id=parent.thread_id,
        project_id=project.id,
        engine="codex",
        model="gpt-5.5",
    )
    store.append_event(
        child.run_id,
        "assistant.message",
        "",
        {"time": "2026-07-11T10:01:00Z", "data": {"text": "[P1] Preserve the transition"}},
    )
    store.set_phase(child.run_id, "completed", "done")
    tool = _read_child_work_result_tool(cfg, project, parent)
    requester = RequestContext(
        device_id="local-mac",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"orchestration.runs.read"}),
    )

    result = json.loads(asyncio.run(tool.handler(requester, {"child_chat_id": child.run_id})))

    assert result["ready"] is True
    assert result["final_result"] == "[P1] Preserve the transition"
    assert result["engine"] == "codex"
    assert result["model"] == "gpt-5.5"


def test_scoped_orchestrator_grant_executes_only_for_its_parent_thread(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        token="brain-secret",
        caps="orchestration.runs.read",
        identity="neil",
    )
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    parent = CockpitThread(
        thread_id="thread_parent",
        project_id=project.id,
        session_id="project:neil-shared:orchestrator:thread_parent",
        title="Review pull request",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="codex",
        model="gpt-5.5",
    )
    CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json").save(parent)
    store = OrchestrationStore(cfg.orchestration.workspace)
    child = store.create_run(
        "Codex review",
        parent_chat_id=parent.thread_id,
        project_id=project.id,
        engine="codex",
        model="gpt-5.5",
    )
    store.append_event(
        child.run_id,
        "assistant.message",
        "",
        {"time": "2026-07-11T10:01:00Z", "data": {"text": "[P2] Preserve the transition"}},
    )
    store.set_phase(child.run_id, "completed", "done")
    requester = RequestContext(
        device_id="cockpit",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"orchestration.runs.read"}),
    )
    grant = mint_orchestrator_grant(
        cfg.orchestration,
        project_id=project.id,
        thread_id=parent.thread_id,
        requester=requester,
    )

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        result = await client.post(
            f"{base}/v1/orchestrator-tools/{project.id}/{parent.thread_id}/read_child_work_result",
            json={"child_chat_id": child.run_id},
            headers={"Authorization": f"Bearer {grant}"},
        )
        wrong_parent = await client.post(
            f"{base}/v1/orchestrator-tools/{project.id}/thread_other/read_child_work_result",
            json={"child_chat_id": child.run_id},
            headers={"Authorization": f"Bearer {grant}"},
        )

        assert result.status_code == 200
        assert json.loads(result.json()["result"])["final_result"] == "[P2] Preserve the transition"
        assert wrong_parent.status_code == 403

    asyncio.run(_with_server(cfg, calls))


def test_child_work_config_defaults_to_read_only_landing(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.connectors.cockpit import _child_work_config

    cfg = _cfg(tmp_path, monkeypatch)
    cfg.orchestration.landing_mode = "draft_pr"

    child_cfg = _child_work_config(cfg, {})

    assert child_cfg is not cfg
    assert child_cfg.orchestration.landing_mode == "none"
    assert cfg.orchestration.landing_mode == "draft_pr"


def test_child_work_config_accepts_explicit_landing_and_rejects_unknown(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.connectors.cockpit import _child_work_config

    cfg = _cfg(tmp_path, monkeypatch)

    assert _child_work_config(cfg, {"landing_mode": "branch_only"}).orchestration.landing_mode == "branch_only"
    with pytest.raises(ValueError, match="unsupported child landing_mode"):
        _child_work_config(cfg, {"landing_mode": "merge"})


def test_duplicate_child_terminal_notifications_schedule_one_parent_continuation(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    parent = CockpitThread(
        thread_id="thread_parent",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_parent",
        title="Review pull request",
        created_at="2026-07-11T10:00:00Z",
        updated_at="2026-07-11T10:00:00Z",
        created_by="neil",
    )
    index.save(parent)
    store = OrchestrationStore(cfg.orchestration.workspace)
    child = store.create_run("Review", parent_chat_id=parent.thread_id, project_id=parent.project_id)
    store.set_phase(child.run_id, "completed", "done")
    requester = RequestContext(
        device_id="local-mac",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"orchestration.runs.read"}),
    )
    index.register_child_watch(parent, [child.run_id], requester=requester)
    started: list[str] = []

    class FakeThread:
        def __init__(self, *, target, args, name, daemon):  # noqa: ANN001
            started.append(name)

        def start(self) -> None:
            return None

    monkeypatch.setattr("jarvis.connectors.cockpit.threading.Thread", FakeThread)

    _start_ready_child_watch(cfg, parent.thread_id)
    _start_ready_child_watch(cfg, parent.thread_id)

    assert len(started) == 1
