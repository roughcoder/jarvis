from __future__ import annotations

import asyncio
import base64
import json
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, replace
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
from jarvis.capabilities import (  # noqa: E402
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
)
from jarvis.brain.memory_client import ConclusionRecord, MemoryMessage, RepresentationRecord, SessionPeer  # noqa: E402
from jarvis.brain.memory_outbox import CurationOutbox  # noqa: E402
from jarvis.connectors.cockpit import (  # noqa: E402
    CockpitConnector,
    CockpitThread,
    CockpitThreadIndex,
    PENDING_EXECUTION_INTERRUPT_KEY,
    PENDING_ORCHESTRATOR_COMPLETION_KEY,
    ProviderTurnError,
    WorkerRequestError,
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


@pytest.fixture(autouse=True)
def _clean_probe_snapshots():  # noqa: ANN202
    """Remembered worker probes are process-global by design (they keep the
    cockpit card truthful between probes) — isolate them per test."""
    from jarvis.orchestration.workers import reset_probe_snapshots

    reset_probe_snapshots()
    yield
    reset_probe_snapshots()


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


async def _with_server(  # noqa: ANN001
    cfg: Config,
    fn: Callable[[str, httpx.AsyncClient], Any],
    *,
    http_get=None,
    http_post=None,
    http_delete=None,
    auto_turn_idempotency: bool = True,
) -> Any:
    runner = web.AppRunner(make_app(cfg, http_get=http_get, http_post=http_post, http_delete=http_delete))
    await runner.setup()
    site = web.TCPSite(runner, "localhost", 0)
    await site.start()
    sockets = site._server.sockets  # type: ignore[union-attr, attr-defined]  # noqa: SLF001
    port = sockets[0].getsockname()[1]
    try:
        class TestClient(httpx.AsyncClient):
            turn_sequence = 0

            async def post(self, url, **kwargs):  # noqa: ANN001
                payload = kwargs.get("json")
                if (
                    auto_turn_idempotency
                    and "/v1/projects/" in str(url)
                    and "/threads/" in str(url)
                    and str(url).endswith("/turns")
                    and isinstance(payload, dict)
                    and not payload.get("idempotency_key")
                ):
                    type(self).turn_sequence += 1
                    kwargs["json"] = {
                        **payload,
                        "idempotency_key": f"test-turn-{type(self).turn_sequence}",
                    }
                return await super().post(url, **kwargs)

        async with TestClient(timeout=10) as client:
            return await fn(f"http://localhost:{port}", client)
    finally:
        await runner.cleanup()


def _cfg(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    caps: str = "",
    profile_caps: str | None = None,
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
    worker_probe_interval_s: str = "0",
    retention_enabled: str = "false",
) -> Config:
    env = tmp_path / ".env"
    workspace = tmp_path / "orchestration"
    workers_path = workspace / "workers.json"
    registry_path = tmp_path / "registry.json"
    users_path = tmp_path / "users"
    profiles_path = tmp_path / "profiles"
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
                f"CAPS_PROFILES_DIR={profiles_path}",
                f"CAPS_USERS_DIR={users_path}",
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
                # Fixtures seed threads with fixed past timestamps; the startup
                # retention sweep would collect them mid-test. Retention has its
                # own suite (test_orchestration_retention.py).
                f"ORCHESTRATION_RETENTION_ENABLED={retention_enabled}",
                f"ORCHESTRATION_WORKER_PROBE_INTERVAL_S={worker_probe_interval_s}",
            ]
        )
    )
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env))
    if profile_caps is not None:
        profiles_path.mkdir(parents=True, exist_ok=True)
        capabilities = [cap.strip() for cap in profile_caps.split(",") if cap.strip()]
        profiles_path.joinpath("local-mac.md").write_text(
            f"---\ncapabilities: {json.dumps(capabilities)}\n---\n",
            encoding="utf-8",
        )
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


def _pull_request_review_params() -> dict[str, Any]:
    return {
        "pull_request": {"repository": "roughcoder/jarvis", "number": 142},
        "reviewers": [
            {"engine": "codex", "model": "gpt-5"},
            {"engine": "claude", "model": "claude-opus"},
        ],
        "access_mode": "full_trust",
    }


def _seed_retention_thread(cfg: Config, thread_id: str, *, days_idle: float = 30.0, project_id: str = "house-story") -> CockpitThread:
    stamp = (datetime.now(UTC) - timedelta(days=days_idle)).replace(microsecond=0).isoformat()
    thread = CockpitThread(
        thread_id=thread_id,
        project_id=project_id,
        session_id=f"project:{project_id}:{thread_id}",
        title=thread_id,
        created_at=stamp,
        updated_at=stamp,
        created_by="neil",
        last_turn_at=stamp,
    )
    return CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json").save(thread)


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
        # Unmeasured fields stay null so "not scanned" never renders as "zero".
        assert workers["workers"][0]["worktree_inventory"] == {
            "root": "",
            "count": 3,
            "disk_bytes": 2048,
            "stale_count": 1,
            "orphan_count": None,
            "status": "measured",
        }
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


def test_cockpit_worker_worktree_prune_reaches_the_worker(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """The cockpit's "Prune stale worktrees" action had no endpoint on this tier
    at all — /v1/workers was read-only, so the button could only ever no-op."""
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    posted: list[tuple[str, dict[str, Any]]] = []
    inventory_after = {"root": "/w/worktrees", "count": 1, "disk_bytes": 4096, "stale_count": 0, "orphan_count": 0}

    def post(url: str, **kwargs) -> Response:  # noqa: ANN001
        posted.append((url, kwargs.get("json") or {}))
        return Response(
            {
                "ok": True,
                "worktrees": 3,
                "bytes": 12_000_000,
                "pruned": [{"name": "old-a", "bytes": 6_000_000}, {"name": "old-b", "bytes": 6_000_000}],
                "refused": [],
                "repos_pruned": ["/repos/jarvis/.git"],
            }
        )

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response({"sessions": []})
        return Response({"ok": True, "worktree_inventory": inventory_after})

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        response = await client.post(
            f"{base}/v1/workers/macbook-worker/worktrees/prune",
            json={"idempotency_key": "prune_1"},
        )
        assert response.status_code == 200
        return response.json()

    import asyncio

    body = asyncio.run(_with_server(cfg, calls, http_get=get, http_post=post))

    assert posted and posted[0][0].endswith("/worktrees/prune")
    assert body["reclamation"]["worktrees"] == 3
    assert body["reclamation"]["bytes"] == 12_000_000
    assert [item["name"] for item in body["pruned"]] == ["old-a", "old-b"]
    # The response carries a fresh recount so a caller can show before/after.
    assert body["worktree_inventory"]["count"] == 1
    assert body["worktree_inventory"]["stale_count"] == 0


def test_cockpit_retention_reads_require_auth_and_return_contract_shape(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, token="secret")
    _seed_retention_thread(cfg, "old_chat")

    async def calls(base: str, client: httpx.AsyncClient) -> tuple[int, int, dict[str, Any], dict[str, Any]]:
        unauthorized = await client.get(f"{base}/v1/retention/plan")
        unauthorized_settings = await client.get(f"{base}/v1/retention/settings")
        headers = {"Authorization": "Bearer secret"}
        plan_response = await client.get(f"{base}/v1/retention/plan", headers=headers)
        settings_response = await client.get(f"{base}/v1/retention/settings", headers=headers)
        assert plan_response.status_code == 200
        assert settings_response.status_code == 200
        return (unauthorized.status_code, unauthorized_settings.status_code, plan_response.json(), settings_response.json())

    import asyncio

    unauthorized_status, unauthorized_settings_status, plan_body, settings_body = asyncio.run(_with_server(cfg, calls))

    assert unauthorized_status == 401
    assert unauthorized_settings_status == 401
    assert plan_body["ok"] is True
    assert plan_body["plan"]["classes"] == [
        {"name": "archived", "ttl_days": 14.0, "count": 0, "bytes": 0, "disabled": False},
        {"name": "chat", "ttl_days": 7.0, "count": 1, "bytes": 0, "disabled": False},
        {"name": "tree", "ttl_days": 7.0, "count": 0, "bytes": 0, "disabled": False},
    ]
    assert plan_body["plan"]["total_count"] == 1
    assert plan_body["plan"]["total_bytes"] == 0
    assert plan_body["plan"]["kept"] == 0
    assert plan_body["settings"] == {
        "enabled": False,
        "interval_s": 21600,
        "archived_ttl_days": 14.0,
        "chat_ttl_days": 7.0,
        "tree_ttl_days": 7.0,
    }
    assert plan_body["auto"] == {
        "enabled": False,
        "interval_s": 21600,
        "last_run_at": None,
        "last_result": None,
    }
    assert settings_body == {
        "ok": True,
        "settings": plan_body["settings"],
        "source": {
            "enabled": "env",
            "interval_s": "env",
            "archived_ttl_days": "env",
            "chat_ttl_days": "env",
            "tree_ttl_days": "env",
        },
    }


def test_cockpit_retention_settings_write_requires_capability_and_is_idempotent(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    denied_root = tmp_path / "denied"
    allowed_root = tmp_path / "allowed"
    denied_root.mkdir()
    allowed_root.mkdir()
    denied_cfg = _cfg(denied_root, monkeypatch)

    async def denied_calls(base: str, client: httpx.AsyncClient) -> tuple[int, int]:
        missing_key = await client.put(f"{base}/v1/retention/settings", json={"chat_ttl_days": 3})
        forbidden = await client.put(
            f"{base}/v1/retention/settings",
            json={"idempotency_key": "settings_denied", "chat_ttl_days": 3},
        )
        return missing_key.status_code, forbidden.status_code

    import asyncio

    assert asyncio.run(_with_server(denied_cfg, denied_calls)) == (400, 403)

    cfg = _cfg(allowed_root, monkeypatch, caps="orchestration.runs.write")

    async def calls(base: str, client: httpx.AsyncClient) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
        first = await client.put(
            f"{base}/v1/retention/settings",
            json={"idempotency_key": "settings_1", "chat_ttl_days": 3.5, "interval_s": 60},
        )
        plan = await client.get(f"{base}/v1/retention/plan")
        replay = await client.put(
            f"{base}/v1/retention/settings",
            json={"idempotency_key": "settings_1", "chat_ttl_days": 3.5, "interval_s": 60},
        )
        cleared = await client.put(
            f"{base}/v1/retention/settings",
            json={"idempotency_key": "settings_2", "chat_ttl_days": None},
        )
        fetched = await client.get(f"{base}/v1/retention/settings")
        assert first.status_code == plan.status_code == replay.status_code == cleared.status_code == fetched.status_code == 200
        return first.json(), plan.json(), replay.json(), cleared.json(), fetched.json()

    first, plan, replay, cleared, fetched = asyncio.run(_with_server(cfg, calls))

    assert first["settings"]["chat_ttl_days"] == 3.5
    assert first["settings"]["interval_s"] == 60
    assert first["source"]["chat_ttl_days"] == "override"
    assert first["source"]["interval_s"] == "override"
    assert plan["plan"]["classes"][1]["name"] == "chat"
    assert plan["plan"]["classes"][1]["ttl_days"] == 3.5
    assert replay["idempotent"] is True
    assert replay["settings"] == first["settings"]
    assert cleared["settings"]["chat_ttl_days"] == 7.0
    assert cleared["source"]["chat_ttl_days"] == "env"
    assert cleared["source"]["interval_s"] == "override"
    assert fetched == cleared


def test_cockpit_retention_prune_requires_capability_and_is_idempotent(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    denied_root = tmp_path / "denied"
    allowed_root = tmp_path / "allowed"
    denied_root.mkdir()
    allowed_root.mkdir()
    denied_cfg = _cfg(denied_root, monkeypatch)

    async def denied_calls(base: str, client: httpx.AsyncClient) -> tuple[int, int]:
        missing_key = await client.post(f"{base}/v1/retention/prune", json={})
        forbidden = await client.post(f"{base}/v1/retention/prune", json={"idempotency_key": "prune_denied"})
        return missing_key.status_code, forbidden.status_code

    import asyncio

    assert asyncio.run(_with_server(denied_cfg, denied_calls)) == (400, 403)

    cfg = _cfg(allowed_root, monkeypatch, caps="orchestration.runs.write")
    _seed_retention_thread(cfg, "old_chat")

    class FakeMemory:
        def __init__(self, _cfg) -> None:  # noqa: ANN001
            pass

        def delete_session(self, _session_id: str) -> None:
            return None

    monkeypatch.setattr(cockpit_api_module, "MemoryClient", FakeMemory)

    async def calls(base: str, client: httpx.AsyncClient) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        first = await client.post(f"{base}/v1/retention/prune", json={"idempotency_key": "prune_1"})
        replay = await client.post(f"{base}/v1/retention/prune", json={"idempotency_key": "prune_1"})
        plan = await client.get(f"{base}/v1/retention/plan")
        assert first.status_code == replay.status_code == plan.status_code == 200
        return first.json(), replay.json(), plan.json()

    first, replay, plan = asyncio.run(_with_server(cfg, calls))

    assert first == {
        "ok": True,
        "deleted": {"archived": 0, "chat": 1, "tree": 0},
        "child_runs": 0,
        "bytes_reclaimed": 0,
        "kept": 0,
    }
    assert replay["idempotent"] is True
    assert replay["deleted"] == first["deleted"]
    assert plan["auto"]["last_run_at"]
    assert plan["auto"]["last_result"] == {"deleted": 1, "bytes": 0}


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
    store, _run_id = _seed_run(cfg)
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("backed-off worker was polled")),
        post=lambda *_args, **_kwargs: Response({}),
        store=store,
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )
    hub = SseSnapshotHub(ctx)
    hub._tick = 2  # noqa: SLF001
    hub._worker_backoff_until["macbook-worker"] = 9  # noqa: SLF001
    cached_worker = {"worker_id": "macbook-worker", "status": "online", "system": {"cpu_model": "cached-probe"}}
    cached_session = {
        "session_ref": make_session_ref("macbook-worker", "worker_only"),
        "worker_id": "macbook-worker",
        "session_id": "worker_only",
        "status": "running",
    }
    previous = {
        "workers": [cached_worker],
        "sessions": {cached_session["session_ref"]: cached_session},
        "requests": [],
        "checkpoints": [],
        "partial": True,
        "diagnostics": [
            {
                "worker_id": "macbook-worker",
                "resource": "sessions",
                "status": "failure",
                "failure_kind": "transport_error",
                "status_code": 0,
                "error_type": "TimeoutError",
                "session_id": "",
            }
        ],
    }

    state = _hub_worker_state(ctx, "probe", _HubWorkerSync(hub), store.list_runs(), previous=previous)

    assert state["workers"] == [cached_worker]
    assert set(row["session_id"] for row in state["sessions"].values()) == {"worker_only", "sess_123"}
    assert state["partial"] is True
    assert state["diagnostics"] == previous["diagnostics"]


def test_cockpit_initial_sse_snapshot_uses_shared_worker_refresh(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions/requests"):
            return Response({"requests": []})
        if url.endswith("/sessions/checkpoints"):
            return Response({"checkpoints": []})
        if url.endswith("/sessions"):
            return Response({"sessions": [{"session_id": "sse_initial", "provider": "codex", "status": "running"}]})
        raise AssertionError(url)

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

    async def snapshot() -> dict[str, Any]:
        return await SseSnapshotHub(ctx)._snapshot("fast")  # noqa: SLF001

    import asyncio

    body = asyncio.run(snapshot())
    assert [row["session_id"] for row in body["sessions"]] == ["sse_initial"]
    assert ctx.worker_state_cache["fast"]["sessions"]


def test_cockpit_dirty_refresh_preserves_untouched_worker_diagnostics() -> None:
    from jarvis.orchestration.api import _reconcile_worker_diagnostics

    previous = [
        {"worker_id": "steady", "resource": "sessions", "status": "failure"},
        {"worker_id": "dirty", "resource": "requests", "status": "failure"},
    ]
    current = [{"worker_id": "dirty", "resource": "checkpoints", "status": "unsupported"}]

    merged = _reconcile_worker_diagnostics(previous, current, {"dirty"}, {"dirty"}, {"dirty", "steady"})

    assert merged == [previous[0], current[0]]


def test_cockpit_full_refresh_clears_workers_removed_from_registry(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.api import _HubWorkerSync, _hub_worker_state

    cfg = _cfg(tmp_path, monkeypatch)

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions"):
            return Response({"sessions": []})
        if url.endswith("/sessions/requests"):
            return Response({"requests": []})
        raise AssertionError(url)

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
    ghost_ref = make_session_ref("removed-worker", "ghost")
    previous = {
        "workers": [{"worker_id": "removed-worker"}],
        "sessions": {ghost_ref: {"session_ref": ghost_ref, "worker_id": "removed-worker", "session_id": "ghost"}},
        "requests": [],
        "checkpoints": [],
        "diagnostics": [{"worker_id": "removed-worker", "resource": "sessions", "status": "failure"}],
    }

    state = _hub_worker_state(ctx, "fast", _HubWorkerSync(SseSnapshotHub(ctx)), [], previous=previous)

    assert [row["worker_id"] for row in state["workers"]] == ["macbook-worker"]
    assert state["sessions"] == {}
    assert state["diagnostics"] == []
    assert state["partial"] is False


def test_cockpit_offline_worker_preserves_cached_worker_only_facets(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.api import _HubWorkerSync, _cockpit_snapshot, _hub_worker_state

    cfg = _cfg(tmp_path, monkeypatch)
    _set_worker_status(cfg, "offline")
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("offline worker was polled")),
        post=lambda *_args, **_kwargs: Response({}),
        store=OrchestrationStore(cfg.orchestration.workspace),
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )
    ref = make_session_ref("macbook-worker", "cached-offline")
    previous = {
        "workers": [{"worker_id": "macbook-worker", "status": "online", "system": {"cpu_model": "cached"}}],
        "sessions": {ref: {"session_ref": ref, "worker_id": "macbook-worker", "session_id": "cached-offline"}},
        "requests": [],
        "checkpoints": [],
        "partial": False,
        "diagnostics": [],
    }

    state = _hub_worker_state(ctx, "fast", _HubWorkerSync(SseSnapshotHub(ctx)), [], previous=previous)
    snapshot = _cockpit_snapshot(
        ctx,
        "fast",
        sync={"mode": "fast", "status": "fresh", "synced_at": "", "errors": []},
        worker_state=state,
        all_runs=[],
    )

    assert state["workers"][0]["status"] == "offline"
    assert state["sessions"] == previous["sessions"]
    assert state["diagnostics"][0]["failure_kind"] == "offline"
    assert state["partial"] is True
    assert snapshot["workers"][0]["status"] == "offline"
    assert snapshot["sync"]["status"] == "partial"


def test_cockpit_backoff_stays_partial_until_successful_retry(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.api import _HubWorkerSync, _hub_worker_state

    cfg = _cfg(tmp_path, monkeypatch)
    failing = {"value": True}

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if failing["value"]:
            raise TimeoutError("worker unavailable")
        if url.endswith("/sessions"):
            return Response({"sessions": []})
        if url.endswith("/sessions/requests"):
            return Response({"requests": []})
        raise AssertionError(url)

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
    hub = SseSnapshotHub(ctx)
    sync = _HubWorkerSync(hub)
    profile = sync.profiles[0]
    with pytest.raises(TimeoutError):
        sync.get(f"{profile.base_url}/sessions")

    first = _hub_worker_state(ctx, "fast", sync, [])
    unsupported_previous = {
        **first,
        "partial": False,
        "diagnostics": [
            {
                "worker_id": profile.worker_id,
                "resource": "checkpoints",
                "status": "unsupported",
                "failure_kind": "unsupported",
                "status_code": 404,
                "error_type": "",
                "session_id": "",
            }
        ],
    }
    unsupported_backoff = _hub_worker_state(ctx, "fast", sync, [], previous=unsupported_previous)
    second = _hub_worker_state(ctx, "fast", sync, [], previous=first)
    failing["value"] = False
    hub._tick = hub._worker_backoff_until[profile.worker_id]  # noqa: SLF001
    recovered = _hub_worker_state(ctx, "fast", sync, [], previous=second)

    assert first["partial"] is True
    assert first["diagnostics"][0]["failure_kind"] == "backoff"
    assert [item["failure_kind"] for item in unsupported_backoff["diagnostics"]] == ["unsupported", "backoff"]
    assert unsupported_backoff["partial"] is True
    assert second["partial"] is True
    assert recovered["partial"] is False
    assert recovered["diagnostics"] == []


def test_cockpit_archived_session_prunes_cached_session_diagnostic(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.api import _HubWorkerSync, _hub_worker_state

    cfg = _cfg(tmp_path, monkeypatch)
    store = OrchestrationStore(cfg.orchestration.workspace)
    store.archive_worker_session("macbook-worker", "diagnostic-tombstone")
    ctx = CockpitAppContext(
        cfg=cfg,
        get=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("backed-off worker was polled")),
        post=lambda *_args, **_kwargs: Response({}),
        store=store,
        idempotency=IdempotencyStore(cfg.orchestration.workspace),
        idempotency_locks={},
        idempotency_lock_refs={},
        source_factory=lambda _source, _cfg: None,
    )
    hub = SseSnapshotHub(ctx)
    hub._worker_backoff_until["macbook-worker"] = 5  # noqa: SLF001
    ref = make_session_ref("macbook-worker", "diagnostic-tombstone")
    previous = {
        "workers": [{"worker_id": "macbook-worker", "status": "online"}],
        "sessions": {ref: {"session_ref": ref, "worker_id": "macbook-worker", "session_id": "diagnostic-tombstone"}},
        "requests": [],
        "checkpoints": [],
        "partial": True,
        "diagnostics": [
            {
                "worker_id": "macbook-worker",
                "resource": "session_checkpoints",
                "status": "failure",
                "failure_kind": "transport_error",
                "session_id": "diagnostic-tombstone",
            }
        ],
    }

    state = _hub_worker_state(ctx, "fast", _HubWorkerSync(hub), [], previous=previous)

    assert state["sessions"] == {}
    assert all(item.get("session_id") != "diagnostic-tombstone" for item in state["diagnostics"])
    assert state["diagnostics"][0]["failure_kind"] == "backoff"


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


def test_cockpit_rest_snapshot_retains_failed_worker_facets_then_clears_successful_empty(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    phase = {"value": "populated"}

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions/requests"):
            if phase["value"] == "failure":
                raise TimeoutError("secret-worker.example requests timed out")
            requests = [] if phase["value"] == "empty" else [
                {"request_id": "req_1", "session_id": "sess_1", "kind": "approval"}
            ]
            return Response({"requests": requests})
        if url.endswith("/sessions/checkpoints"):
            if phase["value"] == "failure":
                raise TimeoutError("secret-worker.example checkpoints timed out")
            checkpoints = [] if phase["value"] == "empty" else [
                {"checkpoint_id": "ckpt_1", "session_id": "sess_1"}
            ]
            return Response({"checkpoints": checkpoints})
        if url.endswith("/sessions"):
            if phase["value"] == "failure":
                raise TimeoutError("secret-worker.example sessions timed out")
            sessions = [] if phase["value"] == "empty" else [
                {"session_id": "sess_1", "provider": "codex", "status": "running"}
            ]
            return Response({"sessions": sessions})
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        populated = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()
        phase["value"] = "failure"
        retained = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()
        phase["value"] = "empty"
        cleared = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

        assert len(populated["sessions"]) == len(populated["requests"]) == len(populated["checkpoints"]) == 1
        assert [row["session_id"] for row in retained["sessions"]] == ["sess_1"]
        assert retained["requests"] == populated["requests"]
        assert retained["checkpoints"] == populated["checkpoints"]
        assert retained["sync"]["status"] == "partial"
        assert {item["resource"] for item in retained["sync"]["diagnostics"]} >= {
            "sessions",
            "requests",
            "checkpoints",
        }
        assert "secret-worker.example" not in json.dumps(retained["sync"])
        assert cleared["sessions"] == []
        assert cleared["requests"] == []
        assert cleared["checkpoints"] == []
        assert cleared["sync"]["status"] == "fresh"
        assert cleared["sync"]["diagnostics"] == []

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_failed_refresh_does_not_resurrect_archived_cached_session(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="orchestration.runs.write")
    failed = {"value": False}

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions/requests"):
            return Response({"requests": []})
        if url.endswith("/sessions/checkpoints"):
            return Response({"checkpoints": []})
        if url.endswith("/sessions"):
            if failed["value"]:
                raise TimeoutError("worker unavailable")
            return Response({"sessions": [{"session_id": "archive_cached", "provider": "codex", "status": "running"}]})
        raise AssertionError(url)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        populated = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()
        ref = populated["sessions"][0]["session_ref"]
        archived = await client.post(f"{base}/v1/sessions/{ref}/archive", json={"idempotency_key": "archive-cached"})
        failed["value"] = True
        refreshed = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "fast"})).json()

        assert archived.status_code == 200
        assert refreshed["sessions"] == []
        assert refreshed["requests"] == []
        assert refreshed["checkpoints"] == []
        assert refreshed["sync"]["status"] == "partial"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


def test_cockpit_checkpoint_fallback_retains_only_failed_session_facet(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.orchestration.api import _HubWorkerSync, _hub_worker_state

    cfg = _cfg(tmp_path, monkeypatch)
    phase = {"value": "populated"}

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/sessions"):
            return Response(
                {
                    "sessions": [
                        {"session_id": "sess_failed", "provider": "codex", "status": "running"},
                        {"session_id": "sess_empty", "provider": "codex", "status": "running"},
                    ]
                }
            )
        if url.endswith("/sessions/requests"):
            return Response({"requests": []})
        if url.endswith("/sessions/checkpoints"):
            if phase["value"] == "populated":
                return Response(
                    {
                        "checkpoints": [
                            {"session_id": "sess_failed", "checkpoint_id": "ckpt_failed"},
                            {"session_id": "sess_empty", "checkpoint_id": "ckpt_empty"},
                        ]
                    }
                )
            return Response({}, status_code=404)
        if url.endswith("/sessions/sess_failed/checkpoints"):
            raise TimeoutError("private checkpoint path")
        if url.endswith("/sessions/sess_empty/checkpoints"):
            return Response({"checkpoints": []})
        raise AssertionError(url)

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
    hub = SseSnapshotHub(ctx)
    sync = _HubWorkerSync(hub)
    previous = _hub_worker_state(ctx, "fast", sync, [])
    phase["value"] = "fallback"
    current = _hub_worker_state(ctx, "fast", sync, [], previous=previous)

    assert [row["checkpoint_id"] for row in current["checkpoints"]] == ["ckpt_failed"]
    assert current["partial"] is True
    failed = [item for item in current["diagnostics"] if item["status"] == "failure"]
    assert failed == [
        {
            "worker_id": "macbook-worker",
            "resource": "session_checkpoints",
            "status": "failure",
            "failure_kind": "transport_error",
            "status_code": 0,
            "error_type": "TimeoutError",
            "session_id": "sess_failed",
        }
    ]


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
            "routines": {"available": True, "builtin_count": 5},
            "schedules": {"available": False, "resident_tick": True, "writable": False},
        }

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_schedule_write_authority_comes_from_device_profile_when_present(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    schedule_body = {
        "name": "Weekday brief",
        "routine_id": "morning-brief",
        "project_id": "neil-shared",
        "hour": 9,
        "minute": 15,
        "weekdays": [0, 1, 2, 3, 4],
        "timezone": "Europe/London",
        "idempotency_key": "create-profile-brief",
    }

    read_only_root = tmp_path / "read-only-profile"
    read_only_root.mkdir()
    read_only_cfg = _cfg(
        read_only_root,
        monkeypatch,
        identity="neil",
        caps="orchestration.schedules.read,orchestration.schedules.write",
        profile_caps="orchestration.schedules.read",
    )
    _seed_project_registry(read_only_cfg)

    async def denied_calls(base: str, client: httpx.AsyncClient) -> None:
        capabilities = await client.get(f"{base}/v1/capabilities")
        created = await client.post(f"{base}/v1/schedules", json=schedule_body)

        assert capabilities.json()["features"]["schedules"] == {
            "available": True,
            "resident_tick": True,
            "writable": False,
        }
        assert created.status_code == 403
        assert created.json()["error"]["message"] == "missing authority: orchestration.schedules.write"

    asyncio.run(_with_server(read_only_cfg, denied_calls))

    writable_root = tmp_path / "writable-profile"
    writable_root.mkdir()
    writable_cfg = _cfg(
        writable_root,
        monkeypatch,
        identity="neil",
        profile_caps="orchestration.schedules.read,orchestration.schedules.write",
    )
    _seed_project_registry(writable_cfg)

    async def allowed_calls(base: str, client: httpx.AsyncClient) -> None:
        capabilities = await client.get(f"{base}/v1/capabilities")
        created = await client.post(f"{base}/v1/schedules", json=schedule_body)

        assert capabilities.json()["features"]["schedules"] == {
            "available": True,
            "resident_tick": True,
            "writable": True,
        }
        assert created.status_code == 201

    asyncio.run(_with_server(writable_cfg, allowed_calls))


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

        # The composer's @-picker types into ?query=, which rides through to the
        # same brain op rather than filtering client-side.
        searched = await client.get(f"{base}/v1/projects/neil-shared/files?query=spec")
        assert searched.status_code == 200

    import asyncio

    asyncio.run(_with_server(cfg, calls))

    assert brain.calls[0]["op"] == "project.file.list"
    assert brain.calls[0]["payload"] == {"project_id": "neil-shared", "include_retracted": True, "query": ""}
    assert brain.calls[1]["payload"] == {"project_id": "neil-shared", "include_retracted": False, "query": "spec"}


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


def test_cockpit_project_memory_degrades_when_live_memory_is_unavailable(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory(
        live_error=cockpit_api_module.UnsupportedMemoryOperation("live memory unsupported"),
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
        "supports": {"steer": False, "queue": True},
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
        "supports": {"steer": False, "queue": True},
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
            "supports": {"steer": False, "queue": True},
        }
    )

    assert execution["active_turn"]["turn_id"] == "turn_active"
    assert execution["pending_requests"] == []


def test_cockpit_thread_approval_resolves_attached_execution_idempotently(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil", caps=WORKER_SESSION_APPROVE)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    index.save(
        CockpitThread(
            thread_id="thread_approval",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_approval"),
            title="Approval conversation",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            engine="codex",
            worker_id="macbook-worker",
            workspace={"worker_id": "macbook-worker", "session_id": "conv_thread_approval"},
        )
    )
    writes: list[tuple[str, dict[str, Any]]] = []

    def worker_post(url: str, **kwargs: Any) -> Response:
        writes.append((url, kwargs["json"]))
        return Response({"ok": True, "event": {"type": "approval.resolved"}})

    def worker_get(url: str, **_kwargs: Any) -> Response:
        assert url == "http://worker.test/sessions/conv_thread_approval/execution-state"
        return Response(
            {
                "session_id": "conv_thread_approval",
                "status": "running",
                "active_turn": {"turn_id": "turn_1", "status": "running", "started_at": "now"},
                "pending_requests": [],
                "supported_controls": ["turn", "approval", "interrupt"],
                "supports": {"steer": False, "queue": False},
            }
        )

    async def calls(base: str, client: httpx.AsyncClient) -> tuple[httpx.Response, httpx.Response]:
        payload = {
            "request_id": "approval_1",
            "decision": "approved",
            "idempotency_key": "approve-once",
            "allowed_actions": ["worker.session.stop"],
            "metadata": {"allowed_actions": ["worker.session.stop"], "surface": "browser"},
        }
        first = await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_approval/approval",
            json=payload,
        )
        replay = await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_approval/approval",
            json=payload,
        )
        return first, replay

    first, replay = asyncio.run(_with_server(cfg, calls, http_get=worker_get, http_post=worker_post))

    assert first.status_code == 200
    assert replay.json() == {**first.json(), "idempotent": True}
    assert writes == [
        (
            "http://worker.test/sessions/conv_thread_approval/approval",
            {
                "request_id": "approval_1",
                "decision": "approved",
                "idempotency_key": "approve-once",
                "metadata": {"surface": "browser"},
                "allowed_actions": [WORKER_SESSION_APPROVE],
            },
        )
    ]
    assert first.json() == {
        "ok": True,
        "api_version": "v1",
        "schema_version": 1,
        "project_id": "neil-shared",
        "thread_id": "thread_approval",
        "control": {
            "action": "approval",
            "accepted": True,
            "request_id": "approval_1",
        },
        "execution": {
            "available": True,
            "status": "running",
            "active_turn": {"turn_id": "turn_1", "status": "running", "started_at": "now"},
            "pending_requests": [],
            "supported_controls": ["turn", "approval", "interrupt"],
            "supports": {"steer": False, "queue": True},
            "diagnostic": None,
        },
    }


def test_cockpit_thread_input_resolves_by_conversation_identity(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil", caps=WORKER_SESSION_INPUT)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    index.save(
        CockpitThread(
            thread_id="thread_input",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_input"),
            title="Input conversation",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            workspace={"worker_id": "macbook-worker", "session_id": "conv_thread_input"},
        )
    )
    writes: list[dict[str, Any]] = []

    def worker_post(url: str, **kwargs: Any) -> Response:
        assert url == "http://worker.test/sessions/conv_thread_input/input"
        writes.append(kwargs["json"])
        return Response({"ok": True})

    def worker_get(_url: str, **_kwargs: Any) -> Response:
        return Response(
            {
                "session_id": "conv_thread_input",
                "status": "running",
                "active_turn": {"turn_id": "turn_1", "status": "running"},
                "pending_requests": [],
                "supported_controls": ["turn", "input"],
                "supports": {"steer": False, "queue": False},
            }
        )

    async def calls(base: str, client: httpx.AsyncClient) -> httpx.Response:
        return await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_input/input",
            json={"request_id": "input_1", "answers": {"target": "Tests"}},
        )

    response = asyncio.run(_with_server(cfg, calls, http_get=worker_get, http_post=worker_post))

    assert response.status_code == 200
    assert response.json()["control"] == {
        "action": "input",
        "accepted": True,
        "request_id": "input_1",
    }
    assert writes == [
        {
            "request_id": "input_1",
            "answers": {"target": "Tests"},
            "metadata": {},
            "allowed_actions": [WORKER_SESSION_INPUT],
        }
    ]


def test_cockpit_thread_interrupt_keeps_conversation_open_and_next_turn_recreates_execution(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil", caps=WORKER_SESSION_INTERRUPT)
    _seed_project_registry(cfg)
    posts: list[tuple[str, dict[str, Any]]] = []
    interrupt_started = threading.Event()
    release_interrupt = threading.Event()

    def worker_get(url: str, **_kwargs: Any) -> Response:
        if url.endswith("/execution-state"):
            return Response(
                {
                    "session_id": "conv_thread_interrupt",
                    "status": "interrupted",
                    "active_turn": None,
                    "pending_requests": [],
                    "supported_controls": ["turn"],
                    "supports": {"steer": False, "queue": False},
                }
            )
        if url.endswith("/health"):
            return Response(
                {
                    "ok": True,
                    "agent": "codex",
                    "default_engine": "codex",
                    "supported_engines": ["codex"],
                    "engine_supports": {"codex": {"streaming": True}},
                    "repositories": [{"repo": "roughcoder/jarvis", "status": "ready"}],
                }
            )
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response({"sessions": []})
        return Response({})

    def worker_post(url: str, **kwargs: Any) -> Response:
        body = kwargs["json"]
        posts.append((url, body))
        workspace = {
            "workspace_id": "neil-shared-thread-interrupt",
            "conversation_id": "neil-shared-thread-interrupt",
            "root": str(tmp_path / "worker" / "conversation"),
            "root_label": "thread-interrupt",
            "cwd_label": "thread-interrupt",
            "status": "ready",
            "provision_phase": "ready",
            "worktrees": [],
        }
        if url.endswith("/interrupt"):
            interrupt_started.set()
            assert release_interrupt.wait(timeout=2)
            return Response({"ok": True, "session": {"status": "interrupted"}})
        if url.endswith("/conversation-workspaces"):
            return Response({"ok": True, "workspace": workspace})
        if url.endswith("/worktrees"):
            workspace["worktrees"] = [{**body, "path": str(tmp_path / "worker" / "conversation" / "runtime")}]
            return Response({"ok": True, "workspace": workspace})
        if url.endswith("/sessions"):
            return Response({"ok": True, "session": {**body, "status": "created"}})
        if url.endswith("/turns"):
            return Response({"ok": True, "session": {"session_id": body.get("session_id", ""), "status": "running"}})
        return Response({"ok": False, "error": "unexpected worker post"}, status_code=400)

    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
        worker_get=worker_get,
        worker_post=worker_post,
    )
    connector.index.save(
        CockpitThread(
            thread_id="thread_interrupt",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_interrupt"),
            title="Interrupt conversation",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            workspace={
                "worker_id": "macbook-worker",
                "session_id": "conv_thread_interrupt",
                "status": "ready",
                "provision_phase": "ready",
                "root": str(tmp_path / "worker" / "conversation"),
            },
        )
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(
        base: str,
        client: httpx.AsyncClient,
    ) -> tuple[
        httpx.Response,
        httpx.Response,
        httpx.Response,
        httpx.Response,
        dict[str, Any],
    ]:
        interrupt_task = asyncio.create_task(
            client.post(
                f"{base}/v1/projects/neil-shared/threads/thread_interrupt/interrupt",
                json={"turn_id": "turn_active", "idempotency_key": "interrupt-once"},
            )
        )
        assert await asyncio.to_thread(interrupt_started.wait, 2)
        duplicate_interrupt = await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_interrupt/interrupt",
            json={"turn_id": "turn_active", "idempotency_key": "interrupt-twice"},
        )
        raced_turn = await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_interrupt/turns",
            json={"text": "Do not reuse the execution being interrupted."},
        )
        release_interrupt.set()
        interrupted = await interrupt_task
        next_turn = await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_interrupt/turns",
            json={"text": "Continue with a fresh execution."},
        )
        detail = (
            await client.get(f"{base}/v1/projects/neil-shared/threads/thread_interrupt")
        ).json()["thread"]
        return interrupted, duplicate_interrupt, raced_turn, next_turn, detail

    interrupted, duplicate_interrupt, raced_turn, next_turn, detail = asyncio.run(
        _with_server(cfg, calls, http_get=worker_get, http_post=worker_post)
    )

    assert interrupted.status_code == 200
    assert interrupted.json()["execution"]["status"] == "interrupted"
    assert interrupted.json()["control"] == {
        "action": "interrupt",
        "accepted": True,
        "turn_id": "turn_active",
    }
    assert duplicate_interrupt.status_code == 409
    assert duplicate_interrupt.json()["error"]["code"] == "execution_busy"
    assert raced_turn.status_code == 200
    assert "thread.turn.error" in raced_turn.text
    assert next_turn.status_code == 200
    assert detail["lifecycle"] == "open"
    assert detail["archived_at"] == ""
    created_sessions = [body for url, body in posts if url.endswith("/sessions")]
    assert created_sessions[-1]["session_id"] == "conv_thread-interrupt_1"
    assert not any(
        url.endswith("/sessions/conv_thread_interrupt/turns") for url, _body in posts
    )


def test_cockpit_ambiguous_remote_interrupt_preserves_durable_recovery_state(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil", caps=WORKER_SESSION_INTERRUPT)
    _seed_project_registry(cfg)

    def worker_get(url: str, **_kwargs: Any) -> Response:
        if url.endswith("/health"):
            return Response(
                {
                    "ok": True,
                    "agent": "codex",
                    "default_engine": "codex",
                    "supported_engines": ["codex"],
                    "engine_supports": {"codex": {"streaming": True}},
                    "repositories": [],
                }
            )
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response({"sessions": []})
        return Response({})

    def worker_post(url: str, **_kwargs: Any) -> Response:
        if url.endswith("/interrupt"):
            raise OSError("worker connection dropped before interrupt acceptance")
        return Response({"ok": False, "error": "unexpected worker post"}, status_code=400)

    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None, worker_get=worker_get, worker_post=worker_post)
    connector.index.save(
        CockpitThread(
            thread_id="thread_interrupt_recovery",
            project_id="neil-shared",
            session_id=orchestrator_session_id(
                "neil-shared",
                "thread_interrupt_recovery",
            ),
            title="Recover failed interrupt",
            created_at="2026-07-20T12:00:00Z",
            updated_at="2026-07-20T12:00:00Z",
            created_by="neil",
            chat_type="orchestrator",
            workspace={
                "worker_id": "macbook-worker",
                "session_id": "orch_interrupt_recovery",
                "session_generation": 2,
                "status": "running",
                "provision_phase": "running",
                PENDING_ORCHESTRATOR_COMPLETION_KEY: {
                    "phase": "accepted",
                    "worker_id": "macbook-worker",
                    "session_id": "orch_interrupt_recovery",
                    "session_generation": 2,
                    "turn_id": "turn_interrupt_recovery",
                    "idempotency_key": "routine-interrupt-recovery",
                    "persisted_text": "Keep waiting for this result.",
                    "requester": {
                        "device_id": "local-mac",
                        "identity": "neil",
                        "scope": "personal",
                        "capabilities": [],
                        "channel": "cockpit",
                        "confidence": "strong",
                        "peer": "neil",
                    },
                },
            },
        )
    )
    wait_calls: list[tuple[str, str, str]] = []
    wait_started = asyncio.Event()

    async def wait_until_cancelled(worker_id, session_id, turn_id):  # noqa: ANN001, ANN202
        wait_calls.append((worker_id, session_id, turn_id))
        wait_started.set()
        await asyncio.Future()

    monkeypatch.setattr(connector, "_wait_for_orchestrator_turn", wait_until_cancelled)
    claim_execution_interrupt = connector.claim_execution_interrupt

    def claim_after_completion(project, thread_id, *, turn_id=""):  # noqa: ANN001, ANN202
        current = connector.index.get(project.id, thread_id)
        assert current is not None
        connector.index.save(replace(current, workspace={**current.workspace, "status": "ready", "provision_phase": "ready"}))
        return claim_execution_interrupt(project, thread_id, turn_id=turn_id)

    monkeypatch.setattr(connector, "claim_execution_interrupt", claim_after_completion)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def scenario(base: str, client: httpx.AsyncClient):
        await asyncio.wait_for(wait_started.wait(), timeout=0.5)
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_interrupt_recovery/interrupt",
            json={
                "turn_id": "turn_interrupt_recovery",
                "idempotency_key": "interrupt-recovery",
            },
        )
        restored = connector.index.get("neil-shared", "thread_interrupt_recovery")
        return response, restored

    response, restored = asyncio.run(
        _with_server(cfg, scenario, http_get=worker_get, http_post=worker_post)
    )

    assert response.status_code >= 500
    assert restored is not None
    assert restored.workspace["status"] == "interrupting"
    assert restored.workspace["provision_phase"] == "interrupting"
    assert restored.workspace["session_id"] == "orch_interrupt_recovery"
    assert restored.workspace[PENDING_EXECUTION_INTERRUPT_KEY]["turn_id"] == ("turn_interrupt_recovery")
    assert wait_calls == [("macbook-worker", "orch_interrupt_recovery", "turn_interrupt_recovery")]
    assert restored.workspace[PENDING_ORCHESTRATOR_COMPLETION_KEY]["turn_id"] == "turn_interrupt_recovery"


def test_cockpit_immediate_definite_interrupt_replay_rearms_pending_completion(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil", caps=WORKER_SESSION_INTERRUPT)
    _seed_project_registry(cfg)
    posts = 0

    def worker_post(url: str, **_kwargs: Any) -> Response:
        nonlocal posts
        if not url.endswith("/interrupt"):
            return Response(
                {"ok": False, "error": "unexpected worker post"},
                status_code=400,
            )
        posts += 1
        if posts == 1:
            raise OSError("interrupt response was lost")
        return Response({"ok": False, "error": "no such session"}, status_code=404)

    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
        worker_post=worker_post,
    )
    thread = CockpitThread(
        thread_id="thread_interrupt_immediate_rearm",
        project_id="neil-shared",
        session_id="project:neil-shared:thread_interrupt_immediate_rearm",
        title="Immediate interrupt rearm",
        created_at="2026-07-20T12:00:00Z",
        updated_at="2026-07-20T12:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        worker_id="macbook-worker",
        workspace={
            "worker_id": "macbook-worker",
            "session_id": "session_interrupt_immediate_rearm",
            "session_generation": 2,
            "status": "running",
            "provision_phase": "running",
            PENDING_ORCHESTRATOR_COMPLETION_KEY: {
                "phase": "accepted",
                "worker_id": "macbook-worker",
                "session_id": "session_interrupt_immediate_rearm",
                "session_generation": 2,
                "turn_id": "turn_interrupt_immediate_rearm",
                "requester": {
                    "device_id": "local-mac",
                    "identity": "neil",
                    "scope": "personal",
                    "capabilities": [],
                    "channel": "cockpit",
                    "confidence": "strong",
                    "peer": "neil",
                },
            },
        },
    )
    connector.index.save(thread)
    rearmed: list[tuple[str, str]] = []

    def record_rearm(project, recovered):  # noqa: ANN001, ANN202
        rearmed.append((project.id, recovered.thread_id))
        return True

    monkeypatch.setattr(
        connector,
        "rearm_pending_orchestrator_completion",
        record_rearm,
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def scenario(base: str, client: httpx.AsyncClient):
        rearmed.clear()
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}/interrupt",
            json={
                "turn_id": "turn_interrupt_immediate_rearm",
                "idempotency_key": "interrupt-immediate-rearm",
            },
        )
        return response, connector.index.get(thread.project_id, thread.thread_id)

    response, recovered = asyncio.run(
        _with_server(cfg, scenario, http_post=worker_post)
    )

    assert response.status_code == 502
    assert posts == 2
    assert recovered is not None
    assert recovered.workspace["status"] == "running"
    assert PENDING_EXECUTION_INTERRUPT_KEY not in recovered.workspace
    assert rearmed == [("neil-shared", thread.thread_id)]


def test_cockpit_thread_controls_require_addressable_request_or_turn_ids(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        caps=f"{WORKER_SESSION_APPROVE},{WORKER_SESSION_INPUT},{WORKER_SESSION_INTERRUPT}",
    )
    _seed_project_registry(cfg)
    CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json").save(
        CockpitThread(
            thread_id="thread_ids",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_ids"),
            title="Addressable controls",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            workspace={"worker_id": "macbook-worker", "session_id": "conv_thread_ids"},
        )
    )
    writes: list[str] = []

    async def calls(base: str, client: httpx.AsyncClient) -> list[httpx.Response]:
        return [
            await client.post(
                f"{base}/v1/projects/neil-shared/threads/thread_ids/{action}",
                json={},
            )
            for action in ("approval", "input", "interrupt")
        ]

    responses = asyncio.run(
        _with_server(
            cfg,
            calls,
            http_post=lambda url, **_kwargs: writes.append(url) or Response({"ok": True}),
        )
    )

    assert [response.status_code for response in responses] == [400, 400, 400]
    assert [response.json()["error"]["message"] for response in responses] == [
        "request_id is required",
        "request_id is required",
        "turn_id is required",
    ]
    assert writes == []


def test_cockpit_thread_controls_reject_archived_unattached_and_legacy_executions_safely(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil", caps=WORKER_SESSION_APPROVE)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    common = {
        "project_id": "neil-shared",
        "session_id": orchestrator_session_id("neil-shared", "thread_control"),
        "title": "Control safety",
        "created_at": "2026-07-13T00:00:00Z",
        "updated_at": "2026-07-13T00:00:00Z",
        "created_by": "neil",
    }
    index.save(
        CockpitThread(
            thread_id="thread_archived",
            archived_at="2026-07-13T00:01:00Z",
            workspace={"worker_id": "macbook-worker", "session_id": "conv_archived"},
            **common,
        )
    )
    index.save(CockpitThread(thread_id="thread_unattached", **common))
    index.save(
        CockpitThread(
            thread_id="thread_legacy",
            workspace={"worker_id": "macbook-worker", "session_id": "conv_legacy"},
            **common,
        )
    )
    writes: list[str] = []

    def worker_post(url: str, **_kwargs: Any) -> Response:
        writes.append(url)
        return Response(
            {"ok": False, "error": "worker session missing required authority: worker.session.approve"},
            status_code=403,
        )

    async def calls(base: str, client: httpx.AsyncClient) -> list[httpx.Response]:
        return [
            await client.post(
                f"{base}/v1/projects/neil-shared/threads/{thread_id}/approval",
                json={"request_id": "approval_1", "decision": "approved"},
            )
            for thread_id in ("thread_archived", "thread_unattached", "thread_legacy")
        ]

    archived, unattached, legacy = asyncio.run(_with_server(cfg, calls, http_post=worker_post))

    assert archived.status_code == 409
    assert archived.json()["error"]["code"] == "thread_archived"
    assert unattached.status_code == 409
    assert unattached.json()["error"]["code"] == "execution_unavailable"
    assert legacy.status_code == 403
    assert legacy.json()["error"]["code"] == "forbidden"
    assert writes == ["http://worker.test/sessions/conv_legacy/approval"]


def test_cockpit_thread_control_requires_global_capability(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json").save(
        CockpitThread(
            thread_id="thread_no_capability",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_no_capability"),
            title="No authority",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            workspace={"worker_id": "macbook-worker", "session_id": "conv_no_capability"},
        )
    )
    writes: list[str] = []

    async def calls(base: str, client: httpx.AsyncClient) -> httpx.Response:
        return await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_no_capability/approval",
            json={"request_id": "approval_1", "decision": "approved"},
        )

    response = asyncio.run(
        _with_server(
            cfg,
            calls,
            http_post=lambda url, **_kwargs: writes.append(url) or Response({"ok": True}),
        )
    )

    assert response.status_code == 403
    assert response.json()["error"]["message"] == f"missing authority: {WORKER_SESSION_APPROVE}"
    assert writes == []


def test_cockpit_active_thread_turn_is_durably_queued_and_deduped_without_transcript_duplication(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    index.save(
        CockpitThread(
            thread_id="thread_queue",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_queue"),
            title="Queued conversation",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            workspace={
                "worker_id": "macbook-worker",
                "session_id": "conv_thread_queue",
                "status": "ready",
            },
        )
    )
    writes: list[str] = []

    def worker_get(url: str, **_kwargs: Any) -> Response:
        assert url.endswith("/sessions/conv_thread_queue/execution-state")
        return Response(
            {
                "session_id": "conv_thread_queue",
                "status": "running",
                "active_turn": {"turn_id": "turn_active", "status": "running"},
                "pending_requests": [],
                "supported_controls": ["turn", "interrupt"],
                "supports": {"steer": False, "queue": False},
            }
        )

    async def calls(base: str, client: httpx.AsyncClient) -> tuple[httpx.Response, httpx.Response, dict[str, Any]]:
        payload = {"text": "Please run the focused tests next.", "idempotency_key": "human-turn-2"}
        first = await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_queue/turns",
            json=payload,
        )
        duplicate = await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_queue/turns",
            json=payload,
        )
        detail = (
            await client.get(f"{base}/v1/projects/neil-shared/threads/thread_queue")
        ).json()["thread"]
        return first, duplicate, detail

    first, duplicate, detail = asyncio.run(
        _with_server(
            cfg,
            calls,
            http_get=worker_get,
            http_post=lambda url, **_kwargs: writes.append(url) or Response({"ok": True}),
        )
    )

    first_queued = [event for event in _sse_events(first.text) if event["_event"] == "thread.turn.queued"]
    duplicate_queued = [
        event for event in _sse_events(duplicate.text) if event["_event"] == "thread.turn.queued"
    ]
    assert first.status_code == 200
    assert first_queued[0]["payload"]["idempotent"] is False
    assert duplicate_queued[0]["payload"]["idempotent"] is True
    assert detail["queued_turns"] == [
        {
            "queue_id": first_queued[0]["payload"]["queue_id"],
            "text": "Please run the focused tests next.",
            "queued_at": detail["queued_turns"][0]["queued_at"],
            "status": "queued",
        }
    ]
    assert detail["messages"] == []
    assert detail["execution"]["supports"] == {"steer": False, "queue": True}
    assert writes == []


def test_cockpit_thread_queue_drains_human_turns_in_submission_order(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    memory = FakeProjectMemory()
    connector = CockpitConnector(
        cfg,
        memory=memory,
        gateway=FakeGateway(["First reply.", "Second reply."]),
        tts=None,
        tracer=None,
    )
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_fifo",
            project_id=project.id,
            session_id=orchestrator_session_id(project.id, "thread_fifo"),
            title="FIFO conversation",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
        )
    )
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    connector.index.enqueue_turn(
        project.id,
        thread.thread_id,
        requester=requester,
        text="First human message.",
        idempotency_key="fifo-1",
    )
    connector.index.enqueue_turn(
        project.id,
        thread.thread_id,
        requester=requester,
        text="Second human message.",
        idempotency_key="fifo-2",
    )

    drained = asyncio.run(connector.drain_queued_turns(project, thread.thread_id))

    stored = connector.index.get_with_messages(project.id, thread.thread_id)
    assert drained == 2
    assert stored is not None
    assert stored.queued_turns == ()
    assert [message["content"] for message in stored.messages] == [
        "First human message.",
        "First reply.",
        "Second human message.",
        "Second reply.",
    ]


def test_cockpit_startup_rearms_and_drains_a_claimed_durable_thread_turn(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway(["Recovered reply."]),
        tts=None,
        tracer=None,
    )
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_restart_queue",
            project_id=project.id,
            session_id=orchestrator_session_id(project.id, "thread_restart_queue"),
            title="Restart queue",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
        )
    )
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    connector.index.enqueue_turn(
        project.id,
        thread.thread_id,
        requester=requester,
        text="Resume me after restart.",
        idempotency_key="restart-1",
    )
    assert connector.index.claim_queued_turn(project.id, thread.thread_id) is not None
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    make_app(cfg)
    for _ in range(100):
        stored = connector.index.get_with_messages(project.id, thread.thread_id)
        if stored is not None and not stored.queued_turns:
            break
        time.sleep(0.02)

    assert stored is not None
    assert stored.queued_turns == ()
    assert [message["content"] for message in stored.messages] == [
        "Resume me after restart.",
        "Recovered reply.",
    ]


def test_cockpit_idle_thread_turn_replays_same_key_and_rejects_conflicting_payload(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    gateway = FakeGateway(["One durable reply."])
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=gateway,
        tts=None,
        tracer=None,
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> tuple[httpx.Response, httpx.Response, httpx.Response, str]:
        opened = (await client.post(f"{base}/v1/projects/neil-shared/threads", json={})).json()["thread"]
        url = f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}/turns"
        first = await client.post(url, json={"text": "Run this once.", "idempotency_key": "idle-once"})
        replay = await client.post(url, json={"text": "Run this once.", "idempotency_key": "idle-once"})
        conflict = await client.post(url, json={"text": "Different work.", "idempotency_key": "idle-once"})
        return first, replay, conflict, opened["thread_id"]

    first, replay, conflict, thread_id = asyncio.run(_with_server(cfg, calls))

    replayed = [event for event in _sse_events(replay.text) if event["_event"] == "thread.turn.replayed"]
    stored = connector.index.get_with_messages("neil-shared", thread_id)
    assert first.status_code == 200
    assert replayed[0]["payload"]["reply"] == "One durable reply."
    assert replayed[0]["payload"]["receipt_status"] == "completed"
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_conflict"
    assert gateway.calls == 1
    assert stored is not None
    assert [message["content"] for message in stored.messages] == ["Run this once.", "One durable reply."]
    receipt = next(item for item in stored.turn_receipts if item["idempotency_key"] == "idle-once")
    assert not ({"text", "requester", "workspace_request", "has_attachments"} & receipt.keys())


def test_cockpit_idle_turn_response_loss_replays_without_redispatch(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    gateway = FakeGateway(["Persisted before delivery."])
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=gateway,
        tts=None,
        tracer=None,
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)
    original_write_sse = cockpit_api_module._write_sse  # noqa: SLF001
    lost = False

    async def lose_first_reply(response, event, cursor, data):  # noqa: ANN001
        nonlocal lost
        if event == "thread.reply" and not lost:
            lost = True
            raise ConnectionResetError("simulated client loss")
        return await original_write_sse(response, event, cursor, data)

    monkeypatch.setattr(cockpit_api_module, "_write_sse", lose_first_reply)

    async def calls(base: str, client: httpx.AsyncClient) -> httpx.Response:
        opened = (await client.post(f"{base}/v1/projects/neil-shared/threads", json={})).json()["thread"]
        url = f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}/turns"
        await client.post(url, json={"text": "Do not repeat me.", "idempotency_key": "lost-response"})
        return await client.post(
            url,
            json={"text": "Do not repeat me.", "idempotency_key": "lost-response"},
        )

    replay = asyncio.run(_with_server(cfg, calls))

    replayed = [event for event in _sse_events(replay.text) if event["_event"] == "thread.turn.replayed"]
    assert replayed[0]["payload"]["reply"] == "Persisted before delivery."
    assert gateway.calls == 1


def test_cockpit_simultaneous_idle_same_key_dispatches_once(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    gateway = FakeGateway(["Only one dispatch."])
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=gateway,
        tts=None,
        tracer=None,
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)
    original_lookup = connector.index.foreground_turn_receipt
    barrier = threading.Barrier(2)

    def synchronized_lookup(*args, **kwargs):  # noqa: ANN002, ANN003
        receipt = original_lookup(*args, **kwargs)
        barrier.wait(timeout=2)
        return receipt

    monkeypatch.setattr(connector.index, "foreground_turn_receipt", synchronized_lookup)

    async def calls(base: str, client: httpx.AsyncClient) -> tuple[httpx.Response, httpx.Response, str]:
        opened = (await client.post(f"{base}/v1/projects/neil-shared/threads", json={})).json()["thread"]
        url = f"{base}/v1/projects/neil-shared/threads/{opened['thread_id']}/turns"
        payload = {"text": "Dispatch once concurrently.", "idempotency_key": "concurrent-once"}
        first, second = await asyncio.gather(client.post(url, json=payload), client.post(url, json=payload))
        return first, second, opened["thread_id"]

    first, second, thread_id = asyncio.run(_with_server(cfg, calls))

    stored = connector.index.get_with_messages("neil-shared", thread_id)
    assert first.status_code == 200
    assert second.status_code == 200
    assert gateway.calls == 1
    assert stored is not None
    assert [message["content"] for message in stored.messages] == [
        "Dispatch once concurrently.",
        "Only one dispatch.",
    ]


def test_cockpit_project_thread_turn_requires_idempotency_key(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json").save(
        CockpitThread(
            thread_id="thread_missing_key",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_missing_key"),
            title="Missing key",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
        )
    )

    async def calls(base: str, client: httpx.AsyncClient) -> httpx.Response:
        return await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_missing_key/turns",
            json={"text": "Missing key."},
        )

    response = asyncio.run(_with_server(cfg, calls, auto_turn_idempotency=False))

    assert response.status_code == 400
    assert response.json()["error"]["message"] == "idempotency_key is required"


def test_cockpit_queued_worker_turn_uses_stable_logical_and_idempotency_ids(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    posts: list[dict[str, Any]] = []

    def worker_get(url: str, **_kwargs: Any) -> Response:
        if url.endswith("/execution-state"):
            return Response({"status": "created", "active_turn": None})
        return Response({"session_id": "conv_stable", "status": "created"})

    def worker_post(url: str, **kwargs: Any) -> Response:
        assert url.endswith("/sessions/conv_stable/turns")
        posts.append(kwargs["json"])
        return Response({"ok": True})

    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
        worker_get=worker_get,
        worker_post=worker_post,
    )
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_stable_queue",
            project_id=project.id,
            session_id=orchestrator_session_id(project.id, "thread_stable_queue"),
            title="Stable queue",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            workspace={
                "worker_id": "macbook-worker",
                "session_id": "conv_stable",
                "status": "ready",
                "provision_phase": "ready",
            },
        )
    )
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    receipt, should_dispatch = connector.index.reserve_foreground_turn(
        project.id,
        thread.thread_id,
        requester=requester,
        text="Stable retry.",
        idempotency_key="stable-key",
        dispatch_mode="worker",
    )
    assert should_dispatch is True
    connector.index.recover_dispatching_turns(project.id, thread.thread_id)
    recovered = connector.index.get(project.id, thread.thread_id)
    assert recovered is not None
    queued = recovered.queued_turns[0]
    assert queued["queue_id"] == receipt["logical_turn_id"]

    asyncio.run(connector.drain_queued_turns(project, thread.thread_id))

    assert posts[0]["turn_id"] == queued["queue_id"]
    assert posts[0]["idempotency_key"] == f"thread-turn:{thread.thread_id}:stable-key"
    completed = connector.index.get(project.id, thread.thread_id)
    assert completed is not None
    completed_receipt = next(item for item in completed.turn_receipts if item["idempotency_key"] == "stable-key")
    assert completed_receipt["status"] == "completed"
    assert {"text", "requester", "workspace_request", "has_attachments"}.isdisjoint(completed_receipt)


def test_cockpit_startup_blocks_ambiguous_brain_receipt_instead_of_redispatching(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_uncertain_brain",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_uncertain_brain"),
            title="Uncertain brain turn",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            worker_id="preferred-worker-only",
        )
    )
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    connector.index.reserve_foreground_turn(
        thread.project_id,
        thread.thread_id,
        requester=requester,
        text="Potentially ambiguous work.",
        idempotency_key="uncertain-key",
        dispatch_mode="brain",
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> httpx.Response:
        return await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}/turns",
            json={"text": "Potentially ambiguous work.", "idempotency_key": "uncertain-key"},
        )

    response = asyncio.run(_with_server(cfg, calls))

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "turn_outcome_uncertain"
    assert "new idempotency_key" in response.json()["error"]["message"]
    uncertain = connector.index.get(thread.project_id, thread.thread_id)
    assert uncertain is not None
    uncertain_receipt = next(item for item in uncertain.turn_receipts if item["idempotency_key"] == "uncertain-key")
    assert uncertain_receipt["status"] == "uncertain"
    assert {"text", "requester", "workspace_request", "has_attachments"}.isdisjoint(uncertain_receipt)


def test_cockpit_dispatching_receipt_replays_in_progress_without_terminal_done(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None)
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_dispatching_replay",
            project_id="neil-shared",
            session_id="session_dispatching_replay",
            title="Dispatching",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
        )
    )
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> httpx.Response:
        connector.index.reserve_foreground_turn(
            thread.project_id,
            thread.thread_id,
            requester=requester,
            dispatch_mode="brain",
            text="Still running.",
            idempotency_key="dispatching-key",
        )
        return await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}/turns",
            json={"text": "Still running.", "idempotency_key": "dispatching-key"},
        )

    response = asyncio.run(_with_server(cfg, calls))
    events = _sse_events(response.text)
    assert [event["_event"] for event in events] == ["thread.turn.in_progress"]


def test_cockpit_completed_queue_receipt_replays_as_completed_not_queued(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None)
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_completed_queue",
            project_id="neil-shared",
            session_id="session_completed_queue",
            title="Completed queue",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
        )
    )
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    _stored, queued, _created = connector.index.enqueue_turn(
        thread.project_id,
        thread.thread_id,
        requester=requester,
        text="Already completed.",
        idempotency_key="completed-queue-key",
    )
    connector.index.finish_queued_turn(thread.project_id, thread.thread_id, queued["queue_id"])
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> httpx.Response:
        return await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}/turns",
            json={"text": "Already completed.", "idempotency_key": "completed-queue-key"},
        )

    response = asyncio.run(_with_server(cfg, calls))
    assert [event["_event"] for event in _sse_events(response.text)] == [
        "thread.turn.replayed",
        "thread.turn.done",
    ]


def test_cockpit_recovery_never_exceeds_queue_cap(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_recovery_cap",
            project_id="neil-shared",
            session_id="session_recovery_cap",
            title="Recovery cap",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            workspace={"worker_id": "macbook-worker", "session_id": "conv_recovery_cap"},
        )
    )
    for index in range(32):
        connector.index.enqueue_turn(
            thread.project_id,
            thread.thread_id,
            requester=requester,
            text=f"Queued {index}",
            idempotency_key=f"queued-{index}",
        )
    connector.index.reserve_foreground_turn(
        thread.project_id,
        thread.thread_id,
        requester=requester,
        dispatch_mode="worker",
        text="Overflow recovery.",
        idempotency_key="overflow-recovery",
    )

    recovered = connector.index.recover_dispatching_turns(thread.project_id, thread.thread_id)
    assert recovered is not None
    assert len(recovered.queued_turns) == 32
    receipt = next(item for item in recovered.turn_receipts if item["idempotency_key"] == "overflow-recovery")
    assert receipt["status"] == "retry_required"
    assert {"text", "requester", "workspace_request", "has_attachments"}.isdisjoint(receipt)


def test_cockpit_queue_rejects_raw_attachments_without_persisting_them(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    thread = connector.index.save(
        CockpitThread(
            thread_id="thread_attachment_queue",
            project_id="neil-shared",
            session_id="session_attachment_queue",
            title="Attachment queue",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
        )
    )
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")

    with pytest.raises(ValueError, match="durable attachment references"):
        connector.index.enqueue_turn(
            thread.project_id,
            thread.thread_id,
            requester=requester,
            text="Queue this image.",
            idempotency_key="attachment-key",
            attachments=[{"name": "secret.png", "content_base64": "raw-secret-base64"}],
        )

    assert "raw-secret-base64" not in Path(connector.index.path).read_text()


def test_cockpit_queue_drainer_waits_through_active_completion_without_detail_poll(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    execution_reads = 0
    turn_posts: list[dict[str, Any]] = []

    def worker_get(url: str, **_kwargs: Any) -> Response:
        nonlocal execution_reads
        if url.endswith("/execution-state"):
            execution_reads += 1
            active = execution_reads <= 3
            return Response(
                {
                    "session_id": "conv_polling_queue",
                    "status": "running" if active else "created",
                    "active_turn": {"turn_id": "turn_old", "status": "running"} if active else None,
                    "pending_requests": [],
                    "supported_controls": ["turn"],
                    "supports": {"steer": False, "queue": False},
                }
            )
        return Response({"session_id": "conv_polling_queue", "status": "created"})

    def worker_post(url: str, **kwargs: Any) -> Response:
        assert url.endswith("/sessions/conv_polling_queue/turns")
        turn_posts.append(kwargs["json"])
        return Response({"ok": True})

    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
        worker_get=worker_get,
        worker_post=worker_post,
    )
    connector.index.save(
        CockpitThread(
            thread_id="thread_polling_queue",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_polling_queue"),
            title="Polling queue",
            created_at="2026-07-13T00:00:00Z",
            updated_at="2026-07-13T00:00:00Z",
            created_by="neil",
            workspace={
                "worker_id": "macbook-worker",
                "session_id": "conv_polling_queue",
                "status": "ready",
                "provision_phase": "ready",
            },
        )
    )
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> httpx.Response:
        return await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_polling_queue/turns",
            json={"text": "Run after completion.", "idempotency_key": "polling-key"},
        )

    response = asyncio.run(_with_server(cfg, calls, http_get=worker_get, http_post=worker_post))
    for _ in range(100):
        if turn_posts:
            break
        time.sleep(0.02)

    assert response.status_code == 200
    assert turn_posts[0]["idempotency_key"] == "thread-turn:thread_polling_queue:polling-key"


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
    assert thread["execution"]["supports"] == {"steer": False, "queue": True}


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
    session_create = next(call["json"] for call in state["posts"] if call["url"].endswith("/sessions"))
    granted = set(session_create["metadata"]["allowed_actions"])
    assert granted >= {
        WORKER_SESSION_INPUT,
        WORKER_SESSION_INTERRUPT,
    }
    # Conversation turns are awaited headlessly, so the session must act rather
    # than raise an approval nobody is there to answer.
    assert WORKER_SESSION_APPROVE not in granted
    assert session_create["metadata"]["execution_envelope"]["allowed_actions"] == session_create["metadata"][
        "allowed_actions"
    ]


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
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
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
    memory.create_session_error = cockpit_api_module.UnsupportedMemoryOperation("live memory unsupported")
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
        memory.create_messages_error = cockpit_api_module.UnsupportedMemoryOperation("live memory unsupported")
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


def test_cockpit_projects_public_canonical_tool_envelope_and_correlation() -> None:
    from jarvis.orchestration.cockpit import project_session_event

    event = project_session_event(
        {
            "event_id": "ev_tool_1",
            "session_id": "sess_1",
            "type": "tool.call",
            "time": "2026-07-01T11:00:00Z",
            "data": {
                "turn_id": "turn_1",
                "provider": "codex",
                "tool_call_id": "call_1",
                "message_id": "call_1",
                "tool_name": "spawn_child_work_session",
                "server_name": "jarvis_orchestrator",
                "title": "jarvis_orchestrator · spawn_child_work_session",
                "item_type": "mcp_tool_call",
                "status": "in_progress",
                "input": {"title": "Review PR"},
                "item": {
                    "id": "call_1",
                    "type": "mcpToolCall",
                    "private_token": "must-not-leak",
                },
            },
        },
        worker_id="macbook-worker",
        run_id="run_1",
        sequence=1,
    )

    assert event["message_id"] == "call_1"
    assert event["data"] == {
        "turn_id": "turn_1",
        "provider": "codex",
        "tool_call_id": "call_1",
        "message_id": "call_1",
        "tool_name": "spawn_child_work_session",
        "server_name": "jarvis_orchestrator",
        "title": "jarvis_orchestrator · spawn_child_work_session",
        "item_type": "mcp_tool_call",
        "status": "in_progress",
        "input": {"title": "Review PR"},
        "item": {"id": "call_1", "type": "mcpToolCall"},
    }


def test_cockpit_correlates_legacy_nested_tool_result_ids() -> None:
    from jarvis.orchestration.cockpit import project_session_event

    event = project_session_event(
        {
            "event_id": "ev_tool_result_1",
            "session_id": "sess_1",
            "type": "tool.result",
            "time": "2026-07-01T11:00:00Z",
            "data": {"turn_id": "turn_1", "item": {"tool_use_id": "call_legacy", "content": "ok"}},
        },
        worker_id="macbook-worker",
        run_id="run_1",
        sequence=2,
    )

    assert event["message_id"] == "call_legacy"


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
    from jarvis.orchestration.workers import reset_probe_snapshots

    reset_probe_snapshots()
    cfg = _cfg(tmp_path, monkeypatch)
    rows = worker_profiles(worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, probe=False)

    # The static profile says online, but nothing has actually seen the worker.
    assert rows[0]["status"] == "online"
    assert rows[0]["last_seen_at"] == ""


def test_cockpit_unprobed_worker_reports_last_probed_inventory(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """The cockpit reads /v1/workers WITHOUT probe=true, and workers.json on disk
    carries no inventory — which is why the card showed 0 worktrees / 0 B while
    12 GB sat on the worker. An unprobed read now reports the last probe."""
    from jarvis.orchestration.cockpit import worker_profiles
    from jarvis.orchestration.workers import reset_probe_snapshots

    reset_probe_snapshots()
    cfg = _cfg(tmp_path, monkeypatch)
    health = {
        "ok": True,
        "runtime": {"version": "0.1.22", "channel": "dogfood", "git_sha": ""},
        "worktree_inventory": {"root": "/w/worktrees", "count": 79, "disk_bytes": 12_000_000_000, "stale_count": 79, "orphan_count": 12},
    }

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response({"sessions": []})
        return Response(health)

    probed = worker_profiles(
        worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, probe=True, http_get=get
    )
    unprobed = worker_profiles(worker_cfg=cfg.worker, workers_path=cfg.orchestration.workers_path, probe=False)

    assert probed[0]["worktree_inventory"]["count"] == 79
    assert unprobed[0]["worktree_inventory"]["count"] == 79
    assert unprobed[0]["worktree_inventory"]["orphan_count"] == 12
    assert unprobed[0]["runtime"]["version"] == "0.1.22"
    assert unprobed[0]["last_seen_at"] != ""


def test_cockpit_snapshot_keeps_model_catalog_after_probe_snapshot_ttl_expires(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _warm_probe_model_catalog_snapshot(cfg)
    probes_path = Path(cfg.orchestration.workers_path).with_name("workers.json.probes.json")
    raw = json.loads(probes_path.read_text())
    for row in raw["snapshots"].values():
        row["expires_at"] = time.time() - 60
    probes_path.write_text(json.dumps(raw))

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        return (await client.get(f"{base}/v1/cockpit/snapshot")).json()

    body = asyncio.run(_with_server(cfg, calls))

    worker = body["workers"][0]
    engines = {row["engine"]: row for row in worker["engines"]}
    assert worker["last_seen_at"] == ""
    assert worker["runtime"]["version"] == ""
    assert worker["worktree_inventory"]["count"] is None
    assert worker["worktree_inventory"]["status"] == "unknown"
    assert engines["codex"]["supports"]["models"] == [
        {"id": "gpt-x", "label": "GPT X"},
        {"id": "gpt-y", "label": "GPT Y"},
    ]
    assert engines["codex"]["supports"]["default_model"] == "gpt-x"
    assert engines["claude"]["supports"]["models"] == [{"id": "opus", "label": "Opus"}]


def test_cockpit_worker_probe_context_runs_enabled_sweep(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, worker_probe_interval_s="0.05")
    calls = []

    async def run() -> None:
        fired = asyncio.Event()

        def sweep(ctx):  # noqa: ANN001, ANN202
            calls.append(ctx)
            fired.set()
            return []

        monkeypatch.setattr(cockpit_api_module, "run_worker_probe_sweep", sweep)
        runner = web.AppRunner(make_app(cfg))
        await runner.setup()
        try:
            await asyncio.wait_for(fired.wait(), timeout=1)
        finally:
            await runner.cleanup()

    asyncio.run(run())

    assert calls


def test_cockpit_worker_probe_context_disabled_by_zero_knob(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, worker_probe_interval_s="0")

    def sweep(_ctx):  # noqa: ANN001, ANN202
        raise AssertionError("disabled worker probe loop should not sweep")

    monkeypatch.setattr(cockpit_api_module, "run_worker_probe_sweep", sweep)

    async def run() -> None:
        runner = web.AppRunner(make_app(cfg))
        await runner.setup()
        try:
            await asyncio.sleep(0.1)
        finally:
            await runner.cleanup()

    asyncio.run(run())


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


def test_orchestrator_turn_streams_and_persists_code_agent_events(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    occurred_at = "2026-07-22T10:30:48Z"
    worker_events = [
        {
            "event_id": "event_reasoning",
            "session_id": "session_a",
            "type": "reasoning.completed",
            "time": occurred_at,
            "data": {
                "turn_id": "turn_current",
                "message_id": "reasoning_1",
                "text": "Checked the transcript ordering.",
            },
        },
        {
            "event_id": "event_tool",
            "session_id": "session_a",
            "type": "tool.call",
            "time": occurred_at,
            "data": {
                "turn_id": "turn_current",
                "item": {"id": "call_1", "name": "web_search", "input": {"query": "result"}},
            },
        },
        {
            "event_id": "event_reply",
            "session_id": "session_a",
            "type": "assistant.message",
            "time": occurred_at,
            "data": {"turn_id": "turn_current", "text": "Done."},
        },
        {
            "event_id": "event_done",
            "session_id": "session_a",
            "type": "turn.completed",
            "time": occurred_at,
            "data": {"turn_id": "turn_current"},
        },
    ]
    monkeypatch.setattr(connector, "_get_worker_json", lambda *_args: {"events": worker_events})
    durable_events: list[dict[str, Any]] = []
    progress_events: list[dict[str, Any]] = []

    async def progress(update: dict[str, Any]) -> None:
        progress_events.append(update)

    connector._orchestrator_event_sinks["turn_current"] = (durable_events, progress)  # noqa: SLF001
    try:
        result = asyncio.run(
            connector._wait_for_orchestrator_turn("worker_a", "session_a", "turn_current")  # noqa: SLF001
        )
    finally:
        connector._orchestrator_event_sinks.pop("turn_current", None)  # noqa: SLF001

    assert result == "Done."
    assert [update["event"]["type"] for update in progress_events] == [
        "reasoning.completed",
        "tool.call",
    ]
    assert [event["type"] for event in durable_events] == ["reasoning.completed", "tool.call"]

    thread = CockpitThread(
        thread_id="thread_code_events",
        project_id="project_code_events",
        session_id="project:code-events",
        title="Code events",
        created_at=occurred_at,
        updated_at=occurred_at,
        created_by="neil",
    )
    connector.index.save(thread)
    updated = connector.index.append_turn(
        thread,
        user_peer_id="neil",
        user_text="Check this",
        assistant_peer_id="jarvis",
        assistant_text="Done.",
        events=durable_events,
        turn_id="turn_current",
    )
    assert [message["role"] for message in updated.messages] == ["user", "event", "event", "assistant"]
    assert {message.get("turn_id") for message in updated.messages} == {"turn_current"}
    assert updated.messages[1]["type"] == "reasoning.completed"
    assert updated.messages[1]["data"]["text"] == "Checked the transcript ordering."
    assert updated.messages[2]["call_id"] == "call_1"


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
    posts: list[tuple[str, dict[str, Any]]] = []

    def post(_worker_id: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        posts.append((path, body))
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

    assert [path for path, _body in posts] == ["/conversation-workspaces", "/sessions"]
    assert retried.workspace["session_id"] == "orch_thread-retry_1"
    assert retried.workspace["provider_started"] is False
    assert retried.workspace["status"] == "ready"
    session_create = posts[-1][1]
    granted = set(session_create["metadata"]["allowed_actions"])
    assert granted >= {
        WORKER_SESSION_INPUT,
        WORKER_SESSION_INTERRUPT,
    }
    # The orchestrator acts autonomously: holding approve authority would make
    # the provider ask for approvals no one can answer on a headless turn.
    assert WORKER_SESSION_APPROVE not in granted
    assert session_create["metadata"]["landing"]["mode"] == "branch_only"
    assert session_create["metadata"]["execution_envelope"]["allowed_actions"] == session_create["metadata"][
        "allowed_actions"
    ]


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

    waiting_parent = index.get(thread.project_id, thread.thread_id)
    assert waiting_parent is not None
    assert waiting_parent.workspace["pending_child_watch_ids"] == [watch_id]
    assert cockpit_api_module._thread_status(waiting_parent, None) == ("running", "")

    appended_parent = index.append_turn(
        thread,
        user_peer_id="neil",
        user_text="Start the review",
        assistant_peer_id="jarvis",
        assistant_text="Watching both reviewers.",
    )
    assert appended_parent.workspace["pending_child_watch_ids"] == [watch_id]
    assert cockpit_api_module._thread_status(appended_parent, None) == ("running", "")

    assert index.claim_ready_child_watch(thread.thread_id, {"run_a"}) is None
    claimed = index.claim_ready_child_watch(thread.thread_id, {"run_a", "run_b"})
    assert claimed is not None
    assert claimed["watch_id"] == watch_id
    assert claimed["requester"]["device_id"] == "local-mac"
    assert claimed["requester"]["capabilities"] == ["orchestration.runs.read"]
    assert claimed["continuation_instruction"] == "Publish the reconciled review before reporting success."
    assert index.claim_ready_child_watch(thread.thread_id, {"run_a", "run_b"}) is None

    index.finish_child_watch(thread.thread_id, watch_id)
    completed_parent = index.get(thread.project_id, thread.thread_id)
    assert completed_parent is not None
    assert "pending_child_watch_ids" not in completed_parent.workspace
    assert cockpit_api_module._thread_status(completed_parent, None) == ("completed", "completed")
    completed_detail = index.get_with_messages(thread.project_id, thread.thread_id)
    assert completed_detail is not None
    projected_watch = next(
        message
        for message in cockpit_api_module._thread_detail_projection(completed_detail)["messages"]
        if message.get("type") == "child_watch"
    )
    assert projected_watch == {
        "role": "system",
        "peer_id": "jarvis",
        "content": "Watching 2 child work session(s) for completion.",
        "observed_at": projected_watch["observed_at"],
        "type": "child_watch",
        "watch_id": watch_id,
        "child_chat_ids": ["run_a", "run_b"],
        "phase": "completed",
        "completed_at": projected_watch["completed_at"],
    }


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


def test_turn_failure_message_extracts_provider_detail() -> None:
    from jarvis.worker_session_contract import turn_failure_message

    codex_payload = {
        "provider_status": "failed",
        "raw": {
            "turn": {
                "status": "failed",
                "error": {
                    "codexErrorInfo": "usageLimitExceeded",
                    "message": "You've hit your usage limit.",
                },
            }
        },
    }
    assert turn_failure_message(codex_payload) == "usageLimitExceeded: You've hit your usage limit."
    assert turn_failure_message({"error": "worker exploded"}) == "worker exploded"
    claude_payload = {
        "provider_status": "error_max_turns",
        "raw": {"is_error": True, "result": "ran out of turns", "subtype": "error_max_turns"},
    }
    assert turn_failure_message(claude_payload) == "ran out of turns"
    assert turn_failure_message({"provider_status": "error_during_execution", "raw": {}}) == "error_during_execution"
    assert turn_failure_message({"provider_status": "failed", "raw": {}}) == ""
    assert turn_failure_message(None) == ""


def test_orchestrator_turn_failure_surfaces_provider_error(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )

    def get_events(_worker_id: str, path: str) -> dict:
        return {
            "events": [
                {
                    "event_id": "event_1",
                    "type": "turn.failed",
                    "data": {
                        "turn_id": "turn_current",
                        "provider": "codex",
                        "provider_status": "failed",
                        "raw": {
                            "turn": {
                                "status": "failed",
                                "error": {
                                    "codexErrorInfo": "usageLimitExceeded",
                                    "message": "You've hit your usage limit.",
                                },
                            }
                        },
                    },
                }
            ]
        }

    monkeypatch.setattr(connector, "_get_worker_json", get_events)

    with pytest.raises(ProviderTurnError, match="usageLimitExceeded"):
        asyncio.run(
            connector._wait_for_orchestrator_turn(  # noqa: SLF001
                "worker_a",
                "session_a",
                "turn_current",
            )
        )


def test_orchestrator_turn_engine_override_switches_idle_thread(tmp_path, monkeypatch) -> None:  # noqa: ANN001
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
        thread_id="thread_engine_switch",
        project_id=project.id,
        session_id=orchestrator_session_id(project.id, "thread_engine_switch"),
        title="Plan",
        created_at="2026-07-13T10:00:00Z",
        updated_at="2026-07-13T10:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="codex",
        model="gpt-5.5",
        workspace={
            "worker_id": "worker_a",
            "session_id": "orch_thread_engine_switch",
            "provider_started": True,
            "status": "ready",
            "session_generation": 3,
        },
    )
    connector.index.save(thread)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    seen: list[CockpitThread] = []

    async def fake_orchestrator_turn(_project, context_thread, *_args, **_kwargs):  # noqa: ANN001, ANN002, ANN003
        seen.append(context_thread)
        return "ok", context_thread

    monkeypatch.setattr(connector, "_orchestrator_turn", fake_orchestrator_turn)

    reply, updated, events = asyncio.run(
        connector.turn(
            project,
            thread,
            requester,
            "switch to claude",
            workspace_request={"engine": "claude"},
        )
    )

    assert reply == "ok"
    assert events == ()
    assert seen[0].engine == "claude"
    stored = connector.index.get(project.id, thread.thread_id)
    assert stored is not None
    assert stored.engine == "claude"
    assert stored.model == ""
    assert stored.workspace["session_id"] == ""
    assert stored.workspace["provider_started"] is False
    assert stored.workspace["session_generation"] == 4

    with pytest.raises(ValueError, match="orchestrator engine must be codex or claude"):
        asyncio.run(
            connector.turn(
                project,
                thread,
                requester,
                "switch to something else",
                workspace_request={"engine": "gpt4"},
            )
        )

    same = asyncio.run(
        connector.turn(
            project,
            thread,
            requester,
            "same engine keeps the session",
            workspace_request={"engine": "claude"},
        )
    )
    assert same[0] == "ok"
    unchanged = connector.index.get(project.id, thread.thread_id)
    assert unchanged is not None
    assert unchanged.workspace["session_generation"] == 4


def test_project_thread_turn_maps_provider_failures_to_engine_error(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    connector = CockpitConnector(
        cfg,
        memory=memory,
        gateway=FakeGateway(["unused"]),
        tts=None,
        tracer=None,
    )

    async def failing_turn(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise ProviderTurnError("usageLimitExceeded: You've hit your usage limit.")

    monkeypatch.setattr(connector, "turn", failing_turn)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        opened = await client.post(f"{base}/v1/projects/neil-shared/threads", json={"title": "Plan"})
        thread = opened.json()["thread"]
        failed = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread['thread_id']}/turns",
            json={"text": "Run a turn.", "idempotency_key": "turn-engine-error-1"},
        )
        return {"events": _sse_events(failed.text)}

    result = asyncio.run(_with_server(cfg, calls))

    errors = [event for event in result["events"] if event["_event"] == "thread.turn.error"]
    assert errors
    payload = json.dumps(errors[0])
    assert "engine_error" in payload
    assert "usageLimitExceeded" in payload


def test_child_work_repo_resolves_registry_alias_to_remote() -> None:
    from jarvis.brain.registry import ProjectEntry, RepoEntry
    from jarvis.connectors.cockpit import _child_work_repo

    project = ProjectEntry(
        id="jarvis",
        name="Jarvis",
        owner="neil",
        members=("neil",),
        repos=(
            RepoEntry(name="runtime", remote="roughcoder/jarvis", default=True),
            RepoEntry(name="cockpit", remote="roughcoder/jarvis-cockpit"),
        ),
    )

    assert _child_work_repo(project, "cockpit") == "roughcoder/jarvis-cockpit"
    assert _child_work_repo(project, "roughcoder/jarvis-cockpit") == "roughcoder/jarvis-cockpit"
    assert _child_work_repo(project, "") == "roughcoder/jarvis"
    assert _child_work_repo(project, "other-org/other-repo") == "other-org/other-repo"
    assert _child_work_repo(project, "/Users/someone/Development/jarvis-cockpit") == "roughcoder/jarvis-cockpit"
    assert _child_work_repo(project, "~/src/runtime") == "roughcoder/jarvis"
    with pytest.raises(ValueError, match="worker-local path"):
        _child_work_repo(project, "/opt/checkouts/unrelated-repo")
    empty = ProjectEntry(id="bare", name="Bare", owner="neil", members=("neil",))
    assert _child_work_repo(empty, "", default="fallback/repo") == "fallback/repo"


def test_child_watch_continuation_survives_worker_restart(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    parent = CockpitThread(
        thread_id="thread_restart_join",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_restart_join",
        title="Restart drill",
        created_at="2026-07-13T12:00:00Z",
        updated_at="2026-07-13T12:00:00Z",
        created_by="neil",
    )
    index.save(parent)
    requester = RequestContext(
        device_id="local-mac",
        identity="neil",
        scope="personal",
        capabilities=frozenset({"orchestration.runs.read"}),
    )
    index.register_child_watch(parent, ["run_restart"], requester=requester)
    watch = index.claim_ready_child_watch(parent.thread_id, {"run_restart"})
    assert watch is not None
    attempts: list[str] = []

    async def turn(_self, _project, _thread, _requester, instruction):  # noqa: ANN001
        attempts.append(instruction)
        if len(attempts) < 3:
            raise ConnectionRefusedError(61, "Connection refused")
        return "done", parent, []

    monkeypatch.setattr(CockpitConnector, "turn", turn)

    _continue_child_watch(cfg, parent.thread_id, watch)

    assert len(attempts) == 3
    stored = index.get(parent.project_id, parent.thread_id)
    assert stored is not None
    records = [
        message
        for message in index._thread_messages(stored)  # noqa: SLF001
        if message.get("type") == "child_watch"
    ]
    assert records
    assert not records[-1].get("error")


def test_worker_state_refresh_is_single_flight_under_a_hung_worker(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    import threading as _threading

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
    entered = _threading.Event()
    release = _threading.Event()
    calls: list[str] = []

    def slow_hub_worker_state(_ctx, mode, *_args, **_kwargs):  # noqa: ANN001, ANN002, ANN003
        calls.append(mode)
        entered.set()
        # Model an unresponsive worker: the read blocks until its timeout.
        release.wait(timeout=5)
        return {"workers": [], "mode": mode}

    monkeypatch.setattr(cockpit_api_module, "_hub_worker_state", slow_hub_worker_state)

    first = _threading.Thread(
        target=lambda: cockpit_api_module._refresh_worker_state(ctx, "all", None),  # noqa: SLF001
        daemon=True,
    )
    first.start()
    assert entered.wait(timeout=5)

    # A second refresh while the first is stuck must not queue another blocking
    # read; it returns immediately with the last-known projection.
    ctx.worker_state_cache["all"] = {"workers": [], "mode": "all", "cached": True}
    done = _threading.Event()

    def second() -> None:
        cockpit_api_module._refresh_worker_state(ctx, "all", None)  # noqa: SLF001
        done.set()

    _threading.Thread(target=second, daemon=True).start()
    assert done.wait(timeout=2), "second refresh convoyed on the stuck worker read"
    assert calls == ["all"]

    release.set()
    first.join(timeout=5)
    assert calls == ["all"]
    assert "all" not in ctx.worker_state_refresh_modes


def test_restart_recovery_releases_orphaned_execution_and_drains_queue(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    thread = CockpitThread(
        thread_id="thread_orphaned_lease",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_orphaned_lease",
        title="Drill",
        created_at="2026-07-13T14:00:00Z",
        updated_at="2026-07-13T14:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="claude",
        workspace={
            "worker_id": "worker_a",
            "session_id": "orch_thread_orphaned_lease",
            "provider_started": True,
            # The process that owned this in-flight turn died mid-turn.
            "status": "running",
            "provision_phase": "running",
            "session_generation": 5,
        },
    )
    index.save(thread)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    index.enqueue_turn(
        thread.project_id,
        thread.thread_id,
        requester=requester,
        text="Queued behind the dead turn.",
        idempotency_key="queued-1",
    )
    stored = index.get(thread.project_id, thread.thread_id)
    assert stored is not None
    assert len(stored.queued_turns) == 1

    recovered = index.recover_orphaned_execution(thread.project_id, thread.thread_id)

    assert recovered is not None
    assert recovered.workspace["status"] == "ready"
    assert recovered.workspace["provision_phase"] == "ready"
    assert recovered.workspace["provider_started"] is False
    # The provider session itself is preserved for the next turn to re-ensure.
    assert recovered.workspace["session_id"] == "orch_thread_orphaned_lease"
    execution = cockpit_api_module._thread_execution_projection(recovered, None)  # noqa: SLF001
    assert not cockpit_api_module._thread_execution_is_active(execution)  # noqa: SLF001


def test_orchestrator_session_authority_is_not_read_only_or_plan_mode() -> None:
    from jarvis.connectors.cockpit import (
        CONVERSATION_SESSION_ALLOWED_ACTIONS,
        CONVERSATION_SESSION_LANDING,
    )
    from jarvis.worker.authority import WorkerSessionAuthority

    authority = WorkerSessionAuthority(
        allowed_actions=list(CONVERSATION_SESSION_ALLOWED_ACTIONS),
        landing=dict(CONVERSATION_SESSION_LANDING),
        trusted_mcp_servers=["jarvis_orchestrator"],
    )

    # The orchestrator must be able to act on the work it coordinates. Plan mode
    # stays an operator choice, never an imposed default.
    assert authority.codex_sandbox == "workspace-write"
    assert authority.claude_permission_mode != "plan"
    assert authority.claude_tool_denial("Bash") == ""
    assert authority.claude_tool_denial("Edit") == ""
    assert authority.claude_tool_denial("mcp__jarvis_orchestrator__spawn_child_work_session") == ""
    # An orchestrator turn runs headless: asking for approval would hang it, so
    # the session must be minted to act without one.
    assert authority.codex_approval_policy == "never"
    assert authority.claude_permission_mode == "dontAsk"
    assert not authority.can_resolve_approval


def test_orchestrator_turn_fails_closed_on_an_unanswerable_approval(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )

    def get_events(_worker_id: str, _path: str) -> dict:
        return {
            "events": [
                {
                    "event_id": "event_1",
                    "type": "approval.requested",
                    "data": {"turn_id": "turn_current", "request_id": "req_1"},
                }
            ]
        }

    monkeypatch.setattr(connector, "_get_worker_json", get_events)

    with pytest.raises(ProviderTurnError, match="approval"):
        asyncio.run(
            connector._wait_for_orchestrator_turn("worker_a", "session_a", "turn_current")  # noqa: SLF001
        )


def test_spawn_child_rejects_an_unknown_engine_with_a_truthful_error(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.connectors.cockpit import _spawn_child_work_tool

    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    thread = CockpitThread(
        thread_id="thread_engine_validation",
        project_id=project.id,
        session_id="project:neil-shared:orchestrator:thread_engine_validation",
        title="Spawn",
        created_at="2026-07-13T15:00:00Z",
        updated_at="2026-07-13T15:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
    )
    tool = _spawn_child_work_tool(cfg, project, thread)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")

    nested_policy = tool.parameters["properties"]["allow_nested_agents"]
    assert nested_policy == {
        "type": "boolean",
        "default": True,
        "description": "Allow the child to launch nested agents. Disable for synchronous review work.",
    }
    invalid = asyncio.run(tool.handler(requester, {"task": "list dirs", "allow_nested_agents": "false"}))
    assert invalid == "error: allow_nested_agents must be a boolean"

    result = asyncio.run(tool.handler(requester, {"task": "list dirs", "engine": "claude-engine"}))

    # An unknown engine used to match no worker and surface as the misleading
    # "No eligible worker found".
    assert "unknown engine" in result
    assert "claude-engine" in result
    assert "claude" in result and "codex" in result


def test_spawn_child_carries_nested_agent_policy_into_the_execution_envelope(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.connectors.cockpit import _spawn_child_work_tool
    from jarvis.orchestration.service import OrchestrationService

    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    thread = CockpitThread(
        thread_id="thread_nested_policy",
        project_id=project.id,
        session_id="project:neil-shared:orchestrator:thread_nested_policy",
        title="Spawn",
        created_at="2026-07-13T15:00:00Z",
        updated_at="2026-07-13T15:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
    )
    seen: list[bool] = []

    def next_work(_self, command, **_kwargs):  # noqa: ANN001
        seen.append(command.allow_nested_agents)
        item = WorkItem(source="manual", id="manual_policy", title="Review", repo="roughcoder/jarvis")
        envelope = ExecutionEnvelope(
            run_id="run_policy",
            repo=item.repo,
            prompt="review",
            allow_nested_agents=command.allow_nested_agents,
        )
        return StartedWork(
            item=item,
            worker=WorkerProfile(worker_id="worker_a", display_name="Worker A"),
            envelope=envelope,
            session=WorkerSessionLink(worker_id="worker_a", session_id="session_policy"),
        )

    monkeypatch.setattr(OrchestrationService, "next_work", next_work)
    tool = _spawn_child_work_tool(cfg, project, thread)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")

    disabled = asyncio.run(tool.handler(requester, {"task": "review", "allow_nested_agents": False}))
    defaulted = asyncio.run(tool.handler(requester, {"task": "review"}))

    assert seen == [False, True]
    assert disabled.startswith("Spawned child chat run_policy")
    assert defaulted.startswith("Spawned child chat run_policy")


def test_spawn_child_validates_and_propagates_access_mode(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.connectors.cockpit import _spawn_child_work_tool
    from jarvis.orchestration.service import OrchestrationService

    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    thread = CockpitThread(
        thread_id="thread_access_policy",
        project_id=project.id,
        session_id="project:neil-shared:orchestrator:thread_access_policy",
        title="Spawn",
        created_at="2026-07-13T15:00:00Z",
        updated_at="2026-07-13T15:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
    )
    seen: list[str] = []

    def next_work(_self, command, **_kwargs):  # noqa: ANN001
        seen.append(command.access_mode)
        return StartedWork(
            item=WorkItem(source="manual", id="manual_access", title="Review", repo="roughcoder/jarvis"),
            worker=WorkerProfile(worker_id="worker_a", display_name="Worker A"),
            envelope=ExecutionEnvelope(run_id="run_access", repo="roughcoder/jarvis", prompt="review"),
            session=WorkerSessionLink(worker_id="worker_a", session_id="session_access"),
        )

    monkeypatch.setattr(OrchestrationService, "next_work", next_work)
    tool = _spawn_child_work_tool(cfg, project, thread)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")

    assert tool.parameters["properties"]["access_mode"]["enum"] == [
        "full_trust",
        "interactive",
        "read_only",
    ]
    invalid = asyncio.run(tool.handler(requester, {"task": "review", "access_mode": "root"}))
    explicit = asyncio.run(tool.handler(requester, {"task": "review", "access_mode": "full_trust"}))
    defaulted = asyncio.run(tool.handler(requester, {"task": "review"}))

    assert invalid == "error: unsupported access_mode: root"
    assert explicit.startswith("Spawned child chat run_access")
    assert defaulted.startswith("Spawned child chat run_access")
    assert seen == ["full_trust", "read_only"]


def test_child_watch_claim_renewal_does_not_grow_the_transcript(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.connectors import cockpit as cockpit_connector_module

    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    parent = CockpitThread(
        thread_id="thread_renewal",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_renewal",
        title="Join",
        created_at="2026-07-13T16:00:00Z",
        updated_at="2026-07-13T16:00:00Z",
        created_by="neil",
    )
    index.save(parent)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    index.register_child_watch(parent, ["run_a"], requester=requester)
    watch = index.claim_ready_child_watch(parent.thread_id, {"run_a"})
    assert watch is not None
    watch_id = str(watch["watch_id"])

    # The projection collapses a watch to its latest record, so growth is only
    # visible in the append-only transcript itself.
    transcript = index._transcript_path(parent.project_id, parent.thread_id)  # noqa: SLF001

    def transcript_lines() -> int:
        return len([line for line in transcript.read_text().splitlines() if line.strip()])

    baseline = transcript_lines()

    # The continuation loop renews on every retry tick while it waits. A live
    # lease must not append a record each time: one join once wrote 158.
    for _ in range(40):
        index.renew_child_watch_claim(parent.thread_id, watch_id)

    assert transcript_lines() == baseline

    # A lease that has aged past the heartbeat interval still renews, so a long
    # join keeps its claim alive.
    monkeypatch.setattr(cockpit_connector_module, "CHILD_WATCH_RENEW_INTERVAL_S", -60)

    index.renew_child_watch_claim(parent.thread_id, watch_id)

    assert transcript_lines() == baseline + 1


def test_completed_child_watch_is_never_reclaimed(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    parent = CockpitThread(
        thread_id="thread_reclaim",
        project_id="neil-shared",
        session_id="project:neil-shared:orchestrator:thread_reclaim",
        title="Join",
        created_at="2026-07-13T16:00:00Z",
        updated_at="2026-07-13T16:00:00Z",
        created_by="neil",
    )
    index.save(parent)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    index.register_child_watch(parent, ["run_a"], requester=requester)

    claimed = index.claim_ready_child_watch(parent.thread_id, {"run_a"})
    assert claimed is not None
    watch_id = str(claimed["watch_id"])
    assert index.child_watch_is_claimed(parent.thread_id, watch_id)

    index.finish_child_watch(parent.thread_id, watch_id)

    # The append-only transcript still holds the original "waiting" record. Only
    # the latest record is the effective state, so the finished watch must not
    # be claimable again — a second claim would fire a duplicate continuation.
    assert not index.child_watch_is_claimed(parent.thread_id, watch_id)
    assert index.claim_ready_child_watch(parent.thread_id, {"run_a"}) is None


def test_publish_review_tool_refuses_to_claim_an_unposted_review(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.connectors.cockpit import _publish_github_pr_review_tool

    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None

    class _Worker:
        worker_id = "worker_a"
        base_url = "http://worker.test"
        token_env = ""

    from jarvis.connectors import cockpit as cockpit_connector_module

    monkeypatch.setattr(cockpit_connector_module, "_github_review_worker", lambda *_a, **_k: _Worker())

    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict[str, Any]:
            # The worker posted nothing: GitHub returned no review.
            return {"ok": True, "review": {"review_id": 0, "url": "", "comments": 0}}

    monkeypatch.setattr(cockpit_connector_module.httpx, "post", lambda *_a, **_k: _Response())

    tool = _publish_github_pr_review_tool(cfg, project)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")

    repo = project.repos[0].remote
    result = asyncio.run(
        tool.handler(
            requester,
            {"repo": repo, "pull_number": 7, "commit_id": "abc", "summary": "s", "comments": []},
        )
    )

    # A receipt must describe what happened: claiming "published" here let an
    # orchestrator faithfully report a review that was never posted.
    assert result.startswith("error:")
    assert "nothing was posted" in result


def test_github_review_worker_stops_after_first_publish_capable_candidate(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.connectors import cockpit as cockpit_connector_module
    from jarvis.connectors.cockpit import _github_review_worker

    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    seen: list[str] = []
    repo_probes: list[str] = []

    class _Registry:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def candidates(self, *, probe):  # noqa: ANN001, ANN202
            assert probe is True
            for worker_id, status, authenticated in (
                ("offline", "offline", False),
                ("eligible", "online", True),
                ("unused", "online", True),
            ):
                seen.append(worker_id)
                yield SimpleNamespace(
                    worker_id=worker_id,
                    status=status,
                    base_url=f"http://{worker_id}",
                    git_identity={"authenticated": authenticated},
                    repo_access=[],
                )

        def with_repo_access(self, profiles, repo):  # noqa: ANN001, ANN202
            repo_probes.append(profiles[0].worker_id)
            profiles[0].repo_access = [{"repo": repo, "accessible": True}]
            return profiles

    monkeypatch.setattr(cockpit_connector_module, "WorkerRegistry", _Registry)

    selected = _github_review_worker(cfg, project, project.repos[0].remote)

    assert selected.worker_id == "eligible"
    assert seen == ["offline", "eligible"]
    assert repo_probes == ["eligible"]


def test_publish_review_tool_rejects_a_replayed_zero_id_result(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.connectors.cockpit import _publish_github_pr_review_tool

    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None

    class _Worker:
        worker_id = "worker_a"
        base_url = "http://worker.test"
        token_env = ""

    from jarvis.connectors import cockpit as cockpit_connector_module

    monkeypatch.setattr(cockpit_connector_module, "_github_review_worker", lambda *_a, **_k: _Worker())

    class _Response:
        status_code = 200

        @staticmethod
        def json() -> dict[str, Any]:
            # A cached idempotency result from an earlier empty response: the
            # worker replays it, but no GitHub review was ever created.
            return {"ok": True, "replayed": True, "review": {"review_id": 0, "url": "", "comments": 0}}

    monkeypatch.setattr(cockpit_connector_module.httpx, "post", lambda *_a, **_k: _Response())

    tool = _publish_github_pr_review_tool(cfg, project)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")

    repo = project.repos[0].remote
    result = asyncio.run(
        tool.handler(
            requester,
            {"repo": repo, "pull_number": 7, "commit_id": "abc", "summary": "s", "comments": []},
        )
    )

    # A zero id means no review exists, replayed or not. Trusting the replay
    # flag preserved the false success on every retry.
    assert result.startswith("error:")
    assert "nothing was posted" in result


# --- mid-conversation model switching ---------------------------------------


def test_orchestrator_turn_model_override_respawns_the_session(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """A model change takes the engine-change path: same engine, fresh session."""
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
        thread_id="thread_model_switch",
        project_id=project.id,
        session_id=orchestrator_session_id(project.id, "thread_model_switch"),
        title="Plan",
        created_at="2026-07-19T10:00:00Z",
        updated_at="2026-07-19T10:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="codex",
        model="gpt-5.5",
        workspace={
            "worker_id": "worker_a",
            "session_id": "orch_thread_model_switch",
            "provider_started": True,
            "status": "ready",
            "session_generation": 3,
            "model": "gpt-5.5",
        },
    )
    connector.index.save(thread)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    seen: list[CockpitThread] = []

    async def fake_orchestrator_turn(_project, context_thread, *_args, **_kwargs):  # noqa: ANN001, ANN002, ANN003
        seen.append(context_thread)
        return "ok", context_thread

    monkeypatch.setattr(connector, "_orchestrator_turn", fake_orchestrator_turn)

    reply, _updated, events = asyncio.run(
        connector.turn(
            project,
            thread,
            requester,
            "use the bigger model",
            workspace_request={"model": "gpt-5.6-codex"},
        )
    )

    assert reply == "ok"
    assert events == ()
    assert seen[0].model == "gpt-5.6-codex"
    stored = connector.index.get(project.id, thread.thread_id)
    assert stored is not None
    # Engine is untouched; the session is dropped so the next spawn takes the model.
    assert stored.engine == "codex"
    assert stored.model == "gpt-5.6-codex"
    assert stored.workspace["model"] == "gpt-5.6-codex"
    assert stored.workspace["session_id"] == ""
    assert stored.workspace["provider_started"] is False
    assert stored.workspace["session_generation"] == 4

    # Re-requesting the model already in force keeps the session.
    asyncio.run(
        connector.turn(
            project,
            connector.index.get(project.id, thread.thread_id),
            requester,
            "same model",
            workspace_request={"model": "gpt-5.6-codex"},
        )
    )
    unchanged = connector.index.get(project.id, thread.thread_id)
    assert unchanged is not None
    assert unchanged.workspace["session_generation"] == 4


def test_orchestrator_engine_switch_without_model_still_clears_the_model(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """Crossing engines retires the old provider's model id."""
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None)
    thread = CockpitThread(
        thread_id="thread_engine_and_model",
        project_id=project.id,
        session_id=orchestrator_session_id(project.id, "thread_engine_and_model"),
        title="Plan",
        created_at="2026-07-19T10:00:00Z",
        updated_at="2026-07-19T10:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="codex",
        model="gpt-5.5",
        workspace={"worker_id": "worker_a", "session_id": "s1", "session_generation": 1, "model": "gpt-5.5"},
    )
    connector.index.save(thread)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")

    async def fake_orchestrator_turn(_project, context_thread, *_args, **_kwargs):  # noqa: ANN001, ANN002, ANN003
        return "ok", context_thread

    monkeypatch.setattr(connector, "_orchestrator_turn", fake_orchestrator_turn)

    asyncio.run(connector.turn(project, thread, requester, "go claude", workspace_request={"engine": "claude"}))
    switched = connector.index.get(project.id, thread.thread_id)
    assert switched is not None
    assert switched.engine == "claude"
    assert switched.model == ""
    assert "model" not in switched.workspace

    # Engine and model together respawn once, on both.
    asyncio.run(
        connector.turn(
            project,
            switched,
            requester,
            "back to codex on a pinned model",
            workspace_request={"engine": "codex", "model": "gpt-5.6"},
        )
    )
    both = connector.index.get(project.id, thread.thread_id)
    assert both is not None
    assert both.engine == "codex"
    assert both.model == "gpt-5.6"
    assert both.workspace["session_generation"] == 3


def test_cockpit_catalog_carries_the_model_catalog_alongside_capability_flags() -> None:
    from jarvis.orchestration.cockpit import cockpit_catalog

    catalog = cockpit_catalog(
        engines=["codex", "claude"],
        engine_supports={
            "codex": {
                "streaming": True,
                "models": [{"id": "gpt-x", "label": "GPT X"}],
                "default_model": "gpt-x",
            }
        },
    )

    rows = {row["engine"]: row for row in catalog["engines"]}
    # The catalog keys survive the bool coercion applied to the capability flags.
    assert rows["codex"]["supports"]["models"] == [{"id": "gpt-x", "label": "GPT X"}]
    assert rows["codex"]["supports"]["default_model"] == "gpt-x"
    assert rows["codex"]["supports"]["streaming"] is True
    # An engine with no catalog reports an empty one, never a missing key.
    assert rows["claude"]["supports"]["models"] == []
    assert rows["claude"]["supports"]["default_model"] == ""


def test_cockpit_catalog_carries_effort_and_speed_alongside_capability_flags() -> None:
    from jarvis.orchestration.cockpit import cockpit_catalog

    catalog = cockpit_catalog(
        engines=["codex", "claude"],
        engine_supports={"codex": _ENGINE_SUPPORTS_CONTRACT_EXAMPLE["codex"]},
    )

    rows = {row["engine"]: row for row in catalog["engines"]}
    codex = rows["codex"]["supports"]
    expected = _ENGINE_SUPPORTS_CONTRACT_EXAMPLE["codex"]
    # Catalog keys survive the bool coercion applied to the capability flags.
    assert codex["efforts"] == expected["efforts"]
    assert codex["default_effort"] == "high"
    assert codex["speeds"] == expected["speeds"]
    assert codex["default_speed"] == "standard"
    assert codex["streaming"] is True
    # An engine with no speeds reports an empty list, never a missing key.
    assert rows["claude"]["supports"]["efforts"] == []
    assert rows["claude"]["supports"]["speeds"] == []
    assert rows["claude"]["supports"]["default_effort"] == ""


# The wire contract exactly as the cockpit picker consumes it: everything lives
# under engine_supports.<engine>, beside the capability booleans.
_ENGINE_SUPPORTS_CONTRACT_EXAMPLE = {
    "codex": {
        "streaming": True,
        "models": [{"id": "gpt-5.6-sol", "label": "GPT-5.6 Sol"}],
        "default_model": "gpt-5.6-sol",
        "efforts": [
            {"id": "low", "label": "Light"},
            {"id": "medium", "label": "Medium"},
            {"id": "high", "label": "High"},
            {"id": "xhigh", "label": "Extra High", "description": "Consumes usage limits faster"},
        ],
        "default_effort": "high",
        "speeds": [
            {"id": "standard", "label": "Standard", "description": "Default speed"},
            {"id": "priority", "label": "Fast", "description": "1.5x speed, more usage"},
        ],
        "default_speed": "standard",
    }
}


def test_worker_health_publishes_the_engine_supports_contract_shape() -> None:
    from jarvis.config import WorkerConfig
    from jarvis.worker.server import _engine_supports

    supports = _engine_supports(["codex", "claude"], WorkerConfig(_env_file=None))

    expected = _ENGINE_SUPPORTS_CONTRACT_EXAMPLE["codex"]
    codex = supports["codex"]
    assert codex["streaming"] is True
    assert codex["default_model"] == expected["default_model"]
    assert codex["models"][0]["id"] == expected["models"][0]["id"]
    assert codex["default_effort"] == expected["default_effort"]
    assert codex["default_speed"] == expected["default_speed"]

    # Ids and labels are the contract; `description` is per-row optional, and
    # the built-ins carry one on every row rather than only where the example
    # spells one out.
    for rows_key in ("efforts", "speeds"):
        published = codex[rows_key]
        assert [(row["id"], row["label"]) for row in published] == [
            (row["id"], row["label"]) for row in expected[rows_key]
        ]
        for row, example in zip(published, expected[rows_key], strict=True):
            if "description" in example:
                assert row["description"] == example["description"]

    # Claude has real efforts but no fast mode — the UI hides the speed row.
    assert supports["claude"]["speeds"] == []
    assert supports["claude"]["default_speed"] == ""
    assert supports["claude"]["default_effort"] == "high"


def test_worker_profile_splits_catalog_out_of_engine_supports() -> None:
    from jarvis.orchestration.workers import (
        _engine_catalog_from_health,
        _engine_models_from_health,
        _engine_supports_from_mapping,
    )

    raw = {
        "codex": {
            "streaming": True,
            "checkpoints": False,
            "models": [{"id": "gpt-x", "label": "GPT X"}, {"id": "gpt-y"}],
            "default_model": "gpt-x",
            "efforts": [{"id": "low", "label": "Light"}, {"id": "high", "label": "High", "description": "Slower"}],
            "default_effort": "high",
            "speeds": [{"id": "priority", "label": "Fast"}],
            "default_speed": "standard",
        }
    }

    supports = _engine_supports_from_mapping(raw)
    # Catalog keys must not leak into the bool mapping — a list would become True,
    # and a default id string would too.
    assert supports == {"codex": {"streaming": True, "checkpoints": False}}

    models, defaults = _engine_models_from_health({"engine_supports": raw})
    assert models == {"codex": [{"id": "gpt-x", "label": "GPT X"}, {"id": "gpt-y", "label": "gpt-y"}]}
    assert defaults == {"codex": "gpt-x"}

    efforts, effort_defaults = _engine_catalog_from_health({"engine_supports": raw}, "efforts", "default_effort")
    # An optional description rides through so the picker can explain a choice.
    assert efforts == {
        "codex": [{"id": "low", "label": "Light"}, {"id": "high", "label": "High", "description": "Slower"}]
    }
    assert effort_defaults == {"codex": "high"}

    speeds, speed_defaults = _engine_catalog_from_health({"engine_supports": raw}, "speeds", "default_speed")
    assert speeds == {"codex": [{"id": "priority", "label": "Fast"}]}
    assert speed_defaults == {"codex": "standard"}

    # A worker predating the contract reports nothing rather than breaking.
    bare = {"engine_supports": {"codex": {"streaming": True}}}
    assert _engine_models_from_health(bare) == ({}, {})
    assert _engine_catalog_from_health(bare, "efforts", "default_effort") == ({}, {})
    assert _engine_catalog_from_health(bare, "speeds", "default_speed") == ({}, {})


def test_worker_profile_payload_folds_the_catalog_back_into_engine_supports() -> None:
    profile = WorkerProfile(
        worker_id="worker_a",
        display_name="Worker A",
        engine_supports={"codex": {"streaming": True}},
        engine_models={"codex": [{"id": "gpt-x", "label": "GPT X"}]},
        engine_default_model={"codex": "gpt-x"},
    )

    payload = profile.public()["engine_supports"]
    assert payload["codex"]["streaming"] is True
    assert payload["codex"]["models"] == [{"id": "gpt-x", "label": "GPT X"}]
    assert payload["codex"]["default_model"] == "gpt-x"


def test_worker_profile_payload_folds_effort_and_speed_back_in() -> None:
    profile = WorkerProfile(
        worker_id="worker_a",
        display_name="Worker A",
        engine_supports={"codex": {"streaming": True}, "claude": {"streaming": True}},
        engine_efforts={"codex": [{"id": "high", "label": "High"}]},
        engine_default_effort={"codex": "high"},
        engine_speeds={"codex": [{"id": "priority", "label": "Fast"}]},
        engine_default_speed={"codex": "standard"},
    )

    payload = profile.public()["engine_supports"]
    assert payload["codex"]["efforts"] == [{"id": "high", "label": "High"}]
    assert payload["codex"]["default_effort"] == "high"
    assert payload["codex"]["speeds"] == [{"id": "priority", "label": "Fast"}]
    assert payload["codex"]["default_speed"] == "standard"
    # An engine with no catalogs still reports every key, so the picker never
    # has to distinguish "missing" from "empty".
    assert payload["claude"]["efforts"] == []
    assert payload["claude"]["speeds"] == []
    assert payload["claude"]["default_effort"] == ""

    # Round-tripping through the stored form keeps the split intact.
    restored = WorkerProfile.from_dict(json.loads(json.dumps(asdict(profile))))
    assert restored.engine_efforts == profile.engine_efforts
    assert restored.engine_default_speed == profile.engine_default_speed


def _seed_worker_model_catalog(cfg: Config) -> None:
    workers_path = Path(cfg.orchestration.workers_path)
    data = json.loads(workers_path.read_text())
    data["workers"][0]["engine_models"] = {
        "codex": [{"id": "gpt-x", "label": "GPT X"}, {"id": "gpt-y", "label": "GPT Y"}],
        "claude": [{"id": "opus", "label": "Opus"}],
    }
    data["workers"][0]["engine_default_model"] = {"codex": "gpt-x", "claude": "opus"}
    workers_path.write_text(json.dumps(data))


def _warm_probe_model_catalog_snapshot(cfg: Config) -> None:
    from jarvis.orchestration.cockpit import worker_profiles
    from jarvis.orchestration.workers import reset_probe_snapshots

    health = {
        "ok": True,
        "default_engine": "codex",
        "supported_engines": ["codex", "claude"],
        "engine_supports": {
            "codex": {
                "streaming": True,
                "resume": True,
                "interrupt": True,
                "approval_requests": True,
                "input_requests": True,
                "checkpoints": True,
                "models": [{"id": "gpt-x", "label": "GPT X"}, {"id": "gpt-y", "label": "GPT Y"}],
                "default_model": "gpt-x",
            },
            "claude": {
                "streaming": True,
                "resume": True,
                "interrupt": False,
                "approval_requests": False,
                "input_requests": False,
                "checkpoints": False,
                "models": [{"id": "opus", "label": "Opus"}],
                "default_model": "opus",
            },
        },
    }

    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/jobs"):
            return Response({"jobs": []})
        if url.endswith("/sessions"):
            return Response({"sessions": []})
        if url.endswith("/health"):
            return Response(health)
        raise AssertionError(url)

    probed = worker_profiles(
        worker_cfg=cfg.worker,
        workers_path=cfg.orchestration.workers_path,
        probe=True,
        http_get=get,
    )
    engines = {row["engine"]: row for row in probed[0]["engines"]}
    assert engines["codex"]["supports"]["models"][0]["id"] == "gpt-x"
    # Simulate a fresh API process: the next read must come from the persisted
    # probe snapshot, not the process-global in-memory snapshot.
    reset_probe_snapshots()


def test_cockpit_thread_turn_rejects_a_model_the_engine_does_not_offer(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    _warm_probe_model_catalog_snapshot(cfg)
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json")
    index.save(
        CockpitThread(
            thread_id="thread_model_validation",
            project_id="neil-shared",
            session_id=orchestrator_session_id("neil-shared", "thread_model_validation"),
            title="Model validation",
            created_at="2026-07-19T00:00:00Z",
            updated_at="2026-07-19T00:00:00Z",
            created_by="neil",
            chat_type="orchestrator",
            engine="codex",
            worker_id="macbook-worker",
            workspace={"worker_id": "macbook-worker", "session_id": "conv_thread_model_validation"},
        )
    )

    async def calls(base: str, client: httpx.AsyncClient) -> httpx.Response:
        return await client.post(
            f"{base}/v1/projects/neil-shared/threads/thread_model_validation/turns",
            json={"text": "switch model", "model": "gpt-nope", "idempotency_key": "turn_bad_model"},
        )

    response = asyncio.run(_with_server(cfg, calls))

    assert response.status_code == 400
    body = response.json()
    assert body["error"]["code"] == "validation_failed"
    # The error names what the caller may pick instead.
    assert "gpt-x, gpt-y" in body["error"]["message"]


def test_cockpit_catalog_endpoint_publishes_last_probed_worker_model_catalog(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    _warm_probe_model_catalog_snapshot(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> dict[str, Any]:
        return (await client.get(f"{base}/v1/cockpit/catalog")).json()

    catalog = asyncio.run(_with_server(cfg, calls))

    workers_path = Path(cfg.orchestration.workers_path)
    assert workers_path.with_name(f"{workers_path.name}.probes.json").exists()
    raw_workers = json.loads(workers_path.read_text())
    assert "engine_models" not in raw_workers["workers"][0]
    rows = {row["engine"]: row for row in catalog["engines"]}
    assert rows["codex"]["supports"]["models"] == [
        {"id": "gpt-x", "label": "GPT X"},
        {"id": "gpt-y", "label": "GPT Y"},
    ]
    assert rows["codex"]["supports"]["default_model"] == "gpt-x"
    assert rows["claude"]["supports"]["default_model"] == "opus"
    # Capability flags are untouched by the catalog merge.
    assert rows["codex"]["supports"]["checkpoints"] is True
    assert rows["claude"]["supports"]["checkpoints"] is False


def test_cockpit_workspace_turn_forwards_the_model_to_the_worker_session(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """Worker sessions switch in place — the model rides on the turn, no respawn."""
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    _seed_worker_model_catalog(cfg)
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None)
    thread = CockpitThread(
        thread_id="thread_ws_model",
        project_id="neil-shared",
        session_id=orchestrator_session_id("neil-shared", "thread_ws_model"),
        title="Workspace",
        created_at="2026-07-19T00:00:00Z",
        updated_at="2026-07-19T00:00:00Z",
        created_by="neil",
        engine="codex",
        model="gpt-x",
        worker_id="macbook-worker",
        workspace={
            "worker_id": "macbook-worker",
            "session_id": "conv_thread_ws_model",
            "kind": "conversation",
            "status": "ready",
            "session_generation": 2,
        },
    )
    connector.index.save(thread)
    posts: list[tuple[str, dict[str, Any]]] = []

    def fake_post(worker_id: str, path: str, payload: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG001
        posts.append((path, payload))
        return {"ok": True, "turn_id": "turn_1"}

    async def fake_ensure_workspace(_project, thread_arg, *_args, **_kwargs):  # noqa: ANN001, ANN002, ANN003
        return thread_arg

    monkeypatch.setattr(connector, "_post_worker_json", fake_post)
    monkeypatch.setattr(connector, "_ensure_workspace", fake_ensure_workspace)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")

    asyncio.run(
        connector.turn(
            RegistryStore(cfg.registry.path).get_project("neil-shared"),
            thread,
            requester,
            "switch model",
            workspace_request={"model": "gpt-y"},
        )
    )

    assert posts[0][0] == "/sessions/conv_thread_ws_model/turns"
    assert posts[0][1]["model"] == "gpt-y"
    stored = connector.index.get("neil-shared", "thread_ws_model")
    assert stored is not None
    # The session is NOT dropped: no generation bump, same session id.
    assert stored.workspace["session_generation"] == 2
    assert stored.workspace["session_id"] == "conv_thread_ws_model"
    # Thread detail reports the new effective model.
    assert stored.model == "gpt-y"


def test_orchestrator_turn_applies_and_reports_effort_and_speed(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """Effort/speed on an orchestrator-thread turn must reach the provider session
    AND be reported by thread detail afterwards.

    #137 wired both for worker sessions; the orchestrator path consumed neither,
    so the request was validated, dropped, and reported back as empty.
    """
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None)
    thread = CockpitThread(
        thread_id="thread_effort_speed",
        project_id=project.id,
        session_id=orchestrator_session_id(project.id, "thread_effort_speed"),
        title="Plan",
        created_at="2026-07-20T10:00:00Z",
        updated_at="2026-07-20T10:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="codex",
        model="gpt-5.5",
        worker_id="worker_a",
        workspace={
            "worker_id": "worker_a",
            "session_id": "orch_thread_effort_speed",
            "provider_started": True,
            "status": "ready",
            "session_generation": 1,
            "model": "gpt-5.5",
        },
    )
    connector.index.save(thread)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")
    posted: list[tuple[str, dict[str, Any]]] = []

    def fake_post(_worker_id: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        posted.append((path, body))
        return {"ok": True, "turn_id": body.get("turn_id")}

    def fake_get(_worker_id: str, _path: str) -> dict[str, Any]:
        return {"status": "ready"}

    async def fake_wait(*_args, **_kwargs) -> str:  # noqa: ANN002, ANN003
        return "done"

    monkeypatch.setattr(connector, "_post_worker_json", fake_post)
    monkeypatch.setattr(connector, "_get_worker_json", fake_get)
    monkeypatch.setattr(connector, "_wait_for_orchestrator_turn", fake_wait)

    reply, _updated, _events = asyncio.run(
        connector.turn(
            project,
            thread,
            requester,
            "think harder",
            workspace_request={"effort": "xhigh", "speed": "priority"},
        )
    )

    assert reply == "done"
    # (a) application: the worker turn carries them, on the same contract the
    # worker-session path uses.
    turn_bodies = [body for path, body in posted if path.endswith("/turns")]
    assert len(turn_bodies) == 1
    assert turn_bodies[0]["effort"] == "xhigh"
    assert turn_bodies[0]["speed"] == "priority"
    # Tuning alone must not respawn the session — codex applies it in place.
    assert turn_bodies[0]["metadata"]["resume_session"] is True

    # (b) reporting: thread detail reflects the effective values, and survives
    # the round-trip through the on-disk index.
    stored = connector.index.get(project.id, thread.thread_id)
    assert stored is not None
    assert stored.effort == "xhigh"
    assert stored.speed == "priority"
    assert stored.model == "gpt-5.5"
    assert stored.workspace["session_generation"] == 1

    reloaded = CockpitThreadIndex(Path(cfg.orchestration.workspace) / "cockpit-threads.json").get(
        project.id, thread.thread_id
    )
    assert reloaded is not None
    assert (reloaded.effort, reloaded.speed) == ("xhigh", "priority")


def test_orchestrator_engine_switch_clears_stale_effort_and_speed(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """Effort/speed are engine-scoped catalogs (claude publishes no speeds), so a
    tier must not survive a switch to an engine that never offered it."""
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    project = RegistryStore(cfg.registry.path).get_project("neil-shared")
    assert project is not None
    connector = CockpitConnector(cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None)
    thread = CockpitThread(
        thread_id="thread_tuning_switch",
        project_id=project.id,
        session_id=orchestrator_session_id(project.id, "thread_tuning_switch"),
        title="Plan",
        created_at="2026-07-20T10:00:00Z",
        updated_at="2026-07-20T10:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        engine="codex",
        model="gpt-5.5",
        effort="xhigh",
        speed="priority",
        workspace={"worker_id": "worker_a", "session_id": "s1", "session_generation": 1, "model": "gpt-5.5"},
    )
    connector.index.save(thread)
    requester = RequestContext("mac", "neil", "personal", frozenset(), channel="cockpit", peer="neil")

    async def fake_orchestrator_turn(_project, context_thread, *_args, **_kwargs):  # noqa: ANN001, ANN002, ANN003
        return "ok", context_thread

    monkeypatch.setattr(connector, "_orchestrator_turn", fake_orchestrator_turn)

    asyncio.run(connector.turn(project, thread, requester, "go claude", workspace_request={"engine": "claude"}))
    switched = connector.index.get(project.id, thread.thread_id)
    assert switched is not None
    assert switched.engine == "claude"
    assert (switched.effort, switched.speed) == ("", "")
    assert "effort" not in switched.workspace
    assert "speed" not in switched.workspace


def test_cockpit_routine_library_resolve_and_idempotent_run(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    calls: list[str] = []

    class FakeRoutineConnector:
        async def open_thread(self, project, requester, **kwargs):  # noqa: ANN001
            calls.append("open")
            now = "2026-07-20T12:00:00+00:00"
            return CockpitThread(
                thread_id="thread_routine",
                project_id=project.id,
                session_id="conv_routine",
                title=kwargs["title"],
                created_at=now,
                updated_at=now,
                created_by=requester.identity,
                chat_type="orchestrator",
                engine=kwargs["engine"],
            )

        async def turn(self, _project, thread, _requester, text, **_kwargs):  # noqa: ANN001
            calls.append(text)
            return "Workspace turn is running.", thread, ()

    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: FakeRoutineConnector())

    async def scenario(base: str, client: httpx.AsyncClient):
        library = await client.get(f"{base}/v1/routines")
        resolved = await client.post(
            f"{base}/v1/routines/morning-brief/resolve",
            json={"params": {"day": "2026-07-20"}},
        )
        body = {
            "project_id": "neil-shared",
            "prompt": "Use the existing rich pull request review prompt.",
            "params": _pull_request_review_params(),
            "idempotency_key": "review-123",
        }
        first = await client.post(f"{base}/v1/routines/pull-request-review/run", json=body)
        replay = await client.post(f"{base}/v1/routines/pull-request-review/run", json=body)
        return library, resolved, first, replay

    library, resolved, first, replay = asyncio.run(_with_server(cfg, scenario))

    assert library.status_code == 200
    assert {item["routine_id"] for item in library.json()["routines"]} == {
        "morning-brief",
        "pull-request-review",
        "issue-triage",
        "system-health-check",
        "draft-release-notes",
    }
    assert resolved.json()["resolution"]["ready"] is True
    assert first.status_code == 202
    assert first.json()["run"]["status"] == "started"
    assert first.json()["thread"]["thread_id"] == "thread_routine"
    assert replay.json()["idempotent"] is True
    assert calls == ["open", "Use the existing rich pull request review prompt."]


@pytest.mark.parametrize(
    ("terminal_state", "stored_status", "retry_status"),
    [
        ("completed", "ready", "completed"),
        ("failed", "failed", "failed"),
        ("interrupted", "interrupted", "interrupted"),
    ],
)
def test_cockpit_routine_run_resumes_terminal_reserved_thread_when_response_save_fails(
    tmp_path, monkeypatch, terminal_state, stored_status, retry_status
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(
        cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None
    )
    active_connector = [connector]
    worker_posts: list[dict[str, Any]] = []
    release_wait = asyncio.Event()

    async def fake_ensure(_project, thread, _requester, *, progress):  # noqa: ANN001
        del progress
        return connector.index.save(
            replace(
                thread,
                worker_id="macbook-worker",
                workspace={
                    **thread.workspace,
                    "worker_id": "macbook-worker",
                    "session_id": "orch_reserved_routine",
                    "session_generation": 0,
                    "status": "ready",
                    "provision_phase": "ready",
                },
            )
        )

    def fake_post(_worker_id: str, _path: str, body: dict[str, Any]) -> dict[str, Any]:
        worker_posts.append(body)
        return {"ok": True}

    async def fake_wait(_worker_id: str, _session_id: str, _turn_id: str) -> str:
        if terminal_state == "failed":
            raise RuntimeError("simulated provider failure after acceptance")
        if terminal_state == "interrupted":
            await release_wait.wait()
        return "Reserved routine completed."

    original_save = cockpit_api_module.IdempotencyStore.save
    routine_response_saves = 0

    def fail_first_routine_response_save(store, scope, key, body, response):  # noqa: ANN001, ANN202
        nonlocal routine_response_saves
        if scope.startswith("routines/pull-request-review/run/principal/"):
            routine_response_saves += 1
            if routine_response_saves == 1:
                raise OSError("simulated crash after worker acceptance")
        return original_save(store, scope, key, body, response)

    monkeypatch.setattr(connector, "_ensure_orchestrator_session", fake_ensure)
    monkeypatch.setattr(connector, "_post_worker_json", fake_post)
    monkeypatch.setattr(connector, "_wait_for_orchestrator_turn", fake_wait)
    monkeypatch.setattr(
        cockpit_api_module, "_cockpit_connector", lambda _ctx: active_connector[0]
    )
    monkeypatch.setattr(
        cockpit_api_module.IdempotencyStore, "save", fail_first_routine_response_save
    )
    body = {
        "project_id": "neil-shared",
        "prompt": "Dispatch this routine exactly once.",
        "params": _pull_request_review_params(),
        "idempotency_key": f"review-response-save-crash-{terminal_state}",
    }

    async def failed_process(base: str, client: httpx.AsyncClient):
        failed = await client.post(
            f"{base}/v1/routines/pull-request-review/run", json=body
        )
        assert failed.status_code == 500
        reserved_thread = connector.index.list_all()[0]
        if terminal_state == "interrupted":
            interrupted = connector.index.detach_execution_if_matches(
                "neil-shared",
                reserved_thread.thread_id,
                expected_session_id="orch_reserved_routine",
                expected_generation=0,
            )
            assert interrupted is not None
            release_wait.set()
        deadline = asyncio.get_running_loop().time() + 0.5
        while asyncio.get_running_loop().time() < deadline:
            reserved_thread = (
                connector.index.get("neil-shared", reserved_thread.thread_id)
                or reserved_thread
            )
            if reserved_thread.workspace.get("status") == stored_status:
                break
            await asyncio.sleep(0.01)
        return reserved_thread

    reserved_thread = asyncio.run(_with_server(cfg, failed_process))
    restarted_connector = CockpitConnector(
        cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None
    )
    active_connector[0] = restarted_connector

    async def restarted_process(base: str, client: httpx.AsyncClient):
        conflicting = await client.post(
            f"{base}/v1/routines/pull-request-review/run",
            json={
                **body,
                "prompt": "A different request must not claim the reservation.",
            },
        )
        retry = await client.post(
            f"{base}/v1/routines/pull-request-review/run", json=body
        )
        return conflicting, retry

    conflicting, retry = asyncio.run(_with_server(cfg, restarted_process))

    assert reserved_thread.workspace["status"] == stored_status
    assert conflicting.status_code == 409
    assert conflicting.json()["error"]["code"] == "idempotency_conflict"
    assert retry.status_code == 202
    assert retry.json()["run"]["status"] == retry_status
    assert retry.json()["run"]["run_id"] == worker_posts[0]["turn_id"]
    assert retry.json()["run"]["thread_id"] == reserved_thread.thread_id
    assert retry.json()["thread"]["thread_id"] == reserved_thread.thread_id
    assert len(restarted_connector.index.list_all()) == 1
    assert len(worker_posts) == 1
    activity, _cursor = cockpit_api_module._project_activity_log(  # noqa: SLF001
        cockpit_api_module.cockpit_context(cfg)
    ).list("neil-shared", limit=20, activity_type="routine.started")
    assert len(activity) == 1
    assert activity[0]["data"]["run_id"] == retry.json()["run"]["run_id"]


def test_cockpit_routine_prompt_override_still_requires_typed_parameters(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)

    async def scenario(base: str, client: httpx.AsyncClient):
        return await client.post(
            f"{base}/v1/routines/pull-request-review/run",
            json={
                "project_id": "neil-shared",
                "prompt": "A rich override cannot replace the routine's typed bindings.",
                "idempotency_key": "review-missing-params",
            },
        )

    response = asyncio.run(_with_server(cfg, scenario))

    assert response.status_code == 400
    assert response.json()["error"] == {
        "code": "validation_failed",
        "message": "missing routine parameters: pull_request, reviewers",
        "recoverable": True,
    }


def test_cockpit_routine_run_returns_after_worker_acceptance_and_continues_async(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    memory = FakeProjectMemory()
    connector = CockpitConnector(
        cfg,
        memory=memory,
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    worker_posts: list[dict[str, Any]] = []
    wait_started = asyncio.Event()
    release_completion = asyncio.Event()

    async def fake_ensure(_project, thread, _requester, *, progress):  # noqa: ANN001
        del progress
        return connector.index.save(
            replace(
                thread,
                worker_id="macbook-worker",
                workspace={
                    **thread.workspace,
                    "worker_id": "macbook-worker",
                    "session_id": "orch_routine_async",
                    "session_generation": 0,
                    "status": "ready",
                    "provision_phase": "ready",
                },
            )
        )

    def fake_post(_worker_id: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        stored = connector.index.list_all()
        assert len(stored) == 1
        pending = stored[0].workspace[PENDING_ORCHESTRATOR_COMPLETION_KEY]
        assert stored[0].workspace["provision_phase"] == "dispatching"
        assert pending["phase"] == "dispatching"
        assert pending["turn_id"] == body["turn_id"]
        worker_posts.append({"path": path, "body": body})
        return {"ok": True}

    async def fake_wait(_worker_id: str, _session_id: str, _turn_id: str) -> str:
        wait_started.set()
        await release_completion.wait()
        return "Routine completed in the background."

    monkeypatch.setattr(connector, "_ensure_orchestrator_session", fake_ensure)
    monkeypatch.setattr(connector, "_post_worker_json", fake_post)
    monkeypatch.setattr(connector, "_wait_for_orchestrator_turn", fake_wait)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def scenario(base: str, client: httpx.AsyncClient):
        body = {
            "project_id": "neil-shared",
            "prompt": "Run the review without holding the HTTP request open.",
            "params": _pull_request_review_params(),
            "idempotency_key": "review-async-123",
        }
        response = await asyncio.wait_for(
            client.post(
                f"{base}/v1/routines/pull-request-review/run",
                json=body,
            ),
            timeout=0.5,
        )
        replay = await asyncio.wait_for(
            client.post(f"{base}/v1/routines/pull-request-review/run", json=body),
            timeout=0.5,
        )
        await asyncio.wait_for(wait_started.wait(), timeout=0.5)
        accepted = connector.index.get("neil-shared", response.json()["thread"]["thread_id"])
        assert accepted is not None
        assert accepted.workspace["status"] == "running"
        assert (
            accepted.workspace[PENDING_ORCHESTRATOR_COMPLETION_KEY]["turn_id"]
            == worker_posts[0]["body"]["turn_id"]
        )
        assert not {
            "prompt",
            "resume",
            "requested_tuning",
        } & accepted.workspace[PENDING_ORCHESTRATOR_COMPLETION_KEY].keys()
        assert accepted.messages == ()

        release_completion.set()
        deadline = asyncio.get_running_loop().time() + 0.5
        completed = accepted
        while asyncio.get_running_loop().time() < deadline:
            completed = connector.index.get_with_messages("neil-shared", accepted.thread_id) or completed
            if completed.workspace.get("status") == "ready" and completed.messages:
                break
            await asyncio.sleep(0.01)
        return response, replay, completed

    response, replay, completed = asyncio.run(_with_server(cfg, scenario))

    assert response.status_code == 202
    assert response.json()["run"]["status"] == "started"
    assert replay.status_code == 202
    assert replay.json()["idempotent"] is True
    assert len(worker_posts) == 1
    assert worker_posts[0]["path"] == "/sessions/orch_routine_async/turns"
    assert completed.workspace["status"] == "ready"
    assert PENDING_ORCHESTRATOR_COMPLETION_KEY not in completed.workspace
    assert [message["content"] for message in completed.messages] == [
        "Run the review without holding the HTTP request open.",
        "Routine completed in the background.",
    ]


def _ambiguous_worker_500(message: str) -> WorkerRequestError:
    return WorkerRequestError(message, code="internal_error", status_code=500)


@pytest.mark.parametrize(
    ("dispatch_error", "failures_before_success"),
    [
        (OSError, 1),
        (TimeoutError, 1),
        (_ambiguous_worker_500, 1),
        (OSError, 4),
        (OSError, 7),
    ],
)
def test_cockpit_routine_initial_ambiguous_dispatch_retries_before_response(tmp_path, monkeypatch, dispatch_error, failures_before_success) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    posts: list[tuple[str, dict[str, Any]]] = []

    async def fake_ensure(_project, thread, _requester, *, progress):  # noqa: ANN001
        del progress
        return connector.index.save(
            replace(
                thread,
                worker_id="macbook-worker",
                workspace={
                    **thread.workspace,
                    "worker_id": "macbook-worker",
                    "session_id": "orch_routine_retry",
                    "session_generation": 0,
                    "status": "ready",
                    "provision_phase": "ready",
                },
            )
        )

    def fake_post(_worker_id: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        posts.append((path, body))
        if len(posts) <= failures_before_success:
            # A 5xx may be returned after the worker accepted the idempotent
            # turn, so recovery must replay this exact request identity.
            raise dispatch_error("ambiguous initial dispatch failure")
        return {"ok": True}

    async def fake_wait(_worker_id: str, _session_id: str, _turn_id: str) -> str:
        return "Recovered without waiting for a process restart."

    monkeypatch.setattr(connector, "_ensure_orchestrator_session", fake_ensure)
    monkeypatch.setattr(connector, "_post_worker_json", fake_post)
    monkeypatch.setattr(connector, "_wait_for_orchestrator_turn", fake_wait)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def scenario(base: str, client: httpx.AsyncClient):
        response = await client.post(
            f"{base}/v1/routines/pull-request-review/run",
            json={
                "project_id": "neil-shared",
                "prompt": "Recover this ambiguous dispatch.",
                "params": _pull_request_review_params(),
                "idempotency_key": "review-ambiguous-dispatch",
            },
        )
        replay = await client.post(
            f"{base}/v1/routines/pull-request-review/run",
            json={
                "project_id": "neil-shared",
                "prompt": "Recover this ambiguous dispatch.",
                "params": _pull_request_review_params(),
                "idempotency_key": "review-ambiguous-dispatch",
            },
        )
        deadline = asyncio.get_running_loop().time() + 2
        completed = connector.index.list_all()[0]
        retained_state = None
        if failures_before_success == 7:
            while len(posts) < 7 and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.01)
            completed = connector.index.get("neil-shared", completed.thread_id) or completed
            retained_state = (completed.workspace.get("status"), completed.workspace.get("provision_phase"), isinstance(completed.workspace.get(PENDING_ORCHESTRATOR_COMPLETION_KEY), dict))
        while asyncio.get_running_loop().time() < deadline:
            completed = connector.index.get_with_messages(
                "neil-shared",
                completed.thread_id,
            ) or completed
            if completed.workspace.get("status") == "ready" and completed.messages:
                break
            await asyncio.sleep(0.01)
        return response, replay, retained_state, completed

    response, replay, retained_state, completed = asyncio.run(_with_server(cfg, scenario))

    assert response.status_code == 202
    assert response.json()["run"]["status"] == (
        "dispatching" if failures_before_success >= 4 else "started"
    )
    assert replay.status_code == 202
    assert replay.json()["idempotent"] is True
    assert len(posts) == failures_before_success + 1
    assert len({body["turn_id"] for _path, body in posts}) == 1
    assert len({body["idempotency_key"] for _path, body in posts}) == 1
    assert all(path == "/sessions/orch_routine_retry/turns" for path, _body in posts)
    if failures_before_success == 7:
        assert retained_state == ("starting", "dispatching", True)
    assert PENDING_ORCHESTRATOR_COMPLETION_KEY not in completed.workspace
    assert len(connector.index.list_all()) == 1
    assert completed.workspace["status"] == "ready"
    assert [message["content"] for message in completed.messages] == [
        "Recover this ambiguous dispatch.",
        "Recovered without waiting for a process restart.",
    ]


@pytest.mark.parametrize("terminal_failure", ["runtime", "worker_4xx", "removed_worker"])
def test_cockpit_routine_dispatch_recovery_stops_on_terminal_failure(
    tmp_path, monkeypatch, terminal_failure
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(
        cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None
    )
    posts: list[str] = []

    async def fake_ensure(_project, thread, _requester, *, progress):  # noqa: ANN001
        del progress
        return connector.index.save(
            replace(
                thread,
                worker_id="removed-worker",
                workspace={
                    **thread.workspace,
                    "worker_id": "removed-worker",
                    "session_id": "orch_removed_worker",
                    "session_generation": 0,
                    "status": "ready",
                    "provision_phase": "ready",
                },
            )
        )

    original_post = connector._post_worker_json

    def flaky_then_terminal(
        worker_id: str, path: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        posts.append(path)
        if len(posts) <= 4:
            raise OSError("ambiguous response loss")
        if terminal_failure == "runtime":
            raise RuntimeError("deterministic local dispatch failure")
        if terminal_failure == "worker_4xx":
            raise WorkerRequestError("worker rejected dispatch", status_code=409)
        return original_post(worker_id, path, body)

    monkeypatch.setattr(connector, "_ensure_orchestrator_session", fake_ensure)
    monkeypatch.setattr(connector, "_post_worker_json", flaky_then_terminal)
    monkeypatch.setattr(
        cockpit_api_module, "_cockpit_connector", lambda _ctx: connector
    )

    async def scenario(base: str, client: httpx.AsyncClient):
        response = await client.post(
            f"{base}/v1/routines/pull-request-review/run",
            json={
                "project_id": "neil-shared",
                "prompt": "Do not retry forever after worker removal.",
                "params": _pull_request_review_params(),
                "idempotency_key": "review-worker-removed",
            },
        )
        deadline = asyncio.get_running_loop().time() + 1
        thread = connector.index.list_all()[0]
        while asyncio.get_running_loop().time() < deadline:
            thread = connector.index.get("neil-shared", thread.thread_id) or thread
            if thread.workspace.get("status") == "failed":
                break
            await asyncio.sleep(0.01)
        attempts_after_failure = len(posts)
        await asyncio.sleep(0.35)
        return response, thread, attempts_after_failure

    response, thread, attempts_after_failure = asyncio.run(_with_server(cfg, scenario))

    assert response.status_code == 202
    assert response.json()["run"]["status"] == "dispatching"
    assert thread.workspace["status"] == "failed"
    assert thread.workspace["provision_phase"] == "failed"
    assert PENDING_ORCHESTRATOR_COMPLETION_KEY not in thread.workspace
    assert attempts_after_failure == 5
    assert len(posts) == attempts_after_failure


def test_cockpit_startup_replays_preaccepted_routine_dispatch_and_keeps_turn_lease(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    now = "2026-07-20T12:00:00+00:00"
    thread = CockpitThread(
        thread_id="thread_routine_restart",
        project_id="neil-shared",
        session_id="project:neil-shared:thread_routine_restart",
        title="Restart-safe routine",
        created_at=now,
        updated_at=now,
        created_by="neil",
        chat_type="orchestrator",
        engine="codex",
        worker_id="macbook-worker",
        workspace={
            "worker_id": "macbook-worker",
            "session_id": "orch_routine_restart",
            "session_generation": 3,
            "status": "starting",
            "provision_phase": "dispatching",
            PENDING_ORCHESTRATOR_COMPLETION_KEY: {
                "phase": "dispatching",
                "worker_id": "macbook-worker",
                "session_id": "orch_routine_restart",
                "session_generation": 3,
                "turn_id": "routine_restart_turn",
                "idempotency_key": "review-restart-123",
                "persisted_text": "Finish this accepted routine after an API restart.",
                "prompt": "Recovered authoritative context and routine instruction.",
                "resume": False,
                "requested_tuning": {"model": "gpt-5"},
                "requester": {
                    "device_id": "local-mac",
                    "identity": "neil",
                    "scope": "personal",
                    "capabilities": [],
                    "channel": "cockpit",
                    "confidence": "strong",
                    "peer": "neil",
                },
            },
        },
    )
    connector.index.save(thread)
    wait_started = asyncio.Event()
    release_completion = asyncio.Event()
    replayed_posts: list[dict[str, Any]] = []

    def fake_post(worker_id: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        replayed_posts.append({"worker_id": worker_id, "path": path, "body": body})
        if len(replayed_posts) == 1:
            raise OSError("worker restarting")
        if len(replayed_posts) == 2:
            raise TimeoutError("worker still starting")
        return {"ok": True}

    async def fake_wait(worker_id: str, session_id: str, turn_id: str) -> str:
        assert (worker_id, session_id, turn_id) == (
            "macbook-worker",
            "orch_routine_restart",
            "routine_restart_turn",
        )
        wait_started.set()
        await release_completion.wait()
        return "The restarted API persisted this worker result."

    monkeypatch.setattr(connector, "_post_worker_json", fake_post)
    monkeypatch.setattr(connector, "_wait_for_orchestrator_turn", fake_wait)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def scenario(_base: str, _client: httpx.AsyncClient):
        await asyncio.wait_for(wait_started.wait(), timeout=2)
        accepted = connector.index.get("neil-shared", thread.thread_id)
        assert accepted is not None
        assert accepted.workspace[PENDING_ORCHESTRATOR_COMPLETION_KEY]["phase"] == "accepted"
        assert not {
            "prompt",
            "resume",
            "requested_tuning",
        } & accepted.workspace[PENDING_ORCHESTRATOR_COMPLETION_KEY].keys()
        with pytest.raises(RuntimeError, match="already has an active turn"):
            connector.index.reserve_execution_turn("neil-shared", thread.thread_id)

        release_completion.set()
        deadline = asyncio.get_running_loop().time() + 0.5
        completed = thread
        while asyncio.get_running_loop().time() < deadline:
            completed = connector.index.get_with_messages("neil-shared", thread.thread_id) or completed
            if completed.workspace.get("status") == "ready" and completed.messages:
                break
            await asyncio.sleep(0.01)
        return completed

    completed = asyncio.run(_with_server(cfg, scenario))

    assert completed.workspace["status"] == "ready"
    assert PENDING_ORCHESTRATOR_COMPLETION_KEY not in completed.workspace
    assert len(replayed_posts) == 3
    assert replayed_posts[0]["worker_id"] == "macbook-worker"
    assert replayed_posts[0]["path"] == "/sessions/orch_routine_restart/turns"
    assert replayed_posts[0]["body"]["turn_id"] == "routine_restart_turn"
    assert (
        replayed_posts[0]["body"]["idempotency_key"]
        == "orchestrator-turn:thread_routine_restart:review-restart-123"
    )
    assert replayed_posts[0]["body"]["prompt"] == (
        "Recovered authoritative context and routine instruction."
    )
    assert replayed_posts[0]["body"]["model"] == "gpt-5"
    assert [message["content"] for message in completed.messages] == [
        "Finish this accepted routine after an API restart.",
        "The restarted API persisted this worker result.",
    ]


def test_cockpit_interrupt_preserves_and_detach_removes_pending_orchestrator_completion(tmp_path) -> None:
    index = CockpitThreadIndex(tmp_path / "threads.json")
    now = "2026-07-20T12:00:00+00:00"

    def running_thread(thread_id: str) -> CockpitThread:
        return CockpitThread(
            thread_id=thread_id,
            project_id="project",
            session_id=f"project:project:{thread_id}",
            title="Routine",
            created_at=now,
            updated_at=now,
            created_by="neil",
            chat_type="orchestrator",
            workspace={
                "session_id": f"session_{thread_id}",
                "session_generation": 4,
                "status": "running",
                "provision_phase": "running",
                PENDING_ORCHESTRATOR_COMPLETION_KEY: {
                    "phase": "accepted",
                    "turn_id": f"turn_{thread_id}",
                },
            },
        )

    interrupted = running_thread("interrupt")
    index.save(interrupted)
    claimed = index.claim_execution_interrupt("project", interrupted.thread_id, turn_id="turn_interrupt")
    assert claimed is not None
    assert claimed.thread.workspace["status"] == "interrupting"
    assert PENDING_ORCHESTRATOR_COMPLETION_KEY in claimed.thread.workspace
    assert claimed.thread.workspace[PENDING_EXECUTION_INTERRUPT_KEY] == {
        "worker_id": "",
        "session_id": "session_interrupt",
        "session_generation": 4,
        "turn_id": "turn_interrupt",
        "restore_status": "running",
        "restore_provision_phase": "running",
        "requested_at": claimed.thread.workspace[PENDING_EXECUTION_INTERRUPT_KEY]["requested_at"],
    }

    recovered = index.recover_orphaned_execution("project", interrupted.thread_id)
    assert recovered is not None
    assert recovered.workspace["status"] == "interrupting"
    assert recovered.workspace["session_id"] == "session_interrupt"
    assert recovered.workspace["session_generation"] == 4
    assert PENDING_EXECUTION_INTERRUPT_KEY in recovered.workspace
    with pytest.raises(RuntimeError, match="being interrupted"):
        index.reserve_execution_turn("project", interrupted.thread_id)

    detached = running_thread("detach")
    index.save(detached)
    released = index.detach_execution_if_matches(
        "project",
        detached.thread_id,
        expected_session_id="session_detach",
        expected_generation=4,
    )
    assert released is not None
    assert released.workspace["status"] == "interrupted"
    assert released.workspace["session_generation"] == 5
    assert PENDING_ORCHESTRATOR_COMPLETION_KEY not in released.workspace


def test_cockpit_restart_replays_interrupt_before_detaching_session(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    connector = CockpitConnector(
        cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None
    )
    thread = CockpitThread(
        thread_id="thread_interrupt_before_worker_acceptance",
        project_id="project",
        session_id="project:project:thread_interrupt_before_worker_acceptance",
        title="Interrupt before acceptance",
        created_at="2026-07-20T12:00:00Z",
        updated_at="2026-07-20T12:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        worker_id="worker_a",
        workspace={
            "worker_id": "worker_a",
            "session_id": "session_interrupt_before_acceptance",
            "session_generation": 7,
            "status": "running",
            "provision_phase": "running",
        },
    )
    connector.index.save(thread)
    claim = connector.index.claim_execution_interrupt(
        thread.project_id, thread.thread_id, turn_id="turn_interrupt_before_acceptance"
    )
    assert claim is not None
    posts: list[tuple[str, str, dict[str, Any]]] = []

    def accept_interrupt(
        worker_id: str, path: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        posts.append((worker_id, path, body))
        return {"ok": True}

    monkeypatch.setattr(connector, "_post_worker_json", accept_interrupt)
    monkeypatch.setattr(
        connector,
        "_get_worker_json",
        lambda _worker_id, _path: {"status": "interrupted"},
    )

    recovered = connector.recover_pending_execution_interrupt(
        thread.project_id, thread.thread_id
    )

    assert posts == [
        (
            "worker_a",
            "/sessions/session_interrupt_before_acceptance/interrupt",
            {
                "turn_id": "turn_interrupt_before_acceptance",
                "metadata": {},
                "allowed_actions": [WORKER_SESSION_INTERRUPT],
            },
        )
    ]
    assert recovered is not None
    assert recovered.workspace["status"] == "interrupted"
    assert recovered.workspace["session_id"] == ""
    assert recovered.workspace["session_generation"] == 8
    assert PENDING_EXECUTION_INTERRUPT_KEY not in recovered.workspace


def test_cockpit_restart_verifies_accepted_interrupt_after_ambiguous_worker_write(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    connector = CockpitConnector(
        cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None
    )
    thread = CockpitThread(
        thread_id="thread_interrupt_after_worker_acceptance",
        project_id="project",
        session_id="project:project:thread_interrupt_after_worker_acceptance",
        title="Interrupt after acceptance",
        created_at="2026-07-20T12:00:00Z",
        updated_at="2026-07-20T12:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        worker_id="worker_a",
        workspace={
            "worker_id": "worker_a",
            "session_id": "session_interrupt_after_acceptance",
            "session_generation": 2,
            "status": "running",
            "provision_phase": "running",
        },
    )
    connector.index.save(thread)
    connector.index.claim_execution_interrupt(
        thread.project_id, thread.thread_id, turn_id="turn_interrupt_after_acceptance"
    )

    def ambiguous_interrupt(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise WorkerRequestError("connection closed after worker acceptance")

    monkeypatch.setattr(connector, "_post_worker_json", ambiguous_interrupt)
    monkeypatch.setattr(
        connector,
        "_get_worker_json",
        lambda _worker_id, _path: {"status": "interrupted"},
    )

    recovered = connector.recover_pending_execution_interrupt(
        thread.project_id, thread.thread_id
    )

    assert recovered is not None
    assert recovered.workspace["status"] == "interrupted"
    assert recovered.workspace["session_id"] == ""
    assert PENDING_EXECUTION_INTERRUPT_KEY not in recovered.workspace


@pytest.mark.parametrize(
    "rejection",
    [
        WorkerRequestError("no such session", status_code=404),
        WorkerRequestError(
            "worker is no longer configured",
            code="worker_not_configured",
            status_code=503,
        ),
    ],
)
def test_cockpit_restart_restores_execution_after_definite_interrupt_rejection(
    tmp_path,
    monkeypatch,
    rejection,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    thread = CockpitThread(
        thread_id="thread_interrupt_rejected",
        project_id="project",
        session_id="project:project:thread_interrupt_rejected",
        title="Interrupt rejected",
        created_at="2026-07-20T12:00:00Z",
        updated_at="2026-07-20T12:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        worker_id="worker_a",
        workspace={
            "worker_id": "worker_a",
            "session_id": "session_interrupt_rejected",
            "session_generation": 9,
            "status": "running",
            "provision_phase": "running",
        },
    )
    connector.index.save(thread)
    connector.index.claim_execution_interrupt(
        thread.project_id,
        thread.thread_id,
        turn_id="turn_interrupt_rejected",
    )

    def reject_interrupt(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise rejection

    def unexpected_state_read(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("a definite rejection must not be treated as ambiguous")

    monkeypatch.setattr(connector, "_post_worker_json", reject_interrupt)
    monkeypatch.setattr(connector, "_get_worker_json", unexpected_state_read)

    recovered = connector.recover_pending_execution_interrupt(
        thread.project_id,
        thread.thread_id,
    )

    assert recovered is not None
    assert recovered.workspace["status"] == "running"
    assert recovered.workspace["provision_phase"] == "running"
    assert recovered.workspace["session_id"] == "session_interrupt_rejected"
    assert recovered.workspace["session_generation"] == 9
    assert PENDING_EXECUTION_INTERRUPT_KEY not in recovered.workspace


def test_cockpit_live_definite_interrupt_rejection_rearms_pending_completion(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    ctx = cockpit_api_module.cockpit_context(cfg)
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )
    thread = CockpitThread(
        thread_id="thread_interrupt_rearm_completion",
        project_id="neil-shared",
        session_id="project:neil-shared:thread_interrupt_rearm_completion",
        title="Interrupt rearm completion",
        created_at="2026-07-20T12:00:00Z",
        updated_at="2026-07-20T12:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        worker_id="macbook-worker",
        workspace={
            "worker_id": "macbook-worker",
            "session_id": "session_interrupt_rearm_completion",
            "session_generation": 4,
            "status": "running",
            "provision_phase": "running",
            PENDING_ORCHESTRATOR_COMPLETION_KEY: {
                "phase": "accepted",
                "worker_id": "macbook-worker",
                "session_id": "session_interrupt_rearm_completion",
                "session_generation": 4,
                "turn_id": "turn_interrupt_rearm_completion",
                "requester": {
                    "device_id": "local-mac",
                    "identity": "neil",
                    "scope": "personal",
                    "capabilities": [],
                    "channel": "cockpit",
                    "confidence": "strong",
                    "peer": "neil",
                },
            },
        },
    )
    connector.index.save(thread)
    connector.index.claim_execution_interrupt(
        thread.project_id,
        thread.thread_id,
        turn_id="turn_interrupt_rearm_completion",
    )
    rearmed: list[tuple[str, str]] = []

    def reject_interrupt(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise WorkerRequestError("no such session", status_code=404)

    def record_rearm(project, recovered):  # noqa: ANN001, ANN202
        rearmed.append((project.id, recovered.thread_id))
        return True

    monkeypatch.setattr(connector, "_post_worker_json", reject_interrupt)
    monkeypatch.setattr(connector, "rearm_pending_orchestrator_completion", record_rearm)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    pending = asyncio.run(
        cockpit_api_module._recover_pending_execution_interrupts(ctx)  # noqa: SLF001
    )

    recovered = connector.index.get(thread.project_id, thread.thread_id)
    assert pending == 0
    assert recovered is not None
    assert recovered.workspace["status"] == "running"
    assert PENDING_EXECUTION_INTERRUPT_KEY not in recovered.workspace
    assert rearmed == [("neil-shared", thread.thread_id)]


def test_cockpit_restart_keeps_interrupt_marker_while_worker_is_still_running(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    connector = CockpitConnector(
        cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None
    )
    thread = CockpitThread(
        thread_id="thread_interrupt_worker_still_running",
        project_id="project",
        session_id="project:project:thread_interrupt_worker_still_running",
        title="Interrupt still pending",
        created_at="2026-07-20T12:00:00Z",
        updated_at="2026-07-20T12:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        worker_id="worker_a",
        workspace={
            "worker_id": "worker_a",
            "session_id": "session_interrupt_worker_still_running",
            "session_generation": 3,
            "status": "running",
            "provision_phase": "running",
        },
    )
    connector.index.save(thread)
    connector.index.claim_execution_interrupt(
        thread.project_id,
        thread.thread_id,
        turn_id="turn_interrupt_worker_still_running",
    )

    def unavailable_interrupt(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise WorkerRequestError("worker unavailable")

    monkeypatch.setattr(connector, "_post_worker_json", unavailable_interrupt)
    monkeypatch.setattr(
        connector, "_get_worker_json", lambda _worker_id, _path: {"status": "running"}
    )

    recovered = connector.recover_pending_execution_interrupt(
        thread.project_id, thread.thread_id
    )

    assert recovered is not None
    assert recovered.workspace["status"] == "interrupting"
    assert recovered.workspace["session_id"] == "session_interrupt_worker_still_running"
    assert recovered.workspace["session_generation"] == 3
    assert PENDING_EXECUTION_INTERRUPT_KEY in recovered.workspace


def test_cockpit_live_recovery_retries_pending_interrupt_without_another_restart(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    index = CockpitThreadIndex(
        Path(cfg.orchestration.workspace) / "cockpit-threads.json"
    )
    thread = CockpitThread(
        thread_id="thread_interrupt_live_retry",
        project_id="neil-shared",
        session_id="project:neil-shared:thread_interrupt_live_retry",
        title="Interrupt live retry",
        created_at="2026-07-20T12:00:00Z",
        updated_at="2026-07-20T12:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        worker_id="macbook-worker",
        workspace={
            "worker_id": "macbook-worker",
            "session_id": "session_interrupt_live_retry",
            "session_generation": 6,
            "status": "running",
            "provision_phase": "running",
        },
    )
    index.save(thread)
    index.claim_execution_interrupt(
        thread.project_id, thread.thread_id, turn_id="turn_interrupt_live_retry"
    )
    attempts = 0
    worker_status = "running"

    def worker_post(url: str, **_kwargs: Any) -> Response:
        nonlocal attempts, worker_status
        if not url.endswith("/interrupt"):
            return Response(
                {"ok": False, "error": "unexpected worker post"}, status_code=400
            )
        attempts += 1
        if attempts == 1:
            raise OSError("worker temporarily unavailable")
        worker_status = "interrupted"
        return Response({"ok": True})

    def worker_get(url: str, **_kwargs: Any) -> Response:
        if url.endswith("/execution-state"):
            return Response({"status": worker_status})
        return Response({"ok": True})

    async def scenario(_base: str, _client: httpx.AsyncClient):
        deadline = asyncio.get_running_loop().time() + 2.0
        recovered = index.get(thread.project_id, thread.thread_id)
        while asyncio.get_running_loop().time() < deadline:
            recovered = index.get(thread.project_id, thread.thread_id)
            if (
                recovered is not None
                and recovered.workspace.get("status") == "interrupted"
            ):
                break
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.05)
        task_names = [task.get_name() for task in asyncio.all_tasks()]
        return recovered, task_names

    recovered, task_names = asyncio.run(
        _with_server(cfg, scenario, http_get=worker_get, http_post=worker_post)
    )

    assert attempts >= 2
    assert recovered is not None
    assert recovered.workspace["status"] == "interrupted"
    assert recovered.workspace["session_id"] == ""
    assert PENDING_EXECUTION_INTERRUPT_KEY not in recovered.workspace
    assert "cockpit-interrupt-recovery" in task_names


def test_cockpit_interrupt_recovery_context_idles_without_polling_when_none_are_pending(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    ctx = cockpit_api_module.cockpit_context(cfg)
    recovery_calls = 0

    async def recover_none(_ctx):  # noqa: ANN001, ANN202
        nonlocal recovery_calls
        recovery_calls += 1
        return 0

    monkeypatch.setattr(
        cockpit_api_module,
        "_recover_pending_execution_interrupts",
        recover_none,
    )

    async def scenario() -> list[str]:
        app = web.Application()
        app[cockpit_api_module.INTERRUPT_RECOVERY_CTX_KEY] = ctx
        manager = cockpit_api_module._interrupt_recovery_context(app)  # noqa: SLF001
        await anext(manager)
        await asyncio.sleep(0.05)
        task_names = [task.get_name() for task in asyncio.all_tasks()]
        await manager.aclose()
        return task_names

    task_names = asyncio.run(scenario())

    assert recovery_calls == 1
    assert "cockpit-interrupt-recovery" in task_names


def test_cockpit_interrupt_recovery_event_does_not_lose_wakeup_during_scan(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    ctx = cockpit_api_module.cockpit_context(cfg)
    scan_started = asyncio.Event()
    release_scan = asyncio.Event()
    recovery_calls = 0

    async def recover_after_wakeup(_ctx):  # noqa: ANN001, ANN202
        nonlocal recovery_calls
        recovery_calls += 1
        if recovery_calls == 1:
            scan_started.set()
            await release_scan.wait()
        return 0

    monkeypatch.setattr(
        cockpit_api_module,
        "_recover_pending_execution_interrupts",
        recover_after_wakeup,
    )

    async def scenario() -> None:
        task = asyncio.create_task(
            cockpit_api_module._retry_pending_execution_interrupts(ctx),  # noqa: SLF001
        )
        cockpit_api_module._start_interrupt_recovery(ctx)  # noqa: SLF001
        await asyncio.wait_for(scan_started.wait(), timeout=1)
        cockpit_api_module._start_interrupt_recovery(ctx)  # noqa: SLF001
        release_scan.set()
        deadline = asyncio.get_running_loop().time() + 1.0
        while recovery_calls < 2 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        assert recovery_calls == 2
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_cockpit_idle_server_rearms_recovery_after_later_ambiguous_interrupt(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil", caps=WORKER_SESSION_INTERRUPT)
    _seed_project_registry(cfg)
    index = CockpitThreadIndex(
        Path(cfg.orchestration.workspace) / "cockpit-threads.json"
    )
    attempts = 0
    worker_status = "running"

    def worker_post(url: str, **_kwargs: Any) -> Response:
        nonlocal attempts, worker_status
        if not url.endswith("/interrupt"):
            return Response(
                {"ok": False, "error": "unexpected worker post"},
                status_code=400,
            )
        attempts += 1
        if attempts <= 2:
            raise OSError("ambiguous interrupt delivery")
        worker_status = "interrupted"
        return Response({"ok": True})

    def worker_get(url: str, **_kwargs: Any) -> Response:
        if url.endswith("/execution-state"):
            return Response({"status": worker_status})
        return Response({"ok": True})

    async def scenario(base: str, client: httpx.AsyncClient):
        assert "cockpit-interrupt-recovery" in {
            task.get_name() for task in asyncio.all_tasks()
        }
        thread = CockpitThread(
            thread_id="thread_interrupt_after_idle_start",
            project_id="neil-shared",
            session_id="project:neil-shared:thread_interrupt_after_idle_start",
            title="Interrupt after idle start",
            created_at="2026-07-20T12:00:00Z",
            updated_at="2026-07-20T12:00:00Z",
            created_by="neil",
            chat_type="orchestrator",
            worker_id="macbook-worker",
            workspace={
                "worker_id": "macbook-worker",
                "session_id": "session_interrupt_after_idle_start",
                "session_generation": 3,
                "status": "running",
                "provision_phase": "running",
            },
        )
        index.save(thread)
        response = await client.post(
            f"{base}/v1/projects/neil-shared/threads/{thread.thread_id}/interrupt",
            json={
                "turn_id": "turn_interrupt_after_idle_start",
                "idempotency_key": "interrupt-after-idle-start",
            },
        )
        deadline = asyncio.get_running_loop().time() + 2.0
        recovered = index.get(thread.project_id, thread.thread_id)
        while asyncio.get_running_loop().time() < deadline:
            recovered = index.get(thread.project_id, thread.thread_id)
            if recovered is not None and recovered.workspace.get("status") == "interrupted":
                break
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.05)
        task_names = [task.get_name() for task in asyncio.all_tasks()]
        return response, recovered, task_names

    response, recovered, task_names = asyncio.run(
        _with_server(
            cfg,
            scenario,
            http_get=worker_get,
            http_post=worker_post,
        )
    )

    assert response.status_code == 502
    assert attempts >= 3
    assert recovered is not None
    assert recovered.workspace["status"] == "interrupted"
    assert recovered.workspace["session_id"] == ""
    assert PENDING_EXECUTION_INTERRUPT_KEY not in recovered.workspace
    assert "cockpit-interrupt-recovery" in task_names


def test_cockpit_interrupt_winning_before_completion_commit_drops_transcript_and_memory(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
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
    requester = RequestContext(
        "local-mac",
        "neil",
        "personal",
        frozenset(),
        channel="cockpit",
        peer="neil",
    )
    thread = CockpitThread(
        thread_id="thread_completion_interrupt_race",
        project_id=project.id,
        session_id="project:neil-shared:thread_completion_interrupt_race",
        title="Completion interrupt race",
        created_at="2026-07-20T12:00:00Z",
        updated_at="2026-07-20T12:00:00Z",
        created_by="neil",
        chat_type="orchestrator",
        workspace={
            "worker_id": "macbook-worker",
            "session_id": "orch_completion_interrupt_race",
            "session_generation": 5,
            "status": "running",
            "provision_phase": "running",
            PENDING_ORCHESTRATOR_COMPLETION_KEY: {
                "phase": "accepted",
                "turn_id": "turn_completion_interrupt_race",
            },
        },
    )
    connector.index.save(thread)
    interrupt_requested = threading.Event()
    interrupt_completed = threading.Event()
    persist_calls: list[tuple[Any, ...]] = []

    def interrupt() -> None:
        assert interrupt_requested.wait(timeout=2)
        claimed = connector.index.claim_execution_interrupt(project.id, thread.thread_id)
        assert claimed is not None
        assert claimed.thread.workspace["status"] == "interrupting"
        interrupt_completed.set()

    interrupter = threading.Thread(target=interrupt)
    interrupter.start()

    async def fake_wait(*_args):  # noqa: ANN002, ANN202
        interrupt_requested.set()
        assert await asyncio.to_thread(interrupt_completed.wait, 2)
        return "This reply must not survive an interrupt that won ownership."

    def record_persist(*args):  # noqa: ANN002, ANN202
        persist_calls.append(args)

    monkeypatch.setattr(connector, "_wait_for_orchestrator_turn", fake_wait)
    monkeypatch.setattr(connector, "_persist_turn", record_persist)

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="execution was interrupted"):
            await connector._complete_orchestrator_turn(  # noqa: SLF001
                project,
                thread,
                requester,
                "Original routine request.",
                worker_id="macbook-worker",
                session_id="orch_completion_interrupt_race",
                session_generation=5,
                turn_id="turn_completion_interrupt_race",
                idempotency_key="completion-interrupt-race",
                progress=None,
                durable_completion=True,
            )

    asyncio.run(scenario())
    interrupter.join(timeout=2)
    assert not interrupter.is_alive()

    interrupted = connector.index.get_with_messages(project.id, thread.thread_id)
    assert interrupted is not None
    assert interrupted.workspace["status"] == "interrupting"
    assert PENDING_ORCHESTRATOR_COMPLETION_KEY in interrupted.workspace
    assert interrupted.messages == ()
    assert persist_calls == []


def test_cockpit_routine_run_does_not_accept_a_synchronous_worker_rejection(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    connector = CockpitConnector(
        cfg,
        memory=FakeProjectMemory(),
        gateway=FakeGateway([]),
        tts=None,
        tracer=None,
    )

    async def fake_ensure(_project, thread, _requester, *, progress):  # noqa: ANN001
        del progress
        return connector.index.save(
            replace(
                thread,
                worker_id="macbook-worker",
                workspace={
                    **thread.workspace,
                    "worker_id": "macbook-worker",
                    "session_id": "orch_routine_rejected",
                    "session_generation": 0,
                    "status": "ready",
                    "provision_phase": "ready",
                },
            )
        )

    monkeypatch.setattr(connector, "_ensure_orchestrator_session", fake_ensure)
    def reject_turn(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        raise WorkerRequestError("worker rejected test turn", status_code=409)

    monkeypatch.setattr(connector, "_post_worker_json", reject_turn)
    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: connector)

    async def scenario(base: str, client: httpx.AsyncClient):
        return await client.post(
            f"{base}/v1/routines/pull-request-review/run",
            json={
                "project_id": "neil-shared",
                "prompt": "This dispatch must fail synchronously.",
                "params": _pull_request_review_params(),
                "idempotency_key": "review-rejected-123",
            },
        )

    response = asyncio.run(_with_server(cfg, scenario))

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "routine_dispatch_failed"
    rejected_threads = connector.index.list_all()
    assert len(rejected_threads) == 1
    assert rejected_threads[0].workspace["status"] == "failed"
    assert PENDING_ORCHESTRATOR_COMPLETION_KEY not in rejected_threads[0].workspace


def test_cockpit_routine_schedule_crud_records_routine_reference(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        caps="orchestration.schedules.read,orchestration.schedules.write",
    )
    _seed_project_registry(cfg)

    async def scenario(base: str, client: httpx.AsyncClient):
        created = await client.post(
            f"{base}/v1/schedules",
            json={
                "name": "Weekday brief",
                "routine_id": "morning-brief",
                "project_id": "neil-shared",
                "hour": 9,
                "minute": 15,
                "weekdays": [0, 1, 2, 3, 4],
                "timezone": "Europe/London",
                "idempotency_key": "create-brief",
            },
        )
        listed = await client.get(f"{base}/v1/schedules")
        schedule_id = created.json()["schedule"]["schedule_id"]
        updated = await client.patch(
            f"{base}/v1/schedules/{schedule_id}",
            json={"enabled": False, "idempotency_key": "disable-brief"},
        )
        deleted = await client.request(
            "DELETE",
            f"{base}/v1/schedules/{schedule_id}",
            json={"idempotency_key": "delete-brief"},
        )
        return created, listed, updated, deleted

    created, listed, updated, deleted = asyncio.run(_with_server(cfg, scenario))

    assert created.status_code == 201
    assert created.json()["schedule"]["routine_id"] == "morning-brief"
    assert created.json()["schedule"]["routine_version"] == 1
    assert created.json()["schedule"]["creator_auth_mode"] == "none"
    assert created.json()["schedule"]["params"] == {}
    assert len(listed.json()["schedules"]) == 1
    assert updated.json()["schedule"]["enabled"] is False
    assert deleted.json() == {
        "ok": True,
        "api_version": "v1",
        "schema_version": 1,
        "schedule_id": created.json()["schedule"]["schedule_id"],
        "deleted": True,
    }


def test_cockpit_routine_schedule_create_rejects_invalid_tuning_without_saving(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        caps="orchestration.schedules.read,orchestration.schedules.write",
    )
    _seed_project_registry(cfg)
    _warm_probe_model_catalog_snapshot(cfg)

    async def scenario(base: str, client: httpx.AsyncClient):
        rejected = await client.post(
            f"{base}/v1/schedules",
            json={
                "routine_id": "morning-brief",
                "project_id": "neil-shared",
                "engine": "codex",
                "model": "gpt-nope",
                "idempotency_key": "create-invalid-model",
            },
        )
        listed = await client.get(f"{base}/v1/schedules")
        return rejected, listed

    rejected, listed = asyncio.run(_with_server(cfg, scenario))

    assert rejected.status_code == 400
    assert rejected.json()["error"]["code"] == "validation_failed"
    assert "gpt-x, gpt-y" in rejected.json()["error"]["message"]
    assert listed.json()["schedules"] == []


def test_cockpit_routine_schedule_update_rejects_invalid_tuning_without_mutating(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        caps="orchestration.schedules.read,orchestration.schedules.write",
    )
    _seed_project_registry(cfg)
    _warm_probe_model_catalog_snapshot(cfg)

    async def scenario(base: str, client: httpx.AsyncClient):
        created = await client.post(
            f"{base}/v1/schedules",
            json={
                "routine_id": "morning-brief",
                "project_id": "neil-shared",
                "engine": "codex",
                "model": "gpt-x",
                "idempotency_key": "create-valid-model",
            },
        )
        schedule_id = created.json()["schedule"]["schedule_id"]
        rejected = await client.patch(
            f"{base}/v1/schedules/{schedule_id}",
            json={"model": "gpt-nope", "idempotency_key": "update-invalid-model"},
        )
        listed = await client.get(f"{base}/v1/schedules")
        return created, rejected, listed

    created, rejected, listed = asyncio.run(_with_server(cfg, scenario))

    assert created.status_code == 201
    assert rejected.status_code == 400
    assert rejected.json()["error"]["code"] == "validation_failed"
    schedules = listed.json()["schedules"]
    assert len(schedules) == 1
    assert schedules[0]["model"] == "gpt-x"


def test_oauth_schedule_writes_require_the_requester_grant(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        profile_caps="orchestration.schedules.read,orchestration.schedules.write",
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    _seed_project_registry(cfg)
    _seed_user_profile(cfg, "neil", capabilities=["orchestration.schedules.read"])
    schedule = cockpit_api_module._routine_schedule_store(  # noqa: SLF001
        cockpit_api_module.cockpit_context(cfg)
    ).create(
        name="Protected brief",
        routine_id="morning-brief",
        routine_version=1,
        project_id="neil-shared",
        created_by="neil",
        creator_auth_mode="oauth",
        hour=9,
        minute=15,
        timezone="UTC",
    )
    token = fixture["sign"](subject="neil", jarvis_user="neil", scope="jarvis:read")
    headers = {"Authorization": f"Bearer {token}"}
    dispatches: list[str] = []

    async def fake_execute(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        dispatches.append("dispatched")
        return {"ok": True}

    monkeypatch.setattr(cockpit_api_module, "_execute_routine", fake_execute)

    async def scenario(base: str, client: httpx.AsyncClient):
        capabilities = await client.get(f"{base}/v1/capabilities", headers=headers)
        created = await client.post(
            f"{base}/v1/schedules",
            headers=headers,
            json={
                "name": "Denied brief",
                "routine_id": "morning-brief",
                "project_id": "neil-shared",
                "hour": 9,
                "minute": 15,
                "timezone": "UTC",
                "idempotency_key": "denied-create",
            },
        )
        updated = await client.patch(
            f"{base}/v1/schedules/{schedule.schedule_id}",
            headers=headers,
            json={"enabled": False, "idempotency_key": "denied-update"},
        )
        run = await client.post(
            f"{base}/v1/schedules/{schedule.schedule_id}/run",
            headers=headers,
            json={"idempotency_key": "denied-run"},
        )
        deleted = await client.request(
            "DELETE",
            f"{base}/v1/schedules/{schedule.schedule_id}",
            headers=headers,
            json={"idempotency_key": "denied-delete"},
        )
        assert capabilities.json()["features"]["schedules"]["writable"] is False
        assert "orchestration.schedules.write" not in capabilities.json()["capabilities"]
        return created, updated, run, deleted

    responses = asyncio.run(_with_server(cfg, scenario, http_get=jwks_get))

    assert [response.status_code for response in responses] == [403, 403, 403, 403]
    assert {
        response.json()["error"]["message"] for response in responses
    } == {"missing authority: orchestration.schedules.write"}
    assert dispatches == []
    assert cockpit_api_module._routine_schedule_store(  # noqa: SLF001
        cockpit_api_module.cockpit_context(cfg)
    ).get(schedule.schedule_id) is not None


def test_oauth_schedule_list_requires_the_requester_read_grant(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        profile_caps="orchestration.schedules.read,orchestration.schedules.write",
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    _seed_project_registry(cfg)
    _seed_user_profile(cfg, "neil", capabilities=["orchestration.schedules.write"])
    token = fixture["sign"](subject="neil", jarvis_user="neil", scope="jarvis:read")
    headers = {"Authorization": f"Bearer {token}"}

    async def scenario(base: str, client: httpx.AsyncClient):
        capabilities = await client.get(f"{base}/v1/capabilities", headers=headers)
        listed = await client.get(f"{base}/v1/schedules", headers=headers)
        return capabilities, listed

    capabilities, listed = asyncio.run(_with_server(cfg, scenario, http_get=jwks_get))

    assert capabilities.status_code == 200
    assert capabilities.json()["features"]["schedules"]["available"] is False
    assert "orchestration.schedules.read" not in capabilities.json()["capabilities"]
    assert listed.status_code == 403
    assert listed.json()["error"]["message"] == "missing authority: orchestration.schedules.read"


def test_scheduled_routine_uses_trigger_local_date_for_dynamic_today(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, identity="neil")
    _seed_project_registry(cfg)
    prompts: list[str] = []

    class FakeRoutineConnector:
        async def open_thread(self, project, requester, **kwargs):  # noqa: ANN001
            now = "2026-07-20T12:00:00+00:00"
            return CockpitThread(
                thread_id="thread_scheduled_date",
                project_id=project.id,
                session_id="conv_scheduled_date",
                title=kwargs["title"],
                created_at=now,
                updated_at=now,
                created_by=requester.identity,
                chat_type="orchestrator",
                engine=kwargs["engine"],
            )

        async def turn(self, _project, thread, _requester, text, **_kwargs):  # noqa: ANN001
            prompts.append(text)
            return "Workspace turn is running.", thread, ()

    monkeypatch.setattr(cockpit_api_module, "_cockpit_connector", lambda _ctx: FakeRoutineConnector())
    ctx = cockpit_api_module.cockpit_context(cfg)
    requester = RequestContext(
        device_id=cfg.capabilities.device_id,
        identity="neil",
        scope="personal",
        capabilities=frozenset(),
        channel="schedule",
        peer="neil",
    )

    asyncio.run(
        cockpit_api_module._execute_routine(  # noqa: SLF001
            ctx,
            requester,
            "morning-brief",
            {"project_id": "neil-shared", "params": {}, "target": {}},
            trigger={"type": "schedule", "local_date": "2030-01-02"},
        )
    )

    assert len(prompts) == 1
    assert "morning brief for 2030-01-02" in prompts[0]
    assert "Focus on Priorities, deadlines, blockers, and decisions that need attention." in prompts[0]


def test_routine_schedule_tick_rechecks_revoked_oauth_creator_grant(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    fixture, jwks_get = _oauth_fixture()
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        profile_caps="orchestration.schedules.write",
        auth_mode="oauth",
        oauth_issuer="https://cockpit.example",
        oauth_audience="jarvis-brain",
        oauth_jwks_url="https://cockpit.example/api/auth/jwks",
        oauth_required_scopes="jarvis:read",
    )
    _seed_project_registry(cfg)
    _seed_user_profile(cfg, "neil", capabilities=["orchestration.schedules.write"])
    token = fixture["sign"](subject="neil", jarvis_user="neil", scope="jarvis:read")

    async def create_schedule(base: str, client: httpx.AsyncClient):
        return await client.post(
            f"{base}/v1/schedules",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "name": "Revocable brief",
                "routine_id": "morning-brief",
                "project_id": "neil-shared",
                "hour": 9,
                "minute": 15,
                "timezone": "UTC",
                "idempotency_key": "revocable-create",
            },
        )

    created = asyncio.run(_with_server(cfg, create_schedule, http_get=jwks_get))
    assert created.status_code == 201
    assert created.json()["schedule"]["creator_auth_mode"] == "oauth"
    schedule_id = created.json()["schedule"]["schedule_id"]

    _seed_user_profile(cfg, "neil", capabilities=[])
    ctx = cockpit_api_module.cockpit_context(cfg)
    dispatches: list[str] = []

    async def fake_execute(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        dispatches.append("dispatched")
        return {"ok": True}

    monkeypatch.setattr(cockpit_api_module, "_execute_routine", fake_execute)
    now = datetime(2026, 7, 20, 9, 15, tzinfo=UTC)
    asyncio.run(cockpit_api_module._dispatch_due_routine_schedules(ctx, now))  # noqa: SLF001

    assert dispatches == []
    schedule = cockpit_api_module._routine_schedule_store(ctx).get(schedule_id)  # noqa: SLF001
    assert schedule is not None
    assert schedule.last_fired_date == ""


def test_routine_schedule_tick_rechecks_authority_and_acks_only_after_dispatch(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        caps="orchestration.schedules.write",
    )
    _seed_project_registry(cfg)
    ctx = cockpit_api_module.cockpit_context(cfg)
    store = cockpit_api_module._routine_schedule_store(ctx)  # noqa: SLF001
    schedule = store.create(
        name="Morning brief",
        routine_id="morning-brief",
        routine_version=1,
        project_id="neil-shared",
        created_by="neil",
        params={"day": "2026-07-20"},
        target={},
        hour=9,
        minute=15,
        timezone="UTC",
        first_eligible_date="2026-07-20",
    )
    dispatches: list[str] = []

    async def fake_execute(  # noqa: ANN001
        _ctx, _requester, routine_id, _body, *, trigger, launch_reservation
    ):
        assert launch_reservation["run_id"].startswith("routine_")
        assert launch_reservation["thread_id"].startswith("thread_")
        dispatches.append(f"{routine_id}:{trigger['schedule_id']}:{trigger['local_date']}")
        return {"ok": True, "run": {"status": "started"}}

    monkeypatch.setattr(cockpit_api_module, "_execute_routine", fake_execute)
    now = datetime(2026, 7, 20, 9, 15, tzinfo=UTC)

    asyncio.run(cockpit_api_module._dispatch_due_routine_schedules(ctx, now))  # noqa: SLF001
    asyncio.run(cockpit_api_module._dispatch_due_routine_schedules(ctx, now))  # noqa: SLF001

    assert dispatches == [f"morning-brief:{schedule.schedule_id}:2026-07-20"]
    assert store.get(schedule.schedule_id).last_fired_date == "2026-07-20"  # type: ignore[union-attr]

    denied_root = tmp_path / "denied"
    denied_root.mkdir()
    denied_cfg = _cfg(denied_root, monkeypatch, identity="neil", caps="")
    denied_ctx = cockpit_api_module.cockpit_context(denied_cfg)
    denied_store = cockpit_api_module._routine_schedule_store(denied_ctx)  # noqa: SLF001
    denied = denied_store.create(
        name="Denied",
        routine_id="morning-brief",
        routine_version=1,
        project_id="neil-shared",
        created_by="neil",
        params={"day": "2026-07-20"},
        target={},
        hour=9,
        minute=15,
        timezone="UTC",
        first_eligible_date="2026-07-20",
    )
    asyncio.run(cockpit_api_module._dispatch_due_routine_schedules(denied_ctx, now))  # noqa: SLF001
    assert denied_store.get(denied.schedule_id).last_fired_date == ""  # type: ignore[union-attr]


def test_routine_schedule_tick_resumes_reserved_launch_after_response_save_failure(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path, monkeypatch, identity="neil", caps="orchestration.schedules.write"
    )
    _seed_project_registry(cfg)
    ctx = cockpit_api_module.cockpit_context(cfg)
    store = cockpit_api_module._routine_schedule_store(ctx)  # noqa: SLF001
    schedule = store.create(
        name="Crash-safe brief",
        routine_id="morning-brief",
        routine_version=1,
        project_id="neil-shared",
        created_by="neil",
        hour=9,
        minute=15,
        timezone="UTC",
        first_eligible_date="2026-07-20",
    )
    connector = CockpitConnector(
        cfg, memory=FakeProjectMemory(), gateway=FakeGateway([]), tts=None, tracer=None
    )
    worker_posts: list[dict[str, Any]] = []

    async def fake_ensure(_project, thread, _requester, *, progress):  # noqa: ANN001
        del progress
        return connector.index.save(
            replace(
                thread,
                worker_id="macbook-worker",
                workspace={
                    **thread.workspace,
                    "worker_id": "macbook-worker",
                    "session_id": "orch_reserved_schedule",
                    "session_generation": 0,
                    "status": "ready",
                    "provision_phase": "ready",
                },
            )
        )

    def fake_post(_worker_id: str, _path: str, body: dict[str, Any]) -> dict[str, Any]:
        worker_posts.append(body)
        return {"ok": True}

    async def fake_wait(_worker_id: str, _session_id: str, _turn_id: str) -> str:
        return "Scheduled routine completed."

    original_save = cockpit_api_module.IdempotencyStore.save
    response_saves = 0

    def fail_first_automatic_response_save(store_instance, scope, key, body, response):  # noqa: ANN001, ANN202
        nonlocal response_saves
        expected_scope = (
            f"routine-schedules/{schedule.schedule_id}/automatic-run/principal/"
        )
        if scope.startswith(expected_scope):
            response_saves += 1
            if response_saves == 1:
                raise OSError("simulated scheduler crash after worker acceptance")
        return original_save(store_instance, scope, key, body, response)

    monkeypatch.setattr(connector, "_ensure_orchestrator_session", fake_ensure)
    monkeypatch.setattr(connector, "_post_worker_json", fake_post)
    monkeypatch.setattr(connector, "_wait_for_orchestrator_turn", fake_wait)
    monkeypatch.setattr(
        cockpit_api_module, "_cockpit_connector", lambda _ctx: connector
    )
    monkeypatch.setattr(
        cockpit_api_module.IdempotencyStore, "save", fail_first_automatic_response_save
    )
    now = datetime(2026, 7, 20, 9, 15, tzinfo=UTC)

    asyncio.run(cockpit_api_module._dispatch_due_routine_schedules(ctx, now))  # noqa: SLF001
    assert store.get(schedule.schedule_id).last_fired_date == ""  # type: ignore[union-attr]
    reserved_thread = connector.index.list_all()[0]

    asyncio.run(cockpit_api_module._dispatch_due_routine_schedules(ctx, now))  # noqa: SLF001

    fired = store.get(schedule.schedule_id)
    assert fired is not None
    assert fired.last_fired_date == "2026-07-20"
    assert len(connector.index.list_all()) == 1
    assert connector.index.list_all()[0].thread_id == reserved_thread.thread_id
    assert len(worker_posts) == 1


def test_routine_schedule_retries_a_failed_dispatch_after_the_target_minute(
    tmp_path, monkeypatch
) -> None:  # noqa: ANN001
    cfg = _cfg(
        tmp_path,
        monkeypatch,
        identity="neil",
        caps="orchestration.schedules.write",
    )
    _seed_project_registry(cfg)
    ctx = cockpit_api_module.cockpit_context(cfg)
    store = cockpit_api_module._routine_schedule_store(ctx)  # noqa: SLF001
    schedule = store.create(
        name="Morning brief",
        routine_id="morning-brief",
        routine_version=1,
        project_id="neil-shared",
        created_by="neil",
        hour=9,
        minute=15,
        timezone="UTC",
        first_eligible_date="2026-07-20",
    )
    dispatches: list[str] = []

    async def flaky_execute(  # noqa: ANN001
        _ctx, _requester, routine_id, _body, *, trigger, launch_reservation
    ):
        assert launch_reservation["run_id"].startswith("routine_")
        dispatches.append(f"{routine_id}:{trigger['schedule_id']}")
        if len(dispatches) == 1:
            raise RuntimeError("worker temporarily unavailable")
        return {"ok": True, "run": {"status": "started"}}

    monkeypatch.setattr(cockpit_api_module, "_execute_routine", flaky_execute)

    asyncio.run(
        cockpit_api_module._dispatch_due_routine_schedules(  # noqa: SLF001
            ctx,
            datetime(2026, 7, 20, 9, 16, tzinfo=UTC),
        )
    )
    assert store.get(schedule.schedule_id).last_fired_date == ""  # type: ignore[union-attr]

    asyncio.run(
        cockpit_api_module._dispatch_due_routine_schedules(  # noqa: SLF001
            ctx,
            datetime(2026, 7, 20, 9, 17, tzinfo=UTC),
        )
    )

    assert dispatches == [
        f"morning-brief:{schedule.schedule_id}",
        f"morning-brief:{schedule.schedule_id}",
    ]
    assert store.get(schedule.schedule_id).last_fired_date == "2026-07-20"  # type: ignore[union-attr]
