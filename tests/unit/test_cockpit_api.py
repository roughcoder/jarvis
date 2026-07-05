from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("aiohttp")
pytest.importorskip("httpx")

import httpx  # noqa: E402
from aiohttp import web  # noqa: E402

from jarvis.config import Config, WorkerConfig  # noqa: E402
import jarvis.orchestration.api as cockpit_api_module  # noqa: E402
from jarvis.orchestration.api import CockpitAppContext, IdempotencyStore, SseSnapshotHub, _command_from_body, _idempotency_scope, make_app, serve  # noqa: E402
from jarvis.orchestration.cockpit import make_session_ref  # noqa: E402
from jarvis.orchestration.models import Artifact, ExecutionEnvelope, WorkItem, WorkerProfile, WorkerSessionLink  # noqa: E402
from jarvis.orchestration.service import StartedWork  # noqa: E402
from jarvis.orchestration.store import OrchestrationStore  # noqa: E402


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


async def _with_server(cfg: Config, fn: Callable[[str, httpx.AsyncClient], Any], *, http_get=None, http_post=None) -> Any:  # noqa: ANN001
    runner = web.AppRunner(make_app(cfg, http_get=http_get, http_post=http_post))
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
) -> Config:
    env = tmp_path / ".env"
    workspace = tmp_path / "orchestration"
    workers_path = workspace / "workers.json"
    registry_path = tmp_path / "registry.json"
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
                "WORKER_HOST=worker.test",
                "WORKER_PORT=8780",
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
                        "capabilities": ["git", "shell", "browser", "codex"],
                        "max_concurrent_jobs": 4,
                        "current_jobs": 1,
                        "status": "online",
                        "agent": "codex",
                        "supported_engines": ["codex", "claude"],
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

        assert snapshot["sessions"][0]["supported_controls"] == ["turn", "stop", "archive"]

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
            await asyncio.sleep(0.35)
        finally:
            await hub.stop()

    import asyncio

    asyncio.run(run_hub())

    assert calls["count"] >= 3
    assert logs == ["cockpit SSE snapshot refresh failed"]


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
        unauthorized = await client.get(f"{base}/v1/health")
        bad_ref = await client.get(f"{base}/v1/sessions/not-a-ref", headers={"Authorization": "Bearer secret"})

        assert unauthorized.status_code == 401
        assert unauthorized.json()["error"]["code"] == "unauthorized"
        assert bad_ref.status_code == 404
        assert bad_ref.json()["error"]["code"] == "not_found"

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
        hidden = await client.get(f"{base}/v1/projects/alice-private")
        missing = await client.get(f"{base}/v1/projects/not-real")

        assert visible.status_code == 200
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
        assert missing.status_code == 404
        assert missing.json()["error"]["code"] == "not_found"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


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

        assert listing.status_code == 401
        assert listing.json()["error"]["code"] == "unauthorized"
        assert detail.status_code == 401
        assert detail.json()["error"]["code"] == "unauthorized"

    import asyncio

    asyncio.run(_with_server(cfg, calls))


def test_cockpit_projects_without_requester_identity_are_empty(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _seed_project_registry(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        listing = await client.get(f"{base}/v1/projects")
        detail = await client.get(f"{base}/v1/projects/house-story")

        assert listing.status_code == 200
        assert listing.json()["projects"] == []
        assert detail.status_code == 404
        assert detail.json()["error"]["code"] == "not_found"

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


def test_cockpit_turn_and_start_reject_attachments_in_v1(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch, caps="worker.session.turn,worker.job.start,worker.session.create,forge.github.branch.push")
    _store, run_id = _seed_run(cfg)
    ref = make_session_ref("macbook-worker", "sess_123")

    def post(_url: str, **_kwargs) -> Response:  # noqa: ANN001
        raise AssertionError("attachment-bearing cockpit requests should fail before worker dispatch")

    attachment = {"kind": "image", "mime_type": "image/png", "name": "screenshot.png", "data_url": "data:image/png;base64,AAAA"}

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        turn = await client.post(f"{base}/v1/sessions/{ref}/turns", json={"idempotency_key": "turn_attach", "prompt": "see this", "attachments": [attachment]})
        start = await client.post(
            f"{base}/v1/work/start",
            json={"idempotency_key": "start_attach", "source": "manual", "repo": "roughcoder/jarvis", "phrase": "start", "attachments": [attachment]},
        )

        assert turn.status_code == 400
        assert start.status_code == 400
        assert turn.json()["error"]["code"] == "validation_failed"
        assert start.json()["error"]["recoverable"] is True
        assert "attachments are not supported" in start.json()["error"]["message"]

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
        assert len(post_calls) == 2
        # The synchronous first-turn events from dispatch land in the run log.
        store = OrchestrationStore(cfg.orchestration.workspace)
        run_id = body["run"]["run_id"]
        persisted_ids = [e.data.get("event_id") for e in store.events(run_id) if isinstance(e.data, dict) and e.data.get("event_id")]
        assert persisted_ids == ["ev_turn"]

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get("")))


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
        repositories=[
            {"repo": "jarvis", "default_branch": "main", "status": "ready"},
            {"repo": "polymarket", "default_branch": "develop", "status": "cloning"},
            {"name": "legacy-name-key"},
            {"no_repo": True},
        ],
    )

    rows = project_worker_profile(profile, default_repo="roughcoder/jarvis")["repositories"]

    assert rows == [
        {"repo": "jarvis", "status": "ready", "default_branch": "main", "is_default": True, "can_start_work": True},
        {"repo": "polymarket", "status": "cloning", "default_branch": "develop", "is_default": False, "can_start_work": False},
        {"repo": "legacy-name-key", "status": "ready", "default_branch": "", "is_default": False, "can_start_work": True},
    ]


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
    assert validation["missing"] == []
    assert validation["missing_authority"] == []
    assert OrchestrationStore(cfg.orchestration.workspace).list_runs() == []


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
    assert frames[0]["payload"]["type"] == "assistant.message"


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
