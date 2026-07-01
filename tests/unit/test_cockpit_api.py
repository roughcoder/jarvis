from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("aiohttp")
pytest.importorskip("httpx")

import httpx  # noqa: E402
from aiohttp import web  # noqa: E402

from jarvis.config import Config  # noqa: E402
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


def _cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, caps: str = "", token: str = "") -> Config:
    env = tmp_path / ".env"
    workspace = tmp_path / "orchestration"
    workers_path = workspace / "workers.json"
    env.write_text(
        "\n".join(
            [
                f"ORCHESTRATION_WORKSPACE={workspace}",
                f"ORCHESTRATION_WORKERS_PATH={workers_path}",
                "ORCHESTRATION_LANDING_MODE=branch_only",
                f"ORCHESTRATION_API_TOKEN={token}",
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


def _fake_get(run_id: str):  # noqa: ANN202
    def get(url: str, **_kwargs) -> Response:  # noqa: ANN001
        if url.endswith("/health"):
            return Response({"ok": True, "agent": "codex", "supported_engines": ["codex", "claude"]})
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
        if url.endswith("/sessions/sess_123/checkpoints"):
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
        assert unknown_cursor.json()["error"]["code"] == "validation_failed"
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
                if "event: snapshot" in seen:
                    break
        await task

        assert "event: snapshot" in seen
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

        assert "event: snapshot" in seen
        assert '"mode": "fast"' in seen
        assert '"status": "fresh"' in seen
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
            assert first_event["cursor"] == second_event["cursor"]
            assert request_calls["count"] == 2
        finally:
            await hub.stop()

    import asyncio

    asyncio.run(run_hub())


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

        assert response.status_code == 200
        assert response.json()["run"]["archived_at"]
        assert snapshot["runs"] == []
        assert snapshot["sessions"] == []
        assert runs["runs"] == []
        assert sessions["sessions"] == []

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
