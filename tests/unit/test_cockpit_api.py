from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("aiohttp")
pytest.importorskip("httpx")

import httpx  # noqa: E402
from aiohttp import web  # noqa: E402

from jarvis.config import Config  # noqa: E402
from jarvis.orchestration.api import make_app  # noqa: E402
from jarvis.orchestration.cockpit import make_session_ref  # noqa: E402
from jarvis.orchestration.models import Artifact, WorkItem, WorkerSessionLink  # noqa: E402
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
                    }
                ]
            }
        )
    )
    return Config()


def _seed_run(cfg: Config) -> tuple[OrchestrationStore, str]:
    store = OrchestrationStore(cfg.orchestration.workspace)
    item = WorkItem(source="github", id="#47", title="Build worker sessions", repo="roughcoder/jarvis")
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
        ),
    )
    store.append_event(run.run_id, "verification_started", "Running tests", {"command": "pytest"})
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
                        }
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
                            "data": {"turn_id": "turn_1", "delta": "hello"},
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
                                "data": {"run_id": run_id, "title": "Approve file edits", "detail": "/Users/example/private/file"},
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
        snapshot = (await client.get(f"{base}/v1/cockpit/snapshot", params={"sync": "probe"})).json()
        workers = (await client.get(f"{base}/v1/workers", params={"sync": "probe"})).json()

        assert catalog["api_version"] == "v1"
        assert "manual" in catalog["work_sources"]
        assert snapshot["schema_version"] == 1
        assert snapshot["sync"]["status"] == "fresh"
        assert snapshot["runs"][0]["run_id"] == run_id
        assert snapshot["runs"][0]["pending_approval_count"] == 1
        assert snapshot["sessions"][0]["session_ref"].startswith("sessref_")
        assert snapshot["sessions"][0]["cwd_label"] == "jarvis"
        assert "/Users/" not in json.dumps(snapshot)
        assert workers["workers"][0]["capacity"]["max_sessions"] == 4
        assert workers["workers"][0]["engines"][0]["engine"] == "codex"

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=get))


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
        assert "cwd" not in detail["raw"]
        assert "metadata" not in detail["raw"]
        assert events["items"][0]["sequence"] == 1
        assert events["has_more"] is True
        all_events = (await client.get(f"{base}/v1/sessions/{ref}/events")).json()["items"]
        delta = [event for event in all_events if event["type"] == "assistant.delta"][0]
        assert delta["message_id"] == "msg_turn_1"
        assert requests["requests"][0]["title"] == "Approve file edits"
        assert "<local-path>" in requests["requests"][0]["detail"]
        assert checkpoints["checkpoints"][0]["session_ref"] == ref

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


def test_cockpit_run_events_and_artifact_pagination(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    cfg = _cfg(tmp_path, monkeypatch)
    _store, run_id = _seed_run(cfg)

    async def calls(base: str, client: httpx.AsyncClient) -> None:
        events = (await client.get(f"{base}/v1/runs/{run_id}/events", params={"limit": 1})).json()
        artifacts = (await client.get(f"{base}/v1/runs/{run_id}/artifacts", params={"limit": 2})).json()

        assert events["items"][0]["type"] == "run_created"
        assert events["has_more"] is True
        kinds = {item["kind"] for item in artifacts["items"]}
        assert {"branch", "pull_request"}.issubset(kinds)
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

    import asyncio

    asyncio.run(_with_server(cfg, calls, http_get=_fake_get(run_id)))


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
        body = {"idempotency_key": "t3_key", "prompt": "continue", "metadata": {"surface": "jarvis-cockpit"}}
        first = (await client.post(f"{base}/v1/sessions/{ref}/turns", json=body)).json()
        second = (await client.post(f"{base}/v1/sessions/{ref}/turns", json=body)).json()
        conflict = await client.post(f"{base}/v1/sessions/{ref}/turns", json={**body, "prompt": "different"})

        assert first["ok"] is True
        assert first["events"][0]["event_id"] == "ev_turn"
        assert second["idempotent"] is True
        assert len(posts) == 1
        assert conflict.status_code == 409
        assert conflict.json()["error"]["code"] == "idempotency_conflict"

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
        return Response({"ok": False, "error": "no such checkpoint: ckpt_missing"}, status_code=409)

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
