"""Fleet status contract for the native operator UI."""

from __future__ import annotations

import asyncio
import json

import pytest

from jarvis.config import Config, WorkerConfig
from jarvis.fleet import collect_fleet_status, docker_compose_status, probe_worker


def test_collect_fleet_status_shape_has_no_tokens(monkeypatch) -> None:
    async def fake_brain(_cfg):  # noqa: ANN001, ANN202
        return {
            "reachable": True,
            "paired": True,
            "identity": "neil",
            "scope": "personal",
            "capabilities": ["web.search"],
        }

    async def fake_worker(_cfg):  # noqa: ANN001, ANN202
        return {"reachable": False, "error": "offline"}

    monkeypatch.setattr("jarvis.fleet.probe_brain", fake_brain)
    monkeypatch.setattr("jarvis.fleet.probe_worker", fake_worker)
    cfg = Config()
    data = asyncio.run(collect_fleet_status(cfg, include_docker=False))

    assert data["device_id"] == cfg.capabilities.device_id
    assert "api" in data["services"]
    assert data["intercom"]["pairing"]["paired"] is True
    assert data["worker"]["probe"]["reachable"] is False
    dumped = json.dumps(data)
    if cfg.intercom.token.get_secret_value():
        assert cfg.intercom.token.get_secret_value() not in dumped
    if cfg.worker.token.get_secret_value():
        assert cfg.worker.token.get_secret_value() not in dumped


def test_probe_worker_reads_health_and_jobs(tmp_path) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web

    from jarvis.worker.server import make_app

    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path), codex_bin="echo")

    async def go():  # noqa: ANN202
        runner = web.AppRunner(make_app(cfg))
        await runner.setup()
        site = web.TCPSite(runner, "localhost", 8820)
        await site.start()
        try:
            all_cfg = Config()
            all_cfg.worker = WorkerConfig(
                _env_file=None,
                host="localhost",
                port=8820,
                request_timeout_s=10.0,
            )
            return await probe_worker(all_cfg)
        finally:
            await runner.cleanup()

    data = asyncio.run(go())
    assert data["reachable"] is True
    assert data["health"]["ok"] is True
    assert data["health"]["browser_enabled"] is True
    assert data["jobs"] == {"total": 0, "running": 0, "recent": []}


def test_docker_compose_status_is_neutral_without_compose_project(tmp_path) -> None:
    data = docker_compose_status(str(tmp_path))

    assert data == {
        "available": False,
        "configured": False,
        "status": "not_configured",
        "detail": "No local Docker compose project found.",
        "services": [],
    }
