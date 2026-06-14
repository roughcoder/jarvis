"""Worker daemon HTTP surface — tested in isolation (Phase 3c).

Spins up the real aiohttp app on a local port and drives it over HTTP: health,
auth, a shell dispatch, an unknown action, and a `code` job lifecycle (using
`echo` as a stand-in agent so it's instant). Self-contained — no gateway/keys.
Skips if aiohttp (the `worker` extra) isn't installed.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

pytest.importorskip("aiohttp")
from aiohttp import web  # noqa: E402

from jarvis.config import WorkerConfig  # noqa: E402
from jarvis.worker.server import make_app  # noqa: E402


async def _with_server(cfg: WorkerConfig, port: int, fn):  # noqa: ANN001
    runner = web.AppRunner(make_app(cfg))
    await runner.setup()
    site = web.TCPSite(runner, "localhost", port)
    await site.start()
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            return await fn(f"http://localhost:{port}", client)
    finally:
        await runner.cleanup()


def test_daemon_health_shell_and_auth() -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace="jarvis-workspace/worker")
    h = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        health = (await c.get(base + "/health")).json()
        noauth = await c.post(base + "/run", json={"action": "shell", "args": {"command": "echo x"}})
        shell = (
            await c.post(base + "/run", json={"action": "shell", "args": {"command": "echo worker-ok"}}, headers=h)
        ).json()
        bad = await c.post(base + "/run", json={"action": "shell"}, headers={"Authorization": "Bearer nope"})
        unknown = await c.post(base + "/run", json={"action": "frobnicate"}, headers=h)
        return health, noauth.status_code, shell, bad.status_code, unknown.status_code

    health, noauth, shell, bad, unknown = asyncio.run(_with_server(cfg, 8802, calls))
    assert health["ok"] is True
    assert noauth == 401  # missing token
    assert bad == 401  # wrong token
    assert shell["output"] == "worker-ok"
    assert unknown == 400  # unknown action


def test_daemon_code_dispatch_runs_a_job() -> None:
    # `echo` stands in for the coding agent so the job finishes instantly.
    cfg = WorkerConfig(_env_file=None, token="", workspace="jarvis-workspace/worker", codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        disp = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "hello-job"}})).json()
        jid = disp["job_id"]
        status = "running"
        for _ in range(100):
            status = (await c.get(f"{base}/jobs/{jid}")).json()["status"]
            if status != "running":
                break
            await asyncio.sleep(0.02)
        listed = (await c.get(base + "/jobs")).json()
        return disp, status, listed

    disp, status, listed = asyncio.run(_with_server(cfg, 8803, calls))
    assert disp["ok"] and disp["job_id"]
    assert status == "done"
    assert len(listed["jobs"]) >= 1
