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


def test_no_repo_jobs_get_isolated_run_dirs(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        r1 = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "a", "name": "job one"}})).json()
        r2 = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "b", "name": "job two"}})).json()
        await asyncio.sleep(0.3)
        j1 = (await c.get(f"{base}/jobs/{r1['job_id']}")).json()
        j2 = (await c.get(f"{base}/jobs/{r2['job_id']}")).json()
        return j1, j2

    j1, j2 = asyncio.run(_with_server(cfg, 8810, calls))
    assert j1["cwd"] != j2["cwd"]  # isolated per job
    assert "/runs/" in j1["cwd"]
    assert "job-one" in j1["cwd"]


def test_repo_job_isolates_on_a_worktree_branch(tmp_path) -> None:
    import pathlib
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run([*git, "init", "-q"], cwd=repo, check=True)
    subprocess.run([*git, "commit", "--allow-empty", "-qm", "init"], cwd=repo, check=True)
    (repo / "original.txt").write_text("untouched")

    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "ws"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        r = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "do x", "name": "refactor", "repo": str(repo)}})).json()
        await asyncio.sleep(0.3)
        j = (await c.get(f"{base}/jobs/{r['job_id']}")).json()
        return r, j

    r, j = asyncio.run(_with_server(cfg, 8812, calls))
    assert r["branch"].startswith("jarvis/refactor-")
    assert "/worktrees/" in j["cwd"] and pathlib.Path(j["cwd"]).exists()
    assert j["cwd"] != str(repo)  # NOT the user's checkout
    assert (repo / "original.txt").read_text() == "untouched"  # checkout untouched
    branches = subprocess.run(["git", "-C", str(repo), "branch", "--list", r["branch"]], capture_output=True, text=True).stdout
    assert r["branch"] in branches
