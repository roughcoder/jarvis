"""Worker daemon HTTP surface — tested in isolation (Phase 3c).

Spins up the real aiohttp app on a local port and drives it over HTTP: health,
auth, a shell dispatch, an unknown action, and a `code` job lifecycle (using
`echo` as a stand-in agent so it's instant). Self-contained — no gateway/keys.
Skips if aiohttp (the `worker` extra) isn't installed.
"""

from __future__ import annotations

import asyncio
import pathlib

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


def test_daemon_health_shell_and_auth(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
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


def test_daemon_shell_uses_expanded_default_workspace(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    cfg = WorkerConfig(_env_file=None, token="", workspace="~/jarvis-worker")

    async def calls(base, c):  # noqa: ANN001
        health = (await c.get(base + "/health")).json()
        shell = (
            await c.post(base + "/run", json={"action": "shell", "args": {"command": "pwd"}})
        ).json()
        return health, shell

    health, shell = asyncio.run(_with_server(cfg, 8817, calls))

    assert health["workspace"] == str(home / "jarvis-worker")
    assert shell["output"] == str(home / "jarvis-worker")


def test_daemon_code_dispatch_runs_a_job(tmp_path) -> None:
    # `echo` stands in for the coding agent so the job finishes instantly.
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"), codex_bin="echo")

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


def test_daemon_code_dispatch_marks_nonzero_agent_exit_as_error(tmp_path) -> None:
    agent = tmp_path / "bad-agent"
    agent.write_text("#!/usr/bin/env python3\nimport sys\nprint('bad auth')\nsys.exit(1)\n")
    agent.chmod(0o755)
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"), codex_bin=str(agent))

    async def calls(base, c):  # noqa: ANN001
        disp = (
            await c.post(
                base + "/run",
                json={"action": "code", "args": {"prompt": "hello"}},
            )
        ).json()
        jid = disp["job_id"]
        job = {}
        for _ in range(100):
            job = (await c.get(f"{base}/jobs/{jid}")).json()
            if job["status"] != "running":
                break
            await asyncio.sleep(0.02)
        return job

    job = asyncio.run(_with_server(cfg, 8820, calls))

    assert job["status"] == "error"
    assert "command exited with 1" in job["output"]
    assert "bad auth" in job["output"]


def test_daemon_health_advertises_supported_engines(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        agent="codex",
        supported_engines="codex,claude",
    )

    async def calls(base, c):  # noqa: ANN001
        return (await c.get(base + "/health")).json()

    health = asyncio.run(_with_server(cfg, 8818, calls))

    assert health["default_engine"] == "codex"
    assert health["supported_engines"] == ["codex", "claude"]


def test_daemon_code_dispatch_persists_engine_session_metadata(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        claude_bin="echo",
        supported_engines="codex,claude",
    )
    session_id = "550e8400-e29b-41d4-a716-446655440000"

    async def calls(base, c):  # noqa: ANN001
        disp = (
            await c.post(
                base + "/run",
                json={
                    "action": "code",
                    "args": {
                        "prompt": "hello",
                        "agent": "claude",
                        "session_id": session_id,
                        "session_name": "jarvis-hello",
                    },
                },
            )
        ).json()
        jid = disp["job_id"]
        job = {}
        for _ in range(100):
            job = (await c.get(f"{base}/jobs/{jid}")).json()
            if job["status"] != "running":
                break
            await asyncio.sleep(0.02)
        return disp, job

    disp, job = asyncio.run(_with_server(cfg, 8821, calls))

    assert disp["ok"]
    assert disp["engine"] == "claude"
    assert disp["session_id"] == session_id
    assert disp["session_name"] == "jarvis-hello"
    assert job["engine"] == "claude"
    assert job["session_id"] == session_id
    assert job["session_name"] == "jarvis-hello"
    assert "--session-id 550e8400-e29b-41d4-a716-446655440000 --name jarvis-hello -p hello" in job["output"]


def test_daemon_code_dispatch_resumes_claude_session(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        claude_bin="echo",
        supported_engines="codex,claude",
    )
    session_id = "550e8400-e29b-41d4-a716-446655440000"

    async def calls(base, c):  # noqa: ANN001
        disp = (
            await c.post(
                base + "/run",
                json={
                    "action": "code",
                    "args": {
                        "prompt": "follow up",
                        "agent": "claude",
                        "session_id": session_id,
                        "resume_session": True,
                    },
                },
            )
        ).json()
        jid = disp["job_id"]
        job = {}
        for _ in range(100):
            job = (await c.get(f"{base}/jobs/{jid}")).json()
            if job["status"] != "running":
                break
            await asyncio.sleep(0.02)
        return job

    job = asyncio.run(_with_server(cfg, 8822, calls))

    assert job["session_id"] == session_id
    assert "-p --resume 550e8400-e29b-41d4-a716-446655440000 follow up" in job["output"]


def test_daemon_resume_uses_worker_owned_cwd_without_cleanup_ownership(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        claude_bin="echo",
        supported_engines="codex,claude",
    )
    reused = tmp_path / "worker" / "worktrees" / "jarvis-existing"
    reused.mkdir(parents=True)

    async def calls(base, c):  # noqa: ANN001
        disp = (
            await c.post(
                base + "/run",
                json={
                    "action": "code",
                    "args": {
                        "prompt": "follow up",
                        "agent": "claude",
                        "session_id": "550e8400-e29b-41d4-a716-446655440000",
                        "resume_session": True,
                        "cwd": str(reused),
                        "branch": "jarvis/existing",
                    },
                },
            )
        ).json()
        jid = disp["job_id"]
        job = {}
        for _ in range(100):
            job = (await c.get(f"{base}/jobs/{jid}")).json()
            if job["status"] != "running":
                break
            await asyncio.sleep(0.02)
        cleaned = (await c.post(base + "/run", json={"action": "cleanup", "args": {"job": jid}})).json()
        return disp, job, cleaned

    disp, job, cleaned = asyncio.run(_with_server(cfg, 8823, calls))

    assert disp["cwd"] == str(reused)
    assert disp["branch"] == "jarvis/existing"
    assert job["cwd"] == str(reused)
    assert job["cleanup_owned"] is False
    assert cleaned["cleaned"] == [job["name"]]
    assert reused.exists()


def test_daemon_prune_keeps_workspace_used_by_running_resume(tmp_path) -> None:
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty", "-m", "init"], cwd=repo, check=True)
    slow_agent = tmp_path / "slow-agent"
    slow_agent.write_text("#!/usr/bin/env python3\nimport time\nprint('started')\ntime.sleep(2)\nprint('done')\n")
    slow_agent.chmod(0o755)
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        codex_bin="echo",
        claude_bin=str(slow_agent),
        supported_engines="codex,claude",
    )

    async def calls(base, c):  # noqa: ANN001
        original = (
            await c.post(
                base + "/run",
                json={"action": "code", "args": {"prompt": "first", "agent": "codex", "repo": str(repo)}},
            )
        ).json()
        original_job = {}
        for _ in range(100):
            original_job = (await c.get(f"{base}/jobs/{original['job_id']}")).json()
            if original_job["status"] != "running":
                break
            await asyncio.sleep(0.02)
        resumed = (
            await c.post(
                base + "/run",
                json={
                    "action": "code",
                    "args": {
                        "prompt": "follow up",
                        "agent": "claude",
                        "session_id": "550e8400-e29b-41d4-a716-446655440000",
                        "resume_session": True,
                        "cwd": original_job["cwd"],
                    },
                },
            )
        ).json()
        cleaned = (await c.post(base + "/run", json={"action": "cleanup", "args": {"job": "all"}})).json()
        listed = (await c.get(base + "/jobs")).json()["jobs"]
        for _ in range(100):
            resumed_job = (await c.get(f"{base}/jobs/{resumed['job_id']}")).json()
            if resumed_job["status"] != "running":
                break
            await asyncio.sleep(0.03)
        return original_job, resumed, cleaned, listed

    original_job, resumed, cleaned, listed = asyncio.run(_with_server(cfg, 8825, calls))

    assert resumed["ok"]
    assert cleaned["cleaned"] == []
    assert any(job["id"] == original_job["id"] for job in listed)
    assert any(job["id"] == resumed["job_id"] and job["status"] == "running" for job in listed)
    assert pathlib.Path(original_job["cwd"]).exists()


def test_daemon_resume_rejects_cwd_outside_worker_workspace(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        claude_bin="echo",
        supported_engines="codex,claude",
    )
    outside = tmp_path / "outside"
    outside.mkdir()

    async def calls(base, c):  # noqa: ANN001
        return await c.post(
            base + "/run",
            json={
                "action": "code",
                "args": {
                    "prompt": "follow up",
                    "agent": "claude",
                    "session_id": "550e8400-e29b-41d4-a716-446655440000",
                    "resume_session": True,
                    "cwd": str(outside),
                },
            },
        )

    response = asyncio.run(_with_server(cfg, 8824, calls))

    assert response.status_code == 400
    assert "refusing to resume outside worker-owned workspace" in response.json()["error"]


def test_daemon_rejects_unsupported_code_engine(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        return await c.post(base + "/run", json={"action": "code", "args": {"prompt": "hello", "agent": "claude"}})

    response = asyncio.run(_with_server(cfg, 8819, calls))

    assert response.status_code == 400
    assert "does not support engine 'claude'" in response.json()["error"]


def test_daemon_refuses_worker_workspace_inside_git_checkout(tmp_path) -> None:
    import subprocess

    repo = tmp_path / "repo"
    workspace = repo / "jarvis-workspace" / "worker"
    workspace.mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(workspace))

    with pytest.raises(ValueError, match="inside a git checkout"):
        make_app(cfg)


def test_daemon_rejects_orchestration_code_job_without_start_action(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        return await c.post(
            base + "/run",
            json={
                "action": "code",
                "args": {
                    "prompt": "hello-job",
                    "execution_envelope": {
                        "run_id": "run_1",
                        "allowed_actions": [],
                        "landing": {"mode": "draft_pr", "allow_merge": False},
                    },
                },
            },
        )

    response = asyncio.run(_with_server(cfg, 8814, calls))
    assert response.status_code == 403
    assert "worker.job.start" in response.json()["error"]


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


def test_non_git_repo_input_is_copied_to_worker_scratch(tmp_path) -> None:
    import pathlib

    user_dir = tmp_path / "plain-input"
    user_dir.mkdir()
    (user_dir / "note.txt").write_text("original")
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "ws"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        r = (
            await c.post(
                base + "/run",
                json={"action": "code", "args": {"prompt": "x", "name": "plain", "repo": str(user_dir)}},
            )
        ).json()
        await asyncio.sleep(0.3)
        j = (await c.get(f"{base}/jobs/{r['job_id']}")).json()
        existed_before_cleanup = pathlib.Path(j["cwd"]).exists()
        clean = (await c.post(base + "/run", json={"action": "cleanup", "args": {"job": "plain"}})).json()
        return r, j, existed_before_cleanup, clean

    r, j, existed_before_cleanup, clean = asyncio.run(_with_server(cfg, 8813, calls))
    assert r["branch"] is None
    assert j["cwd"] != str(user_dir)
    assert "/worktrees/" in j["cwd"] and j["cwd"].endswith("-scratch")
    assert existed_before_cleanup
    assert (user_dir / "note.txt").read_text() == "original"
    assert "plain" in clean["cleaned"]
    assert not pathlib.Path(j["cwd"]).exists()
    assert user_dir.exists()


def test_cleanup_removes_worktree_and_branch(tmp_path) -> None:
    import pathlib
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run([*git, "init", "-q"], cwd=repo, check=True)
    subprocess.run([*git, "commit", "--allow-empty", "-qm", "init"], cwd=repo, check=True)
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "ws"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        r = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "x", "name": "cleanme", "repo": str(repo)}})).json()
        await asyncio.sleep(0.3)
        wt = (await c.get(f"{base}/jobs/{r['job_id']}")).json()["cwd"]
        clean = (await c.post(base + "/run", json={"action": "cleanup", "args": {"job": "cleanme"}})).json()
        after = (await c.get(base + "/jobs")).json()["jobs"]
        return r["branch"], wt, clean, after

    branch, wt, clean, after = asyncio.run(_with_server(cfg, 8814, calls))
    assert "cleanme" in clean["cleaned"]
    assert not pathlib.Path(wt).exists()  # worktree removed
    branches = subprocess.run(["git", "-C", str(repo), "branch", "--list", branch], capture_output=True, text=True).stdout
    assert branch not in branches  # branch deleted
    assert after == []  # job dropped from the list


def test_unknown_repo_returns_helpful_error(tmp_path) -> None:
    import subprocess

    dev = tmp_path / "dev"
    (dev / "realrepo").mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=dev / "realrepo", check=True)
    cfg = WorkerConfig(
        _env_file=None, token="", workspace=str(tmp_path / "ws"),
        repo_root=str(dev), clone_missing=False, codex_bin="echo",
    )

    async def calls(base, c):  # noqa: ANN001
        r = await c.post(base + "/run", json={"action": "code", "args": {"prompt": "x", "repo": "missing"}})
        return r.status_code, r.json()

    status, data = asyncio.run(_with_server(cfg, 8817, calls))
    assert status == 404
    assert "couldn't find" in data["error"]
    assert "realrepo" in data["error"]  # the error lists the repos it can see


def test_list_repos_action(tmp_path) -> None:
    import subprocess

    dev = tmp_path / "dev"
    (dev / "alpha").mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=dev / "alpha", check=True)
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "ws"), repo_root=str(dev))

    async def calls(base, c):  # noqa: ANN001
        return (await c.post(base + "/run", json={"action": "list_repos"})).json()

    data = asyncio.run(_with_server(cfg, 8818, calls))
    assert data["repos"] == ["alpha"]


def test_cleanup_all_removes_scratch_dir(tmp_path) -> None:
    import pathlib

    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "ws"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        r = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "x", "name": "scratchy"}})).json()
        await asyncio.sleep(0.3)
        cwd = (await c.get(f"{base}/jobs/{r['job_id']}")).json()["cwd"]
        clean = (await c.post(base + "/run", json={"action": "cleanup", "args": {"job": ""}})).json()
        return cwd, clean

    cwd, clean = asyncio.run(_with_server(cfg, 8815, calls))
    assert "scratchy" in clean["cleaned"]
    assert not pathlib.Path(cwd).exists()
