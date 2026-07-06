"""Worker actions + job manager — tested in isolation (Phase 3c).

No aiohttp, no brain, no network — just the subprocess plumbing and the
background-job lifecycle. Uses `echo`/missing binaries so it's fast and safe.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys

from jarvis.worker.actions import cleanup_job, code_argv, prepare_worktree, run_exec, run_shell
from jarvis.worker.jobs import JobManager


def test_run_shell_captures_output() -> None:
    assert asyncio.run(run_shell("echo hello", None, 5)) == "hello"


def test_run_shell_bad_cwd_returns_error() -> None:
    out = asyncio.run(run_shell("echo hi", "/no/such/dir/xyz", 5))
    assert out.startswith("error:")


def test_run_exec_echo() -> None:
    assert asyncio.run(run_exec(["echo", "hi there"], None, 5)) == "hi there"


def test_run_exec_missing_binary_returns_error() -> None:
    out = asyncio.run(run_exec(["definitely-not-a-real-binary-xyz"], None, 5))
    assert out.startswith("error:")


def test_run_exec_nonzero_exit_returns_error() -> None:
    out = asyncio.run(run_exec([sys.executable, "-c", "import sys; print('bad token'); sys.exit(7)"], None, 5))

    assert out.startswith("error: command exited with 7")
    assert "bad token" in out


def test_code_argv_for_each_agent() -> None:
    assert code_argv("codex", "codex", "claude", "fix bug") == ["codex", "exec", "fix bug"]
    assert code_argv("claude", "codex", "claude", "fix bug") == ["claude", "-p", "fix bug"]
    assert code_argv("codex", "codex", "claude", "fix bug", session_id="abc") == [
        "codex",
        "exec",
        "resume",
        "abc",
        "fix bug",
    ]
    assert code_argv(
        "claude",
        "codex",
        "claude",
        "fix bug",
        session_id="550e8400-e29b-41d4-a716-446655440000",
        session_name="jarvis-fix-bug",
    ) == [
        "claude",
        "--session-id",
        "550e8400-e29b-41d4-a716-446655440000",
        "--name",
        "jarvis-fix-bug",
        "-p",
        "fix bug",
    ]
    assert code_argv(
        "claude",
        "codex",
        "claude",
        "follow up",
        session_id="550e8400-e29b-41d4-a716-446655440000",
        resume_session=True,
    ) == [
        "claude",
        "-p",
        "--resume",
        "550e8400-e29b-41d4-a716-446655440000",
        "follow up",
    ]
    try:
        code_argv("whatever", "codex", "claude", "x")
    except ValueError as exc:
        assert "unsupported coding engine" in str(exc)
    else:
        raise AssertionError("unknown engines must not silently fall back to codex")


def _drain(coro) -> object:
    async def go():
        jm = JobManager()
        job = jm.start("code", "label", coro)
        for _ in range(100):
            if jm.get(job.id).status != "running":
                break
            await asyncio.sleep(0.01)
        return jm.get(job.id)

    return asyncio.run(go())


def test_job_manager_records_success() -> None:
    async def work() -> str:
        return "done output"

    job = _drain(work())
    assert job.status == "done"
    assert job.output == "done output"
    assert job.ended is not None


def test_job_manager_records_error() -> None:
    async def boom() -> str:
        raise RuntimeError("kaboom")

    job = _drain(boom())
    assert job.status == "error"
    assert "kaboom" in job.output


def test_job_manager_marks_error_output_as_error() -> None:
    async def work() -> str:
        return "error: command exited with 1\nFailed to authenticate."

    job = _drain(work())

    assert job.status == "error"
    assert "Failed to authenticate" in job.output


def test_jobs_persist_to_disk_and_reload(tmp_path) -> None:
    async def work() -> str:
        return "session id: abc-123\npong"

    async def go() -> str:
        jm = JobManager(store_dir=str(tmp_path))
        job = jm.start("code", "label", work())
        for _ in range(100):
            if jm.get(job.id).status != "running":
                break
            await asyncio.sleep(0.01)
        return job.id

    jid = asyncio.run(go())
    # a fresh manager (daemon restart) loads the job from disk
    reloaded = JobManager(store_dir=str(tmp_path)).get(jid)
    assert reloaded is not None
    assert reloaded.status == "done"
    assert reloaded.session_id == "abc-123"  # bridge to `codex resume`


def test_resolve_repo_and_list_repos(tmp_path) -> None:
    import subprocess

    from jarvis.worker.actions import list_repos, resolve_repo

    root = tmp_path / "dev"
    (root / "polymarket").mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=root / "polymarket", check=True)

    assert resolve_repo("polymarket", str(root)) == str(root / "polymarket")  # by name
    assert resolve_repo(str(root / "polymarket"), "") == str(root / "polymarket")  # abs path
    assert resolve_repo("nope", str(root)) is None  # not found
    assert list_repos(str(root)) == ["polymarket"]  # git repos under root
    assert list_repos("") == []


def test_repo_inventory_reports_name_default_branch_and_readiness(tmp_path) -> None:
    import subprocess

    from jarvis.worker.actions import repo_inventory

    root = tmp_path / "dev"
    (root / "polymarket").mkdir(parents=True)
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root / "polymarket", check=True)
    origin_head = root / "polymarket" / ".git" / "refs" / "remotes" / "origin"
    origin_head.mkdir(parents=True)
    (origin_head / "HEAD").write_text("ref: refs/remotes/origin/develop\n")
    (root / "jarvis").mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=root / "jarvis", check=True)
    (root / "not-a-repo").mkdir()

    rows = repo_inventory(str(root))

    assert rows == [
        {"repo": "jarvis", "default_branch": "main", "status": "ready"},  # falls back to .git/HEAD
        {"repo": "polymarket", "default_branch": "develop", "status": "ready"},  # origin/HEAD wins
    ]
    assert repo_inventory("") == []


def test_repo_inventory_reports_broken_checkout(tmp_path) -> None:
    from jarvis.worker.actions import repo_inventory

    root = tmp_path / "dev"
    broken = root / "broken"
    (broken / ".git").mkdir(parents=True)

    rows = repo_inventory(str(root))

    assert rows[0]["repo"] == "broken"
    assert rows[0]["status"] == "broken"
    assert rows[0]["detail"]


def test_worker_diagnostics_reports_engines_packages_browser_and_uses_ttl(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    from types import SimpleNamespace

    import jarvis.worker.actions as actions
    from jarvis.worker.actions import diagnostics

    actions._DIAGNOSTICS_CACHE.clear()  # noqa: SLF001
    calls = {"run": 0}

    def fake_which(name: str) -> str:
        return f"/bin/{name}" if name in {"codex", "uv"} else ""

    def fake_run_quick(argv, *, timeout_s=3.0):  # noqa: ANN001
        calls["run"] += 1
        if argv == ["codex", "--version"]:
            return actions._QuickResult(0, "codex 1.2.3")  # noqa: SLF001
        if argv == ["codex", "login", "status"]:
            return actions._QuickResult(0, "logged in")  # noqa: SLF001
        return actions._QuickResult(1, "missing")  # noqa: SLF001

    monkeypatch.setattr(actions.shutil, "which", fake_which)
    monkeypatch.setattr(actions, "_run_quick", fake_run_quick)
    monkeypatch.setattr(actions, "_browser_diagnostic", lambda _cfg: {"available": True, "detail": "ready"})

    first = diagnostics(
        repo_root=str(tmp_path / "repos"),
        engines=["codex"],
        codex_bin="codex",
        claude_bin="claude",
        browser_cfg=SimpleNamespace(enabled=True, chrome_path=""),
        ttl_s=60,
    )
    second = diagnostics(
        repo_root=str(tmp_path / "repos"),
        engines=["codex"],
        codex_bin="codex",
        claude_bin="claude",
        browser_cfg=SimpleNamespace(enabled=True, chrome_path=""),
        ttl_s=60,
    )

    assert first == second
    assert calls["run"] == 2  # version + auth status only once
    assert first["engines"][0]["installed"] is True
    assert first["engines"][0]["authenticated"] is True
    assert {"name": "uv", "available": True} in first["package_managers"]
    assert first["browser"]["available"] is True


def test_codex_auth_file_is_indeterminate_when_login_status_fails(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    import jarvis.worker.actions as actions

    home = tmp_path / "home"
    auth_dir = home / ".codex"
    auth_dir.mkdir(parents=True)
    (auth_dir / "auth.json").write_text("{}")
    (auth_dir / "config.toml").write_text("model = 'x'\n")
    monkeypatch.setattr(actions.pathlib.Path, "home", classmethod(lambda cls: home))
    monkeypatch.setattr(
        actions,
        "_run_quick",
        lambda *_args, **_kwargs: actions._QuickResult(1, "read ~/.codex/auth.json failed"),  # noqa: SLF001
    )

    row = actions._codex_auth("codex")  # noqa: SLF001

    assert row["authenticated"] is None
    assert row["detail"] == "codex login status failed but auth file present"


def test_codex_config_file_alone_is_not_auth_state(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    import jarvis.worker.actions as actions

    home = tmp_path / "home"
    config_dir = home / ".codex"
    config_dir.mkdir(parents=True)
    (config_dir / "config.toml").write_text("model = 'x'\n")
    monkeypatch.setattr(actions.pathlib.Path, "home", classmethod(lambda cls: home))
    monkeypatch.setattr(
        actions,
        "_run_quick",
        lambda *_args, **_kwargs: actions._QuickResult(1, "not logged in"),  # noqa: SLF001
    )

    row = actions._codex_auth("codex")  # noqa: SLF001

    assert row["authenticated"] is False
    assert row["detail"] == "not logged in"


def test_claude_state_file_is_indeterminate_without_credentials(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    import jarvis.worker.actions as actions

    home = tmp_path / "home"
    home.mkdir()
    (home / ".claude.json").write_text("{}")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_API_KEY", raising=False)
    monkeypatch.setattr(actions.pathlib.Path, "home", classmethod(lambda cls: home))

    row = actions._claude_auth()  # noqa: SLF001

    assert row["authenticated"] is None
    assert "not cheaply determinable" in row["detail"]


def test_claude_auth_prefers_sdk_probe_when_available(monkeypatch) -> None:  # noqa: ANN001
    import jarvis.worker.actions as actions

    monkeypatch.setattr(
        actions,
        "_claude_sdk_auth_probe",
        lambda binary: {"authenticated": True, "detail": f"sdk probe ok via {binary}"},
    )

    row = actions._claude_auth("fake-claude")  # noqa: SLF001

    assert row == {"authenticated": True, "detail": "sdk probe ok via fake-claude"}


def test_slugify_makes_readable_handles() -> None:
    from jarvis.worker.jobs import slugify

    assert slugify("Polymarket Refactor!") == "polymarket-refactor"
    assert slugify("") == "job"


def test_prepare_worktree_copies_non_git_inputs_to_scratch(tmp_path) -> None:
    src = tmp_path / "input"
    src.mkdir()
    (src / "note.txt").write_text("original")
    worktrees = tmp_path / "worker" / "worktrees"

    cwd, branch, err = asyncio.run(
        prepare_worktree(str(src), str(worktrees), "plain-dir", "jarvis", 5)
    )

    assert err is None
    assert branch is None
    assert cwd is not None and cwd != str(src)
    assert "/worktrees/" in cwd and cwd.endswith("-scratch")
    copied = pathlib.Path(cwd)
    assert (copied / "note.txt").read_text() == "original"
    (copied / "note.txt").write_text("changed")
    assert (src / "note.txt").read_text() == "original"


def test_prepare_worktree_returns_absolute_paths_from_relative_workspace(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run([*git, "init", "-q"], cwd=repo, check=True)
    subprocess.run([*git, "commit", "--allow-empty", "-qm", "init"], cwd=repo, check=True)
    monkeypatch.chdir(tmp_path)

    cwd, branch, err = asyncio.run(
        prepare_worktree(str(repo), "relative-worker/worktrees", "jarvis-smoke", "jarvis", 5)
    )

    assert err is None
    assert branch is not None
    assert cwd is not None and os.path.isabs(cwd)
    assert pathlib.Path(cwd).exists()


def test_cleanup_refuses_non_worker_owned_paths(tmp_path) -> None:
    user_dir = tmp_path / "user-owned"
    user_dir.mkdir()
    (user_dir / "keep.txt").write_text("keep")
    owned = tmp_path / "worker" / "runs"

    out = asyncio.run(cleanup_job("", str(user_dir), None, 5, owned_roots=[str(owned)]))

    assert out.startswith("refused")
    assert (user_dir / "keep.txt").exists()


def test_cleanup_refuses_repo_worktree_outside_owned_roots(tmp_path) -> None:
    user_dir = tmp_path / "user-worktree"
    user_dir.mkdir()
    (user_dir / "keep.txt").write_text("keep")
    owned = tmp_path / "worker" / "worktrees"

    out = asyncio.run(
        cleanup_job(str(tmp_path), str(user_dir), "jarvis/job", 5, owned_roots=[str(owned)])
    )

    assert out.startswith("refused")
    assert (user_dir / "keep.txt").exists()


def test_jobs_named_and_findable() -> None:
    async def go():
        jm = JobManager()

        async def w() -> str:
            return "ok"

        job = jm.start("code", "fix the login bug", w(), name="login fix")
        for _ in range(100):
            if jm.get(job.id).status != "running":
                break
            await asyncio.sleep(0.01)
        return jm, job

    jm, job = asyncio.run(go())
    assert job.name == "login-fix"  # user-given name, slugified
    assert jm.find("login").id == job.id  # by name
    assert jm.find("login bug").id == job.id  # by label
    assert jm.find("nonexistent") is None


def test_stale_running_job_reloads_as_interrupted(tmp_path) -> None:
    import json

    (tmp_path / "deadbeef0000.json").write_text(
        json.dumps(
            {"id": "deadbeef0000", "action": "code", "label": "x", "status": "running",
             "output": "", "started": 1.0, "ended": None}
        )
    )
    jm = JobManager(store_dir=str(tmp_path))
    assert jm.get("deadbeef0000").status == "interrupted"
