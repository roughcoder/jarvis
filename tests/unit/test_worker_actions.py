"""Worker actions + job manager — tested in isolation (Phase 3c).

No aiohttp, no brain, no network — just the subprocess plumbing and the
background-job lifecycle. Uses `echo`/missing binaries so it's fast and safe.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import subprocess
import sys
import threading
import time

import pytest

from jarvis.worker import actions
from jarvis.worker.actions import cleanup_job, code_argv, prepare_worktree, prune_worktrees, run_exec, run_shell, worktree_inventory
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
    assert calls["run"] == 4  # version + auth status + git identity only once
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
    monkeypatch.setattr(actions, "_claude_sdk_auth_probe", lambda _binary: None)

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


def test_worktree_inventory_counts_bytes_and_stale_while_sparing_live_sessions(tmp_path) -> None:
    worktrees = tmp_path / "worker" / "worktrees"
    sessions = tmp_path / "worker" / "sessions"
    stale = worktrees / "old"
    live = worktrees / "live"
    stale.mkdir(parents=True)
    live.mkdir(parents=True)
    (stale / "payload.txt").write_text("stale payload")
    (live / "payload.txt").write_text("live payload")
    session_dir = sessions / "sess_live"
    session_dir.mkdir(parents=True)
    (session_dir / "session.json").write_text(json.dumps({"status": "running", "cwd": str(live)}))

    inventory = worktree_inventory(str(worktrees), str(sessions), stale_ttl_s=0)
    result = asyncio.run(prune_worktrees(str(worktrees), str(sessions), stale_ttl_s=0))

    assert inventory["count"] == 2
    assert inventory["disk_bytes"] >= len("stale payload") + len("live payload")
    assert inventory["stale_count"] == 1
    assert result["worktrees"] == 1
    assert result["bytes"] >= len("stale payload")
    assert not stale.exists()
    assert live.exists()


def test_prune_spares_worktree_when_live_session_uses_subdirectory(tmp_path) -> None:
    worktrees = tmp_path / "worker" / "worktrees"
    sessions = tmp_path / "worker" / "sessions"
    worktree = worktrees / "live-parent"
    live_cwd = worktree / "subdir"
    live_cwd.mkdir(parents=True)
    (live_cwd / "payload.txt").write_text("live payload")
    session_dir = sessions / "sess_live"
    session_dir.mkdir(parents=True)
    (session_dir / "session.json").write_text(json.dumps({"status": "running", "cwd": str(live_cwd)}))

    inventory = worktree_inventory(str(worktrees), str(sessions), stale_ttl_s=0)
    result = asyncio.run(prune_worktrees(str(worktrees), str(sessions), stale_ttl_s=0))

    assert inventory["stale_count"] == 0
    assert result["worktrees"] == 0
    assert worktree.exists()


def _job_record(jobs_dir, job_id: str, cwd, status: str) -> None:  # noqa: ANN001
    jobs_dir.mkdir(parents=True, exist_ok=True)
    (jobs_dir / f"{job_id}.json").write_text(
        json.dumps({"id": job_id, "action": "code", "label": "", "cwd": str(cwd), "status": status})
    )


def test_prune_spares_worktree_of_a_running_job_with_no_session(tmp_path) -> None:
    """A repo job holds its worktree for the whole run without ever writing a
    session record — consulting sessions alone would delete it mid-run."""
    worktrees = tmp_path / "worker" / "worktrees"
    jobs = tmp_path / "worker" / "jobs"
    running = worktrees / "job-running"
    finished = worktrees / "job-finished"
    running.mkdir(parents=True)
    finished.mkdir(parents=True)
    _job_record(jobs, "job_running", running, "running")
    _job_record(jobs, "job_finished", finished, "done")

    inventory = worktree_inventory(str(worktrees), "", jobs_dir=str(jobs), stale_ttl_s=0)
    result = asyncio.run(prune_worktrees(str(worktrees), "", jobs_dir=str(jobs), stale_ttl_s=0))

    assert inventory["count"] == 2
    assert inventory["stale_count"] == 1
    assert result["worktrees"] == 1
    assert running.exists()
    assert not finished.exists()


def test_inventory_counts_orphans_and_prune_removes_them(tmp_path) -> None:
    """Worktrees whose job record is gone have no other cleanup path — they are
    what let a review worker reach 79 trees / 12 GB."""
    worktrees = tmp_path / "worker" / "worktrees"
    jobs = tmp_path / "worker" / "jobs"
    orphan = worktrees / "orphan"
    tracked = worktrees / "tracked"
    orphan.mkdir(parents=True)
    tracked.mkdir(parents=True)
    (orphan / "payload.txt").write_text("orphaned payload")
    _job_record(jobs, "job_tracked", tracked, "done")

    inventory = worktree_inventory(str(worktrees), "", jobs_dir=str(jobs), stale_ttl_s=0)
    result = asyncio.run(prune_worktrees(str(worktrees), "", jobs_dir=str(jobs), stale_ttl_s=0))

    assert inventory["orphan_count"] == 1
    assert inventory["root"] == str(worktrees.resolve())
    assert result["worktrees"] == 2
    assert not orphan.exists()


def test_stale_classification_respects_age_threshold(tmp_path) -> None:
    worktrees = tmp_path / "worker" / "worktrees"
    fresh = worktrees / "fresh"
    old = worktrees / "old"
    fresh.mkdir(parents=True)
    old.mkdir(parents=True)
    ttl_s = 72 * 60 * 60
    aged = time.time() - (ttl_s + 60)
    os.utime(old, (aged, aged))

    inventory = worktree_inventory(str(worktrees), "", stale_ttl_s=ttl_s)
    result = asyncio.run(prune_worktrees(str(worktrees), "", stale_ttl_s=ttl_s))

    assert inventory["count"] == 2
    assert inventory["stale_count"] == 1
    assert result["worktrees"] == 1
    assert fresh.exists()
    assert not old.exists()


def test_prune_runs_git_worktree_prune_against_the_source_repo(tmp_path) -> None:
    repo = tmp_path / "repo"
    worktrees = tmp_path / "worker" / "worktrees"
    worktrees.mkdir(parents=True)
    repo.mkdir()
    for argv in (
        ["git", "init", "-q", str(repo)],
        ["git", "-C", str(repo), "config", "user.email", "t@example.com"],
        ["git", "-C", str(repo), "config", "user.name", "t"],
        ["git", "-C", str(repo), "commit", "-q", "--allow-empty", "-m", "init"],
        ["git", "-C", str(repo), "worktree", "add", "-q", "-b", "jarvis/gc-probe", str(worktrees / "gc-probe")],
    ):
        subprocess.run(argv, check=True, capture_output=True)

    listed_before = subprocess.run(
        ["git", "-C", str(repo), "worktree", "list"], check=True, capture_output=True, text=True
    ).stdout
    result = asyncio.run(prune_worktrees(str(worktrees), "", stale_ttl_s=0, timeout_s=30))
    listed_after = subprocess.run(
        ["git", "-C", str(repo), "worktree", "list"], check=True, capture_output=True, text=True
    ).stdout

    assert "gc-probe" in listed_before
    assert result["worktrees"] == 1
    assert result["repos_pruned"]
    # The repo no longer advertises the worktree we deleted.
    assert "gc-probe" not in listed_after


def test_prune_refuses_target_outside_worktree_root(tmp_path) -> None:
    worktrees = tmp_path / "worker" / "worktrees"
    outside = tmp_path / "outside"
    worktrees.mkdir(parents=True)
    outside.mkdir()

    result = asyncio.run(prune_worktrees(str(worktrees), target=str(outside), stale_ttl_s=0))

    assert result["ok"] is False
    assert result["refused"][0]["reason"] == "outside worktree root"
    assert outside.exists()


def test_worktree_inventory_skips_symlinked_directory_contents(tmp_path) -> None:
    worktrees = tmp_path / "worker" / "worktrees"
    worktree = worktrees / "linked"
    outside = tmp_path / "outside"
    worktree.mkdir(parents=True)
    outside.mkdir()
    (outside / "large.bin").write_bytes(b"x" * 1024 * 1024)
    try:
        os.symlink(outside, worktree / "outside-link", target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlinks unavailable: {exc}")

    inventory = worktree_inventory(str(worktrees), stale_ttl_s=0)

    assert inventory["count"] == 1
    assert inventory["disk_bytes"] < 1024 * 1024


def test_prune_worktrees_runs_disk_walk_and_session_reads_off_event_loop(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    # prune_worktrees is `async` but used to run _live_session_cwds (reads every
    # sessions/*/session.json) and _disk_usage (os.walk) synchronously inline on the
    # event loop; delete_session and the prune endpoint `await` it directly, so a
    # large worktree blocked the whole worker daemon for seconds. Both must run via
    # asyncio.to_thread.
    worktrees = tmp_path / "worker" / "worktrees"
    sessions = tmp_path / "worker" / "sessions"
    stale = worktrees / "old"
    stale.mkdir(parents=True)
    (stale / "payload.txt").write_text("stale payload")
    session_dir = sessions / "sess_untouched"
    session_dir.mkdir(parents=True)
    (session_dir / "session.json").write_text(json.dumps({"status": "running", "cwd": str(worktrees / "other")}))

    caller_thread = threading.get_ident()
    disk_usage_threads: list[int] = []
    session_read_threads: list[int] = []
    real_disk_usage = actions._disk_usage  # noqa: SLF001
    real_live_session_cwds = actions._live_session_cwds  # noqa: SLF001

    def spy_disk_usage(path):  # noqa: ANN001
        disk_usage_threads.append(threading.get_ident())
        return real_disk_usage(path)

    def spy_live_session_cwds(sessions_dir, worktrees_root):  # noqa: ANN001
        session_read_threads.append(threading.get_ident())
        return real_live_session_cwds(sessions_dir, worktrees_root)

    monkeypatch.setattr(actions, "_disk_usage", spy_disk_usage)
    monkeypatch.setattr(actions, "_live_session_cwds", spy_live_session_cwds)

    result = asyncio.run(prune_worktrees(str(worktrees), str(sessions), stale_ttl_s=0))

    assert result["worktrees"] == 1
    assert not stale.exists()
    assert disk_usage_threads and all(t != caller_thread for t in disk_usage_threads)
    assert session_read_threads and all(t != caller_thread for t in session_read_threads)


def test_prune_deletes_associated_jarvis_branch(tmp_path) -> None:
    repo = tmp_path / "repo"
    worktrees = tmp_path / "worker" / "worktrees"
    worktree = worktrees / "branch-prune"
    repo.mkdir()
    worktrees.mkdir(parents=True)
    subprocess.run(["git", "-C", str(repo), "init"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.email", "test@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(repo), "config", "user.name", "Test User"], check=True)
    (repo / "README.md").write_text("base\n")
    subprocess.run(["git", "-C", str(repo), "add", "README.md"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-m", "initial"], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "worktree", "add", "-b", "jarvis/branch-prune", str(worktree)], check=True, capture_output=True)

    result = asyncio.run(prune_worktrees(str(worktrees), target=str(worktree), stale_ttl_s=0))
    branch = subprocess.run(["git", "-C", str(repo), "branch", "--list", "jarvis/branch-prune"], check=True, capture_output=True, text=True)

    assert result["ok"] is True
    assert result["worktrees"] == 1
    assert not worktree.exists()
    assert branch.stdout.strip() == ""


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


def test_prepare_worktree_serializes_concurrent_calls_per_repo(tmp_path) -> None:  # noqa: ANN001
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run([*git, "init", "-q"], cwd=repo, check=True)
    subprocess.run([*git, "commit", "--allow-empty", "-qm", "init"], cwd=repo, check=True)
    worktrees = tmp_path / "worktrees"

    async def run_concurrent() -> list[tuple[str | None, str | None, str | None]]:
        return await asyncio.gather(
            *(
                prepare_worktree(str(repo), str(worktrees), f"race-{index}", "jarvis", 30)
                for index in range(6)
            )
        )

    results = asyncio.run(run_concurrent())

    errors = [err for _, _, err in results if err]
    assert errors == []
    paths = [cwd for cwd, _, _ in results]
    branches = [branch for _, branch, _ in results]
    assert len(set(paths)) == 6
    assert len(set(branches)) == 6
