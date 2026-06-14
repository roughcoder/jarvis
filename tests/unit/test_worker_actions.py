"""Worker actions + job manager — tested in isolation (Phase 3c).

No aiohttp, no brain, no network — just the subprocess plumbing and the
background-job lifecycle. Uses `echo`/missing binaries so it's fast and safe.
"""

from __future__ import annotations

import asyncio

from jarvis.worker.actions import code_argv, run_exec, run_shell
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


def test_code_argv_for_each_agent() -> None:
    assert code_argv("codex", "codex", "claude", "fix bug") == ["codex", "exec", "fix bug"]
    assert code_argv("claude", "codex", "claude", "fix bug") == ["claude", "-p", "fix bug"]
    # unknown agent falls back to codex
    assert code_argv("whatever", "codex", "claude", "x")[0] == "codex"


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
