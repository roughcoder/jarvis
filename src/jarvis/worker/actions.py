"""Worker actions — what the daemon can do on its host (Phase 3c).

Each returns a text result. Pure subprocess plumbing; no brain imports, no
aiohttp — so the actions are unit-testable on their own.
"""

from __future__ import annotations

import asyncio
import pathlib
import shutil
import time
import uuid


async def run_shell(cmd: str, cwd: str | None, timeout_s: float) -> str:
    """Run a command through the shell, capturing stdout+stderr (timeout-bounded).
    Never raises — a bad cwd / spawn error comes back as an 'error:' string."""
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            cwd=cwd or None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except OSError as exc:
        return f"error: {exc}"
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout_s)
    except (asyncio.TimeoutError, TimeoutError):
        proc.kill()
        return f"error: command timed out after {timeout_s:.0f}s"
    return out.decode("utf-8", "replace").strip() or "(no output)"


async def run_exec(argv: list[str], cwd: str | None, timeout_s: float) -> str:
    """Run a binary with explicit args (no shell) — for coding agents/built-ins.
    Never raises — a missing binary / bad cwd comes back as an 'error:' string."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=cwd or None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except OSError as exc:
        return f"error: {exc}"
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout_s)
    except (asyncio.TimeoutError, TimeoutError):
        proc.kill()
        return f"error: timed out after {timeout_s:.0f}s"
    return out.decode("utf-8", "replace").strip() or "(no output)"


async def run_applescript(script: str, timeout_s: float) -> str:
    return await run_exec(["osascript", "-e", script], None, timeout_s)


async def take_screenshot(workspace: str, name: str | None, timeout_s: float) -> str:
    ws = pathlib.Path(workspace)
    ws.mkdir(parents=True, exist_ok=True)
    path = ws / (name or f"screen-{int(time.time())}.png")
    out = await run_exec(["screencapture", "-x", str(path)], None, timeout_s)
    if out.startswith("error"):
        return out
    return f"screenshot saved to {path}"


def code_argv(agent: str, codex_bin: str, claude_bin: str, prompt: str) -> list[str]:
    """The headless coding-agent command for `agent`. Both run non-interactively
    in the job's repo cwd; tune flags per your setup via the *_bin config."""
    if agent == "claude":
        return [claude_bin, "-p", prompt]
    return [codex_bin, "exec", prompt]  # codex default


def list_repos(repo_root: str) -> list[str]:
    """Git repos directly under the configured repo root (the names a job may use)."""
    if not repo_root:
        return []
    root = pathlib.Path(repo_root).expanduser()
    if not root.is_dir():
        return []
    return sorted(d.name for d in root.iterdir() if (d / ".git").exists())


def resolve_repo(repo: str, repo_root: str) -> str | None:
    """Turn a repo reference into an absolute path: an existing absolute path as
    given, or a bare name resolved under the repo root. None if not found."""
    p = pathlib.Path(repo).expanduser()
    if p.is_absolute() and p.is_dir():
        return str(p)
    if repo_root:
        candidate = pathlib.Path(repo_root).expanduser() / pathlib.Path(repo).name
        if candidate.is_dir():
            return str(candidate)
    return None


async def clone_repo(name: str, repo_root: str, timeout_s: float) -> tuple[str | None, str | None]:
    """Clone a missing repo into repo_root with `gh repo clone` (auth handled by
    gh). `name` may be a bare name (your namespace) or "org/name". Returns
    (path, None) on success or (None, error)."""
    dest = pathlib.Path(repo_root).expanduser() / pathlib.Path(name).name
    if (dest / ".git").exists():
        return str(dest), None
    out = await run_exec(["gh", "repo", "clone", name, str(dest)], None, timeout_s)
    if (dest / ".git").exists():
        return str(dest), None
    return None, f"couldn't clone {name!r}: {out[:200]}"


async def prepare_worktree(
    repo: str, worktrees_dir: str, slug: str, branch_prefix: str, timeout_s: float
) -> tuple[str | None, str | None, str | None]:
    """Isolate a repo job. For a git repo, create a fresh worktree on a new branch
    off HEAD and return (worktree_path, branch, None) — the job edits there, never
    the user's checkout. For a non-git directory, return (repo, None, None) (run in
    place; there's no working tree to protect). On failure to isolate a real repo,
    return (None, None, error) so the caller refuses rather than touching HEAD."""
    inside = await run_exec(["git", "-C", repo, "rev-parse", "--is-inside-work-tree"], None, timeout_s)
    if inside.strip() != "true":
        return repo, None, None  # not a git repo — run in the dir as given
    suffix = uuid.uuid4().hex[:6]
    branch = f"{branch_prefix}/{slug}-{suffix}"
    worktree = str(pathlib.Path(worktrees_dir) / f"{slug}-{suffix}")
    pathlib.Path(worktrees_dir).mkdir(parents=True, exist_ok=True)
    out = await run_exec(["git", "-C", repo, "worktree", "add", "-b", branch, worktree], None, timeout_s)
    if not pathlib.Path(worktree).exists():
        return None, None, f"error: could not create worktree ({out})"
    return worktree, branch, None


async def cleanup_job(repo: str, cwd: str, branch: str | None, timeout_s: float) -> str:
    """Tidy a finished job's working area: remove the worktree + delete the branch
    for a repo job, or delete the scratch dir for a no-repo job."""
    if repo and branch and cwd:
        await run_exec(["git", "-C", repo, "worktree", "remove", "--force", cwd], None, timeout_s)
        await run_exec(["git", "-C", repo, "branch", "-D", branch], None, timeout_s)
        return f"removed worktree + branch {branch}"
    if cwd:
        shutil.rmtree(cwd, ignore_errors=True)
        return f"removed {cwd}"
    return "nothing to remove"
