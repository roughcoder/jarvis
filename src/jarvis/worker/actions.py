"""Worker actions — what the daemon can do on its host (Phase 3c).

Each returns a text result. Pure subprocess plumbing; no brain imports, no
aiohttp — so the actions are unit-testable on their own.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import shutil
import time
import uuid

from jarvis.engines import code_engine_argv


# A model-driven shell runs with ONLY these operational vars from the host env — never
# the full process environment. Secrets are added explicitly via the WORKER_SHELL_SECRETS
# allowlist (the `env` arg), so a command can't print a secret that wasn't allowlisted.
_SAFE_ENV_KEYS = (
    "PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "LC_CTYPE", "TERM", "TMPDIR", "SHELL", "TZ",
)


def _safe_base_env() -> dict:
    return {k: os.environ[k] for k in _SAFE_ENV_KEYS if k in os.environ}


async def run_shell(cmd: str, cwd: str | None, timeout_s: float, env: dict | None = None) -> str:
    """Run a command through the shell, capturing stdout+stderr (timeout-bounded). Runs
    with a SCRUBBED baseline env (operational vars only) plus the allowlisted secrets in
    `env` — deny-by-default, so it can't leak a non-allowlisted host secret. Never raises
    — a bad cwd / spawn error comes back as an 'error:' string."""
    try:
        proc = await asyncio.create_subprocess_shell(
            cmd,
            cwd=cwd or None,
            env={**_safe_base_env(), **(env or {})},
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


async def run_exec(
    argv: list[str], cwd: str | None, timeout_s: float, env: dict | None = None
) -> str:
    """Run a binary with explicit args (no shell) — for coding agents/built-ins.
    `env` (if given) is layered ON TOP of the inherited environment. Never raises —
    a missing binary / bad cwd comes back as an 'error:' string."""
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=cwd or None,
            env={**os.environ, **env} if env else None,
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


async def run_peekaboo(
    peekaboo_bin: str, argv: list[str], timeout_s: float, env: dict | None = None
) -> str:
    """Drive GUI automation via peekaboo (Phase 3c). Returns a clear 'not installed'
    message rather than failing when the binary is absent — it needs `brew install` +
    Screen-Recording/Accessibility permissions. `env` carries peekaboo's AI-provider
    config (for the `agent` subcommand) so it can route to OpenAI or LiteLLM."""
    if not shutil.which(peekaboo_bin):
        return (
            "mac GUI control isn't set up — install peekaboo and grant Screen "
            "Recording + Accessibility permissions (see `jarvis worker --doctor`)."
        )
    return await run_exec([peekaboo_bin, *argv], None, timeout_s, env=env)


def gui_doctor(peekaboo_bin: str) -> dict:
    """Report what mac GUI control needs (the perms can't be auto-granted)."""
    present = bool(shutil.which(peekaboo_bin))
    return {
        "peekaboo_installed": present,
        "binary": peekaboo_bin,
        "next_steps": (
            "ready — verify Screen Recording + Accessibility are granted in System "
            "Settings > Privacy & Security."
            if present
            else "install peekaboo (e.g. `brew install peekaboo`), then grant Screen "
            "Recording + Accessibility permissions to the worker's terminal."
        ),
    }


async def capture_screen_jpeg_b64(timeout_s: float) -> tuple[str, str]:
    """Capture the screen as a base64 JPEG → (b64, error). JPEG (not PNG) keeps the
    payload small for the HTTP hop + the vision model. Needs Screen Recording."""
    import base64
    import tempfile

    tmp = pathlib.Path(tempfile.gettempdir()) / f"jarvis-screen-{uuid.uuid4().hex[:8]}.jpg"
    out = await run_exec(["screencapture", "-x", "-t", "jpg", str(tmp)], None, timeout_s)
    try:
        if not tmp.exists():
            return "", out or "error: screencapture produced no file"
        data = base64.b64encode(tmp.read_bytes()).decode("ascii")
        return data, ""
    finally:
        tmp.unlink(missing_ok=True)


def code_argv(agent: str, codex_bin: str, claude_bin: str, prompt: str) -> list[str]:
    """The headless coding-agent command for `agent`. Both run non-interactively
    in the job's repo cwd; tune flags per your setup via the *_bin config."""
    return code_engine_argv(agent, codex_bin, claude_bin, prompt)


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
    the user's checkout. For a non-git directory, copy it into worker-owned scratch
    and run there. On failure to isolate a real repo, return (None, None, error) so
    the caller refuses rather than touching HEAD."""
    repo_path = pathlib.Path(repo).expanduser().resolve()
    worktrees_root = pathlib.Path(worktrees_dir).expanduser().resolve()
    inside = await run_exec(["git", "-C", str(repo_path), "rev-parse", "--is-inside-work-tree"], None, timeout_s)
    suffix = uuid.uuid4().hex[:6]
    worktrees_root.mkdir(parents=True, exist_ok=True)
    if inside.strip() != "true":
        scratch = worktrees_root / f"{slug}-{suffix}-scratch"
        try:
            shutil.copytree(repo_path, scratch, ignore=shutil.ignore_patterns(".git", "__pycache__"))
        except OSError as exc:
            return None, None, f"error: could not copy non-git input into scratch ({exc})"
        return str(scratch), None, None
    branch = f"{branch_prefix}/{slug}-{suffix}"
    worktree = worktrees_root / f"{slug}-{suffix}"
    out = await run_exec(["git", "-C", str(repo_path), "worktree", "add", "-b", branch, str(worktree)], None, timeout_s)
    if not worktree.exists():
        return None, None, f"error: could not create worktree ({out})"
    return str(worktree), branch, None


def _is_under(path: pathlib.Path, roots: list[pathlib.Path]) -> bool:
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path.absolute()
    for root in roots:
        try:
            resolved.relative_to(root.resolve())
            return True
        except ValueError:
            continue
    return False


async def cleanup_job(
    repo: str,
    cwd: str,
    branch: str | None,
    timeout_s: float,
    owned_roots: list[str] | None = None,
) -> str:
    """Tidy a finished job's working area: remove the worktree + delete the branch
    for a repo job, or delete worker-owned scratch. Never delete arbitrary
    user-supplied paths."""
    if repo and branch and cwd:
        await run_exec(["git", "-C", repo, "worktree", "remove", "--force", cwd], None, timeout_s)
        await run_exec(["git", "-C", repo, "branch", "-D", branch], None, timeout_s)
        return f"removed worktree + branch {branch}"
    if cwd:
        roots = [pathlib.Path(p) for p in (owned_roots or []) if p]
        if roots and not _is_under(pathlib.Path(cwd), roots):
            return f"refused to remove non-worker-owned path {cwd}"
        shutil.rmtree(cwd, ignore_errors=True)
        return f"removed {cwd}"
    return "nothing to remove"
