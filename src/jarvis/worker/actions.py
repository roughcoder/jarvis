"""Worker actions — what the daemon can do on its host (Phase 3c).

Each returns a text result. Pure subprocess plumbing; no brain imports, no
aiohttp — so the actions are unit-testable on their own.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import shutil
import subprocess
import time
import uuid
from collections.abc import Iterable
from typing import Any

from jarvis.engines import ENGINE_CLAUDE, ENGINE_CODEX, code_engine_argv, normalize_engine_id
from jarvis.worker_session_contract import ACTIVE_SESSION_STATUSES


# A model-driven shell runs with ONLY these operational vars from the host env — never
# the full process environment. Secrets are added explicitly via the WORKER_SHELL_SECRETS
# allowlist (the `env` arg), so a command can't print a secret that wasn't allowlisted.
_SAFE_ENV_KEYS = (
    "PATH", "HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "LC_CTYPE", "TERM", "TMPDIR", "SHELL", "TZ",
)
_DIAGNOSTICS_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_REPO_ACCESS_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


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
    a missing binary / bad cwd / non-zero exit comes back as an 'error:' string."""
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
    text = out.decode("utf-8", "replace").strip()
    if proc.returncode:
        suffix = f"\n{text}" if text else ""
        return f"error: command exited with {proc.returncode}{suffix}"
    return text or "(no output)"


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


def code_argv(
    agent: str,
    codex_bin: str,
    claude_bin: str,
    prompt: str,
    *,
    session_id: str = "",
    session_name: str = "",
    resume_session: bool = False,
) -> list[str]:
    """The headless coding-agent command for `agent`. Both run non-interactively
    in the job's repo cwd; tune flags per your setup via the *_bin config."""
    return code_engine_argv(
        agent,
        codex_bin,
        claude_bin,
        prompt,
        session_id=session_id,
        session_name=session_name,
        resume_session=resume_session,
    )


def list_repos(repo_root: str) -> list[str]:
    """Git repos directly under the configured repo root (the names a job may use)."""
    if not repo_root:
        return []
    root = pathlib.Path(repo_root).expanduser()
    if not root.is_dir():
        return []
    return sorted(d.name for d in root.iterdir() if (d / ".git").exists())


def repo_inventory(repo_root: str, *, ttl_s: float = 0.0) -> list[dict[str, str]]:
    """Public-safe repo rows for the health contract: bare name, default branch,
    and readiness. Git subprocesses are timeout-bounded and optionally cached so
    health can report real checkout state without becoming fragile."""
    cache_key = f"repos:{pathlib.Path(repo_root).expanduser() if repo_root else ''}"
    now = time.monotonic()
    cached = _DIAGNOSTICS_CACHE.get(cache_key)
    if ttl_s > 0 and cached is not None and cached[0] > now:
        return list(cached[1].get("repositories", []))
    rows: list[dict[str, str]] = []
    for name in list_repos(repo_root):
        repo_path = pathlib.Path(repo_root).expanduser() / name
        rows.append(_repo_check(name, repo_path))
    if ttl_s > 0:
        _DIAGNOSTICS_CACHE[cache_key] = (now + ttl_s, {"repositories": list(rows)})
    return rows


def _repo_check(name: str, repo_path: pathlib.Path) -> dict[str, str]:
    git_dir = repo_path / ".git"
    default_branch = _default_branch(git_dir)
    if not git_dir.exists():
        return {
            "repo": name,
            "default_branch": default_branch,
            "status": "broken",
            "detail": "missing .git directory",
        }
    status = _run_quick(["git", "-C", str(repo_path), "status", "--porcelain"], timeout_s=3.0)
    if status.returncode != 0:
        return {
            "repo": name,
            "default_branch": default_branch,
            "status": "broken",
            "detail": _short_detail(status.output or "git status failed"),
        }
    branch = _run_quick(["git", "-C", str(repo_path), "rev-parse", "--abbrev-ref", "HEAD"], timeout_s=3.0)
    if not default_branch:
        default_branch = branch.output.strip() if branch.returncode == 0 and branch.output.strip() != "HEAD" else ""
    if not default_branch:
        return {
            "repo": name,
            "default_branch": "",
            "status": "broken",
            "detail": "default branch not resolvable",
        }
    return {"repo": name, "default_branch": default_branch, "status": "ready"}


def _default_branch(git_dir: pathlib.Path) -> str:
    # origin/HEAD is the clone-time default; fall back to the checked-out branch.
    for ref in (git_dir / "refs" / "remotes" / "origin" / "HEAD", git_dir / "HEAD"):
        try:
            text = ref.read_text().strip()
        except OSError:
            continue
        if text.startswith("ref:"):
            return text.rsplit("/", 1)[-1]
    return ""


class _QuickResult:
    def __init__(self, returncode: int, output: str) -> None:
        self.returncode = returncode
        self.output = output


def _run_quick(argv: list[str], *, timeout_s: float = 3.0) -> _QuickResult:
    try:
        result = subprocess.run(
            argv,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_s,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return _QuickResult(1, str(exc))
    return _QuickResult(result.returncode, (result.stdout + result.stderr).strip())


def _short_detail(value: str) -> str:
    text = str(value or "").strip().replace("\n", " ")
    if not text:
        return ""
    return text[:200]


def diagnostics(
    *,
    repo_root: str,
    engines: Iterable[str],
    codex_bin: str,
    claude_bin: str,
    browser_cfg: Any,
    ttl_s: float,
    probe_timeout_s: float = 8.0,
) -> dict[str, Any]:
    """Cheap worker readiness diagnostics for health/doctor surfaces.

    The result is cached because health is a probe endpoint. All subprocesses are
    timeout-bounded and local-only; failures become status rows, never exceptions
    for callers.
    """
    cache_key = "|".join(
        [
            "diagnostics",
            str(pathlib.Path(repo_root).expanduser() if repo_root else ""),
            ",".join(normalize_engine_id(e) for e in engines),
            codex_bin,
            claude_bin,
            str(getattr(browser_cfg, "enabled", "")),
            str(getattr(browser_cfg, "chrome_path", "")),
        ]
    )
    now = time.monotonic()
    cached = _DIAGNOSTICS_CACHE.get(cache_key)
    if ttl_s > 0 and cached is not None and cached[0] > now:
        return dict(cached[1])
    try:
        rows = {
            "engines": [
                _engine_diagnostic(engine, codex_bin=codex_bin, claude_bin=claude_bin)
                for engine in engines
            ],
            "git_identity": git_identity(ttl_s=ttl_s, timeout_s=probe_timeout_s),
            "repositories": repo_inventory(repo_root, ttl_s=ttl_s),
            "package_managers": _package_managers(),
            "browser": _browser_diagnostic(browser_cfg),
            "checked_at": int(time.time()),
            "ttl_s": ttl_s,
        }
    except Exception as exc:  # noqa: BLE001 - diagnostics must degrade, not break health
        rows = {"error": _short_detail(str(exc) or exc.__class__.__name__)}
    if ttl_s > 0:
        _DIAGNOSTICS_CACHE[cache_key] = (now + ttl_s, dict(rows))
    return rows


def git_identity(*, ttl_s: float = 0.0, timeout_s: float = 8.0) -> dict[str, Any]:
    """Public-safe view of the worker's GitHub identity.

    GitHub credentials live on the worker device. The brain only receives the
    account label and freshness signal needed to decide dispatch eligibility.
    """
    cache_key = "git_identity"
    now = time.monotonic()
    cached = _DIAGNOSTICS_CACHE.get(cache_key)
    if ttl_s > 0 and cached is not None and cached[0] > now:
        return dict(cached[1])
    row: dict[str, Any] = {
        "provider": "github",
        "connected": False,
        "authenticated": False,
        "auth_fresh": False,
        "login": "",
        "git_user_name": _git_config("user.name"),
        "git_user_email": _git_config("user.email"),
        "checked_at": int(time.time()),
        "detail": "",
    }
    if not shutil.which("gh"):
        row["detail"] = "gh binary not found"
    else:
        # These are network calls to GitHub (same probe class as probe_repo_access),
        # so they honor the caller-configured timeout (WORKER_REPO_ACCESS_PROBE_TIMEOUT_S
        # / cfg.repo_access_probe_timeout_s) instead of a hardcoded value.
        user = _run_quick(["gh", "api", "user", "--jq", ".login"], timeout_s=timeout_s)
        if user.returncode == 0 and user.output.strip():
            row.update(
                {
                    "connected": True,
                    "authenticated": True,
                    "auth_fresh": True,
                    "login": _short_detail(user.output.strip()),
                    "detail": "gh user probe succeeded",
                }
            )
        else:
            status = _run_quick(["gh", "auth", "status", "-h", "github.com"], timeout_s=timeout_s)
            detail = _short_detail(status.output or user.output)
            row["detail"] = detail or "gh authentication not connected"
            if status.returncode == 0:
                row["connected"] = True
                row["authenticated"] = None
                row["auth_fresh"] = None
    if ttl_s > 0:
        _DIAGNOSTICS_CACHE[cache_key] = (now + ttl_s, dict(row))
    return row


def _git_config(key: str) -> str:
    # Identity display is a health/readiness quick check, not the repo access
    # decision itself; keep it bounded independently of the slower access probe.
    result = _run_quick(["git", "config", "--global", "--get", key], timeout_s=3.0)
    return _short_detail(result.output) if result.returncode == 0 else ""


def probe_repo_access(repo: str, *, timeout_s: float, ttl_s: float) -> dict[str, Any]:
    """Check whether this worker identity can read a GitHub repo.

    The probe is intentionally equivalent to "could dispatch materialize this?"
    and is cached per worker process. It reports public access as accessible even
    without a connected GitHub identity.
    """
    repo_ref = _normalize_github_repo(repo)
    now = time.monotonic()
    cache_key = f"repo_access:{repo_ref or repo}"
    cached = _REPO_ACCESS_CACHE.get(cache_key)
    if ttl_s > 0 and cached is not None and cached[0] > now:
        row = dict(cached[1])
        row["cached"] = True
        return row
    identity = git_identity(ttl_s=ttl_s)
    row: dict[str, Any] = {
        "repo": repo_ref or str(repo or ""),
        "accessible": False,
        "public": False,
        "reason_code": "",
        "reason": "",
        "checked_at": int(time.time()),
        "ttl_s": ttl_s,
        "cached": False,
        "git_identity": {
            "provider": identity.get("provider") or "github",
            "connected": bool(identity.get("connected")),
            "login": str(identity.get("login") or ""),
            "auth_fresh": identity.get("auth_fresh"),
        },
    }
    if not repo_ref:
        row.update(
            {
                "reason_code": "repo-reference-unsupported",
                "reason": "repo access probe only supports GitHub owner/name refs",
            }
        )
        return row
    gh_row = _probe_repo_with_gh(repo_ref, timeout_s=timeout_s)
    if gh_row is not None:
        row.update(gh_row)
    else:
        row.update(_probe_repo_with_git(repo_ref, timeout_s=timeout_s))
    if not row["accessible"] and not identity.get("connected"):
        row["reason_code"] = "worker-not-connected-to-github"
        row["reason"] = "Connect GitHub on this worker."
    elif not row["accessible"] and not row.get("reason_code"):
        row["reason_code"] = "identity-lacks-repo-access"
        row["reason"] = f"Request access to {repo_ref} for this worker identity."
    if ttl_s > 0:
        _REPO_ACCESS_CACHE[cache_key] = (now + ttl_s, dict(row))
    return row


def _normalize_github_repo(repo: str) -> str:
    text = str(repo or "").strip()
    if text.startswith("git@github.com:"):
        text = text.removeprefix("git@github.com:")
    elif text.startswith("https://github.com/"):
        text = text.removeprefix("https://github.com/")
    text = text.removesuffix(".git").strip("/")
    parts = [part for part in text.split("/") if part]
    if len(parts) == 2 and all(part.replace("-", "").replace("_", "").replace(".", "").isalnum() for part in parts):
        return "/".join(parts)
    return ""


def _probe_repo_with_gh(repo: str, *, timeout_s: float) -> dict[str, Any] | None:
    if not shutil.which("gh"):
        return None
    result = _run_quick(
        ["gh", "repo", "view", repo, "--json", "nameWithOwner,visibility", "--jq", ".nameWithOwner + \" \" + .visibility"],
        timeout_s=timeout_s,
    )
    if result.returncode != 0:
        return None
    parts = result.output.strip().split()
    visibility = parts[1].lower() if len(parts) > 1 else ""
    return {
        "repo": parts[0] if parts else repo,
        "accessible": True,
        "public": visibility == "public",
        "reason_code": "accessible",
        "reason": "Worker GitHub identity can read this repo.",
    }


def _probe_repo_with_git(repo: str, *, timeout_s: float) -> dict[str, Any]:
    remote = f"https://github.com/{repo}.git"
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    try:
        result = subprocess.run(
            ["git", "ls-remote", "--exit-code", remote],
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_s,
            env=env,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "accessible": False,
            "reason_code": "repo-access-probe-failed",
            "reason": _short_detail(str(exc) or exc.__class__.__name__),
        }
    if result.returncode == 0:
        return {
            "accessible": True,
            "public": False,
            "reason_code": "accessible",
            "reason": "Git can read this repo with the worker's configured credentials.",
        }
    return {
        "accessible": False,
        "reason_code": "identity-lacks-repo-access",
        "reason": _short_detail((result.stderr or result.stdout) or "git ls-remote failed"),
    }


def worktree_inventory(worktrees_dir: str, sessions_dir: str = "", *, stale_ttl_s: float = 0.0) -> dict[str, Any]:
    root = pathlib.Path(worktrees_dir).expanduser().resolve(strict=False)
    live = _live_session_cwds(sessions_dir, root)
    count = 0
    disk_bytes = 0
    stale_count = 0
    for path in _worktree_children(root):
        count += 1
        disk_bytes += _disk_usage(path)
        if _stale_worktree(path, root, live, stale_ttl_s):
            stale_count += 1
    return {"count": count, "disk_bytes": disk_bytes, "stale_count": stale_count}


async def prune_worktrees(
    worktrees_dir: str,
    sessions_dir: str = "",
    *,
    target: str = "",
    stale_ttl_s: float = 0.0,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    root = pathlib.Path(worktrees_dir).expanduser().resolve(strict=False)
    # This is `async` (delete_session/the prune endpoint `await` it directly), but the
    # session-json reads and disk walks below are synchronous filesystem work — run
    # them via asyncio.to_thread so a large worktree doesn't block the whole daemon's
    # event loop (and every other in-flight request) for seconds.
    live = await asyncio.to_thread(_live_session_cwds, sessions_dir, root)
    if target:
        candidate, reason = _worktree_target(root, target)
        candidates = [(candidate, target, reason)]
    else:
        children = await asyncio.to_thread(_worktree_children, root)
        candidates = [
            (path, path.name, "")
            for path in children
            if _stale_worktree(path, root, live, stale_ttl_s)
        ]
    pruned = []
    refused = []
    reclaimed = 0
    for path, label, reason in candidates:
        if path is None:
            refused.append({"target": label, "reason": reason or "worktree not found"})
            continue
        if not _is_under(path, [root]):
            refused.append({"target": str(path), "reason": "outside worktree root"})
            continue
        if _overlaps_live_session(path, live):
            refused.append({"target": path.name, "reason": "live session uses this worktree"})
            continue
        bytes_before = await asyncio.to_thread(_disk_usage, path)
        await _remove_worktree_path(path, timeout_s)
        if path.exists():
            refused.append({"target": path.name, "reason": "remove failed"})
            continue
        reclaimed += bytes_before
        pruned.append({"name": path.name, "bytes": bytes_before})
    return {
        "ok": not refused,
        "worktrees": len(pruned),
        "bytes": reclaimed,
        "pruned": pruned,
        "refused": refused,
    }


def _engine_diagnostic(engine: str, *, codex_bin: str, claude_bin: str) -> dict[str, Any]:
    engine_id = normalize_engine_id(engine)
    binary = codex_bin if engine_id == ENGINE_CODEX else claude_bin if engine_id == ENGINE_CLAUDE else engine_id
    installed = bool(shutil.which(binary))
    row: dict[str, Any] = {
        "engine": engine_id,
        "installed": installed,
        "authenticated": None,
        "version": "",
        "detail": "",
    }
    if not installed:
        row["authenticated"] = False
        row["detail"] = "binary not found on PATH"
        return row
    row["version"] = _version(binary)
    if engine_id == ENGINE_CODEX:
        row.update(_codex_auth(binary))
    elif engine_id == ENGINE_CLAUDE:
        row.update(_claude_auth(binary))
    else:
        row["detail"] = "auth check not defined for this engine"
    return row


def _version(binary: str) -> str:
    result = _run_quick([binary, "--version"], timeout_s=3.0)
    if result.returncode != 0:
        return ""
    return _short_detail(result.output.splitlines()[0] if result.output else "")


def _codex_auth(binary: str) -> dict[str, Any]:
    status = _run_quick([binary, "login", "status"], timeout_s=5.0)
    if status.returncode == 0:
        return {"authenticated": True, "detail": "codex login status succeeded"}
    if _codex_auth_file_present():
        return {"authenticated": None, "detail": "codex login status failed but auth file present"}
    detail = _short_detail(status.output) or "codex auth state not found"
    return {"authenticated": False, "detail": detail}


def _codex_auth_file_present() -> bool:
    home = pathlib.Path.home()
    return any(
        path.exists()
        for path in (
            home / ".codex" / "auth.json",
            home / ".codex" / "credentials.json",
        )
    )


def _claude_auth(binary: str = "claude") -> dict[str, Any]:
    probe = _claude_sdk_auth_probe(binary)
    if probe is not None:
        return probe
    return _claude_auth_file_check()


def _claude_sdk_auth_probe(binary: str) -> dict[str, Any] | None:
    try:
        from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient  # type: ignore[import-not-found]
    except ModuleNotFoundError as exc:
        if exc.name == "claude_agent_sdk":
            return None
        raise

    async def never_yielding_prompt():  # noqa: ANN202 - async generator shape is required by SDK
        if False:
            yield {}

    async def probe() -> dict[str, Any]:
        client = ClaudeSDKClient(
            options=ClaudeAgentOptions(
                cli_path=binary or None,
                stderr=lambda _text: None,
            )
        )
        try:
            await client.connect(never_yielding_prompt())
            info = await client.get_server_info()
            detail = _claude_account_detail(info) or "claude-agent-sdk initialized without consuming a prompt"
            return {"authenticated": True, "detail": detail}
        finally:
            await client.disconnect()

    try:
        return asyncio.run(asyncio.wait_for(probe(), timeout=8.0))
    except Exception as exc:  # noqa: BLE001 - doctor falls back to cheap local state checks
        fallback = _claude_auth_file_check()
        return {
            **fallback,
            "detail": f"{fallback['detail']}; SDK auth probe failed: {_short_detail(str(exc) or exc.__class__.__name__)}",
        }


def _claude_auth_file_check() -> dict[str, Any]:
    if os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY"):
        return {"authenticated": True, "detail": "credential environment variable present"}
    home = pathlib.Path.home()
    if (home / ".claude" / ".credentials.json").exists():
        return {"authenticated": True, "detail": "claude credential state present"}
    if (home / ".claude.json").exists():
        return {"authenticated": None, "detail": "claude state present; credential status not cheaply determinable"}
    return {"authenticated": False, "detail": "claude credential state not found"}


def _claude_account_detail(info: Any) -> str:
    if not isinstance(info, dict):
        return ""
    values: list[str] = []
    for key in ("email", "account_email", "subscription", "organization_name", "account_name"):
        value = _find_nested_value(info, key)
        if value:
            values.append(f"{key.replace('_', ' ')}: {value}")
    return "claude-agent-sdk initialized" + (f" ({', '.join(values)})" if values else "")


def _find_nested_value(value: Any, key: str) -> str:
    if isinstance(value, dict):
        for item_key, item_value in value.items():
            if str(item_key) == key and item_value:
                return _short_detail(str(item_value))
            nested = _find_nested_value(item_value, key)
            if nested:
                return nested
    elif isinstance(value, list):
        for item in value:
            nested = _find_nested_value(item, key)
            if nested:
                return nested
    return ""


def _package_managers() -> list[dict[str, Any]]:
    rows = []
    for name in ("npm", "pnpm", "yarn", "uv", "pip"):
        rows.append({"name": name, "available": bool(shutil.which(name))})
    return rows


def _browser_diagnostic(browser_cfg: Any) -> dict[str, Any]:
    if not getattr(browser_cfg, "enabled", False):
        return {"available": False, "detail": "browser lane disabled"}
    try:
        from jarvis.browser import browser_doctor

        raw = browser_doctor(browser_cfg)
    except Exception as exc:  # noqa: BLE001 - browser diagnostics are best-effort
        return {"available": False, "detail": _short_detail(str(exc) or exc.__class__.__name__)}
    details = []
    if not raw.get("nodriver_installed"):
        details.append("nodriver not installed")
    chrome_path = str(raw.get("chrome_path") or "")
    if not chrome_path or chrome_path == "(not found)":
        details.append("chrome binary not found")
    return {
        "available": bool(raw.get("ready")),
        "nodriver_installed": bool(raw.get("nodriver_installed")),
        "chrome_found": bool(chrome_path and chrome_path != "(not found)"),
        "headless": bool(raw.get("headless")),
        "default_context": str(raw.get("default_context") or ""),
        "detail": "; ".join(details) or "ready",
    }


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


async def fetch_repo(repo: str, timeout_s: float) -> str | None:
    """Best-effort fetch for an existing repo; returns an operator-visible error.

    This intentionally runs against the current checkout until the worker grows
    a bare-mirror cache. Callers must treat failures as warnings when a usable
    local checkout already exists.
    """
    remote = await run_exec(["git", "-C", repo, "remote", "get-url", "origin"], None, min(timeout_s, 10.0))
    if remote.startswith("error"):
        return None
    out = await run_exec(["git", "-C", repo, "fetch", "--prune", "origin"], None, timeout_s)
    if out.startswith("error"):
        return f"couldn't fetch {repo!r}: {out[:500]}"
    return None


_REPO_MUTATION_LOCKS: dict[str, asyncio.Lock] = {}


def repo_mutation_lock(repo: pathlib.Path | str) -> asyncio.Lock:
    """Serialize ref/worktree mutations per source repo.

    Concurrent `git worktree add` calls against the same checkout race on the
    refs directory ("cannot lock ref ... unable to create directory"), so any
    branch-resolve + worktree-add pair must hold this lock.
    """
    key = str(pathlib.Path(repo).expanduser().resolve())
    lock = _REPO_MUTATION_LOCKS.get(key)
    if lock is None:
        lock = _REPO_MUTATION_LOCKS.setdefault(key, asyncio.Lock())
    return lock


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
    async with repo_mutation_lock(repo_path):
        branch = await resolve_available_worktree_branch(repo_path, f"{branch_prefix}/{slug}-{suffix}", timeout_s)
        worktree = worktrees_root / f"{slug}-{suffix}"
        out = await run_exec(["git", "-C", str(repo_path), "worktree", "add", "-b", branch, str(worktree)], None, timeout_s)
    if not worktree.exists():
        return None, None, f"error: could not create worktree ({out})"
    return str(worktree), branch, None


async def resolve_available_worktree_branch(repo: pathlib.Path | str, desired_branch: str, timeout_s: float) -> str:
    """Return a local branch name Git can create for a worktree."""
    repo_path = pathlib.Path(repo).expanduser().resolve()
    if not await _branch_ref_conflicts(repo_path, desired_branch, timeout_s):
        return desired_branch
    flattened = desired_branch.replace("/", "-")
    for suffix in range(0, 101):
        candidate = flattened if suffix == 0 else f"{flattened}-{suffix}"
        if not await _branch_ref_conflicts(repo_path, candidate, timeout_s):
            return candidate
    raise RuntimeError(f"could not find an available worktree branch for {desired_branch!r}")


async def _branch_ref_conflicts(repo: pathlib.Path, branch: str, timeout_s: float) -> bool:
    exact = await run_exec(["git", "-C", str(repo), "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"], None, timeout_s)
    if not exact.startswith("error:"):
        return True
    parts = branch.split("/")
    for index in range(1, len(parts)):
        prefix = "/".join(parts[:index])
        out = await run_exec(["git", "-C", str(repo), "show-ref", "--verify", "--quiet", f"refs/heads/{prefix}"], None, timeout_s)
        if not out.startswith("error:"):
            return True
    descendants = await run_exec(
        ["git", "-C", str(repo), "for-each-ref", "--format=%(refname:short)", f"refs/heads/{branch}/"],
        None,
        timeout_s,
    )
    return bool(descendants and descendants != "(no output)" and not descendants.startswith("error:"))


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
    roots = [pathlib.Path(p) for p in (owned_roots or []) if p]
    if repo and branch and cwd:
        if roots and not _is_under(pathlib.Path(cwd), roots):
            return f"refused to remove non-worker-owned path {cwd}"
        await run_exec(["git", "-C", repo, "worktree", "remove", "--force", cwd], None, timeout_s)
        await run_exec(["git", "-C", repo, "branch", "-D", branch], None, timeout_s)
        return f"removed worktree + branch {branch}"
    if cwd:
        if roots and not _is_under(pathlib.Path(cwd), roots):
            return f"refused to remove non-worker-owned path {cwd}"
        shutil.rmtree(cwd, ignore_errors=True)
        return f"removed {cwd}"
    return "nothing to remove"


def _worktree_children(root: pathlib.Path) -> list[pathlib.Path]:
    if not root.is_dir():
        return []
    return sorted(path for path in root.iterdir() if path.is_dir())


def _worktree_target(root: pathlib.Path, target: str) -> tuple[pathlib.Path | None, str]:
    text = str(target or "").strip()
    if not text:
        return None, "worktree not found"
    raw = pathlib.Path(text).expanduser()
    path = raw if raw.is_absolute() else root / raw.name
    path = path.resolve(strict=False)
    root = root.resolve(strict=False)
    if not path.is_relative_to(root):
        return None, "outside worktree root"
    if not path.is_dir():
        return None, "worktree not found"
    return path, ""


def _live_session_cwds(sessions_dir: str, worktrees_root: pathlib.Path) -> set[str]:
    root = pathlib.Path(sessions_dir).expanduser() if sessions_dir else pathlib.Path()
    if not root.is_dir():
        return set()
    live: set[str] = set()
    worktrees_root = worktrees_root.resolve(strict=False)
    for path in root.glob("*/session.json"):
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        if str(data.get("status") or "") not in ACTIVE_SESSION_STATUSES:
            continue
        cwd = pathlib.Path(str(data.get("cwd") or "")).expanduser().resolve(strict=False)
        if cwd.is_relative_to(worktrees_root):
            live.add(str(cwd))
    return live


def _stale_worktree(
    path: pathlib.Path,
    worktrees_root: pathlib.Path,
    live_cwds: set[str],
    stale_ttl_s: float,
) -> bool:
    resolved = path.resolve(strict=False)
    if not resolved.is_relative_to(worktrees_root.resolve(strict=False)):
        return False
    if _overlaps_live_session(resolved, live_cwds):
        return False
    ttl = max(0.0, float(stale_ttl_s or 0.0))
    if ttl <= 0:
        return True
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return False
    return time.time() - mtime >= ttl


def _overlaps_live_session(path: pathlib.Path, live_cwds: set[str]) -> bool:
    resolved = path.resolve(strict=False)
    for live_text in live_cwds:
        live = pathlib.Path(live_text).resolve(strict=False)
        if resolved == live or resolved.is_relative_to(live) or live.is_relative_to(resolved):
            return True
    return False


def _disk_usage(path: pathlib.Path) -> int:
    total = 0
    try:
        total += path.lstat().st_size
        for dirpath, dirnames, filenames in os.walk(path, followlinks=False):
            base = pathlib.Path(dirpath)
            kept_dirs = []
            for dirname in dirnames:
                item = base / dirname
                try:
                    total += item.lstat().st_size
                except OSError:
                    continue
                if not item.is_symlink():
                    kept_dirs.append(dirname)
            dirnames[:] = kept_dirs
            for filename in filenames:
                item = base / filename
                try:
                    total += item.lstat().st_size
                except OSError:
                    continue
    except OSError:
        return 0
    return total


async def _worktree_branch(path: pathlib.Path, timeout_s: float) -> tuple[str, str]:
    branch = await run_exec(["git", "-C", str(path), "rev-parse", "--abbrev-ref", "HEAD"], None, timeout_s)
    common_dir = await run_exec(["git", "-C", str(path), "rev-parse", "--git-common-dir"], None, timeout_s)
    if branch.startswith("error:") or common_dir.startswith("error:"):
        return "", ""
    branch_name = branch.strip()
    common_path = common_dir.strip()
    if not common_path:
        return branch_name, ""
    git_dir = pathlib.Path(common_path)
    if not git_dir.is_absolute():
        git_dir = (path / git_dir).resolve(strict=False)
    return branch_name, str(git_dir)


async def _delete_worktree_branch(git_dir: str, branch: str, timeout_s: float) -> None:
    if not git_dir or not branch.startswith("jarvis/"):
        return
    await run_exec(["git", f"--git-dir={git_dir}", "branch", "-D", branch], None, timeout_s)


async def _remove_worktree_path(path: pathlib.Path, timeout_s: float) -> None:
    git_dir = path / ".git"
    branch = ""
    common_git_dir = ""
    if git_dir.exists() or git_dir.is_file():
        branch, common_git_dir = await _worktree_branch(path, timeout_s)
        await run_exec(["git", "-C", str(path), "worktree", "remove", "--force", str(path)], None, timeout_s)
        await _delete_worktree_branch(common_git_dir, branch, timeout_s)
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
