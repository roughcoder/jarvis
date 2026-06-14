"""Worker actions — what the daemon can do on its host (Phase 3c).

Each returns a text result. Pure subprocess plumbing; no brain imports, no
aiohttp — so the actions are unit-testable on their own.
"""

from __future__ import annotations

import asyncio
import pathlib
import time


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
