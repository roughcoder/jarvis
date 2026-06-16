"""Worker tools — the brain's gated HTTP client to the worker daemon (Phase 3c).

A thin dispatch layer: it imports nothing from `jarvis.worker` (the daemon), it
just calls it over HTTP. Each tool needs its own `worker.*` capability
(deny-by-default), so a device only gets these if its profile grants them. Deep
work (`start_coding_job`) is fire-and-forget — it returns a job id immediately,
never blocking the turn.
"""

from __future__ import annotations

from typing import Any

import httpx

from jarvis.brain.context import RequestContext
from jarvis.config import WorkerConfig
from jarvis.tools.base import Tool


_CODEX_NOISE = (
    "OpenAI Codex", "workdir:", "model:", "provider:", "approval:", "sandbox:",
    "reasoning", "session id:", "--------", "tokens used",
)


def _clean_output(text: str) -> str:
    """Strip a coding agent's session boilerplate (headers, hook lines, token
    counts) so the spoken readback is the actual result, not noise."""
    keep: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s in ("user", "codex") or s.isdigit() or s.startswith("hook:"):
            continue
        if any(s.startswith(p) for p in _CODEX_NOISE):
            continue
        keep.append(s)
    cleaned = " ".join(keep).strip()
    return cleaned[-500:] if len(cleaned) > 500 else (cleaned or "(no output)")


def make_worker_tools(cfg: WorkerConfig) -> list[Tool]:
    def headers() -> dict[str, str]:
        tok = cfg.token.get_secret_value()
        return {"Authorization": f"Bearer {tok}"} if tok else {}

    async def post(action: str, args: dict[str, Any], timeout: float | None = None) -> dict[str, Any]:
        # No raise_for_status: the daemon returns helpful JSON errors on 4xx too.
        async with httpx.AsyncClient(timeout=timeout or cfg.request_timeout_s) as client:
            r = await client.post(
                f"{cfg.base_url}/run", json={"action": action, "args": args}, headers=headers()
            )
            return r.json()

    async def shell(ctx: RequestContext, args: dict[str, Any]) -> str:
        cmd = (args.get("command") or "").strip()
        if not cmd:
            return "error: empty command"
        try:
            data = await post("shell", {"command": cmd})
        except Exception as exc:  # noqa: BLE001 - worker may be down
            return f"error: worker unreachable ({exc})"
        return data.get("error") or data.get("output") or "(no output)"

    async def code(ctx: RequestContext, args: dict[str, Any]) -> str:
        task = (args.get("task") or "").strip()
        if not task:
            return "error: empty task"
        body: dict[str, Any] = {"prompt": task}
        if args.get("name"):
            body["name"] = args["name"]
        if args.get("agent"):
            body["agent"] = args["agent"]  # codex | claude (local agent choice)
        if args.get("repo"):
            body["repo"] = args["repo"]
        try:
            # generous timeout: dispatch may clone a missing repo first
            data = await post("code", body, timeout=cfg.clone_timeout_s + 10)
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        if data.get("error"):
            return data["error"]  # e.g. "couldn't find a repo called 'x'. I can see: ..."
        branch = data.get("branch")
        where = f" on an isolated branch, {branch}," if branch else ""
        return (
            f"Started the coding job {data.get('name')!r}{where} on the worker. "
            "It runs in the background — ask me to check on it by name."
        )

    async def check(ctx: RequestContext, args: dict[str, Any]) -> str:
        # By name or id; defaults to the most recent ("check the coding job").
        ref = (args.get("job") or args.get("job_id") or "").strip() or "latest"
        try:
            async with httpx.AsyncClient(timeout=cfg.request_timeout_s) as client:
                r = await client.get(f"{cfg.base_url}/jobs/{ref}", headers=headers())
            if r.status_code == 404:
                return "no coding jobs yet" if ref == "latest" else f"no job called {ref!r}"
            r.raise_for_status()
            data = r.json()
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        name = data.get("name") or data.get("label") or data.get("action")
        status = data.get("status")
        branch = data.get("branch")
        on = f" on branch {branch}" if branch else ""
        if status == "running":
            return f"the job {name!r}{on} is still running."
        return f"the job {name!r}{on} {status}. result: {_clean_output(data.get('output') or '')}"

    async def jobs_list(ctx: RequestContext, args: dict[str, Any]) -> str:
        try:
            async with httpx.AsyncClient(timeout=cfg.request_timeout_s) as client:
                r = await client.get(f"{cfg.base_url}/jobs", headers=headers())
            r.raise_for_status()
            jobs = r.json().get("jobs", [])
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        if not jobs:
            return "no coding jobs."
        running = sum(1 for j in jobs if j.get("status") == "running")
        recent = "; ".join(
            f"{j.get('name') or j.get('label')} — {j.get('status')}" for j in jobs[-5:]
        )
        return f"{running} running, {len(jobs)} total. Recent: {recent}."

    async def cleanup(ctx: RequestContext, args: dict[str, Any]) -> str:
        ref = (args.get("job") or "").strip()
        try:
            async with httpx.AsyncClient(timeout=cfg.request_timeout_s) as client:
                r = await client.post(
                    f"{cfg.base_url}/run",
                    json={"action": "cleanup", "args": {"job": ref}},
                    headers=headers(),
                )
            data = r.json()  # the daemon returns JSON even on 404
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        if data.get("error"):
            return data["error"]
        cleaned = data.get("cleaned", [])
        if not cleaned:
            return "nothing to clean up."
        return f"cleaned up {len(cleaned)} job(s): {', '.join(cleaned)}."

    async def screenshot(ctx: RequestContext, args: dict[str, Any]) -> str:
        try:
            data = await post("screenshot", {})
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        return data.get("error") or data.get("output") or "(no output)"

    async def applescript(ctx: RequestContext, args: dict[str, Any]) -> str:
        script = (args.get("script") or "").strip()
        if not script:
            return "error: empty script"
        try:
            data = await post("applescript", {"script": script})
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        return data.get("error") or data.get("output") or "(no output)"

    async def peekaboo(argv: list[str], timeout: float | None = None) -> str:
        try:
            data = await post("peekaboo", {"argv": argv}, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        return data.get("error") or data.get("output") or "(no output)"

    # Atomic GUI hands (no AI key — Jarvis orchestrates them). All gated worker.gui.
    async def see_screen(ctx: RequestContext, args: dict[str, Any]) -> str:
        # `--mode screen` captures the whole screen (bare `see` prints usage).
        return await peekaboo(["see", "--mode", "screen"])

    async def list_apps(ctx: RequestContext, args: dict[str, Any]) -> str:
        return await peekaboo(["list", "apps"])

    async def launch_app(ctx: RequestContext, args: dict[str, Any]) -> str:
        app = (args.get("app") or "").strip()
        return await peekaboo(["app", "launch", app]) if app else "error: which app?"

    async def click(ctx: RequestContext, args: dict[str, Any]) -> str:
        target = (args.get("target") or "").strip()
        return await peekaboo(["click", target]) if target else "error: what should I click?"

    async def type_text(ctx: RequestContext, args: dict[str, Any]) -> str:
        text = args.get("text") or ""
        return await peekaboo(["type", text]) if text else "error: nothing to type"

    async def press_keys(ctx: RequestContext, args: dict[str, Any]) -> str:
        keys = (args.get("keys") or "").strip()  # e.g. "cmd,space" or "cmd,shift,t"
        return await peekaboo(["hotkey", keys]) if keys else "error: which keys?"

    async def look_at_screen(ctx: RequestContext, args: dict[str, Any]) -> str:
        # Capture the screen and return it as base64 JPEG. The tool loop (produces_image)
        # feeds it to Jarvis's OWN vision model — Jarvis sees the pixels directly.
        try:
            data = await post("capture", {}, timeout=cfg.request_timeout_s)
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        if data.get("error"):
            return f"error: {data['error']}"
        return data.get("image_b64") or "error: no image captured"

    async def describe_screen(ctx: RequestContext, args: dict[str, Any]) -> str:
        # peekaboo's vision (`--analyze`): captures the screen + answers a question
        # about it as TEXT. For visual content NOT in the UI element list (images,
        # charts, rendered web pages). Needs peekaboo's AI provider configured.
        q = (args.get("question") or "Describe what is currently shown on the screen.").strip()
        return await peekaboo(["image", "--mode", "screen", "--analyze", q], timeout=cfg.request_timeout_s)

    async def control_mac(ctx: RequestContext, args: dict[str, Any]) -> str:
        # peekaboo's OWN agent: one-shot natural-language automation. Needs peekaboo
        # configured with an AI provider key (PEEKABOO_AI_PROVIDERS) — unlike the
        # atomic tools above. Prefer those unless a whole task is easier in one go.
        task = (args.get("task") or "").strip()
        if not task:
            return "error: empty task"
        return await peekaboo(["agent", task], timeout=cfg.request_timeout_s)

    async def repos_list(ctx: RequestContext, args: dict[str, Any]) -> str:
        try:
            data = await post("list_repos", {})
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        repos = data.get("repos", [])
        if not repos:
            return "no repos are configured (the worker repo root isn't set)."
        return "the repos I can work in are: " + ", ".join(repos) + "."

    obj = "object"
    return [
        Tool(
            "run_shell",
            "Run a shell command on the worker Mac and return its output.",
            {"type": obj, "properties": {"command": {"type": "string"}}, "required": ["command"]},
            "worker.shell",
            shell,
            announce=True,
        ),
        Tool(
            "start_coding_job",
            "Kick off an autonomous coding task on the worker Mac (a coding agent "
            "runs it in the background). Use for write/fix/refactor requests on a repo.",
            {
                "type": obj,
                "properties": {
                    "task": {"type": "string", "description": "What to build/fix."},
                    "name": {
                        "type": "string",
                        "description": "A short human name for the job if the user gives one (optional).",
                    },
                    "agent": {
                        "type": "string",
                        "enum": ["codex", "claude"],
                        "description": "Which local coding agent to use (optional; default is the worker's).",
                    },
                    "repo": {"type": "string", "description": "Repo name or path (optional)."},
                },
                "required": ["task"],
            },
            "worker.code",
            code,
            announce=True,
        ),
        Tool(
            "check_coding_job",
            "Check a coding job's status and result. Give the job's name or id, or "
            "nothing to check the most recent.",
            {
                "type": obj,
                "properties": {
                    "job": {"type": "string", "description": "Job name or id (optional)."}
                },
            },
            "worker.code",
            check,
            announce=False,
        ),
        Tool(
            "list_coding_jobs",
            "List recent coding jobs and how many are running.",
            {"type": obj, "properties": {}},
            "worker.code",
            jobs_list,
            announce=False,
        ),
        Tool(
            "list_repos",
            "List the git repos the worker can run coding jobs in (use the exact "
            "name when starting a job).",
            {"type": obj, "properties": {}},
            "worker.code",
            repos_list,
            announce=False,
        ),
        Tool(
            "clean_up_coding_jobs",
            "Clean up a finished coding job by name (removes its worktree and "
            "deletes its branch), or all finished jobs if none is named. Only do "
            "this once the user has reviewed/merged the work — the branch is deleted.",
            {"type": obj, "properties": {"job": {"type": "string", "description": "Job name (optional)."}}},
            "worker.code",
            cleanup,
            announce=True,
        ),
        Tool(
            "take_screenshot",
            "Take a screenshot on the worker Mac; returns where it was saved.",
            {"type": obj, "properties": {}},
            "worker.screenshot",
            screenshot,
            announce=False,
        ),
        Tool(
            "run_applescript",
            "Run an AppleScript on the worker Mac to control apps.",
            {"type": obj, "properties": {"script": {"type": "string"}}, "required": ["script"]},
            "worker.applescript",
            applescript,
            announce=True,
        ),
        Tool(
            "see_screen",
            "Look at the Mac's screen — returns the visible UI elements/text (peekaboo). "
            "Use first to find what to click before acting.",
            {"type": obj, "properties": {}},
            "worker.gui",
            see_screen,
            announce=True,
        ),
        Tool(
            "list_apps",
            "List the apps currently running on the Mac.",
            {"type": obj, "properties": {}},
            "worker.gui",
            list_apps,
            announce=False,
        ),
        Tool(
            "launch_app",
            "Open/launch an app on the Mac by name (e.g. Safari, Calculator).",
            {"type": obj, "properties": {"app": {"type": "string"}}, "required": ["app"]},
            "worker.gui",
            launch_app,
            announce=True,
        ),
        Tool(
            "click",
            "Click an on-screen element by its visible label or text (run see_screen "
            "first to know what's there).",
            {"type": obj, "properties": {"target": {"type": "string", "description": "The element label/text to click."}}, "required": ["target"]},
            "worker.gui",
            click,
            announce=True,
        ),
        Tool(
            "type_text",
            "Type text into the focused field on the Mac.",
            {"type": obj, "properties": {"text": {"type": "string"}}, "required": ["text"]},
            "worker.gui",
            type_text,
            announce=True,
        ),
        Tool(
            "press_keys",
            "Press a keyboard shortcut on the Mac, e.g. 'cmd,space' or 'cmd,shift,t'.",
            {"type": obj, "properties": {"keys": {"type": "string", "description": "Comma-separated combo."}}, "required": ["keys"]},
            "worker.gui",
            press_keys,
            announce=True,
        ),
        Tool(
            "look_at_screen",
            "Look at the Mac's screen YOURSELF — captures it and sends the actual image "
            "to your vision. Use to read or understand on-screen content (results, "
            "images, web pages) that the UI element list doesn't give you.",
            {"type": obj, "properties": {}},
            "worker.gui",
            look_at_screen,
            announce=True,
            produces_image=True,
        ),
        Tool(
            "describe_screen",
            "Describe what's VISUALLY on the Mac's screen, or answer a question about "
            "it (peekaboo's vision). An alternative to look_at_screen that returns text "
            "instead of sending you the image. Needs peekaboo's AI provider configured.",
            {"type": obj, "properties": {"question": {"type": "string", "description": "What to look for (optional)."}}},
            "worker.gui",
            describe_screen,
            announce=True,
        ),
        Tool(
            "control_mac",
            "Do a whole GUI task on the Mac from one natural-language description "
            "(peekaboo's own agent). Needs peekaboo configured with an AI key; prefer "
            "see_screen + click/type/press_keys/launch_app when you can.",
            {"type": obj, "properties": {"task": {"type": "string", "description": "What to do on screen."}}, "required": ["task"]},
            "worker.gui",
            control_mac,
            announce=True,
        ),
    ]
