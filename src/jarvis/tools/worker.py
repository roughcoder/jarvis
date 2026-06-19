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


_AGENT_AUTHORISED = (
    " The user asked for this by voice and has authorised it, so carry the WHOLE task "
    "out and do NOT pause to ask them to confirm — they've already consented by asking. "
    "(Still avoid clearly catastrophic or irreversible actions — deleting data, sending "
    "money, posting publicly — unless that was explicitly the request.)"
)


def _agent_task(task: str, autonomous: bool) -> str:
    """The instruction handed to peekaboo's agent. When autonomous, append the
    authorisation clause so it finishes instead of stalling on a confirmation that a
    one-shot call can never answer."""
    return f"{task}{_AGENT_AUTHORISED}" if autonomous else task


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

    async def look_at_screen(ctx: RequestContext, args: dict[str, Any]) -> str:
        # Read-only: capture the screen as base64 JPEG. The tool loop (produces_image)
        # feeds it to Jarvis's OWN vision model — Jarvis sees the pixels directly. Uses
        # `screencapture` (not peekaboo); it cannot control anything.
        try:
            data = await post("capture", {}, timeout=cfg.request_timeout_s)
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        if data.get("error"):
            return f"error: {data['error']}"
        return data.get("image_b64") or "error: no image captured"

    async def control_mac(ctx: RequestContext, args: dict[str, Any]) -> str:
        # The ONLY way to act on the Mac's GUI: peekaboo's autonomous agent. (Direct
        # atomic peekaboo tools were removed — the agent has the skill to drive the UI;
        # naive click/type orchestration didn't.) Needs an AI provider configured.
        task = (args.get("task") or "").strip()
        if not task:
            return "error: empty task"
        # Pin the model (peekaboo defaults to gpt-5.5, which a restricted project may
        # lack) and add --verbose so the agent's step trace lands in the worker log.
        argv = ["agent", _agent_task(task, cfg.peekaboo_agent_autonomous),
                "--model", cfg.peekaboo_agent_model]
        if cfg.verbose:
            argv.append("--verbose")
        # give the HTTP call a bit more than the worker's agent budget so it doesn't
        # time out before the agent finishes.
        return await peekaboo(argv, timeout=cfg.peekaboo_agent_timeout_s + 10)

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
            timeout_s=cfg.request_timeout_s + 5,  # a shell call can exceed the 8s hot-path guard
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
            "look_at_screen",
            "Look at the Mac's screen YOURSELF — captures it and sends the actual image "
            "to your vision so you can read or understand what's on screen (results, "
            "images, web pages). Read-only: it sees but does not act.",
            {"type": obj, "properties": {}},
            "worker.gui",
            look_at_screen,
            announce=True,
            produces_image=True,
        ),
        Tool(
            "control_mac",
            "Do ANY task on the Mac's screen from one natural-language description — an "
            "autonomous agent that plans, focuses the right window, acts, and verifies. "
            "This is the only way to control the GUI (open apps, click, type, drive any "
            "app), e.g. 'open the BBC Sport site in Chrome' or 'leave the Discord call'.",
            {"type": obj, "properties": {"task": {"type": "string", "description": "The full task to carry out on screen."}}, "required": ["task"]},
            "worker.gui",
            control_mac,
            announce=True,
            # The peekaboo agent drives the screen for up to a couple of minutes; give the
            # registry a budget past the handler's HTTP timeout so it isn't cancelled
            # mid-run with an empty error (it should normally run via run_in_background).
            timeout_s=cfg.peekaboo_agent_timeout_s + 15,
        ),
    ]
