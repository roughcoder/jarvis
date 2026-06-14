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


def make_worker_tools(cfg: WorkerConfig) -> list[Tool]:
    def headers() -> dict[str, str]:
        tok = cfg.token.get_secret_value()
        return {"Authorization": f"Bearer {tok}"} if tok else {}

    async def post(action: str, args: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=cfg.request_timeout_s) as client:
            r = await client.post(
                f"{cfg.base_url}/run", json={"action": action, "args": args}, headers=headers()
            )
            r.raise_for_status()
            return r.json()

    async def shell(ctx: RequestContext, args: dict[str, Any]) -> str:
        cmd = (args.get("command") or "").strip()
        if not cmd:
            return "error: empty command"
        try:
            data = await post("shell", {"command": cmd})
        except Exception as exc:  # noqa: BLE001 - worker may be down
            return f"error: worker unreachable ({exc})"
        return data.get("output", "") or "(no output)"

    async def code(ctx: RequestContext, args: dict[str, Any]) -> str:
        task = (args.get("task") or "").strip()
        if not task:
            return "error: empty task"
        body: dict[str, Any] = {"prompt": task}
        if args.get("repo"):
            body["repo"] = args["repo"]
        try:
            data = await post("code", body)
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        return (
            f"Started a coding job on the worker (id {data.get('job_id')}). "
            "It runs in the background — ask me to check on it."
        )

    async def check(ctx: RequestContext, args: dict[str, Any]) -> str:
        jid = (args.get("job_id") or "").strip()
        if not jid:
            return "error: need a job_id"
        try:
            async with httpx.AsyncClient(timeout=cfg.request_timeout_s) as client:
                r = await client.get(f"{cfg.base_url}/jobs/{jid}", headers=headers())
            if r.status_code == 404:
                return f"no job {jid}"
            r.raise_for_status()
            data = r.json()
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        status = data.get("status")
        if status == "running":
            return f"job {jid} is still running."
        return f"job {jid} {status}. output: {(data.get('output') or '')[-1000:]}"

    async def screenshot(ctx: RequestContext, args: dict[str, Any]) -> str:
        try:
            data = await post("screenshot", {})
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        return data.get("output", "")

    async def applescript(ctx: RequestContext, args: dict[str, Any]) -> str:
        script = (args.get("script") or "").strip()
        if not script:
            return "error: empty script"
        try:
            data = await post("applescript", {"script": script})
        except Exception as exc:  # noqa: BLE001
            return f"error: worker unreachable ({exc})"
        return data.get("output", "") or "(no output)"

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
                    "repo": {"type": "string", "description": "Repo path on the worker (optional)."},
                },
                "required": ["task"],
            },
            "worker.code",
            code,
            announce=True,
        ),
        Tool(
            "check_coding_job",
            "Check the status and result of a coding job by its id.",
            {"type": obj, "properties": {"job_id": {"type": "string"}}, "required": ["job_id"]},
            "worker.code",
            check,
            announce=False,
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
    ]
