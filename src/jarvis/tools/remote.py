"""Remote coding tools — dispatch to Claude's cloud (Managed Agents, Phase 3 §8).

A thin wrapper over the RemoteClient: start a cloud session and send it a task
(fire-and-forget), and check a session's status. Gated by `remote.code`, distinct
from local `worker.code` so cloud coding is granted on purpose.
"""

from __future__ import annotations

from typing import Any

import httpx

from jarvis.runtime import RequestContext
from jarvis.config import RemoteConfig
from jarvis.remote.client import RemoteClient
from jarvis.tools.base import Tool

CAPABILITY = "remote.code"


def make_remote_tools(cfg: RemoteConfig) -> list[Tool]:
    client = RemoteClient(cfg)

    async def start(ctx: RequestContext, args: dict[str, Any]) -> str:
        task = (args.get("task") or "").strip()
        if not task:
            return "error: empty task"
        if not cfg.configured:
            return "remote coding isn't set up yet — run `jarvis remote-setup` first."
        title = (args.get("title") or task)[:80]
        try:
            session = await client.create_session(title)
            sid = session.get("id")
            await client.send_task(sid, task)
        except httpx.HTTPStatusError as exc:
            return f"error: the cloud rejected the session ({exc.response.status_code})"
        except Exception as exc:  # noqa: BLE001
            return f"error: couldn't start the remote session ({exc})"
        return (
            f"Started a remote coding session in the cloud (id {sid}). "
            "It runs on Anthropic's infrastructure — ask me to check on it."
        )

    async def check(ctx: RequestContext, args: dict[str, Any]) -> str:
        sid = (args.get("session_id") or "").strip()
        if not sid:
            return "error: need a session id"
        try:
            data = await client.get_session(sid)
        except Exception as exc:  # noqa: BLE001
            return f"error: couldn't reach the remote session ({exc})"
        status = data.get("status") or "unknown"
        title = data.get("title") or sid
        return f"the remote session {title!r} is {status}."

    obj = "object"
    return [
        Tool(
            "start_remote_coding_job",
            "Kick off a coding task in the CLOUD (an Anthropic-managed sandbox), not "
            "on the local Mac. Use when the user asks to run it remotely or in the cloud.",
            {
                "type": obj,
                "properties": {
                    "task": {"type": "string", "description": "What to build/fix."},
                    "title": {"type": "string", "description": "Optional short title."},
                },
                "required": ["task"],
            },
            CAPABILITY,
            start,
            announce=True,
        ),
        Tool(
            "check_remote_coding_job",
            "Check a remote (cloud) coding session's status by its id.",
            {"type": obj, "properties": {"session_id": {"type": "string"}}, "required": ["session_id"]},
            CAPABILITY,
            check,
            announce=False,
        ),
    ]
