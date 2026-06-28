"""The `run_in_background` tool — the brain's entry to the fire-and-forget lane.

Gated by `background.run`. The handler hands the task to a `BackgroundRunner`
(injected at registration) and returns IMMEDIATELY — it never waits for the work.
The model is told to say it's on it and to expect a proactive update later, so the
voice turn stays fast while the real work happens detached (see brain/background.py).
"""

from __future__ import annotations

from typing import Protocol

from jarvis.runtime import RequestContext
from jarvis.tools.base import Tool


class BackgroundRunner(Protocol):
    def start(self, ctx: RequestContext, task: str) -> tuple[bool, str]: ...


def make_background_tool(runner: BackgroundRunner) -> Tool:
    async def handler(ctx: RequestContext, args: dict) -> str:
        task = str(args.get("task") or "").strip()
        if not task:
            return "error: a background task needs a description"
        ok, msg = runner.start(ctx, task)
        if not ok:
            return f"error: {msg}"
        return (
            f"{msg}. Tell the user you're on it and will let them know when it's done. "
            "Do NOT wait for the result or claim the task is finished — it runs in the "
            "background and you'll report the outcome to them proactively when it lands."
        )

    return Tool(
        "run_in_background",
        "Kick off a SLOW, multi-step task to run in the background and carry on talking "
        "— e.g. 'book a table at the pub for eight', deep research that needs many "
        "lookups, or a long task on the Mac. Jarvis says it's on it now and proactively "
        "reports the outcome once the task finishes. Use this whenever a request would "
        "otherwise make the user wait a long time with nothing to say. The task runs "
        "unattended with your CURRENT permissions, so include every detail it needs to "
        "finish without asking anything.",
        {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": (
                        "The full task to carry out unattended, with all the detail "
                        "needed to complete it without follow-up questions."
                    ),
                }
            },
            "required": ["task"],
        },
        "background.run",
        handler,
    )
