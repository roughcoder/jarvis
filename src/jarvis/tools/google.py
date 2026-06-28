"""Email/calendar tools — Jarvis's own Google account via `gogcli` (Phase 3 §6).

A thin client like `worker.py`: it shells out to the local `gogcli` binary (which
holds its own OAuth token from `jarvis google-setup`) and never embeds provider
credentials. The tool surface is provider-neutral: mail and calendar actions are
gated by `email.*` / `calendar.*` capabilities even though this adapter currently
uses Google underneath. Every call is timeout-bounded so the hot path can't hang.
With the binary absent the tools still register but report "not set up".
"""

from __future__ import annotations

import asyncio
import shutil
from typing import Any

from jarvis.brain.context import RequestContext
from jarvis.config import GoogleConfig
from jarvis.tools.base import Tool


def make_google_tools(cfg: GoogleConfig) -> list[Tool]:
    async def run(args: list[str], *, enabled: str) -> str:
        if not shutil.which(cfg.gogcli_bin):
            return "google isn't set up yet — run `jarvis google-setup` first."
        try:
            proc = await asyncio.create_subprocess_exec(
                cfg.gogcli_bin,
                "--plain",
                "--no-input",
                f"--enable-commands-exact={enabled}",
                *args,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            out, _ = await asyncio.wait_for(proc.communicate(), cfg.timeout_s)
        except TimeoutError:
            return "error: google timed out"
        except Exception as exc:  # noqa: BLE001
            return f"error: couldn't run google ({exc})"
        text = (out or b"").decode("utf-8", "replace").strip()
        return text or "(no output)"

    async def search_email(ctx: RequestContext, args: dict[str, Any]) -> str:
        query = (args.get("query") or "").strip()
        if not query:
            return "error: need a search query"
        return await run(["gmail", "search", "--query", query], enabled="gmail.search")

    async def upcoming_events(ctx: RequestContext, args: dict[str, Any]) -> str:
        days = str(args.get("days") or cfg.calendar_days)
        return await run(["calendar", "events", "--days", days], enabled="calendar.events")

    async def send_email(ctx: RequestContext, args: dict[str, Any]) -> str:
        to = (args.get("to") or "").strip()
        subject = (args.get("subject") or "").strip()
        body = (args.get("body") or "").strip()
        if not (to and body):
            return "error: an email needs a recipient and a body"
        return await run(
            ["gmail", "send", "--to", to, "--subject", subject, "--body", body],
            enabled="gmail.send",
        )

    obj = "object"
    return [
        Tool(
            "search_email",
            "Search Jarvis's email (the house account) and return matching messages.",
            {"type": obj, "properties": {"query": {"type": "string"}}, "required": ["query"]},
            "email.read",
            search_email,
            announce=True,
        ),
        Tool(
            "upcoming_events",
            "List upcoming calendar events for the next few days.",
            {"type": obj, "properties": {"days": {"type": "integer", "description": "Look-ahead window."}}},
            "calendar.read",
            upcoming_events,
            announce=True,
        ),
        Tool(
            "send_email",
            "Send an email from Jarvis's house account. Use only when the user clearly "
            "asks to send a message.",
            {
                "type": obj,
                "properties": {
                    "to": {"type": "string"},
                    "subject": {"type": "string"},
                    "body": {"type": "string"},
                },
                "required": ["to", "body"],
            },
            "email.send",
            send_email,
            announce=True,
        ),
    ]
