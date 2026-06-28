"""Email/calendar tools — Jarvis's own house account via account adapters.

The visible tool surface is provider-neutral. The first provider adapter is
OpenClaw `gogcli`, which holds its own OAuth state from `jarvis google-setup`
and is selected behind the account router.
"""

from __future__ import annotations

from typing import Any

from jarvis.brain.account_adapters import GogcliAccountAdapter
from jarvis.brain.account_router import AccountRouter
from jarvis.brain.accounts import AccountBinding
from jarvis.brain.context import RequestContext
from jarvis.brain.identity import HOUSE
from jarvis.config import GoogleConfig
from jarvis.tools.base import Tool


def _house_email_binding() -> AccountBinding:
    return AccountBinding(
        name="house-gogcli-email",
        principal=HOUSE,
        kind="email",
        provider="gogcli",
        grants=frozenset({"email.read", "email.draft", "email.send"}),
    )


def _house_calendar_binding() -> AccountBinding:
    return AccountBinding(
        name="house-gogcli-calendar",
        principal=HOUSE,
        kind="calendar",
        provider="gogcli",
        grants=frozenset({"calendar.freebusy", "calendar.read"}),
    )


def make_google_tools(
    cfg: GoogleConfig,
    *,
    router: AccountRouter | None = None,
    email_binding: AccountBinding | None = None,
    calendar_binding: AccountBinding | None = None,
) -> list[Tool]:
    if router is None:
        adapter = GogcliAccountAdapter(cfg)
        router = AccountRouter(
            email_adapters={"gogcli": adapter},
            calendar_adapters={"gogcli": adapter},
        )
    email_binding = email_binding or _house_email_binding()
    calendar_binding = calendar_binding or _house_calendar_binding()

    async def search_email(ctx: RequestContext, args: dict[str, Any]) -> str:
        query = (args.get("query") or "").strip()
        if not query:
            return "error: need a search query"
        return await router.search_email(ctx, email_binding, query)

    async def upcoming_events(ctx: RequestContext, args: dict[str, Any]) -> str:
        days = int(args.get("days") or cfg.calendar_days)
        return await router.list_events(ctx, calendar_binding, days=days)

    async def send_email(ctx: RequestContext, args: dict[str, Any]) -> str:
        to = (args.get("to") or "").strip()
        subject = (args.get("subject") or "").strip()
        body = (args.get("body") or "").strip()
        if not (to and body):
            return "error: an email needs a recipient and a body"
        return await router.send_email(
            ctx,
            email_binding,
            {"to": to, "subject": subject, "body": body},
        )

    obj = "object"
    return [
        Tool(
            "search_email",
            "Search Jarvis's email (the house account) and return matching messages.",
            {
                "type": obj,
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
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
