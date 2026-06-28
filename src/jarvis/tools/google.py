"""Email/calendar tools — Jarvis's own house account via account adapters.

The visible tool surface is provider-neutral. The first provider adapter is
OpenClaw `gogcli`, which holds its own OAuth state from `jarvis google-setup`
and is selected behind the account router.
"""

from __future__ import annotations

from typing import Any

from jarvis.brain.account_adapters import GogcliAccountAdapter
from jarvis.brain.account_router import AccountRouter, classify_email_recipient
from jarvis.brain.accounts import AccountBinding, AccountBindingError, load_account_binding
from jarvis.brain.context import RequestContext
from jarvis.brain.identity import HOUSE
from jarvis.config import AccountConfig, GoogleConfig
from jarvis.tools.base import Tool

_MAX_CALENDAR_DAYS = 366


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
    accounts: AccountConfig | None = None,
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
    email_binding = email_binding or _configured_house_binding(
        accounts, name=getattr(accounts, "house_email_binding", ""), kind="email"
    ) or _house_email_binding()
    calendar_binding = calendar_binding or _configured_house_binding(
        accounts, name=getattr(accounts, "house_calendar_binding", ""), kind="calendar"
    ) or _house_calendar_binding()

    async def search_email(ctx: RequestContext, args: dict[str, Any]) -> str:
        query = (args.get("query") or "").strip()
        if not query:
            return "error: need a search query"
        return await router.search_email(ctx, email_binding, query)

    async def upcoming_events(ctx: RequestContext, args: dict[str, Any]) -> str:
        days = _parse_days(args.get("days"), default=cfg.calendar_days)
        if isinstance(days, str):
            return days
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
            recipient_class=classify_email_recipient(email_binding, to),
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


def _configured_house_binding(
    accounts: AccountConfig | None,
    *,
    name: str,
    kind: str,
) -> AccountBinding | None:
    if accounts is None or not name:
        return None
    try:
        binding = load_account_binding(accounts.bindings_dir, HOUSE, name)
    except AccountBindingError:
        return _closed_house_binding(name=name, kind=kind)
    return binding if binding.kind == kind else _closed_house_binding(name=name, kind=kind)


def _parse_days(value: Any, *, default: int) -> int | str:
    if value is None or value == "":
        value = default
    if isinstance(value, bool):
        return "error: days must be a positive integer"
    try:
        days = int(value)
    except (TypeError, ValueError):
        return "error: days must be a positive integer"
    if days < 1:
        return "error: days must be a positive integer"
    if days > _MAX_CALENDAR_DAYS:
        return f"error: days must be at most {_MAX_CALENDAR_DAYS}"
    return days


def _closed_house_binding(*, name: str, kind: str) -> AccountBinding:
    return AccountBinding(
        name=name,
        principal=HOUSE,
        kind=kind,
        provider="unconfigured",
        grants=frozenset(),
    )
