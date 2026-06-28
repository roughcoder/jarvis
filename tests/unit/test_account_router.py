from __future__ import annotations

import asyncio

from jarvis.brain.account_adapters import FakeAccountAdapter
from jarvis.brain.account_router import AccountRouter
from jarvis.brain.accounts import AccountBinding
from jarvis.brain.context import RequestContext
from jarvis.brain.identity import HOUSE


def _ctx(
    *caps: str,
    identity: str = HOUSE,
    scope: str = "house",
    confidence: str = "strong",
) -> RequestContext:
    return RequestContext("dev", identity, scope, frozenset(caps), confidence=confidence)


def _binding(kind: str, *grants: str, principal: str = HOUSE, provider: str = "fake") -> AccountBinding:
    return AccountBinding(
        name=f"{principal}-{kind}",
        principal=principal,
        kind=kind,
        provider=provider,
        grants=frozenset(grants),
    )


def test_router_dispatches_allowed_email_search_to_adapter() -> None:
    adapter = FakeAccountAdapter({"email.search": "found mail"})
    router = AccountRouter(email_adapters={"fake": adapter})
    binding = _binding("email", "email.read")

    out = asyncio.run(router.search_email(_ctx("email.read"), binding, "school"))

    assert out == "found mail"
    assert [call.operation for call in adapter.calls] == ["email.search"]
    assert adapter.calls[0].payload["query"] == "school"


def test_router_blocks_denied_email_search_before_adapter() -> None:
    adapter = FakeAccountAdapter()
    router = AccountRouter(email_adapters={"fake": adapter})
    binding = _binding("email", "email.read", principal="neil")

    out = asyncio.run(router.search_email(_ctx("email.read", confidence="unknown"), binding, "school"))

    assert "account policy denied" in out
    assert adapter.calls == []


def test_router_rejects_wrong_binding_kind_before_adapter() -> None:
    adapter = FakeAccountAdapter()
    router = AccountRouter(email_adapters={"fake": adapter})
    binding = _binding("calendar", "email.read")

    out = asyncio.run(router.search_email(_ctx("email.read"), binding, "school"))

    assert "not an email account" in out
    assert adapter.calls == []


def test_router_reports_missing_provider_adapter() -> None:
    router = AccountRouter(email_adapters={})
    binding = _binding("email", "email.read", provider="missing")

    out = asyncio.run(router.search_email(_ctx("email.read"), binding, "school"))

    assert "no email adapter" in out


def test_router_converts_send_to_draft_when_policy_downgrades() -> None:
    adapter = FakeAccountAdapter({"email.create_draft": "draft saved"})
    router = AccountRouter(email_adapters={"fake": adapter})
    binding = _binding("email", "email.draft")

    out = asyncio.run(
        router.send_email(
            _ctx("email.draft"),
            binding,
            {"to": "family@example.invalid", "body": "hi"},
            recipient_class="household",
        )
    )

    assert out == "draft saved"
    assert [call.operation for call in adapter.calls] == ["email.create_draft"]


def test_router_requires_confirmation_for_external_send() -> None:
    adapter = FakeAccountAdapter()
    router = AccountRouter(email_adapters={"fake": adapter})
    binding = _binding("email", "email.send")

    out = asyncio.run(
        router.send_email(
            _ctx("email.send"),
            binding,
            {"to": "external@example.invalid", "body": "hi"},
        )
    )

    assert "confirmation required" in out
    assert adapter.calls == []


def test_router_dispatches_calendar_events_to_calendar_adapter() -> None:
    adapter = FakeAccountAdapter({"calendar.list_events": "events"})
    router = AccountRouter(calendar_adapters={"fake": adapter})
    binding = _binding("calendar", "calendar.read")

    out = asyncio.run(router.list_events(_ctx("calendar.read"), binding, days=3))

    assert out == "events"
    assert [call.operation for call in adapter.calls] == ["calendar.list_events"]
    assert adapter.calls[0].payload["days"] == 3
