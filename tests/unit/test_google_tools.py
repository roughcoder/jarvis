"""Email/calendar tools (Google adapter) — gating and graceful no-binary behaviour."""

from __future__ import annotations

import asyncio

from jarvis.brain.account_adapters import FakeAccountAdapter
from jarvis.brain.account_router import AccountRouter
from jarvis.brain.accounts import AccountBinding
from jarvis.brain.identity import HOUSE
from jarvis.brain.context import RequestContext
from jarvis.config import GoogleConfig, ToolsConfig
from jarvis.tools import build_registry
from jarvis.tools.google import make_google_tools


def _ctx(
    *caps: str,
    identity: str = HOUSE,
    scope: str = HOUSE,
    confidence: str = "strong",
) -> RequestContext:
    return RequestContext("mac", identity, scope, frozenset(caps), confidence=confidence)


def _binding(kind: str, *grants: str) -> AccountBinding:
    return AccountBinding(
        name=f"house-{kind}",
        principal=HOUSE,
        kind=kind,
        provider="fake",
        grants=frozenset(grants),
    )


def test_google_tools_registered_and_gated() -> None:
    reg = build_registry(ToolsConfig(_env_file=None), google=GoogleConfig(_env_file=None))
    # deny-by-default: no email/calendar caps => no account tools
    assert not {"search_email", "upcoming_events", "send_email"} & {
        t.name for t in reg.available_for(_ctx())
    }
    email_read = {t.name for t in reg.available_for(_ctx("email.read"))}
    calendar_read = {t.name for t in reg.available_for(_ctx("calendar.read"))}
    assert "search_email" in email_read
    assert "upcoming_events" not in email_read
    assert "upcoming_events" in calendar_read
    assert "send_email" not in email_read  # send is the separate email.send capability
    assert "send_email" in {t.name for t in reg.available_for(_ctx("email.send"))}


def test_missing_binary_reports_not_set_up() -> None:
    cfg = GoogleConfig(_env_file=None, gogcli_bin="gogcli-does-not-exist")
    tools = {t.name: t for t in make_google_tools(cfg)}
    out = asyncio.run(tools["search_email"].handler(_ctx("email.read"), {"query": "hi"}))
    assert "google-setup" in out


def test_empty_args_validated() -> None:
    tools = {t.name: t for t in make_google_tools(GoogleConfig(_env_file=None))}
    assert "need a search query" in asyncio.run(tools["search_email"].handler(_ctx("email.read"), {}))
    assert "recipient" in asyncio.run(tools["send_email"].handler(_ctx("email.send"), {"to": "x"}))


def test_gog_invocation_is_noninteractive_and_allowlisted(monkeypatch) -> None:  # noqa: ANN001
    calls: list[tuple[str, ...]] = []

    class FakeProc:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"ok", b""

    async def fake_exec(*argv, stdout=None, stderr=None):  # noqa: ANN001
        calls.append(tuple(argv))
        return FakeProc()

    monkeypatch.setattr("jarvis.brain.account_adapters.shutil.which", lambda _bin: "/usr/bin/gog")
    monkeypatch.setattr("jarvis.brain.account_adapters.asyncio.create_subprocess_exec", fake_exec)
    tools = {t.name: t for t in make_google_tools(GoogleConfig(_env_file=None, gogcli_bin="gog"))}

    out = asyncio.run(tools["upcoming_events"].handler(_ctx("calendar.read"), {"days": 2}))

    assert out == "ok"
    assert calls == [
        (
            "gog",
            "--plain",
            "--no-input",
            "--enable-commands-exact=calendar.events",
            "calendar",
            "events",
            "--days",
            "2",
        )
    ]


def test_tools_route_through_account_policy_before_adapter() -> None:
    adapter = FakeAccountAdapter({"email.search": "mail"})
    router = AccountRouter(email_adapters={"fake": adapter})
    binding = AccountBinding(
        name="neil-email",
        principal="neil",
        kind="email",
        provider="fake",
        grants=frozenset({"email.read"}),
    )
    tools = {
        t.name: t
        for t in make_google_tools(
            GoogleConfig(_env_file=None),
            router=router,
            email_binding=binding,
        )
    }

    denied = asyncio.run(
        tools["search_email"].handler(
            _ctx("email.read", confidence="unknown"),
            {"query": "school"},
        )
    )

    assert "mail" == asyncio.run(
        tools["search_email"].handler(
            _ctx("email.read", identity="neil", scope="personal"),
            {"query": "school"},
        )
    )
    assert "account policy denied" in denied
    assert [call.operation for call in adapter.calls] == ["email.search"]


def test_send_email_requires_confirmation_before_gogcli(monkeypatch) -> None:  # noqa: ANN001
    calls: list[tuple[str, ...]] = []

    class FakeProc:
        async def communicate(self) -> tuple[bytes, bytes]:
            return b"sent", b""

    async def fake_exec(*argv, stdout=None, stderr=None):  # noqa: ANN001
        calls.append(tuple(argv))
        return FakeProc()

    monkeypatch.setattr("jarvis.brain.account_adapters.shutil.which", lambda _bin: "/usr/bin/gog")
    monkeypatch.setattr("jarvis.brain.account_adapters.asyncio.create_subprocess_exec", fake_exec)
    tools = {t.name: t for t in make_google_tools(GoogleConfig(_env_file=None, gogcli_bin="gog"))}

    out = asyncio.run(
        tools["send_email"].handler(
            _ctx("email.send"),
            {"to": "external@example.invalid", "subject": "Hi", "body": "Hello"},
        )
    )

    assert "confirmation required" in out
    assert calls == []


def test_send_email_passes_household_recipient_policy_to_gogcli(monkeypatch) -> None:  # noqa: ANN001
    calls: list[tuple[str, ...]] = []

    class FakeProc:
        async def communicate(self) -> tuple[bytes, bytes]:
            return b"sent", b""

    async def fake_exec(*argv, stdout=None, stderr=None):  # noqa: ANN001
        calls.append(tuple(argv))
        return FakeProc()

    monkeypatch.setattr("jarvis.brain.account_adapters.shutil.which", lambda _bin: "/usr/bin/gog")
    monkeypatch.setattr("jarvis.brain.account_adapters.asyncio.create_subprocess_exec", fake_exec)
    tools = {t.name: t for t in make_google_tools(GoogleConfig(_env_file=None, gogcli_bin="gog"))}

    out = asyncio.run(
        tools["send_email"].handler(
            _ctx("email.send"),
            {
                "to": "family@example.invalid",
                "subject": "Hi",
                "body": "Hello",
                "recipient_class": "household",
            },
        )
    )

    assert out == "sent"
    assert calls == [
        (
            "gog",
            "--plain",
            "--no-input",
            "--enable-commands-exact=gmail.send",
            "gmail",
            "send",
            "--to",
            "family@example.invalid",
            "--subject",
            "Hi",
            "--body",
            "Hello",
        )
    ]
