"""Email/calendar tools (Google adapter) — gating and graceful no-binary behaviour."""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.config import GoogleConfig, ToolsConfig
from jarvis.tools import build_registry
from jarvis.tools.google import make_google_tools


def _ctx(*caps: str) -> RequestContext:
    return RequestContext("mac", "house", "house", frozenset(caps))


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


def test_legacy_google_capability_aliases_still_work() -> None:
    reg = build_registry(ToolsConfig(_env_file=None), google=GoogleConfig(_env_file=None))

    legacy_read = {t.name for t in reg.available_for(_ctx("google.read"))}
    assert {"search_email", "upcoming_events"} <= legacy_read
    assert "send_email" not in legacy_read
    assert "send_email" in {t.name for t in reg.available_for(_ctx("google.send"))}


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

    monkeypatch.setattr("jarvis.tools.google.shutil.which", lambda _bin: "/usr/bin/gog")
    monkeypatch.setattr("jarvis.tools.google.asyncio.create_subprocess_exec", fake_exec)
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
