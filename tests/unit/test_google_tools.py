"""Google tools (Phase 3 §6) — gating, schema, and graceful no-binary behaviour."""

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
    # deny-by-default: no google caps => no google tools
    assert not {"search_email", "upcoming_events", "send_email"} & {
        t.name for t in reg.available_for(_ctx())
    }
    read = {t.name for t in reg.available_for(_ctx("google.read"))}
    assert {"search_email", "upcoming_events"} <= read
    assert "send_email" not in read  # send is the separate google.send capability
    assert "send_email" in {t.name for t in reg.available_for(_ctx("google.send"))}


def test_missing_binary_reports_not_set_up() -> None:
    cfg = GoogleConfig(_env_file=None, gogcli_bin="gogcli-does-not-exist")
    tools = {t.name: t for t in make_google_tools(cfg)}
    out = asyncio.run(tools["search_email"].handler(_ctx("google.read"), {"query": "hi"}))
    assert "google-setup" in out


def test_empty_args_validated() -> None:
    tools = {t.name: t for t in make_google_tools(GoogleConfig(_env_file=None))}
    assert "need a search query" in asyncio.run(tools["search_email"].handler(_ctx("google.read"), {}))
    assert "recipient" in asyncio.run(tools["send_email"].handler(_ctx("google.send"), {"to": "x"}))
