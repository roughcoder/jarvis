"""Integration: live BrowserHost driving a real Chrome (needs the [browser] extra).

Exercises the open → snapshot → read → click loop headless against a controlled
page (example.com). Skips cleanly when nodriver / Chrome are absent, so the default
suite stays green on a machine without the extra.
"""

from __future__ import annotations

import asyncio
import importlib.util

import pytest

from jarvis.browser.doctor import browser_doctor
from jarvis.config import BrowserConfig

pytestmark = pytest.mark.integration


def _ready() -> BrowserConfig:
    if importlib.util.find_spec("nodriver") is None:
        pytest.skip("nodriver not installed (uv sync --extra browser)")
    # Headless + ephemeral profile so the test pops no window and leaves no state.
    cfg = BrowserConfig(_env_file=None, headless=True, jarvis_profile_dir="")
    if not browser_doctor(cfg)["chrome_path"] or browser_doctor(cfg)["chrome_path"] == "(not found)":
        pytest.skip("no Chrome binary found")
    return cfg


def test_browser_host_open_snapshot_read_click() -> None:
    from jarvis.browser import BrowserHost

    cfg = _ready()

    async def go() -> None:
        host = BrowserHost(cfg)
        try:
            opened = await host.open("example.com", "jarvis")
            assert opened["ok"] and "Example Domain" in opened["title"]

            snap = await host.snapshot("jarvis")
            assert snap["ok"]
            assert "[1]" in snap["elements"]  # the "More information" link got a ref

            read = await host.read("jarvis")
            assert read["ok"] and "Example Domain" in read["text"]

            clicked = await host.click(1, "jarvis")  # follow the link
            assert clicked["ok"] and "iana.org" in clicked["url"]
        finally:
            await host.aclose()

    asyncio.run(go())
