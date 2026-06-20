"""Integration: browser_press dispatches real keyboard events (the keyboard primitive).

The keyboard half of widget interaction — keyboard-driven dropdowns, tabbing, submit,
Escape. Focuses an input and presses a sequence, asserting the page saw real keydowns.
Self-skips without the [browser] extra / Chrome.
"""

from __future__ import annotations

import asyncio
import importlib.util
import re

import pytest

from jarvis.browser.doctor import browser_doctor
from jarvis.config import BrowserConfig

pytestmark = pytest.mark.integration

_PAGE = (
    "<!doctype html><title>START</title><input id=q><script>"
    "var s=[];document.getElementById('q').addEventListener('keydown',"
    "function(e){s.push(e.key);document.title=s.join(',');});</script>"
)


def test_browser_press_fires_real_keys(tmp_path) -> None:
    if importlib.util.find_spec("nodriver") is None:
        pytest.skip("nodriver not installed (uv sync --extra browser)")
    cfg = BrowserConfig(_env_file=None, headless=True, jarvis_profile_dir="")
    if browser_doctor(cfg)["chrome_path"] in ("", "(not found)"):
        pytest.skip("no Chrome binary found")
    page = tmp_path / "keys.html"
    page.write_text(_PAGE)

    from jarvis.browser import BrowserHost

    async def go() -> None:
        host = BrowserHost(cfg)
        try:
            await host.open(page.as_uri(), "jarvis")
            snap = await host.snapshot("jarvis")
            ref = int(re.search(r"\[(\d+)\] input", snap["elements"]).group(1))
            res = await host.press(["ArrowDown", "ArrowDown", "Enter", "Escape"], "jarvis", ref=ref)
            assert res["title"] == "ArrowDown,ArrowDown,Enter,Escape"  # real keydowns reached the page
        finally:
            await host.aclose()

    asyncio.run(go())
