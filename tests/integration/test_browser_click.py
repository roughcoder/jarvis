"""Integration: the browser uses REAL pointer clicks (general, not site-specific).

Modern widgets (React-Aria/MUI dropdowns, custom comboboxes) open on
pointerdown/mousedown, which a synthetic DOM `.click()` never fires. The host clicks
via CDP mouse events (move→press→release); this proves those events actually reach
the page. Self-skips without the [browser] extra / Chrome.
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
    "<!doctype html><title>START</title><button id=b>menu</button><script>"
    "var b=document.getElementById('b'),g={};"
    "b.addEventListener('pointerdown',()=>{g.pd=1;u();});"
    "b.addEventListener('mousedown',()=>{g.md=1;u();});"
    "b.addEventListener('mouseup',()=>{g.mu=1;u();});"
    "function u(){document.title='PD='+(g.pd?1:0)+' MD='+(g.md?1:0)+' MU='+(g.mu?1:0);}"
    "</script>"
)


def test_real_pointer_click_fires_pointer_events(tmp_path) -> None:
    if importlib.util.find_spec("nodriver") is None:
        pytest.skip("nodriver not installed (uv sync --extra browser)")
    cfg = BrowserConfig(_env_file=None, headless=True, jarvis_profile_dir="")
    if browser_doctor(cfg)["chrome_path"] in ("", "(not found)"):
        pytest.skip("no Chrome binary found")
    page = tmp_path / "pd.html"
    page.write_text(_PAGE)

    from jarvis.browser import BrowserHost

    async def go() -> None:
        host = BrowserHost(cfg)
        try:
            await host.open(page.as_uri(), "jarvis")
            snap = await host.snapshot("jarvis")
            ref = int(re.search(r"\[(\d+)\] button \"menu\"", snap["elements"]).group(1))
            res = await host.click(ref, "jarvis")
            # pointerdown + mousedown + mouseup all fired — a synthetic .click() fires none.
            assert res["title"] == "PD=1 MD=1 MU=1"
        finally:
            await host.aclose()

    asyncio.run(go())
