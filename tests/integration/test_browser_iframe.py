"""Integration: cross-origin iframe traversal (needs the [browser] extra + Chrome).

The capability that unblocks real booking widgets (the Old Crown's date/time form is
a third-party cross-origin frame). Serves a parent page (reached via `localhost`) that
embeds a child from `127.0.0.1` — a genuine cross-origin OOPIF — and asserts the
snapshot lists the in-frame button and a click reaches inside it. Self-skips without
nodriver/Chrome.
"""

from __future__ import annotations

import asyncio
import importlib.util
import re
import socket
import threading
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import pytest

from jarvis.browser.doctor import browser_doctor
from jarvis.config import BrowserConfig

pytestmark = pytest.mark.integration

_CHILD = b'<!doctype html><body><button id="b" onclick="this.textContent=\'CLICKED_INSIDE\'">INSIDE_FRAME_BTN</button></body>'


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _ready_cfg() -> BrowserConfig:
    if importlib.util.find_spec("nodriver") is None:
        pytest.skip("nodriver not installed (uv sync --extra browser)")
    cfg = BrowserConfig(_env_file=None, headless=True, jarvis_profile_dir="")
    if browser_doctor(cfg)["chrome_path"] in ("", "(not found)"):
        pytest.skip("no Chrome binary found")
    return cfg


def test_cross_origin_iframe_snapshot_and_click(tmp_path) -> None:
    cfg = _ready_cfg()
    port = _free_port()
    (tmp_path / "child.html").write_bytes(_CHILD)
    (tmp_path / "parent.html").write_text(
        f'<!doctype html><title>Parent</title><body><button>top</button>'
        f'<iframe src="http://127.0.0.1:{port}/child.html" width="320" height="150"></iframe>'
    )
    handler = partial(SimpleHTTPRequestHandler, directory=str(tmp_path))
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    from jarvis.browser import BrowserHost

    async def go() -> None:
        host = BrowserHost(cfg)
        try:
            await host.open(f"http://localhost:{port}/parent.html", "jarvis")
            await asyncio.sleep(1.0)
            snap = await host.snapshot("jarvis")
            assert "INSIDE_FRAME_BTN" in snap["elements"]  # element inside the cross-origin frame
            assert "(in frame)" in snap["elements"]
            ref = int(re.search(r"\[(\d+)\] button \"INSIDE_FRAME_BTN\"", snap["elements"]).group(1))
            assert (await host.click(ref, "jarvis"))["ok"]
            await asyncio.sleep(0.5)
            assert "CLICKED_INSIDE" in (await host.snapshot("jarvis"))["elements"]  # click reached inside
        finally:
            await host.aclose()

    try:
        asyncio.run(go())
    finally:
        server.shutdown()
