"""Browser lane readiness — is nodriver installed and a Chrome binary present?

Mirrors the worker's `gui_doctor`: a cheap check the brain/CLI can call to report
what's missing before the lane is used, instead of failing deep inside a turn.
"""

from __future__ import annotations

import importlib.util
import pathlib

_MAC_CHROMES = (
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
)


def _find_chrome(explicit: str) -> str:
    if explicit and pathlib.Path(explicit).exists():
        return explicit
    for p in _MAC_CHROMES:
        if pathlib.Path(p).exists():
            return p
    return ""


def browser_doctor(cfg) -> dict:  # noqa: ANN001 - BrowserConfig (duck-typed)
    nodriver_ok = importlib.util.find_spec("nodriver") is not None
    chrome = _find_chrome(cfg.chrome_path)
    steps = []
    if not nodriver_ok:
        steps.append("install the extra: uv sync --extra browser")
    if not chrome:
        steps.append("install Google Chrome (or set BROWSER_CHROME_PATH)")
    return {
        "nodriver_installed": nodriver_ok,
        "chrome_path": chrome or "(not found)",
        "headless": cfg.headless,
        "default_context": cfg.default_context,
        "ready": nodriver_ok and bool(chrome),
        "next_steps": "; ".join(steps) or "ready",
    }
