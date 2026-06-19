"""Browser lane — a real Chrome the brain drives over CDP (nodriver, no Playwright).

Self-contained: this package imports nothing from `jarvis.brain`. The worker hosts
it (one `BrowserHost` per process); the brain acts on it over HTTP. It can later be
lifted into its own daemon/package/repo by changing a URL — the same boundary
discipline as the worker itself.
"""

from __future__ import annotations

from jarvis.browser.doctor import browser_doctor
from jarvis.browser.host import BrowserHost

__all__ = ["BrowserHost", "browser_doctor"]
