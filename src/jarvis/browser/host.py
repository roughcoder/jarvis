"""BrowserHost — drive a real Chrome over CDP via nodriver (validated, no Playwright).

The acting model mirrors OpenClaw/Hermes: **snapshot** the page to a compact list of
interactive elements each with a stable `[ref]`, **act** on a ref (click/type), then
**re-snapshot**. Refs are tagged onto the DOM (`data-jref`) by the snapshot JS, so
click/type resolve by a CSS attribute selector — no fragile pixel coordinates, and a
stale ref fails cleanly ("snapshot again") instead of mis-clicking.

Two device-scoped contexts share one host, lazily launched:
- `jarvis` — his own headed, persistent profile (his accounts; zero setup).
- `device` — the machine's default Chrome profile (its real logins).

`nodriver` is imported lazily so a worker without the `[browser]` extra (or with the
lane disabled) pays nothing. Every public method returns a plain JSON-able dict and
never raises — a failure comes back as `{"ok": False, "error": …}` for the model.
"""

from __future__ import annotations

import asyncio
import pathlib

# Tag every visible, interactive element with a 1-based [ref] and return one line per
# element: `[n] role "name"`. Re-running it re-tags from scratch (refs are per-snapshot).
# A single chained expression (filter→map→join), not an IIFE — nodriver's evaluate
# returns the value of a plain expression but not always an IIFE's return.
_SNAPSHOT_JS = r"""
Array.from(document.querySelectorAll('a,button,input,textarea,select,[role=button],[role=link],[role=textbox],[role=checkbox],[onclick],[contenteditable=true]'))
  .filter(el => {
    const r = el.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) return false;
    const st = window.getComputedStyle(el);
    return st.visibility !== 'hidden' && st.display !== 'none';
  })
  .map((el, i) => {
    el.setAttribute('data-jref', String(i + 1));
    const role = el.getAttribute('role') || el.tagName.toLowerCase();
    const name = (el.getAttribute('aria-label') || el.placeholder || el.value ||
                  el.innerText || el.getAttribute('name') || '').trim().replace(/\s+/g, ' ').slice(0, 80);
    return '[' + (i + 1) + '] ' + role + ' ' + JSON.stringify(name);
  })
  .join('\n')
"""
_TITLE_JS = "document.title"
_URL_JS = "location.href"
_TEXT_JS = "document.body ? document.body.innerText.slice(0, 6000) : ''"


class BrowserHost:
    def __init__(self, cfg) -> None:  # noqa: ANN001 - BrowserConfig (kept duck-typed; no brain import)
        self._cfg = cfg
        self._browsers: dict[str, object] = {}  # context -> nodriver Browser
        self._tabs: dict[str, object] = {}  # context -> current Tab
        self._lock = asyncio.Lock()

    def _profile_dir(self, context: str) -> str | None:
        if context == "device":
            return self._cfg.device_profile_dir or None
        return self._cfg.jarvis_profile_dir or None

    async def _browser(self, context: str):  # noqa: ANN202
        async with self._lock:
            b = self._browsers.get(context)
            if b is not None:
                return b
            import nodriver as uc  # lazy: only when the lane is actually used

            kwargs: dict = {"headless": self._cfg.headless}
            prof = self._profile_dir(context)
            if prof:
                pathlib.Path(prof).mkdir(parents=True, exist_ok=True)
                kwargs["user_data_dir"] = prof
            if self._cfg.chrome_path:
                kwargs["browser_executable_path"] = self._cfg.chrome_path
            b = await uc.start(**kwargs)
            self._browsers[context] = b
            return b

    async def _tab(self, context: str):  # noqa: ANN202
        tab = self._tabs.get(context)
        if tab is None:
            raise RuntimeError(f"no open page in the {context!r} browser — open a URL first")
        return tab

    async def _where(self, tab) -> tuple[str, str]:  # noqa: ANN001 - (url, title)
        return (await tab.evaluate(_URL_JS) or ""), (await tab.evaluate(_TITLE_JS) or "")

    async def open(self, url: str, context: str) -> dict:
        if not (url or "").strip():
            return {"ok": False, "error": "no url"}
        if "://" not in url:
            url = "https://" + url
        try:
            b = await self._browser(context)
            tab = await asyncio.wait_for(b.get(url), self._cfg.nav_timeout_s)
            self._tabs[context] = tab
            await asyncio.sleep(0.6)  # let the page settle before the model reads it
            cur, title = await self._where(tab)
            return {"ok": True, "context": context, "url": cur, "title": title}
        except Exception as exc:  # noqa: BLE001 - never raise to the model
            return {"ok": False, "error": f"couldn't open {url}: {exc}"}

    async def snapshot(self, context: str) -> dict:
        try:
            tab = await self._tab(context)
            listing = await tab.evaluate(_SNAPSHOT_JS)
            cur, title = await self._where(tab)
            return {
                "ok": True, "context": context, "url": cur, "title": title,
                "elements": listing or "(no interactive elements found)",
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    async def _resolve(self, tab, ref: int):  # noqa: ANN001, ANN202
        try:
            return await tab.select(f'[data-jref="{ref}"]', timeout=5)
        except Exception:  # noqa: BLE001 - not found / timed out
            return None

    async def click(self, ref: int, context: str) -> dict:
        try:
            tab = await self._tab(context)
            el = await self._resolve(tab, ref)
            if el is None:
                return {"ok": False, "error": f"no element [{ref}] — snapshot again, the page may have changed"}
            await el.click()
            await asyncio.sleep(0.5)
            cur, title = await self._where(tab)
            return {"ok": True, "url": cur, "title": title}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    async def type(self, ref: int, text: str, context: str, *, submit: bool = False) -> dict:
        try:
            tab = await self._tab(context)
            el = await self._resolve(tab, ref)
            if el is None:
                return {"ok": False, "error": f"no element [{ref}] — snapshot again"}
            await el.send_keys(text)
            if submit:
                await el.send_keys("\r\n")
                await asyncio.sleep(0.6)
            cur, title = await self._where(tab)
            return {"ok": True, "url": cur, "title": title}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    async def read(self, context: str) -> dict:
        try:
            tab = await self._tab(context)
            text = await tab.evaluate(_TEXT_JS)
            cur, title = await self._where(tab)
            return {"ok": True, "url": cur, "title": title, "text": text or ""}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    async def aclose(self) -> None:
        for b in self._browsers.values():
            try:
                b.stop()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass
        self._browsers.clear()
        self._tabs.clear()
