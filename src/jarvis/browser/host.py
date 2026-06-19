"""BrowserHost — drive a real Chrome over CDP via nodriver (no Playwright).

The acting model mirrors OpenClaw/Hermes: **snapshot** the page to a compact list of
interactive elements each with a stable `[ref]`, **act** on a ref (click/type), then
**re-snapshot**. Refs are tagged onto the DOM (`data-jref`) by the snapshot JS, so
click/type resolve by a CSS attribute selector — no fragile pixel coordinates, and a
stale ref fails cleanly ("snapshot again") instead of mis-clicking.

Two device-scoped contexts share one host, lazily launched:
- `jarvis` — his own headed, persistent profile (his accounts; zero setup).
- `device` — the machine's default Chrome profile (its real logins).

Reliability (learned from live tests): a dropped CDP connection is detected and the
browser relaunched + re-navigated once (a reused tab can die under target
replacement); `open` waits for the page to finish loading and auto-dismisses a
cookie/consent banner (many sites' widgets won't load until you do); a stale
Singleton lock from an orphaned Chrome is cleared before launch.

`nodriver` is imported lazily so a worker without the `[browser]` extra (or with the
lane disabled) pays nothing. Every public method returns a plain JSON-able dict and
never raises — a failure comes back as `{"ok": False, "error": …}` for the model.
"""

from __future__ import annotations

import asyncio
import contextlib
import pathlib

# Tag every visible, interactive element with a 1-based [ref] and return one line per
# element: `[n] role "name"`. A single chained expression (filter→map→join), not an
# IIFE — nodriver's evaluate returns a plain expression's value but not always an IIFE's.
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
# Click the first button/link whose text looks like a cookie/consent accept. Side-effect
# only (the click) — its return value isn't relied on.
_ACCEPT_COOKIES_JS = r"""
(() => {
  const rx = /^(accept|allow|agree|i agree|got it|ok|accept all|allow all|accept all cookies|allow all cookies)\b/i;
  for (const el of document.querySelectorAll('button,a,[role=button],input[type=button],input[type=submit]')) {
    const t = (el.innerText || el.value || el.getAttribute('aria-label') || '').trim();
    if (t && rx.test(t)) { el.click(); return t; }
  }
  return '';
})()
"""
def _submit_js(ref: int) -> str:
    """Real Enter on the input (a typed '\\n' doesn't fire the keypress many search
    boxes listen for), then submit its form if it has one."""
    return (
        '(() => { const el = document.querySelector(\'[data-jref="%d"]\'); if (!el) return false;'
        " el.focus();"
        " ['keydown','keypress','keyup'].forEach(t => el.dispatchEvent(new KeyboardEvent(t,"
        " {key:'Enter', code:'Enter', keyCode:13, which:13, bubbles:true})));"
        " try { if (el.form && el.form.requestSubmit) el.form.requestSubmit(); } catch (e) {}"
        " return true; })()" % ref
    )


_TITLE_JS = "document.title"
_URL_JS = "location.href"
_TEXT_JS = "document.body ? document.body.innerText.slice(0, 6000) : ''"
_READY_JS = "document.readyState"
_FRAMES_JS = "document.querySelectorAll('iframe').length"

# Substrings that mark a dropped/failed CDP connection (recover + retry once).
_CONN_MARKERS = ("no close frame", "connection closed", "failed to connect", "websocket", "is closed")


def _is_conn_error(exc: Exception) -> bool:
    return any(m in str(exc).lower() for m in _CONN_MARKERS)


class BrowserHost:
    def __init__(self, cfg) -> None:  # noqa: ANN001 - BrowserConfig (duck-typed; no brain import)
        self._cfg = cfg
        self._browsers: dict[str, object] = {}  # context -> nodriver Browser
        self._tabs: dict[str, object] = {}  # context -> current Tab
        self._urls: dict[str, str] = {}  # context -> last navigated URL (for recovery)
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
                p = pathlib.Path(prof)
                p.mkdir(parents=True, exist_ok=True)
                for lock in p.glob("Singleton*"):  # stale lock from an orphaned Chrome
                    try:
                        lock.unlink()
                    except OSError:
                        pass
                kwargs["user_data_dir"] = prof
            if self._cfg.chrome_path:
                kwargs["browser_executable_path"] = self._cfg.chrome_path
            b = await uc.start(**kwargs)
            self._browsers[context] = b
            return b

    async def _reset(self, context: str) -> None:
        async with self._lock:
            b = self._browsers.pop(context, None)
            self._tabs.pop(context, None)
        if b is not None:
            try:
                b.stop()
            except Exception:  # noqa: BLE001 - best-effort
                pass

    async def _recover(self, context: str) -> bool:
        """A dropped CDP connection: relaunch the browser and re-navigate the last URL."""
        url = self._urls.get(context)
        await self._reset(context)
        if not url:
            return False
        try:
            b = await self._browser(context)
            tab = await asyncio.wait_for(b.get(url), self._cfg.nav_timeout_s)
            self._tabs[context] = tab
            await self._settle(tab)
            return True
        except Exception:  # noqa: BLE001
            return False

    async def _settle(self, tab) -> None:  # noqa: ANN001
        """Wait for the page to finish loading, then dismiss a cookie/consent banner."""
        for _ in range(12):
            try:
                if await tab.evaluate(_READY_JS) == "complete":
                    break
            except Exception:  # noqa: BLE001 - connection may be settling
                break
            await asyncio.sleep(0.4)
        try:
            await tab.evaluate(_ACCEPT_COOKIES_JS)
            await asyncio.sleep(0.6)  # let any gated widget load after consent
        except Exception:  # noqa: BLE001
            pass

    async def _tab(self, context: str):  # noqa: ANN202
        tab = self._tabs.get(context)
        if tab is None:
            raise RuntimeError(f"no open page in the {context!r} browser — open a URL first")
        return tab

    async def _where(self, tab) -> tuple[str, str]:  # noqa: ANN001 - (url, title)
        return (await tab.evaluate(_URL_JS) or ""), (await tab.evaluate(_TITLE_JS) or "")

    async def _run(self, context: str, fn) -> dict:  # noqa: ANN001
        """Run fn(tab) → dict, recovering once from a dropped CDP connection."""
        try:
            return await fn(await self._tab(context))
        except Exception as exc:  # noqa: BLE001
            if not _is_conn_error(exc):
                return {"ok": False, "error": str(exc)}
        if not await self._recover(context):
            return {"ok": False, "error": "browser connection was lost and could not be recovered — open the page again"}
        try:
            return await fn(await self._tab(context))
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}

    async def open(self, url: str, context: str) -> dict:
        if not (url or "").strip():
            return {"ok": False, "error": "no url"}
        if "://" not in url:
            url = "https://" + url
        for attempt in (1, 2):
            try:
                b = await self._browser(context)
                tab = await asyncio.wait_for(b.get(url), self._cfg.nav_timeout_s)
                self._tabs[context] = tab
                self._urls[context] = url
                await self._settle(tab)
                cur, title = await self._where(tab)
                return {"ok": True, "context": context, "url": cur, "title": title}
            except Exception as exc:  # noqa: BLE001
                if attempt == 1 and _is_conn_error(exc):
                    await self._reset(context)  # stale browser → relaunch fresh and retry
                    continue
                return {"ok": False, "error": f"couldn't open {url}: {exc}"}
        return {"ok": False, "error": f"couldn't open {url}"}

    async def snapshot(self, context: str) -> dict:
        async def fn(tab):  # noqa: ANN001, ANN202
            listing = await tab.evaluate(_SNAPSHOT_JS)
            nframes = await tab.evaluate(_FRAMES_JS) or 0
            cur, title = await self._where(tab)
            elements = listing or "(no interactive elements found)"
            if nframes:
                elements += (
                    f"\n({int(nframes)} embedded frame(s) on this page — controls inside them "
                    "aren't listed yet; if the form you need is in one, tell the user.)"
                )
            return {"ok": True, "context": context, "url": cur, "title": title, "elements": elements}

        return await self._run(context, fn)

    async def _resolve(self, tab, ref: int):  # noqa: ANN001, ANN202
        try:
            return await tab.select(f'[data-jref="{ref}"]', timeout=5)
        except Exception:  # noqa: BLE001 - not found / timed out
            return None

    async def click(self, ref: int, context: str) -> dict:
        async def fn(tab):  # noqa: ANN001, ANN202
            el = await self._resolve(tab, ref)
            if el is None:
                return {"ok": False, "error": f"no element [{ref}] — snapshot again, the page may have changed"}
            await el.click()
            await asyncio.sleep(0.6)
            cur, title = await self._where(tab)
            return {"ok": True, "url": cur, "title": title}

        return await self._run(context, fn)

    async def type(self, ref: int, text: str, context: str, *, submit: bool = False) -> dict:
        async def fn(tab):  # noqa: ANN001, ANN202
            el = await self._resolve(tab, ref)
            if el is None:
                return {"ok": False, "error": f"no element [{ref}] — snapshot again"}
            await el.send_keys(text)
            if submit:
                # nodriver's documented way to press Enter is sending the newline
                # keystroke; the JS form-submit is a belt-and-braces fallback for boxes
                # that ignore it. (Some single-page search widgets need neither — they
                # react to typing — so re-snapshot afterwards to see what changed.)
                await el.send_keys("\n")
                with contextlib.suppress(Exception):
                    await tab.evaluate(_submit_js(ref))
                await asyncio.sleep(1.4)  # let the navigation/search happen
            cur, title = await self._where(tab)
            return {"ok": True, "url": cur, "title": title}

        return await self._run(context, fn)

    async def read(self, context: str) -> dict:
        async def fn(tab):  # noqa: ANN001, ANN202
            text = await tab.evaluate(_TEXT_JS)
            cur, title = await self._where(tab)
            return {"ok": True, "url": cur, "title": title, "text": text or ""}

        return await self._run(context, fn)

    async def aclose(self) -> None:
        for b in self._browsers.values():
            try:
                b.stop()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass
        self._browsers.clear()
        self._tabs.clear()
        self._urls.clear()
