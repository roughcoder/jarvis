# Browser lane + text harness

How Jarvis acts on the web like an assistant — and how to drive/test it headlessly.
Built to match the experience of OpenClaw and Hermes (act by default, persist down a
ladder of hands, hand off only at a real wall), keeping `control_mac` as the native-GUI
hand.

## The ladder of hands

A request that needs the world picks the cheapest capable hand and persists down:

1. **`web_search`** — read-only facts.
2. **browser** — *interactive* web: availability, forms, logins, bookings, reading a page.
3. **`control_mac`** — native macOS GUI (apps that aren't web).
4. **ask** — only at a genuine wall (login / 2FA / payment / captcha / destructive).

The persona (`SOUL.md` §Agency + the `_AGENCY` prompt fragment) makes "act, don't
advise" the default and forbids "phone them / do it yourself" when a tool can do it.
Slow multi-step work routes through `run_in_background` (`_BACKGROUND_GUIDANCE`) so the
conversation never blocks.

## Browser lane (nodriver, no Playwright)

A real Chrome driven over CDP. `src/jarvis/browser/` is self-contained (imports nothing
from `brain/`): the **worker hosts it**, the brain acts on it over HTTP — so it lifts to
its own daemon/package/repo by changing a URL. `nodriver` is behind the `[browser]`
extra, lazy-imported; the `worker.browser` capability gates every browser tool
(deny-by-default), granted per device in `profiles/<device>.md`.

**Two device-scoped contexts** (a `context` arg on each tool; default from
`BROWSER_DEFAULT_CONTEXT`):
- **`jarvis`** — his own headed, persistent profile (his accounts; zero setup). Headed +
  persistent so you can take the wheel for a login/captcha and it sticks.
- **`device`** — the machine's default Chrome profile (its real logins). Live-attach to an
  open Chrome needs it launched with `--remote-debugging-port`; the profile-reuse flavour
  needs Chrome closed.

**Tools** (the snapshot → act → read loop): `browser_open(url)` ·
`browser_snapshot()` (lists interactive elements, each a `[ref]`) · `browser_click(ref)` ·
`browser_type(ref, text, submit)` · `browser_press(keys, ref?)` (keyboard) ·
`browser_read()` (page text). Refs are tagged onto the DOM (`data-jref`) so a stale ref
fails cleanly ("snapshot again"), never mis-clicks.

**Acting like a real user — mouse OR keyboard (general, not site-specific):**
- **Real pointer clicks** — `browser_click` dispatches a genuine CDP mouse sequence
  (move → press → release) at the element's centre, not a synthetic DOM `.click()`. Many
  modern widgets (React-Aria/MUI dropdowns, custom comboboxes) only open on
  `pointerdown`/`mousedown`; this fires them.
- **Keyboard** — `browser_press` sends real key events (Enter, Tab, Arrows, Escape,
  Space…), optionally focusing an element first — for the many widgets that open via
  keyboard, not click (focus a combobox + ArrowDown).

**Reliability (learned from live runs):**
- **Dead-connection recovery** — a dropped CDP connection is detected and the browser
  relaunched + re-navigated once, instead of every call failing.
- **Wait for client-render** — after load/consent, the host waits for the interactive
  element count to settle, so a React/Next.js widget (e.g. a Zonal/Guestline booking
  form) is present before the first snapshot. A click that opens a popover also waits for
  it to render.
- **Cookie/consent** — auto-dismisses a consent banner (many widgets won't load until you
  accept).
- **Cross-origin iframe traversal** — the snapshot walks the main page **plus every
  connectable frame** (OOPIFs), tagging global refs; click/type route back into the right
  frame transparently.
- **Graceful shutdown / stale-lock clear** — the worker stops Chrome on SIGTERM (no
  orphans), and a stale profile lock is cleared before launch.

Setup: `uv sync --extra browser` + Google Chrome. Check with the worker
(`browser_doctor`). Config: `BROWSER_*` in `.env.example`.

## Text harness — `jarvis text`

The brain already does text turns (`TextIn` → `ReplyText`); `jarvis text` is a terminal
client that sends `TextIn(text_only=True)` so the brain **skips TTS** — no mic, no STT, no
TTS key. Interactive REPL, piped stdin, or one-shot:

```
jarvis text                       # REPL (or pipe stdin)
jarvis text --once "open file:///…/x.html and read the code"   # scriptable, asserted
```

It's how the whole brain (persona, tools, background, browser) is driven and verified
headlessly — stand up `jarvis brain` + `jarvis worker` (in cmux/tmux panes), then drive
`jarvis text`. Proactive pushes (background completions, heartbeat) print as they arrive.

## Verified

Airtight end-to-end via the harness (a random token only readable by actually browsing):

```
you:    open file:///…/proof.html and tell me the exact secret code
jarvis: The secret code is: ZARK-1fd697f1-QX
        (brain log: browser_open [worker.browser] → browser_read [worker.browser])
```

Live integration tests (self-skip without the `[browser]` extra/Chrome):
- `test_browser_live.py` — open/snapshot/read/click on example.com.
- `test_browser_iframe.py` — cross-origin iframe: list + click an element inside an OOPIF.
- `test_browser_click.py` — a real pointer click fires pointerdown/mousedown/mouseup.
- `test_browser_keys.py` — `browser_press` delivers real key events.

## Known follow-ups

- **Same-origin nested iframes**: cross-origin frames (OOPIFs) are traversed; same-origin
  iframes (rarer for third-party widgets) aren't yet.
- **Per-device `browser_default`**: the global `BROWSER_DEFAULT_CONTEXT` is active; a
  per-device override in the profile front-matter isn't wired yet.
- **`device` live-attach**: a `jarvis browser attach-setup` to relaunch the machine's
  Chrome with the debug port (for "see my current tabs") isn't built — `jarvis` context
  works with zero setup.
- **Stubborn commercial widgets**: most React-Aria flows work via the pointer/keyboard
  primitives + the render wait; a given booking widget's final step may still need a
  bespoke nudge (and real bookings hit login/captcha — the human-handoff wall, by design).
