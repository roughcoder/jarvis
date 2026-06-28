# Testing Jarvis — a follow-along strategy

Work top-to-bottom. It's ordered by how much provisioning each step needs:
**Tier 0** automated (no setup), **Tier 1** works on your Mac now, **Tier 2** needs
a one-time OAuth, **Tier 3** needs external installs / hardware. Each step says what
to run and the **pass criteria**.

Run everything with `uv run …` from the repo root.

---

## Tier 0 — Automated (the gate, ~1 min)

```bash
uv run ruff check src/ tests/          # style/lint
uv run pytest -q                       # unit — fast, no services
uv run pytest --run-integration -q     # + live gateway/memory/TTS/STT/loopback
```

**Pass:** ruff clean; unit all green; integration green with only env-gated tests
self-skipping (`wacli` always; others skip only if a service/key is down). Re-run
these after any change — they're the regression net.

What the unit suite already proves (no manual work): identity resolution + trust
tiers (`test_identity`), per-`(device×user)` isolation (`test_contexts`), per-device
pairing (`test_pairing`), server identity routing (`test_server_identity`), per-user
MCP credential isolation (`test_mcp_isolation`), the skills cap-subset invariant
(`test_skills`), heartbeat silent-sentinel (`test_heartbeat`), WhatsApp routing
(`test_whatsapp`), tool relevance (`test_tool_selection` / `test_embedding_relevance`),
google/mac-control gating, the MCP bridge round-trip (`test_mcp_client`).

---

## Tier 1 — Works on your Mac right now (no extra setup)

### 1.1 Config sanity
```bash
uv run jarvis config
```
**Pass:** prints resolved config, secrets masked `<set>/<unset>`, no traceback. Spot-check
`capabilities.users_dir`, `brain.devices`, `tools.relevance_mode`, `mcp.servers`.

### 1.2 MCP probe (stdio servers, no OAuth)
```bash
uv run jarvis mcp                 # house view
uv run jarvis mcp --user neil     # neil's view
```
**Pass:** connects context7 + obsidian (stdio); prints tools grouped by capability.
OAuth servers (granola/notion/linear) report "no authorized user / run jarvis mcp
login" — expected until Tier 2.

### 1.3 Single-process voice loop + identity
```bash
uv run jarvis run --local
```
- Say **"Hey Jarvis, what's the time?"** → spoken answer. **Pass:** STT→reply→TTS works.
- Watch the console: `⚙ tool: …  [cap]  args` lines appear when a tool fires (set
  `TOOLS_LOG_CALLS=false` to silence). `⚙ tools: offered N/M` shows the relevance
  prefilter narrowing.
- **Identity (Neil):** your Mac (`local-mac`) is bound to Neil in `users/neil.md`, so
  the loop resolves to **neil / personal** automatically. Say "remember my favourite
  colour is teal." **Pass:** after the turn a file `.cache/representation-neil.json`
  appears (his memory peer), not the base `representation.json`.

### 1.4 Claimed identity + the privacy wall (the 3d headline)
A *personal* device always resolves to its owner, so to exercise a **shared**
device + voice claim locally, pretend the Mac is the room Pi for one run:
```bash
CAPS_DEVICE_ID=room-pi uv run jarvis run --local
```
- Plain question ("what's the weather?") → **house** scope (unknown speaker).
- Say **"It's Jules — what do you know about me?"** → resolves to **jules / personal**.
**Pass:** check `.cache/representation-jules.json` is separate from Neil's; Jules's
turn never reads Neil's memory. (This is the privacy wall — two principals, two caches.)

### 1.5 Skills
With `skills.run` granted on `local-mac`:
- Ask **"Give me a news briefing on <topic>."** **Pass:** the `news_briefing` skill
  runs (you'll see it compose `web_search` in the tool log) and speaks a summary.
- Self-author: **"Save that as a skill called morning news."** **Pass:** a new
  `jarvis-workspace/skills/morning_news.md` appears and `SKILLS.md` is updated.

### 1.6 Worker (local coding lane — uses your codex/claude subscription)
```bash
uv run jarvis worker        # terminal A (daemon)
uv run jarvis run --local   # terminal B
```
- Ask **"Start a coding job to add a hello function in <repo>."** then **"check the
  coding job."** **Pass:** `uv run jarvis jobs` lists it on an isolated
  `jarvis/<name>-<id>` branch; the daemon logs the run.

### 1.7 Brain + intercom (the two-process split)
```bash
uv run jarvis brain         # terminal A
uv run jarvis status        # terminal B — reachable? what am I allowed to do?
uv run jarvis fleet-status --json --no-docker   # operator/toolbar status contract
uv run jarvis run           # terminal B — connect as an intercom over WebSocket
```
**Pass:** `status` prints identity/scope/capabilities; `run` pairs and behaves like 1.3
but over the WebSocket. Try a barge-in (say "Hey Jarvis" while it's speaking) → it stops.

### 1.8 Per-device pairing (token binding)
In `.env`: `BRAIN_DEVICES=[{"token":"t-pi","device_id":"room-pi"}]`, restart `jarvis brain`.
```bash
INTERCOM_TOKEN=t-pi CAPS_DEVICE_ID=room-pi uv run jarvis status   # → paired, house scope
INTERCOM_TOKEN=wrong CAPS_DEVICE_ID=room-pi uv run jarvis status   # → rejected
INTERCOM_TOKEN=t-pi CAPS_DEVICE_ID=local-mac uv run jarvis status  # → rejected (token bound to room-pi)
```
**Pass:** the three outcomes above (a leaked device token can't impersonate another device).

### 1.9 Traces
```bash
uv run jarvis traces -n 10
```
**Pass:** per-turn STT/LLM/TTS timings; with caching active the `llm` stage shows
`cached_tokens`/`prompt_tokens`.

### 1.10 Text console (the headless harness)
```bash
uv run jarvis brain                                  # one pane
uv run jarvis text --once "what's two plus two?"     # another → prints the reply
uv run jarvis text                                   # interactive REPL (or pipe stdin)
```
**Pass:** replies come back with no mic/STT/TTS. Proactive pushes (alarms, background
results, heartbeat) print `🔔 …` as they arrive, even between turns.

### 1.11 Alarms & proactive notifications
```bash
uv run jarvis brain                                  # alarms/notify run here
# Over voice (jarvis run) or text (jarvis text):
#   "set a timer for thirty seconds"   → fires + REPEATS until acknowledged
#   (while ringing) "stop" / "dismiss" → "Alarm off." (no wake word needed on voice)
#   "in the background, <slow task>, and let me know" → "on it", then a spoken result
```
**Pass:** the timer rings on the setting device, repeats on the ring/quiet cadence, and
"stop" silences it; a background task says "on it" then notifies (and on voice opens the
mic for a reply). Tune `ALARM_*` / `NOTIFY_*` in `.env`. (How the tone/voice *sound* is
the one human-verified bit.)

### 1.12 Hot-path / dead-boundary invariant (constraint #2)
```bash
MEMORY_PORT=1 uv run jarvis run --local
```
**Pass:** turns still answer (hot path reads the local cache); only the cold-path
write/refresh fails cleanly at the boundary — no hang, no crash.

---

## Tier 2 — One-time OAuth (a browser, ~5 min)

### 2.1 MCP work bundle (Notion / Granola / Linear), per user
```bash
uv run jarvis mcp login --user neil      # walks each OAuth server: SPACE to authorize
uv run jarvis mcp --user neil            # re-probe: should now list their tools
```
**Pass:** browser opens per server, token saved to `.mcp-auth/neil/<server>.json`;
the brain then offers those tools when Neil speaks. Ask **"What are my open Linear
issues?"** in `jarvis run --local` → routed under Neil's token (tool log shows
`[mcp.linear]`). Jules: `jarvis mcp login --user jules` — her tokens never serve Neil.

### 2.2 Heartbeat (proactive)
In `.env`: `HEARTBEAT_ENABLED=true`, `HEARTBEAT_INTERVAL_S=60`. Put a checkable item in
`jarvis-workspace/HEARTBEAT.md`. Run `jarvis brain` + an intercom.
**Pass:** when the checklist warrants it, a Proactive message is pushed after the
interval; when it doesn't, **nothing** is sent (silent-completion sentinel). Verify
heartbeat output never appears in the next turn's conversation transcript.

---

## Tier 3 — External installs / hardware (provision, then test)

Each is code-complete; its integration test self-skips until the dependency exists.

### 3.1 WhatsApp connector — needs `wacli` linked
```bash
uv run jarvis worker --doctor        # (sanity pattern for external tools)
# install + link wacli, then:
uv run jarvis whatsapp
```
**Pass:** a WhatsApp message to the linked number → Jarvis replies; the sender's
number resolves to a user via `users/<name>.md` (`whatsapp:` binding).
`uv run pytest --run-integration tests/integration/test_whatsapp_live.py` stops skipping.

### 3.2 email/calendar tool — needs `gogcli` + OAuth
```bash
# install gogcli, then:
uv run jarvis google-setup           # browser OAuth for the house account
```
**Pass:** ask **"What's on my calendar this week?"** → `upcoming_events` runs (tool log
`[calendar.read]`). `send_email` only fires with `email.send` granted.

### 3.3 mac-control — needs peekaboo + permissions
```bash
brew install peekaboo                 # then grant Screen Recording + Accessibility
uv run jarvis worker --doctor         # → peekaboo_installed: true
```
Grant `worker.gui` in the device profile, then ask Jarvis to **"look at my screen"**
(`look_at_screen`, read-only vision) or **"open Safari and …"** (`control_mac`, the
autonomous agent — the only way to act on the GUI). **Pass:** doctor reports ready; the
GUI action runs.

### 3.4 Room Pi — second device (see docs/PI.md)
On the brain: add the Pi to `BRAIN_DEVICES`. On the Pi: `uv sync --extra stt --extra
vad --extra wake`, set `INTERCOM_*` + `CAPS_DEVICE_ID=room-pi`, then `jarvis status`
and `jarvis run`. **Pass:** the Pi pairs (house scope), reaches STT/TTS/LLM over the
LAN, and a voice claim ("it's Jules") upgrades scope for that conversation — while
your Mac, simultaneously, stays Neil with a separate session + memory.

### 3.5 Browser lane — needs the `[browser]` extra + Chrome (see docs/BROWSER.md)
```bash
uv sync --extra browser               # installs nodriver
uv run jarvis worker                  # hosts the browser (BROWSER_HEADLESS=false to watch)
uv run pytest --run-integration tests/integration/test_browser_*.py   # real Chrome, controlled pages
```
Grant `worker.browser` in the device profile, then (voice/text) ask **"open Wikipedia
and tell me who built Polesden Lacey."** **Pass:** the integration tests go green; Jarvis
opens a real Chrome, navigates, reads, and answers (it uses the browser rather than
deferring). Live bookings hit login/captcha — the human-handoff wall, by design.

---

## Invariants to actively try to break

- **Privacy wall:** as Jules, ask for "my notes" — you must never get Neil's data or
  have his token used. Two principals → two memory caches, two `.mcp-auth/<user>/`.
- **Deny-by-default:** remove a capability from `profiles/local-mac.md` (e.g.
  `mcp.linear`) → that tool isn't even offered; the model can't call it.
- **Hot path never blocks:** Tier 1.10. Also confirm an MCP/web call shows the soft
  "still working" pulse rather than silence, and a barge-in cuts speech <~100ms.
- **Skill ≤ profile:** a skill that lists a tool you haven't granted simply isn't
  offered (it can't escalate).
- **Identity is known-or-asked, never guessed:** on a shared device, a personal
  request with an unknown speaker makes Jarvis ask "who am I talking to?" rather than
  assume.

## When something fails
`jarvis traces` for per-turn timings; `TOOLS_LOG_CALLS=true` for tool/MCP activity;
`jarvis config` to confirm what's actually resolved; `jarvis mcp --user <x>` /
`jarvis status` for connectivity + grants. Re-run Tier 0 to localise a regression.
