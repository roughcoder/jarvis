# Jarvis — Phase 3 Spec: One Brain, Many Intercoms

> Phase 3 turns Jarvis from a single-Mac voice loop into **one brain with many
> bodies and hands**: it speaks through multiple devices and channels (rooms,
> Macs, WhatsApp), acts through tools and a worker machine, knows *who* it's
> talking to, and can reach out on its own. It is built entirely on the two
> Phase 1 constraints — that's what makes it a small set of additions, not a
> rewrite. Build it against the **local** brain now; the Frankfurt relocation
> (Phase 2) stays an independent env swap, done whenever.

This is the "non-voice surfaces / multi-user household peers" work that
`PHASE2.md` §7 anticipated and deferred — and that justifies Frankfurt being a
proper always-on tier rather than "Postgres, relocated."

## 1. The model, in one picture

There is **one Jarvis** (the *brain*). Every device is a thin *intercom* — a
microphone and a speaker that phones home. The intercom holds no intelligence
and no credentials; the brain holds all of it. **What Jarvis will do depends on
two things on every request: who is speaking, and which device they're speaking
from.** Adding a room is installing an intercom, not training a new assistant.

```
                       ┌──────────────────────────────────────┐
                       │               THE BRAIN              │
                       │   (Python server; localhost now,     │
                       │    Frankfurt later — one env swap)   │
                       │   LLM gateway · memory · tools ·     │
                       │   skills · SOUL · identity/profiles ·│
                       │   heartbeat scheduler                │
                       └───────────────┬──────────────────────┘
            WebSocket (audio/text up; audio/text + proactive push down)
        ┌───────────────┬──────────────┼──────────────┬───────────────────┐
   ┌─────────┐    ┌──────────┐    ┌──────────┐   ┌──────────┐      ┌──────────────┐
   │ room Pi │    │ your Mac │    │ WhatsApp │   │ worker   │      │ cloud lanes  │
   │ intercom│    │ intercom │    │ connector│   │ Mac      │      │ Claude Code  │
   │ (room   │    │ (full    │    │ (number  │   │ daemon   │      │ web · MCPs   │
   │  scope) │    │  scope)  │    │  =person)│   │(peekaboo)│      │              │
   └─────────┘    └──────────┘    └──────────┘   └──────────┘      └──────────────┘
```

## 2. The two hard constraints still rule

- **Network boundary everywhere (constraint #1).** Intercoms, the worker
  daemon, connectors, STT/TTS services, and MCP servers are all just more
  boundary peers reached at a `host:port` from config. Nothing new is violated;
  the boundary is the *substrate* that makes this possible.
- **The hot path never blocks (constraint #2).** Voice latency stays sacred.
  Heartbeat, deep-work dispatch, async channels, heavy multi-MCP synthesis, and
  memory writes all live on the cold/async path. Prompt caching (see §11) cuts
  TTFT. A room intercom answering "what's the weather" never waits on any of it.

## 3. Topology & stack

**Language verdict: Python for the brain and ML services; any language for the
hands; thin clients for the intercoms — the boundary is what lets them mix.** No
rewrite of existing code.

| Tier | What it is | Language | Notes |
|---|---|---|---|
| Brain | orchestrator, identity, profiles, tool dispatch, gateway+memory clients, heartbeat | **Python** | Was a single-process loop in P1; **now a long-running server** exposing a WebSocket API. |
| STT / TTS | speech↔text services | **Python** (ML) | Run as `host:port` services. Capable boxes run them locally; weak devices reach them over the LAN. This is what gets Jarvis onto a cheap Pi. |
| Intercom | wake + audio I/O + WebSocket client | **Python via `uv` now; native Rust/Go later** | Thin. The torch/Whisper weight left the device when STT/TTS became services. |
| Tools | the hands | **polyglot CLIs** | wacli (Go), peekaboo/AXorcist (Swift), gogcli (Go) — invoked, never reimplemented. |
| Worker | local Mac control daemon | Swift/Python | Wraps peekaboo/AXorcist behind a dispatch API. |
| Gateway | LiteLLM | (irrelevant) | Unchanged. Talked to over HTTP only. |

**Transport.** Each intercom holds one persistent **WebSocket** to the brain:
audio/text up, audio/text down, and the brain pushes proactively (heartbeat)
down the same socket. One mechanism for channel I/O *and* proactivity.

**A device = which services run local vs remote.** Your Mac runs STT/TTS/wake
locally (lowest latency, self-contained). A Pi runs only wake + audio locally
and reaches STT/TTS/LLM/memory over the LAN. *Same code, different `.env`.*

**Credential boundary (do not blur this).** "Runs STT/TTS locally" means the
credentialed *service* is co-located on that machine — **never** that the thin
intercom embeds provider keys. Provider credentials (TTS/LLM/`google`/MCP) live
only in the brain or a credentialed service; the intercom holds **no** provider
keys and authenticates **only** to the brain/service with its pairing token. So
the existing in-process TTS key (`tts/__init__.py`) is correct *because that
client is brain/service-side* — it must never ship inside an intercom build.

**Device lifecycle (the intercom CLI):**

```
jarvis install     # uv tool install (now)        → download binary (later)
jarvis login       # PAIR to the brain: token + which profile this device is
jarvis run         # open the WebSocket, become an intercom
jarvis upgrade     # uv tool upgrade (now)         → self-update binary (later)
jarvis status      # brain reachable? what am I allowed to do?
```

`login` is a **pairing** step (like wacli linking as a WhatsApp device): the
device authenticates so a random Pi can't connect, and is told which capability
profile it is. Keep the WebSocket protocol language-neutral so the Python client
and a future Rust client are drop-in interchangeable — extract the native client
only when the fleet/Pi reality makes the rewrite worth it.

## 4. The resolution stack

Every request — voice or text — flows down this stack. The layers are not an
abstraction; they are composable files (§7) assembled per turn.

```
Channel/intercom  →  the request arrives, stamped {device, raw identity}
  Identity        →  WHO is speaking?  (know, or ask — never guess)
  Scope           →  personal or house?
  Credentials     →  whose accounts? (speaker's own, else the house's)
  Capabilities    →  is this allowed for (who × where)?
  Tools/MCP/Skills→  the action
  Memory          →  the speaker's peer (+ a shared house peer)
```

## 5. Identity & credentials (multi-user)

Identity doesn't just gate what's allowed — it decides *whose accounts* the
request runs against. Jarvis has its **own** accounts (the *house* principal);
each human has theirs.

**The rule: know, or ask. Never guess.**

- **Strong channel** (your own Mac, your WhatsApp) → identity known → that
  user's scope and credentials.
- **Unknown speaker** → **house scope, Jarvis's own accounts.** No personal data.
- **Personal request on an uncertain channel** → Jarvis **asks "who am I talking
  to?"** — and only asks when the request actually needs personal scope (general
  questions never trigger it).

**Trust tiers (v1):** strong identity → full personal scope · claimed identity
("it's Jules" on the shared Pi) → family-grade scope · unknown → house only.
**Scope is gated by identity confidence:** a shared room mic gets house scope
only unless Jarvis can strongly confirm the speaker. This dissolves the
voice-speaker-ID problem — biometric ID becomes a *future upgrade that reduces
how often it asks*, never a prerequisite.

**Two hard invariants:**

1. **Per-user credential isolation is a privacy wall.** Jules's tokens are never
   used for Neil's request, and her data never surfaces to him — even though
   they share one Jarvis. Same weight as the memory/personality separation.
2. **House and personal are distinct principals.** Jarvis "as itself" (house
   account) is a different actor from Jarvis "on behalf of Jules." Never blur them.

Per-user config lives in `users/<name>.md` (channels + credential **bindings** —
references, never secrets; secrets stay in `.env`/a vault). House is the default
principal: unknown or family/shared → house.

## 6. The v1 tool surface

Deliberately small. OpenClaw is a menu to shop from, not a kit to clone.

**Every tool declares a required capability and runs behind a deny-by-default
gate** — the `RequestContext`→capability check from §4 stands in front of all of
them, present from 3a (§12) before the first tool that touches an account or the
filesystem. A tool with no granted capability does not run, single-principal or not.

**Atomic tools (hand-built, few):**

| Tool | Source | Status |
|---|---|---|
| `web-search` | Tavily wrapper (provider-configurable) | ✅ built |
| `files` | fs-safe pattern (root-bounded read/list/write) | ✅ built |
| `worker.*` | coding/shell/screenshot dispatch → worker daemon (§8) | ✅ built |
| `remote.*` | cloud coding → Claude Managed Agents (§8) | ✅ built, dormant |
| `email/calendar` | provider-neutral surface; current adapter is gogcli for Jarvis's house Gmail + Calendar | ✅ built |

**MCP bridge (✅ built):** a native MCP client (`mcp/`, isolation-first — imports
nothing from the brain) + a profile-gated **work bundle**. Each configured server
(`MCP_SERVERS`, mirroring a Claude-Code `mcpServers` entry — `stdio` or `http`) is
connected once at brain startup (cold path), its tools discovered, namespaced
`<server>_<tool>`, and registered gated by `mcp.<server>`. **The profile is the
firewall against sprawl** — a device sees a server's tools only if its profile
grants `mcp.<server>`, so "lots of MCPs" never becomes "400 tools every turn"
(plus a per-server `include` allow-list + `max_tools_per_server` cap). **Every
call is hard-bounded twice** (the registry's `tools.timeout_s` and the bridge's
`mcp.call_timeout_s`) so no bridged call can hang the hot path. Probe with
`jarvis mcp`. The tool layer (`tools/mcp.py`) is a thin client over the bridge,
exactly as `tools/worker.py` is over the worker daemon. Connections are keyed per
server now; per-`(user, service)` keying lands with multi-user (3d). Heavy
multi-MCP synthesis still belongs on skills/heartbeat (cold).

**OAuth (http servers).** A server with no static headers authenticates via
OAuth 2.0 (Notion, Granola, Linear, M365). Interactive auth happens ONLY in
`jarvis mcp login` — it walks the OAuth servers one at a time, opens the browser,
catches the redirect on a localhost loopback, and caches the token per server
under `<MCP_AUTH_DIR>/<server>.json` (gitignored). The **brain never pops a
browser**: at startup it builds a *headless* provider that silently refreshes a
cached token, or — if fresh auth is needed — fails fast and the bridge skips that
server with a "run `jarvis mcp login`" hint. The SDK supplies PKCE + dynamic
client registration + metadata discovery; we supply only the token store and the
loopback (`mcp/auth.py`).

**Lanes:** coding deep-work via the worker daemon (§8, ✅ built) · WhatsApp
channel + heartbeat (⬜ 3b) · `mac-control` (peekaboo/AXorcist, ⏸ deferred).

**Not building now:** the ~40 OpenClaw project-internal tools; the crawler
family (a maybe-someday memory feature); spogo/goplaces/clawpdf/rastermill/
remindctl/imsg/clawdex (cheap to add later if missed). Tachikoma too — LiteLLM
is the router.

## 7. Skills — composed, self-authored

**Tools stay few and atomic; skills are many, composed, and mostly written by
Jarvis itself.** This is what keeps the tool core tiny while behaviour grows.

A skill is a markdown recipe (same spirit as `SOUL.md`): **name / when-to-use ·
recipe · allowed tools · params.** "World Cup update" = `web-search` →
summarise → speak / drop a note via `files`. No new tool.

- **Creation:** explicit now ("save that as a skill"); emergent later (the cold
  path notices a recurring ask and proposes one). Both run **off the hot path**.
- **Selection:** the model matches the request against skill *descriptions* —
  cheap, like tool selection, so latency is untouched.
- **Safety invariant:** a skill can only compose tools the current capability
  profile already grants. It cannot invent powers, so self-authoring is safe by
  construction.
- **Storage:** a `SKILLS.md` index + `skills/*.md` bodies — the same one-line
  index pattern Jarvis already uses for `MEMORY.md`.

## 8. Coding lanes (deep work) — as built

Jarvis kicks off autonomous coding via the **worker daemon** (`jarvis worker`,
Phase 3c): a standalone HTTP service the brain dispatches to (boundary peer,
token-authed), gated by `worker.code` / `worker.shell`. The brain tool is a thin
HTTP client; the daemon imports nothing from the brain.

**Local — the default (built).** The worker runs a coding agent headless on the
worker Mac: **Codex** (`codex exec`) or **Claude** (`claude -p`), chosen per job.
- Repo jobs run on an **isolated git worktree + branch** (`jarvis/<name>-<id>`),
  never your checkout — you review the branch and merge. No-repo jobs get their
  own `runs/<name>-<id>/` scratch dir.
- Repos are **named, not pathed** (`WORKER_REPO_ROOT`); an unknown name is cloned
  with `gh`. Jobs are **named, persisted to disk** (`<workspace>/jobs/`), and
  checkable / listable / cleanable by name (`jarvis jobs`, or by voice), each
  linked to the agent's session (`codex resume <id>`).
- **Cost + visibility (why it's the default):** both CLIs run on your *existing
  Codex/Claude subscriptions*, and results are visible through Jarvis (the jobs
  list, the worktree branch, `codex resume`). Subscription-billed and
  Jarvis-visible.

**Remote (cloud) — built but dormant.** `start_remote_coding_job` (gated
`remote.code`) dispatches to **Claude Managed Agents** (`POST /v1/sessions`;
`jarvis remote-setup` creates the agent + environment). It works, but a hard
three-way trade-off surfaced — you can have any *two* of {Jarvis-triggered,
subscription-billed, app-visible}, not all three:

| Path | Jarvis-triggered | Billing | Visible in Claude app |
|---|---|---|---|
| **Managed Agents** (built) | ✅ clean API | pay-as-you-go | ❌ console only |
| **Claude Code on the web** | ❌ app-launched | subscription | ✅ |
| **Local worker** (default) | ✅ | subscription | ✅ via `jarvis jobs` |

So the remote lane stays **off** (no `ANTHROPIC_AGENT_ID`) — it's API-cost +
console-only, a worse deal than local for everyday use. Keep it for *programmatic
cloud* only; flip one config value if Anthropic ships a subscription-billed
Sessions API.

**Local machine control (deferred).** `shell` / `applescript` / `screenshot`
already work (macOS built-ins). Rich GUI automation (peekaboo + AXorcist) needs a
`brew install` + Screen-Recording/Accessibility permissions — the action pattern
is ready for it.

## 9. Session, caching & hygiene

Borrowed from OpenClaw's session-management reference — but Jarvis is turn-based
with memory externalised to Honcho, so take three ideas and reject the heavy
compaction apparatus.

- **Cache-ordered prompt assembly (a latency win).** Order the layered files
  stable→volatile so the prefix is an Anthropic cache hit every turn, cutting
  TTFT: `SOUL → device profile → user profile → skills` ─ *cache breakpoint* ─
  `memory cache → recent turns → current utterance`. The file-composition order
  *is* the caching strategy. Cache is keyed per `(device × user)`; a handful of
  warm prefixes for a household. Add cache hit/miss to `jarvis traces`.
  *Status:* the stable→volatile ordering is in place and offered tool schemas are
  name-sorted (canonical → cacheable). **Deferred:** explicit Anthropic
  `cache_control` breakpoints, tool-block cache markers, and cache hit/miss in
  `jarvis traces` (needs usage plumbing through `gateway_client`, incl. streaming
  `include_usage`) — verify against the live gateway.
- **Tool relevance prefilter (a latency win with many MCP tools).** With several
  MCP servers a turn can face 100+ tool schemas; the prefilter (`tools/selection.py`,
  `TOOLS_RELEVANCE_FILTER`) offers only the servers relevant to the utterance
  (built-ins always on), keeping every tool registered + gated. *Status:* built,
  keyword-based. **Deferred:** replace the keyword matcher with embedding similarity
  (the keyword heuristic is brittle — tune per-server `keywords` meanwhile).
- **Transcript hygiene.** Background/system events (heartbeat, cold-path writes,
  dispatch notifications) **never enter the conversational transcript** that
  feeds the voice prompt — the hot/cold split extended into context. Keep
  tool-call+result paired when trimming the per-channel history window.
- **Session management — stay lean.** Continuous Honcho externalisation already
  beats OpenClaw's pre-compaction memory flush; don't adopt the flush.
  Compaction is a *fallback* only for long WhatsApp threads (summarise-old/keep-
  recent), default off for voice. **Adopt a silent-completion sentinel** (their
  `NO_REPLY`): a heartbeat that decides nothing's worth saying produces no output
  and never streams a partial — essential for a *speaking* assistant.

Config knobs (into `config.py`, per convention): context cap, `reserve_tokens`,
per-channel `history_window`, `compaction_enabled` (off for voice), the silent
sentinel. No floors, no branch summaries.

**Restructure note (single-principal → per-context).** Today's state is global:
one cache file (`.cache/representation.json`), one `voice` session, one rolling
`_history`. The brain split must make each **per-`(device × user)`** — a cache
key, a session, and a history window per context, plus a shared house context —
with isolation tests written as that lands (§11). Single-principal is the
characterised baseline until then; do not let it leak across users once channels
multiply.

## 10. Repo & project layout

The split that matters is **engine vs instance**, not "Jarvis vs Jarvis-Extra".

**Engine — one monorepo** (`jarvis/`, the existing repo, restructured). One repo
because the brain and intercom must agree on the WebSocket protocol; polyglot is
fine because components talk over the boundary, not a shared build.

```
jarvis/                    ← THE ENGINE (one repo, polyglot)
  protocol/                  WebSocket contract — shared by brain + intercom
  brain/                     Python: orchestrator, identity, profiles, dispatch, clients, heartbeat
  intercom/ py/  rs/         thin client — Python now, Rust later, same protocol
  services/                  Python ML services: stt/, tts/
  tools/                     ONLY tools we author: web-search, files, google, mcp-bridge
  worker/                    macOS worker daemon (wraps peekaboo/AXorcist)
  connectors/                whatsapp (wraps wacli), …
  deploy/  docs/
```

- External tools (wacli, peekaboo, AXorcist, gogcli) are **dependencies, not
  vendored** — installed via brew/go, referenced by config.
- The Python tier is a **`uv` workspace** (`brain`, `intercom/py`, `services`,
  `tools` as members); `intercom/rs` and `worker/` carry their own toolchains.
- The current `src/jarvis/` cleaves along the boundary: `audio/wake/vad/stt/tts`
  → intercom + services; `gateway_client/memory_client/tracing/config` → brain;
  `turnloop` splits in two with the protocol as the seam. This is a re-draw, not
  a rewrite.
- **Don't over-split:** keep the intercom in the monorepo until the Rust client
  wants its own release cadence; extract then, with `protocol/` published as a
  shared schema. The boundary makes that cheap.

**Instance — a separate private repo/dir** (`jarvis-workspace/`, the "soul and
memory", runtime-mutable, holds user data + credential bindings — never in the
engine repo):

```
jarvis-workspace/          ← THE INSTANCE (private, runtime-mutable)
  SOUL.md                    personality (authoritative)
  HEARTBEAT.md               proactive checklist
  SKILLS.md  skills/*.md     self-authored skills (Jarvis writes here as it runs)
  profiles/<device>.md       capability profiles (≈ OpenClaw TOOLS.md, scoped per device)
  users/<name>.md            per-user context + credential bindings (≈ USER, one per person)
  MEMORY.md  memory/         local memory cache (long-term in Honcho)
  .env                       secrets, per-deployment (gitignored)
```

Drop OpenClaw's `BOOT.md`/`BOOTSTRAP.md` — Jarvis's startup is deterministic
code (`config.py`), not an LLM-read prompt file. A turn assembles its context by
layering these files down the resolution stack (§4), which is also the cache
order (§9).

## 11. Testing

The boundary architecture is, conveniently, very testable. Two tiers (`tests/`):

- **Unit** (`tests/unit`, default, runs in <1s): pure logic. `uv run pytest`.
- **Integration** (`tests/integration`, opt-in): real gateway / memory / TTS /
  STT, latency budgets, and the dead-boundary readiness check. Marked
  `integration`, self-skip when a dependency is absent. `uv run pytest
  --run-integration`.

**In place now** (characterising current Phase 1 behaviour): conversation-end
detection, streaming segmentation, config resolution, the memory hot/cold
boundary (the PHASE2 readiness check, automated), VAD endpointing, tracing.

**Added as each Phase 3 layer lands** (does not exist yet — write the test *with*
the code, TDD): identity/scope/capability gating, cache-ordered prompt assembly,
protocol (de)serialisation, and per-`(device × user)` cache/session/history
isolation. The boundary is the **mock seam** (test each side against a faked
counterpart); the shared `protocol/` gets **contract tests** both clients check
against; the two hard constraints become **assertions** (hot read works at a
dead boundary; the hot path never awaits the network). Always write the test
before moving the code — characterise, restructure, prove the cleave held.

## 12. Build status

**Enforcement before capability** — the privacy/capability wall is built *before*
any tool that can touch an account or the filesystem, even while there's only one
user. The identity stack is not deferred to 3d; only its *multiplicity* is.

- ✅ **3a — Brain-as-server + capability spine + tools.** WebSocket brain server +
  thin intercom (`jarvis brain` / `jarvis run`, plus `--local`), a deny-by-default
  capability gate + `RequestContext`, and the atomic tools `web-search` + `files`.
  The gate exists from the first capability-bearing tool — nothing predates the wall.
- ✅ **3c — Worker daemon (built ahead of 3b).** Local codex/claude coding jobs
  with git-worktree isolation, repo resolve-or-clone, on-disk persistence, names,
  cleanup (§8); plus the remote Managed-Agents lane (built, dormant). `jarvis
  worker` / `jarvis jobs` / `jarvis remote-setup`.
- ✅ **MCP bridge.** A native MCP client (`mcp/`, stdio + streamable-HTTP) that
  connects configured servers at startup, discovers + namespaces their tools, and
  registers them gated by `mcp.<server>` with double-bounded call timeouts. Config
  is `MCP_SERVERS` (JSON, mirrors a Claude-Code `mcpServers` entry); the thin tool
  layer is `tools/mcp.py`; probe with `jarvis mcp`. Proven end-to-end against the
  context7 stdio server. The work bundle (gh / Granola / Notion / Slack / Linear)
  is now just config — add a server entry + grant its `mcp.<name>` capability.
  OAuth http servers (Notion/Granola/Linear/M365) are authorized once with
  `jarvis mcp login` (browser loopback, tokens cached + auto-refreshed; the voice
  path never pops a browser) — see `mcp/auth.py`.
- ✅ **3d — Multi-device + multi-user (the resolution stack, fully populated).**
  Per-utterance identity resolution (`brain/identity.py`: strong/claimed/unknown,
  know-or-ask) from `users/<name>.md`; per-`(device × user)` sessions
  (`brain/contexts.py`) with isolated history + memory peer/cache; per-device
  profiles + identity-aware pairing (`BRAIN_DEVICES`, `authorise_device`, `jarvis
  status`); per-user MCP credentials (`.mcp-auth/<user>/`, `jarvis mcp login
  --user`). Room-Pi profile + `docs/PI.md`. Wired in both the brain server and the
  `--local` loop. Isolation + gating + pairing unit-tested; loopback integration.
- ✅ **Skills (§7).** `SKILLS.md` + `skills/*.md`, selected like tools, run via a
  bounded gated tool loop, self-authored with `save_skill` — and a skill can never
  exceed its profile's powers (`extra_capabilities` invariant). `brain/skills.py`.
- ✅ **3b — WhatsApp connector + heartbeat.** `connectors/whatsapp.py` (wraps
  `wacli`; `jarvis whatsapp`) — inbound message → brain turn (channel=whatsapp,
  number→user), reply back out. Cold-path heartbeat (`brain/heartbeat.py` +
  `HEARTBEAT.md`) with the silent-completion sentinel + Proactive broadcast. Live
  WhatsApp self-skips until `wacli` is linked.
  - **Access + groups (deny-by-default).** DMs gated by `WHATSAPP_DM_POLICY`
    (`allowlist` | `pairing` | `open` | `disabled`); groups gated by
    `WHATSAPP_GROUP_POLICY` (`ignore` | `mention` | `open`) — under `mention` it
    replies only when called out by `WHATSAPP_TRIGGER` ("Jarvis, …").
  - **Remote onboarding (`dm_policy=pairing`).** An unknown DM-er gets a holding
    reply and the admin (`WHATSAPP_ADMIN`, sole approver, no auto-approve) gets
    `approve <code> <name>` / `deny <code>`. Approval calls `add_whatsapp_number`,
    which **creates or merges** `users/<name>.md` (own personal scope + Honcho
    peer, existing file preserved/idempotent). The brain hot-reloads `users/*.md`
    on the cold-path tick (`_maybe_reload_users`), so a freshly-paired user is
    recognised without a restart.
  - **Channel-aware replies.** The system prompt picks its format by
    `ctx.channel`: voice gets the spoken rules + TTS cues, messaging surfaces
    (WhatsApp, the text console) get written prose (`_MESSAGING_FORMAT` — normal
    numerals/dates, light WhatsApp formatting, no [cues]); open-mic end-detection
    is voice-only.
- ✅ **Email/calendar tool (current adapter: gogcli / Gmail+Calendar).**
  `tools/google.py`, gated by provider-neutral `email.read`, `email.send`, and
  `calendar.read`; `jarvis google-setup`. Self-skips without gogcli.
- ✅ **`mac-control` (peekaboo).** `control_mac` (the autonomous agent) + `look_at_screen`
  (read-only native vision) worker tools gated `worker.gui`; `jarvis worker --doctor`.
  Self-skips without peekaboo + perms. (The earlier atomic see/click/type tools were
  removed — GUI control is agent-only so the model can't bypass it and flail.)
- ✅ **WS8 polish.** Embedding-based relevance (opt-in `TOOLS_RELEVANCE_MODE=
  embedding`, keyword fallback) + prompt-cache hit/miss in `jarvis traces`
  (cached/prompt tokens via gateway usage; cache-friendly stable prefix).

## 13. Invariants to keep true

- **SOUL.md is authoritative for personality; memory is user-scoped and
  subordinate** (carried from Phase 1).
- **Per-user credential isolation is a privacy wall**; house and personal are
  distinct principals (§5).
- **A skill cannot exceed the capabilities of the profile it runs under** (§7).
- **Identity is known or asked, never guessed; personal scope requires
  confidence** (§5).
- **No work MCP or dispatch ever lands on the voice hot path** (§2, §9); every
  bridged call has a timeout.
- **The intercom holds no intelligence and no credentials** — the brain does
  (§1). Provider credentials (TTS/LLM/`google`/MCP) live only in the brain or a
  credentialed service; an intercom authenticates only to the brain via pairing
  token (§3).
- **No capability-bearing tool predates the deny-by-default gate** — the gate
  exists from 3a, before any tool that touches an account or the filesystem (§12).
- **Secrets live in `.env`/a vault, never in instance markdown** (§10).

## 14. What Phase 3 is NOT

- Not an OpenClaw clone. ~6 tools + a gated MCP bridge, not a 60-tool store.
- Not "build every connector" — bridge the MCPs you already run; hand-build only
  `web-search`, `files`, `google`.
- Not dependent on voice biometrics — "know or ask" ships multi-user safely first.
- Not a rewrite — the brain stays Python; the existing code re-draws along the
  boundary it already enforces.
- Not Frankfurt-dependent — built on the local brain; relocation is a later,
  independent env swap.
