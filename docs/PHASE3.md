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

| Tool | Source | Lane | Runs on |
|---|---|---|---|
| `web-search` | **build** (Brave/Tavily/Exa) | cloud | brain |
| `files` | fs-safe pattern (root-bounded) | local | device/worker |
| `google` | gogcli — Jarvis's own Gmail + Calendar | cloud | brain |

**MCP bridge (v1 core, disciplined):** a native MCP client + a profile-gated
**work bundle** (Granola, Notion, Slack, Linear). Connections are keyed by
`(user, service)`. The profile is the firewall against sprawl — each profile
sees only its slice, so "lots of MCPs" never becomes "400 tools every turn".
**Timeout every call.** Consume fast lookups inline (hot); push heavy multi-MCP
synthesis to skills/heartbeat (cold).

**Lanes (decided, build alongside):** WhatsApp channel + heartbeat · `code-dispatch`
(§9) · `mac-control` (peekaboo/AXorcist via the worker daemon).

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

## 8. Dispatch lanes (deep work)

Two distinct lanes; do not conflate them.

**Cloud coding — vendor-agnostic dispatch.** Jarvis is a *dispatcher*: it creates
a task and hands you a session URL to watch/steer in the vendor's app.
- **Start with Claude Code on the web** — it has the real HTTP API today
  (Sessions API `POST /v1/sessions`; **Routines**, each a saved prompt+repos+
  connectors bundle with its own bearer-token `/fire` endpoint returning a live
  session URL). Routines *are* the "small reusable feature" model.
- **Codex is pluggable behind the same interface** (via `codex exec` / app-server
  now; its cloud task API when it ships). You like watching agents in the Codex
  app — keep that as an alternative backend, not the starting point.
- The `code-dispatch` tool is one stateless HTTP call → returns a URL → Jarvis
  WhatsApps it to you. A perfect boundary-everywhere citizen.

**Local machine control — the worker daemon.** peekaboo + AXorcist on a
dedicated Mac, behind a dispatch API. The cloud can't touch a real Mac's GUI;
this is the genuine "its own Mac." Approvals route back over a channel.

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

## 12. Build order

Each step is small and independently useful; the full system emerges. Never hold
all of it in your head at once.

**Enforcement before capability** — the privacy/capability wall is built *before*
any tool that can touch an account or the filesystem, even while there's only one
user. The identity stack is not deferred to 3d; only its *multiplicity* is.

1. **3a — Brain-as-server + the capability spine, _then_ the tools.** First a
   minimal `RequestContext` (who / where / scope — single-principal to start) and
   a **deny-by-default capability gate**: no tool runs without an explicit grant.
   *Only then* add `web-search`, `files`, `google`, each registered against a
   required capability. One user, one device, house principal — but the gate
   exists from the first capability-bearing tool, so nothing predates the wall.
2. **3b — WhatsApp connector + heartbeat.** A real second channel → the first
   exercise of the know-or-ask identity rule and per-user credential resolution.
3. **3c — Worker Mac daemon + `code-dispatch`.** Jarvis gets hands.
4. **3d — Room Pi + per-device profiles + a second user (Jules).** Multi-device,
   multi-person — the resolution stack now *fully populated* (enforcement built in
   3a; the multiplicity of profiles, users, and credentials added here).

The MCP **work bundle** slots into 3a/3b as your real work stack comes online,
gated to your work profile.

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
