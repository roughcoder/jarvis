# AGENTS.md — Jarvis

Guidance for AI agents (and humans) working in this repo. Read this before
making changes.

## What this is

Jarvis is an **all-local voice assistant**: wake on "Hey Jarvis" → transcribe
locally → answer with a hosted LLM via a gateway → speak with streaming cloud
TTS → interruptible mid-sentence (barge-in) → remembers across conversations.
Phase 1 runs everything on one Apple Silicon Mac. It is built so the heavy tier
(memory + DB + gateway) can move to a remote server in Phase 2 by changing env
vars only. The decision-locked spec is the "Phase 1 Build Spec" the project was
started from; honour it.

## Two hard constraints (do not violate)

1. **Everything talks over a network boundary, even on localhost.** Every call
   to the memory service, database, and LLM gateway goes over HTTP to a
   `host:port` from env (defaults to localhost). No in-process imports of those
   services, no "same machine so call it directly" shortcuts. Phase 2 = change
   `MEMORY_HOST` / `DB_HOST` / gateway host env vars to Tailscale hostnames,
   nothing else.
2. **The hot path never blocks on a memory write.** A turn has a *hot path*
   (user is waiting: wake → capture → STT → read LOCAL cached memory → LLM →
   stream TTS) and a *cold path* (fire-and-forget after the reply: write the
   turn to memory → background reasoning → refresh the local cache). The in-turn
   memory read MUST hit the local cache file, never a live reasoning/dialectic
   call.

## Architecture

Phase 3 restructured `src/jarvis/` into engine subpackages along the network
boundary (see `docs/PHASE3.md`). Modules are unchanged — only their homes moved.

```
src/jarvis/
  config.py            all config from env / .env (pydantic-settings); nothing hardcoded
  cli.py               entry point: jarvis <command>
  intercom/            the thin edge (wake, capture, playback) — runs on each device
    audio/             always-open mic (MicStream) + streaming player (hard-stop barge-in)
    vad/  wake/        Silero VAD + Endpointer; wake word (openWakeWord/Porcupine)
  services/            stt/ (Faster-Whisper) + tts/ (Inworld) — in-process under the brain for 3a
  brain/               orchestration tier
    server.py          WebSocket brain server (intercoms connect here)
    turnloop/          single-process loop (jarvis run --local); 5-state machine
    session.py         BrainSession — shared think/speak core (prompt, tools, end-detect)
    dialog.py          pure text helpers (prompt fragments, end-detection, segmentation)
    context.py / capabilities.py   RequestContext + deny-by-default capability gate (§4)
    identity.py / contexts.py      who's speaking (trust tiers, users/) + per-(device×user) sessions (3d §5/§9)
    skills.py          self-authored recipes composing gated tools (§7)
    heartbeat.py       proactive cold-path scheduler (silent sentinel, §3b)
    scheduler.py       alarms & timers: fire on the setting device, repeat (ring/quiet) until acknowledged ('stop')
    proactive.py / tones.py   server-initiated voice delivery: tone + spoken text frames for alarms/notifications (idle-aware hold, quiet hours, device + WhatsApp routing)
    background.py      fire-and-forget lane: 'on it' now, run detached (asker's caps, no recursion), report the outcome via the proactive push
    gateway_client/    HTTP -> LiteLLM proxy; memory_client/ Honcho (per-user peer); tracing/
  protocol/            brain<->intercom WebSocket message schemas (Phase 3 W4)
  tools/               capability-gated tools: web_search, files, worker, remote, google, mcp, background, browser, alarm (+ selection prefilter)
  mcp/                 native MCP client + bridge (stdio/http, per-user OAuth) -> gated tools
  worker/              standalone deep-work daemon (jarvis worker) — codex/claude, worktrees, jobs, GUI, + browser host
  browser/             self-contained Chrome-over-CDP host (nodriver, no Playwright); embedded in the worker, extractable; two contexts (device | jarvis)
  remote/              Claude Managed Agents client (cloud coding lane, dormant)
  connectors/          non-voice channels: whatsapp/ (wraps wacli) + text.py (terminal console, the headless harness) — bridge to the brain over the protocol
```

Keep the turn loop, audio I/O, memory/gateway clients, the worker, and the remote
client as **separate modules with config-driven URLs / boundary peers** — this is
what makes the tiers independently movable (Phase 2 relocation; Phase 3
brain/intercom split; the worker & remote talk over HTTP, importing nothing from
the brain).

### Services (docker-compose.yml)

- `litellm` — the LLM gateway (OpenAI-compatible). The turn loop talks ONLY to
  this, never a provider SDK. Has its own `litellm` Postgres DB for the admin UI
  + request tracing.
- `honcho-api` + `honcho-deriver` + `honcho-db` (pgvector) + `honcho-redis` —
  the memory service. Honcho's LLM reasoning routes back through LiteLLM (the
  `custom` provider) — no local model.

## Setup & run

Python is pinned to **3.12** via `uv` (`.python-version`). The system default
(3.14) lacks wheels for torch / ctranslate2.

```bash
uv sync --extra gateway --extra tts --extra stt --extra vad --extra wake --extra memory
cp .env.example .env            # fill in secrets (see below)
docker compose up -d            # litellm + honcho stack
./deploy/litellm/setup-attribution.sh    # create gateway team/users/keys (once per DB)
uv run jarvis config            # dry-run: prints resolved config
uv run jarvis run               # the hands-free loop
```

Secrets to put in `.env` (gitignored): `OPENAI_API_KEY`, `OPENROUTER_API_KEY`,
`TTS_API_KEY` (Inworld). Wake word (openWakeWord) needs no key.

## CLI commands

| Command | Purpose |
|---|---|
| `jarvis config` | print resolved config (env-driven, secret-masked) |
| `jarvis ping-gateway [--route R]` | test LLM gateway routes |
| `jarvis say "text" [--stop-after S]` | streaming TTS + hard-stop demo |
| `jarvis listen [--rounds N]` | push-to-talk STT |
| `jarvis chat [--manual] [--rounds N]` | push-to-talk / VAD round-trip |
| `jarvis brain` | run the brain WebSocket server (Phase 3 W4) |
| `jarvis run [--no-bargein] [--local] [--brain H:P]` | hands-free loop: thin intercom → brain (`--local` = one process) |
| `jarvis worker [--doctor]` | run the deep-work daemon (codex/claude jobs, shell, screenshot, GUI); `--doctor` checks peekaboo |
| `jarvis whatsapp` | run the WhatsApp connector (bridge wacli ↔ brain, 3b) |
| `jarvis text [--once MSG]` | text console: drive the brain from the terminal (no mic/STT/TTS); the headless dev + test harness |
| `jarvis status` | is the brain reachable + what is this device allowed to do? (§3) |
| `jarvis google-setup` | one-time OAuth for the google tool (gogcli) |
| `jarvis jobs [-n N] [--prune]` | list worker jobs (name, status, branch, `codex resume`); `--prune` cleans finished |
| `jarvis remote-setup` | one-time: create the cloud agent + environment for the (dormant) remote lane |
| `jarvis mcp` | probe configured MCP servers: discover their tools + the capability each needs |
| `jarvis mcp login [--server N] [--user U]` | one-time interactive OAuth for http MCP servers (browser); tokens cached per user |
| `jarvis traces [-n N]` | view per-turn pipeline timings |

Phase 3 (see `docs/PHASE3.md`) split the loop into a **brain server** + thin
**intercom** clients over a WebSocket protocol (`jarvis brain` + `jarvis run`),
with a deny-by-default capability gate and a tool layer. `jarvis run --local`
keeps the original single-process behaviour. The think/speak core is shared
(`brain/session.py`, `BrainSession`); the edge (mic/wake/VAD/playback) lives in
`intercom/`.

### Further docs
- `docs/BROWSER.md` — the browser lane (CDP/nodriver, real pointer + keyboard, iframes,
  reliability) + the `jarvis text` headless harness.
- `docs/NOTIFICATIONS.md` — alarms/timers + proactive voice delivery, multi-channel
  routing, idle-aware timing.
- `docs/PHASE3.md` — Phase 3 status/spec; `docs/PHASE2.md` — relocation; `docs/PI.md` —
  room device; `docs/TESTING.md` — how to test each surface.

## Conventions

- **All config from env**, via `config.py` (pydantic-settings, one class per
  concern). Add a field there + an `.env.example` line; never hardcode hosts,
  ports, keys, or model names. `jarvis config` must keep working.
- **Lint with `uv run ruff check src/`** before committing. Keep comments at the
  density of the surrounding code; explain *why*, not *what*.
- **Latency is the product.** Felt speed = STT + time-to-first-token + TTS-start.
  Keep memory and routing off the critical path; when in doubt, move work to the
  cold path. Per-turn timings are in `jarvis traces` / `.cache/traces.jsonl`.
- **Tunables live in config** (endpoint silence, barge-in mode/threshold, model
  routing, TTS voice/delivery, history window, refresh interval, etc.).
- **Tracing/memory must never break a turn** — they're best-effort and guarded.

## Gotchas (learned the hard way)

- **Audio playback**: the Mac speakers expose only ~20ms of hardware buffer, so
  the streaming player uses `blocksize=2048` + a silence pre-roll + a deque
  buffer (never concatenate a growing array under the realtime-callback lock).
  CallbackAbort = barge-in; CallbackStop = clean drain.
- **Honcho is /v2; the honcho-ai SDK is /v3** — mismatch, so `memory_client`
  uses the raw `/v2` REST API. Honcho's hot `/representation` endpoint is empty
  until ~20 messages, so `refresh_cache` uses the dialectic (`/chat`) on the
  COLD path only. All Honcho LLM features use the `custom` (OpenAI-compatible)
  provider pointed at `http://litellm:4000/v1`.
- **Barge-in self-trigger**: with no AEC mic, default barge-in is `wakeword`
  mode (say "Hey Jarvis" to interrupt) so Jarvis's own voice can't trip it. Per
  spec, do NOT build software AEC.
- **openWakeWord** pulls a Linux-only `tflite-runtime` with no cp312 wheel →
  `pyproject.toml` restricts `[tool.uv] environments = ["sys_platform ==
  'darwin'"]` (we use the ONNX backend).
- **Conversation-end detection** is a 3-layer hybrid (model `[[END]]` marker +
  deterministic user sign-off net + Jarvis-reply-farewell backstop) — see
  `turnloop/__init__.py`. It's transcript-driven; add new failure phrases to the
  test cases.
- **MCP bridge lifecycle**: an MCP session holds anyio cancel scopes that MUST be
  entered AND exited on the same task — and several servers' scopes interleave on
  one task and fail to close independently (the symptom: "exit a cancel scope that
  isn't the current task's current cancel scope" on the *second* server's
  teardown). So each server runs in its **own dedicated runner task** that holds
  the SDK's `async with` blocks open for the connection's life (`mcp/client.py`
  `_run`); `aclose()` just cancels that task, so enter/exit are always same-task.
  Tool *calls* come from any turn task — the SDK's streams tolerate that. The `mcp`
  SDK is imported lazily, so a brain with `MCP_ENABLED=false` (or no servers) needs
  neither the extra nor any startup cost. Servers connect on the COLD path
  (startup), never the hot path; every call is timeout-bounded twice (registry +
  bridge). A chatty server (Obsidian alone exposes ~39 tools) can dominate the
  per-turn tool list — use a per-server `include` allow-list or lower
  `MCP_MAX_TOOLS_PER_SERVER` if it gets heavy.
- **TTS-2 expressive prompting**: ONE steering directive `[say ...]` at the
  START (scopes the whole call), non-verbals `[laugh]` inline; numbers as words;
  no markdown/emoji. Streaming reuses the leading steering tag per sentence.

## Invariants to keep true

- **SOUL.md is authoritative for personality; memory is strictly user-scoped and
  subordinate.** Honcho scopes memory to the *user* peer; the system prompt
  injects it as "what you know about the **user**", never "who you are". The
  deriver summarises the user, not Jarvis. Don't let memory blur into
  personality — keep these on separate rails (this is already true by
  construction; the note is so it stays true).
- **Phase 2 readiness: `MEMORY_HOST` is the only load-bearing migration var on
  Hive.** Jarvis never connects to Postgres directly (only Honcho does, via its
  compose-internal URI), so the DB moves *with* Honcho as a unit — `DB_HOST` in
  the app's `.env` is effectively vestigial. Readiness test (run before any
  migration): point memory at a dead boundary and confirm the hot-path cache
  read still works while the cold-path write/refresh fails cleanly at the
  boundary (ConnectError, not a hang, not a silent half-success):
  `MEMORY_PORT=1 uv run python -c "..."` — see the cold-path methods in
  `memory_client`. If anything on the hot path needs the network, there's a
  co-location shortcut to find before migrating.
- **Watch cold-path/VAD contention in the follow-up window.** The cold path
  (memory write + deriver, and the debounced dialectic) fires right as
  conversation mode opens the mic for the next utterance — the most
  compute-active moment coincides with the most noise-sensitive one. On a Mac,
  Docker shares the host CPU, so a deriver burst can starve the VAD loop. If
  real-room testing shows follow-up twitchiness, **check the trace timeline
  first** (`jarvis traces` — a slow turn overlapping a `(cold path)` line =
  contention) before reaching for `VAD_SPEECH_THRESHOLD` / `VAD_MIN_SPEECH_MS`.
  Don't tune the symptom if the cause is contention.

## Memory for agents

Durable, non-obvious project decisions live in the Claude memory file
(`~/.claude/projects/.../memory/jarvis-phase1-decisions.md`). Check it for stack
choices, key values, and the reasoning behind deviations from the spec.
