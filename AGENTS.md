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

```
src/jarvis/
  config.py        all config from env / .env (pydantic-settings); nothing hardcoded
  cli.py           entry point: jarvis <command>
  audio/           always-open mic (MicStream) + streaming player (hard-stop barge-in)
  gateway_client/  HTTP -> LiteLLM proxy (OpenAI-compatible); per-turn model routing
  stt/             local Faster-Whisper transcription
  vad/             Silero VAD + Endpointer (endpointing AND barge-in, one instance)
  wake/            wake word: openWakeWord (default) or Porcupine, same interface
  tts/             Inworld streaming TTS -> raw PCM
  memory_client/   Honcho /v2 REST over httpx; hot=local cache, cold=write+refresh
  tracing/         per-turn pipeline traces (STT/LLM/TTS/memory timings)
  turnloop/        the 5-state machine: PASSIVE→ACTIVE→THINKING→SPEAKING→(INTERRUPTED)
```

Keep the turn loop, audio I/O, memory client, and gateway client as **separate
modules with config-driven URLs** — this is what makes the tiers independently
movable in Phase 2.

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
| `jarvis run [--no-bargein]` | the hands-free wake-word loop |
| `jarvis traces [-n N]` | view per-turn pipeline timings |

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
