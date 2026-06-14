# Jarvis — Phase 1 (all-local voice assistant)

Wake on "Jarvis" → transcribe (local) → answer via a hosted LLM (through a
gateway) → speak with streaming cloud TTS → interruptible mid-sentence →
remembers across conversations. Everything co-located on one machine in
Phase 1; built so the heavy tier can move to a remote server in Phase 2 by
changing env vars only.

## Two hard constraints (spec §3)
1. **Everything talks over a network boundary**, even on localhost. Hosts/ports
   are env-driven (`GATEWAY_HOST`, `MEMORY_HOST`, `DB_HOST`, …). No in-process
   shortcuts. Phase 2 = swap those vars to Tailscale hostnames.
2. **The hot path never blocks on a memory write.** Hot path reads a *local
   cache*; memory writes + background reasoning are fire-and-forget cold path.

## Layout
```
src/jarvis/
  config.py          # all config from env / .env, defaults to localhost
  cli.py             # `jarvis config` dry-run
  audio/             # always-open mic + streaming playback + hard stop
  gateway_client/    # HTTP -> LiteLLM proxy (OpenAI-compatible)
  memory_client/     # HTTP -> Honcho; hot=local cache, cold=write/refresh
  turnloop/          # 5-state machine (PASSIVE/ACTIVE/THINKING/SPEAKING/INTERRUPTED)
```

## Runtime
Python is pinned to **3.12** (`.python-version`) via `uv`. The system default
3.14 lacks wheels for the locked ML stack (torch / ctranslate2). This is a
runtime-version pin, not a stack substitution — all spec §4 choices stand.

## Quickstart
```bash
uv sync                       # base deps (Step 0)
cp .env.example .env          # then fill in secrets
uv run jarvis config          # dry-run: prints resolved config (Step 0 gate)
```

Per-step extras install as each build step is reached, e.g.
`uv sync --extra gateway`, `--extra tts`, `--extra stt`, `--extra vad`,
`--extra wake`.

## Build status (spec §6)
- [x] Step 0 — skeleton + env-driven config (`jarvis config`)
- [x] Step 1 — LiteLLM gateway up; `fast`+`strong` routes return completions
      (`jarvis ping-gateway`), streaming verified. Routes live via OpenRouter;
      OpenAI-direct routes configured pending that account's billing.
- [x] Step 2 — Inworld streaming TTS (`inworld-tts-2`); `jarvis say` plays
      before synthesis completes (~700-850ms TTFA warm), clean audio; barge-in
      cut ~56ms (`--stop-after`), in-flight request cancelled. Callback player
      with deque buffer + blocksize 2048 + silence pre-roll (device has only
      ~20ms hardware buffer).
- [x] Step 3 — local Faster-Whisper STT (`distil-large-v3`, CPU int8). Live mic
      confirmed (transcribed a spoken question correctly). `jarvis listen`.
      Watch latency (~2.7s/3s audio). Single-stdin-reader push-to-talk helper.
- [x] Step 4 — minimal turn loop `jarvis chat`: 3-turn live run confirmed
      (STT→gateway→streaming TTS, per-turn fast/strong routing). Single-stdin
      push-to-talk helper.
- [x] Step 5 — Silero VAD endpointing (900ms trailing silence; confirmed live
      in `jarvis run`). `--manual` push-to-talk fallback in `chat`.
- [x] Step 6 — wake word `jarvis run`: full state machine PASSIVE→ACTIVE→
      THINKING→SPEAKING via single always-open mic (MicStream). openWakeWord
      "hey_jarvis" (FOSS, no account); Porcupine selectable. Confirmed live.
      Wake acknowledgement (AUDIO_ACK_MODE: beep | speak | none).
- [x] Step 7 — barge-in: during SPEAKING a monitor cuts playback (~56ms) +
      cancels in-flight TTS, then re-enters ACTIVE (not PASSIVE). Default mode
      `wakeword` (say "Hey Jarvis" to interrupt — robust without AEC; confirmed
      live). `vad` mode available (needs AEC mic/headphones). Toggle via
      VAD_BARGEIN_ENABLED / --no-bargein.
- [x] Step 8 — Honcho v2.0.3 stack (api+deriver+pgvector+redis) in compose;
      all LLM features route through LiteLLM via the `custom` provider (verified
      in gateway logs). Memory client uses the raw /v2 REST API. Recall via the
      cold-path dialectic (Neil/sailing fact stored + retrieved). DB on :5433
      (5432 taken by alice-postgres).
- [x] Step 9 — memory wired into `jarvis run`: hot path injects the local
      cached representation (~0.03ms read); cold path is a detached task (write
      → deriver → refresh) that never blocks. Confirmed live: cross-session
      recall + mid-conversation fact updates. Plus conversation mode (follow-up
      window so you don't re-wake each turn, VAD_CONVERSATION_MODE).
- [ ] Step 10 — Phase 1 acceptance
