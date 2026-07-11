# Jarvis — Phase 1 (all-local voice assistant)

Wake on "Jarvis" → transcribe (local) → answer via a hosted LLM (through a
gateway) → speak with streaming cloud TTS → interruptible mid-sentence →
remembers across conversations. Everything co-located on one machine in
Phase 1; built so the heavy tier can move to a remote server in Phase 2 by
changing env vars only.

## Two hard constraints (spec §3)
1. **Everything talks over a network boundary**, even on localhost. Hosts/ports
   are env-driven (`GATEWAY_HOST`, `MEMORY_HOST`, `DB_HOST`, …). No in-process
   shortcuts. Phase 2 = swap those vars to Twingate/private-network hostnames.
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
Development and Homebrew installs use **Python 3.12** (`.python-version`) via
`uv`. Raspberry Pi installs may use Raspberry Pi OS Bookworm's Python 3.11 so
Linux wake-word wheels remain installable. The system default 3.14 lacks wheels
for the locked ML stack (torch / ctranslate2). This is a runtime-version pin,
not a stack substitution — all spec §4 choices stand.

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
- [x] Step 10 — Phase 1 acceptance: hands-free wake→ask→answer→idle, natural
      endpointing, barge-in, cross-session recall, no felt memory latency,
      per-turn routing, all env-driven (Phase-2 readiness test passes).

## Beyond the spec (added during Phase 1)

- Conversation mode (follow-ups without re-waking) + voice-controlled ending
  (model `[[END]]` + deterministic sign-off net + reply-farewell backstop)
- Voice modes: default short-task behavior plus stay mode for persistent spoken
  sessions until an explicit exit
- Soul (`SOUL.md` personality) + shared conversation context
- Emotional speech (Inworld TTS-2 steering) and **streamed sentence-by-sentence
  replies** (lower time-to-first-audio)
- Latency: `small.en` STT (~0.8s), debounced memory refresh
- Full LiteLLM attribution (team / key / internal user / end-user=speaker /
  room tag) + per-turn pipeline tracing with a wall-clock timeline

## Docs

- **[AGENTS.md](AGENTS.md)** — architecture, hard constraints, conventions,
  gotchas, invariants (read first; `CLAUDE.md` imports it).
- **[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md)** — install-first deployment path
  for the iMac brain, Mac workers/intercoms, Raspberry Pi intercoms, Homebrew,
  pairing, and updates.
- **[docs/BRINGUP.md](docs/BRINGUP.md)** — physical fleet acceptance checklist
  for iMac, laptop, Raspberry Pi, private-network, and update proof.
- **[docs-site/](docs-site/)** — static deployment preview published by the
  GitHub Pages workflow from `main`.
- **[docs/PUBLIC_RELEASE.md](docs/PUBLIC_RELEASE.md)** — public repository
  readiness gates and required scans.
- **[docs/HOUSEHOLD_COMMS.md](docs/HOUSEHOLD_COMMS.md)** — provider-neutral
  household email/calendar permissions, onboarding, and adapter plan.
- **[docs/PHASE2.md](docs/PHASE2.md)** — the Frankfurt migration spec; the
  readiness test is its entry gate.
- **[docs/FLEET.md](docs/FLEET.md)** — iMac / laptop / Pi service deployment,
  status contract, and update shape.
- **[docs/DOGFOOD.md](docs/DOGFOOD.md)** — unreleased review-ring deployment,
  rollback, live two-model PR-review acceptance, and the PR/release gate.
- **[docs/VOICE_MODES.md](docs/VOICE_MODES.md)** — default vs stay mode, exit
  behavior, and temporary identity lifetime for voice conversations.
- **[docs/SWIFT_TOOLBAR_SPEC.md](docs/SWIFT_TOOLBAR_SPEC.md)** — handoff spec for
  the separate native macOS menu bar app.

## Deployment Direction

Development uses `uv` directly. Fleet deployment should not require cloning this
repo or understanding `uv`: install the `jarvis` runtime package, install the
`jarvis-app` native app, choose roles in the app, pair devices, and let Homebrew
own runtime/app updates.

Fresh Mac install:

```bash
curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/install_mac.sh | bash
```

The installer uses Homebrew internally, prepares `~/.jarvis`, clears app
quarantine while Jarvis is ad-hoc signed, and opens the app.

Clean uninstall for fresh end-to-end testing:

```bash
curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/main/scripts/uninstall_mac.sh | bash
```

Physical bring-up evidence:

```bash
jarvis bringup --json --role brain --role api --role worker --role intercom --hardware \
  --brain-host imac.private --output ~/Desktop/jarvis-bringup-evidence
jarvis bringup-summary ~/Desktop/jarvis-bringup-evidence \
  --expect-role brain --expect-role api --expect-role worker --expect-role intercom \
  --expect-current-release --min-files 4 \
  --output ~/Desktop/jarvis-bringup-evidence/jarvis-fleet-summary.json
```

## Runtime Release

Runtime releases are published **only** by the `Release` workflow in GitHub
Actions. Do not run `scripts/release_runtime.sh` locally; the script refuses to
publish outside that workflow because local runs can create version, tag,
release asset, and Homebrew formula mismatches.

Run the workflow with:

- `draft`: whether the GitHub release should remain draft
- `skip_homebrew`: whether to skip the tap update

The workflow computes the runtime version automatically from Conventional
Commits (based on the latest `vX.Y.Z` tag), then builds a
`jarvis-<version>.tar.gz` source archive, publishes the GitHub Release, uploads
release artifacts, and updates `roughcoder/homebrew-infinite-stack` when
`skip_homebrew` is false.

Release notes are generated from commits in the release range. The generator
uses commit subjects for categorisation, commit trailers for user-facing detail,
and `.env.example` diffs for newly added or removed env vars. If
`OPENAI_API_KEY` and the repository variable `JARVIS_RELEASE_NOTES_MODEL` are
configured, the workflow asks an AI model to polish the notes; otherwise it
publishes the deterministic summary from the same facts. Set
`JARVIS_RELEASE_NOTES_AI=always` when you want release creation to fail instead
of falling back.

Release-note quality is checked before publication. `feat(...)`, `fix(...)`,
and `perf(...)` commits in the release range must include either
`Release-note: <user-facing text>` or `Release-note: skip`; missing trailers fail
the release. Generated notes also fail if raw commit-scope bullets such as
`voice: ...` leak into the output or if a change section is too noisy to scan.

Workflow inputs are now limited to:

- `draft`: whether the GitHub release should remain draft
- `skip_homebrew`: whether to skip the tap update

Versioning rules:

- `feat(...)` increments minor
- `fix(...)`, `chore(...)`, `docs(...)`, `style(...)`, `refactor(...)`,
  `test(...)`, `perf(...)`, `build(...)`, `ci(...)`, `revert(...)` increment patch
- `!` in the subject or `BREAKING CHANGE:` in the body increments major

The tap update requires a repository secret named `HOMEBREW_TAP_TOKEN` with write
access to the tap.

If your branch contains any non-Conventional Commit messages, they can be
ignored during version calculation by default; set
`JARVIS_IGNORE_NON_CONVENTIONAL_COMMITS=0` for strict behavior.

Release-note trailers:

```text
feat(intercom): route room devices independently

Release-note: Added per-room intercom routing for multi-device homes.
Env: JARVIS_ROOM_ID added; set this on each room device before enabling routing.
Upgrade-note: Restart room intercom services after setting JARVIS_ROOM_ID.
Docs: docs/DEPLOYMENT.md covers room device pairing.
```

Use `Release-note:` for changes worth mentioning, `Env:` for new/changed/removed
configuration, `Upgrade-note:` for explicit operator action, `Docs:` for a
release-note documentation pointer, and `Breaking Change:` for migration impact.
Use `Release-note: skip` only for mechanical or internal commits that should not
appear in user-facing notes.

Local preflight only:

```bash
scripts/compute_next_release_version.sh   # dry-run candidate version from commit history
tmpfile="$(mktemp)"
uv run python scripts/generate_release_notes.py \
  --version "$(scripts/compute_next_release_version.sh)" \
  --ai never \
  --strict \
  --output "$tmpfile"
sed -n '1,160p' "$tmpfile"
rm -f "$tmpfile"
```

## Conventional Commits (local)

Install the local commit hook once:

```bash
scripts/install_commit_hook.sh
```

Commits must be in Conventional Commit format, for example:

```text
feat: add new runtime installer check
```

This is enforced in CI and by the local commit hook.

Quick local check to verify your branch before release:

```bash
scripts/check_conventional_commits.sh $(git describe --tags --abbrev=0) HEAD
```

Release smoke test (recommended for this repo):

1. Run `scripts/compute_next_release_version.sh` and confirm the number matches
   your intended bump (`feat` => minor, patch set only when no `feat`).
2. Preview deterministic release notes locally with
   `scripts/generate_release_notes.py`; do not publish from your machine.
3. Trigger `Release` with `skip_homebrew: true` and `draft: true` to validate the
   computed release output and artifact packaging without touching your tap.
