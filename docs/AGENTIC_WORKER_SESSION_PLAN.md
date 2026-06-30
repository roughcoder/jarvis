# Agentic Worker Session Plan

Date: 2026-06-30

This is the durable plan for the `codex/worker-sessions-api` branch. It promotes
the relevant OMX planning context into tracked project documentation so the PR
does not depend on ignored `.omx/` runtime state.

## Goal

Move Jarvis agentic coding execution from one-shot worker jobs to live,
durable worker sessions while keeping Jarvis as the orchestration source of
truth.

The target shape is:

```text
WorkSource -> WorkCommand -> OrchestrationRun -> ExecutionEnvelope
  -> WorkerSession(s) -> SessionEvent[] -> Branch/PR/artifacts -> Report
```

`WorkerJob` and `/run` may remain as internal transition/debug plumbing, but no
new agentic coding path should dispatch through them.

## Design Decision

The planning review chose the worker-session pivot over extending stdout-based
worker jobs.

Accepted:

- Jarvis owns run state, policy, public writes, landing decisions, schedules,
  campaigns, and reports.
- Workers own live provider sessions and event capture.
- Provider events are evidence, not authority.
- Codex, Claude, and future providers map into one canonical session event
  stream.
- UIs such as a T3-style cockpit read Jarvis runs and worker sessions; they do
  not create a parallel work graph.

Rejected:

- Adding more behavior to `/run` for new coding orchestration. It cannot support
  reliable live steering, approvals, interruption, resume, checkpointing, and
  multi-surface control.
- Making a UI project/thread model the top-level truth. Jarvis needs its own
  WorkCommand, authority, schedule, campaign, tracker, and reporting semantics.
- Letting providers decide policy from prompt text. Authority is represented in
  `ExecutionEnvelope` and enforced by Jarvis/worker code.

## Acceptance Criteria

- All new agentic coding execution can be represented as
  `OrchestrationRun -> WorkerSession -> SessionEvent[]`.
- Codex and Claude can start turns, stream events, accept user input, surface
  approval requests, be interrupted/stopped, and retain durable resume metadata.
- `ExecutionEnvelope` policy is stored with the session and enforced outside
  provider prompt text.
- Session state survives worker daemon restart and can be reconciled back into
  the Jarvis run graph.
- CLI, voice, WhatsApp, and web/operator surfaces can inspect and control the
  same run/session state.
- Public writes remain deny-by-default unless capability and landing policy
  allow them.
- The PR includes enough docs for a future agent to continue without `.omx/`
  state.

## Stage Plan And Status

### 1. Session API foundation

Status: done in PR #47.

- Added durable `WorkerSession` records and append-only `SessionEvent` storage.
- Added `/sessions` endpoints on the worker daemon.
- Added `WorkerSessionLink` to the orchestration run graph.
- Documented the initial worker session API contract.

### 2. Full pivot record

Status: done in this branch.

- Expanded `docs/AGENTIC.md` to make worker sessions the planned execution
  substrate.
- Recorded that `/run` and `WorkerJob` are migration/debug paths for coding
  work, not the product architecture.
- Added this tracked plan so the context is preserved outside ignored OMX
  state.

### 3. Session contract hardening

Status: done in this branch for the active contract; continue hardening as
providers mature.

- Added canonical session events for turns, provider lifecycle, approvals,
  input, checkpoints, completion, failure, interrupt, and stop.
- Added request surfaces for pending approvals/input and checkpoint restore.
- Added worker-side authority metadata validation through
  `WorkerSessionAuthority`.
- Added capability vocabulary for session creation, turns, approvals, input,
  interruption, and stop.

Remaining:

- Add formal schema fixtures if an external UI begins codegen from this API.
- Add long-stream compaction once event histories become large.

### 4. Provider adapter boundary

Status: done in this branch.

- Added a worker-owned provider adapter interface under
  `src/jarvis/worker/providers/`.
- Added deterministic fake provider coverage for session/event behavior.
- Kept provider runtime code inside the worker boundary, not the brain or
  orchestration packages.

### 5. CLI and session observability

Status: done in this branch.

- Added `jarvis sessions` inspection and control commands.
- Added run reporting/sync paths that can include linked worker sessions.
- Added pending request listing so operator surfaces can find blocked sessions.

### 6. Session-backed orchestration dispatch

Status: done in this branch.

- Converted new agentic coding dispatch to create worker sessions from
  `ExecutionEnvelope`.
- Linked sessions back to `OrchestrationRun`.
- Added resume behavior that appends a turn to the selected existing session
  when possible.
- Updated schedules and campaigns to dispatch session-backed work.

### 7. Codex provider sessions

Status: initial adapter done in this branch.

- Added `codex app-server` provider adapter.
- Projected Codex JSON-RPC notifications into canonical `SessionEvent`s.
- Preserved provider thread/session/checkpoint metadata in private worker
  session metadata.

Remaining:

- Dogfood with a real authenticated Codex app-server session on a live worktree.
- Extend event mapping as the app-server protocol evolves.

### 8. Claude provider sessions

Status: initial adapter done in this branch.

- Added Claude provider support through `claude -p --output-format stream-json`.
- Stored durable `--session-id` / `--resume` metadata.
- Projected Claude stream JSON into the canonical session event stream.

Remaining:

- Replace the subprocess path with a local TypeScript sidecar around
  `@anthropic-ai/claude-agent-sdk` when richer permission/question callbacks are
  needed.
- Dogfood with real Claude auth and a live repository.

### 9. Approvals and input across surfaces

Status: primitives done; surface-specific UX remains.

- Worker sessions can record pending input and approval requests.
- CLI can list and answer them.
- Session requests are durable/pollable so voice, WhatsApp, and a web UI can
  route replies later.

Remaining:

- Add voice and WhatsApp notification/response flows on top of the durable
  request primitives.

### 10. Checkpoints, rollback, and recovery

Status: initial checkpoint event/control contract done.

- Added checkpoint event vocabulary and restore endpoint shape.
- Provider-specific checkpoint ids stay in worker session metadata/events.

Remaining:

- Prove restore semantics against real providers before exposing rollback as a
  high-confidence operator action.
- Add restart reconciliation for active provider processes.

### 11. T3-style operator cockpit

Status: API contract ready; UI work remains outside this branch.

- The UI should render Jarvis runs as tasks/projects and worker sessions as live
  execution threads.
- The UI should control turns, input, approvals, interrupt, stop, checkpoints,
  artifacts, branches, PRs, and reports through Jarvis/worker APIs.
- The UI must not own a separate orchestration graph.

### 12. Verification and landing gate

Status: partial.

- Unit tests cover the worker daemon, session providers, orchestration dispatch,
  resume, schedules, campaigns, and reporting.
- CI passed Python tests and public-readiness scan on PR #48 before this doc
  update, except for a release-note trailer that this branch update fixes.

Remaining:

- Run a real Codex session dogfood.
- Run a real Claude session dogfood.
- Decide when the draft PR is ready for review after live dogfood evidence is
  captured.

### 13. Scheduled and campaign session starts

Status: done in this branch.

- Schedules can dispatch session-backed work.
- Campaign child runs can attach linked worker sessions and report blocked or
  started children.

### 14. Ensemble sessions

Status: done in this branch at the orchestration level.

- `engine_strategy: "ensemble"` can create one Jarvis run with one worker
  session per provider.
- Provider outputs remain evidence that later synthesis/reporting can evaluate.

### 15. Public reporting and reflection

Status: done for reports; keep policy conservative.

- Run reports can summarize session state, provider evidence, artifacts, and
  pending requests.
- Public comments remain capability- and landing-policy-gated.

Remaining:

- Harden report redaction after real provider payloads are observed.

## Context To Preserve

- Branch: `codex/worker-sessions-api`.
- Prior baseline: PR #47 added the worker session API foundation.
- This branch is the follow-up that makes worker sessions operational for
  orchestration and initial Codex/Claude providers.
- The original OMX artifacts were:
  - `.omx/plans/agentic-worker-session-pivot.md`
  - `.omx/plans/agentic-full-remaining-work.md`
  - `.omx/plans/agentic-full-remaining-work.ralplan-consensus.json`
  - `.omx/context/agentic-sessions-steps-1-8-20260630T194909Z.md`
  - `.omx/context/agentic-sessions-9-15-20260630T205421Z.md`
- `.omx/` is ignored and should not be required to review or continue this PR.

## Verification Used On This Branch

Local validation before PR update:

```bash
uv run ruff check src/ tests/ scripts/generate_release_notes.py
uv run pytest tests/unit/test_worker_*.py tests/unit/test_*orchestration*.py -q
git diff --check origin/main...HEAD
```

Observed result before this doc update:

- Ruff passed.
- Targeted worker/orchestration tests passed: 129 passed, with existing aiohttp
  `NotAppKeyWarning` warnings.
- Whitespace check passed.
- GitHub PR #48 reported `MERGEABLE`.
- GitHub `python` and public-readiness `scan` checks passed.
- GitHub `conventional-commits` failed only because a feature commit lacked a
  `Release-note:` trailer.
