# Worker Sessions API

Jarvis worker sessions are the live-agent execution contract for operator UIs,
voice, WhatsApp, and future provider adapters. They supersede one-shot coding
jobs as the primary agentic execution model. The existing `/run` worker jobs API
may remain as an internal transition/debug path, but new coding orchestration
should target `/sessions`, which owns long-lived provider sessions and structured
events.

The implementation plan and continuation context for this pivot live in
`docs/AGENTIC_WORKER_SESSION_PLAN.md`.

No new agentic coding path may dispatch through `/run` or `WorkerJob`. Any
remaining `/run` use must be non-agentic shell, scratch, or explicit debug
plumbing outside `WorkCommand` / `ExecutionEnvelope` coding flows.

Jarvis remains the orchestration source of truth. A UI such as a T3 fork should
read Jarvis runs and worker sessions; it should not create its own separate
work graph.

```text
WorkCommand -> OrchestrationRun -> ExecutionEnvelope
  -> WorkerSession(s) -> SessionEvent[] -> Branch/PR/artifacts -> Report
```

## Resource Model

### WorkerSession

`session_id` is the Jarvis worker-session id used in `/sessions/:id` routes and
orchestration run links. Provider-native resume ids are stored under
`metadata.provider_session_id` or provider-specific metadata keys such as
`metadata.codex_thread_id` and `metadata.claude_session_id`.

```json
{
  "session_id": "sess_1760000000_abcd1234",
  "provider": "codex",
  "engine": "codex",
  "status": "created",
  "run_id": "run_1760000000_abcd1234",
  "repo": "roughcoder/jarvis",
  "branch": "jarvis/eng-42-worker-heartbeat",
  "cwd": "/worker/worktrees/jarvis-eng-42-worker-heartbeat",
  "title": "Add worker heartbeat status",
  "created_at": "2026-06-30T18:00:00+00:00",
  "updated_at": "2026-06-30T18:00:00+00:00",
  "metadata": {
    "surface": "voice"
  }
}
```

### SessionEvent

```json
{
  "event_id": "ev_1760000001_abcd1234",
  "session_id": "sess_1760000000_abcd1234",
  "type": "turn.started",
  "time": "2026-06-30T18:00:01+00:00",
  "data": {
    "turn_id": "turn_1",
    "prompt": "Continue from the current diff and run the tests."
  }
}
```

## Endpoints

All endpoints use the worker daemon bearer token when `WORKER_TOKEN` is set.

### `POST /sessions`

Create a worker session record. The first implementation records durable state
and emits `session.created`; provider adapters attach behind this contract.
Codex sessions use `codex app-server` JSON-RPC. Claude sessions currently use
`claude -p --output-format stream-json` with durable `--session-id` / `--resume`
metadata, with a Claude Agent SDK sidecar planned as the richer live runtime.
Real provider sessions must include an `ExecutionEnvelope` authority context in
session metadata. Unknown provider ids are rejected when a turn is started rather
than falling back to a default provider.
Provider adapters consume this through the worker-side `WorkerSessionAuthority`
boundary object; shared ids, slugging, and capability constants live in neutral
`jarvis.*` modules so orchestration and worker packages remain boundary peers.

Request:

```json
{
  "run_id": "run_1760000000_abcd1234",
  "provider": "codex",
  "engine": "codex",
  "repo": "roughcoder/jarvis",
  "branch": "jarvis/eng-42-worker-heartbeat",
  "cwd": "",
  "title": "Add worker heartbeat status",
  "metadata": {
    "surface": "t3",
    "execution_envelope": {
      "run_id": "run_1760000000_abcd1234",
      "allowed_actions": ["worker.session.create", "worker.session.turn"],
      "landing": {"mode": "branch_only", "allow_merge": false}
    }
  }
}
```

Response:

```json
{
  "ok": true,
  "session": {"session_id": "sess_1760000000_abcd1234"},
  "event": {"type": "session.created"}
}
```

### `GET /sessions`

List worker sessions on that worker.

Response:

```json
{"sessions": [{"session_id": "sess_1760000000_abcd1234", "status": "created"}]}
```

### `GET /sessions/:id`

Inspect a single session.

### `GET /sessions/:id/events`

Read the append-only event stream for a session.

Query parameters:

- `after`: event id cursor.
- `limit`: maximum events, capped by the worker.

### `GET /sessions/requests`

List pending approval and input requests across all worker sessions. This is a
derived view over append-only events, not a second state store.

Response:

```json
{
  "requests": [
    {
      "session_id": "sess_1760000000_abcd1234",
      "request_id": "approval_turn_1",
      "kind": "approval",
      "status": "pending",
      "event": {"type": "approval.requested"}
    }
  ]
}
```

### `GET /sessions/:id/requests`

List pending approval and input requests for one session. A request is pending
when the stream has `approval.requested` or `input.requested` without a matching
`approval.resolved` or `input.received` event for the same `request_id`.

### `GET /sessions/:id/checkpoints`

List provider checkpoints projected from `checkpoint.created` events.

Response:

```json
{
  "checkpoints": [
    {
      "session_id": "sess_1760000000_abcd1234",
      "checkpoint_id": "ckpt_turn_1",
      "label": "before review fixes",
      "provider": "codex",
      "restored": false,
      "event": {"type": "checkpoint.created"}
    }
  ]
}
```

### `POST /sessions/:id/turns`

Start or enqueue a provider turn. Jarvis records `turn.started`, then the
selected provider adapter appends canonical session events as the provider
streams progress. Real providers fail closed if the session metadata does not
grant `worker.session.turn` through the `ExecutionEnvelope` authority context.

Request:

```json
{
  "turn_id": "turn_1",
  "prompt": "Inspect the repo and propose a plan.",
  "metadata": {
    "surface": "t3",
    "principal": "local-user"
  }
}
```

Response:

```json
{
  "ok": true,
  "turn_id": "turn_1",
  "events": [
    {"type": "turn.started"},
    {"type": "provider.started"}
  ]
}
```

### `POST /sessions/:id/input`

Answer a provider question or supply user text.

Request:

```json
{
  "request_id": "input_1",
  "text": "Use the existing orchestration store patterns."
}
```

Emits `input.received`.

### `POST /sessions/:id/approval`

Resolve a provider approval request. Provider adapters must enforce the
`ExecutionEnvelope` and Jarvis authority gates outside prompt text.

Request:

```json
{
  "request_id": "approval_1",
  "decision": "approved",
  "scope": "shell",
  "reason": "Targeted test command only."
}
```

Emits `approval.resolved`.

### `POST /sessions/:id/checkpoints/restore`

Ask the provider adapter to restore a known checkpoint. The worker validates the
checkpoint id against the session event stream before calling the provider.

Request:

```json
{
  "checkpoint_id": "ckpt_turn_1",
  "metadata": {"surface": "t3"}
}
```

Emits `checkpoint.restored` when accepted.

### `POST /sessions/:id/interrupt`

Interrupt the live provider turn without deleting session state. Emits
`session.interrupted` and sets `status` to `interrupted`.

### `POST /sessions/:id/stop`

Stop the session. Emits `session.stopped` and sets `status` to `stopped`.

## Canonical Event Types

The implementation source of truth for session statuses, event type constants,
request lifecycle mappings, checkpoint keys, and terminal-state groups is
`src/jarvis/worker_session_contract.py`. Keep this list aligned with that
contract when publishing API changes.

Initial event vocabulary:

- `session.created`
- `turn.started`
- `provider.started`
- `provider.process.started`
- `provider.session.ready`
- `provider.event`
- `provider.thread.ready`
- `provider.turn.started`
- `provider.log`
- `provider.error`
- `assistant.delta`
- `assistant.message`
- `tool.call`
- `tool.result`
- `approval.requested`
- `approval.resolved`
- `input.requested`
- `input.received`
- `checkpoint.created`
- `checkpoint.restored`
- `turn.completed`
- `turn.failed`
- `session.interrupted`
- `session.stopped`

Provider-specific payloads go under `data.provider_payload`. Common fields stay
at the top of `data` so voice, WhatsApp, and web UIs can render them without
knowing provider internals.

## UI Integration Notes

For a T3 fork:

- Treat Jarvis `OrchestrationRun` as the project/task source of truth.
- Treat `WorkerSession` as the live execution thread beneath that run.
- Render `SessionEvent[]` as the timeline.
- Send user replies to `/sessions/:id/input`.
- Send approvals to `/sessions/:id/approval`.
- Read pending questions/approvals from `/sessions/requests`.
- Read checkpoint options from `/sessions/:id/checkpoints` and restore through
  `/sessions/:id/checkpoints/restore`.
- Use `/sessions/:id/interrupt` and `/sessions/:id/stop` for control buttons.
- Link PRs, branches, and evidence through Jarvis artifacts, not a UI-local
  project model.

Useful CLI equivalents:

```bash
jarvis sessions --requests all
jarvis sessions --requests sess_1760000000_abcd1234
jarvis sessions --input sess_1760000000_abcd1234 --text "Use the existing pattern."
jarvis sessions --approval sess_1760000000_abcd1234 --request-id approval_turn_1 --decision denied
jarvis sessions --checkpoints sess_1760000000_abcd1234
jarvis sessions --restore-checkpoint sess_1760000000_abcd1234 --checkpoint-id ckpt_turn_1
jarvis schedules tick --dispatch --json
jarvis work next --start --engine-strategy ensemble --engine codex,claude
jarvis runs --report run_1760000000_abcd1234 --public-comment
```

Provider adapters are expected to map Codex app-server JSON-RPC, Claude stream
JSON / Claude Agent SDK events, Cursor, and OpenCode into this canonical event
stream.
