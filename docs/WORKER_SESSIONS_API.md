# Worker Sessions API

Jarvis worker sessions are the live-agent execution contract for operator UIs,
voice, WhatsApp, and future provider adapters. They sit beside the legacy
`/run` worker jobs API. The compatibility rule is simple: `/run` starts
one-shot worker jobs; `/sessions` owns long-lived provider sessions and
structured events.

Jarvis remains the orchestration source of truth. A UI such as a T3 fork should
read Jarvis runs and worker sessions; it should not create its own separate
work graph.

```text
WorkCommand -> OrchestrationRun -> ExecutionEnvelope
  -> WorkerSession(s) -> SessionEvent[] -> Branch/PR/artifacts -> Report
```

## Resource Model

### WorkerSession

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
    "surface": "t3"
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

### `POST /sessions/:id/turns`

Start or enqueue a provider turn. Until the Codex/Claude adapters land, Jarvis
records the turn and emits `turn.waiting_provider` rather than pretending the
provider ran.

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
    {"type": "turn.waiting_provider"}
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

### `POST /sessions/:id/interrupt`

Interrupt the live provider turn without deleting session state. Emits
`session.interrupted` and sets `status` to `interrupted`.

### `POST /sessions/:id/stop`

Stop the session. Emits `session.stopped` and sets `status` to `stopped`.

## Canonical Event Types

Initial event vocabulary:

- `session.created`
- `turn.started`
- `turn.waiting_provider`
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
- Use `/sessions/:id/interrupt` and `/sessions/:id/stop` for control buttons.
- Link PRs, branches, and evidence through Jarvis artifacts, not a UI-local
  project model.

Provider adapters are expected to map Codex app-server JSON-RPC, Claude Agent
SDK events, Cursor, and OpenCode into this canonical event stream.
