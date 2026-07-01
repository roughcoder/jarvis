# Jarvis Cockpit API v1

This is the implementation contract for a T3-style Jarvis cockpit.

Jarvis owns orchestration, workers, authority, sessions, provider state, and
artifacts. T3 renders cockpit projections and sends operator intents. Jarvis
validates every write, executes through its own orchestration and worker
boundaries, records the outcome, and returns reconciliation state.

The cockpit API is intentionally a projection. It does not expose private worker
URLs, tokens, local absolute paths, provider credentials, or a second source of
truth for orchestration.

## Versioning

All endpoints live under `/v1`.

Responses that describe stable cockpit schemas include:

```json
{
  "api_version": "v1",
  "schema_version": 1
}
```

`api_version` is the HTTP API namespace. `schema_version` is the response shape
contract used by the cockpit UI.

## Stable Identifiers

`run_id` is the Jarvis orchestration run id.

`session_ref` is the public cockpit session id. It must be opaque and URL-safe:
no `/`, no route separators, and no client parsing contract.

Example:

```json
{
  "session_ref": "sessref_bWFjYm9vay13b3JrZXIAc2Vzc18xMjM",
  "worker_id": "macbook-worker",
  "session_id": "sess_123"
}
```

T3 routes by `session_ref`. Jarvis owns the lookup back to `worker_id` and
`session_id`. The current implementation uses a `sessref_` prefix plus a
base64url payload, but clients must treat the whole string as opaque.

## Endpoints

### Health

```text
GET /v1/health
```

### Cockpit

```text
GET /v1/cockpit/catalog
GET /v1/cockpit/snapshot?sync=none|fast|probe
GET /v1/cockpit/events?after=<cursor>
```

### Workers

```text
GET /v1/workers
GET /v1/workers/{worker_id}
```

### Runs

```text
GET /v1/runs
GET /v1/runs/{run_id}
GET /v1/runs/{run_id}/events?after=<cursor>&limit=100
GET /v1/runs/{run_id}/artifacts?after=<cursor>&limit=100
```

### Sessions

```text
GET /v1/sessions
GET /v1/sessions/{session_ref}
GET /v1/sessions/{session_ref}/events?after=<cursor>&limit=100
GET /v1/sessions/{session_ref}/requests
GET /v1/sessions/{session_ref}/checkpoints
```

### Work

```text
POST /v1/work/start
POST /v1/work/resume
```

### Session Controls

```text
POST /v1/sessions/{session_ref}/turns
POST /v1/sessions/{session_ref}/input
POST /v1/sessions/{session_ref}/approval
POST /v1/sessions/{session_ref}/interrupt
POST /v1/sessions/{session_ref}/stop
POST /v1/sessions/{session_ref}/checkpoints/restore
```

## Sync Modes

Snapshot and list endpoints may accept a `sync` query parameter.

`none`: read the local Jarvis orchestration store only.

`fast`: refresh linked run/session status from known workers without expensive
worker probing.

`probe`: probe worker health/capacity and refresh linked run/session status.

Snapshot responses include visible freshness state:

```json
{
  "sync": {
    "mode": "probe",
    "status": "fresh",
    "synced_at": "2026-07-01T12:00:00Z",
    "errors": []
  }
}
```

## Catalog

`GET /v1/cockpit/catalog` returns stable UI option data for forms and selectors.
It is not current operational state.

```json
{
  "api_version": "v1",
  "schema_version": 1,
  "engines": [
    {
      "engine": "codex",
      "display_name": "Codex",
      "description": "OpenAI Codex provider session",
      "supports": {
        "streaming": true,
        "resume": true,
        "interrupt": true,
        "approval_requests": true,
        "input_requests": true,
        "checkpoints": true
      }
    }
  ],
  "capabilities": [
    {
      "capability": "code.edit",
      "display_name": "Edit code",
      "maps_to": ["worker.session.create", "worker.session.turn"]
    }
  ],
  "work_sources": ["manual", "github", "linear", "voice", "whatsapp"],
  "engine_strategies": ["single", "parallel", "review_panel"],
  "branch_strategies": ["auto", "use_existing", "create", "none"],
  "landing_policies": ["branch_only", "draft_pr", "ready_pr", "confirm_before_pr"],
  "request_kinds": ["approval", "input"]
}
```

The cockpit may display friendly public terms, but Jarvis maps them to internal
policy and engine names.

## Snapshot

`GET /v1/cockpit/snapshot` is the first-load endpoint for T3. It returns current
operational state rich enough to render the shell, sidebars, status badges,
worker selectors, and artifact links without a pile of follow-up calls.

```json
{
  "api_version": "v1",
  "schema_version": 1,
  "cursor": "evt_123",
  "generated_at": "2026-07-01T12:00:00Z",
  "sync": {
    "mode": "probe",
    "status": "fresh",
    "synced_at": "2026-07-01T12:00:00Z",
    "errors": []
  },
  "runs": [],
  "sessions": [],
  "workers": [],
  "artifacts": []
}
```

Snapshot rows are summaries. Timelines, logs, report bodies, checkpoint detail,
and large provider payloads stay behind lazy detail endpoints.

## WorkerProfile

Worker profiles are public-safe rows for selectors and status displays.

```json
{
  "worker_id": "macbook-worker",
  "display_name": "MacBook Pro",
  "status": "online",
  "health": "healthy",
  "last_seen_at": "2026-07-01T12:00:00Z",
  "capabilities": ["code.edit", "shell.run", "browser.use", "github.pr.create"],
  "engines": [
    {
      "engine": "codex",
      "display_name": "Codex",
      "status": "available",
      "default": true,
      "supports": {
        "streaming": true,
        "resume": true,
        "interrupt": true,
        "approval_requests": true,
        "input_requests": true,
        "checkpoints": true
      }
    }
  ],
  "capacity": {
    "max_sessions": 4,
    "active_sessions": 1,
    "queued_sessions": 0
  },
  "repositories": [
    {
      "repo": "roughcoder/jarvis",
      "status": "ready",
      "default_branch": "main"
    }
  ],
  "public_metadata": {}
}
```

Do not include private worker base URLs, token env names, local absolute paths,
secret-derived fields, or local machine-private details.

## RunSummary

Run summaries appear in snapshots and run lists.

```json
{
  "run_id": "run_123",
  "title": "Build worker sessions",
  "objective": "Expose live worker sessions",
  "status": "running",
  "phase": "implementing",
  "repo": "roughcoder/jarvis",
  "branch": "jarvis/foo",
  "session_count": 2,
  "active_session_count": 1,
  "pending_input_count": 0,
  "pending_approval_count": 1,
  "artifact_count": 3,
  "primary_artifact_ids": ["artifact_123"],
  "latest_activity_at": "2026-07-01T12:00:00Z",
  "latest_cursor": "evt_123",
  "created_at": "2026-07-01T11:00:00Z",
  "updated_at": "2026-07-01T12:00:00Z",
  "terminal_reason": null
}
```

## SessionSummary

Session summaries appear in snapshots and session lists.

```json
{
  "session_ref": "sessref_bWFjYm9vay13b3JrZXIAc2Vzc18xMjM",
  "worker_id": "macbook-worker",
  "session_id": "sess_123",
  "run_id": "run_123",
  "title": "Codex implementation",
  "provider": "codex",
  "engine": "codex",
  "status": "running",
  "repo": "roughcoder/jarvis",
  "branch": "jarvis/foo",
  "cwd_label": "jarvis",
  "latest_event_cursor": "evt_123",
  "pending_input_count": 0,
  "pending_approval_count": 1,
  "checkpoint_count": 2,
  "created_at": "2026-07-01T11:00:00Z",
  "updated_at": "2026-07-01T12:00:00Z"
}
```

Use `cwd_label`, not public absolute `cwd`.

## Request Object

Pending requests are explicit cockpit controls. T3 should not reverse-engineer
them from raw provider events.

Approval request:

```json
{
  "request_id": "req_123",
  "session_ref": "sessref_bWFjYm9vay13b3JrZXIAc2Vzc18xMjM",
  "run_id": "run_123",
  "kind": "approval",
  "status": "pending",
  "title": "Approve file edits",
  "detail": "Apply patch to apps/server/src/...",
  "created_at": "2026-07-01T12:00:00Z",
  "expires_at": null,
  "payload": {
    "request_kind": "file-change"
  }
}
```

Input request:

```json
{
  "request_id": "req_456",
  "session_ref": "sessref_bWFjYm9vay13b3JrZXIAc2Vzc18xMjM",
  "run_id": "run_123",
  "kind": "input",
  "status": "pending",
  "title": "Input needed",
  "detail": "",
  "created_at": "2026-07-01T12:00:00Z",
  "expires_at": null,
  "questions": [
    {
      "id": "response",
      "header": "Input",
      "question": "Which worker should continue?",
      "options": []
    }
  ],
  "payload": {}
}
```

## SessionEvent

Session events are canonical, ordered, and renderable by T3.

```json
{
  "event_id": "ev_123",
  "sequence": 42,
  "session_ref": "sessref_bWFjYm9vay13b3JrZXIAc2Vzc18xMjM",
  "run_id": "run_123",
  "type": "assistant.delta",
  "occurred_at": "2026-07-01T12:00:00Z",
  "turn_id": "turn_123",
  "message_id": "msg_123",
  "data": {}
}
```

For `assistant.delta`, `message_id` must be stable across chunks so T3 renders
one streaming assistant message rather than many fragments.

Canonical event types:

```text
session.created
turn.started
provider.started
provider.session.ready
assistant.delta
assistant.message
tool.call
tool.result
approval.requested
input.requested
approval.resolved
input.received
checkpoint.created
checkpoint.restored
turn.completed
turn.failed
session.interrupted
session.stopped
```

## Artifacts

Artifacts are public-safe summaries for branches, pull requests, reports,
verification, logs, files, URLs, status comments, and provider evidence.

```json
{
  "artifact_id": "artifact_123",
  "run_id": "run_123",
  "session_ref": "sessref_bWFjYm9vay13b3JrZXIAc2Vzc18xMjM",
  "kind": "pull_request",
  "provider": "github",
  "external_id": "47",
  "is_primary": true,
  "visibility": "public-safe",
  "title": "PR #47",
  "status": "open",
  "summary": "Adds worker sessions API",
  "url": "https://github.com/roughcoder/jarvis/pull/47",
  "branch": "jarvis/foo",
  "commit_sha": "abc123",
  "created_at": "2026-07-01T12:00:00Z",
  "updated_at": "2026-07-01T12:00:00Z",
  "metadata": {}
}
```

Supported artifact kinds:

```text
branch
pull_request
report
verification
log
file
url
status_comment
provider_evidence
```

Verification artifacts use first-class fields:

```json
{
  "artifact_id": "artifact_456",
  "run_id": "run_123",
  "session_ref": "sessref_bWFjYm9vay13b3JrZXIAc2Vzc18xMjM",
  "kind": "verification",
  "status": "passed",
  "command": "pnpm test",
  "summary": "187 passed",
  "started_at": "2026-07-01T11:55:00Z",
  "completed_at": "2026-07-01T12:00:00Z",
  "visibility": "public-safe",
  "metadata": {}
}
```

## Pagination

Large detail endpoints return paginated lists:

```text
GET /v1/runs/{run_id}/events?after=<cursor>&limit=100
GET /v1/runs/{run_id}/artifacts?after=<cursor>&limit=100
GET /v1/sessions/{session_ref}/events?after=<cursor>&limit=100
```

Response:

```json
{
  "items": [],
  "cursor": "evt_200",
  "has_more": false
}
```

## Writes

Every write accepts an idempotency key and public metadata:

```json
{
  "idempotency_key": "t3_...",
  "metadata": {
    "surface": "jarvis-cockpit"
  }
}
```

`POST /v1/work/start` is a high-level operator intent. Jarvis parses/selects
work, creates or claims a run, chooses worker and engine, dispatches sessions,
and validates authority.

`POST /v1/work/resume` is a high-level resume intent. Jarvis chooses the best
resumable session for the selected run.

`POST /v1/sessions/{session_ref}/turns` appends a prompt to one exact session. T3
uses this for the thread composer once the operator is already inside a session.

Successful writes return a reconciliation packet:

```json
{
  "ok": true,
  "cursor": "evt_130",
  "run": {},
  "session": {},
  "events": [],
  "requests": [],
  "artifacts": []
}
```

If a write is replayed with the same `idempotency_key` and the same request
body, Jarvis may return the stored reconciliation packet with an additional
`"idempotent": true` field.

Rejected writes return structured errors:

```json
{
  "ok": false,
  "error": {
    "code": "session_active",
    "message": "Session already has an active turn.",
    "recoverable": true
  }
}
```

## Standard Error Codes

```text
unauthorized
forbidden
not_found
validation_failed
idempotency_conflict
worker_unavailable
worker_capacity_exceeded
session_active
session_terminal
request_not_pending
checkpoint_not_found
provider_unavailable
stale_cursor
internal_error
```

## SSE Event Stream

`GET /v1/cockpit/events` is the cockpit-level update stream, not a raw internal
event log.

It supports:

- `Last-Event-ID`
- `?after=<cursor>`
- heartbeat comments
- snapshot fallback for stale or unknown cursors

Each SSE event has both an SSE `id:` and a JSON `cursor`:

```text
id: evt_124
event: session.event
data: {"cursor":"evt_124","occurred_at":"2026-07-01T12:00:01Z","type":"session.event","run_id":"run_1","session_ref":"sessref_bWFjYm9vay13b3JrZXIAc2Vzc18x","payload":{}}
```

Event envelope:

```json
{
  "cursor": "evt_124",
  "occurred_at": "2026-07-01T12:00:01Z",
  "type": "session.event",
  "run_id": "run_1",
  "session_ref": "sessref_bWFjYm9vay13b3JrZXIAc2Vzc18x",
  "payload": {}
}
```

SSE event types:

```text
snapshot
run.updated
session.updated
session.event
worker.updated
artifact.upserted
artifact.removed
request.updated
checkpoint.updated
```

Future filters may be added without changing the base stream:

```text
?run_id=...
?session_ref=...
?worker_id=...
```

## Deferred From v1

`GET /v1/capabilities` is deferred. T3 v1 should use:

- `/v1/cockpit/catalog` for stable option data and friendly capability labels
- `/v1/workers` for worker capabilities and engine availability
- write responses for authoritative allow/deny outcomes

Add `/v1/capabilities` later only if T3 needs a separate policy or debugging
view.

## Implementation Order

1. `/v1/cockpit/catalog` and worker projection.
2. `/v1/cockpit/snapshot`.
3. `/v1/cockpit/events` SSE.
4. Lazy run/session detail endpoints.
5. Exact-session writes with idempotency.
6. `/v1/work/start` and `/v1/work/resume`.
7. Provider runtime hardening.

## Appendix: Change Log

Future API changes should be appended here with date, schema version, compatible
or breaking status, and migration notes.

### 2026-07-01 - v1 Implementation Start

- Added the first Jarvis cockpit API server behind `jarvis api`.
- Added env-driven listener settings: `ORCHESTRATION_API_HOST`,
  `ORCHESTRATION_API_PORT`, `ORCHESTRATION_API_BIND_HOST`,
  `ORCHESTRATION_API_TOKEN`, and `ORCHESTRATION_API_ALLOW_INSECURE`.
- Added a `cockpit` optional dependency extra for the API server's HTTP/SSE
  runtime.
- Clarified that `session_ref` values are `sessref_` prefixed, URL-safe, and
  opaque. The implementation currently uses a base64url payload, but clients
  must not decode it.
- Clarified idempotency replay behavior: successful repeated writes with the same
  key/body may include `"idempotent": true`.
- Implemented `/v1/cockpit/events` as a cursor-aware SSE stream that sends an
  initial or stale-cursor snapshot, heartbeat comments, and fresh snapshot
  reconciliation packets when the projected cockpit cursor changes.

### 2026-07-01 - v1 Draft

- Defined the first Jarvis-owned cockpit API contract for the T3 fork.
- Added URL-safe opaque `session_ref`.
- Added first-load snapshot and cockpit-level SSE stream.
- Added catalog, worker, run, session, request, event, artifact, pagination, and
  write response schemas.
- Deferred `/v1/capabilities` until a separate policy/debug surface is needed.
