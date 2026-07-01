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
  "session_ref": "sessref_QF2r7mN8kT6vH3pa",
  "worker_id": "macbook-worker",
  "session_id": "sess_123"
}
```

T3 routes by `session_ref`. Jarvis owns the lookup back to `worker_id` and
`session_id`. The implementation uses a `sessref_` prefix plus a deterministic,
URL-safe opaque token and resolves it through Jarvis state, including a local
session-ref index. Clients must not decode, construct, or compare subfields
inside the ref.

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
POST /v1/runs/{run_id}/archive
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
POST /v1/sessions/{session_ref}/archive
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

Stable sync statuses:

```text
fresh
partial
stale
failed
```

`sync=none` returns `stale` because Jarvis did not probe current worker state.
Recoverable worker sync errors return `partial` with details in `errors`.

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
        "streaming": false,
        "resume": false,
        "interrupt": false,
        "approval_requests": false,
        "input_requests": false,
        "checkpoints": false
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
  "work_sources": ["manual", "github", "linear"],
  "engine_strategies": ["single", "parallel"],
  "request_kinds": ["approval", "input"]
}
```

The cockpit may display friendly public terms, but Jarvis maps them to internal
policy and engine names. Catalog engine rows are stable option labels only;
current worker-specific engine capabilities come from `WorkerProfile.engines`.

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
Engine `supports` values are published by the worker profile or worker health
contract. Cockpit projection must not infer provider capabilities from engine
names.

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

Workers may publish the source data as an `engine_supports` mapping:

```json
{
  "engine_supports": {
    "codex": {
      "streaming": true,
      "resume": true,
      "interrupt": true,
      "approval_requests": true,
      "input_requests": true,
      "checkpoints": true
    }
  }
}
```

Workers may also publish engine rows with nested `supports` objects in a health
response. If a worker does not publish support metadata, Jarvis returns `false`
for each support flag rather than guessing from the engine name.

Stable worker health values:

```text
healthy
degraded
unhealthy
unknown
```

Do not include private worker base URLs, token env names, local absolute paths,
secret-derived fields, or local machine-private details.

## RunSummary

Run summaries appear in snapshots and run lists.

```json
{
  "authority": "jarvis",
  "supported_controls": ["archive"],
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
  "terminal_reason": null,
  "archived_at": null
}
```

`GET /v1/runs/{run_id}` returns a public-safe run detail projection. It extends
`RunSummary` with projected `work_items`, `sessions`, and `artifacts`; it must
not expose raw work item bodies, source-internal IDs, worker `cwd` paths, or raw
provider metadata.

## SessionSummary

Session summaries appear in snapshots and session lists.

```json
{
  "authority": "jarvis",
  "supported_controls": ["turn", "input", "approval", "interrupt", "stop", "checkpoint_restore", "archive"],
  "session_ref": "sessref_QF2r7mN8kT6vH3pa",
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
  "updated_at": "2026-07-01T12:00:00Z",
  "archived_at": null
}
```

Use `cwd_label`, not public absolute `cwd`.
Use `authority` and `supported_controls` to route cockpit commands. T3 should
not infer authority only from id shape.

## Request Object

Pending requests are explicit cockpit controls. T3 should not reverse-engineer
them from raw provider events.

Approval request:

```json
{
  "request_id": "req_123",
  "session_ref": "sessref_QF2r7mN8kT6vH3pa",
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
  "session_ref": "sessref_QF2r7mN8kT6vH3pa",
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
  "session_ref": "sessref_QF2r7mN8kT6vH3pa",
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
  "session_ref": "sessref_QF2r7mN8kT6vH3pa",
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
  "session_ref": "sessref_QF2r7mN8kT6vH3pa",
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

Artifact `created_at` and `updated_at` are always present strings. For generated
Jarvis report artifacts, Jarvis uses the owning run's timestamps.

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

If `after` does not match a cursor/id in the current page source, Jarvis returns
`400 validation_failed` instead of silently restarting pagination from the
beginning. Clients should clear the cursor and refetch from the first page when
they receive that error.

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

Turn attachments are explicitly unsupported in cockpit API v1. If
`POST /v1/sessions/{session_ref}/turns` or `POST /v1/work/start` includes a
non-empty `attachments` array, Jarvis returns `validation_failed` instead of
silently dropping attachment data.

Future attachment shape, if enabled in a later schema version:

```json
{
  "prompt": "...",
  "attachments": [
    {
      "kind": "image",
      "mime_type": "image/png",
      "name": "screenshot.png",
      "data_url": "data:image/png;base64,..."
    }
  ]
}
```

`POST /v1/runs/{run_id}/archive` and
`POST /v1/sessions/{session_ref}/archive` hide the selected run or session from
cockpit snapshot/list views. Archive state is owned by Jarvis, not by T3 local
storage. Direct detail endpoints may still resolve archived objects by id/ref
for reconciliation.

`POST /v1/sessions/{session_ref}/checkpoints/restore` uses `checkpoint_id`.
Checkpoint IDs are durable and stable within a session. Clients must not restore
by page position, turn count, or rendered list index.

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
- heartbeat comments, currently every 15 seconds while connected
- snapshot fallback for stale or unknown cursors

Jarvis computes each subscribed sync-mode snapshot once per API process refresh
tick and fans it out to all matching SSE clients. It must not rebuild the full
snapshot once per connected browser tab.

Native browser `EventSource` cannot set an `Authorization` header. T3 should
either proxy this endpoint through its server-side Jarvis client or use a
fetch-based SSE client that can send the bearer token. Jarvis does not expose
browser CORS as the primary auth path for v1; server-side proxying is the
recommended integration.

Each SSE event has both an SSE `id:` and a JSON `cursor`:

```text
id: evt_124
event: session.event
data: {"cursor":"evt_124","occurred_at":"2026-07-01T12:00:01Z","type":"session.event","run_id":"run_1","session_ref":"sessref_K9vY2pQx7rN4Lm6A","payload":{}}
```

Event envelope:

```json
{
  "cursor": "evt_124",
  "occurred_at": "2026-07-01T12:00:01Z",
  "type": "session.event",
  "run_id": "run_1",
  "session_ref": "sessref_K9vY2pQx7rN4Lm6A",
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
- Clarified that `session_ref` values are `sessref_` prefixed, URL-safe,
  signed, and opaque. Clients must not decode or construct them.
- Clarified idempotency replay behavior: successful repeated writes with the same
  key/body may include `"idempotent": true`.
- Implemented `/v1/cockpit/events` as a cursor-aware SSE stream that sends an
  initial or stale-cursor snapshot, heartbeat comments, and fresh snapshot
  reconciliation packets when the projected cockpit cursor changes.
- Hardened review findings before merge: run detail responses are public-safe
  projections, blocking worker/store calls are kept off the aiohttp event loop,
  session event sequence numbers remain stable across pagination, unsafe API
  bind refusal exits non-zero, and SSE events include `occurred_at`.
- Normalized public enum vocabularies before merge: sync status now uses
  `fresh|partial|stale|failed`, and worker health uses
  `healthy|degraded|unhealthy|unknown`.
- Hardened provider/store projections before merge: session events, run events,
  requests, checkpoints, worker error messages, and generated report artifacts
  now redact private paths/tokens and avoid raw provider/store payloads.
- Kept `sync=none` and SSE refresh snapshots store-only so connected cockpit
  clients do not poll workers once per stream.
- Added Jarvis-owned archive controls for runs and sessions. Archived objects
  are hidden from cockpit snapshot/list views without requiring T3-local hiding.
- Added `authority` and `supported_controls` to run/session summaries so cockpit
  command routing can use server-published capability metadata.
- Clarified that turn/start attachments are unsupported in v1 and fail with a
  structured validation error instead of being silently dropped.
- Clarified that checkpoint restore uses durable per-session `checkpoint_id`
  values, not page position or turn count.
- Hardened the cockpit API before T3 dependency: SSE snapshots are refreshed
  once per app process and fanned out to subscribers; exact-session refs resolve
  through a local Jarvis index instead of worker sweeps; unknown pagination
  cursors return `validation_failed`; worker-down session detail degrades to the
  stored public row; idempotency records expire and corrupt records are treated
  as misses; engine support flags come from worker-published metadata; browser
  SSE auth is documented as a server-side proxy or fetch-SSE concern.

### 2026-07-01 - v1 Draft

- Defined the first Jarvis-owned cockpit API contract for the T3 fork.
- Added URL-safe opaque `session_ref`.
- Added first-load snapshot and cockpit-level SSE stream.
- Added catalog, worker, run, session, request, event, artifact, pagination, and
  write response schemas.
- Deferred `/v1/capabilities` until a separate policy/debug surface is needed.
