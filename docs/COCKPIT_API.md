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
POST /v1/work/validate
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
  "request_kinds": ["approval", "input"],
  "start_options": {
    "sources": ["manual", "github", "linear"],
    "engines": ["codex", "claude"],
    "engine_strategies": ["single", "parallel"],
    "landing_modes": ["branch_only", "draft_pr", "ready_pr", "confirm_before_pr"],
    "required_fields": {
      "manual": ["phrase or work_item.title", "repo (unless a default repo is configured)"],
      "github": ["repo (unless a default repo is configured)"],
      "linear": ["repo (unless a default repo is configured)"]
    },
    "defaults": {
      "source": "manual",
      "worker_id": "macbook-worker",
      "repo": "roughcoder/jarvis",
      "engine": "codex",
      "engine_strategy": "single",
      "landing_mode": "draft_pr"
    }
  }
}
```

The cockpit may display friendly public terms, but Jarvis maps them to internal
policy and engine names. Catalog engine rows are option labels drawn from the
engines the configured workers support (falling back to the built-in engine
list when no workers are configured); current worker-specific engine
capabilities still come from `WorkerProfile.engines`.

`start_options` is Jarvis-owned wizard data for the start-work form: available
sources, engines, strategies, landing modes, the fields each source requires,
and the server-side defaults (default worker, default engine, the configured
`ORCHESTRATION_DEFAULT_REPO`, and the active landing mode). Cockpits must read
defaults from here instead of shipping their own (for example a cockpit-local
`JARVIS_DEFAULT_REPO`). Empty-string defaults mean "no default configured".

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
  "artifacts": [],
  "requests": [],
  "checkpoints": []
}
```

`requests` and `checkpoints` carry the pending Request objects and checkpoint
summaries aggregated from workers. They are populated in `fast`/`probe` sync
modes and empty (`[]`) in `none` mode, which stays store-only. A request is
listed only while its session is visible: requests belonging to archived runs
or sessions are filtered out even if the worker still reports them.

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
      "repo": "jarvis",
      "status": "ready",
      "default_branch": "main",
      "is_default": true,
      "can_start_work": true
    }
  ],
  "public_metadata": {}
}
```

`repositories` is the Jarvis-owned repo registry for this worker. Rows come
from the worker profile (`workers.json`) and, on probe, from the worker's
authorised `/health` response (the worker publishes the git checkouts under its
configured repo root with each repo's default branch). `is_default` marks the
repo matching the worker profile's own `default_repo` when set, otherwise
`ORCHESTRATION_DEFAULT_REPO` (an `org/name` default matches a bare checkout
name on the trailing segment). `can_start_work` is true when the repo's status
is `ready`. Workers that publish nothing return `[]` — the cockpit should then
fall back to `start_options.defaults.repo` or a manual repo field.

`last_seen_at` is stamped when a probe of the worker last succeeded; without a
probe in the current request it may be empty even for a worker recorded as
online. `capacity.queued_sessions` is always `0` today: workers dispatch
sessions immediately and have no queue.

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
  "status": "active",
  "phase": "running",
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
  "state_reason": "Worker sessions active",
  "blocked_reason": null,
  "waiting_on": [],
  "last_error": null,
  "archived_at": null
}
```

Run lifecycle reason fields explain what a run is doing without scraping
events:

- `state_reason` — human-readable reason for the current state: the recorded
  terminal/blocking reason when one exists, otherwise a stable phrase derived
  from `phase`. `null` only for unknown phases with no recorded reason.
- `blocked_reason` — set only while `phase` is `blocked`, `stalled`, or
  `needs_human`; explains what stopped progress.
- `waiting_on` — list drawn from `approval`, `input`, and `human`; empty when
  nothing is waiting on the operator.
- `last_error` — set only when `phase` is `failed`; the redacted failure
  reason.

Live contract values: `run.status` is `active` for any non-archived run that
has not reached a terminal phase (the phase, not the status, carries lifecycle
detail). Public string fields that have no value are empty strings (`""`), not
omitted keys — for example artifact `summary`, `url`, and `commit_sha`, and
session-event `turn_id` / `message_id` on events with no turn context. Clients
must treat empty strings as "not available", not as errors.

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
  "waiting_on": ["approval"],
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

Providers sometimes emit their own spellings for canonical events. Jarvis
normalizes known aliases before exposing events to the cockpit, so clients only
ever see the canonical name:

| Provider alias | Canonical type |
|---|---|
| `provider.thread.ready` | `provider.session.ready` |

Unknown event types pass through unchanged; clients should render unrecognized
types generically rather than failing.

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

Verification artifacts add first-class fields (`command`, `started_at`,
`completed_at`) on top of the base shape; `summary` is populated for any
artifact kind that records one:

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
`400 stale_cursor` (recoverable) instead of silently restarting pagination from
the beginning. Clients should clear the cursor and refetch from the first page
when they receive that error.

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

The start/resume reconciliation packet always includes the created run and the
dispatched session summary — including `session.session_ref` — so the cockpit
can promote an optimistic draft to the real session immediately instead of
waiting for polling reconciliation.

`POST /v1/work/resume` is a high-level resume intent. Jarvis chooses the best
resumable session for the selected run.

`POST /v1/work/validate` is a read-only dry run of a start intent. It accepts
the same body as `/v1/work/start` (phrase/command, `source`, `repo`,
`worker_id`, `engine`, `engine_strategy`, `work_item`) but never creates a run,
claims work, or dispatches a session — including the `needs_human` run that a
failed start records. Use it to power start-wizard validation.

```json
{
  "ok": true,
  "api_version": "v1",
  "schema_version": 1,
  "validation": {
    "can_start": false,
    "source": "manual",
    "operation": "start_next_work",
    "repo": "",
    "worker_id": "macbook-worker",
    "engine": "codex",
    "engines": ["codex"],
    "engine_strategy": "single",
    "landing_mode": "draft_pr",
    "work_item": null,
    "owned_by_run_id": null,
    "missing": ["repo"],
    "missing_authority": [],
    "reasons": ["work item has no repo/default repo; cannot start a coding worker"],
    "notes": []
  }
}
```

`worker_id`/`engine`/`engines` report the selection Jarvis would make;
`missing` lists absent required fields, `missing_authority` lists denied
capability actions, and `reasons` is the human-readable roll-up. For github and
linear sources, Jarvis peeks at the source read-only (`next()` lists without
claiming) and reports the candidate as `work_item`
(`{source, id, title, repo, kind}`); if the source has no eligible item,
`can_start` is false with a matching reason. When read authority is missing or
the source is unreachable, the peek is skipped and `notes` says why. The exact
item can still change between validate and start. When the resolved item is
already attached to an active run, `owned_by_run_id` names it and `can_start`
is false — the same ownership rule start enforces with `WorkAlreadyOwnedError`.

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

Unarchive is unsupported in v1. If Jarvis adds unarchive later, it should use
the same consolidated archive bookkeeping path instead of letting T3 mutate
visibility locally.

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

`worker_capacity_exceeded` (409, recoverable) means a worker matches the
capability and engine requirements but has no free session slots — retry later
or stop something. `worker_unavailable` means nothing configured can do the
work at all. `stale_cursor` (400, recoverable) is returned for unknown
pagination cursors; clear the cursor and refetch from the first page.

## SSE Event Stream

`GET /v1/cockpit/events` is the cockpit-level update stream, not a raw internal
event log.

It supports:

- `Last-Event-ID`
- `?after=<cursor>`
- heartbeat comments, every 15 seconds by default while connected
- snapshot fallback for stale or unknown cursors

Jarvis computes each subscribed sync-mode snapshot once per API process refresh
tick and fans it out to all matching SSE clients. It must not rebuild the full
snapshot once per connected browser tab. Operators can tune the app-level
refresh and heartbeat cadence with `ORCHESTRATION_SSE_REFRESH_INTERVAL_S` and
`ORCHESTRATION_SSE_HEARTBEAT_INTERVAL_S`.

Native browser `EventSource` cannot set an `Authorization` header. T3 should
either proxy this endpoint through its server-side Jarvis client or use a
fetch-based SSE client that can send the bearer token. Server-side proxying is
the recommended integration. If direct browser access is required, configure
`ORCHESTRATION_API_CORS_ORIGINS` as a comma-separated allow-list; Jarvis will
answer matching `OPTIONS` preflights and attach CORS headers to matching
responses.

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

Delivery model: a connected client that is exactly one refresh tick behind
receives granular events diffed from the previous snapshot projection; each
carries the new snapshot `cursor` and the full public row as `payload`
(`artifact.removed` carries just the `artifact_id`). Any client whose cursor is
missing, stale, or more than one tick behind gets a full `snapshot` event
instead — snapshot fallback is always correct, granular events are an
optimization. Run/session creation appears as the first `.updated` event for
that id; terminal transitions appear as `.updated` events whose payload carries
the terminal `phase`/`status`/`terminal_reason`. Archive operations (a run,
session, or checkpoint disappearing from the projection) always force a
snapshot event.

`session.event` frames stream the per-turn worker events (turn/assistant/tool/
approval frames) that dispatch responses and the sync loop persist to the run's
local event log. Only worker-originated SessionEvents (records carrying a
worker `event_id`) are streamed — internal orchestration bookkeeping never
appears as timeline entries. The payload is the canonical SessionEvent
projection, and the frame envelope carries `run_id`, `session_ref`, and
`worker_id` so stream filters apply. Frames are emitted on the same refresh
tick cadence, so streamed `assistant.delta` text arrives in per-tick batches
rather than token-by-token. In `none` sync mode, new events only appear when
something else (a cockpit write, a CLI sync) lands them in the store; use
`fast` for live streaming.

`request.updated` fires when a pending request appears or changes; when a
request stops being pending, Jarvis emits `request.updated` with payload
`{"request_id": ..., "status": "closed", "session_ref": ...}` — fetch the
session's requests for the final decision. `checkpoint.updated` fires when a
checkpoint appears or changes. Both require `fast`/`probe` sync mode, since
`none`-mode snapshots do not poll workers.

Stream filters are supported and combine with AND semantics:

```text
?run_id=...
?session_ref=...
?worker_id=...
```

Filters apply to granular events only: frames that do not explicitly carry the
requested id (in the envelope or payload) are dropped, and a tick whose frames
are all filtered out degrades to a heartbeat. Snapshot events are always the
full projection; filtering clients should ignore rows they do not care about.

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

### 2026-07-04 - v1 PR review hardening (compatible)

Fixes from PR #55 review (human + Codex):

- SSE event-count baselines prime at subscribe time and are dropped when a
  mode loses its last subscriber, so the first live `session.event` after
  connecting is never absorbed into the baseline and idle periods are not
  replayed.
- Capacity-only selection failures classify from probed worker state, so a
  worker that is full only per live probe data returns
  `worker_capacity_exceeded`, not `worker_unavailable`.
- `last_seen_at` is only ever probe-stamped; unprobed workers report `""`
  even when their static profile says online.
- Validation peeks sources via `list(limit=1)` instead of `next()` so a
  future source with a side-effecting `next()` cannot be advanced.
- Dispatch responses persist their synchronous first-turn events to the run
  event log, so providers that answer immediately still get durable timelines
  and `session.event` frames.
- Only worker-originated SessionEvents (with an `event_id`) stream as
  `session.event`; internal store bookkeeping is excluded. Frames carry
  `worker_id` so `?worker_id=` filters apply to them.
- `/v1/work/validate` mirrors the start ownership check read-only and reports
  `owned_by_run_id` when the item is already attached to an active run.
- Snapshot `requests` are filtered to visible sessions, so archived
  runs/sessions no longer leak pending requests.
- `start_options.required_fields.linear` lists the repo requirement, matching
  what a Linear start actually needs to dispatch.

### 2026-07-04 - v1 Completeness round (compatible, one error-code change)

Closes the remaining gaps between the documented contract and the
implementation. `schema_version` stays 1.

- The orchestration sync loop now persists newly-fetched worker session events
  to the run's local event log (deduped by event id), making run timelines
  durable and enabling live streaming.
- `/v1/cockpit/events` now emits `session.event` frames (per-turn worker events
  batched per refresh tick), `request.updated` (including a `status: "closed"`
  frame when a request stops being pending), and `checkpoint.updated`. These
  need `fast`/`probe` sync mode.
- Snapshot responses now include `requests` and `checkpoints` arrays (populated
  in `fast`/`probe` mode, `[]` in `none` mode).
- SSE stream filters `?run_id=`, `?session_ref=`, `?worker_id=` are implemented
  with AND semantics over granular events.
- Unknown pagination cursors now return `stale_cursor` instead of
  `validation_failed` (same 400 status, still recoverable) — update clients
  that switch on the code.
- Capacity-only worker selection failures now return
  `worker_capacity_exceeded` (409) instead of `worker_unavailable`.
- `/v1/work/validate` now peeks github/linear sources read-only and reports the
  candidate `work_item`.
- Verification artifacts project first-class `command`/`started_at`/
  `completed_at`, and artifact `summary` is populated when recorded.
- Worker `last_seen_at` is stamped at probe success rather than synthesized;
  `capacity.queued_sessions` documented as always `0` (workers have no queue).
- Worker profiles may declare their own `default_repo`, which wins over the
  global default for `is_default` marking and catalog defaults.
- Catalog engine rows and `start_options.engines` are derived from the engines
  configured workers actually support.

### 2026-07-04 - v1 Cockpit feedback round (compatible)

Additive changes from the first external cockpit integration. No breaking
changes; `schema_version` stays 1.

- Documented live contract values: `run.status` is `active` for non-terminal
  runs, and empty public strings (`summary`, `url`, `commit_sha`, event
  `turn_id`/`message_id`) are valid "not available" values.
- Documented that `/v1/work/start` and `/v1/work/resume` reconciliation packets
  include `session.session_ref` for immediate draft promotion (already true in
  the implementation).
- Added `POST /v1/work/validate`: a read-only dry run of a start intent that
  reports selected repo/worker/engine, missing fields, and missing authority
  without creating a run.
- Added `start_options` to `/v1/cockpit/catalog`: sources, engines, strategies,
  landing modes, per-source required fields, and server-owned defaults
  (default worker/repo/engine/landing mode) so cockpits stop hardcoding their
  own defaults.
- Populated `WorkerProfile.repositories` from worker profiles and the worker's
  authorised `/health` response (repo name, default branch, readiness), with
  `is_default` marking the configured default repo and `can_start_work`.
- Added run lifecycle reason fields (`state_reason`, `blocked_reason`,
  `waiting_on`, `last_error`) and session `waiting_on`.
- Normalized provider event-type aliases before exposure
  (`provider.thread.ready` → `provider.session.ready`) and documented the alias
  table.
- Upgraded `/v1/cockpit/events`: clients one tick behind now receive granular
  `run.updated` / `session.updated` / `worker.updated` / `artifact.upserted` /
  `artifact.removed` events diffed from the snapshot projection, with snapshot
  fallback for stale cursors and archive transitions.

### 2026-07-01 - v1 Implementation Start

- Added the first Jarvis cockpit API server behind `jarvis api`.
- Added env-driven listener settings: `ORCHESTRATION_API_HOST`,
  `ORCHESTRATION_API_PORT`, `ORCHESTRATION_API_BIND_HOST`,
  `ORCHESTRATION_API_TOKEN`, `ORCHESTRATION_API_ALLOW_INSECURE`, and
  `ORCHESTRATION_API_CORS_ORIGINS`.
- Added env-driven cockpit SSE cadence settings:
  `ORCHESTRATION_SSE_REFRESH_INTERVAL_S` and
  `ORCHESTRATION_SSE_HEARTBEAT_INTERVAL_S`.
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
