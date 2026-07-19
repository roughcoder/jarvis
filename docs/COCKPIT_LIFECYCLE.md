# Cockpit Lifecycle And Cleanup

Jarvis exposes two distinct lifecycle verbs for cockpit-owned heavy state.

- Archive is reversible hide-only. It keeps records, events, memory sessions,
  and worker worktrees in place and returns a zero reclamation summary.
- Delete is irreversible cleanup. It removes the object record and owned heavy
  state where Jarvis can prove ownership, then returns a reclamation summary.

## Worker Worktrees

Workers create isolated worktrees under `WORKER_WORKSPACE/worktrees`. The worker
daemon reports:

```json
{
  "worktree_inventory": {
    "count": 3,
    "disk_bytes": 123456,
    "stale_count": 1
  }
}
```

`stale_count` includes only worktrees that are under the configured worker
worktree root, have no live worker session using them, and are older than
`WORKER_WORKTREE_STALE_TTL_S`. A TTL of `0` treats every non-live worktree as
stale for explicit sweeps.

Worker cleanup endpoints:

```text
GET  /worktrees
POST /worktrees/prune
DELETE /sessions/{session_id}
```

Prune/delete refuses paths outside `WORKER_WORKSPACE/worktrees` and refuses a
worktree with a live session.

## Cockpit Delete Routes

```text
DELETE /v1/sessions/{session_ref}
DELETE /v1/runs/{run_id}
DELETE /v1/projects/{project_id}/threads/{thread_id}
```

Each returns:

```json
{
  "ok": true,
  "deleted": true,
  "reclamation": {
    "records": 1,
    "events": 2,
    "worktrees": 1,
    "bytes": 4096,
    "memory_sessions": 0,
    "notes": []
  }
}
```

Session and run deletes call the owning worker over HTTP to delete worker
session state and prune owned worktrees. Conversation thread delete calls the
configured Honcho memory backend over the memory HTTP client. If the active
memory backend does not support session deletion, Jarvis deletes the local
thread index and returns a note in `reclamation.notes`.

Deletes keep small local tombstones so repeated DELETE calls are idempotent.
Unknown ids still return `404 not_found`.

## Conversation Retention

Archive hides; nothing used to collect. The cockpit API now runs a periodic
retention sweep that deletes dead conversations through the *same* lifecycle
delete described above — there is no second delete path.

| Class | What qualifies | Ages from | Default TTL |
|---|---|---|---|
| `archived` | any thread with `archived_at` set | `archived_at` | `ORCHESTRATION_RETENTION_ARCHIVED_TTL_DAYS` (14d) |
| `chat` | non-archived thread with no child runs | `last_turn_at`/`updated_at` | `ORCHESTRATION_RETENTION_CHAT_TTL_DAYS` (7d) |
| `tree` | non-archived thread with child runs (review parent + children) | newest of the parent and every child | `ORCHESTRATION_RETENTION_TREE_TTL_DAYS` (7d) |

Archive wins over structure: an archived review tree ages on the archived TTL.
Cascade is a mechanism, not a class — a tree is always deleted whole, children
first, so a partial sweep can never orphan a child.

A per-class TTL of `0` disables that class outright: it is never auto-deleted,
and `jarvis conversations` reports it as `disabled`. This mirrors the worker's
`WORKER_WORKTREE_GC` refusal — an unbounded threshold is a sane answer for an
explicit sweep but a foot-gun on a timer.

`ORCHESTRATION_RETENTION_ENABLED=false`, or every class disabled, means the timer
skips deletion work. `ORCHESTRATION_RETENTION_INTERVAL_S=0` runs a startup sweep
only while enabled. The API keeps a lightweight settings poll alive so Cockpit
settings changes take effect without restart; interval changes apply on the next
retention loop tick. The sweep is best-effort: it logs one summary line per run,
records the last outcome for Cockpit, and never propagates into the API's
lifecycle.

### Cockpit API

Cockpit can drive the same policy directly:

```text
GET /v1/retention/plan
GET /v1/retention/settings
PUT /v1/retention/settings
POST /v1/retention/prune
```

Reads use the normal Cockpit read auth. `POST /v1/retention/prune` and
`PUT /v1/retention/settings` require `orchestration.runs.write` and an
`idempotency_key`, the same authority stance as worker worktree prune.

Settings are resolved from env defaults first, then from the persisted
`retention-settings.json` override record in the orchestration workspace. A
`null` value in `PUT /v1/retention/settings` clears that field back to env.

### Protections

A conversation is kept — no matter how old — if any of these hold. All are read
from local state, never a worker probe: an unreachable worker must not read as
"nothing is running".

- a turn is in flight for it in this API process
- it has queued turns
- its workspace status is `starting`, `running`, `interrupting`, `waiting_input`,
  or `waiting_approval`
- it has `pending_child_watch_ids` (a registered child watch)
- any child run is non-terminal, has an active worker session, or still has
  worker jobs (run delete `409`s on jobs, and a tree that cannot be collected
  whole must not be collected at all)
- its activity timestamp is missing or unparseable

If the memory backend is unreachable, the delete fails closed and the
conversation survives to the next sweep, rather than dropping the record while
leaving its memory session behind.

### Manual entry point

```bash
jarvis conversations                 # report what the policy would collect
jarvis conversations --dry-run -v    # per-conversation delete/keep reasons
jarvis conversations --json          # machine-readable plan
jarvis conversations --prune         # apply it now
jarvis conversations --prune --ttl-days 0.5   # override every class TTL
```

The report and the automatic sweep build the same plan from the same inputs, so
`--dry-run` is exactly what the timer would do.

## Project Delete Policy

Project delete is conservative while tree-aware chat cascade/reparent work is
still in flight. A normal `DELETE /v1/projects/{id}` blocks with
`409 project_not_empty` when project threads exist. Delete child work first, or
implement the explicit tree-aware cascade lane once the orchestration
`parent_chat_id` model lands.
