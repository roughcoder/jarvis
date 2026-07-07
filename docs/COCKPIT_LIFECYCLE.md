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

## Project Delete Policy

Project delete is conservative while tree-aware chat cascade/reparent work is
still in flight. A normal `DELETE /v1/projects/{id}` blocks with
`409 project_not_empty` when project threads exist. Delete child work first, or
implement the explicit tree-aware cascade lane once the orchestration
`parent_chat_id` model lands.
