# Orchestration Chat Tree

Jarvis owns hierarchical agent chat state for cockpit and other clients. The
cockpit renders this state, but `parent_chat_id` is stored and reconciled by
Jarvis.

## Chat ids

- Project orchestrator threads use their `thread_id` as `chat_id`.
- Work sessions use their `run_id` as `chat_id`.
- Root chats have `parent_chat_id: null` or `""`.
- Child work sessions can be spawned through `POST /v1/work/start` with
  `parent_chat_id`.

Snapshot, run detail, session detail, and thread projections include
`parent_chat_id` so clients can render a nested tree. Run projections also
include `child_chat_ids` for Jarvis-owned work-session children.

## Lifecycle

Archiving a parent chat never deletes or archives children. Immediate children
are promoted to root by clearing their `parent_chat_id`; the child remains
visible and keeps its own history.

`POST /v1/sessions/{session_ref}/close` is the autonomous child-close API. It:

1. Stops the worker session through the worker HTTP boundary.
2. Requests best-effort session worktree cleanup through the narrow
   `/sessions/{session_id}/cleanup` worker hook.
3. Archives the session in Jarvis cockpit state.

The lifecycle-cleanup branch owns the final worktree inventory and prune
implementation. This branch only defines the narrow hook and records cleanup
evidence.

## Notifications

When a child work run reaches a terminal phase, Jarvis appends a
`child_terminal` event to the parent run. The cockpit SSE stream emits that as a
`run.event` frame, and the durable event is visible from
`GET /v1/runs/{parent_run_id}/events`.

## Orchestrator Model

Project orchestrator sessions use the env-driven
`ORCHESTRATION_ORCHESTRATOR_MODEL` route as their strong model route. The value
is a LiteLLM route name, not a provider model id. Build worker engines remain
selected through worker engine routing.
