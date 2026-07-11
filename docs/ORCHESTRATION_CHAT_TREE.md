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

Project orchestrator threads can compose this lifecycle through general tools:

1. `spawn_child_work_session` creates a child with independent `engine`,
   provider `model`, provider-instance provenance, and optional fleet
   `worker_id`. Provider instance and worker identity are deliberately separate.
2. `watch_child_work_sessions` durably registers the complete expected child-id
   set and returns immediately. It does not hold an HTTP request or poll.
3. When every watched child is terminal, Jarvis leases one automatic parent
   continuation. Duplicate terminal notifications cannot schedule another
   continuation; an abandoned lease can be recovered by a later notification.
4. The resumed parent uses `read_child_work_result` to read a bounded canonical
   assistant transcript. A terminal child without assistant output is reported
   as incomplete rather than as an empty successful result.

This is workflow-neutral machinery: pull-request review is one recipe built on
it, not a special orchestration state machine.

## Structured GitHub reviews

`publish_github_pr_review` is gated by `forge.github.pr.comment` and routes the
write to an eligible worker with a freshly authenticated GitHub identity and
access to the project repository. The brain never reads or copies the worker's
GitHub credential.

The worker requires a stable idempotency key and the reviewed head SHA. Before
posting it re-reads the current head and diff, validates each line anchor,
formats titles as `[P1]`, `[P2]`, or `[P3]`, and emits GitHub `suggestion`
blocks only on applicable right-side lines. Invalid anchors become review-level
summary findings; equivalent existing inline comments are suppressed. The
idempotency record prevents a retry from creating a second review.

## Orchestrator Model

Project orchestrator sessions use the env-driven
`ORCHESTRATION_ORCHESTRATOR_MODEL` route as their strong model route. The value
is a LiteLLM route name, not a provider model id. Build worker engines remain
selected through worker engine routing.

Child work-session `model` is an explicit provider model id and is propagated
unchanged through the execution envelope and worker-session metadata into the
Codex app-server or Claude SDK. It is not the orchestrator LiteLLM route.
