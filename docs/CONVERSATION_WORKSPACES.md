# Conversation Workspaces

Project threads start as planning-only Honcho-backed conversations. In that mode
Jarvis has project memory and registry context, but no checkout, so prompts must
not imply repository files are readable.

When a thread needs code access it escalates through the worker boundary:

1. the brain asks a configured worker to create a durable conversation workspace;
2. the worker materializes one or more repo worktrees under that workspace;
3. the brain creates or reuses a worker provider session whose `cwd` is the
   workspace root;
4. later thread turns are sent to that provider session, so Codex and Claude see
   the same workspace/tool surface.

Worker state lives below `WORKER_CONVERSATION_WORKSPACE_ROOT`, or below
`<WORKER_WORKSPACE>/conversations` when that env var is empty. The worker exposes
authenticated endpoints for:

- `POST /conversation-workspaces`
- `GET /conversation-workspaces/{conversation_id}`
- `POST /conversation-workspaces/{conversation_id}/worktrees`
- `DELETE /conversation-workspaces/{conversation_id}/worktrees/{repo_name}`

Provisioning phases are `resolving-access`, `cloning`, `creating-worktree`, and
`running`. The current phase, workspace label, worker session, and materialized
worktrees are persisted on the project thread and projected through the cockpit
API with full local paths redacted to labels.
