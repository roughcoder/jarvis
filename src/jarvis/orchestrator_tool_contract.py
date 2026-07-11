"""Shared contract for the tools exposed to code-agent orchestrators."""

from __future__ import annotations


SPAWN_CHILD_WORK_SESSION = "spawn_child_work_session"
READ_CHILD_WORK_RESULT = "read_child_work_result"
WATCH_CHILD_WORK_SESSIONS = "watch_child_work_sessions"
PUBLISH_GITHUB_PR_REVIEW = "publish_github_pr_review"

ORCHESTRATOR_TOOL_NAMES = (
    SPAWN_CHILD_WORK_SESSION,
    READ_CHILD_WORK_RESULT,
    WATCH_CHILD_WORK_SESSIONS,
    PUBLISH_GITHUB_PR_REVIEW,
)
ORCHESTRATOR_TOOL_NAME_SET = frozenset(ORCHESTRATOR_TOOL_NAMES)
