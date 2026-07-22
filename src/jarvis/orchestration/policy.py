from __future__ import annotations

from jarvis.capabilities import (
    FORGE_BRANCH_PUSH,
    FORGE_PR_COMMENT,
    FORGE_PR_CREATE,
    WORKER_JOB_START,
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_CREATE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_RESTORE,
    WORKER_SESSION_STOP,
    WORKER_SESSION_TURN,
)

READ_ACTIONS = {
    "work.github.issues.read",
    "work.github.pr.read",
    "work.linear.read",
    "orchestration.runs.read",
    "orchestration.schedules.read",
}

WRITE_ACTIONS = {
    WORKER_JOB_START,
    WORKER_SESSION_CREATE,
    WORKER_SESSION_TURN,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_RESTORE,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_STOP,
    "work.linear.write",
    "work.github.issues.write",
    FORGE_BRANCH_PUSH,
    FORGE_PR_CREATE,
    FORGE_PR_COMMENT,
    "orchestration.runs.write",
    "orchestration.schedules.write",
}

HIGH_RISK_ACTIONS = {
    "forge.github.merge",
    "release.trigger",
    "secrets.read",
    "public.write.autonomous",
}


def allowed(action: str, capabilities: set[str], *, public_write_mode: str = "draft_then_confirm") -> bool:
    if action in HIGH_RISK_ACTIONS:
        return action in capabilities
    if action in WRITE_ACTIONS:
        if action.startswith("forge.github") and public_write_mode == "confirm_before_write":
            return False
        return action in capabilities
    if action in READ_ACTIONS:
        return action in capabilities or "owner.full" in capabilities
    return action in capabilities or "owner.full" in capabilities


def required_for_command(operation: str, source: str) -> list[str]:
    if operation in {"inspect_runs", "inspect_blocked", "resume_run"}:
        return ["orchestration.runs.read"]
    if source == "linear":
        return ["work.linear.read"]
    if operation == "inspect_pr_comments":
        return ["work.github.pr.read"]
    if source == "github":
        return ["work.github.issues.read"]
    return []


def required_for_landing_mode(mode: str) -> list[str]:
    if mode in {"draft_pr", "ready_pr", "confirm_before_pr"}:
        return [FORGE_BRANCH_PUSH, FORGE_PR_CREATE]
    if mode == "branch_only":
        return [FORGE_BRANCH_PUSH]
    return []


def required_for_worker_dispatch(landing_mode: str) -> list[str]:
    return [WORKER_JOB_START, WORKER_SESSION_CREATE, WORKER_SESSION_TURN, *required_for_landing_mode(landing_mode)]


def envelope_allowed_actions(landing_mode: str, access_mode: str = "") -> list[str]:
    actions = [
        *required_for_worker_dispatch(landing_mode),
        WORKER_SESSION_INTERRUPT,
        WORKER_SESSION_STOP,
    ]
    if access_mode == "interactive":
        actions.append(WORKER_SESSION_APPROVE)
    return actions
