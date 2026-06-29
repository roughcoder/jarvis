from __future__ import annotations


READ_ACTIONS = {
    "work.github.issues.read",
    "work.github.pr.read",
    "work.linear.read",
    "orchestration.runs.read",
    "orchestration.schedules.read",
}

WRITE_ACTIONS = {
    "worker.job.start",
    "work.linear.write",
    "work.github.issues.write",
    "forge.github.branch.push",
    "forge.github.pr.create",
    "forge.github.pr.comment",
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
