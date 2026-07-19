from __future__ import annotations

from typing import Any

SESSION_CREATED = "created"
SESSION_RUNNING = "running"
SESSION_WAITING_PROVIDER = "waiting_provider"
SESSION_WAITING_INPUT = "waiting_input"
SESSION_WAITING_APPROVAL = "waiting_approval"
SESSION_COMPLETED = "completed"
SESSION_DONE = "done"
SESSION_FAILED = "failed"
SESSION_ERROR = "error"
SESSION_INTERRUPTED = "interrupted"
SESSION_STOPPED = "stopped"
SESSION_BLOCKED = "blocked"

ACTIVE_SESSION_STATUSES = {
    SESSION_CREATED,
    SESSION_RUNNING,
    SESSION_WAITING_PROVIDER,
    SESSION_WAITING_INPUT,
    SESSION_WAITING_APPROVAL,
}
SUCCESS_SESSION_STATUSES = {SESSION_COMPLETED, SESSION_DONE}
FAILED_SESSION_STATUSES = {
    SESSION_FAILED,
    SESSION_ERROR,
    SESSION_INTERRUPTED,
    SESSION_STOPPED,
    SESSION_BLOCKED,
}
CANCELLED_SESSION_STATUSES = {SESSION_INTERRUPTED, SESSION_STOPPED}
TURN_STARTABLE_SESSION_STATUSES = {SESSION_CREATED}
TURN_RESUMABLE_SESSION_STATUSES = SUCCESS_SESSION_STATUSES

WORKER_ERROR_SESSION_ACTIVE = "session_active"
WORKER_ERROR_SESSION_TERMINAL = "session_terminal"

# The worker publishes each engine's model / effort / speed catalogs inside
# `engine_supports.<engine>` alongside the capability booleans. These keys are
# catalog data (lists and strings), so every consumer must lift them out before
# coercing the rest of the mapping to bools.
ENGINE_CATALOG_KEYS = (
    "models",
    "default_model",
    "efforts",
    "default_effort",
    "speeds",
    "default_speed",
)


def model_ids(models: list[dict[str, Any]] | None) -> list[str]:
    return [str(row.get("id") or "") for row in (models or []) if isinstance(row, dict) and row.get("id")]


# Efforts and speeds are the same `{id, label}` row shape as models.
catalog_ids = model_ids


def validate_catalog_choice(
    value: str | None,
    rows: list[dict[str, Any]] | None,
    engine: str,
    kind: str,
) -> str:
    """Return the requested id, or raise ValueError naming the allowed ids.

    Empty means "keep the current value" and passes straight through. An engine
    with no published catalog accepts anything: the worker may predate this
    contract, and rejecting every value would be worse than trusting the caller.
    """
    value = str(value or "").strip()
    if not value:
        return ""
    allowed = catalog_ids(rows)
    if not allowed or value in allowed:
        return value
    raise ValueError(f"unknown {kind} {value!r} for engine {engine!r}; allowed: {', '.join(allowed)}")


def validate_model(model: str | None, models: list[dict[str, Any]] | None, engine: str) -> str:
    return validate_catalog_choice(model, models, engine, "model")


def validate_effort(effort: str | None, efforts: list[dict[str, Any]] | None, engine: str) -> str:
    return validate_catalog_choice(effort, efforts, engine, "effort")


def validate_speed(speed: str | None, speeds: list[dict[str, Any]] | None, engine: str) -> str:
    return validate_catalog_choice(speed, speeds, engine, "speed")

EVENT_SESSION_CREATED = "session.created"
EVENT_SESSION_INTERRUPTED = "session.interrupted"
EVENT_SESSION_STOPPED = "session.stopped"
EVENT_PROVISIONING_PROGRESS = "provisioning.progress"
EVENT_TURN_STARTED = "turn.started"
EVENT_TURN_COMPLETED = "turn.completed"
EVENT_TURN_FAILED = "turn.failed"
EVENT_PROVIDER_STARTED = "provider.started"
EVENT_PROVIDER_PROCESS_STARTED = "provider.process.started"
EVENT_PROVIDER_THREAD_READY = "provider.thread.ready"
EVENT_PROVIDER_TURN_STARTED = "provider.turn.started"
EVENT_PROVIDER_SESSION_READY = "provider.session.ready"
EVENT_PROVIDER_EVENT = "provider.event"
EVENT_PROVIDER_ERROR = "provider.error"
EVENT_PROVIDER_LOG = "provider.log"
EVENT_ASSISTANT_DELTA = "assistant.delta"
EVENT_ASSISTANT_MESSAGE = "assistant.message"
EVENT_TOOL_CALL = "tool.call"
EVENT_TOOL_RESULT = "tool.result"
EVENT_APPROVAL_REQUESTED = "approval.requested"
EVENT_APPROVAL_RESOLVED = "approval.resolved"
EVENT_INPUT_REQUESTED = "input.requested"
EVENT_INPUT_RECEIVED = "input.received"
EVENT_CHECKPOINT_CREATED = "checkpoint.created"
EVENT_CHECKPOINT_RESTORED = "checkpoint.restored"
EVENT_ARTIFACT_UPDATED = "artifact.updated"
EVENT_PLAN_UPDATED = "plan.updated"

REQUEST_KIND_APPROVAL = "approval"
REQUEST_KIND_INPUT = "input"

REQUEST_EVENT_TYPES = {
    EVENT_APPROVAL_REQUESTED: REQUEST_KIND_APPROVAL,
    EVENT_INPUT_REQUESTED: REQUEST_KIND_INPUT,
}
RESOLVED_REQUEST_EVENT_TYPES = {
    EVENT_APPROVAL_RESOLVED: REQUEST_KIND_APPROVAL,
    EVENT_INPUT_RECEIVED: REQUEST_KIND_INPUT,
}

CHECKPOINT_ID_KEY = "checkpoint_id"

IDEMPOTENT_SESSION_EVENT_TYPES = {
    EVENT_TURN_STARTED,
    EVENT_PROVIDER_STARTED,
    EVENT_TURN_COMPLETED,
    EVENT_TURN_FAILED,
}


def request_type(event_type: str) -> str:
    return REQUEST_EVENT_TYPES.get(event_type, "")


def resolved_request_type(event_type: str) -> str:
    return RESOLVED_REQUEST_EVENT_TYPES.get(event_type, "")


def turn_failure_message(data: Any) -> str:
    """Operator-readable failure text for a `turn.failed` event payload.

    Providers attach their terminal payload under `raw` with provider-specific
    shapes; older workers may emit neither `error` nor a parseable `raw`, so an
    empty string means "no detail available" and callers keep their fallback.
    """
    if not isinstance(data, dict):
        return ""
    error = str(data.get("error") or "").strip()
    if error:
        return error
    raw = data.get("raw")
    if not isinstance(raw, dict):
        return ""
    turn = raw.get("turn")
    if isinstance(turn, dict) and isinstance(turn.get("error"), dict):
        turn_error = turn["error"]
        message = str(turn_error.get("message") or "").strip()
        code = str(turn_error.get("codexErrorInfo") or turn_error.get("code") or "").strip()
        if message and code:
            return f"{code}: {message}"
        if message or code:
            return message or code
    for key in ("error", "result"):
        value = raw.get(key)
        if isinstance(value, dict):
            message = str(value.get("message") or "").strip()
            if message:
                return message
        elif isinstance(value, str) and value.strip() and bool(raw.get("is_error")):
            return value.strip()
    status = str(data.get("provider_status") or "").strip()
    if status and status not in {"failed", "error"}:
        return status
    return ""
