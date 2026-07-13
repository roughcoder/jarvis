"""Shared helpers for provider adapters (codex.py, claude.py).

Kept intentionally tiny: sandbox-boundary checks and event-log bookkeeping
that both providers need byte-identically, parameterized only where the two
providers' text differs (the provider label).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from jarvis.config import WorkerConfig
from jarvis.worker.providers.base import ProviderTurn
from jarvis.worker.sessions import SessionManager, WorkerSession
from jarvis.worker.workspaces import is_worker_owned_path_for_config
from jarvis.worker_session_contract import CANCELLED_SESSION_STATUSES, EVENT_PROVIDER_LOG

# Throttle: stop recording provider log events for a turn after this many.
PROVIDER_LOG_EVENT_LIMIT = 20
# Truncate provider log text to this many characters before storing.
PROVIDER_LOG_TEXT_LIMIT = 1000

JarvisApprovalDecision = Literal[
    "approved",
    "approved_for_session",
    "denied",
    "declined",
    "cancelled",
]
ProviderApprovalDecision = Literal[
    "approved",
    "approved_for_session",
    "denied",
    "cancelled",
]


def normalize_approval_decision(value: Any) -> ProviderApprovalDecision:
    """Normalize Jarvis's five public decisions and legacy provider aliases."""
    decision = str(value or "").strip().lower()
    if decision in {"approved", "approve", "allow", "allowed", "yes", "accept"}:
        return "approved"
    if decision in {
        "approved_for_session",
        "approvedforsession",
        "accept_for_session",
        "acceptforsession",
        "always",
    }:
        return "approved_for_session"
    if decision in {"cancel", "cancelled", "canceled"}:
        return "cancelled"
    return "denied"


def session_cwd(session: WorkerSession, worker_cfg: WorkerConfig, *, provider: str) -> str:
    candidates = [
        session.cwd,
        str(session.metadata.get("provider_cwd") or ""),
        str(session.metadata.get("cwd") or ""),
    ]
    rejected: list[str] = []
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser().resolve(strict=False)
        if not is_worker_owned_path_for_config(path, worker_cfg):
            rejected.append(str(path))
            continue
        if path.is_dir():
            return str(path)
        rejected.append(str(path))
    if rejected:
        raise RuntimeError(f"worker session cwd is not a valid worker-owned directory: {', '.join(rejected)}")
    raise RuntimeError(f"worker session cwd is required for {provider} provider turns")


def session_cancelled(sessions: SessionManager, session_id: str) -> bool:
    session = sessions.get(session_id)
    return session is not None and session.status in CANCELLED_SESSION_STATUSES


def record_provider_log(session_id: str, turn: ProviderTurn, sessions: SessionManager, text: str, *, provider: str) -> None:
    if not text:
        return
    recent = [
        event
        for event in sessions.events(session_id)
        if event.type == EVENT_PROVIDER_LOG and event.data.get("turn_id") == turn.turn_id
    ]
    if len(recent) >= PROVIDER_LOG_EVENT_LIMIT:
        return
    sessions.append_event(
        session_id,
        EVENT_PROVIDER_LOG,
        {
            "turn_id": turn.turn_id,
            "idempotency_key": turn.idempotency_key,
            "provider": provider,
            "text": text[:PROVIDER_LOG_TEXT_LIMIT],
        },
    )


def control_request_id(request: dict[str, Any]) -> str:
    request_id = str(request.get("request_id") or request.get("id") or "").strip()
    if not request_id:
        raise RuntimeError("request_id is required")
    return request_id
