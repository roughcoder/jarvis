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

_CANONICAL_TOOL_ITEM_TYPES = {
    "commandExecution": "command_execution",
    "command_execution": "command_execution",
    "fileChange": "file_change",
    "file_change": "file_change",
    "mcpToolCall": "mcp_tool_call",
    "mcp_tool_call": "mcp_tool_call",
    "dynamicToolCall": "dynamic_tool_call",
    "dynamic_tool_call": "dynamic_tool_call",
    "collabAgentToolCall": "collab_agent_tool_call",
    "collab_agent_tool_call": "collab_agent_tool_call",
    "webSearch": "web_search",
    "web_search": "web_search",
    "imageView": "image_view",
    "image_view": "image_view",
    "tool_use": "dynamic_tool_call",
    "server_tool_use": "mcp_tool_call",
    "tool_result": "dynamic_tool_call",
    "advisor_tool_result": "dynamic_tool_call",
}


def canonical_tool_item_type(item: dict[str, Any]) -> str:
    return _CANONICAL_TOOL_ITEM_TYPES.get(str(item.get("type") or ""), "")


def tool_event_data(item: dict[str, Any], *, phase: str) -> dict[str, Any]:
    """Return the provider-neutral public envelope for one tool lifecycle event."""
    tool_call_id = str(
        item.get("tool_call_id")
        or item.get("toolCallId")
        or item.get("id")
        or item.get("tool_use_id")
        or item.get("toolUseId")
        or ""
    ).strip()
    tool_name = str(item.get("name") or item.get("tool") or "").strip()
    server_name = str(item.get("server") or item.get("server_name") or item.get("serverName") or "").strip()
    title = " · ".join(part for part in (server_name, tool_name) if part)
    is_failure = bool(item.get("is_error") or item.get("error")) or str(item.get("status") or "").lower() in {
        "cancelled",
        "declined",
        "error",
        "failed",
    }
    status = "in_progress" if phase == "call" else "failed" if is_failure else "completed"
    data: dict[str, Any] = {
        "item": item,
        "item_type": canonical_tool_item_type(item) or "dynamic_tool_call",
        "status": status,
    }
    if tool_call_id:
        data["tool_call_id"] = tool_call_id
        data["message_id"] = tool_call_id
    if tool_name:
        data["tool_name"] = tool_name
    if server_name:
        data["server_name"] = server_name
    if title:
        data["title"] = title
    tool_input = item.get("input", item.get("arguments"))
    if tool_input not in (None, "", [], {}):
        data["input"] = tool_input
    tool_output = item.get("output", item.get("result", item.get("content")))
    if tool_output not in (None, "", [], {}):
        data["output"] = tool_output
    if item.get("error") not in (None, "", [], {}):
        data["error"] = item["error"]
    return data


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
