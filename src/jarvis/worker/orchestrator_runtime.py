"""Provider-neutral bootstrap for a thread-scoped Jarvis orchestrator MCP server."""

from __future__ import annotations

import sys
from typing import Any

from jarvis.worker.providers.base import ProviderTurn


ORCHESTRATOR_MCP_SERVER_NAME = "jarvis_orchestrator"
ORCHESTRATOR_MCP_TOOL_PREFIX = f"mcp__{ORCHESTRATOR_MCP_SERVER_NAME}__"
ORCHESTRATOR_TOOL_NAMES = (
    "spawn_child_work_session",
    "read_child_work_result",
    "watch_child_work_sessions",
    "publish_github_pr_review",
)
ORCHESTRATOR_ALLOWED_TOOLS = [
    f"{ORCHESTRATOR_MCP_TOOL_PREFIX}{name}" for name in ORCHESTRATOR_TOOL_NAMES
]
ORCHESTRATOR_INSTRUCTIONS = (
    "You are a Jarvis project orchestrator, not a repository worker. Do not edit files or inspect a checkout. "
    "Use the Jarvis orchestrator tools to create child work sessions, register an event-driven watch, read each "
    "terminal child result, reconcile and deduplicate the findings, and perform only the capability-gated external "
    "action requested by the user. Never claim that a review was published unless publish_github_pr_review returned "
    "published=true."
)


def orchestrator_mcp_server(turn: ProviderTurn) -> dict[str, Any] | None:
    raw = turn.runtime_context.get("orchestrator_mcp")
    if not isinstance(raw, dict):
        return None
    api_url = str(raw.get("api_url") or "").strip()
    project_id = str(raw.get("project_id") or "").strip()
    thread_id = str(raw.get("thread_id") or "").strip()
    grant_file = str(raw.get("grant_file") or "").strip()
    if not api_url or not project_id or not thread_id or not grant_file:
        raise RuntimeError("orchestrator MCP runtime context is incomplete")
    return {
        "type": "stdio",
        "command": sys.executable,
        "args": [
            "-m",
            "jarvis.cli",
            "orchestrator-mcp",
            "--api-url",
            api_url,
            "--project-id",
            project_id,
            "--thread-id",
            thread_id,
        ],
        "env": {"JARVIS_ORCHESTRATOR_GRANT_FILE": grant_file},
    }
