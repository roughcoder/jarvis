"""Local MCP bridge from a code-agent session to one Jarvis parent thread."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import httpx

from jarvis.orchestrator_tool_contract import (
    PUBLISH_GITHUB_PR_REVIEW,
    READ_CHILD_WORK_RESULT,
    SPAWN_CHILD_WORK_SESSION,
    WATCH_CHILD_WORK_SESSIONS,
)


_TOKEN_FILE_ENV = "JARVIS_ORCHESTRATOR_GRANT_FILE"


def run_orchestrator_mcp(
    *,
    api_url: str,
    project_id: str,
    thread_id: str,
    timeout_s: float = 90.0,
) -> None:
    from mcp.server.fastmcp import FastMCP

    token_file = os.environ.get(_TOKEN_FILE_ENV, "").strip()
    if not token_file:
        raise RuntimeError(f"{_TOKEN_FILE_ENV} is required")
    base_url = api_url.rstrip("/")
    if not base_url or not project_id.strip() or not thread_id.strip():
        raise RuntimeError("api_url, project_id, and thread_id are required")

    mcp = FastMCP(
        "Jarvis orchestrator",
        instructions=(
            "These tools are scoped to the current Jarvis project orchestrator chat. "
            "Spawn child work, watch it, read terminal results, then publish only when the user requested it."
        ),
    )

    async def call(tool_name: str, args: dict[str, Any]) -> str:
        try:
            token = Path(token_file).read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise RuntimeError("Jarvis orchestrator grant is unavailable") from exc
        if not token:
            raise RuntimeError("Jarvis orchestrator grant is unavailable")
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            response = await client.post(
                f"{base_url}/v1/orchestrator-tools/{project_id}/{thread_id}/{tool_name}",
                json=args,
                headers={"Authorization": f"Bearer {token}"},
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Jarvis orchestrator tool returned HTTP {response.status_code}"
            ) from exc
        if response.status_code >= 400 or body.get("ok") is not True:
            error = body.get("error") if isinstance(body, dict) else ""
            if isinstance(error, dict):
                error = error.get("message") or error.get("code")
            raise RuntimeError(
                str(
                    error
                    or f"Jarvis orchestrator tool returned HTTP {response.status_code}"
                )
            )
        return str(body.get("result") or "")

    @mcp.tool(name=SPAWN_CHILD_WORK_SESSION)
    async def spawn_child_work_session(
        task: str,
        title: str = "",
        repo: str = "",
        worker_id: str = "",
        provider_instance_id: str = "",
        engine: str = "",
        model: str = "",
        landing_mode: str = "none",
    ) -> str:
        return await call(
            SPAWN_CHILD_WORK_SESSION,
            {
                "task": task,
                "title": title,
                "repo": repo,
                "worker_id": worker_id,
                "provider_instance_id": provider_instance_id,
                "engine": engine,
                "model": model,
                "landing_mode": landing_mode,
            },
        )

    @mcp.tool(name=READ_CHILD_WORK_RESULT)
    async def read_child_work_result(child_chat_id: str) -> str:
        return await call(READ_CHILD_WORK_RESULT, {"child_chat_id": child_chat_id})

    @mcp.tool(name=WATCH_CHILD_WORK_SESSIONS)
    async def watch_child_work_sessions(
        child_chat_ids: list[str],
        expected_count: int = 0,
        continuation_instruction: str = "",
    ) -> str:
        args: dict[str, Any] = {
            "child_chat_ids": child_chat_ids,
            "continuation_instruction": continuation_instruction,
        }
        if expected_count:
            args["expected_count"] = expected_count
        return await call(WATCH_CHILD_WORK_SESSIONS, args)

    @mcp.tool(name=PUBLISH_GITHUB_PR_REVIEW)
    async def publish_github_pr_review(
        repo: str,
        pull_number: int,
        commit_id: str,
        summary: str,
        idempotency_key: str,
        comments: list[dict[str, Any]],
    ) -> str:
        return await call(
            PUBLISH_GITHUB_PR_REVIEW,
            {
                "repo": repo,
                "pull_number": pull_number,
                "commit_id": commit_id,
                "summary": summary,
                "idempotency_key": idempotency_key,
                "comments": comments,
            },
        )

    mcp.run("stdio")
