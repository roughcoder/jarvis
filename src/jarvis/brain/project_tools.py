"""Voice project-switching tools."""

from __future__ import annotations

import asyncio
from typing import Any

from jarvis.brain.contexts import ActiveProject, ContextStore
from jarvis.brain.memory_client import MemoryBackend
from jarvis.brain.registry import RegistryStore
from jarvis.config import MemoryConfig
from jarvis.runtime import RequestContext
from jarvis.tools.base import Tool

PROJECT_SWITCH_CAPABILITY = "project.switch"


def make_project_tools(
    cfg: MemoryConfig,
    *,
    memory: MemoryBackend,
    registry: RegistryStore,
    contexts: ContextStore,
) -> list[Tool]:
    async def switch_project(ctx: RequestContext, args: dict[str, Any]) -> str:
        query = (
            args.get("project")
            or args.get("project_name")
            or args.get("name")
            or args.get("query")
            or ""
        ).strip()
        if not query:
            return "confirmation required: which project should I open?"
        resolution = registry.resolve_project(query, ctx.identity)
        if resolution.status == "ambiguous":
            return "confirmation required: which project? " + ", ".join(resolution.speakable_names)
        if resolution.status == "not_found" or resolution.entry is None:
            return "confirmation required: I couldn't find a visible project matching that name."

        project = resolution.entry
        contexts.set_active_project(
            ctx,
            ActiveProject(id=project.id, name=project.name, peer_id=project.peer_id),
        )
        refresh_note = ""
        try:
            await asyncio.wait_for(
                memory.refresh_cache(min_interval_s=0.0, user=project.peer_id),
                timeout=cfg.tool_timeout_s,
            )
        except Exception as exc:  # noqa: BLE001 - switching remains usable with an empty cache.
            cached = memory.read_cached_representation(project.peer_id)
            detail = "using the local cache" if cached else "no cached project memory yet"
            refresh_note = f" Project memory refresh is unavailable ({type(exc).__name__}); {detail}."
        return f"Opening {project.name} project.{refresh_note}"

    def close_project(ctx: RequestContext, _args: dict[str, Any]) -> str:
        project = contexts.clear_active_project(ctx)
        if project is None:
            return "No project is currently open."
        return f"Closed {project.name} project."

    def current_project(ctx: RequestContext, _args: dict[str, Any]) -> str:
        project = contexts.active_project(ctx)
        if project is None:
            return "No project is currently open."
        return f"The active project is {project.name}."

    project_param = {
        "type": "object",
        "properties": {
            "project": {
                "type": "string",
                "description": "Spoken project name or alias, such as 'the Jarvis project'.",
            },
        },
        "required": ["project"],
    }
    empty_param = {"type": "object", "properties": {}}

    return [
        Tool(
            name="switch_project",
            description=(
                "Open, switch to, or move to a visible Jarvis project by spoken name or alias. "
                "Use for phrases like 'open project Jarvis' or 'move to the bird story project'."
            ),
            parameters=project_param,
            required_capability=PROJECT_SWITCH_CAPABILITY,
            handler=switch_project,
            timeout_s=cfg.tool_timeout_s + 1.0,
        ),
        Tool(
            name="close_project",
            description="Close the currently active project for this device and speaker.",
            parameters=empty_param,
            required_capability=PROJECT_SWITCH_CAPABILITY,
            handler=close_project,
        ),
        Tool(
            name="current_project",
            description="Report which project is currently active for this device and speaker.",
            parameters=empty_param,
            required_capability=PROJECT_SWITCH_CAPABILITY,
            handler=current_project,
        ),
    ]
