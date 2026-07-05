"""Tool adapters for exposing Jarvis brain powers over MCP."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from jarvis.brain.capabilities import (
    RequestContext,
    can_query_memory_peer,
    can_write_memory_peer,
    context_for_resolution,
)
from jarvis.brain.identity import Resolution
from jarvis.brain.memory_outbox import CurationOutbox
from jarvis.brain.memory_tools import make_memory_tools
from jarvis.brain.registry import ProjectEntry, RegistryStore
from jarvis.config import Config
from jarvis.runtime import CapabilityError, ToolRegistry
from jarvis.users import User, load_users

if TYPE_CHECKING:
    from jarvis.connectors.cockpit import CockpitConnector


class MCPAccessError(PermissionError):
    """The caller is not authenticated or not allowed to perform the action."""


class JarvisMCPService:
    """Boundary-peer facade used by MCP transports.

    The service builds a principal `RequestContext` from a bearer token and then
    calls the same memory tools, curation outbox, project registry read path, and
    Cockpit connector used by other non-voice surfaces.
    """

    def __init__(
        self,
        cfg: Config,
        *,
        memory: Any | None = None,
        registry: RegistryStore | None = None,
        cockpit: "CockpitConnector | None" = None,
        users: dict[str, User] | None = None,
    ) -> None:
        self.cfg = cfg
        self._memory = memory
        self._registry = registry
        self._cockpit = cockpit
        self._users = users

    @property
    def users(self) -> dict[str, User]:
        if self._users is None:
            self._users = load_users(self.cfg.capabilities.users_dir)
        return self._users

    @property
    def registry(self) -> RegistryStore:
        if self._registry is None:
            self._registry = RegistryStore(self.cfg.registry.path)
        return self._registry

    @property
    def memory(self) -> Any:
        if self._memory is None:
            from jarvis.brain.memory_client import MemoryClient

            self._memory = MemoryClient(self.cfg.memory)
        return self._memory

    @property
    def cockpit(self) -> CockpitConnector:
        if self._cockpit is None:
            from jarvis.connectors.cockpit import CockpitConnector

            self._cockpit = CockpitConnector(self.cfg, memory=self.memory)
        return self._cockpit

    def context_for_principal(self, principal: str) -> RequestContext:
        user = self.users.get(principal)
        if user is None:
            raise MCPAccessError(f"unknown MCP principal {principal!r}")
        resolution = Resolution(user.name, user.scope, "strong", user)
        return replace(context_for_resolution(self.cfg.capabilities, resolution), channel="mcp")

    async def project_list(self, ctx: RequestContext, *, include_archived: bool = False) -> dict[str, Any]:
        projects = await asyncio.to_thread(
            self._visible_projects,
            ctx,
            include_archived=include_archived,
        )
        return {"projects": [project.as_dict() for project in projects]}

    async def project_get(self, ctx: RequestContext, *, project_id: str) -> dict[str, Any]:
        project = await asyncio.to_thread(self._visible_project, ctx, project_id)
        if project is None:
            raise MCPAccessError("project not found or not visible")
        return {"project": project.as_dict()}

    async def memory_search(
        self,
        ctx: RequestContext,
        *,
        search_query: str,
        target: str = "",
    ) -> dict[str, str]:
        result = await self._execute_memory_tool(
            ctx,
            "memory_search",
            {"search_query": search_query, "target": target},
        )
        if result.startswith("error:"):
            raise MCPAccessError(result)
        return {"result": result}

    async def record_finding(
        self,
        ctx: RequestContext,
        *,
        project: str,
        content: str,
        status: str = "",
        agent: str = "",
        observed_at: str = "",
    ) -> dict[str, str]:
        return await self._record_project_artifact(
            ctx,
            project=project,
            content=content,
            artifact_type="finding",
            status=status or "open",
            agent=agent,
            observed_at=observed_at,
        )

    async def record_decision(
        self,
        ctx: RequestContext,
        *,
        project: str,
        content: str,
        status: str = "",
        agent: str = "",
        observed_at: str = "",
    ) -> dict[str, str]:
        return await self._record_project_artifact(
            ctx,
            project=project,
            content=content,
            artifact_type="decision",
            status=status or "accepted",
            agent=agent,
            observed_at=observed_at,
        )

    async def remember(
        self,
        ctx: RequestContext,
        *,
        content: str,
        target: str = "",
        agent: str = "",
        observed_at: str = "",
    ) -> dict[str, str]:
        content = content.strip()
        if not content:
            raise ValueError("content is required")
        peer_id = self._resolve_peer(target, ctx)
        decision = can_write_memory_peer(ctx, peer_id, registry=self.registry)
        if not decision.allowed:
            raise MCPAccessError(decision.reason)
        outbox = self._outbox()
        entry = outbox.enqueue_create(
            observed_id=peer_id,
            observer_id=ctx.memory_peer,
            content=content,
            metadata=_mcp_metadata(ctx, agent=agent, observed_at=observed_at),
        )
        return {"result": f"Noted - queued memory ({entry.content_hash})."}

    async def open_thread(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        title: str = "",
    ) -> dict[str, Any]:
        project = await asyncio.to_thread(self._visible_project, ctx, project_id)
        if project is None:
            raise MCPAccessError("project not found or not visible")
        thread = await self.cockpit.open_thread(project, ctx, title=title)
        return {"thread": thread.as_dict()}

    async def send_turn(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        thread_id: str,
        text: str,
    ) -> dict[str, Any]:
        project = await asyncio.to_thread(self._visible_project, ctx, project_id)
        if project is None:
            raise MCPAccessError("project not found or not visible")
        thread = await asyncio.to_thread(self.cockpit.index.get, project.id, thread_id)
        if thread is None:
            raise MCPAccessError("thread not found")
        reply, updated = await self.cockpit.turn(project, thread, ctx, text)
        return {"reply": reply, "thread": updated.as_dict()}

    async def upload_file(
        self,
        _ctx: RequestContext,
        *,
        project_id: str = "",
        path: str = "",
        content_base64: str = "",
        agent: str = "",
    ) -> dict[str, str]:
        _ = (project_id, path, content_base64, agent)
        return {
            "error": (
                "upload_file is not yet available: the file-vault ingestion flow "
                "has not landed in this branch."
            )
        }

    async def _record_project_artifact(
        self,
        ctx: RequestContext,
        *,
        project: str,
        content: str,
        artifact_type: str,
        status: str,
        agent: str,
        observed_at: str,
    ) -> dict[str, str]:
        project_query = project.strip()
        content = content.strip()
        if not project_query or not content:
            raise ValueError(f"project and {artifact_type} content are required")
        resolution = self.registry.resolve_project(project_query, ctx.identity)
        if resolution.entry is None or resolution.status != "matched":
            raise MCPAccessError("project not found or not visible")
        entry = resolution.entry
        decision = can_write_memory_peer(ctx, entry.peer_id, registry=self.registry)
        if not decision.allowed:
            raise MCPAccessError(decision.reason)
        queued = self._outbox().enqueue_create(
            observed_id=entry.peer_id,
            observer_id=ctx.memory_peer,
            content=content,
            metadata={
                **_mcp_metadata(ctx, agent=agent, observed_at=observed_at),
                "project_id": entry.id,
                "artifact_type": artifact_type,
                "status": status,
            },
        )
        return {"result": f"Noted - queued {artifact_type} for {entry.name} ({queued.content_hash})."}

    def _visible_projects(self, ctx: RequestContext, *, include_archived: bool) -> list[ProjectEntry]:
        projects = [
            project
            for project in self.registry.list_projects(ctx.identity, include_archived=include_archived)
            if can_query_memory_peer(ctx, project.peer_id, registry=self.registry, users=self.users).allowed
        ]
        return sorted(projects, key=lambda project: project.name.lower())

    def _visible_project(self, ctx: RequestContext, project_id: str) -> ProjectEntry | None:
        project = self.registry.get_project(project_id)
        if project is None:
            return None
        decision = can_query_memory_peer(ctx, project.peer_id, registry=self.registry, users=self.users)
        return project if decision.allowed else None

    async def _execute_memory_tool(
        self,
        ctx: RequestContext,
        name: str,
        args: dict[str, Any],
    ) -> str:
        tools = ToolRegistry()
        for tool in make_memory_tools(
            self.cfg.memory,
            memory=self.memory,
            outbox=self._outbox(),
            registry=self.registry,
            users=self.users,
        ):
            tools.register(tool)
        try:
            return await tools.execute(ctx, name, args, timeout_s=self.cfg.tools.timeout_s)
        except CapabilityError as exc:
            raise MCPAccessError(str(exc)) from exc

    def _outbox(self) -> CurationOutbox:
        return CurationOutbox(
            self.cfg.memory.curation_outbox_path,
            max_retries=self.cfg.memory.curation_outbox_max_retries,
            backoff_initial_s=self.cfg.memory.curation_outbox_backoff_initial_s,
            backoff_max_s=self.cfg.memory.curation_outbox_backoff_max_s,
        )

    def _resolve_peer(self, target: str, ctx: RequestContext) -> str:
        value = target.strip()
        if not value:
            return ctx.memory_peer
        if value.startswith(("contact:", "project:")):
            return value
        user = self.users.get(value)
        if user is not None:
            return user.peer
        project = self.registry.resolve_project(value, ctx.identity)
        if project.entry is not None and project.status == "matched":
            return project.entry.peer_id
        contact = self.registry.resolve_contact(value, ctx.identity)
        if contact.entry is not None and contact.status == "matched":
            return contact.entry.peer_id
        return value


def _mcp_metadata(ctx: RequestContext, *, agent: str, observed_at: str) -> dict[str, str]:
    return {
        "recorded_by": ctx.memory_peer,
        "source": "mcp",
        "channel": "mcp",
        "agent": agent.strip() or "external-agent",
        "observed_at": observed_at.strip() or datetime.now(UTC).date().isoformat(),
    }
