"""Tool adapters for exposing Jarvis brain powers over MCP."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from dataclasses import replace
from typing import Any

from jarvis.brain.capabilities import (
    RequestContext,
    can_edit_project,
    can_query_memory_peer,
    can_write_memory_peer,
    context_for_resolution,
)
from jarvis.brain.contexts import ActiveProject, ContextStore
from jarvis.brain.identity import Resolution
from jarvis.brain.memory_client import MemoryBackend, MemoryMessage, SessionPeer
from jarvis.brain.memory_outbox import CurationOutbox
from jarvis.brain.memory_tools import make_memory_tools
from jarvis.brain.project_management import BrainProjectClient, ProjectOperationError
from jarvis.brain.project_tools import make_project_tools
from jarvis.brain.registry import ProjectEntry, RegistryStore
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.config import Config
from jarvis.connectors.cockpit import (
    CockpitConnector,
    CockpitMemoryView,
    CockpitThread,
    _drain_cold_tasks,
    _project_context,
)
from jarvis.ids import utc_now
from jarvis.runtime import CapabilityError, ToolRegistry
from jarvis.tools import build_registry
from jarvis.users import User, load_users


MCP_SEND_TURN_CAPABILITIES = frozenset(
    {
        "memory.query",
        "memory.curate",
        "project.switch",
    }
)
"""Capabilities a BrainSession may see when driven by external MCP send_turn.

The token principal still resolves normally, but the conversation tool ceiling
is structurally limited to memory/project verbs. Host-device grants such as
web.search, files.*, worker.*, browser, google, remote, or background.run never
become available to an external MCP agent through send_turn.
"""


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
        project_client: BrainProjectClient | None = None,
        users: dict[str, User] | None = None,
    ) -> None:
        self.cfg = cfg
        self._memory = memory
        self._registry = registry
        self._cockpit = cockpit
        self._project_client = project_client
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
            self._cockpit = MCPCockpitConnector(self.cfg, memory=self.memory)
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

    async def project_create(self, ctx: RequestContext, **payload: Any) -> dict[str, Any]:
        return await self._brain_project_write(ctx, "project.create", payload)

    async def project_update(self, ctx: RequestContext, *, project_id: str, **fields: Any) -> dict[str, Any]:
        return await self._brain_project_write(ctx, "project.update", {"project_id": project_id, **fields})

    async def project_set_repos(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        repos: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return await self._brain_project_write(ctx, "project.repos.set", {"project_id": project_id, "repos": repos})

    async def project_set_visibility(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        visibility: str,
    ) -> dict[str, Any]:
        return await self._brain_project_write(
            ctx,
            "project.visibility.set",
            {"project_id": project_id, "visibility": visibility},
        )

    async def project_set_members(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        members: list[str],
    ) -> dict[str, Any]:
        return await self._brain_project_write(ctx, "project.members.set", {"project_id": project_id, "members": members})

    async def project_archive(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        archived: bool = True,
    ) -> dict[str, Any]:
        return await self._brain_project_write(
            ctx,
            "project.archive",
            {"project_id": project_id, "archived": archived},
        )

    async def project_delete(self, ctx: RequestContext, *, project_id: str) -> dict[str, Any]:
        return await self._brain_project_write(ctx, "project.delete", {"project_id": project_id})

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

    async def forget(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        query: str,
        confirm: bool = False,
        conclusion_ids: list[str] | None = None,
    ) -> dict[str, str]:
        result = await self._brain_project_write(
            ctx,
            "project.memory.forget",
            {
                "project_id": project_id,
                "query": query,
                "confirm": confirm,
                "conclusion_ids": conclusion_ids or [],
                "source": "mcp",
                "channel": "mcp",
            },
        )
        return {"result": str(result.get("result") or "")}

    async def correct(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        query: str,
        replacement: str,
        confirm: bool = False,
        conclusion_ids: list[str] | None = None,
        observed_at: str = "",
    ) -> dict[str, str]:
        result = await self._brain_project_write(
            ctx,
            "project.memory.correct",
            {
                "project_id": project_id,
                "query": query,
                "replacement": replacement,
                "confirm": confirm,
                "conclusion_ids": conclusion_ids or [],
                "observed_at": observed_at,
                "source": "mcp",
                "channel": "mcp",
            },
        )
        return {"result": str(result.get("result") or "")}

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

    async def archive_thread(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        thread_id: str,
        reason: str = "",
    ) -> dict[str, Any]:
        project = await asyncio.to_thread(self._visible_project, ctx, project_id)
        if project is None:
            raise MCPAccessError("project not found or not visible")
        if not can_edit_project(ctx, project).allowed:
            raise MCPAccessError("project not found or not editable")
        thread = await asyncio.to_thread(
            self.cockpit.archive_thread,
            project,
            thread_id,
            by=ctx.memory_peer,
            reason=reason,
        )
        if thread is None:
            raise MCPAccessError("thread not found")
        return {"thread": thread.as_dict()}

    async def unarchive_thread(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        thread_id: str,
    ) -> dict[str, Any]:
        project = await asyncio.to_thread(self._visible_project, ctx, project_id)
        if project is None:
            raise MCPAccessError("project not found or not visible")
        if not can_edit_project(ctx, project).allowed:
            raise MCPAccessError("project not found or not editable")
        thread = await asyncio.to_thread(self.cockpit.unarchive_thread, project, thread_id)
        if thread is None:
            raise MCPAccessError("thread not found")
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
        if thread.archived_at:
            raise MCPAccessError("thread is archived")
        reply, updated = await self.cockpit.turn(project, thread, mcp_send_turn_context(ctx), text)
        return {"reply": reply, "thread": updated.as_dict()}

    async def upload_file(
        self,
        ctx: RequestContext,
        *,
        project_id: str = "",
        path: str = "",
        url: str = "",
        content: str = "",
        filename: str = "",
        title: str = "",
        artifact_type: str = "spec",
        agent: str = "",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "project_id": project_id,
            "artifact_type": artifact_type,
            "title": title,
            "agent": agent,
            "channel": "mcp",
        }
        if content:
            payload["content_text"] = content
            payload["filename"] = filename or "upload.md"
        elif path:
            payload["source_path"] = path
        elif url:
            payload["source_url"] = url
            payload["filename"] = filename or "upload"
        else:
            raise ValueError("upload_file requires content, path, or url")
        return await self._brain_project_write(ctx, "project.file.upload", payload)

    async def retract_file(self, ctx: RequestContext, *, project_id: str, doc_id: str) -> dict[str, Any]:
        return await self._brain_project_write(
            ctx,
            "project.file.retract",
            {"project_id": project_id, "doc_id": doc_id},
        )

    async def project_list_files(
        self,
        ctx: RequestContext,
        *,
        project_id: str,
        include_retracted: bool = False,
    ) -> dict[str, Any]:
        return await self._brain_project_write(
            ctx,
            "project.file.list",
            {"project_id": project_id, "include_retracted": include_retracted},
        )

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

    async def _brain_project_write(self, ctx: RequestContext, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            return await self.project_client.execute(ctx, op, payload)
        except ProjectOperationError as exc:
            raise MCPAccessError(str(exc)) from exc

    @property
    def project_client(self) -> BrainProjectClient:
        if self._project_client is None:
            self._project_client = BrainProjectClient(self.cfg)
        return self._project_client

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


def mcp_send_turn_context(ctx: RequestContext) -> RequestContext:
    return replace(
        ctx,
        channel="mcp",
        capabilities=frozenset(ctx.capabilities & MCP_SEND_TURN_CAPABILITIES),
    )


class MCPCockpitConnector(CockpitConnector):
    async def turn(
        self,
        project: ProjectEntry,
        thread: CockpitThread,
        requester: RequestContext,
        text: str,
    ) -> tuple[str, CockpitThread]:
        text = text.strip()
        if not text:
            raise ValueError("turn text is required")
        project_context = _project_context(self._memory, project, thread)
        view = CockpitMemoryView(
            self._memory,
            project_peer_id=project.peer_id,
            project_context=project_context,
        )
        session = self._make_session(
            requester,
            project=project,
            memory=view,
        )
        trace = self._tracer.turn(
            room=self._cfg.gateway.room,
            speaker=requester.identity,
            channel="mcp",
            device_id=requester.device_id,
        )
        result = TurnResult()
        reply = await session.respond_text(text, trace, result)
        session.finalize(text, result, trace)
        await _drain_cold_tasks(session)
        if self._tracer is not None:
            self._tracer.emit(trace)
        reply = result.reply or reply
        await asyncio.to_thread(
            self._persist_turn,
            thread.session_id,
            requester.memory_peer,
            requester.device_id,
            text,
            reply,
            "mcp",
        )
        updated = self._index.append_turn(
            thread,
            user_peer_id=requester.memory_peer,
            user_text=text,
            assistant_peer_id=self._cfg.memory.assistant_peer_id,
            assistant_text=reply,
        )
        return reply, updated

    def _make_session(
        self,
        requester: RequestContext,
        *,
        project: ProjectEntry,
        memory: MemoryBackend,
    ) -> BrainSession:
        ctx = mcp_send_turn_context(requester)
        contexts = ContextStore(lambda _ctx: None)  # type: ignore[arg-type]
        active = ActiveProject(id=project.id, name=project.name, peer_id=project.peer_id)
        contexts.set_active_project(ctx, active)
        registry_store = RegistryStore(self._cfg.registry.path)
        users = load_users(self._cfg.capabilities.users_dir)
        tools = build_registry(
            self._cfg.tools,
            worker=self._cfg.worker,
            remote=self._cfg.remote,
            google=self._cfg.google,
            accounts=self._cfg.accounts,
            browser=self._cfg.browser,
            capabilities=self._cfg.capabilities,
            memory=memory,
        )
        outbox = CurationOutbox(
            self._cfg.memory.curation_outbox_path,
            max_retries=self._cfg.memory.curation_outbox_max_retries,
            backoff_initial_s=self._cfg.memory.curation_outbox_backoff_initial_s,
            backoff_max_s=self._cfg.memory.curation_outbox_backoff_max_s,
        )
        for tool in make_memory_tools(
            self._cfg.memory,
            memory=memory,
            outbox=outbox,
            registry=registry_store,
            users=users,
        ):
            tools.register(tool)
        for tool in make_project_tools(
            self._cfg.memory,
            memory=memory,
            registry=registry_store,
            contexts=contexts,
        ):
            tools.register(tool)
        session = BrainSession(
            self._cfg,
            ctx,
            gateway=self._turn_gateway(),
            tts=self._tts,
            memory=memory,
            tracer=self._tracer,
            registry=tools,
            memory_user=ctx.memory_peer,
            active_project_getter=lambda: active,
        )
        session.load_soul()
        return session

    def _persist_turn(
        self,
        session_id: str,
        requester_peer_id: str,
        device_id: str | None,
        user_text: str,
        assistant_text: str,
        channel: str,
    ) -> None:
        self._memory.create_session(
            session_id,
            peers=[SessionPeer(peer_id=requester_peer_id, observe_me=True, observe_others=True)],
        )
        user_metadata = {"channel": channel, "role": "user", "observed_at": utc_now()}
        assistant_metadata = {"channel": channel, "role": "assistant", "observed_at": utc_now()}
        if device_id:
            user_metadata["device_id"] = device_id
            assistant_metadata["device_id"] = device_id
        self._memory.create_messages(
            session_id,
            [
                MemoryMessage(
                    peer_id=requester_peer_id,
                    content=user_text,
                    metadata=user_metadata,
                ),
                MemoryMessage(
                    peer_id=self._cfg.memory.assistant_peer_id,
                    content=assistant_text,
                    metadata=assistant_metadata,
                ),
            ],
        )
