"""Cockpit project-thread connector.

The Cockpit API is an HTTP/SSE boundary peer, but the turn itself still uses the
shared BrainSession text core. This module owns the glue: project-scoped thread
metadata, live project context assembly, BrainSession construction, and explicit
Lane 1 transcript writes into the named Honcho session.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from jarvis.brain.capabilities import RequestContext
from jarvis.brain.background import BackgroundRunner
from jarvis.brain.contexts import ActiveProject, ContextStore
from jarvis.brain.gateway_client import GatewayClient
from jarvis.brain.memory_client import (
    ConclusionRecord,
    MemoryBackend,
    MemoryClient,
    MemoryMessage,
    SessionPeer,
    UnsupportedMemoryOperation,
)
from jarvis.brain.memory_outbox import CurationOutbox
from jarvis.brain.memory_tools import make_memory_tools
from jarvis.brain.project_tools import make_project_tools
from jarvis.brain.registry import ProjectEntry, RegistryStore
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.brain.tracing import Tracer
from jarvis.config import Config
from jarvis.ids import new_id, utc_now
from jarvis.tools import build_registry
from jarvis.tools.background import make_background_tool
from jarvis.users import load_users


THREAD_INDEX_FILENAME = "cockpit-threads.json"
THREAD_HISTORY_LIMIT = 24


@dataclass(frozen=True)
class CockpitThread:
    thread_id: str
    project_id: str
    session_id: str
    title: str
    created_at: str
    updated_at: str
    created_by: str
    archived_at: str = ""
    archived_by: str = ""
    archive_reason: str = ""
    messages: tuple[dict[str, str], ...] = ()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CockpitThread":
        return cls(
            thread_id=str(data.get("thread_id") or ""),
            project_id=str(data.get("project_id") or ""),
            session_id=str(data.get("session_id") or ""),
            title=str(data.get("title") or ""),
            created_at=str(data.get("created_at") or ""),
            updated_at=str(data.get("updated_at") or ""),
            created_by=str(data.get("created_by") or ""),
            archived_at=str(data.get("archived_at") or ""),
            archived_by=str(data.get("archived_by") or ""),
            archive_reason=str(data.get("archive_reason") or ""),
            messages=tuple(
                {
                    "role": str(item.get("role") or ""),
                    "peer_id": str(item.get("peer_id") or ""),
                    "content": str(item.get("content") or ""),
                    "observed_at": str(item.get("observed_at") or ""),
                }
                for item in data.get("messages") or ()
                if isinstance(item, dict)
            ),
        )

    def as_dict(self, *, include_messages: bool = False) -> dict[str, Any]:
        data = {
            "thread_id": self.thread_id,
            "project_id": self.project_id,
            "session_id": self.session_id,
            "title": self.title,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "created_by": self.created_by,
            "archived_at": self.archived_at,
            "archived_by": self.archived_by,
            "archive_reason": self.archive_reason,
        }
        if include_messages:
            data["messages"] = [dict(message) for message in self.messages]
        return data


class CockpitThreadIndex:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()

    def list(self, project_id: str, *, include_archived: bool = False) -> list[CockpitThread]:
        return sorted(
            [
                thread
                for thread in self._threads().values()
                if thread.project_id == project_id and (include_archived or not thread.archived_at)
            ],
            key=lambda thread: thread.updated_at or thread.created_at,
            reverse=True,
        )

    def get(self, project_id: str, thread_id: str) -> CockpitThread | None:
        thread = self._threads().get(thread_id)
        if thread is None or thread.project_id != project_id:
            return None
        return thread

    def save(self, thread: CockpitThread) -> CockpitThread:
        data = self._read()
        threads = data.setdefault("threads", {})
        threads[thread.thread_id] = thread.as_dict(include_messages=True)
        _atomic_write_json(self.path, data)
        return thread

    def set_archived(
        self,
        project_id: str,
        thread_id: str,
        *,
        archived: bool,
        by: str = "",
        reason: str = "",
    ) -> CockpitThread | None:
        thread = self.get(project_id, thread_id)
        if thread is None:
            return None
        if archived and thread.archived_at:
            return thread
        archive_reason = reason.strip()[:500]
        updated = replace(
            thread,
            archived_at=utc_now() if archived else "",
            archived_by=by if archived else "",
            archive_reason=archive_reason if archived else "",
        )
        return self.save(updated)

    def append_turn(
        self,
        thread: CockpitThread,
        *,
        user_peer_id: str,
        user_text: str,
        assistant_peer_id: str,
        assistant_text: str,
    ) -> CockpitThread:
        observed_at = utc_now()
        messages = [
            *thread.messages,
            {
                "role": "user",
                "peer_id": user_peer_id,
                "content": user_text,
                "observed_at": observed_at,
            },
            {
                "role": "assistant",
                "peer_id": assistant_peer_id,
                "content": assistant_text,
                "observed_at": observed_at,
            },
        ][-THREAD_HISTORY_LIMIT:]
        stored = self.get(thread.project_id, thread.thread_id)
        archive_source = stored or thread
        return self.save(
            replace(
                thread,
                title=thread.title or _thread_title(user_text),
                updated_at=observed_at,
                archived_at=archive_source.archived_at,
                archived_by=archive_source.archived_by,
                archive_reason=archive_source.archive_reason,
                messages=tuple(messages),
            )
        )

    def _threads(self) -> dict[str, CockpitThread]:
        return {
            thread_id: CockpitThread.from_dict(raw)
            for thread_id, raw in self._read().get("threads", {}).items()
            if isinstance(raw, dict)
        }

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"version": 1, "threads": {}}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {"version": 1, "threads": {}}
        if not isinstance(data, dict):
            return {"version": 1, "threads": {}}
        if not isinstance(data.get("threads"), dict):
            data["threads"] = {}
        data.setdefault("version", 1)
        return data


class CockpitMemoryView:
    """Memory facade for BrainSession.

    It injects live project/thread context through the same active-project prompt
    path while suppressing BrainSession's default Lane 1 personal write. The
    connector persists the authoritative thread messages explicitly afterward.
    """

    def __init__(
        self,
        backend: MemoryBackend,
        *,
        project_peer_id: str,
        project_context: str,
    ) -> None:
        self._backend = backend
        self._project_peer_id = project_peer_id
        self._project_context = project_context

    def read_cached_representation(self, user: str | None = None) -> str:
        if user == self._project_peer_id:
            return self._project_context
        return self._backend.read_cached_representation(user)

    async def write_turn(
        self,
        _user_text: str,
        _assistant_text: str,
        *,
        user: str | None = None,
        channel: str = "voice",
        device_id: str | None = None,
    ) -> None:
        return None

    async def refresh_cache(self, min_interval_s: float = 0.0, *, user: str | None = None) -> bool:
        if user == self._project_peer_id:
            return await self._backend.refresh_cache(min_interval_s=min_interval_s, user=user)
        return False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._backend, name)


class CockpitConnector:
    def __init__(
        self,
        cfg: Config,
        *,
        memory: MemoryBackend | None = None,
        gateway: Any | None = None,
        tts: Any | None = None,
        tracer: Any | None = None,
    ) -> None:
        self._cfg = cfg
        self._memory = memory or MemoryClient(cfg.memory)
        self._gateway = gateway
        self._tts = tts
        self._tracer = tracer or Tracer(cfg.trace)
        self._index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / THREAD_INDEX_FILENAME)

    @property
    def index(self) -> CockpitThreadIndex:
        return self._index

    def list_threads(self, project: ProjectEntry, *, include_archived: bool = False) -> list[CockpitThread]:
        return self._index.list(project.id, include_archived=include_archived)

    def archive_thread(self, project: ProjectEntry, thread_id: str, *, by: str = "", reason: str = "") -> CockpitThread | None:
        return self._index.set_archived(project.id, thread_id, archived=True, by=by, reason=reason)

    def unarchive_thread(self, project: ProjectEntry, thread_id: str) -> CockpitThread | None:
        return self._index.set_archived(project.id, thread_id, archived=False)

    async def open_thread(
        self,
        project: ProjectEntry,
        requester: RequestContext,
        *,
        title: str = "",
    ) -> CockpitThread:
        thread_id = new_id("thread")
        session_id = orchestrator_session_id(project.id, thread_id)
        now = utc_now()
        thread = CockpitThread(
            thread_id=thread_id,
            project_id=project.id,
            session_id=session_id,
            title=title.strip() or "New project thread",
            created_at=now,
            updated_at=now,
            created_by=requester.memory_peer,
        )
        peers = _thread_peers(self._cfg, project, requester)
        await asyncio.to_thread(
            self._memory.create_session,
            session_id,
            peers=peers,
            metadata={
                "kind": "cockpit_orchestrator",
                "project_id": project.id,
                "thread_id": thread_id,
                "created_by": requester.memory_peer,
                "created_at": now,
            },
        )
        return self._index.save(thread)

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
            channel="cockpit",
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
        ctx = _cockpit_project_context(requester)
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
        if self._cfg.background.enabled:
            async def notify_background(_text: str, _identity: str, _device_id: str) -> None:
                return None

            runner = BackgroundRunner(
                self._cfg.background,
                session_factory=lambda inner_ctx: self._make_session(inner_ctx, project=project, memory=memory),
                notify=notify_background,
            )
            tools.register(make_background_tool(runner))
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

    def _turn_gateway(self) -> Any:
        if self._gateway is None:
            self._gateway = GatewayClient(self._cfg.gateway)
        return self._gateway

    def _persist_turn(
        self,
        session_id: str,
        requester_peer_id: str,
        device_id: str | None,
        user_text: str,
        assistant_text: str,
    ) -> None:
        self._memory.create_session(
            session_id,
            peers=[SessionPeer(peer_id=requester_peer_id, observe_me=True, observe_others=True)],
        )
        user_metadata = {"channel": "cockpit", "role": "user", "observed_at": utc_now()}
        assistant_metadata = {"channel": "cockpit", "role": "assistant", "observed_at": utc_now()}
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


def orchestrator_session_id(project_id: str, thread_id: str) -> str:
    return f"project:{project_id}:orchestrator:{thread_id}"


def _thread_peers(
    cfg: Config,
    project: ProjectEntry,
    requester: RequestContext,
) -> list[SessionPeer]:
    return [
        SessionPeer(peer_id=project.peer_id, observe_me=True, observe_others=True),
        SessionPeer(peer_id=requester.memory_peer, observe_me=True, observe_others=True),
        SessionPeer(peer_id=cfg.memory.assistant_peer_id, observe_me=False, observe_others=True),
    ]


def _cockpit_project_context(requester: RequestContext) -> RequestContext:
    return RequestContext(
        device_id=requester.device_id,
        identity=requester.identity,
        scope=requester.scope,
        capabilities=requester.capabilities,
        channel="cockpit",
        confidence=requester.confidence,
        peer=requester.memory_peer,
    )


def _project_context(memory: MemoryBackend, project: ProjectEntry, thread: CockpitThread) -> str:
    parts = [
        "Project registry entry:\n" + json.dumps(project.as_dict(), sort_keys=True),
    ]
    representation = _safe_project_representation(memory, project.peer_id)
    if representation:
        parts.append("Live project representation:\n" + representation)
    conclusions = _safe_project_conclusions(memory, project)
    if conclusions:
        lines = [
            f"- {row['artifact_type']}: {row['content']} "
            f"(recorded_by={row['recorded_by']}, observed_at={row['observed_at']})"
            for row in conclusions
        ]
        parts.append("Recent explicit project conclusions:\n" + "\n".join(lines))
    if thread.messages:
        history = []
        for message in thread.messages[-THREAD_HISTORY_LIMIT:]:
            role = message.get("role", "")
            peer = message.get("peer_id", "")
            content = " ".join(message.get("content", "").split())
            if len(content) > 800:
                content = content[:799] + "..."
            history.append(f"- {role} ({peer}): {content}")
        parts.append("Recent orchestrator thread history:\n" + "\n".join(history))
    return "\n\n".join(parts)


def _safe_project_representation(memory: MemoryBackend, peer_id: str) -> str:
    cached = ""
    try:
        cached = str(memory.read_cached_representation(peer_id) or "")
    except Exception:
        cached = ""
    try:
        live = memory.read_representation(peer_id)
    except Exception:
        return cached
    return str(live.representation or cached)


def _safe_project_conclusions(memory: MemoryBackend, project: ProjectEntry) -> list[dict[str, str]]:
    rows: list[ConclusionRecord] = []
    for artifact_type in ("finding", "decision"):
        try:
            rows.extend(
                memory.list_conclusions(
                    observed_id=project.peer_id,
                    level="explicit",
                    metadata={"project_id": project.id, "artifact_type": artifact_type},
                )
            )
        except (UnsupportedMemoryOperation, TimeoutError, OSError, RuntimeError):
            continue
        except Exception:
            continue
    rows.sort(key=lambda row: (str(row.metadata.get("observed_at") or ""), row.id), reverse=True)
    return [
        {
            "id": row.id,
            "content": row.content,
            "artifact_type": str(row.metadata.get("artifact_type") or ""),
            "recorded_by": str(row.metadata.get("recorded_by") or ""),
            "observed_at": str(row.metadata.get("observed_at") or ""),
        }
        for row in rows[:20]
    ]


async def _drain_cold_tasks(session: BrainSession) -> None:
    tasks = tuple(task for task in session.pending_cold_tasks if not task.done())
    if not tasks:
        return
    await asyncio.gather(*tasks, return_exceptions=True)


def _thread_title(text: str) -> str:
    title = " ".join(text.split())
    if not title:
        return "Project thread"
    return title if len(title) <= 72 else title[:71] + "..."


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
