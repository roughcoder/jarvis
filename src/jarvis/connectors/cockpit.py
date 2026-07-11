"""Cockpit project-thread connector.

The Cockpit API is an HTTP/SSE boundary peer, but the turn itself still uses the
shared BrainSession text core. This module owns the glue: project-scoped thread
metadata, live project context assembly, BrainSession construction, and explicit
Lane 1 transcript writes into the named Honcho session.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import re
import tempfile
import threading
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from jarvis.capabilities import FORGE_PR_COMMENT, WORKER_SESSION_CREATE, WORKER_SESSION_STOP, WORKER_SESSION_TURN
from jarvis.brain.facade import (
    PROJECT_THREAD_TOOL_SURFACE_CONTRACT,
    ActiveProject,
    BackgroundRunner,
    BrainSession,
    ConclusionRecord,
    ContextStore,
    CurationOutbox,
    GatewayClient,
    MemoryBackend,
    MemoryClient,
    MemoryMessage,
    ProjectEntry,
    RegistryStore,
    RequestContext,
    SessionPeer,
    Tracer,
    TurnResult,
    UnsupportedMemoryOperation,
    make_memory_tools,
    make_project_tools,
)
from jarvis.config import Config
from jarvis.ids import new_id, utc_now
from jarvis.orchestration.cockpit import project_session_event
from jarvis.orchestration.models import WorkCommand, WorkItem
from jarvis.orchestration.store import OrchestrationStore
from jarvis.orchestration.service import StartedWork, OrchestrationService
from jarvis.orchestration.redaction import public_error_message
from jarvis.orchestration.workers import WorkerRegistry, worker_token_value
from jarvis.tools import build_registry
from jarvis.tools.background import make_background_tool
from jarvis.tools.base import Tool
from jarvis.text import slugify
from jarvis.users import load_users
from jarvis.worker_session_contract import EVENT_TOOL_CALL, EVENT_TOOL_RESULT


THREAD_INDEX_FILENAME = "cockpit-threads.json"
THREAD_TRANSCRIPTS_DIRNAME = "cockpit-thread-transcripts"
THREAD_HISTORY_LIMIT = 24
CHILD_WATCH_LEASE_S = 300
CHILD_WORK_LANDING_MODES = {"none", "branch_only", "draft_pr", "ready_pr", "confirm_before_pr"}
_THREAD_INDEX_LOCK = threading.RLock()
_WORK_SESSION_OFFER = (
    "I can't do that from this project conversation because it has no workspace, "
    "repo checkout, test runner, or code-review tool attached. Dispatch it through "
    "/v1/work/start as a work session and I can handle it there."
)
_WORK_ACTION_CLAIM_RE = re.compile(
    # Only the fabrication mode: a first-person present/future claim that THIS
    # conversation is doing untooled work on the real repo/tests right now
    # ("I'll start a code review", "I'm reviewing the repo", "I've started
    # running pytest"). No bare/third-person or backward-looking ("...done",
    # "...finished", "...completed") branches here — those match truthful status
    # reports about already-completed or child-run work (e.g. "the code review
    # is done — run_abc finished") and must not be clobbered.
    r"\b(?:i(?:'|’)ll|i will|i(?:'|’)m|i am|i(?:'|’)ve|i have)\s+"
    r"(?:start(?:ed|ing)?|begin(?:ning|begun)?|run(?:ning)?|review(?:ing)?|inspect(?:ing)?|check(?:ing)?|look(?:ing)?\s+through)"
    r"[^.?!\n]{0,120}\b(?:code\s+review|review|repo|repository|codebase|pull\s+request|pr|tests?|test\s+suite|pytest|ruff|lint|typecheck)\b",
    re.IGNORECASE,
)
_WORK_ACTION_TOOL_NAMES = {
    "publish_github_pr_review",
    "read_child_work_result",
    "spawn_child_work_session",
    "watch_child_work_sessions",
}


@dataclass(frozen=True)
class CockpitThread:
    thread_id: str
    project_id: str
    session_id: str
    title: str
    created_at: str
    updated_at: str
    created_by: str
    parent_chat_id: str = ""
    archived_at: str = ""
    archived_by: str = ""
    archive_reason: str = ""
    last_turn_at: str = ""
    messages: tuple[dict[str, Any], ...] = ()
    workspace: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any], *, include_messages: bool = True) -> "CockpitThread":
        return cls(
            thread_id=str(data.get("thread_id") or ""),
            project_id=str(data.get("project_id") or ""),
            session_id=str(data.get("session_id") or ""),
            title=str(data.get("title") or ""),
            created_at=str(data.get("created_at") or ""),
            updated_at=str(data.get("updated_at") or ""),
            created_by=str(data.get("created_by") or ""),
            parent_chat_id=str(data.get("parent_chat_id") or ""),
            archived_at=str(data.get("archived_at") or ""),
            archived_by=str(data.get("archived_by") or ""),
            archive_reason=str(data.get("archive_reason") or ""),
            last_turn_at=str(data.get("last_turn_at") or ""),
            messages=tuple(_normalized_messages(data.get("messages") or ())) if include_messages else (),
            workspace=dict(data.get("workspace") or {}),
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
            "parent_chat_id": self.parent_chat_id,
            "archived_at": self.archived_at,
            "archived_by": self.archived_by,
            "archive_reason": self.archive_reason,
            "last_turn_at": self.last_turn_at,
        }
        if include_messages:
            data["messages"] = [dict(message) for message in self.messages]
        if self.workspace:
            data["workspace"] = dict(self.workspace)
        return data


class CockpitThreadIndex:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()
        self.transcripts_dir = self.path.parent / THREAD_TRANSCRIPTS_DIRNAME

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

    def get_with_messages(
        self,
        project_id: str,
        thread_id: str,
        *,
        limit: int | None = None,
    ) -> CockpitThread | None:
        thread = self.get(project_id, thread_id)
        if thread is None:
            return None
        messages = self._thread_messages(thread, limit=limit)
        return replace(thread, messages=tuple(messages))

    def save(self, thread: CockpitThread) -> CockpitThread:
        with _THREAD_INDEX_LOCK:
            data = self._read()
            threads = data.setdefault("threads", {})
            threads[thread.thread_id] = thread.as_dict(include_messages=False)
            _atomic_write_json(self.path, data)
            return replace(thread, messages=())

    def set_archived(
        self,
        project_id: str,
        thread_id: str,
        *,
        archived: bool,
        by: str = "",
        reason: str = "",
    ) -> CockpitThread | None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return None
            if archived and thread.archived_at:
                return thread
            if archived:
                self.promote_children(thread.thread_id)
            archive_reason = reason.strip()[:500]
            updated = replace(
                thread,
                archived_at=utc_now() if archived else "",
                archived_by=by if archived else "",
                archive_reason=archive_reason if archived else "",
            )
            return self.save(updated)

    def delete(self, project_id: str, thread_id: str) -> tuple[CockpitThread | None, bool]:
        # Hold the same lock as every other mutator: an unlocked read-modify-write
        # here can race a concurrent append_turn/save and resurrect a just-deleted
        # thread (its transcript already unlinked) or clobber a concurrent
        # rename/promotion.
        with _THREAD_INDEX_LOCK:
            data = self._read()
            threads = data.setdefault("threads", {})
            deleted = data.setdefault("deleted_threads", {})
            raw = threads.pop(thread_id, None)
            if isinstance(raw, dict) and str(raw.get("project_id") or "") == project_id:
                thread = CockpitThread.from_dict(raw)
                # Messages may live in the transcript file, inline, or the legacy key;
                # count them all for the reclamation summary, then reclaim the file.
                messages = self._thread_messages(thread)
                deleted[thread_id] = {
                    **thread.as_dict(include_messages=False),
                    "deleted_at": utc_now(),
                }
                _atomic_write_json(self.path, data)
                try:
                    self._transcript_path(project_id, thread_id).unlink(missing_ok=True)
                except OSError:
                    pass
                return replace(thread, messages=tuple(messages)), True
            if raw is not None:
                threads[thread_id] = raw
            tombstone = deleted.get(thread_id)
            if isinstance(tombstone, dict) and str(tombstone.get("project_id") or "") == project_id:
                return CockpitThread.from_dict(tombstone), False
            return None, False
    def rename(self, project_id: str, thread_id: str, title: str) -> CockpitThread | None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return None
            clean_title = " ".join(title.split())
            if not clean_title:
                raise ValueError("title is required")
            return self.save(replace(thread, title=clean_title, updated_at=utc_now()))

    def promote_children(self, parent_chat_id: str) -> list[CockpitThread]:
        with _THREAD_INDEX_LOCK:
            promoted: list[CockpitThread] = []
            for thread in self._threads().values():
                if thread.parent_chat_id != parent_chat_id:
                    continue
                promoted.append(self.save(replace(thread, parent_chat_id="", updated_at=utc_now())))
            return promoted

    def append_child_terminal_system_message(self, parent_chat_id: str, child: Any) -> bool:
        with _THREAD_INDEX_LOCK:
            thread = next((item for item in self._threads().values() if item.thread_id == parent_chat_id), None)
            if thread is None:
                return False
            messages = self._thread_messages(thread)
            if any(
                message.get("type") == "child_terminal"
                and message.get("child_chat_id") == child.run_id
                and message.get("phase") == child.phase
                for message in messages
            ):
                return True
            observed_at = utc_now()
            reason = f": {child.terminal_reason}" if child.terminal_reason else ""
            messages.append(
                {
                    "role": "system",
                    "peer_id": "jarvis",
                    "type": "child_terminal",
                    "content": f"Child {child.objective} ({child.run_id}) reached {child.phase}{reason}.",
                    "observed_at": observed_at,
                    "child_chat_id": child.run_id,
                    "child_run_id": child.run_id,
                    "phase": child.phase,
                    "status": child.status,
                    "terminal_reason": child.terminal_reason,
                }
            )
            updated = self.save(replace(thread, updated_at=observed_at))
            self._write_thread_messages(updated, messages)
            return True

    def register_child_watch(
        self,
        thread: CockpitThread,
        child_ids: list[str],
        *,
        requester: RequestContext,
    ) -> str:
        with _THREAD_INDEX_LOCK:
            stored = self.get(thread.project_id, thread.thread_id) or thread
            messages = self._thread_messages(stored)
            normalized = sorted(set(child_ids))
            watch_id = hashlib.sha256("\n".join(normalized).encode("utf-8")).hexdigest()[:20]
            if not any(message.get("type") == "child_watch" and message.get("watch_id") == watch_id for message in messages):
                messages.append(
                    {
                        "role": "system",
                        "peer_id": "jarvis",
                        "type": "child_watch",
                        "watch_id": watch_id,
                        "child_chat_ids": normalized,
                        "requester": {
                            "device_id": requester.device_id,
                            "identity": requester.identity,
                            "scope": requester.scope,
                            "capabilities": sorted(requester.capabilities),
                            "channel": requester.channel,
                            "confidence": requester.confidence,
                            "peer": requester.peer,
                        },
                        "phase": "waiting",
                        "content": f"Watching {len(normalized)} child work session(s) for completion.",
                        "observed_at": utc_now(),
                    }
                )
                self._write_thread_messages(stored, messages)
            return watch_id

    def claim_ready_child_watch(self, parent_chat_id: str, terminal_child_ids: set[str]) -> dict[str, Any] | None:
        with _THREAD_INDEX_LOCK:
            thread = next((item for item in self._threads().values() if item.thread_id == parent_chat_id), None)
            if thread is None:
                return None
            messages = self._thread_messages(thread)
            claimed: dict[str, Any] | None = None
            for message in messages:
                expected = {str(item) for item in message.get("child_chat_ids") or []}
                phase = str(message.get("phase") or "")
                lease_expired = phase == "claimed" and _timestamp_before(
                    str(message.get("claimed_at") or ""),
                    datetime.now(UTC) - timedelta(seconds=CHILD_WATCH_LEASE_S),
                )
                if (
                    message.get("type") == "child_watch"
                    and (phase == "waiting" or lease_expired)
                    and expected
                    and expected <= terminal_child_ids
                ):
                    message["phase"] = "claimed"
                    message["claimed_at"] = utc_now()
                    claimed = dict(message)
                    break
            if claimed is not None:
                self._write_thread_messages(thread, messages)
            return claimed

    def finish_child_watch(self, parent_chat_id: str, watch_id: str, *, error: str = "") -> None:
        with _THREAD_INDEX_LOCK:
            thread = next((item for item in self._threads().values() if item.thread_id == parent_chat_id), None)
            if thread is None:
                return
            messages = self._thread_messages(thread)
            for message in messages:
                if message.get("type") == "child_watch" and message.get("watch_id") == watch_id:
                    message["phase"] = "failed" if error else "completed"
                    message["completed_at"] = utc_now()
                    if error:
                        message["error"] = public_error_message(error)
                    break
            self._write_thread_messages(thread, messages)

    def append_turn(
        self,
        thread: CockpitThread,
        *,
        user_peer_id: str,
        user_text: str,
        assistant_peer_id: str,
        assistant_text: str,
        events: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    ) -> CockpitThread:
        with _THREAD_INDEX_LOCK:
            observed_at = utc_now()
            stored = self.get(thread.project_id, thread.thread_id)
            archive_source = stored or thread
            messages = [
                *self._thread_messages(archive_source, seed_messages=thread.messages),
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
                *_thread_event_messages(events, assistant_peer_id=assistant_peer_id),
            ]
            updated = self.save(
                replace(
                    thread,
                    title=thread.title or _thread_title(user_text),
                    updated_at=observed_at,
                    archived_at=archive_source.archived_at,
                    archived_by=archive_source.archived_by,
                    archive_reason=archive_source.archive_reason,
                    last_turn_at=observed_at,
                    messages=(),
                )
            )
            self._write_thread_messages(updated, messages)
            return replace(updated, messages=tuple(messages))

    def append_pending_turn(
        self,
        thread: CockpitThread,
        *,
        user_peer_id: str,
        user_text: str,
        assistant_peer_id: str,
    ) -> CockpitThread:
        observed_at = utc_now()
        stored = self.get(thread.project_id, thread.thread_id)
        archive_source = stored or thread
        messages = [
            *self._thread_messages(archive_source, seed_messages=thread.messages),
            {
                "role": "user",
                "peer_id": user_peer_id,
                "content": user_text,
                "observed_at": observed_at,
            },
            {
                "role": "assistant",
                "peer_id": assistant_peer_id,
                "content": "[workspace turn pending]",
                "observed_at": observed_at,
            },
        ]
        updated = self.save(
            replace(
                thread,
                title=thread.title or _thread_title(user_text),
                updated_at=observed_at,
                archived_at=archive_source.archived_at,
                archived_by=archive_source.archived_by,
                archive_reason=archive_source.archive_reason,
                last_turn_at=observed_at,
                messages=(),
            )
        )
        self._write_thread_messages(updated, messages)
        return replace(updated, messages=tuple(messages))

    def _threads(self, *, include_messages: bool = False) -> dict[str, CockpitThread]:
        return {
            thread_id: CockpitThread.from_dict(raw, include_messages=include_messages)
            for thread_id, raw in self._read().get("threads", {}).items()
            if isinstance(raw, dict)
        }

    def _thread_messages(
        self,
        thread: CockpitThread,
        *,
        limit: int | None = None,
        seed_messages: tuple[dict[str, Any], ...] = (),
    ) -> list[dict[str, Any]]:
        messages = self._read_thread_messages(thread)
        if not messages:
            messages = [dict(message) for message in seed_messages]
        if limit is not None:
            if limit <= 0:
                return []
            messages = messages[-limit:]
        return messages

    def _read_thread_messages(self, thread: CockpitThread) -> list[dict[str, Any]]:
        path = self._transcript_path(thread.project_id, thread.thread_id)
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
        if not isinstance(data, dict):
            return []
        return _normalized_messages(data.get("messages") or ())

    def _write_thread_messages(self, thread: CockpitThread, messages: list[dict[str, Any]]) -> None:
        _atomic_write_json(
            self._transcript_path(thread.project_id, thread.thread_id),
            {
                "version": 1,
                "project_id": thread.project_id,
                "thread_id": thread.thread_id,
                "messages": messages,
            },
        )

    def _transcript_path(self, project_id: str, thread_id: str) -> Path:
        return self.transcripts_dir / _safe_path_segment(project_id) / f"{_safe_path_segment(thread_id)}.json"

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
        if not isinstance(data.get("deleted_threads"), dict):
            data["deleted_threads"] = {}
        data.setdefault("version", 1)
        if self._migrate_legacy_messages(data):
            _atomic_write_json(self.path, data)
        return data

    def _migrate_legacy_messages(self, data: dict[str, Any]) -> bool:
        changed = False
        threads = data.get("threads") if isinstance(data.get("threads"), dict) else {}
        for thread_id, raw in threads.items():
            if not isinstance(raw, dict) or "messages" not in raw:
                continue
            messages = _normalized_messages(raw.get("messages") or ())
            project_id = str(raw.get("project_id") or "")
            if messages and project_id:
                path = self._transcript_path(project_id, str(thread_id))
                if not path.exists():
                    _atomic_write_json(
                        path,
                        {
                            "version": 1,
                            "project_id": project_id,
                            "thread_id": str(thread_id),
                            "messages": messages,
                        },
                    )
            raw.pop("messages", None)
            changed = True
        return changed


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
        worker_post: Callable[..., Any] | None = None,
        worker_get: Callable[..., Any] | None = None,
    ) -> None:
        self._cfg = cfg
        self._memory = memory or MemoryClient(cfg.memory)
        self._gateway = gateway
        self._tts = tts
        self._tracer = tracer or Tracer(cfg.trace)
        self._worker_post = worker_post or httpx.post
        self._worker_get = worker_get or httpx.get
        self._index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / THREAD_INDEX_FILENAME)
        self._worker_registry: WorkerRegistry | None = None
        self._curation_outbox = CurationOutbox(
            self._cfg.memory.curation_outbox_path,
            max_retries=self._cfg.memory.curation_outbox_max_retries,
            backoff_initial_s=self._cfg.memory.curation_outbox_backoff_initial_s,
            backoff_max_s=self._cfg.memory.curation_outbox_backoff_max_s,
        )

    @property
    def index(self) -> CockpitThreadIndex:
        return self._index

    def _registry(self) -> WorkerRegistry:
        # Built once and reused: WorkerRegistry() re-reads workers.json from disk on
        # construction, and each workspace turn used to make one per worker HTTP call
        # (~5+ per turn) for no reason.
        if self._worker_registry is None:
            self._worker_registry = WorkerRegistry(
                self._cfg.worker,
                profiles_path=self._cfg.orchestration.workers_path,
                http_get=self._worker_get,
            )
        return self._worker_registry

    def list_threads(self, project: ProjectEntry, *, include_archived: bool = False) -> list[CockpitThread]:
        return self._index.list(project.id, include_archived=include_archived)

    def archive_thread(self, project: ProjectEntry, thread_id: str, *, by: str = "", reason: str = "") -> CockpitThread | None:
        return self._index.set_archived(project.id, thread_id, archived=True, by=by, reason=reason)

    def unarchive_thread(self, project: ProjectEntry, thread_id: str) -> CockpitThread | None:
        return self._index.set_archived(project.id, thread_id, archived=False)

    def rename_thread(self, project: ProjectEntry, thread_id: str, title: str) -> CockpitThread | None:
        thread = self._index.get(project.id, thread_id)
        if thread is None:
            return None
        title = " ".join(title.split())
        if not title or title == thread.title:
            return thread
        return self._index.save(replace(thread, title=title[:200], updated_at=utc_now()))
    def delete_thread(self, project: ProjectEntry, thread_id: str) -> tuple[CockpitThread | None, bool]:
        return self._index.delete(project.id, thread_id)

    async def open_thread(
        self,
        project: ProjectEntry,
        requester: RequestContext,
        *,
        title: str = "",
        parent_chat_id: str = "",
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
            parent_chat_id=parent_chat_id.strip(),
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
                "parent_chat_id": parent_chat_id.strip(),
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
        *,
        attachments: list[dict[str, Any]] | None = None,
        workspace_request: dict[str, Any] | None = None,
        progress: Callable[[dict[str, Any]], Any] | None = None,
    ) -> tuple[str, CockpitThread, tuple[dict[str, Any], ...]]:
        text = text.strip()
        if not text:
            raise ValueError("turn text is required")
        context_thread = self._index.get_with_messages(project.id, thread.thread_id, limit=THREAD_HISTORY_LIMIT) or thread
        explicit_workspace_request = workspace_request is not None
        workspace_request = dict(workspace_request or {})
        if context_thread.workspace or explicit_workspace_request:
            reply, updated = await self._workspace_turn(
                project,
                context_thread,
                requester,
                _text_with_attachment_markers(text, attachments),
                workspace_request=workspace_request,
                progress=progress,
            )
            return reply, updated, ()
        project_context = _project_context(self._memory, project, context_thread)
        view = CockpitMemoryView(
            self._memory,
            project_peer_id=project.peer_id,
            project_context=project_context,
        )
        session = self._make_session(
            requester,
            project=project,
            memory=view,
            thread=thread,
        )
        trace = self._tracer.turn(
            room=self._cfg.gateway.room,
            speaker=requester.identity,
            channel="cockpit",
            device_id=requester.device_id,
        )
        result = TurnResult()
        reply = await session.respond_text(text, trace, result, attachments=attachments)
        events = _safe_thread_tool_events(thread.session_id, result.tool_messages)
        guarded_reply = _guard_project_thread_reply(result.raw or reply, events)
        if guarded_reply != (result.raw or reply):
            result.raw = guarded_reply
        session.finalize(text, result, trace)
        await _drain_cold_tasks(session)
        if self._tracer is not None:
            self._tracer.emit(trace)
        reply = result.reply or reply
        # Durable stores (transcript + memory) keep the text plus a compact
        # marker per image — later turns and the deriver know an image was
        # shared, but base64 payloads live only in the gateway request.
        persisted_text = _text_with_attachment_markers(text, attachments)
        # Best-effort per AGENTS.md: tracing/memory must never break a turn. The
        # worker/reply already happened, so a Honcho outage here must not raise
        # out of the turn handler and turn a delivered reply into a user-facing
        # error.
        try:
            await asyncio.to_thread(
                self._persist_turn,
                thread.session_id,
                requester.memory_peer,
                requester.device_id,
                persisted_text,
                reply,
            )
        except Exception:
            pass
        updated = self._index.append_turn(
            thread,
            user_peer_id=requester.memory_peer,
            user_text=persisted_text,
            assistant_peer_id=self._cfg.memory.assistant_peer_id,
            assistant_text=reply,
            events=events,
        )
        return reply, updated, events

    async def _workspace_turn(
        self,
        project: ProjectEntry,
        thread: CockpitThread,
        requester: RequestContext,
        text: str,
        *,
        workspace_request: dict[str, Any],
        progress: Callable[[dict[str, Any]], Any] | None,
    ) -> tuple[str, CockpitThread]:
        if progress is not None:
            progress({"phase": "resolving-access", "thread_id": thread.thread_id})
        thread = await self._ensure_workspace(project, thread, requester, workspace_request=workspace_request, progress=progress)
        worker_id = str(thread.workspace.get("worker_id") or "")
        session_id = str(thread.workspace.get("session_id") or "")
        if not worker_id or not session_id:
            raise RuntimeError("conversation workspace has no worker session")
        if progress is not None:
            progress({"phase": "running", "thread_id": thread.thread_id, "workspace": workspace_public(thread.workspace)})
        turn = await asyncio.to_thread(
            self._post_worker_json,
            worker_id,
            f"/sessions/{session_id}/turns",
            {
                "prompt": _workspace_prompt(project, thread, text),
                "metadata": {
                    "surface": "cockpit_thread",
                    "project_id": project.id,
                    "thread_id": thread.thread_id,
                    "honcho_session_id": thread.session_id,
                    "allowed_actions": [WORKER_SESSION_TURN],
                },
                # `thread` here comes from _ensure_workspace, whose index round-trip
                # always returns messages=() (CockpitThreadIndex.save() strips them),
                # so a len(thread.messages) key would be constant and every repeat of
                # the same text would collide as a "replay" the worker never re-runs.
                # A fresh id() per call keeps each real turn distinct; the cockpit
                # doesn't retry the same turn object, so there is no dedupe need here.
                "idempotency_key": f"thread-turn:{thread.thread_id}:{new_id('turn')}:{_stable_text_hash(text)}",
            },
        )
        if not turn.get("ok", True):
            raise RuntimeError(str(turn.get("error") or "worker rejected conversation turn"))
        reply = "Workspace turn is running."
        # Best-effort per AGENTS.md: a memory outage here must not raise out of
        # the turn handler after the worker turn was already dispatched — that
        # would surface an error to the user for a turn that is actually running,
        # and skip recording the pending-turn marker below.
        try:
            await asyncio.to_thread(
                self._persist_user_turn,
                thread.session_id,
                requester.memory_peer,
                requester.device_id,
                text,
            )
        except Exception:
            pass
        updated = self._index.append_pending_turn(
            thread,
            user_peer_id=requester.memory_peer,
            user_text=text,
            assistant_peer_id=self._cfg.memory.assistant_peer_id,
        )
        return reply, updated

    async def _ensure_workspace(
        self,
        project: ProjectEntry,
        thread: CockpitThread,
        requester: RequestContext,
        *,
        workspace_request: dict[str, Any],
        progress: Callable[[dict[str, Any]], Any] | None,
    ) -> CockpitThread:
        # Skip the full re-handshake (worker choose()/probe, workspace POST, worktree
        # POSTs, session create-or-get, index saves) when the workspace is already
        # provisioned and this call isn't asking for a repo it doesn't already have.
        # Every user turn in an existing workspace thread used to pay ~5+ sequential
        # worker round-trips for nothing.
        if _workspace_is_ready(thread) and not _workspace_needs_new_repo(project, thread, workspace_request):
            return thread
        state = dict(thread.workspace or {})
        try:
            worker_id = str(state.get("worker_id") or workspace_request.get("worker_id") or "")
            profile = self._registry().choose(
                required=["git"],
                preferred=[worker_id] if worker_id else None,
                engine=str(workspace_request.get("engine") or ""),
            )
            if profile is None:
                raise RuntimeError("no eligible worker found for conversation workspace")
            worker_id = profile.worker_id
            conversation_id = _conversation_workspace_id(project, thread)
            thread = self._index.save(
                replace(
                    thread,
                    workspace={
                        **state,
                        "worker_id": worker_id,
                        "workspace_id": conversation_id,
                        "provision_phase": "resolving-access",
                        "status": "provisioning",
                    },
                )
            )
            workspace = await asyncio.to_thread(
                self._post_worker_json,
                worker_id,
                "/conversation-workspaces",
                {
                    "conversation_id": conversation_id,
                    "metadata": {
                        "project_id": project.id,
                        "thread_id": thread.thread_id,
                        "honcho_session_id": thread.session_id,
                        "created_by": requester.memory_peer,
                    },
                },
            )
            workspace_state = dict(workspace.get("workspace") or {})
            thread = self._index.save(
                replace(
                    thread,
                    workspace={
                        **state,
                        "worker_id": worker_id,
                        **workspace_state,
                    },
                )
            )
            repos = _workspace_repos(project, workspace_request)
            for repo in repos:
                thread = self._index.save(
                    replace(
                        thread,
                        workspace={
                            **thread.workspace,
                            "provision_phase": "cloning",
                            "status": "provisioning",
                        },
                    )
                )
                if progress is not None:
                    progress({"phase": "cloning", "thread_id": thread.thread_id, "repo": repo.get("name") or repo.get("repo")})
                thread = self._index.save(
                    replace(
                        thread,
                        workspace={
                            **thread.workspace,
                            "provision_phase": "creating-worktree",
                            "status": "provisioning",
                        },
                    )
                )
                if progress is not None:
                    progress({"phase": "creating-worktree", "thread_id": thread.thread_id, "repo": repo.get("name") or repo.get("repo")})
                materialized = await asyncio.to_thread(
                    self._post_worker_json,
                    worker_id,
                    f"/conversation-workspaces/{conversation_id}/worktrees",
                    repo,
                )
                workspace_state = dict(materialized.get("workspace") or workspace_state)
                thread = self._index.save(
                    replace(
                        thread,
                        workspace={
                            **thread.workspace,
                            **workspace_state,
                        },
                    )
                )
            session_id = str(state.get("session_id") or f"conv_{slugify(thread.thread_id)}")
            engine = str(workspace_request.get("engine") or profile.default_engine or profile.agent or self._cfg.worker.agent)
            await asyncio.to_thread(
                self._ensure_worker_session,
                worker_id,
                session_id,
                {
                    "session_id": session_id,
                    "provider": engine,
                    "engine": engine,
                    "cwd": str(workspace_state.get("root") or ""),
                    "title": thread.title or project.name,
                    "metadata": {
                        "execution_envelope": {
                            "run_id": thread.thread_id,
                            "engine": engine,
                            "allowed_actions": [WORKER_SESSION_CREATE, WORKER_SESSION_TURN, WORKER_SESSION_STOP],
                            "landing": {"mode": "branch_only", "allow_merge": False},
                        },
                        "allowed_actions": [WORKER_SESSION_CREATE, WORKER_SESSION_TURN, WORKER_SESSION_STOP],
                        "landing": {"mode": "branch_only", "allow_merge": False},
                        "conversation_workspace": True,
                        "workspace_id": workspace_state.get("workspace_id") or "",
                        "project_id": project.id,
                        "thread_id": thread.thread_id,
                        "honcho_session_id": thread.session_id,
                    },
                },
            )
            updated = replace(
                thread,
                workspace={
                    **state,
                    "worker_id": worker_id,
                    "session_id": session_id,
                    "engine": engine,
                    **workspace_state,
                },
            )
            return self._index.save(updated)
        except Exception:
            self._index.save(
                replace(
                    thread,
                    workspace={
                        **thread.workspace,
                        "status": "failed",
                        "provision_phase": "failed",
                        "updated_at": utc_now(),
                    },
                )
            )
            raise

    def _ensure_worker_session(self, worker_id: str, session_id: str, body: dict[str, Any]) -> dict[str, Any]:
        try:
            return self._post_worker_json(worker_id, "/sessions", body)
        except RuntimeError as exc:
            if "already exists" not in str(exc):
                raise
            return self._get_worker_json(worker_id, f"/sessions/{session_id}")

    def _post_worker_json(self, worker_id: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        profile = self._registry().get(worker_id, probe=False)
        if profile is None:
            raise RuntimeError(f"worker {worker_id!r} is not configured")
        response = self._worker_post(
            f"{profile.base_url}{path}",
            json=body,
            headers=_worker_headers(self._cfg.worker, profile),
            timeout=self._cfg.worker.request_timeout_s,
        )
        data = _json_response(response)
        if getattr(response, "status_code", 200) >= 400 or data.get("ok") is False:
            raise RuntimeError(str(data.get("error") or getattr(response, "text", "") or "worker request failed"))
        return data

    def _get_worker_json(self, worker_id: str, path: str) -> dict[str, Any]:
        profile = self._registry().get(worker_id, probe=False)
        if profile is None:
            raise RuntimeError(f"worker {worker_id!r} is not configured")
        response = self._worker_get(
            f"{profile.base_url}{path}",
            headers=_worker_headers(self._cfg.worker, profile),
            timeout=self._cfg.worker.request_timeout_s,
        )
        data = _json_response(response)
        if getattr(response, "status_code", 200) >= 400:
            raise RuntimeError(str(data.get("error") or getattr(response, "text", "") or "worker request failed"))
        return data

    def _make_session(
        self,
        requester: RequestContext,
        *,
        project: ProjectEntry,
        memory: MemoryBackend,
        thread: CockpitThread | None = None,
    ) -> BrainSession:
        ctx = _cockpit_project_context(requester)
        contexts = ContextStore(lambda _ctx: None)  # type: ignore[arg-type]
        active = ActiveProject(id=project.id, name=project.name, peer_id=project.peer_id)
        contexts.set_active_project(ctx, active)
        registry_store = RegistryStore(
            self._cfg.registry.path,
            memory=memory,
            curation_outbox=self._curation_outbox,
        )
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
        for tool in make_memory_tools(
            self._cfg.memory,
            memory=memory,
            outbox=self._curation_outbox,
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
        if thread is not None:
            tools.register(_spawn_child_work_tool(self._cfg, project, thread))
            tools.register(_read_child_work_result_tool(self._cfg, project, thread))
            tools.register(_publish_github_pr_review_tool(self._cfg, project))
            tools.register(_watch_child_work_sessions_tool(self._cfg, project, thread))
        if self._cfg.background.enabled:
            async def notify_background(_text: str, _identity: str, _device_id: str) -> None:
                return None

            runner = BackgroundRunner(
                self._cfg.background,
                session_factory=lambda inner_ctx: self._make_session(inner_ctx, project=project, memory=memory, thread=thread),
                notify=notify_background,
            )
            tools.register(make_background_tool(runner))
        session_cfg = self._session_config()
        session = BrainSession(
            session_cfg,
            ctx,
            gateway=self._turn_gateway(),
            tts=self._tts,
            memory=memory,
            tracer=self._tracer,
            registry=tools,
            memory_user=ctx.memory_peer,
            active_project_getter=lambda: active,
            extra_system_prompt=PROJECT_THREAD_TOOL_SURFACE_CONTRACT,
        )
        session.load_soul()
        return session

    def _session_config(self) -> Config:
        model = self._cfg.orchestration.orchestrator_model.strip()
        if not model or model == self._cfg.gateway.strong_model:
            return self._cfg
        # This is a shallow top-level copy on purpose: project orchestrator turns
        # start on gateway.strong_model for cockpit text channels. The gateway
        # config has no generic "model" route; fast/voice stay shared so cheap
        # helpers and discovery projections keep their normal routing semantics.
        cfg = copy.copy(self._cfg)
        cfg.gateway = self._cfg.gateway.model_copy(update={"strong_model": model})
        return cfg

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

    def _persist_user_turn(
        self,
        session_id: str,
        requester_peer_id: str,
        device_id: str | None,
        user_text: str,
    ) -> None:
        self._memory.create_session(
            session_id,
            peers=[SessionPeer(peer_id=requester_peer_id, observe_me=True, observe_others=True)],
        )
        user_metadata = {"channel": "cockpit", "role": "user", "observed_at": utc_now(), "workspace_turn": True}
        if device_id:
            user_metadata["device_id"] = device_id
        self._memory.create_messages(
            session_id,
            [
                MemoryMessage(
                    peer_id=requester_peer_id,
                    content=user_text,
                    metadata=user_metadata,
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


def _thread_event_messages(
    events: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    assistant_peer_id: str,
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        event_type = str(event.get("type") or "")
        name = _thread_event_tool_name(event)
        content = " ".join(part for part in (event_type, name) if part)
        messages.append(
            {
                "role": "event",
                "peer_id": assistant_peer_id,
                "content": content,
                "observed_at": str(event.get("occurred_at") or ""),
                "event": dict(event),
            }
        )
    return messages


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
    if thread.workspace:
        parts.append("Conversation workspace:\n" + json.dumps(workspace_public(thread.workspace), sort_keys=True))
    else:
        parts.append(
            "Conversation workspace: planning-only. No checkout is materialized for this "
            "thread yet, so do not claim to inspect repository files. If code access is "
            "needed, say that the conversation needs to escalate to a workspace first."
        )
    return "\n\n".join(parts)


def _spawn_child_work_tool(cfg: Config, project: ProjectEntry, thread: CockpitThread) -> Tool:
    class ManualSource:
        def __init__(self, item: WorkItem) -> None:
            self.item = item

        def list(self, *, repo: str = "", filters: dict | None = None, limit: int = 10) -> list[WorkItem]:
            return [self.item]

        def next(self, *, repo: str = "", filters: dict | None = None) -> WorkItem:
            return self.item

    async def spawn(ctx: RequestContext, args: dict[str, Any]) -> str:
        task = str(args.get("task") or args.get("prompt") or "").strip()
        title = str(args.get("title") or args.get("name") or task).strip()
        if not task:
            return "error: task is required"
        repo = str(args.get("repo") or _project_default_repo(project) or cfg.orchestration.default_repo).strip()
        item = WorkItem(
            source="manual",
            id=new_id("manual"),
            title=title or "Child work session",
            body=task,
            repo=repo,
            kind="manual",
        )
        worker_id = str(args.get("worker_id") or "").strip()
        provider_instance_id = str(args.get("provider_instance_id") or "").strip()
        command = WorkCommand(
            operation="start_next_work",
            source="manual",
            filters={"project_id": project.id},
            target_worker_id=worker_id,
            target_engine_id=str(args.get("engine") or ""),
            target_model_id=str(args.get("model") or ""),
            provider_instance_id=provider_instance_id,
            start=True,
        )
        try:
            child_cfg = _child_work_config(cfg, args)
        except ValueError as exc:
            return f"error: {exc}"
        service = OrchestrationService(
            cfg=child_cfg,
            capabilities=set(ctx.capabilities),
            source_factory=lambda _name, _cfg=None: ManualSource(item),
            thread_child_terminal_notifier=make_child_terminal_notifier(child_cfg),
            thread_children_promoter=CockpitThreadIndex(
                Path(child_cfg.orchestration.workspace) / THREAD_INDEX_FILENAME
            ).promote_children,
        )
        try:
            result = await asyncio.to_thread(service.next_work, command, start=True, parent_chat_id=thread.thread_id)
        except Exception as exc:  # noqa: BLE001 - tool output must stay concise and non-crashing
            return f"error: could not spawn child work session ({public_error_message(str(exc))})"
        if not isinstance(result, StartedWork):
            return "error: no child work session was spawned"
        return (
            f"Spawned child chat {result.envelope.run_id} under {thread.thread_id}. "
            f"Worker session {result.session.session_id} is {result.session.status}."
        )

    return Tool(
        "spawn_child_work_session",
        "Spawn a child Jarvis work session under this orchestrator chat.",
        {
            "type": "object",
            "properties": {
                "task": {"type": "string", "description": "The work instruction for the child agent."},
                "title": {"type": "string", "description": "Short child chat title."},
                "repo": {"type": "string", "description": "Repository name such as roughcoder/jarvis."},
                "worker_id": {"type": "string", "description": "Optional target worker id."},
                "provider_instance_id": {
                    "type": "string",
                    "description": "Optional explicit cockpit provider instance id, preserved independently of fleet worker routing.",
                },
                "engine": {"type": "string", "description": "Optional worker engine route, e.g. codex or claude."},
                "model": {"type": "string", "description": "Optional explicit provider model id."},
                "landing_mode": {
                    "type": "string",
                    "enum": sorted(CHILD_WORK_LANDING_MODES),
                    "description": "Child delivery policy. Defaults to none so review and analysis children stay read-only.",
                },
            },
            "required": ["task"],
        },
        "worker.session.create",
        spawn,
        announce=True,
        timeout_s=cfg.worker.request_timeout_s + 5,
    )


def _child_work_config(cfg: Config, args: dict[str, Any]) -> Config:
    landing_mode = str(args.get("landing_mode") or "none").strip()
    if landing_mode not in CHILD_WORK_LANDING_MODES:
        raise ValueError(f"unsupported child landing_mode: {landing_mode}")
    child_cfg = copy.deepcopy(cfg)
    child_cfg.orchestration.landing_mode = landing_mode
    return child_cfg


def _read_child_work_result_tool(cfg: Config, project: ProjectEntry, thread: CockpitThread) -> Tool:
    async def read_result(_ctx: RequestContext, args: dict[str, Any]) -> str:
        child_id = str(args.get("child_chat_id") or args.get("child_run_id") or "").strip()
        if not child_id:
            return "error: child_chat_id is required"
        store = OrchestrationStore(cfg.orchestration.workspace)
        child = await asyncio.to_thread(store.get, child_id)
        if child is None:
            return "error: no such child work session"
        if child.parent_chat_id != thread.thread_id and child.parent_run_id != thread.thread_id:
            return "error: child work session does not belong to this orchestrator chat"
        if child.project_id != project.id:
            return "error: child work session does not belong to this project"
        if child.status != "terminal":
            return json.dumps(
                {
                    "child_chat_id": child.run_id,
                    "phase": child.phase,
                    "status": child.status,
                    "ready": False,
                },
                sort_keys=True,
            )
        events = await asyncio.to_thread(store.events, child.run_id)
        messages: list[dict[str, str]] = []
        for event in events:
            if event.type != "assistant.message" or not isinstance(event.data, dict):
                continue
            data = event.data.get("data") if isinstance(event.data.get("data"), dict) else {}
            text = str(data.get("text") or "").strip()
            if not text:
                continue
            messages.append(
                {
                    "text": text[:12_000],
                    "time": str(event.data.get("time") or event.time or ""),
                }
            )
        messages = messages[-8:]
        if not messages:
            return json.dumps(
                {
                    "child_chat_id": child.run_id,
                    "phase": child.phase,
                    "status": child.status,
                    "ready": False,
                    "error": "child work session finished without an assistant result",
                },
                sort_keys=True,
            )
        return json.dumps(
            {
                "child_chat_id": child.run_id,
                "title": child.objective,
                "phase": child.phase,
                "status": child.status,
                "ready": True,
                "terminal_reason": child.terminal_reason,
                "engine": child.engine,
                "model": child.model,
                "provider_instance_id": child.provider_instance_id,
                "final_result": messages[-1]["text"] if messages else "",
                "assistant_messages": messages,
            },
            sort_keys=True,
        )

    return Tool(
        "read_child_work_result",
        "Read the bounded assistant transcript and final result of a child work session after it finishes.",
        {
            "type": "object",
            "properties": {
                "child_chat_id": {"type": "string", "description": "Child chat/run id returned by spawn_child_work_session."},
            },
            "required": ["child_chat_id"],
        },
        "orchestration.runs.read",
        read_result,
        timeout_s=5,
    )


def _watch_child_work_sessions_tool(cfg: Config, project: ProjectEntry, thread: CockpitThread) -> Tool:
    async def watch(ctx: RequestContext, args: dict[str, Any]) -> str:
        raw_ids = args.get("child_chat_ids")
        if not isinstance(raw_ids, list):
            return "error: child_chat_ids must be an array"
        child_ids = [str(item).strip() for item in raw_ids if str(item).strip()]
        if not child_ids:
            return "error: at least one child_chat_id is required"
        child_ids = list(dict.fromkeys(child_ids))
        raw_expected_count = args.get("expected_count")
        if raw_expected_count is not None:
            try:
                expected_count = int(raw_expected_count)
            except (TypeError, ValueError):
                return "error: expected_count must be a positive integer"
            if expected_count < 1:
                return "error: expected_count must be a positive integer"
            if len(child_ids) != expected_count:
                return f"error: expected {expected_count} distinct child_chat_ids, received {len(child_ids)}"
        store = OrchestrationStore(cfg.orchestration.workspace)
        for child_id in child_ids:
            child = await asyncio.to_thread(store.get, child_id)
            if child is None:
                return f"error: no such child work session {child_id}"
            if (
                child.project_id != project.id
                or (child.parent_chat_id != thread.thread_id and child.parent_run_id != thread.thread_id)
            ):
                return f"error: child work session {child_id} does not belong to this orchestrator chat"
        watch_id = await asyncio.to_thread(CockpitThreadIndex(
            Path(cfg.orchestration.workspace) / THREAD_INDEX_FILENAME
        ).register_child_watch, thread, child_ids, requester=ctx)
        await asyncio.to_thread(_start_ready_child_watch, cfg, thread.thread_id)
        return json.dumps(
            {"watch_id": watch_id, "child_chat_ids": sorted(set(child_ids)), "registered": True},
            sort_keys=True,
        )

    return Tool(
        "watch_child_work_sessions",
        "Register child work sessions for one event-driven parent continuation after all become terminal. Returns immediately.",
        {
            "type": "object",
            "properties": {
                "child_chat_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                },
                "expected_count": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional exact number of distinct child sessions required before registering the watch.",
                },
            },
            "required": ["child_chat_ids"],
        },
        "orchestration.runs.read",
        watch,
        timeout_s=5,
    )


def make_child_terminal_notifier(cfg: Config) -> Callable[[str, Any], bool]:
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / THREAD_INDEX_FILENAME)

    def notify(parent_chat_id: str, child: Any) -> bool:
        appended = index.append_child_terminal_system_message(parent_chat_id, child)
        _start_ready_child_watch(cfg, parent_chat_id)
        return appended

    return notify


def _start_ready_child_watch(cfg: Config, parent_chat_id: str) -> None:
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / THREAD_INDEX_FILENAME)
    store = OrchestrationStore(cfg.orchestration.workspace)
    terminal_ids = {
        run.run_id
        for run in store.list_runs()
        if (run.parent_chat_id == parent_chat_id or run.parent_run_id == parent_chat_id)
        and run.status == "terminal"
    }
    watch = index.claim_ready_child_watch(parent_chat_id, terminal_ids)
    if watch is None:
        return
    thread = threading.Thread(
        target=_continue_child_watch,
        args=(cfg, parent_chat_id, watch),
        name=f"jarvis-child-watch-{watch['watch_id']}",
        daemon=True,
    )
    thread.start()


def _continue_child_watch(cfg: Config, parent_chat_id: str, watch: dict[str, Any]) -> None:
    index = CockpitThreadIndex(Path(cfg.orchestration.workspace) / THREAD_INDEX_FILENAME)
    watch_id = str(watch.get("watch_id") or "")

    async def run() -> None:
        thread = next((item for item in index._threads().values() if item.thread_id == parent_chat_id), None)  # noqa: SLF001
        if thread is None:
            raise RuntimeError("parent orchestrator chat no longer exists")
        registry = RegistryStore(cfg.registry.path)
        project = registry.get_project(thread.project_id)
        if project is None:
            raise RuntimeError("parent orchestrator project no longer exists")
        requester_snapshot = watch.get("requester")
        if not isinstance(requester_snapshot, dict):
            raise RuntimeError("child watch is missing its requester authority snapshot")
        capabilities = frozenset(
            str(item)
            for item in requester_snapshot.get("capabilities") or []
            if str(item).strip()
        )
        requester = RequestContext(
            device_id=str(requester_snapshot.get("device_id") or ""),
            identity=str(requester_snapshot.get("identity") or ""),
            scope=str(requester_snapshot.get("scope") or "personal"),
            capabilities=capabilities,
            channel=str(requester_snapshot.get("channel") or "cockpit"),
            confidence=str(requester_snapshot.get("confidence") or "strong"),
            peer=str(requester_snapshot.get("peer") or ""),
        )
        child_ids = [str(item) for item in watch.get("child_chat_ids") or []]
        instruction = (
            "Automatic orchestration continuation: all watched child work sessions are terminal. "
            f"Read each result with read_child_work_result for these child_chat_ids: {', '.join(child_ids)}. "
            "Then continue the original workflow: combine and deduplicate the results, perform any requested "
            "capability-gated external action, and report the outcome. Do not spawn replacement children unless "
            "a result explicitly failed and the original user request requires recovery."
        )
        connector = CockpitConnector(cfg)
        await connector.turn(project, thread, requester, instruction)

    try:
        asyncio.run(run())
    except Exception as exc:  # noqa: BLE001 - failure is durable and visible on the parent thread
        index.finish_child_watch(parent_chat_id, watch_id, error=str(exc))
    else:
        index.finish_child_watch(parent_chat_id, watch_id)


def _publish_github_pr_review_tool(cfg: Config, project: ProjectEntry) -> Tool:
    async def publish(ctx: RequestContext, args: dict[str, Any]) -> str:
        repo = str(args.get("repo") or "").strip()
        if not _project_has_repo(project, repo):
            return "error: repo is not registered to this project"
        comments = args.get("comments")
        if not isinstance(comments, list):
            return "error: comments must be an array"
        try:
            worker = await asyncio.to_thread(_github_review_worker, cfg, project, repo)
            token = worker_token_value(worker.token_env) if worker.token_env else ""
            if not token and worker.worker_id == "local-worker":
                token = cfg.worker.token.get_secret_value()
            response = await asyncio.to_thread(
                httpx.post,
                f"{worker.base_url}/run",
                json={
                    "action": "github_pr_review",
                    "args": {
                        "repo": repo,
                        "pull_number": int(args.get("pull_number") or 0),
                        "commit_id": str(args.get("commit_id") or ""),
                        "summary": str(args.get("summary") or ""),
                        "comments": [dict(item) for item in comments if isinstance(item, dict)],
                        "idempotency_key": str(args.get("idempotency_key") or ""),
                        "execution_envelope": {
                            "allowed_actions": sorted(ctx.capabilities),
                            "landing": {"mode": "review", "allow_merge": False},
                        },
                    },
                },
                headers={"Authorization": f"Bearer {token}"} if token else {},
                timeout=30,
            )
            payload = response.json()
            if response.status_code >= 400 or not payload.get("ok"):
                raise RuntimeError(str(payload.get("error") or response.text or "worker rejected GitHub review"))
        except Exception as exc:  # noqa: BLE001 - external write errors become bounded tool output
            return f"error: could not publish GitHub review ({public_error_message(str(exc))})"
        result = payload.get("review") if isinstance(payload.get("review"), dict) else {}
        return json.dumps(
            {
                "published": True,
                "review_id": int(result.get("review_id") or 0),
                "url": str(result.get("url") or ""),
                "comments": int(result.get("comments") or 0),
                "skipped_comments": int(result.get("skipped_comments") or 0),
                "replayed": bool(payload.get("replayed")),
                "worker_id": worker.worker_id,
            },
            sort_keys=True,
        )

    return Tool(
        "publish_github_pr_review",
        "Publish one structured GitHub pull-request review with line comments and optional suggestions.",
        {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "description": "GitHub repository in owner/name form; must belong to this project."},
                "pull_number": {"type": "integer", "minimum": 1},
                "commit_id": {"type": "string", "description": "Optional reviewed head commit SHA."},
                "summary": {"type": "string", "description": "Required review-level summary or findings that cannot be placed inline."},
                "idempotency_key": {"type": "string", "description": "Stable unique key for this reviewed commit and joined result."},
                "comments": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": {"type": "string"},
                            "line": {"type": "integer", "minimum": 1},
                            "side": {"type": "string", "enum": ["LEFT", "RIGHT"]},
                            "severity": {"type": "string", "enum": ["P1", "P2", "P3"]},
                            "title": {"type": "string"},
                            "body": {"type": "string"},
                            "suggestion": {"type": "string", "description": "Optional exact replacement for a GitHub suggestion block."},
                        },
                        "required": ["path", "line", "severity", "title", "body"],
                    },
                },
            },
            "required": ["repo", "pull_number", "commit_id", "summary", "idempotency_key", "comments"],
        },
        FORGE_PR_COMMENT,
        publish,
        announce=True,
        timeout_s=30,
    )


def _project_has_repo(project: ProjectEntry, repo: str) -> bool:
    normalized = repo.removesuffix(".git").rstrip("/")
    return any(
        candidate == normalized or candidate.endswith(f"/{normalized}")
        for entry in project.repos
        for candidate in (
            entry.remote.removesuffix(".git").rstrip("/"),
            entry.name.removesuffix(".git").rstrip("/"),
        )
    )


def _github_review_worker(cfg: Config, project: ProjectEntry, repo: str):  # noqa: ANN202 - WorkerProfile inferred across boundary
    registry = WorkerRegistry(cfg.worker, profiles_path=cfg.orchestration.workers_path)
    profiles = registry.with_repo_access(registry.profiles(probe=True), repo)
    for worker in profiles:
        access = next(
            (
                item
                for item in worker.repo_access
                if str(item.get("repo") or "").removesuffix(".git") == repo.removesuffix(".git")
            ),
            {},
        )
        if (
            worker.status == "online"
            and worker.base_url
            and access.get("accessible") is True
            and worker.git_identity.get("authenticated") is True
        ):
            return worker
    raise RuntimeError(f"no authenticated worker can publish reviews for a repository in project {project.id}")


def _project_default_repo(project: ProjectEntry) -> str:
    for repo in project.repos:
        if repo.default and repo.remote:
            return repo.remote
    for repo in project.repos:
        if repo.remote:
            return repo.remote
    return ""


def _safe_thread_tool_events(session_ref: str, tool_messages: list) -> tuple[dict[str, Any], ...]:
    # Best-effort: tool-event projection is a display nicety, not part of the
    # turn contract, so any drift here must not break the turn — but note this
    # does mean a single malformed message drops the whole turn's tool events
    # rather than just that one. Left broad on purpose; narrowing needs a look
    # at _thread_tool_events' call/result pairing invariants, out of scope here.
    try:
        return tuple(_thread_tool_events(session_ref, tool_messages))
    except Exception:
        return ()


def _thread_tool_events(session_ref: str, tool_messages: list) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    calls_by_id: dict[str, dict[str, Any]] = {}
    turn_id = new_id("turn")
    sequence = 0
    for message in tool_messages:
        if not isinstance(message, dict):
            continue
        if message.get("role") == "assistant":
            for call in message.get("tool_calls") or ():
                if not isinstance(call, dict):
                    continue
                function = call.get("function") if isinstance(call.get("function"), dict) else {}
                call_id = str(call.get("id") or new_id("call"))
                name = str(function.get("name") or "")
                arguments = str(function.get("arguments") or "")
                calls_by_id[call_id] = {"id": call_id, "name": name, "arguments": arguments}
                sequence += 1
                events.append(
                    _thread_tool_event(
                        session_ref,
                        event_type=EVENT_TOOL_CALL,
                        sequence=sequence,
                        turn_id=turn_id,
                        data={
                            "id": call_id,
                            "item": {
                                "id": call_id,
                                "type": "tool_use",
                                "name": name,
                                "input": _tool_input(arguments),
                            },
                        },
                    )
                )
        elif message.get("role") == "tool":
            call_id = str(message.get("tool_call_id") or "")
            call = calls_by_id.get(call_id, {"id": call_id, "name": "", "arguments": ""})
            sequence += 1
            content = str(message.get("content") or "")
            events.append(
                _thread_tool_event(
                    session_ref,
                    event_type=EVENT_TOOL_RESULT,
                    sequence=sequence,
                    turn_id=turn_id,
                    data={
                        "id": call_id,
                        "item": {
                            "id": call_id,
                            "type": "tool_result",
                            "tool_use_id": call_id,
                            "name": call.get("name") or "",
                            "content": content,
                        },
                    },
                )
            )
    return events


def _thread_tool_event(
    session_ref: str,
    *,
    event_type: str,
    sequence: int,
    turn_id: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    raw = {
        "event_id": new_id("ev"),
        "session_id": session_ref,
        "type": event_type,
        "time": utc_now(),
        "data": data,
    }
    # project_session_event() requires worker_id to mint a session_ref, but the
    # real session_ref (thread-scoped, not worker-scoped) is set explicitly
    # below and overwrites it — worker_id="" is a throwaway. Leaving it: fixing
    # this cleanly means giving project_session_event() an optional worker_id,
    # which lives in orchestration/cockpit.py, out of scope here.
    projected = project_session_event(raw, worker_id="", sequence=sequence)
    return {
        **projected,
        "session_ref": session_ref,
        "run_id": "",
        "turn_id": turn_id,
    }


def _json_or_text(value: str) -> Any:
    if not value:
        return {}
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return value


def _tool_input(arguments: str) -> dict[str, Any]:
    parsed = _json_or_text(arguments)
    if isinstance(parsed, dict):
        return parsed
    if parsed in ("", [], {}):
        return {}
    return {"arguments": parsed}


def _guard_project_thread_reply(
    reply: str,
    events: tuple[dict[str, Any], ...] | list[dict[str, Any]],
) -> str:
    if not _claims_workspace_action(reply) or _has_workspace_action_event(events):
        return reply
    return _WORK_SESSION_OFFER


def _claims_workspace_action(text: str) -> bool:
    normalized = " ".join((text or "").split())
    return bool(_WORK_ACTION_CLAIM_RE.search(normalized))


def _has_workspace_action_event(events: tuple[dict[str, Any], ...] | list[dict[str, Any]]) -> bool:
    return any(_thread_event_tool_name(event) in _WORK_ACTION_TOOL_NAMES for event in events if isinstance(event, dict))


def _thread_event_tool_name(event: dict[str, Any]) -> str:
    data = event.get("data") if isinstance(event.get("data"), dict) else {}
    item = data.get("item") if isinstance(data.get("item"), dict) else {}
    return str(item.get("name") or data.get("name") or "")


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


def _text_with_attachment_markers(text: str, attachments: list[dict[str, Any]] | None) -> str:
    if not attachments:
        return text
    markers = []
    for attachment in attachments:
        name = str(attachment.get("name") or "image")
        mime_type = str(attachment.get("mime_type") or "")
        markers.append(f"[image attached: {name}{f' ({mime_type})' if mime_type else ''}]")
    return "\n".join([text, *markers])


def _thread_title(text: str) -> str:
    title = " ".join(text.split())
    if not title:
        return "Project thread"
    return title if len(title) <= 72 else title[:71] + "..."


def _timestamp_before(value: str, threshold: datetime) -> bool:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return True
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed < threshold


def _normalized_messages(messages: Any) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in messages:
        if not isinstance(item, dict):
            continue
        message: dict[str, Any] = {
            "role": str(item.get("role") or ""),
            "peer_id": str(item.get("peer_id") or ""),
            "content": str(item.get("content") or ""),
            "observed_at": str(item.get("observed_at") or ""),
        }
        for key in (
            "type",
            "child_chat_id",
            "child_run_id",
            "phase",
            "status",
            "terminal_reason",
            "watch_id",
            "claimed_at",
            "completed_at",
            "error",
        ):
            if key in item:
                message[key] = str(item.get(key) or "")
        if isinstance(item.get("child_chat_ids"), list):
            message["child_chat_ids"] = [str(value) for value in item["child_chat_ids"] if str(value)]
        requester = item.get("requester")
        if isinstance(requester, dict):
            message["requester"] = {
                "device_id": str(requester.get("device_id") or ""),
                "identity": str(requester.get("identity") or ""),
                "scope": str(requester.get("scope") or "personal"),
                "capabilities": sorted(
                    {str(value) for value in requester.get("capabilities") or [] if str(value).strip()}
                ),
                "channel": str(requester.get("channel") or "cockpit"),
                "confidence": str(requester.get("confidence") or "strong"),
                "peer": str(requester.get("peer") or ""),
            }
        event = item.get("event")
        if isinstance(event, dict):
            message["event"] = dict(event)
        normalized.append(message)
    return normalized


def _safe_path_segment(value: str) -> str:
    segment = "".join(char if char.isalnum() or char in {"-", "_", "."} else "_" for char in value)
    return segment or "unknown"


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


def _workspace_is_ready(thread: CockpitThread) -> bool:
    workspace = thread.workspace or {}
    return (
        bool(workspace.get("worker_id"))
        and bool(workspace.get("session_id"))
        and str(workspace.get("status") or "") == "ready"
    )


def _workspace_needs_new_repo(project: ProjectEntry, thread: CockpitThread, workspace_request: dict[str, Any]) -> bool:
    # Only an explicit repos ask can justify re-provisioning an already-ready
    # workspace; a bare re-request of the same turn must not re-clone/re-attach.
    if not workspace_request.get("repos"):
        return False
    existing = {
        (str(item.get("name") or ""), str(item.get("repo") or ""))
        for item in thread.workspace.get("worktrees", [])
        if isinstance(item, dict)
    }
    requested = _workspace_repos(project, workspace_request)
    return any((repo.get("name"), repo.get("repo")) not in existing for repo in requested)


def _workspace_repos(project: ProjectEntry, request: dict[str, Any]) -> list[dict[str, str]]:
    raw = request.get("repos")
    rows: list[dict[str, str]] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str):
                rows.append(_repo_payload(project, item, base_ref=str(request.get("base_ref") or "")))
            elif isinstance(item, dict):
                name = str(item.get("name") or item.get("repo") or "")
                rows.append(_repo_payload(project, name, base_ref=str(item.get("base_ref") or request.get("base_ref") or "")))
    if rows:
        return rows
    defaults = [repo for repo in project.repos if repo.default]
    selected = defaults[:1] or list(project.repos[:1])
    return [{"name": repo.name, "repo": repo.remote, "base_ref": str(request.get("base_ref") or "")} for repo in selected]


def _repo_payload(project: ProjectEntry, name: str, *, base_ref: str = "") -> dict[str, str]:
    match = next((repo for repo in project.repos if repo.name == name or repo.remote == name), None)
    if match is not None:
        return {"name": match.name, "repo": match.remote, "base_ref": base_ref}
    return {"name": Path(name).name, "repo": name, "base_ref": base_ref}


def _conversation_workspace_id(project: ProjectEntry, thread: CockpitThread) -> str:
    return slugify(f"{project.id}-{thread.thread_id}")


def _workspace_prompt(project: ProjectEntry, thread: CockpitThread, text: str) -> str:
    workspace = workspace_public(thread.workspace)
    return (
        f"Project: {project.name} ({project.id})\n"
        f"Honcho session: {thread.session_id}\n"
        f"Conversation workspace: {json.dumps(workspace, sort_keys=True)}\n\n"
        f"User turn:\n{text}"
    )


def workspace_public(workspace: dict[str, Any]) -> dict[str, Any]:
    return {
        "worker_id": str(workspace.get("worker_id") or ""),
        "session_id": str(workspace.get("session_id") or ""),
        "engine": str(workspace.get("engine") or ""),
        "workspace_id": str(workspace.get("workspace_id") or ""),
        "root_label": str(workspace.get("root_label") or ""),
        "cwd_label": str(workspace.get("cwd_label") or workspace.get("root_label") or ""),
        "status": str(workspace.get("status") or ""),
        "provision_phase": str(workspace.get("provision_phase") or ""),
        "worktrees": [
            {
                "name": str(item.get("name") or ""),
                "repo": str(item.get("repo") or ""),
                "path_label": str(item.get("path_label") or ""),
                "branch": str(item.get("branch") or ""),
                "base_ref": str(item.get("base_ref") or ""),
                "status": str(item.get("status") or ""),
                "provision_phase": str(item.get("provision_phase") or ""),
            }
            for item in workspace.get("worktrees", [])
            if isinstance(item, dict)
        ],
        "created_at": str(workspace.get("created_at") or ""),
        "updated_at": str(workspace.get("updated_at") or ""),
    }


def _stable_text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _worker_headers(worker_cfg: Any, profile: Any) -> dict[str, str]:
    token = worker_token_value(profile.token_env) if getattr(profile, "token_env", "") else ""
    if not token and getattr(profile, "worker_id", "") == "local-worker":
        token = worker_cfg.token.get_secret_value()
    return {"Authorization": f"Bearer {token}"} if token else {}


def _json_response(response: Any) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}
