"""Cockpit project-thread connector for Jarvis and code-agent conversations."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import inspect
import json
import logging
import os
import re
import tempfile
import threading
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any

import httpx

from jarvis.capabilities import (
    FORGE_BRANCH_PUSH,
    FORGE_PR_COMMENT,
    WORKER_SESSION_CREATE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_STOP,
    WORKER_SESSION_TURN,
)
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
from jarvis.engines import BUILTIN_CODE_ENGINES, normalize_engine_id, worker_supports_engine
from jarvis.ids import new_id, utc_now
from jarvis.jsonl_cache import JsonlCacheEntry, read_jsonl_projection
from jarvis.orchestrator_tool_contract import (
    ORCHESTRATOR_TOOL_NAME_SET,
    PUBLISH_GITHUB_PR_REVIEW,
    READ_CHILD_WORK_RESULT,
    SPAWN_CHILD_WORK_SESSION,
    WATCH_CHILD_WORK_SESSIONS,
)
from jarvis.orchestration.cockpit import project_session_event
from jarvis.orchestration.models import WorkCommand, WorkItem
from jarvis.orchestration.store import OrchestrationStore
from jarvis.orchestration.service import StartedWork, OrchestrationService
from jarvis.orchestration.redaction import public_error_message
from jarvis.orchestration.orchestrator_grants import mint_orchestrator_grant, orchestrator_api_base_url
from jarvis.orchestration.workers import WorkerRegistry, worker_token_value
from jarvis.runtime import ToolRegistry
from jarvis.storage import atomic_write_json
from jarvis.tools import build_registry
from jarvis.tools.background import make_background_tool
from jarvis.tools.base import Tool
from jarvis.text import slugify
from jarvis.users import load_users
from jarvis.worker_session_contract import (
    EVENT_APPROVAL_REQUESTED,
    EVENT_ASSISTANT_MESSAGE,
    EVENT_TOOL_CALL,
    EVENT_TOOL_RESULT,
    EVENT_TURN_COMPLETED,
    EVENT_TURN_FAILED,
    FAILED_SESSION_STATUSES,
    WORKER_ERROR_SESSION_ACTIVE,
    WORKER_ERROR_SESSION_TERMINAL,
    turn_failure_message,
)


class ProviderTurnError(RuntimeError):
    """A provider/worker turn reached a terminal failure the operator must see."""


ORCHESTRATOR_ENGINES = {"codex", "claude"}

THREAD_INDEX_FILENAME = "cockpit-threads.json"
THREAD_TRANSCRIPTS_DIRNAME = "cockpit-thread-transcripts"
# The orchestrator conversation is the operator's most capable surface: it must
# be able to act, not only read. A read-only envelope maps Codex to a read-only
# sandbox and Claude to plan mode, which leaves the agent unable to do the work
# it is being asked to coordinate. Plan mode remains a per-turn choice the
# operator can make from the composer, never an imposed default.
# WORKER_SESSION_APPROVE is deliberately absent: holding it makes Codex ask for
# approval on every command and Claude ask before acting, and an orchestrator
# turn runs headless with nobody to answer — it would hang until the job
# timeout. Without it the session acts autonomously (Codex "never", Claude
# "dontAsk") inside its workspace-write sandbox.
CONVERSATION_SESSION_ALLOWED_ACTIONS = [
    WORKER_SESSION_CREATE,
    WORKER_SESSION_TURN,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_STOP,
    FORGE_BRANCH_PUSH,
]
CONVERSATION_SESSION_LANDING = {"mode": "branch_only", "allow_merge": False}
THREAD_HISTORY_LIMIT = 24
CHILD_WATCH_LEASE_S = 300
# A claim only needs a heartbeat, not a durable write per retry tick. Renewing
# on every tick appended a child_watch record each time and grew the transcript
# without bound (a 40s wait once produced 158 records for one watch).
CHILD_WATCH_RENEW_INTERVAL_S = CHILD_WATCH_LEASE_S // 3
THREAD_TURN_QUEUE_LIMIT = 32
THREAD_TURN_RECEIPT_REPLY_LIMIT = 4_000
# Workspace keys the thread store owns outright. Callers hand back workspace
# snapshots that can be arbitrarily stale, so these are always re-read from the
# stored thread rather than merged from the caller's copy.
STORE_OWNED_WORKSPACE_KEYS = frozenset({"pending_child_watch_ids"})
CHILD_WORK_LANDING_MODES = {"none", "branch_only", "draft_pr", "ready_pr", "confirm_before_pr"}
_THREAD_INDEX_LOCK = threading.RLock()
_THREAD_TRANSCRIPT_LOCKS_LOCK = threading.Lock()
_THREAD_TRANSCRIPT_LOCKS: dict[Path, threading.RLock] = {}
_THREAD_TRANSCRIPT_CACHE_MAX = 500
logger = logging.getLogger(__name__)
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
_WORK_ACTION_TOOL_NAMES = ORCHESTRATOR_TOOL_NAME_SET


class WorkerRequestError(RuntimeError):
    def __init__(self, message: str, *, code: str = "", status_code: int = 0) -> None:
        self.code = code
        self.status_code = status_code
        super().__init__(message)


def _snapshot_from_context(requester: RequestContext) -> dict[str, Any]:
    return {
        "device_id": requester.device_id,
        "identity": requester.identity,
        "scope": requester.scope,
        "capabilities": sorted(requester.capabilities),
        "channel": requester.channel,
        "confidence": requester.confidence,
        "peer": requester.peer,
    }


def _context_from_snapshot(snapshot: dict[str, Any]) -> RequestContext:
    capabilities = frozenset(
        str(item)
        for item in snapshot.get("capabilities") or []
        if str(item).strip()
    )
    return RequestContext(
        device_id=str(snapshot.get("device_id") or ""),
        identity=str(snapshot.get("identity") or ""),
        scope=str(snapshot.get("scope") or "personal"),
        capabilities=capabilities,
        channel=str(snapshot.get("channel") or "cockpit"),
        confidence=str(snapshot.get("confidence") or "strong"),
        peer=str(snapshot.get("peer") or ""),
    )


def persist_turn_messages(
    memory: MemoryBackend,
    session_id: str,
    requester_peer_id: str,
    device_id: str | None,
    user_text: str,
    assistant_text: str | None = None,
    *,
    assistant_peer_id: str = "",
    channel: str = "cockpit",
    extra_user_metadata: dict[str, Any] | None = None,
) -> None:
    """Create-or-reuse a memory session and persist a turn's message pair.

    Shared by every cockpit/MCP surface that writes user (and optionally
    assistant) turns into memory, so the session/metadata shape stays in sync
    across call sites.
    """
    memory.create_session(
        session_id,
        peers=[SessionPeer(peer_id=requester_peer_id, observe_me=True, observe_others=True)],
    )
    user_metadata: dict[str, Any] = {"channel": channel, "role": "user", "observed_at": utc_now()}
    if extra_user_metadata:
        user_metadata.update(extra_user_metadata)
    if device_id:
        user_metadata["device_id"] = device_id
    messages = [
        MemoryMessage(
            peer_id=requester_peer_id,
            content=user_text,
            metadata=user_metadata,
        ),
    ]
    if assistant_text is not None:
        assistant_metadata: dict[str, Any] = {"channel": channel, "role": "assistant", "observed_at": utc_now()}
        if device_id:
            assistant_metadata["device_id"] = device_id
        messages.append(
            MemoryMessage(
                peer_id=assistant_peer_id,
                content=assistant_text,
                metadata=assistant_metadata,
            )
        )
    memory.create_messages(session_id, messages)


@dataclass(frozen=True)
class CockpitThread:
    thread_id: str
    project_id: str
    session_id: str
    title: str
    created_at: str
    updated_at: str
    created_by: str
    chat_type: str = "assistant"
    engine: str = "jarvis"
    model: str = ""
    # Effective reasoning effort / speed tier. Empty means "whatever the engine
    # defaults to" — the worker's catalog owns that default, not the thread.
    effort: str = ""
    speed: str = ""
    worker_id: str = ""
    parent_chat_id: str = ""
    archived_at: str = ""
    archived_by: str = ""
    archive_reason: str = ""
    last_turn_at: str = ""
    messages: tuple[dict[str, Any], ...] = ()
    workspace: dict[str, Any] = field(default_factory=dict)
    queued_turns: tuple[dict[str, Any], ...] = ()
    turn_receipts: tuple[dict[str, Any], ...] = ()

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
            chat_type=str(data.get("chat_type") or "assistant"),
            engine=str(data.get("engine") or "jarvis"),
            model=str(data.get("model") or ""),
            effort=str(data.get("effort") or ""),
            speed=str(data.get("speed") or ""),
            worker_id=str(data.get("worker_id") or ""),
            parent_chat_id=str(data.get("parent_chat_id") or ""),
            archived_at=str(data.get("archived_at") or ""),
            archived_by=str(data.get("archived_by") or ""),
            archive_reason=str(data.get("archive_reason") or ""),
            last_turn_at=str(data.get("last_turn_at") or ""),
            messages=tuple(_normalized_messages(data.get("messages") or ())) if include_messages else (),
            workspace=dict(data.get("workspace") or {}),
            queued_turns=tuple(dict(item) for item in data.get("queued_turns") or () if isinstance(item, dict)),
            turn_receipts=tuple(dict(item) for item in data.get("turn_receipts") or () if isinstance(item, dict)),
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
            "chat_type": self.chat_type,
            "engine": self.engine,
            "model": self.model,
            "effort": self.effort,
            "speed": self.speed,
            "worker_id": self.worker_id,
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
        if self.queued_turns:
            data["queued_turns"] = [dict(item) for item in self.queued_turns]
        if self.turn_receipts:
            data["turn_receipts"] = [dict(item) for item in self.turn_receipts[-100:]]
        return data


class CockpitThreadIndex:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()
        self.transcripts_dir = self.path.parent / THREAD_TRANSCRIPTS_DIRNAME
        # (size, mtime_ns, device, inode, safe_offset, messages). The offset
        # stops before a partial final line so a concurrent writer cannot make a
        # reader cache an incomplete JSON record.
        self._transcript_cache: OrderedDict[Path, JsonlCacheEntry[dict[str, Any]]] = OrderedDict()
        self._transcript_cache_lock = threading.Lock()

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

    def list_all(self) -> list[CockpitThread]:
        """Every live thread across every project (retention sweeps this)."""

        return list(self._threads().values())

    def transcript_bytes(self, project_id: str, thread_id: str) -> int:
        """On-disk size of a thread's transcript, for reclamation reporting."""

        total = 0
        for path in (self._transcript_path(project_id, thread_id), self._legacy_transcript_path(project_id, thread_id)):
            try:
                total += path.stat().st_size
            except OSError:
                continue
        return total

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
            atomic_write_json(self.path, data)
            return replace(thread, messages=())

    def enqueue_turn(
        self,
        project_id: str,
        thread_id: str,
        *,
        requester: RequestContext,
        text: str,
        idempotency_key: str,
        workspace_request: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
    ) -> tuple[CockpitThread, dict[str, Any], bool]:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                raise KeyError(thread_id)
            if attachments:
                raise ValueError("queued attachments require durable attachment references")
            key = idempotency_key.strip()
            fingerprint = _thread_turn_fingerprint(text, workspace_request, attachments)
            for receipt in thread.turn_receipts:
                if str(receipt.get("idempotency_key") or "") == key:
                    if str(receipt.get("fingerprint") or "") != fingerprint:
                        raise ValueError("idempotency key was already used for a different thread turn")
                    if str(receipt.get("status") or "") == "failed":
                        thread = replace(
                            thread,
                            turn_receipts=tuple(item for item in thread.turn_receipts if item is not receipt),
                        )
                        break
                    queue_id = str(receipt.get("queue_id") or "")
                    existing = next(
                        (item for item in thread.queued_turns if str(item.get("queue_id") or "") == queue_id),
                        {"queue_id": queue_id, "text": text, "queued_at": "", "status": "completed"},
                    )
                    return thread, dict(existing), False
            if len(thread.queued_turns) >= THREAD_TURN_QUEUE_LIMIT:
                raise OverflowError("thread turn queue is full")
            queued_at = utc_now()
            item = {
                "queue_id": new_id("queuedturn"),
                "idempotency_key": key,
                "fingerprint": fingerprint,
                "text": text,
                "queued_at": queued_at,
                "status": "queued",
                "requester": _snapshot_from_context(requester),
                "workspace_request": dict(workspace_request) if workspace_request is not None else None,
            }
            updated = self.save(
                replace(
                    thread,
                    updated_at=queued_at,
                    queued_turns=(*thread.queued_turns, item),
                    turn_receipts=(
                        *thread.turn_receipts[-99:],
                        {
                            "idempotency_key": key,
                            "queue_id": str(item["queue_id"]),
                            "fingerprint": fingerprint,
                            "status": "queued",
                        },
                    ),
                )
            )
            return updated, item, True

    def foreground_turn_receipt(
        self,
        project_id: str,
        thread_id: str,
        *,
        text: str,
        idempotency_key: str,
        workspace_request: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        thread = self.get(project_id, thread_id)
        if thread is None:
            return None
        fingerprint = _thread_turn_fingerprint(text, workspace_request, attachments)
        receipt = next(
            (item for item in thread.turn_receipts if item.get("idempotency_key") == idempotency_key),
            None,
        )
        if receipt is None or str(receipt.get("status") or "") == "failed":
            return None
        if str(receipt.get("fingerprint") or "") != fingerprint:
            raise ValueError("idempotency key was already used for a different thread turn")
        return dict(receipt)

    def reserve_foreground_turn(
        self,
        project_id: str,
        thread_id: str,
        *,
        text: str,
        idempotency_key: str,
        requester: RequestContext,
        dispatch_mode: str,
        workspace_request: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                raise KeyError(thread_id)
            fingerprint = _thread_turn_fingerprint(text, workspace_request, attachments)
            receipts = [dict(item) for item in thread.turn_receipts]
            for index, receipt in enumerate(receipts):
                if receipt.get("idempotency_key") != idempotency_key:
                    continue
                if str(receipt.get("fingerprint") or "") != fingerprint:
                    raise ValueError("idempotency key was already used for a different thread turn")
                if str(receipt.get("status") or "") != "failed":
                    return receipt, False
                receipt.update(
                    status="dispatching",
                    attempt=int(receipt.get("attempt") or 0) + 1,
                    updated_at=utc_now(),
                    text=text,
                    requester=_snapshot_from_context(requester),
                    workspace_request=dict(workspace_request) if workspace_request is not None else None,
                    has_attachments=bool(attachments),
                    dispatch_mode=dispatch_mode,
                )
                receipts[index] = receipt
                self.save(replace(thread, turn_receipts=tuple(receipts[-100:])))
                return receipt, True
            receipt = {
                "idempotency_key": idempotency_key,
                "fingerprint": fingerprint,
                "logical_turn_id": new_id("turn"),
                "status": "dispatching",
                "attempt": 1,
                "created_at": utc_now(),
                "text": text,
                "requester": _snapshot_from_context(requester),
                "workspace_request": dict(workspace_request) if workspace_request is not None else None,
                "has_attachments": bool(attachments),
                "dispatch_mode": dispatch_mode,
            }
            self.save(replace(thread, turn_receipts=(*thread.turn_receipts[-99:], receipt)))
            return receipt, True

    def finish_foreground_turn(
        self,
        project_id: str,
        thread_id: str,
        idempotency_key: str,
        *,
        status: str,
        reply: str = "",
    ) -> None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return
            receipts = [dict(item) for item in thread.turn_receipts]
            for index, receipt in enumerate(receipts):
                if receipt.get("idempotency_key") != idempotency_key:
                    continue
                receipt.update(
                    status=status,
                    reply=reply[:THREAD_TURN_RECEIPT_REPLY_LIMIT],
                    reply_truncated=len(reply) > THREAD_TURN_RECEIPT_REPLY_LIMIT,
                    updated_at=utc_now(),
                )
                if status in {"completed", "accepted", "failed"}:
                    _scrub_turn_receipt_recovery_fields(receipt)
                receipts[index] = receipt
                self.save(replace(thread, turn_receipts=tuple(receipts)))
                return

    def fail_foreground_turn(self, project_id: str, thread_id: str, idempotency_key: str) -> None:
        self.finish_foreground_turn(project_id, thread_id, idempotency_key, status="failed")

    def has_queued_turns(self, project_id: str, thread_id: str) -> bool:
        thread = self.get(project_id, thread_id)
        return bool(thread and thread.queued_turns)

    def turn_receipt(self, project_id: str, thread_id: str, idempotency_key: str) -> dict[str, Any] | None:
        thread = self.get(project_id, thread_id)
        if thread is None:
            return None
        receipt = next(
            (
                item
                for item in thread.turn_receipts
                if str(item.get("idempotency_key") or "") == idempotency_key
            ),
            None,
        )
        if receipt is None:
            return None
        queue_id = str(receipt.get("queue_id") or "")
        queued = next(
            (item for item in thread.queued_turns if str(item.get("queue_id") or "") == queue_id),
            None,
        )
        return dict(queued or {**receipt, "status": "completed"})

    def rearm_queued_turns(self, project_id: str, thread_id: str) -> CockpitThread | None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None or not any(item.get("status") == "claimed" for item in thread.queued_turns):
                return thread
            queued = tuple(
                {key: value for key, value in item.items() if key != "claimed_at"}
                | {"status": "queued"}
                if item.get("status") == "claimed"
                else dict(item)
                for item in thread.queued_turns
            )
            return self.save(replace(thread, queued_turns=queued))

    def recover_dispatching_turns(self, project_id: str, thread_id: str) -> CockpitThread | None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return None
            receipts = [dict(item) for item in thread.turn_receipts]
            queued = [dict(item) for item in thread.queued_turns]
            changed = False
            for index, receipt in enumerate(receipts):
                if str(receipt.get("status") or "") != "dispatching":
                    continue
                changed = True
                if receipt.get("has_attachments"):
                    receipt["status"] = "retry_required"
                    receipt["recovery_reason"] = "durable attachment references unavailable"
                    _scrub_turn_receipt_recovery_fields(receipt)
                elif str(receipt.get("dispatch_mode") or "brain") != "worker":
                    receipt["status"] = "uncertain"
                    receipt["recovery_reason"] = "brain turn outcome is ambiguous after restart"
                    _scrub_turn_receipt_recovery_fields(receipt)
                else:
                    if len(queued) >= THREAD_TURN_QUEUE_LIMIT:
                        receipt["status"] = "retry_required"
                        receipt["recovery_reason"] = "thread turn queue is full during recovery"
                        _scrub_turn_receipt_recovery_fields(receipt)
                        receipts[index] = receipt
                        continue
                    queue_id = str(receipt.get("logical_turn_id") or new_id("queuedturn"))
                    if not any(str(item.get("queue_id") or "") == queue_id for item in queued):
                        queued.append(
                            {
                                "queue_id": queue_id,
                                "idempotency_key": str(receipt.get("idempotency_key") or queue_id),
                                "fingerprint": str(receipt.get("fingerprint") or ""),
                                "text": str(receipt.get("text") or ""),
                                "queued_at": str(receipt.get("created_at") or utc_now()),
                                "status": "queued",
                                "requester": dict(receipt.get("requester") or {}),
                                "workspace_request": receipt.get("workspace_request"),
                                "attachments": [],
                            }
                        )
                    receipt["queue_id"] = queue_id
                    receipt["status"] = "queued"
                receipts[index] = receipt
            if not changed:
                return thread
            return self.save(
                replace(
                    thread,
                    queued_turns=tuple(queued),
                    turn_receipts=tuple(receipts),
                    updated_at=utc_now(),
                )
            )

    def claim_queued_turn(self, project_id: str, thread_id: str) -> dict[str, Any] | None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return None
            queued = [dict(item) for item in thread.queued_turns]
            for index, item in enumerate(queued):
                if str(item.get("status") or "queued") != "queued":
                    continue
                item["status"] = "claimed"
                item["claimed_at"] = utc_now()
                queued[index] = item
                self.save(replace(thread, queued_turns=tuple(queued)))
                return dict(item)
            return None

    def finish_queued_turn(
        self,
        project_id: str,
        thread_id: str,
        queue_id: str,
        *,
        retry: bool = False,
    ) -> None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return
            queued: list[dict[str, Any]] = []
            for raw in thread.queued_turns:
                item = dict(raw)
                if str(item.get("queue_id") or "") != queue_id:
                    queued.append(item)
                elif retry:
                    item.pop("claimed_at", None)
                    item["status"] = "queued"
                    queued.append(item)
            receipts = [dict(item) for item in thread.turn_receipts]
            if not retry:
                for index, receipt in enumerate(receipts):
                    if str(receipt.get("queue_id") or "") == queue_id:
                        receipt["status"] = "completed"
                        receipt["updated_at"] = utc_now()
                        _scrub_turn_receipt_recovery_fields(receipt)
                        receipts[index] = receipt
                        break
            self.save(
                replace(
                    thread,
                    queued_turns=tuple(queued),
                    turn_receipts=tuple(receipts),
                    updated_at=utc_now(),
                )
            )

    def reserve_execution_turn(self, project_id: str, thread_id: str) -> CockpitThread | None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return None
            if not str(thread.workspace.get("session_id") or ""):
                return thread
            status = str(thread.workspace.get("status") or "")
            if status == "interrupting":
                raise RuntimeError("conversation execution is being interrupted")
            if status in {"starting", "running"}:
                raise RuntimeError("conversation execution already has an active turn")
            return self.save(
                replace(
                    thread,
                    updated_at=utc_now(),
                    workspace={
                        **thread.workspace,
                        "status": "starting",
                        "provision_phase": "starting",
                    },
                )
            )

    def release_execution_turn(self, project_id: str, thread_id: str) -> CockpitThread | None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None or str(thread.workspace.get("status") or "") != "starting":
                return thread
            return self.save(
                replace(
                    thread,
                    workspace={
                        **thread.workspace,
                        "status": "ready",
                        "provision_phase": "ready",
                    },
                )
            )

    def recover_orphaned_execution(self, project_id: str, thread_id: str) -> CockpitThread | None:
        """Release an execution lease left behind by a previous API process.

        A foreground turn holds the lease in memory of the process running it.
        Once that process is gone the turn cannot still be in flight, so a lease
        surviving a restart is orphaned — and while it survives, every new turn
        queues behind an execution that will never finish. The provider session
        itself is untouched: the next turn re-ensures or replaces it.
        """
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return None
            status = str(thread.workspace.get("status") or "")
            if status not in {"starting", "running"}:
                return thread
            return self.save(
                replace(
                    thread,
                    updated_at=utc_now(),
                    workspace={
                        **thread.workspace,
                        "status": "ready",
                        "provision_phase": "ready",
                        "provider_started": False,
                    },
                )
            )

    def claim_execution_interrupt(self, project_id: str, thread_id: str) -> CockpitThread | None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return None
            if not str(thread.workspace.get("session_id") or ""):
                return thread
            return self.save(
                replace(
                    thread,
                    updated_at=utc_now(),
                    workspace={
                        **thread.workspace,
                        "status": "interrupting",
                        "provision_phase": "interrupting",
                    },
                )
            )

    def detach_execution_if_matches(
        self,
        project_id: str,
        thread_id: str,
        *,
        expected_session_id: str,
        expected_generation: int,
    ) -> CockpitThread | None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return None
            current_session_id = str(thread.workspace.get("session_id") or "")
            current_generation = int(thread.workspace.get("session_generation") or 0)
            if (
                current_session_id != expected_session_id
                or current_generation != expected_generation
            ):
                return thread
            return self.save(
                replace(
                    thread,
                    updated_at=utc_now(),
                    workspace={
                        **thread.workspace,
                        "session_id": "",
                        "provider_started": False,
                        "status": "interrupted",
                        "provision_phase": "interrupted",
                        "session_generation": current_generation + 1,
                    },
                )
            )

    def update_execution_if_matches(
        self,
        project_id: str,
        thread_id: str,
        *,
        expected_session_id: str,
        expected_generation: int,
        status: str,
        provision_phase: str,
        extra: dict[str, Any] | None = None,
    ) -> CockpitThread | None:
        with _THREAD_INDEX_LOCK:
            thread = self.get(project_id, thread_id)
            if thread is None:
                return None
            if (
                str(thread.workspace.get("session_id") or "") != expected_session_id
                or int(thread.workspace.get("session_generation") or 0) != expected_generation
                or str(thread.workspace.get("status") or "") == "interrupting"
            ):
                return None
            return self.save(
                replace(
                    thread,
                    updated_at=utc_now(),
                    workspace={
                        **thread.workspace,
                        "status": status,
                        "provision_phase": provision_phase,
                        **(extra or {}),
                    },
                )
            )

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
        # Transcript writers take the per-thread lock before the index lock.
        # Keep that order here so a deletion cannot interleave with an append
        # after the index update but before the transcript write.
        with self._transcript_lock(project_id, thread_id):
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
                    atomic_write_json(self.path, data)
                    for transcript_path in (
                        self._transcript_path(project_id, thread_id),
                        self._legacy_transcript_path(project_id, thread_id),
                        self._legacy_transcript_path(project_id, thread_id).with_suffix(".json.bak"),
                    ):
                        try:
                            transcript_path.unlink(missing_ok=True)
                        except OSError:
                            pass
                        self._transcript_cache_pop(transcript_path)
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
        thread = next((item for item in self._threads().values() if item.thread_id == parent_chat_id), None)
        if thread is None:
            return False
        with self._transcript_lock(thread.project_id, thread.thread_id):
            thread = self.get(thread.project_id, thread.thread_id)
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
            self._append_thread_messages(updated, [messages[-1]])
            return True

    def register_child_watch(
        self,
        thread: CockpitThread,
        child_ids: list[str],
        *,
        requester: RequestContext,
        continuation_instruction: str = "",
    ) -> str:
        normalized = sorted(set(child_ids))
        watch_id = hashlib.sha256("\n".join(normalized).encode("utf-8")).hexdigest()[:20]
        with self._transcript_lock(thread.project_id, thread.thread_id):
            stored = self.get(thread.project_id, thread.thread_id)
            if stored is None:
                return watch_id
            messages = self._thread_messages(stored)
            continuation = str(continuation_instruction or "").strip()
            requester_snapshot = _snapshot_from_context(requester)
            existing = self._latest_child_watches(messages).get(watch_id)
            existing_phase = str((existing or {}).get("phase") or "")
            if existing is None:
                messages.append(
                    {
                        "role": "system",
                        "peer_id": "jarvis",
                        "type": "child_watch",
                        "watch_id": watch_id,
                        "child_chat_ids": normalized,
                        "continuation_instruction": continuation,
                        "requester": requester_snapshot,
                        "phase": "waiting",
                        "content": f"Watching {len(normalized)} child work session(s) for completion.",
                        "observed_at": utc_now(),
                    }
                )
                self._append_thread_messages(stored, [messages[-1]])
            elif existing_phase in {"completed", "failed"}:
                # watch_id is deterministic from the child ids, so re-watching the
                # same children after a finished run lands on the terminal record.
                # Reopen it as waiting instead of leaving a pending marker that can
                # never be claimed (which would pin the parent to 'running').
                reopened = dict(existing)
                reopened["phase"] = "waiting"
                reopened["continuation_instruction"] = continuation
                reopened["requester"] = requester_snapshot
                reopened["observed_at"] = utc_now()
                for key in ("claimed_at", "completed_at", "error"):
                    reopened.pop(key, None)
                self._append_thread_messages(stored, [reopened])
            elif (
                existing_phase == "waiting"
                and continuation
                and existing.get("requester") == requester_snapshot
                and continuation != str(existing.get("continuation_instruction") or "")
            ):
                refreshed = dict(existing)
                refreshed["continuation_instruction"] = continuation
                refreshed["observed_at"] = utc_now()
                self._append_thread_messages(stored, [refreshed])
            pending_watch_ids = {
                str(item)
                for item in stored.workspace.get("pending_child_watch_ids") or []
                if str(item).strip()
            }
            if watch_id not in pending_watch_ids:
                pending_watch_ids.add(watch_id)
                self.save(
                    replace(
                        stored,
                        workspace={
                            **stored.workspace,
                            "pending_child_watch_ids": sorted(pending_watch_ids),
                        },
                        updated_at=utc_now(),
                    )
                )
            return watch_id

    @staticmethod
    def _latest_child_watches(messages: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        """Effective state per watch: the transcript is append-only, so each
        renewal/claim/finish adds a record and only the *last* one for a watch
        id is current. Scanning for the first match reads state that a later
        record has already superseded — a completed watch still looked
        `waiting`, which could re-claim it and fire a duplicate continuation.
        """
        latest: dict[str, dict[str, Any]] = {}
        for message in messages:
            if message.get("type") != "child_watch":
                continue
            watch_id = str(message.get("watch_id") or "")
            if watch_id:
                latest[watch_id] = message
        return latest

    def claim_ready_child_watch(self, parent_chat_id: str, terminal_child_ids: set[str]) -> dict[str, Any] | None:
        thread = next((item for item in self._threads().values() if item.thread_id == parent_chat_id), None)
        if thread is None:
            return None
        with self._transcript_lock(thread.project_id, thread.thread_id):
            thread = self.get(thread.project_id, thread.thread_id)
            if thread is None:
                return None
            messages = self._thread_messages(thread)
            claimed: dict[str, Any] | None = None
            for message in self._latest_child_watches(messages).values():
                expected = {str(item) for item in message.get("child_chat_ids") or []}
                phase = str(message.get("phase") or "")
                lease_expired = phase == "claimed" and _timestamp_before(
                    str(message.get("claimed_at") or ""),
                    datetime.now(UTC) - timedelta(seconds=CHILD_WATCH_LEASE_S),
                )
                if (phase == "waiting" or lease_expired) and expected and expected <= terminal_child_ids:
                    message = dict(message)
                    message["phase"] = "claimed"
                    message["claimed_at"] = utc_now()
                    claimed = message
                    break
            if claimed is not None:
                self._append_thread_messages(thread, [claimed])
            return claimed

    def finish_child_watch(self, parent_chat_id: str, watch_id: str, *, error: str = "") -> None:
        thread = next((item for item in self._threads().values() if item.thread_id == parent_chat_id), None)
        if thread is None:
            return
        with self._transcript_lock(thread.project_id, thread.thread_id):
            thread = self.get(thread.project_id, thread.thread_id)
            if thread is None:
                return
            messages = self._thread_messages(thread)
            latest = self._latest_child_watches(messages).get(watch_id)
            if latest is None:
                return
            finished = dict(latest)
            finished["phase"] = "failed" if error else "completed"
            finished["completed_at"] = utc_now()
            if error:
                finished["error"] = public_error_message(error)
            self._append_thread_messages(thread, [finished])
            pending_watch_ids = {
                str(item)
                for item in thread.workspace.get("pending_child_watch_ids") or []
                if str(item).strip() and str(item) != watch_id
            }
            workspace = {**thread.workspace}
            if pending_watch_ids:
                workspace["pending_child_watch_ids"] = sorted(pending_watch_ids)
            else:
                workspace.pop("pending_child_watch_ids", None)
            self.save(replace(thread, workspace=workspace, updated_at=utc_now()))

    def renew_child_watch_claim(self, parent_chat_id: str, watch_id: str) -> None:
        thread = next((item for item in self._threads().values() if item.thread_id == parent_chat_id), None)
        if thread is None:
            return
        with self._transcript_lock(thread.project_id, thread.thread_id):
            thread = self.get(thread.project_id, thread.thread_id)
            if thread is None:
                return
            messages = self._thread_messages(thread)
            latest = self._latest_child_watches(messages).get(watch_id)
            if latest is None or str(latest.get("phase") or "") != "claimed":
                return
            # The lease is still comfortably alive; another record would only
            # grow the transcript.
            if not _timestamp_before(
                str(latest.get("claimed_at") or ""),
                datetime.now(UTC) - timedelta(seconds=CHILD_WATCH_RENEW_INTERVAL_S),
            ):
                return
            renewed = dict(latest)
            renewed["claimed_at"] = utc_now()
            self._append_thread_messages(thread, [renewed])

    def child_watch_is_claimed(self, parent_chat_id: str, watch_id: str) -> bool:
        thread = next((item for item in self._threads().values() if item.thread_id == parent_chat_id), None)
        if thread is None:
            return False
        with self._transcript_lock(thread.project_id, thread.thread_id):
            thread = self.get(thread.project_id, thread.thread_id)
            if thread is None:
                return False
            latest = self._latest_child_watches(self._thread_messages(thread)).get(watch_id)
            return latest is not None and str(latest.get("phase") or "") == "claimed"

    def append_turn(
        self,
        thread: CockpitThread,
        *,
        user_peer_id: str,
        user_text: str,
        assistant_peer_id: str,
        assistant_text: str,
        events: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
        idempotency_key: str = "",
    ) -> CockpitThread:
        with self._transcript_lock(thread.project_id, thread.thread_id):
            observed_at = utc_now()
            stored = self.get(thread.project_id, thread.thread_id)
            if stored is None and self._is_deleted(thread.project_id, thread.thread_id):
                raise KeyError(thread.thread_id)
            archive_source = stored or thread
            existing_messages = self._thread_messages(archive_source, seed_messages=thread.messages)
            if idempotency_key and any(
                message.get("turn_idempotency_key") == idempotency_key
                for message in existing_messages
            ):
                return replace(archive_source, messages=tuple(existing_messages))
            appended = [
                {
                    "role": "user",
                    "peer_id": user_peer_id,
                    "content": user_text,
                    "observed_at": observed_at,
                    "turn_idempotency_key": idempotency_key,
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
                    # The store owns the workspace: a caller's snapshot can be
                    # arbitrarily stale and would drop or resurrect pending
                    # child-watch ids on merge.
                    workspace=dict(archive_source.workspace),
                    queued_turns=tuple(archive_source.queued_turns),
                    turn_receipts=tuple(archive_source.turn_receipts),
                )
            )
            messages = self._append_thread_messages(updated, appended, seed_messages=thread.messages)
            return replace(updated, messages=tuple(messages))

    def append_pending_turn(
        self,
        thread: CockpitThread,
        *,
        user_peer_id: str,
        user_text: str,
        assistant_peer_id: str,
        idempotency_key: str = "",
    ) -> CockpitThread:
        with self._transcript_lock(thread.project_id, thread.thread_id):
            observed_at = utc_now()
            stored = self.get(thread.project_id, thread.thread_id)
            if stored is None and self._is_deleted(thread.project_id, thread.thread_id):
                raise KeyError(thread.thread_id)
            archive_source = stored or thread
            existing_messages = self._thread_messages(archive_source, seed_messages=thread.messages)
            if idempotency_key and any(
                message.get("turn_idempotency_key") == idempotency_key
                for message in existing_messages
            ):
                return replace(archive_source, messages=tuple(existing_messages))
            appended = [
                {
                    "role": "user",
                    "peer_id": user_peer_id,
                    "content": user_text,
                    "observed_at": observed_at,
                    "turn_idempotency_key": idempotency_key,
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
                    workspace=dict(archive_source.workspace),
                    queued_turns=tuple(archive_source.queued_turns),
                    turn_receipts=tuple(archive_source.turn_receipts),
                )
            )
            messages = self._append_thread_messages(updated, appended, seed_messages=thread.messages)
            return replace(updated, messages=tuple(messages))

    def _threads(self, *, include_messages: bool = False) -> dict[str, CockpitThread]:
        return {
            thread_id: CockpitThread.from_dict(raw, include_messages=include_messages)
            for thread_id, raw in self._read().get("threads", {}).items()
            if isinstance(raw, dict)
        }

    def _is_deleted(self, project_id: str, thread_id: str) -> bool:
        raw = self._read().get("deleted_threads", {}).get(thread_id)
        return isinstance(raw, dict) and str(raw.get("project_id") or "") == project_id

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
        with self._transcript_lock(thread.project_id, thread.thread_id):
            legacy_path = self._legacy_transcript_path(thread.project_id, thread.thread_id)
            if legacy_path.exists():
                if not self._migrate_legacy_transcript(legacy_path, path):
                    return self._read_legacy_thread_messages(legacy_path)
            return self._read_jsonl_messages(path)

    def _append_thread_messages(
        self,
        thread: CockpitThread,
        messages: list[dict[str, Any]],
        *,
        seed_messages: tuple[dict[str, Any], ...] = (),
    ) -> list[dict[str, Any]]:
        """Append complete records and retain a full projection for the caller."""

        path = self._transcript_path(thread.project_id, thread.thread_id)
        with self._transcript_lock(thread.project_id, thread.thread_id):
            existing = self._cached_jsonl_messages(path)
            seed_records: list[dict[str, Any]] = []
            if existing is None:
                if path.exists():
                    existing = self._read_jsonl_messages(path)
                else:
                    seed_records = _normalized_messages(seed_messages)
                    existing = [dict(message) for message in seed_records]
            cached_entry = self._transcript_cache_entry(path)
            # A crashed/external writer can leave bytes without a line ending.
            # Terminate that unusable record before appending so our first JSON
            # record never becomes part of an invalid trailing line.
            needs_separator = cached_entry is not None and cached_entry[4] < cached_entry[0]
            appended = _normalized_messages(messages)
            if appended:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as handle:
                    if needs_separator:
                        handle.write("\n")
                    for message in seed_records:
                        handle.write(json.dumps(message, sort_keys=True) + "\n")
                    for message in appended:
                        handle.write(json.dumps(message, sort_keys=True) + "\n")
                if needs_separator:
                    self._transcript_cache_pop(path)
                    return self._read_jsonl_messages(path)
                for message in appended:
                    _merge_thread_message(existing, message)
                try:
                    stat = path.stat()
                except OSError:
                    self._transcript_cache_pop(path)
                else:
                    self._cache_thread_messages(
                        path,
                        stat.st_size,
                        stat.st_mtime_ns,
                        stat.st_dev,
                        stat.st_ino,
                        stat.st_size,
                        existing,
                    )
            return [dict(message) for message in existing]

    def _read_jsonl_messages(self, path: Path) -> list[dict[str, Any]]:
        try:
            stat = path.stat()
        except OSError:
            self._transcript_cache_pop(path)
            return []
        cached_entry = self._transcript_cache_entry(path)
        try:
            fingerprint, offset, messages = read_jsonl_projection(
                path,
                stat,
                cached_entry,
                clone=dict,
                merge=self._merge_jsonl_message,
            )
        except OSError:
            return []
        self._cache_thread_messages(path, *fingerprint, offset, messages)
        return [dict(message) for message in messages]

    def _migrate_legacy_transcript(self, legacy_path: Path, path: Path) -> bool:
        """Convert the old single-JSON transcript once, retaining a recoverable copy."""

        if path.exists():
            self._finish_legacy_transcript_migration(legacy_path)
            return True
        try:
            data = json.loads(legacy_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return False
        if not isinstance(data, dict):
            return False
        try:
            _atomic_write_jsonl(path, _normalized_messages(data.get("messages") or ()))
        except OSError:
            return False
        self._transcript_cache_pop(path)
        self._finish_legacy_transcript_migration(legacy_path)
        return True

    @staticmethod
    def _read_legacy_thread_messages(path: Path) -> list[dict[str, Any]]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return []
        return _normalized_messages(data.get("messages") or ()) if isinstance(data, dict) else []

    @staticmethod
    def _finish_legacy_transcript_migration(legacy_path: Path) -> None:
        if not legacy_path.exists():
            return
        backup_path = legacy_path.with_suffix(legacy_path.suffix + ".bak")
        try:
            if backup_path.exists():
                legacy_path.unlink()
            else:
                os.replace(legacy_path, backup_path)
        except OSError:
            pass

    def _transcript_lock(self, project_id: str, thread_id: str) -> threading.RLock:
        path = self._transcript_path(project_id, thread_id)
        with _THREAD_TRANSCRIPT_LOCKS_LOCK:
            return _THREAD_TRANSCRIPT_LOCKS.setdefault(path, threading.RLock())

    def _cached_jsonl_messages(self, path: Path, *, stat: os.stat_result | None = None) -> list[dict[str, Any]] | None:
        cached = self._transcript_cache_entry(path)
        if cached is None:
            return None
        if stat is None:
            try:
                stat = path.stat()
            except OSError:
                self._transcript_cache_pop(path)
                return None
        if cached[:4] != (stat.st_size, stat.st_mtime_ns, stat.st_dev, stat.st_ino):
            return None
        return [dict(message) for message in cached[5]]

    def _transcript_cache_entry(self, path: Path) -> tuple[int, int, int, int, int, list[dict[str, Any]]] | None:
        with self._transcript_cache_lock:
            cached = self._transcript_cache.get(path)
            if cached is not None:
                self._transcript_cache.move_to_end(path)
            return cached

    def _transcript_cache_pop(self, path: Path) -> None:
        with self._transcript_cache_lock:
            self._transcript_cache.pop(path, None)

    def _cache_thread_messages(
        self,
        path: Path,
        size: int,
        mtime_ns: int,
        device: int,
        inode: int,
        offset: int,
        messages: list[dict[str, Any]],
    ) -> None:
        with self._transcript_cache_lock:
            self._transcript_cache[path] = (size, mtime_ns, device, inode, offset, list(messages))
            self._transcript_cache.move_to_end(path)
            while len(self._transcript_cache) > _THREAD_TRANSCRIPT_CACHE_MAX:
                self._transcript_cache.popitem(last=False)

    @staticmethod
    def _merge_jsonl_message(messages: list[dict[str, Any]], record: object) -> None:
        if isinstance(record, dict):
            for message in _normalized_messages([record]):
                _merge_thread_message(messages, message)

    def _transcript_path(self, project_id: str, thread_id: str) -> Path:
        return self.transcripts_dir / _safe_path_segment(project_id) / f"{_safe_path_segment(thread_id)}.jsonl"

    def _legacy_transcript_path(self, project_id: str, thread_id: str) -> Path:
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
            atomic_write_json(self.path, data)
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
                    _atomic_write_jsonl(path, messages)
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

    def claim_execution_interrupt(self, project: ProjectEntry, thread_id: str) -> CockpitThread | None:
        return self._index.claim_execution_interrupt(project.id, thread_id)

    def detach_interrupted_execution(
        self,
        project: ProjectEntry,
        thread_id: str,
        *,
        expected_session_id: str,
        expected_generation: int,
    ) -> CockpitThread | None:
        return self._index.detach_execution_if_matches(
            project.id,
            thread_id,
            expected_session_id=expected_session_id,
            expected_generation=expected_generation,
        )

    async def drain_queued_turns(self, project: ProjectEntry, thread_id: str) -> int:
        drained = 0
        while True:
            thread = self._index.get(project.id, thread_id)
            if thread is None or thread.archived_at or not thread.queued_turns:
                return drained
            worker_id = str(thread.workspace.get("worker_id") or thread.worker_id or "")
            session_id = str(thread.workspace.get("session_id") or "")
            if worker_id and session_id:
                execution = await asyncio.to_thread(
                    self._get_worker_json,
                    worker_id,
                    f"/sessions/{session_id}/execution-state",
                )
                if isinstance(execution.get("active_turn"), dict) or str(execution.get("status") or "") in {
                    "starting",
                    "running",
                    "waiting_approval",
                    "waiting_input",
                    "interrupting",
                }:
                    return drained
            queued = self._index.claim_queued_turn(project.id, thread_id)
            if queued is None:
                return drained
            queue_id = str(queued.get("queue_id") or "")
            try:
                await self.turn(
                    project,
                    thread,
                    _context_from_snapshot(dict(queued.get("requester") or {})),
                    str(queued.get("text") or ""),
                    attachments=[dict(item) for item in queued.get("attachments") or () if isinstance(item, dict)] or None,
                    workspace_request=(
                        dict(queued["workspace_request"])
                        if isinstance(queued.get("workspace_request"), dict)
                        else None
                    ),
                    logical_turn_id=queue_id,
                    idempotency_key=str(queued.get("idempotency_key") or queue_id),
                )
            except WorkerRequestError as exc:
                self._index.release_execution_turn(project.id, thread_id)
                self._index.finish_queued_turn(project.id, thread_id, queue_id, retry=True)
                if exc.code == WORKER_ERROR_SESSION_ACTIVE:
                    return drained
                raise
            except Exception:
                self._index.release_execution_turn(project.id, thread_id)
                self._index.finish_queued_turn(project.id, thread_id, queue_id, retry=True)
                raise
            self._index.finish_queued_turn(project.id, thread_id, queue_id)
            drained += 1

    def delete_thread(self, project_id: str, thread_id: str) -> tuple[CockpitThread | None, bool]:
        return self._index.delete(project_id, thread_id)

    async def open_thread(
        self,
        project: ProjectEntry,
        requester: RequestContext,
        *,
        title: str = "",
        parent_chat_id: str = "",
        chat_type: str = "assistant",
        engine: str = "",
        model: str = "",
        worker_id: str = "",
    ) -> CockpitThread:
        chat_type = str(chat_type or "assistant").strip().lower()
        if chat_type not in {"assistant", "orchestrator"}:
            raise ValueError(f"unsupported chat_type: {chat_type}")
        resolved_engine = str(engine or ("jarvis" if chat_type == "assistant" else "codex")).strip().lower()
        if chat_type == "orchestrator" and resolved_engine not in {"codex", "claude"}:
            raise ValueError("orchestrator engine must be codex or claude")
        if chat_type == "assistant":
            resolved_engine = "jarvis"
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
            chat_type=chat_type,
            engine=resolved_engine,
            model=str(model or "").strip(),
            worker_id=str(worker_id or "").strip(),
            parent_chat_id=parent_chat_id.strip(),
        )
        peers = _thread_peers(self._cfg, project, requester)
        await asyncio.to_thread(
            self._memory.create_session,
            session_id,
            peers=peers,
            metadata={
                "kind": "cockpit_orchestrator",
                "chat_type": chat_type,
                "engine": resolved_engine,
                "model": str(model or "").strip(),
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
        cold_task_sink: Callable[[BrainSession], Any] | None = None,
        logical_turn_id: str = "",
        idempotency_key: str = "",
    ) -> tuple[str, CockpitThread, tuple[dict[str, Any], ...]]:
        text = text.strip()
        if not text:
            raise ValueError("turn text is required")
        context_thread = self._index.get_with_messages(project.id, thread.thread_id, limit=THREAD_HISTORY_LIMIT) or thread
        explicit_workspace_request = workspace_request is not None
        workspace_request = dict(workspace_request or {})
        if context_thread.chat_type == "orchestrator":
            requested_engine = str(workspace_request.get("engine") or "").strip().lower()
            requested_tuning = {
                key: str(workspace_request.get(key) or "").strip()
                for key in ("model", "effort", "speed")
            }
            requested_tuning = {key: value for key, value in requested_tuning.items() if value}
            if requested_engine or requested_tuning:
                context_thread = self._apply_orchestrator_engine_and_model(
                    project,
                    context_thread,
                    requested_engine,
                    requested_tuning.get("model", ""),
                )
            reply, updated = await self._orchestrator_turn(
                project,
                context_thread,
                requester,
                _text_with_attachment_markers(text, attachments),
                requested_tuning=requested_tuning,
                progress=progress,
                logical_turn_id=logical_turn_id,
                idempotency_key=idempotency_key,
            )
            return reply, updated, ()
        if is_conversation_workspace(context_thread.workspace) or explicit_workspace_request:
            reply, updated = await self._workspace_turn(
                project,
                context_thread,
                requester,
                _text_with_attachment_markers(text, attachments),
                workspace_request=workspace_request,
                progress=progress,
                logical_turn_id=logical_turn_id,
                idempotency_key=idempotency_key,
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
        async def on_text(delta: str) -> None:
            await _emit_progress(progress, {"type": "text.delta", "delta": delta})

        reply = await session.respond_text(
            text, trace, result, attachments=attachments, on_text=on_text if progress is not None else None
        )
        events = _safe_thread_tool_events(thread.session_id, result.tool_messages)
        guarded_reply = _guard_project_thread_reply(result.raw or reply, events)
        if guarded_reply != (result.raw or reply):
            result.raw = guarded_reply
        session.finalize(text, result, trace)
        if cold_task_sink is not None:
            emitted = cold_task_sink(session)
            if inspect.isawaitable(emitted):
                await emitted
        else:
            schedule_cold_task_drain(session)
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
            idempotency_key=idempotency_key,
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
        logical_turn_id: str = "",
        idempotency_key: str = "",
    ) -> tuple[str, CockpitThread]:
        await _emit_progress(progress, {"phase": "resolving-access", "thread_id": thread.thread_id})
        thread = self._index.reserve_execution_turn(project.id, thread.thread_id) or thread
        reserved_session_id = str(thread.workspace.get("session_id") or "")
        thread = await self._ensure_workspace(project, thread, requester, workspace_request=workspace_request, progress=progress)
        stored_thread = self._index.get(project.id, thread.thread_id) or thread
        if (
            str(stored_thread.workspace.get("session_id") or "") != reserved_session_id
            or str(stored_thread.workspace.get("status") or "") != "starting"
        ):
            thread = self._index.reserve_execution_turn(project.id, thread.thread_id) or thread
        else:
            thread = stored_thread
        worker_id = str(thread.workspace.get("worker_id") or "")
        session_id = str(thread.workspace.get("session_id") or "")
        session_generation = int(thread.workspace.get("session_generation") or 0)
        if not worker_id or not session_id:
            raise RuntimeError("conversation workspace has no worker session")
        await _emit_progress(progress, {"phase": "running", "thread_id": thread.thread_id, "workspace": workspace_public(thread.workspace)})
        requested_tuning = {
            key: str(workspace_request.get(key) or "").strip()
            for key in ("model", "effort", "speed")
        }
        requested_tuning = {key: value for key, value in requested_tuning.items() if value}
        turn = await asyncio.to_thread(
            self._post_worker_json,
            worker_id,
            f"/sessions/{session_id}/turns",
            {
                "turn_id": logical_turn_id or new_id("turn"),
                "prompt": _workspace_prompt(project, thread, text),
                "metadata": {
                    "surface": "cockpit_thread",
                    "project_id": project.id,
                    "thread_id": thread.thread_id,
                    "honcho_session_id": thread.session_id,
                    "allowed_actions": [WORKER_SESSION_TURN],
                },
                "idempotency_key": (
                    f"thread-turn:{thread.thread_id}:{idempotency_key}"
                    if idempotency_key
                    else f"thread-turn:{thread.thread_id}:{new_id('turn')}:{_stable_text_hash(text)}"
                ),
                # Worker sessions switch model, effort and speed in place — the
                # worker writes them to session metadata and the provider picks
                # them up on this turn. No respawn, unlike the orchestrator path.
                **requested_tuning,
            },
        )
        if not turn.get("ok", True):
            raise RuntimeError(str(turn.get("error") or "worker rejected conversation turn"))
        thread = self._index.update_execution_if_matches(
            project.id,
            thread.thread_id,
            expected_session_id=session_id,
            expected_generation=session_generation,
            status="ready",
            provision_phase="ready",
        )
        if thread is None:
            raise RuntimeError("conversation execution was interrupted")
        applied = {
            key: value
            for key, value in requested_tuning.items()
            if value != getattr(thread, key, "")
        }
        if applied:
            # The worker accepted them, so they are effective from here on;
            # thread detail reports them without waiting for a provider event.
            thread = self._index.save(
                replace(
                    thread,
                    **applied,
                    updated_at=utc_now(),
                    workspace={**thread.workspace, **applied},
                )
            )
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
            idempotency_key=idempotency_key,
        )
        return reply, updated

    async def _orchestrator_turn(
        self,
        project: ProjectEntry,
        thread: CockpitThread,
        requester: RequestContext,
        text: str,
        *,
        requested_tuning: dict[str, str] | None = None,
        progress: Callable[[dict[str, Any]], Any] | None,
        logical_turn_id: str = "",
        idempotency_key: str = "",
    ) -> tuple[str, CockpitThread]:
        requested_tuning = dict(requested_tuning or {})
        thread = self._index.reserve_execution_turn(project.id, thread.thread_id) or thread
        reserved_session_id = str(thread.workspace.get("session_id") or "")
        thread = await self._ensure_orchestrator_session(project, thread, requester, progress=progress)
        stored_thread = self._index.get(project.id, thread.thread_id) or thread
        if (
            str(stored_thread.workspace.get("session_id") or "") != reserved_session_id
            or str(stored_thread.workspace.get("status") or "") != "starting"
        ):
            thread = self._index.reserve_execution_turn(project.id, thread.thread_id) or thread
        else:
            thread = stored_thread
        worker_id = str(thread.workspace.get("worker_id") or "")
        session_id = str(thread.workspace.get("session_id") or "")
        session_generation = int(thread.workspace.get("session_generation") or 0)
        if not worker_id or not session_id:
            raise RuntimeError("orchestrator conversation has no worker session")
        context = _project_context(self._memory, project, thread)
        prompt = (
            "Authoritative Jarvis project context for this turn:\n"
            f"{context}\n\nCurrent orchestration instruction:\n{text}"
        )
        resume = bool(thread.workspace.get("provider_started"))
        grant = mint_orchestrator_grant(
            self._cfg.orchestration,
            project_id=project.id,
            thread_id=thread.thread_id,
            requester=requester,
        )
        turn_id = logical_turn_id or new_id("turn")
        response = await asyncio.to_thread(
            self._post_worker_json,
            worker_id,
            f"/sessions/{session_id}/turns",
            {
                "turn_id": turn_id,
                "prompt": prompt,
                "metadata": {
                    "surface": "cockpit_orchestrator",
                    "project_id": project.id,
                    "thread_id": thread.thread_id,
                    "resume_session": resume,
                    "allowed_actions": [WORKER_SESSION_TURN],
                },
                "runtime_context": {
                    "orchestrator_mcp": {
                        "api_url": orchestrator_api_base_url(self._cfg.orchestration),
                        "project_id": project.id,
                        "thread_id": thread.thread_id,
                        "grant": grant,
                        "timeout_s": _orchestrator_mcp_timeout_s(self._cfg),
                    }
                },
                "idempotency_key": (
                    f"orchestrator-turn:{thread.thread_id}:{idempotency_key}"
                    if idempotency_key
                    else f"orchestrator-turn:{thread.thread_id}:{turn_id}"
                ),
                # Same contract as worker-session turns: the worker writes model,
                # effort and speed to session metadata before reserving the turn,
                # and the provider reads them when it starts. A model change has
                # already respawned the session above; effort and speed apply in
                # place (codex) or via the adapter's retire+resume (claude).
                **requested_tuning,
            },
        )
        if response.get("ok") is False:
            raise RuntimeError(str(response.get("error") or "worker rejected orchestrator turn"))
        thread = self._index.update_execution_if_matches(
            project.id,
            thread.thread_id,
            expected_session_id=session_id,
            expected_generation=session_generation,
            status="running",
            provision_phase="running",
            extra={"provider_started": True},
        )
        if thread is None:
            raise RuntimeError("conversation execution was interrupted")
        applied = {
            key: value
            for key, value in requested_tuning.items()
            if value != getattr(thread, key, "")
        }
        if applied:
            # The worker accepted them, so they are effective from here on;
            # thread detail reports them without waiting for a provider event.
            thread = self._index.save(
                replace(
                    thread,
                    **applied,
                    updated_at=utc_now(),
                    workspace={**thread.workspace, **applied},
                )
            )
        await _emit_progress(progress, {"phase": "running", "thread_id": thread.thread_id, "workspace": workspace_public(thread.workspace)})
        try:
            reply = await self._wait_for_orchestrator_turn(worker_id, session_id, turn_id)
        except Exception:
            failed = self._index.update_execution_if_matches(
                project.id,
                thread.thread_id,
                expected_session_id=session_id,
                expected_generation=session_generation,
                status="failed",
                provision_phase="failed",
            )
            if failed is not None:
                await _emit_progress(
                    progress,
                    {
                        "phase": "failed",
                        "thread_id": failed.thread_id,
                        "workspace": workspace_public(failed.workspace),
                    },
                )
            raise
        try:
            await asyncio.to_thread(
                self._persist_turn,
                thread.session_id,
                requester.memory_peer,
                requester.device_id,
                text,
                reply,
            )
        except Exception:
            pass
        completed = self._index.update_execution_if_matches(
            project.id,
            thread.thread_id,
            expected_session_id=session_id,
            expected_generation=session_generation,
            status="ready",
            provision_phase="ready",
        )
        if completed is None:
            raise RuntimeError("conversation execution was interrupted")
        return reply, self._index.append_turn(
            completed,
            user_peer_id=requester.memory_peer,
            user_text=text,
            assistant_peer_id=self._cfg.memory.assistant_peer_id,
            assistant_text=reply,
            idempotency_key=idempotency_key,
        )

    def _apply_orchestrator_engine_and_model(
        self,
        project: ProjectEntry,
        thread: CockpitThread,
        engine: str,
        model: str = "",
    ) -> CockpitThread:
        """Route the reserved turn to `engine`/`model`, replacing any previous
        provider session.

        Turn reservation upstream guarantees no other execution is in flight, so
        dropping the session is replay-safe: history lives on the thread and the
        orchestrator prompt rebuilds project context every turn. A model change
        takes the same path as an engine change — provider sessions are pinned to
        the model they spawned with, so a fresh one is the only honest way to
        switch.
        """
        engine = str(engine or "").strip().lower() or thread.engine
        model = str(model or "").strip()
        if engine not in ORCHESTRATOR_ENGINES:
            raise ValueError("orchestrator engine must be codex or claude")
        engine_changed = engine != thread.engine
        # Crossing engines retires the old model id — it belongs to the previous
        # provider. Without an explicit request the new session takes its own
        # default.
        target_model = model if model else ("" if engine_changed else thread.model)
        if not engine_changed and target_model == thread.model:
            return thread
        workspace = {
            **thread.workspace,
            "session_id": "",
            "provider_started": False,
            "session_generation": int(thread.workspace.get("session_generation") or 0) + 1,
        }
        if target_model:
            workspace["model"] = target_model
        else:
            workspace.pop("model", None)
        # Effort and speed are engine-scoped catalogs (claude publishes no speeds
        # at all), so a stale tier must not survive an engine change. The turn's
        # own effort/speed, already validated against the target engine, is
        # re-applied on the turn body once the new session exists.
        tuning: dict[str, str] = {}
        if engine_changed:
            tuning = {"effort": "", "speed": ""}
            workspace.pop("effort", None)
            workspace.pop("speed", None)
        return self._index.save(
            replace(
                thread,
                engine=engine,
                model=target_model,
                updated_at=utc_now(),
                workspace=workspace,
                **tuning,
            )
        )

    async def _ensure_orchestrator_session(
        self,
        project: ProjectEntry,
        thread: CockpitThread,
        requester: RequestContext,
        *,
        progress: Callable[[dict[str, Any]], Any] | None,
    ) -> CockpitThread:
        existing_worker_id = str(thread.workspace.get("worker_id") or "")
        existing_session_id = str(thread.workspace.get("session_id") or "")
        if existing_worker_id and existing_session_id:
            session = await asyncio.to_thread(
                self._get_worker_json,
                existing_worker_id,
                f"/sessions/{existing_session_id}",
            )
            if str(session.get("status") or "") not in FAILED_SESSION_STATUSES:
                return thread
            thread = self._index.save(
                replace(
                    thread,
                    updated_at=utc_now(),
                    workspace={
                        **thread.workspace,
                        "session_id": "",
                        "provider_started": False,
                        "status": "failed",
                        "provision_phase": "failed",
                        "session_generation": int(thread.workspace.get("session_generation") or 0) + 1,
                    },
                )
            )
        await _emit_progress(progress, {"phase": "resolving-access", "thread_id": thread.thread_id})
        preferred = [thread.worker_id] if thread.worker_id else None
        profile = self._registry().choose(preferred=preferred, engine=thread.engine)
        if thread.worker_id and (profile is None or profile.worker_id != thread.worker_id):
            raise RuntimeError(
                f"requested worker {thread.worker_id!r} is not eligible for {thread.engine} orchestrator"
            )
        if profile is None:
            raise RuntimeError(f"no eligible {thread.engine} worker has capacity for the orchestrator")
        if thread.worker_id and (
            profile.status == "offline"
            or not worker_supports_engine(profile.supported_engines, thread.engine)
            or profile.current_jobs + 1 > profile.max_concurrent_jobs
        ):
            raise RuntimeError(
                f"requested worker {thread.worker_id!r} is not eligible for {thread.engine} orchestrator"
            )
        conversation_id = _conversation_workspace_id(project, thread)
        workspace_response = await asyncio.to_thread(
            self._post_worker_json,
            profile.worker_id,
            "/conversation-workspaces",
            {
                "conversation_id": conversation_id,
                "metadata": {
                    "chat_type": "orchestrator",
                    "project_id": project.id,
                    "thread_id": thread.thread_id,
                    "honcho_session_id": thread.session_id,
                    "created_by": requester.memory_peer,
                },
            },
        )
        workspace = dict(workspace_response.get("workspace") or {})
        cwd = str(workspace.get("root") or "")
        if not cwd:
            raise RuntimeError("worker did not create an orchestrator workspace")
        generation = int(thread.workspace.get("session_generation") or 0)
        suffix = f"_{generation}" if generation else ""
        session_id = f"orch_{slugify(thread.thread_id)}{suffix}"
        await asyncio.to_thread(
            self._ensure_worker_session,
            profile.worker_id,
            session_id,
            {
                "session_id": session_id,
                "run_id": thread.thread_id,
                "provider": thread.engine,
                "engine": thread.engine,
                "cwd": cwd,
                "title": thread.title or project.name,
                "metadata": {
                    "execution_envelope": {
                        "run_id": thread.thread_id,
                        "engine": thread.engine,
                        "model": thread.model,
                        "allowed_actions": CONVERSATION_SESSION_ALLOWED_ACTIONS,
                        "landing": dict(CONVERSATION_SESSION_LANDING),
                    },
                    "allowed_actions": CONVERSATION_SESSION_ALLOWED_ACTIONS,
                    "landing": dict(CONVERSATION_SESSION_LANDING),
                    "model": thread.model,
                    # A respawned session starts under the thread's tuning rather
                    # than the engine default; the turn re-asserts it anyway, but
                    # this keeps the very first provider start honest.
                    "effort": thread.effort,
                    "speed": thread.speed,
                    "trusted_mcp_servers": ["jarvis_orchestrator"],
                    "chat_type": "orchestrator",
                    "project_id": project.id,
                    "thread_id": thread.thread_id,
                    "honcho_session_id": thread.session_id,
                },
            },
        )
        return self._index.save(
            replace(
                thread,
                worker_id=profile.worker_id,
                workspace={
                    **thread.workspace,
                    **workspace,
                    "worker_id": profile.worker_id,
                    "session_id": session_id,
                    "provider_started": False,
                    "engine": thread.engine,
                    "model": thread.model,
                    "status": "ready",
                    "provision_phase": "ready",
                },
            )
        )

    async def _wait_for_orchestrator_turn(self, worker_id: str, session_id: str, turn_id: str) -> str:
        deadline = asyncio.get_running_loop().time() + max(1.0, float(self._cfg.worker.job_timeout_s))
        assistant_messages: list[str] = []
        after_event_id = ""
        while asyncio.get_running_loop().time() < deadline:
            event_path = f"/sessions/{session_id}/events?limit=500"
            if after_event_id:
                event_path = f"{event_path}&after={after_event_id}"
            body = await asyncio.to_thread(
                self._get_worker_json,
                worker_id,
                event_path,
            )
            terminal_error = ""
            terminal = False
            events = body.get("events") or []
            for event in events:
                if not isinstance(event, dict):
                    continue
                data = event.get("data") if isinstance(event.get("data"), dict) else {}
                if str(data.get("turn_id") or "") != turn_id:
                    continue
                event_type = str(event.get("type") or "")
                if event_type == EVENT_ASSISTANT_MESSAGE:
                    event_text = str(data.get("text") or "").strip()
                    if event_text:
                        assistant_messages.append(event_text)
                elif event_type == EVENT_TURN_FAILED:
                    terminal = True
                    terminal_error = turn_failure_message(data) or "orchestrator turn failed"
                elif event_type == EVENT_APPROVAL_REQUESTED:
                    # The session is minted to never ask; an approval here means
                    # nobody can answer it, so fail closed instead of hanging
                    # headless until the job timeout.
                    terminal = True
                    terminal_error = (
                        "orchestrator turn requested an approval it cannot receive; "
                        "the session is configured to act without approvals"
                    )
                elif event_type == EVENT_TURN_COMPLETED:
                    terminal = True
            if events and isinstance(events[-1], dict):
                latest_event_id = str(events[-1].get("event_id") or "")
                if latest_event_id:
                    after_event_id = latest_event_id
            if terminal_error:
                raise ProviderTurnError(public_error_message(terminal_error))
            if terminal:
                if not assistant_messages:
                    raise ProviderTurnError("orchestrator turn completed without an assistant result")
                return assistant_messages[-1]
            if len(events) >= 500 and after_event_id:
                continue
            await asyncio.sleep(0.25)
        raise TimeoutError("orchestrator turn timed out")

    def _mark_phase(
        self,
        thread: CockpitThread,
        *,
        phase: str,
        status: str = "provisioning",
        extra: dict[str, Any] | None = None,
    ) -> CockpitThread:
        """Stamp `provision_phase`/`status` onto a thread's workspace and persist it."""
        workspace = {**thread.workspace, "provision_phase": phase, "status": status}
        if extra:
            workspace.update(extra)
        return self._index.save(replace(thread, workspace=workspace))

    async def _provision_repo_worktrees(
        self,
        thread: CockpitThread,
        project: ProjectEntry,
        workspace_request: dict[str, Any],
        worker_id: str,
        conversation_id: str,
        workspace_state: dict[str, Any],
        progress: Callable[[dict[str, Any]], Any] | None,
    ) -> tuple[CockpitThread, dict[str, Any]]:
        repos = _workspace_repos(project, workspace_request)
        for repo in repos:
            thread = self._mark_phase(thread, phase="cloning")
            await _emit_progress(
                progress,
                {"phase": "cloning", "thread_id": thread.thread_id, "repo": repo.get("name") or repo.get("repo")},
            )
            thread = self._mark_phase(thread, phase="creating-worktree")
            await _emit_progress(
                progress,
                {
                    "phase": "creating-worktree",
                    "thread_id": thread.thread_id,
                    "repo": repo.get("name") or repo.get("repo"),
                },
            )
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
        return thread, workspace_state

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
            thread = self._mark_phase(
                thread,
                phase="resolving-access",
                extra={"worker_id": worker_id, "workspace_id": conversation_id},
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
            thread, workspace_state = await self._provision_repo_worktrees(
                thread,
                project,
                workspace_request,
                worker_id,
                conversation_id,
                workspace_state,
                progress,
            )
            generation = int(state.get("session_generation") or 0)
            suffix = f"_{generation}" if generation else ""
            session_id = str(state.get("session_id") or f"conv_{slugify(thread.thread_id)}{suffix}")
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
                            "allowed_actions": CONVERSATION_SESSION_ALLOWED_ACTIONS,
                            "landing": {"mode": "branch_only", "allow_merge": False},
                        },
                        "allowed_actions": CONVERSATION_SESSION_ALLOWED_ACTIONS,
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
            self._mark_phase(
                thread,
                phase="failed",
                status="failed",
                extra={"updated_at": utc_now()},
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
            error = data.get("error")
            if isinstance(error, dict):
                message = str(error.get("message") or error.get("code") or "worker request failed")
                code = str(error.get("code") or data.get("code") or "")
            else:
                message = str(error or getattr(response, "text", "") or "worker request failed")
                code = str(data.get("code") or "")
            raise WorkerRequestError(
                message,
                code=code,
                status_code=int(getattr(response, "status_code", 0) or 0),
            )
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
        persist_turn_messages(
            self._memory,
            session_id,
            requester_peer_id,
            device_id,
            user_text,
            assistant_text,
            assistant_peer_id=self._cfg.memory.assistant_peer_id,
        )

    def _persist_user_turn(
        self,
        session_id: str,
        requester_peer_id: str,
        device_id: str | None,
        user_text: str,
    ) -> None:
        persist_turn_messages(
            self._memory,
            session_id,
            requester_peer_id,
            device_id,
            user_text,
            extra_user_metadata={"workspace_turn": True},
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
    if is_conversation_workspace(thread.workspace):
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
        try:
            repo = _child_work_repo(project, str(args.get("repo") or ""), default=cfg.orchestration.default_repo)
        except ValueError as exc:
            return f"error: {exc}"
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
        # An unknown engine id matched no worker and surfaced as the misleading
        # "No eligible worker found"; name the real problem instead.
        requested_engine = normalize_engine_id(str(args.get("engine") or ""))
        if requested_engine and requested_engine not in BUILTIN_CODE_ENGINES:
            return (
                f"error: unknown engine {requested_engine!r}; "
                f"valid engines are {', '.join(sorted(BUILTIN_CODE_ENGINES))}"
            )
        command = WorkCommand(
            operation="start_next_work",
            source="manual",
            filters={"project_id": project.id},
            target_worker_id=worker_id,
            target_engine_id=requested_engine,
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
        SPAWN_CHILD_WORK_SESSION,
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
                "engine": {
                    "type": "string",
                    "enum": sorted(BUILTIN_CODE_ENGINES),
                    "description": "Optional worker engine route: exactly 'codex' or 'claude'.",
                },
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
        READ_CHILD_WORK_RESULT,
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
        continuation_instruction = str(args.get("continuation_instruction") or "").strip()
        if len(continuation_instruction) > 12_000:
            return "error: continuation_instruction must be 12000 characters or fewer"
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
        ).register_child_watch, thread, child_ids, requester=ctx, continuation_instruction=continuation_instruction)
        await asyncio.to_thread(_start_ready_child_watch, cfg, thread.thread_id)
        return json.dumps(
            {"watch_id": watch_id, "child_chat_ids": sorted(set(child_ids)), "registered": True},
            sort_keys=True,
        )

    return Tool(
        WATCH_CHILD_WORK_SESSIONS,
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
                "continuation_instruction": {
                    "type": "string",
                    "maxLength": 12000,
                    "description": "Optional exact instruction for the automatically resumed parent turn after all children are terminal.",
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
        requester = _context_from_snapshot(requester_snapshot)
        child_ids = [str(item) for item in watch.get("child_chat_ids") or []]
        instruction = (
            "Automatic orchestration continuation: all watched child work sessions are terminal. "
            f"Read each result with read_child_work_result for these child_chat_ids: {', '.join(child_ids)}. "
            "Then continue the original workflow: combine and deduplicate the results, perform any requested "
            "capability-gated external action, and report the outcome. Do not spawn replacement children unless "
            "a result explicitly failed and the original user request requires recovery."
        )
        continuation_instruction = str(watch.get("continuation_instruction") or "").strip()
        if continuation_instruction:
            instruction = f"{instruction}\n\nCompletion instruction from the original workflow:\n{continuation_instruction}"
        connector = CockpitConnector(cfg)
        deadline = asyncio.get_running_loop().time() + max(1.0, float(cfg.worker.job_timeout_s))
        while True:
            if not index.child_watch_is_claimed(parent_chat_id, watch_id):
                return
            try:
                if index.has_queued_turns(thread.project_id, thread.thread_id):
                    await connector.drain_queued_turns(project, thread.thread_id)
                    if index.has_queued_turns(thread.project_id, thread.thread_id):
                        index.renew_child_watch_claim(parent_chat_id, watch_id)
                        await asyncio.sleep(1.0)
                        continue
                await connector.turn(project, thread, requester, instruction)
                break
            except (RuntimeError, OSError, TimeoutError, httpx.TransportError) as exc:
                message = str(exc).lower()
                code = exc.code if isinstance(exc, WorkerRequestError) else ""
                # A worker restart mid-join surfaces as a transport failure;
                # the child results are durable, so keep the claim and retry
                # until the deadline instead of losing the continuation.
                transport = isinstance(exc, (OSError, TimeoutError, httpx.TransportError)) or any(
                    marker in message
                    for marker in ("connection refused", "connection reset", "timed out", "connect error")
                )
                retryable = transport or code in {
                    WORKER_ERROR_SESSION_ACTIVE,
                    WORKER_ERROR_SESSION_TERMINAL,
                } or (
                    not code
                    and (
                        "active turn" in message
                        or "already has an active turn" in message
                        or "does not accept new turns" in message
                    )
                )
                if not retryable or asyncio.get_running_loop().time() >= deadline:
                    raise
                index.renew_child_watch_claim(parent_chat_id, watch_id)
                await asyncio.sleep(2.0 if transport else 1.0)

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
        review_id = int(result.get("review_id") or 0)
        replayed = bool(payload.get("replayed"))
        # A receipt must describe what actually happened. Reporting published
        # for an empty result let an orchestrator truthfully relay a review it
        # never posted. A zero id means no GitHub review exists — including a
        # replayed idempotency result cached from an earlier empty or
        # all-duplicate response, which would otherwise preserve the false
        # success on every retry.
        if review_id <= 0:
            return (
                "error: GitHub returned no review for this publish; nothing was posted. "
                f"skipped_comments={int(result.get('skipped_comments') or 0)}"
            )
        return json.dumps(
            {
                "published": True,
                "review_id": review_id,
                "url": str(result.get("url") or ""),
                "comments": int(result.get("comments") or 0),
                "skipped_comments": int(result.get("skipped_comments") or 0),
                "replayed": replayed,
                "worker_id": worker.worker_id,
            },
            sort_keys=True,
        )

    return Tool(
        PUBLISH_GITHUB_PR_REVIEW,
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
                            "line": {
                                "type": "integer",
                                "minimum": 1,
                                "description": "1-based line number in the file at commit_id, not a gh diff output position.",
                            },
                            "line_kind": {
                                "type": "string",
                                "enum": ["FILE", "GLOBAL_DIFF_POSITION"],
                                "description": "Declare FILE after verifying the 1-based file line. Use GLOBAL_DIFF_POSITION only for an ordinal gh pr diff output line.",
                            },
                            "side": {
                                "type": "string",
                                "enum": ["LEFT", "RIGHT"],
                                "description": "RIGHT for the PR-head file line; LEFT for the base-file line.",
                            },
                            "severity": {"type": "string", "enum": ["P1", "P2", "P3"]},
                            "title": {"type": "string"},
                            "body": {"type": "string"},
                            "suggestion": {"type": "string", "description": "Optional exact replacement for a GitHub suggestion block."},
                        },
                        "required": ["path", "line", "line_kind", "severity", "title", "body"],
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


async def execute_orchestrator_tool(
    cfg: Config,
    *,
    project: ProjectEntry,
    thread: CockpitThread,
    requester: RequestContext,
    tool_name: str,
    args: dict[str, Any],
) -> str:
    """Execute one parent-thread tool after the API validates its scoped grant."""
    tools = ToolRegistry()
    tools.register(_spawn_child_work_tool(cfg, project, thread))
    tools.register(_read_child_work_result_tool(cfg, project, thread))
    tools.register(_watch_child_work_sessions_tool(cfg, project, thread))
    tools.register(_publish_github_pr_review_tool(cfg, project))
    return await tools.execute(
        requester,
        tool_name,
        args,
        timeout_s=max(float(cfg.tools.timeout_s), float(cfg.worker.request_timeout_s) + 5),
    )


def _orchestrator_mcp_timeout_s(cfg: Config) -> float:
    server_timeout = max(
        float(cfg.tools.timeout_s),
        float(cfg.worker.request_timeout_s) + 5.0,
        30.0,
    )
    return server_timeout + 5.0


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


def _child_work_repo(project: ProjectEntry, requested: str, *, default: str = "") -> str:
    """Resolve a child work repo request to a worker-usable remote.

    Orchestrators reference repos by their registry alias (the name shown in
    the workspace projection) or echo another worker's absolute checkout path;
    workers only understand remotes, so both must map through the registry.
    A path that maps to no registry repo is rejected rather than dispatched —
    it is meaningless (or worse, someone else's checkout) on another worker.
    """
    repo = requested.strip() or _project_default_repo(project) or default
    registry_match = next((entry for entry in project.repos if repo in (entry.name, entry.remote)), None)
    if registry_match is not None and registry_match.remote:
        return registry_match.remote
    if repo.startswith(("/", "~")):
        basename = PurePosixPath(repo).name
        basename_match = next(
            (
                entry
                for entry in project.repos
                if entry.remote and (entry.name == basename or entry.remote.rstrip("/").rsplit("/", 1)[-1].removesuffix(".git") == basename)
            ),
            None,
        )
        if basename_match is not None:
            return basename_match.remote
        raise ValueError(
            f"child work repo {repo!r} is a worker-local path with no matching project repository; "
            "use a registry repo name or org/name remote"
        )
    return repo


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
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            logger.error(
                "cockpit cold task failed",
                exc_info=(type(result), result, result.__traceback__),
            )


def schedule_cold_task_drain(session: BrainSession) -> None:
    task = asyncio.create_task(_drain_cold_tasks(session))

    def report_failure(completed: asyncio.Task) -> None:
        try:
            completed.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("cockpit cold task drain failed")

    task.add_done_callback(report_failure)


async def _emit_progress(
    progress: Callable[[dict[str, Any]], Any] | None,
    payload: dict[str, Any],
) -> None:
    if progress is None:
        return
    emitted = progress(payload)
    if inspect.isawaitable(emitted):
        await emitted


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


def _scrub_turn_receipt_recovery_fields(receipt: dict[str, Any]) -> None:
    for key in ("text", "requester", "workspace_request", "has_attachments"):
        receipt.pop(key, None)


def _thread_turn_fingerprint(
    text: str,
    workspace_request: dict[str, Any] | None,
    attachments: list[dict[str, Any]] | None,
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "text": text,
                "workspace_request": workspace_request,
                "attachments": attachments or [],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


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
            "title",
            "phase",
            "status",
            "terminal_reason",
            "watch_id",
            "continuation_instruction",
            "claimed_at",
            "completed_at",
            "error",
            "turn_idempotency_key",
        ):
            if key in item:
                message[key] = str(item.get(key) or "")
        if isinstance(item.get("child_chat_ids"), list):
            message["child_chat_ids"] = [str(value) for value in item["child_chat_ids"] if str(value)]
        requester = item.get("requester")
        if isinstance(requester, dict):
            message["requester"] = _snapshot_from_context(_context_from_snapshot(requester))
        event = item.get("event")
        if isinstance(event, dict):
            message["event"] = dict(event)
        normalized.append(message)
    return normalized


def _merge_thread_message(messages: list[dict[str, Any]], message: dict[str, Any]) -> None:
    """Apply append-only child-watch state records without changing projections."""

    if message.get("type") == "child_watch" and message.get("watch_id"):
        for index, existing in enumerate(messages):
            if existing.get("type") == "child_watch" and existing.get("watch_id") == message["watch_id"]:
                messages[index] = message
                return
    messages.append(message)


def _safe_path_segment(value: str) -> str:
    segment = "".join(char if char.isalnum() or char in {"-", "_", "."} else "_" for char in value)
    return segment or "unknown"


def _atomic_write_jsonl(path: Path, messages: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for message in _normalized_messages(messages):
                handle.write(json.dumps(message, sort_keys=True) + "\n")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def is_conversation_workspace(workspace: dict[str, Any]) -> bool:
    """True when a workspace carries real worker-session state, not just bookkeeping."""
    return any(key not in STORE_OWNED_WORKSPACE_KEYS for key in workspace)


def _workspace_is_ready(thread: CockpitThread) -> bool:
    workspace = thread.workspace or {}
    return (
        bool(workspace.get("worker_id"))
        and bool(workspace.get("session_id"))
        and str(workspace.get("status") or "") in {"ready", "starting"}
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
