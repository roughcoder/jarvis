"""Brain-owned project management operations.

REST and MCP boundary peers authenticate callers and relay requests here. The
brain remains the sole registry writer and owns file-vault writes.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import http.client
import ipaddress
import json
import logging
import mimetypes
import re
import socket
import ssl
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path
from typing import Any

import websockets

from jarvis.brain._storage import atomic_write_json
from jarvis.brain.capabilities import (
    RequestContext,
    can_admin_project,
    can_create_project,
    can_edit_project,
)
from jarvis.brain.identity import load_users
from jarvis.brain.memory_client import MemoryBackend, UnsupportedMemoryOperation
from jarvis.brain.memory_outbox import CurationOutbox
from jarvis.brain.memory_tools import make_memory_tools
from jarvis.brain.registry import ProjectEntry, ProjectLinks, RegistryConflict, RegistryError, RegistryStore, RepoEntry
from jarvis.config import Config
from jarvis.ids import utc_now
from jarvis.protocol.messages import (
    Hello,
    ProjectOperationRequest,
    ProjectOperationResponse,
    Reject,
    Welcome,
    decode,
    encode,
)
from jarvis.runtime import CapabilityError, ToolRegistry

MEMBER_UPDATE_FIELDS = {"name", "aliases", "status", "links", "files_root", "repos"}
OWNER_ONLY_FIELDS = {"owner", "members", "visibility"}
PROJECT_STATUSES = {"active", "paused", "archived"}
PROJECT_VISIBILITIES = {"household", "private", "shared"}
_REDIRECT_STATUSES = {301, 302, 303, 307, 308}
# The manifest is per-project and small, but the composer's @-picker types
# server-side so it never has to hold the whole list — cap what a query returns.
FILE_QUERY_LIMIT = 20
logger = logging.getLogger(__name__)


class ProjectOperationError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        status: int = 400,
        recoverable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.recoverable = recoverable

    def body(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": str(self),
            "status": self.status,
            "recoverable": self.recoverable,
        }


class ProjectOperationService:
    def __init__(
        self,
        cfg: Config,
        *,
        registry: RegistryStore,
        memory: MemoryBackend,
        lock: asyncio.Lock | None = None,
    ) -> None:
        self.cfg = cfg
        self.registry = registry
        self.memory = memory
        self._lock = lock or asyncio.Lock()

    async def execute(self, ctx: RequestContext, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        if op == "project.file.upload":
            return await self._execute_file_upload(ctx, payload)
        if op == "project.file.retract":
            return await self._execute_file_retract(ctx, payload)
        if op == "project.file.list":
            async with self._lock:
                return self._execute_file_list(ctx, payload)
        if op in {"project.memory.forget", "project.memory.correct"}:
            async with self._lock:
                project = self._member_project(ctx, payload)
            return await self._execute_memory(ctx, op, payload, project)
        async with self._lock:
            try:
                return self._execute_registry(ctx, op, payload)
            except RegistryConflict as exc:
                raise ProjectOperationError("conflict", str(exc), status=409, recoverable=True) from exc
            except RegistryError as exc:
                raise ProjectOperationError("validation_failed", str(exc), status=400, recoverable=True) from exc

    def _execute_registry(self, ctx: RequestContext, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        if op == "project.create":
            decision = can_create_project(ctx)
            if not decision.allowed:
                raise ProjectOperationError("unauthorized", decision.reason, status=401)
            project = self.registry.create_project(_project_from_create_payload(ctx, payload))
            return {"project": project.as_dict()}

        project_id = _project_id(payload)
        project = self.registry.get_project(project_id)
        if project is None:
            raise ProjectOperationError("not_found", "project not found", status=404)

        if op in {"project.update", "project.repos.set"}:
            _require_member(ctx, project)
            if op == "project.update":
                updated = _updated_project(project, payload)
            else:
                updated = replace(project, repos=_repos(payload.get("repos", ())))
            return {"project": self.registry.update_project(updated).as_dict()}

        if op in {"project.members.set", "project.visibility.set", "project.archive", "project.delete"}:
            _require_owner(ctx, project)
            if op == "project.members.set":
                updated = replace(project, members=_strings(payload.get("members", ())))
                return {"project": self.registry.update_project(updated).as_dict()}
            if op == "project.visibility.set":
                visibility = str(payload.get("visibility") or "").strip()
                if visibility not in PROJECT_VISIBILITIES:
                    raise RegistryError(f"invalid project visibility: {visibility}")
                return {"project": self.registry.update_project(replace(project, visibility=visibility)).as_dict()}
            if op == "project.archive":
                archived = bool(payload.get("archived", True))
                return {
                    "project": self.registry.update_project(
                        replace(project, status="archived" if archived else "active")
                    ).as_dict()
                }
            self.registry.delete_project(project.id)
            return {"deleted": True, "project_id": project.id}

        raise ProjectOperationError("validation_failed", f"unsupported project operation: {op}", status=400)

    def _member_project(self, ctx: RequestContext, payload: dict[str, Any]) -> ProjectEntry:
        project = self.registry.get_project(_project_id(payload))
        if project is None:
            raise ProjectOperationError("not_found", "project not found", status=404)
        _require_member(ctx, project)
        return project

    async def _execute_file_upload(self, ctx: RequestContext, payload: dict[str, Any]) -> dict[str, Any]:
        async with self._lock:
            project = self._member_project(ctx, payload)
        filename, data = await asyncio.to_thread(_upload_bytes, self.cfg, payload)
        async with self._lock:
            project = self._member_project(ctx, payload)
            upload = self._materialize_upload(ctx, project, payload, filename, data)

        ingestion: dict[str, Any] = {"queued": False}
        try:
            raw = await asyncio.to_thread(
                self.memory.upload_file,
                upload["session_id"],
                peer_id=project.peer_id,
                path=Path(upload["original_path"]),
                metadata=upload["metadata"],
            )
            ingestion = {"queued": True, "response": raw}
        except UnsupportedMemoryOperation:
            logger.warning("project file ingestion is unsupported", exc_info=True)
            ingestion = _ingestion_failed()
        except Exception:  # noqa: BLE001 - vault write succeeded; report recoverable ingestion failure.
            logger.warning("project file ingestion failed", exc_info=True)
            ingestion = _ingestion_failed()
        async with self._lock:
            file_entry = self._update_manifest_ingestion(project.id, upload["doc_id"], ingestion)
        return {"project_id": project.id, **upload, "ingestion": ingestion, "file": file_entry}

    async def _execute_file_retract(self, ctx: RequestContext, payload: dict[str, Any]) -> dict[str, Any]:
        async with self._lock:
            project = self._member_project(ctx, payload)
            doc_id = _doc_id(str(payload.get("doc_id") or ""))
            if not doc_id:
                raise ProjectOperationError("validation_failed", "doc_id is required", status=400, recoverable=True)
            existing = self._manifest_entry(project.id, doc_id)
            session_id = str(existing.get("session_id") or upload_session_id(project.id, doc_id))
        try:
            await asyncio.to_thread(self.memory.delete_session, session_id)
        except UnsupportedMemoryOperation as exc:
            raise ProjectOperationError("memory_unavailable", str(exc), status=503, recoverable=True) from exc
        async with self._lock:
            file_entry = self._mark_manifest_retracted(project.id, doc_id)
        return {
            "project_id": project.id,
            "doc_id": doc_id,
            "session_id": session_id,
            "retracted": True,
            "file": file_entry,
        }

    def _execute_file_list(self, ctx: RequestContext, payload: dict[str, Any]) -> dict[str, Any]:
        project = self._member_project(ctx, payload)
        query = str(payload.get("query") or "").strip()
        files = project_file_rows(
            self.cfg,
            project.id,
            include_retracted=bool(payload.get("include_retracted", False)),
            query=query,
            limit=FILE_QUERY_LIMIT if query else 0,
        )
        return {"project_id": project.id, "files": files, "query": query}

    async def _execute_memory(
        self,
        ctx: RequestContext,
        op: str,
        payload: dict[str, Any],
        project: ProjectEntry,
    ) -> dict[str, Any]:
        tool_name = "forget_memory" if op == "project.memory.forget" else "correct_memory"
        outbox = CurationOutbox(
            self.cfg.memory.curation_outbox_path,
            max_retries=self.cfg.memory.curation_outbox_max_retries,
            backoff_initial_s=self.cfg.memory.curation_outbox_backoff_initial_s,
            backoff_max_s=self.cfg.memory.curation_outbox_backoff_max_s,
        )
        tools = ToolRegistry()
        users = load_users(self.cfg.capabilities.users_dir)
        for tool in make_memory_tools(
            self.cfg.memory,
            memory=self.memory,
            outbox=outbox,
            registry=self.registry,
            users=users,
        ):
            tools.register(tool)
        args = {
            **payload,
            "target": project.peer_id,
            "source": str(payload.get("source") or ctx.channel or "brain"),
            "channel": str(payload.get("channel") or ctx.channel or "brain"),
        }
        try:
            result = await tools.execute(ctx, tool_name, args, timeout_s=self.cfg.tools.timeout_s)
        except CapabilityError as exc:
            raise ProjectOperationError("forbidden", str(exc), status=403) from exc
        if result.startswith("error:"):
            raise ProjectOperationError(
                "validation_failed",
                result.removeprefix("error:").strip(),
                status=400,
                recoverable=True,
            )
        return {"project_id": project.id, "result": result}

    def _materialize_upload(
        self,
        ctx: RequestContext,
        project: ProjectEntry,
        payload: dict[str, Any],
        filename: str,
        data: bytes,
    ) -> dict[str, Any]:
        content_hash = "sha256:" + hashlib.sha256(data).hexdigest()
        doc_id = _doc_id(str(payload.get("doc_id") or "")) or _doc_id(f"{Path(filename).stem}-{content_hash[-12:]}")
        root = _files_root(self.cfg, project)
        root.mkdir(parents=True, exist_ok=True)
        suffix = Path(filename).suffix
        stored_name = f"{doc_id}{suffix}" if suffix and not doc_id.endswith(suffix.lower()) else doc_id
        original_path = root / stored_name
        original_path.write_bytes(data)
        title = str(payload.get("title") or Path(filename).stem or doc_id).strip()
        artifact_type = str(payload.get("artifact_type") or "spec").strip() or "spec"
        metadata = {
            "project_id": project.id,
            "artifact_type": artifact_type,
            "title": title,
            "uploaded_by": ctx.memory_peer,
            "source": "file",
            "channel": str(payload.get("channel") or ctx.channel or "brain"),
            "content_hash": content_hash,
            "original_path": str(original_path),
            "mime_type": str(payload.get("mime_type") or mimetypes.guess_type(filename)[0] or ""),
            "observed_at": str(payload.get("observed_at") or utc_now()),
        }
        agent = str(payload.get("agent") or "").strip()
        if agent:
            metadata["agent"] = agent
        session_id = upload_session_id(project.id, doc_id)
        manifest_entry = {
            "doc_id": doc_id,
            "title": title,
            "session_id": session_id,
            "original_path": str(original_path),
            "content_hash": content_hash,
            "artifact_type": artifact_type,
            "uploaded_by": ctx.memory_peer,
            "observed_at": metadata["observed_at"],
            "mime_type": metadata["mime_type"],
            "channel": metadata["channel"],
            "retracted": False,
            "retracted_at": "",
            "ingestion": {"queued": False},
        }
        if agent:
            manifest_entry["agent"] = agent
        _upsert_manifest_entry(self.cfg, project.id, manifest_entry)
        return {
            "doc_id": doc_id,
            "session_id": session_id,
            "content_hash": content_hash,
            "original_path": str(original_path),
            "metadata": metadata,
        }

    def _manifest_entry(self, project_id: str, doc_id: str) -> dict[str, Any]:
        for entry in _manifest_entries(self.cfg, project_id):
            if entry.get("doc_id") == doc_id:
                return entry
        return {}

    def _update_manifest_ingestion(
        self,
        project_id: str,
        doc_id: str,
        ingestion: dict[str, Any],
    ) -> dict[str, Any]:
        entry = self._manifest_entry(project_id, doc_id)
        if not entry:
            return {}
        entry["ingestion"] = ingestion
        _upsert_manifest_entry(self.cfg, project_id, entry)
        return entry

    def _mark_manifest_retracted(self, project_id: str, doc_id: str) -> dict[str, Any]:
        entry = self._manifest_entry(project_id, doc_id)
        if not entry:
            entry = {
                "doc_id": doc_id,
                "title": doc_id,
                "session_id": upload_session_id(project_id, doc_id),
                "original_path": "",
                "content_hash": "",
                "artifact_type": "",
                "uploaded_by": "",
                "observed_at": "",
                "ingestion": {},
            }
        entry["retracted"] = True
        entry["retracted_at"] = utc_now()
        _upsert_manifest_entry(self.cfg, project_id, entry)
        return entry


class BrainProjectClient:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    async def execute(self, ctx: RequestContext, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        request_id = "projop-" + uuid.uuid4().hex[:12]
        peer_token = self.cfg.brain.peer_token.get_secret_value()
        if not peer_token:
            raise ProjectOperationError(
                "unauthorized",
                "BRAIN_PEER_TOKEN is not configured for project operations",
                status=503,
                recoverable=True,
            )
        async with websockets.connect(
            self.cfg.intercom.brain_url,
            open_timeout=self.cfg.intercom.websocket_open_timeout_s,
            close_timeout=self.cfg.intercom.websocket_close_timeout_s,
            max_size=self.cfg.intercom.websocket_max_size,
            ping_interval=self.cfg.intercom.websocket_ping_interval_s,
            ping_timeout=self.cfg.intercom.websocket_ping_timeout_s,
        ) as ws:
            await ws.send(
                encode(
                    Hello(
                        device_id=self.cfg.capabilities.device_id,
                        token=peer_token,
                        identity=ctx.identity,
                        channel=ctx.channel or "cockpit",
                    )
                )
            )
            first = decode(await asyncio.wait_for(ws.recv(), self.cfg.intercom.websocket_open_timeout_s))
            if isinstance(first, Reject):
                raise ProjectOperationError("unauthorized", first.reason, status=401)
            if not isinstance(first, Welcome):
                raise ProjectOperationError("protocol_error", "brain did not accept project client", status=502)
            await ws.send(
                encode(
                    ProjectOperationRequest(
                        request_id=request_id,
                        op=op,  # type: ignore[arg-type]
                        requester=request_context_to_dict(ctx),
                        payload=payload,
                    )
                )
            )
            raw = await self._read_response(ws, request_id)
        if not raw.ok:
            err = raw.error or {}
            raise ProjectOperationError(
                str(err.get("code") or "brain_error"),
                str(err.get("message") or "brain project operation failed"),
                status=int(err.get("status") or 500),
                recoverable=bool(err.get("recoverable", False)),
            )
        return raw.result

    async def _read_response(self, ws, request_id: str) -> ProjectOperationResponse:  # noqa: ANN001
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.cfg.tools.timeout_s + 10.0
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise ProjectOperationError("protocol_error", "timed out waiting for brain project response", status=502)
            frame = await asyncio.wait_for(ws.recv(), remaining)
            if isinstance(frame, bytes):
                continue
            raw = decode(frame)
            if isinstance(raw, ProjectOperationResponse) and raw.request_id == request_id:
                return raw


def request_context_to_dict(ctx: RequestContext) -> dict[str, Any]:
    return {
        "device_id": ctx.device_id,
        "identity": ctx.identity,
        "scope": ctx.scope,
        "capabilities": sorted(ctx.capabilities),
        "channel": ctx.channel,
        "confidence": ctx.confidence,
        "peer": ctx.peer,
    }


def request_context_from_dict(data: dict[str, Any]) -> RequestContext:
    return RequestContext(
        device_id=str(data.get("device_id") or ""),
        identity=str(data.get("identity") or ""),
        scope=str(data.get("scope") or "personal"),
        capabilities=frozenset(str(item) for item in data.get("capabilities") or ()),
        channel=str(data.get("channel") or "cockpit"),
        confidence=str(data.get("confidence") or "strong"),
        peer=str(data.get("peer") or ""),
    )


def upload_session_id(project_id: str, doc_id: str) -> str:
    return f"project:{project_id}:uploads:{doc_id}"


def _ingestion_failed() -> dict[str, Any]:
    return {
        "queued": False,
        "code": "ingestion_failed",
        "error": "file ingestion failed",
        "recoverable": True,
    }


def _project_from_create_payload(ctx: RequestContext, payload: dict[str, Any]) -> ProjectEntry:
    project_id = _project_id(payload)
    members = _strings(payload.get("members", ()))
    return ProjectEntry(
        id=project_id,
        name=str(payload.get("name") or project_id).strip(),
        aliases=_strings(payload.get("aliases", ())),
        owner=ctx.identity,
        members=(ctx.identity, *members),
        visibility=str(payload.get("visibility") or "household"),
        status=str(payload.get("status") or "active"),
        repos=_repos(payload.get("repos", ())),
        links=_links(payload.get("links")),
        files_root=_validate_files_root(payload.get("files_root"), project_id),
    )


def _updated_project(project: ProjectEntry, payload: dict[str, Any]) -> ProjectEntry:
    forbidden = OWNER_ONLY_FIELDS & set(payload)
    if forbidden:
        raise RegistryError("owner-only project fields must use their explicit routes: " + ", ".join(sorted(forbidden)))
    unknown = set(payload) - MEMBER_UPDATE_FIELDS - {"project_id", "id"}
    if unknown:
        raise RegistryError("unsupported project fields: " + ", ".join(sorted(unknown)))
    changes: dict[str, Any] = {}
    if "name" in payload:
        changes["name"] = str(payload["name"]).strip()
    if "aliases" in payload:
        changes["aliases"] = _strings(payload["aliases"])
    if "status" in payload:
        status = str(payload["status"] or "").strip()
        if status not in {"active", "paused"}:
            raise RegistryError("project status edits may only set active or paused")
        if project.status == "archived":
            raise RegistryError("archived projects must be unarchived through the owner route")
        changes["status"] = status
    if "links" in payload:
        changes["links"] = _links(payload["links"])
    if "files_root" in payload:
        changes["files_root"] = _validate_files_root(payload["files_root"], project.id)
    if "repos" in payload:
        changes["repos"] = _repos(payload["repos"])
    return replace(project, **changes)


def _require_member(ctx: RequestContext, project: ProjectEntry) -> None:
    if not (ctx.identity == project.owner or ctx.identity in project.members):
        raise ProjectOperationError("not_found", "project not found", status=404)
    decision = can_edit_project(ctx, project)
    if not decision.allowed:
        raise ProjectOperationError("not_found", "project not found", status=404)


def _require_owner(ctx: RequestContext, project: ProjectEntry) -> None:
    if not (ctx.identity == project.owner or ctx.identity in project.members):
        raise ProjectOperationError("not_found", "project not found", status=404)
    decision = can_admin_project(ctx, project)
    if not decision.allowed:
        raise ProjectOperationError("forbidden", decision.reason, status=403)


def _project_id(payload: dict[str, Any]) -> str:
    project_id = str(payload.get("project_id") or payload.get("id") or "").strip()
    if not project_id:
        raise ProjectOperationError("validation_failed", "project_id is required", status=400, recoverable=True)
    return project_id


def _links(value: Any) -> ProjectLinks:
    if isinstance(value, ProjectLinks):
        return value
    if value is None:
        return ProjectLinks()
    if not isinstance(value, dict):
        raise RegistryError("links must be an object")
    return ProjectLinks.from_dict(value)


def _repos(value: Any) -> tuple[RepoEntry, ...]:
    if not isinstance(value, (list, tuple)):
        raise RegistryError("repos must be an array")
    return tuple(item if isinstance(item, RepoEntry) else RepoEntry.from_dict(dict(item)) for item in value)


def _strings(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value.strip(),) if value.strip() else ()
    return tuple(str(item).strip() for item in (value or ()) if str(item).strip())


def _upload_bytes(cfg: Config, payload: dict[str, Any]) -> tuple[str, bytes]:
    filename = str(payload.get("filename") or payload.get("path") or "upload.txt").split("/")[-1] or "upload.txt"
    max_bytes = max(1, int(cfg.registry.max_upload_bytes))
    if "content_text" in payload:
        data = str(payload.get("content_text") or "").encode("utf-8")
        _ensure_upload_size(data, max_bytes)
        return filename, data
    if "content_base64" in payload:
        raw = str(payload.get("content_base64") or "")
        if len(raw) > ((max_bytes + 2) // 3) * 4 + 4:
            raise ProjectOperationError("validation_failed", "upload exceeds max size", status=413, recoverable=True)
        data = base64.b64decode(raw, validate=True)
        _ensure_upload_size(data, max_bytes)
        return filename, data
    source_path = str(payload.get("source_path") or "").strip()
    if source_path:
        path = _confined_source_path(cfg, source_path)
        if path.stat().st_size > max_bytes:
            raise ProjectOperationError("validation_failed", "upload exceeds max size", status=413, recoverable=True)
        return path.name, path.read_bytes()
    source_url = str(payload.get("source_url") or "").strip()
    if source_url:
        return filename or Path(urllib.parse.urlparse(source_url).path).name or "upload", _read_url_upload(cfg, source_url)
    raise ProjectOperationError("validation_failed", "upload content, path, or URL is required", status=400, recoverable=True)


def _doc_id(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9._-]+", "-", value).strip("-._")
    return value[:96]


def _files_root(cfg: Config, project: ProjectEntry) -> Path:
    raw = _validate_files_root(project.files_root, project.id)
    base = Path(cfg.registry.files_vault_root).expanduser().resolve(strict=False)
    path = (base / raw).resolve(strict=False)
    try:
        path.relative_to(base)
    except ValueError as exc:
        raise ProjectOperationError("validation_failed", "files_root escapes vault root", status=400, recoverable=True) from exc
    return path


def _validate_files_root(value: Any, project_id: str) -> str:
    raw = str(value or f"projects/{project_id}/files").strip()
    path = Path(raw)
    if path.is_absolute() or ".." in path.parts:
        raise RegistryError("files_root must be a relative path inside the project vault")
    return path.as_posix()


def _ensure_upload_size(data: bytes, max_bytes: int) -> None:
    if len(data) > max_bytes:
        raise ProjectOperationError("validation_failed", "upload exceeds max size", status=413, recoverable=True)


def _confined_source_path(cfg: Config, source_path: str) -> Path:
    root = Path(cfg.registry.upload_staging_root).expanduser().resolve(strict=False)
    path = Path(source_path).expanduser().resolve(strict=True)
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ProjectOperationError(
            "validation_failed",
            "source_path must be inside the configured upload staging root",
            status=400,
            recoverable=True,
        ) from exc
    return path


@dataclass(frozen=True)
class _ResolvedUploadURL:
    url: str
    parsed: urllib.parse.ParseResult
    address: str
    port: int


def _read_url_upload(cfg: Config, source_url: str) -> bytes:
    max_bytes = max(1, int(cfg.registry.max_upload_bytes))
    url = source_url
    for _ in range(max(0, int(cfg.registry.upload_url_max_redirects)) + 1):
        resolved = _resolve_upload_url(url)
        _reject_dns_rebind(resolved)
        response = _open_pinned_upload_url(resolved)
        try:
            if response.status in _REDIRECT_STATUSES:
                location = response.getheader("Location")
                if not location:
                    raise ProjectOperationError("validation_failed", "upload URL redirect missing location", status=400)
                url = urllib.parse.urljoin(url, location)
                continue
            if response.status >= 400:
                raise ProjectOperationError(
                    "validation_failed",
                    f"upload URL failed with HTTP {response.status}",
                    status=400,
                    recoverable=True,
                )
            length = response.getheader("Content-Length")
            if length and int(length) > max_bytes:
                raise ProjectOperationError(
                    "validation_failed",
                    "upload exceeds max size",
                    status=413,
                    recoverable=True,
                )
            chunks: list[bytes] = []
            total = 0
            while True:
                remaining = max_bytes + 1 - total
                if remaining <= 0:
                    raise ProjectOperationError(
                        "validation_failed",
                        "upload exceeds max size",
                        status=413,
                        recoverable=True,
                    )
                chunk = response.read(min(65536, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > max_bytes:
                    raise ProjectOperationError(
                        "validation_failed",
                        "upload exceeds max size",
                        status=413,
                        recoverable=True,
                    )
            return b"".join(chunks)
        finally:
            response.close()
    raise ProjectOperationError("validation_failed", "upload URL redirected too many times", status=400, recoverable=True)


def _resolve_upload_url(url: str) -> _ResolvedUploadURL:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ProjectOperationError("validation_failed", "upload URL must use http or https", status=400, recoverable=True)
    if not parsed.hostname:
        raise ProjectOperationError("validation_failed", "upload URL host is required", status=400, recoverable=True)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(parsed.hostname, port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ProjectOperationError("validation_failed", "upload URL host could not be resolved", status=400, recoverable=True) from exc
    for info in infos:
        address = info[4][0]
        if not _blocked_upload_address(address):
            return _ResolvedUploadURL(url=url, parsed=parsed, address=address, port=port)
    raise ProjectOperationError("validation_failed", "upload URL host is not allowed", status=400, recoverable=True)


def _reject_dns_rebind(resolved: _ResolvedUploadURL) -> None:
    try:
        infos = socket.getaddrinfo(resolved.parsed.hostname, resolved.port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ProjectOperationError("validation_failed", "upload URL host could not be resolved", status=400, recoverable=True) from exc
    if any(_blocked_upload_address(info[4][0]) for info in infos):
        raise ProjectOperationError("validation_failed", "upload URL host is not allowed", status=400, recoverable=True)


def _blocked_upload_address(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def _open_pinned_upload_url(resolved: _ResolvedUploadURL) -> http.client.HTTPResponse:
    connection_cls: type[http.client.HTTPConnection]
    if resolved.parsed.scheme == "https":
        connection_cls = _PinnedHTTPSConnection
    else:
        connection_cls = _PinnedHTTPConnection
    conn = connection_cls(
        resolved.address,
        resolved.port,
        timeout=30,
        original_host=resolved.parsed.hostname or "",
    )
    path = urllib.parse.urlunparse(
        ("", "", resolved.parsed.path or "/", resolved.parsed.params, resolved.parsed.query, "")
    )
    conn.request(
        "GET",
        path,
        headers={
            "Host": _host_header(resolved.parsed),
            "User-Agent": "jarvis-project-upload/1.0",
        },
    )
    return conn.getresponse()


class _PinnedHTTPConnection(http.client.HTTPConnection):
    def __init__(self, host: str, port: int, *, timeout: float, original_host: str) -> None:
        super().__init__(host, port=port, timeout=timeout)
        self._original_host = original_host


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    def __init__(self, host: str, port: int, *, timeout: float, original_host: str) -> None:
        super().__init__(host, port=port, timeout=timeout, context=ssl.create_default_context())
        self._original_host = original_host

    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), self.timeout, self.source_address)
        if self._tunnel_host:
            self._tunnel()
        self.sock = self._context.wrap_socket(self.sock, server_hostname=self._original_host)


def _host_header(parsed: urllib.parse.ParseResult) -> str:
    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    default_port = 443 if parsed.scheme == "https" else 80
    if parsed.port and parsed.port != default_port:
        return f"{host}:{parsed.port}"
    return host


def _manifest_path(cfg: Config) -> Path:
    return Path(cfg.registry.upload_manifest_path).expanduser()


def _load_manifest(cfg: Config) -> dict[str, Any]:
    path = _manifest_path(cfg)
    if not path.exists():
        return {"version": 1, "projects": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ProjectOperationError("validation_failed", f"invalid upload manifest: {path}", status=500) from exc
    if not isinstance(data, dict):
        return {"version": 1, "projects": {}}
    data.setdefault("version", 1)
    data.setdefault("projects", {})
    return data


def _save_manifest(cfg: Config, data: dict[str, Any]) -> None:
    # Shared helper also fsyncs the containing directory, hardening durability
    # beyond what this hand-rolled version did.
    atomic_write_json(_manifest_path(cfg), data)


def _manifest_entries(cfg: Config, project_id: str) -> list[dict[str, Any]]:
    data = _load_manifest(cfg)
    projects = data.get("projects") if isinstance(data.get("projects"), dict) else {}
    entries = projects.get(project_id) if isinstance(projects.get(project_id), list) else []
    return [dict(entry) for entry in entries if isinstance(entry, dict)]


def stored_filename(row: dict[str, Any]) -> str:
    """The file's name as stored in the vault — the mentionable `@<filename>`.

    Derived from `original_path` rather than persisted, so rows written before
    the field existed still carry it and it can never drift from what the
    mention resolver matches against.
    """
    return Path(str(row.get("original_path") or "")).name


def project_file_rows(
    cfg: Config,
    project_id: str,
    *,
    include_retracted: bool = False,
    query: str = "",
    limit: int = 0,
) -> list[dict[str, Any]]:
    """Manifest rows for a project, optionally name-filtered and ranked.

    The single read path behind both `project.file.list` and @-mention
    resolution, so the picker and the resolver can never disagree about which
    files exist. Each row gains a derived `filename` — the handle a composer
    needs, without making callers parse a brain-host path.
    """
    rows = [
        {**entry, "filename": stored_filename(entry)}
        for entry in _manifest_entries(cfg, project_id)
        if include_retracted or not entry.get("retracted")
    ]
    needle = query.strip().lower()
    if needle:
        ranked = [(rank, row) for row in rows if (rank := _file_query_rank(row, needle)) is not None]
        ranked.sort(key=lambda item: (item[0], str(item[1].get("observed_at") or "")))
        rows = [row for _, row in ranked]
    if limit > 0:
        rows = rows[:limit]
    return rows


def _file_query_rank(row: dict[str, Any], needle: str) -> int | None:
    """Rank a row against a lowercased needle; None when it does not match.

    Prefix matches on the stored filename outrank prefix matches on doc_id or
    title, which in turn outrank plain substring hits.
    """
    stored_name = stored_filename(row).lower()
    doc_id = str(row.get("doc_id") or "").lower()
    title = str(row.get("title") or "").lower()
    for rank, candidates in ((0, (stored_name,)), (1, (doc_id, title))):
        if any(value.startswith(needle) for value in candidates if value):
            return rank
    if any(needle in value for value in (stored_name, doc_id, title) if value):
        return 2
    return None


def _upsert_manifest_entry(cfg: Config, project_id: str, entry: dict[str, Any]) -> None:
    data = _load_manifest(cfg)
    projects = data.setdefault("projects", {})
    entries = [dict(item) for item in projects.get(project_id, []) if isinstance(item, dict)]
    doc_id = str(entry.get("doc_id") or "")
    updated = False
    for index, existing in enumerate(entries):
        if existing.get("doc_id") == doc_id:
            entries[index] = dict(entry)
            updated = True
            break
    if not updated:
        entries.append(dict(entry))
    projects[project_id] = sorted(entries, key=lambda item: str(item.get("observed_at") or ""))
    _save_manifest(cfg, data)
