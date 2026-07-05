"""Brain-owned project management operations.

REST and MCP boundary peers authenticate callers and relay requests here. The
brain remains the sole registry writer and owns file-vault writes.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import mimetypes
import re
import urllib.request
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any

import websockets

from jarvis.brain.capabilities import (
    RequestContext,
    can_admin_project,
    can_create_project,
    can_edit_project,
)
from jarvis.brain.memory_client import MemoryBackend, UnsupportedMemoryOperation
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

MEMBER_UPDATE_FIELDS = {"name", "aliases", "status", "links", "files_root", "repos"}
OWNER_ONLY_FIELDS = {"owner", "members", "visibility"}
PROJECT_STATUSES = {"active", "paused", "archived"}
PROJECT_VISIBILITIES = {"household", "private", "shared"}


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
        if op in {"project.file.upload", "project.file.retract"}:
            async with self._lock:
                return await self._execute_file(ctx, op, payload)
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

    async def _execute_file(self, ctx: RequestContext, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        project = self.registry.get_project(_project_id(payload))
        if project is None:
            raise ProjectOperationError("not_found", "project not found", status=404)
        _require_member(ctx, project)
        if op == "project.file.retract":
            doc_id = _doc_id(str(payload.get("doc_id") or ""))
            if not doc_id:
                raise ProjectOperationError("validation_failed", "doc_id is required", status=400, recoverable=True)
            session_id = upload_session_id(project.id, doc_id)
            try:
                await asyncio.to_thread(self.memory.delete_session, session_id)
            except UnsupportedMemoryOperation as exc:
                raise ProjectOperationError("memory_unavailable", str(exc), status=503, recoverable=True) from exc
            return {"project_id": project.id, "doc_id": doc_id, "session_id": session_id, "retracted": True}

        upload = await asyncio.to_thread(self._materialize_upload, ctx, project, payload)
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
        except UnsupportedMemoryOperation as exc:
            ingestion = {"queued": False, "error": str(exc), "recoverable": True}
        return {"project_id": project.id, **upload, "ingestion": ingestion}

    def _materialize_upload(
        self,
        ctx: RequestContext,
        project: ProjectEntry,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        filename, data = _upload_bytes(payload)
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
        return {
            "doc_id": doc_id,
            "session_id": session_id,
            "content_hash": content_hash,
            "original_path": str(original_path),
            "metadata": metadata,
        }


class BrainProjectClient:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    async def execute(self, ctx: RequestContext, op: str, payload: dict[str, Any]) -> dict[str, Any]:
        request_id = "projop-" + uuid.uuid4().hex[:12]
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
                        token=self.cfg.intercom.token.get_secret_value(),
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
            raw = decode(await asyncio.wait_for(ws.recv(), self.cfg.tools.timeout_s + 10.0))
        if not isinstance(raw, ProjectOperationResponse) or raw.request_id != request_id:
            raise ProjectOperationError("protocol_error", "unexpected brain response", status=502)
        if not raw.ok:
            err = raw.error or {}
            raise ProjectOperationError(
                str(err.get("code") or "brain_error"),
                str(err.get("message") or "brain project operation failed"),
                status=int(err.get("status") or 500),
                recoverable=bool(err.get("recoverable", False)),
            )
        return raw.result


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
        files_root=str(payload.get("files_root") or f"projects/{project_id}/files").strip(),
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
        changes["status"] = status
    if "links" in payload:
        changes["links"] = _links(payload["links"])
    if "files_root" in payload:
        changes["files_root"] = str(payload["files_root"] or "").strip()
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


def _upload_bytes(payload: dict[str, Any]) -> tuple[str, bytes]:
    filename = str(payload.get("filename") or payload.get("path") or "upload.txt").split("/")[-1] or "upload.txt"
    if "content_text" in payload:
        return filename, str(payload.get("content_text") or "").encode("utf-8")
    if "content_base64" in payload:
        return filename, base64.b64decode(str(payload.get("content_base64") or ""), validate=True)
    source_path = str(payload.get("source_path") or "").strip()
    if source_path:
        path = Path(source_path).expanduser()
        return path.name, path.read_bytes()
    source_url = str(payload.get("source_url") or "").strip()
    if source_url:
        with urllib.request.urlopen(source_url, timeout=30) as response:  # noqa: S310 - explicit user-provided upload source.
            data = response.read(25 * 1024 * 1024)
        return filename or Path(source_url).name or "upload", data
    raise ProjectOperationError("validation_failed", "upload content, path, or URL is required", status=400, recoverable=True)


def _doc_id(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9._-]+", "-", value).strip("-._")
    return value[:96]


def _files_root(cfg: Config, project: ProjectEntry) -> Path:
    raw = project.files_root or f"projects/{project.id}/files"
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path
    registry_root = Path(cfg.registry.path).expanduser().parent.parent
    if path.parts and path.parts[0] == registry_root.name:
        return registry_root.parent / path
    return registry_root / path
