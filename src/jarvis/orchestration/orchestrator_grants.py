"""Short-lived, thread-scoped grants for code-agent orchestrator tools."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import stat
import time
from dataclasses import dataclass
from pathlib import Path

from jarvis.config import OrchestrationConfig
from jarvis.orchestrator_tool_contract import ORCHESTRATOR_TOOL_NAME_SET
from jarvis.runtime import RequestContext


GRANT_TTL_SECONDS = 2 * 60 * 60
_PREFIX = "jv_orch_"
_SIGNING_CONTEXT = b"jarvis-orchestrator-tool-grant-v1"
_SIGNING_KEY_FILENAME = ".orchestrator-grant-signing-key"


class OrchestratorGrantError(ValueError):
    """A code-agent orchestrator grant is missing, invalid, or expired."""


@dataclass(frozen=True)
class OrchestratorGrant:
    project_id: str
    thread_id: str
    requester: RequestContext
    tools: frozenset[str]
    expires_at: int


def mint_orchestrator_grant(
    cfg: OrchestrationConfig,
    *,
    project_id: str,
    thread_id: str,
    requester: RequestContext,
    now: int | None = None,
) -> str:
    current = int(time.time()) if now is None else int(now)
    payload = {
        "v": 1,
        "project_id": project_id,
        "thread_id": thread_id,
        "requester": {
            "device_id": requester.device_id,
            "identity": requester.identity,
            "scope": requester.scope,
            "capabilities": sorted(requester.capabilities),
            "channel": requester.channel,
            "confidence": requester.confidence,
            "peer": requester.peer,
        },
        "tools": sorted(ORCHESTRATOR_TOOL_NAME_SET),
        "iat": current,
        "exp": current + GRANT_TTL_SECONDS,
    }
    encoded = _b64(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signature = _sign(_signing_key(cfg), encoded.encode("ascii"))
    return f"{_PREFIX}{encoded}.{signature}"


def resolve_orchestrator_grant(
    cfg: OrchestrationConfig,
    token: str,
    *,
    now: int | None = None,
) -> OrchestratorGrant:
    value = str(token or "").strip()
    if not value.startswith(_PREFIX) or "." not in value:
        raise OrchestratorGrantError("invalid orchestrator grant")
    encoded, supplied_signature = value[len(_PREFIX) :].rsplit(".", 1)
    expected_signature = _sign(_signing_key(cfg), encoded.encode("ascii"))
    if not hmac.compare_digest(supplied_signature, expected_signature):
        raise OrchestratorGrantError("invalid orchestrator grant")
    try:
        payload = json.loads(_unb64(encoded))
    except (ValueError, json.JSONDecodeError) as exc:
        raise OrchestratorGrantError("invalid orchestrator grant") from exc
    if not isinstance(payload, dict) or payload.get("v") != 1:
        raise OrchestratorGrantError("invalid orchestrator grant")
    expires_at = int(payload.get("exp") or 0)
    current = int(time.time()) if now is None else int(now)
    if expires_at <= current:
        raise OrchestratorGrantError("orchestrator grant expired")
    requester = payload.get("requester")
    if not isinstance(requester, dict):
        raise OrchestratorGrantError("invalid orchestrator grant")
    project_id = str(payload.get("project_id") or "").strip()
    thread_id = str(payload.get("thread_id") or "").strip()
    tools = frozenset(
        str(item)
        for item in payload.get("tools") or []
        if str(item) in ORCHESTRATOR_TOOL_NAME_SET
    )
    if not project_id or not thread_id or not tools:
        raise OrchestratorGrantError("invalid orchestrator grant")
    return OrchestratorGrant(
        project_id=project_id,
        thread_id=thread_id,
        requester=RequestContext(
            device_id=str(requester.get("device_id") or ""),
            identity=str(requester.get("identity") or ""),
            scope=str(requester.get("scope") or "personal"),
            capabilities=frozenset(
                str(item)
                for item in requester.get("capabilities") or []
                if str(item).strip()
            ),
            channel=str(requester.get("channel") or "cockpit"),
            confidence=str(requester.get("confidence") or "strong"),
            peer=str(requester.get("peer") or ""),
        ),
        tools=tools,
        expires_at=expires_at,
    )


def orchestrator_api_base_url(cfg: OrchestrationConfig) -> str:
    host = str(cfg.api_host or "").strip()
    if not host or host in {"0.0.0.0", "::"}:
        raise OrchestratorGrantError(
            "ORCHESTRATION_API_HOST must be a worker-reachable brain address for code-agent orchestrators"
        )
    display_host = f"[{host}]" if ":" in host and not host.startswith("[") else host
    return f"http://{display_host}:{int(cfg.api_port)}"


def _signing_key(cfg: OrchestrationConfig) -> bytes:
    secret = cfg.grant_signing_secret.get_secret_value().strip()
    if not secret:
        secret = cfg.api_token.get_secret_value().strip()
    if not secret:
        secret = _persistent_local_signing_secret(cfg)
    return hmac.new(secret.encode("utf-8"), _SIGNING_CONTEXT, hashlib.sha256).digest()


def _persistent_local_signing_secret(cfg: OrchestrationConfig) -> str:
    root = Path(cfg.workspace).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    path = root / _SIGNING_KEY_FILENAME
    temp_path = root / f"{_SIGNING_KEY_FILENAME}.{secrets.token_hex(8)}.tmp"
    try:
        fd = os.open(temp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except OSError as exc:
        raise OrchestratorGrantError("unable to create orchestrator grant signing key") from exc
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(secrets.token_urlsafe(48))
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp_path, path, follow_symlinks=False)
        except FileExistsError:
            pass
        except OSError as exc:
            raise OrchestratorGrantError("unable to install orchestrator grant signing key") from exc
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise OrchestratorGrantError("unable to read orchestrator grant signing key") from exc
    with os.fdopen(fd, "r", encoding="utf-8") as handle:
        file_stat = os.fstat(handle.fileno())
        if not stat.S_ISREG(file_stat.st_mode):
            raise OrchestratorGrantError("orchestrator grant signing key is not a regular file")
        os.fchmod(handle.fileno(), 0o600)
        secret = handle.read().strip()
    if not secret:
        raise OrchestratorGrantError("orchestrator grant signing key is empty")
    return secret


def _sign(key: bytes, payload: bytes) -> str:
    return _b64(
        hmac.new(key, _SIGNING_CONTEXT + b"\0" + payload, hashlib.sha256).digest()
    )


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
