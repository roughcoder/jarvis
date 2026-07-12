"""Public MCP status snapshots shared by the brain and cockpit API."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jarvis.config import Config
from jarvis.ids import utc_now
from jarvis.mcp.bridge import MCPBridge
from jarvis.mcp_server.tokens import MCPTokenError, MCPTokenRecord, MCPTokenStore
from jarvis.oauth import (
    auth_mode,
    oauth_endpoint_urls_are_secure,
    protected_resource_metadata_url,
)
from jarvis.redaction import public_error_message
from jarvis.storage import atomic_write_json

logger = logging.getLogger(__name__)

MCP_STATUS_FILENAME = "mcp-status.json"
MCP_TOKENS_MANAGE_CAPABILITY = "mcp.tokens.manage"
MCP_STATUS_STALE_AFTER_S = 60 * 60


def mcp_status_path(cfg: Config) -> Path:
    return Path(cfg.orchestration.workspace) / MCP_STATUS_FILENAME


def publish_mcp_status_snapshot(cfg: Config, bridge: MCPBridge) -> None:
    """Best-effort cold-path sidecar write for the separate cockpit API process."""
    try:
        snapshot = bridge.status()
        snapshot["generated_at"] = utc_now()
        atomic_write_json(mcp_status_path(cfg), snapshot)
    except Exception as exc:  # noqa: BLE001 - status visibility must not break brain startup
        logger.warning("mcp status snapshot write failed: %s", public_error_message(str(exc)))


def cockpit_mcp_status(cfg: Config) -> dict[str, Any]:
    snapshot = _read_snapshot(cfg)
    source = "snapshot" if snapshot is not None else "config"
    servers = _snapshot_servers(snapshot) if snapshot is not None else _config_servers(cfg)
    serve = _serve_contract(cfg)
    return {
        "api_version": "v1",
        "schema_version": 1,
        "enabled": bool(cfg.mcp.enabled),
        "source": source,
        "generated_at": str((snapshot or {}).get("generated_at") or ""),
        "stale": _snapshot_stale(snapshot),
        "servers": servers,
        "serve": serve,
    }


def cockpit_mcp_tools(cfg: Config, *, server: str = "") -> dict[str, Any]:
    snapshot = _read_snapshot(cfg)
    tools = _snapshot_tools(snapshot) if snapshot is not None else []
    if server:
        tools = [tool for tool in tools if tool.get("server") == server]
    return {
        "api_version": "v1",
        "schema_version": 1,
        "source": "snapshot" if snapshot is not None else "config",
        "generated_at": str((snapshot or {}).get("generated_at") or ""),
        "stale": _snapshot_stale(snapshot),
        "tools": tools,
    }


def token_record_public(record: MCPTokenRecord) -> dict[str, str]:
    return {
        "token_id": record.token_id,
        "principal": record.principal,
        "name": record.name,
        "prefix": record.token_prefix,
        "created_at": record.created_at,
        "revoked_at": record.revoked_at,
    }


def _read_snapshot(cfg: Config) -> dict[str, Any] | None:
    path = mcp_status_path(cfg)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("mcp status snapshot read failed: %s", public_error_message(str(exc)))
        return None
    return data if isinstance(data, dict) else None


def _config_servers(cfg: Config) -> list[dict[str, Any]]:
    return [
        {
            "name": spec.name,
            "transport": spec.transport,
            "connected": None,
            "tool_count": 0,
            "error": "",
            "connected_at": None,
            "required_capability": spec.required_capability,
        }
        for spec in cfg.mcp.servers
    ]


def _snapshot_servers(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    servers = snapshot.get("servers")
    if not isinstance(servers, list):
        return []
    result = []
    for item in servers:
        if not isinstance(item, dict):
            continue
        connected = item.get("connected")
        result.append(
            {
                "name": str(item.get("name") or ""),
                "transport": str(item.get("transport") or ""),
                "connected": connected if isinstance(connected, bool) else None,
                "tool_count": int(item.get("tool_count") or 0),
                "error": public_error_message(str(item.get("error") or "")),
                "connected_at": str(item.get("connected_at") or ""),
                "required_capability": str(item.get("required_capability") or ""),
            }
        )
    return result


def _snapshot_tools(snapshot: dict[str, Any] | None) -> list[dict[str, str]]:
    if snapshot is None:
        return []
    tools = snapshot.get("tools")
    if not isinstance(tools, list):
        return []
    result = []
    for item in tools:
        if not isinstance(item, dict):
            continue
        result.append(
            {
                "name": str(item.get("offered_name") or item.get("name") or ""),
                "server": str(item.get("server") or ""),
                "description": public_error_message(str(item.get("description") or "")),
                "required_capability": str(item.get("required_capability") or ""),
            }
        )
    return result


def _serve_contract(cfg: Config) -> dict[str, Any]:
    tokens = {"active": 0, "revoked": 0}
    token_store_path = Path(cfg.mcp_serve.token_store_path).expanduser()
    try:
        records = MCPTokenStore(token_store_path).list(include_revoked=True)
        tokens["active"] = sum(1 for record in records if not record.revoked)
        tokens["revoked"] = sum(1 for record in records if record.revoked)
    except MCPTokenError as exc:
        logger.warning("mcp token count failed: %s", public_error_message(str(exc)))
    codex_wired, reason = _codex_wired()
    body = {
        "configured": token_store_path.exists(),
        "host": cfg.mcp_serve.host,
        "port": int(cfg.mcp_serve.port),
        "auth_mode": auth_mode(str(cfg.mcp_serve.auth_mode)),
        "oauth": _serve_oauth_contract(cfg),
        "tokens": tokens,
        "codex_wired": codex_wired,
    }
    if reason:
        body["codex_wired_reason"] = reason
    return body


def _serve_oauth_contract(cfg: Config) -> dict[str, Any]:
    mode = auth_mode(str(cfg.mcp_serve.auth_mode))
    issuer = str(cfg.mcp_serve.oauth_issuer).strip()
    jwks_url = str(cfg.mcp_serve.oauth_jwks_url).strip()
    resource = cfg.mcp_serve.resolved_resource_url
    configured = bool(
        mode in {"oauth", "hybrid"}
        and issuer
        and jwks_url
        and resource
        and oauth_endpoint_urls_are_secure(issuer=issuer, jwks_url=jwks_url)
    )
    return {
        "configured": configured,
        "issuer": issuer,
        "resource": resource,
        "metadata_url": protected_resource_metadata_url(resource),
    }


def _codex_wired() -> tuple[bool, str]:
    return (
        False,
        "worker Codex sessions do not currently inject the Jarvis MCP serve endpoint",
    )


def _snapshot_stale(snapshot: dict[str, Any] | None) -> bool:
    if snapshot is None:
        return True
    generated_at = str(snapshot.get("generated_at") or "")
    if not generated_at:
        return True
    try:
        generated = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError:
        return True
    if generated.tzinfo is None:
        generated = generated.replace(tzinfo=timezone.utc)
    age_s = (datetime.now(timezone.utc) - generated.astimezone(timezone.utc)).total_seconds()
    return age_s > MCP_STATUS_STALE_AFTER_S
