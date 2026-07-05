"""Revocable bearer-token store for the Jarvis MCP server."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from jarvis.ids import new_id, utc_now


class MCPTokenError(ValueError):
    """Token-store operation failed."""


@dataclass(frozen=True)
class MCPTokenRecord:
    token_id: str
    principal: str
    name: str
    token_hash: str
    token_prefix: str
    created_at: str
    revoked_at: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MCPTokenRecord":
        return cls(
            token_id=str(data.get("token_id") or data.get("id") or ""),
            principal=str(data.get("principal") or ""),
            name=str(data.get("name") or ""),
            token_hash=str(data.get("token_hash") or ""),
            token_prefix=str(data.get("token_prefix") or ""),
            created_at=str(data.get("created_at") or ""),
            revoked_at=str(data.get("revoked_at") or ""),
        )

    def as_dict(self) -> dict[str, str]:
        return {
            "token_id": self.token_id,
            "principal": self.principal,
            "name": self.name,
            "token_hash": self.token_hash,
            "token_prefix": self.token_prefix,
            "created_at": self.created_at,
            "revoked_at": self.revoked_at,
        }

    @property
    def revoked(self) -> bool:
        return bool(self.revoked_at)


class MCPTokenStore:
    """Small JSON token store with atomic writes.

    Plain bearer tokens are shown once on creation. The store keeps only a
    SHA-256 hash plus a short prefix for operator listing.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()

    def add(self, *, principal: str, name: str = "") -> tuple[str, MCPTokenRecord]:
        principal = principal.strip()
        if not principal:
            raise MCPTokenError("principal is required")
        token = "jv_mcp_" + secrets.token_urlsafe(32)
        record = MCPTokenRecord(
            token_id=new_id("mcptok"),
            principal=principal,
            name=name.strip(),
            token_hash=_hash_token(token),
            token_prefix=token[:16],
            created_at=utc_now(),
        )
        data = self._read()
        data.setdefault("tokens", []).append(record.as_dict())
        self._write(data)
        return token, record

    def list(self, *, include_revoked: bool = False) -> list[MCPTokenRecord]:
        records = [MCPTokenRecord.from_dict(item) for item in self._read().get("tokens", [])]
        records = [record for record in records if record.token_id and record.principal]
        if not include_revoked:
            records = [record for record in records if not record.revoked]
        return sorted(records, key=lambda record: record.created_at)

    def resolve(self, token: str) -> MCPTokenRecord | None:
        digest = _hash_token(token.strip())
        if not digest:
            return None
        for record in self.list(include_revoked=True):
            if not record.revoked and secrets.compare_digest(record.token_hash, digest):
                return record
        return None

    def revoke(self, token_id_or_prefix: str) -> MCPTokenRecord:
        needle = token_id_or_prefix.strip()
        if not needle:
            raise MCPTokenError("token id is required")
        data = self._read()
        records = [MCPTokenRecord.from_dict(item) for item in data.get("tokens", [])]
        matches = [
            record
            for record in records
            if record.token_id == needle or record.token_id.startswith(needle)
        ]
        if not matches:
            raise MCPTokenError(f"token {needle!r} not found")
        if len(matches) > 1:
            raise MCPTokenError(f"token id prefix {needle!r} is ambiguous")
        target = matches[0]
        revoked = MCPTokenRecord(
            token_id=target.token_id,
            principal=target.principal,
            name=target.name,
            token_hash=target.token_hash,
            token_prefix=target.token_prefix,
            created_at=target.created_at,
            revoked_at=target.revoked_at or utc_now(),
        )
        data["tokens"] = [
            revoked.as_dict() if record.token_id == target.token_id else record.as_dict()
            for record in records
        ]
        self._write(data)
        return revoked

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"version": 1, "tokens": []}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            raise MCPTokenError(f"token store {self.path} is not readable: {exc}") from exc
        if not isinstance(data, dict):
            raise MCPTokenError(f"token store {self.path} must contain a JSON object")
        tokens = data.get("tokens")
        if not isinstance(tokens, list):
            data["tokens"] = []
        data.setdefault("version", 1)
        return data

    def _write(self, data: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=self.path.parent,
            prefix=f".{self.path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            tmp = Path(handle.name)
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.path)
        try:
            self.path.chmod(0o600)
        except OSError:
            pass
        try:
            dir_fd = os.open(self.path.parent, os.O_DIRECTORY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest() if token else ""
