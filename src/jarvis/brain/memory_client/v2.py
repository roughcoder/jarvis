"""Honcho v2 memory backend adapter."""

from __future__ import annotations

import asyncio
import json
import pathlib
import time
from pathlib import Path
from typing import Any

from jarvis.brain.memory_client.encoding import cache_key
from jarvis.brain.memory_client.interface import (
    ConclusionLevel,
    ConclusionRecord,
    MemoryMessage,
    PeerRecord,
    QueueStatus,
    RepresentationRecord,
    SessionPeer,
    SessionRecord,
)
from jarvis.config import MemoryConfig


_SESSION_ID = "voice"
_MEMORY_QUERY = (
    "Summarise everything important you know about the user — their name, "
    "preferences, and any facts or ongoing context — in a few concise sentences. "
    "If you know nothing about them yet, reply with an empty string."
)


class UnsupportedMemoryOperation(NotImplementedError):
    """Raised when the active backend cannot provide a v3-only memory surface."""


class HonchoV2MemoryClient:
    def __init__(self, cfg: MemoryConfig) -> None:
        self._cfg = cfg
        self._ws = cfg.workspace_id
        self._ensured: set[str] = set()
        self._last_refresh: dict[str, float] = {}

    def _ws_url(self) -> str:
        return f"{self._cfg.base_url}/v2/workspaces/{self._ws}"

    def _headers(self) -> dict[str, str]:
        key = self._cfg.api_key.get_secret_value()
        return {"Authorization": f"Bearer {key}"} if key else {}

    def _peer(self, user: str | None) -> str:
        return user or self._cfg.user_peer_id

    def _session_id(self, user: str | None) -> str:
        peer = self._peer(user)
        return _SESSION_ID if peer == self._cfg.user_peer_id else f"{_SESSION_ID}-{peer}"

    def _cache_path(self, user: str | None) -> pathlib.Path:
        base = pathlib.Path(self._cfg.cache_path)
        peer = self._peer(user)
        if peer == self._cfg.user_peer_id:
            return base
        return base.with_name(f"{base.stem}-{cache_key(peer)}{base.suffix}")

    def _ensure(self, client: Any, user: str | None) -> None:
        peer = self._peer(user)
        if peer in self._ensured:
            return
        client.post(f"{self._cfg.base_url}/v2/workspaces", json={"id": self._ws})
        for p in (peer, self._cfg.assistant_peer_id):
            client.post(f"{self._ws_url()}/peers", json={"id": p})
        client.post(f"{self._ws_url()}/sessions", json={"id": self._session_id(user)})
        self._ensured.add(peer)

    def read_cached_representation(self, user: str | None = None) -> str:
        path = self._cache_path(user)
        if not path.exists():
            return ""
        try:
            return json.loads(path.read_text(encoding="utf-8")).get("representation", "")
        except (json.JSONDecodeError, OSError):
            return ""

    def _write_turn_sync(self, user_text: str, assistant_text: str, user: str | None = None) -> None:
        import httpx

        with httpx.Client(timeout=self._cfg.write_timeout_s, headers=self._headers()) as c:
            self._ensure(c, user)
            r = c.post(
                f"{self._ws_url()}/sessions/{self._session_id(user)}/messages/",
                json={
                    "messages": [
                        {"content": user_text, "peer_id": self._peer(user)},
                        {"content": assistant_text, "peer_id": self._cfg.assistant_peer_id},
                    ]
                },
            )
            r.raise_for_status()

    def _refresh_cache_sync(self, min_interval_s: float = 0.0, user: str | None = None) -> str | None:
        import httpx

        if min_interval_s > 0 and self.read_cached_representation(user):
            if (time.monotonic() - self._last_refresh.get(self._peer(user), 0.0)) < min_interval_s:
                return None
        with httpx.Client(timeout=self._cfg.write_timeout_s, headers=self._headers()) as c:
            self._ensure(c, user)
            r = c.post(
                f"{self._ws_url()}/peers/{self._peer(user)}/chat",
                json={"queries": _MEMORY_QUERY, "session_id": self._session_id(user)},
            )
            r.raise_for_status()
            text = (r.json() or {}).get("content", "").strip()
        path = self._cache_path(user)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"representation": text}), encoding="utf-8")
        self._last_refresh[self._peer(user)] = time.monotonic()
        return text

    async def write_turn(self, user_text: str, assistant_text: str, *, user: str | None = None) -> None:
        await asyncio.to_thread(self._write_turn_sync, user_text, assistant_text, user)

    async def refresh_cache(self, min_interval_s: float = 0.0, *, user: str | None = None) -> bool:
        result = await asyncio.to_thread(self._refresh_cache_sync, min_interval_s, user)
        return result is not None

    def deriver_idle(self) -> bool:
        return True

    def ping(self) -> bool:
        import httpx

        try:
            r = httpx.get(f"{self._cfg.base_url}/openapi.json", timeout=5.0)
            return r.status_code == 200
        except httpx.HTTPError:
            return False

    def get_or_create_peer(
        self,
        peer_id: str,
        *,
        observe_me: bool | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PeerRecord:
        raise UnsupportedMemoryOperation("Honcho v2 does not support the peer interface")

    def get_peer_card(self, peer_id: str, *, target: str | None = None) -> tuple[str, ...]:
        raise UnsupportedMemoryOperation("Honcho v2 does not support peer cards")

    def set_peer_card(self, peer_id: str, card: list[str], *, target: str | None = None) -> tuple[str, ...]:
        raise UnsupportedMemoryOperation("Honcho v2 does not support peer cards")

    def create_session(
        self,
        session_id: str,
        *,
        peers: list[SessionPeer] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SessionRecord:
        raise UnsupportedMemoryOperation("Honcho v2 does not support explicit session membership")

    def delete_session(self, session_id: str) -> None:
        raise UnsupportedMemoryOperation("Honcho v2 does not support session deletion")

    def create_messages(self, session_id: str, messages: list[MemoryMessage]) -> list[dict[str, Any]]:
        raise UnsupportedMemoryOperation("Honcho v2 messages use write_turn only")

    def create_conclusion(
        self,
        *,
        observed_id: str,
        content: str,
        observer_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ConclusionRecord:
        raise UnsupportedMemoryOperation("Honcho v2 does not support explicit conclusions")

    def list_conclusions(
        self,
        *,
        observed_id: str | None = None,
        observer_id: str | None = None,
        session_id: str | None = None,
        level: ConclusionLevel | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[ConclusionRecord]:
        raise UnsupportedMemoryOperation("Honcho v2 does not support explicit conclusions")

    def query_conclusions(
        self,
        query: str,
        *,
        observed_id: str | None = None,
        observer_id: str | None = None,
        session_id: str | None = None,
        level: ConclusionLevel | None = None,
        metadata: dict[str, Any] | None = None,
        limit: int | None = None,
    ) -> list[ConclusionRecord]:
        raise UnsupportedMemoryOperation("Honcho v2 does not support explicit conclusions")

    def delete_conclusion(self, conclusion_id: str) -> None:
        raise UnsupportedMemoryOperation("Honcho v2 does not support explicit conclusions")

    def read_representation(
        self,
        peer_id: str,
        *,
        session_id: str | None = None,
        search_query: str | None = None,
        search_top_k: int | None = None,
        target: str | None = None,
        max_conclusions: int | None = None,
    ) -> RepresentationRecord:
        raise UnsupportedMemoryOperation("Honcho v2 live representation reads are not exposed")

    def dialectic_chat(
        self,
        peer_id: str,
        query: str,
        *,
        session_id: str | None = None,
        target: str | None = None,
        reasoning_level: str = "low",
    ) -> str:
        raise UnsupportedMemoryOperation("Honcho v2 live dialectic reads are not exposed")

    def queue_status(self) -> QueueStatus:
        raise UnsupportedMemoryOperation("Honcho v2 queue status is not exposed")

    def upload_file(
        self,
        session_id: str,
        *,
        peer_id: str,
        path: Path,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        raise UnsupportedMemoryOperation("Honcho v2 does not support file uploads")
