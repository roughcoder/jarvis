"""Honcho v3 HTTP memory backend."""

from __future__ import annotations

import asyncio
import json
import pathlib
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from jarvis.brain.memory_client.encoding import (
    assert_honcho_safe,
    cache_key,
    decode_honcho_id,
    encode_honcho_id,
)
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
from jarvis.brain.memory_client.sidecar import ConclusionMetadataSidecar
from jarvis.config import MemoryConfig


_SESSION_ID = "voice"
_CONCLUSION_LEVELS = {"explicit", "deductive", "inductive", "contradiction"}
# Shape proven end-to-end by deploy/honcho-v3/validate.py (step 1). Verify
# against the dev stack before cutover whether workspace-level env defaults
# make this redundant; sending it explicitly is safe either way.
_SESSION_CONFIGURATION = {
    "summary": {
        "enabled": True,
        "messages_per_short_summary": 10,
        "messages_per_long_summary": 20,
    },
    "reasoning": {"enabled": True},
    "dream": {"enabled": True},
}
_MEMORY_QUERY = (
    "Summarise everything important you know about the user — their name, "
    "preferences, and any facts or ongoing context — in a few concise sentences. "
    "If you know nothing about them yet, reply with an empty string."
)


def _turn_metadata(*, channel: str, device_id: str | None) -> dict[str, str]:
    metadata = {"channel": (channel or "voice").strip() or "voice"}
    if device_id:
        metadata["device_id"] = device_id
    return metadata


class HonchoV3MemoryClient:
    def __init__(self, cfg: MemoryConfig, *, transport: httpx.BaseTransport | None = None) -> None:
        self._cfg = cfg
        self._ws = cfg.workspace_id
        self._encoded_ws = assert_honcho_safe(encode_honcho_id(cfg.workspace_id))
        self._ensured_workspace = False
        self._ensured_peers: set[str] = set()
        self._ensured_sessions: set[str] = set()
        self._last_refresh: dict[str, float] = {}
        self._transport = transport
        self._sidecar = ConclusionMetadataSidecar(cfg.conclusion_sidecar_path)

    def _client(self) -> httpx.Client:
        kwargs: dict[str, Any] = {"timeout": self._cfg.write_timeout_s, "headers": self._headers()}
        if self._transport is not None:
            kwargs["transport"] = self._transport
        return httpx.Client(**kwargs)

    def _url(self, path: str) -> str:
        return f"{self._cfg.base_url}{path}"

    def _ws_path(self, suffix: str = "") -> str:
        return f"/v3/workspaces/{self._q(self._encoded_ws)}{suffix}"

    def _headers(self) -> dict[str, str]:
        key = self._cfg.api_key.get_secret_value()
        return {"Authorization": f"Bearer {key}"} if key else {}

    def _q(self, value: str) -> str:
        return quote(value, safe="")

    def _peer(self, user: str | None) -> str:
        return user or self._cfg.user_peer_id

    def _session_id(self, user: str | None) -> str:
        peer = self._peer(user)
        return _SESSION_ID if peer == self._cfg.user_peer_id else f"{_SESSION_ID}:{peer}"

    def _cache_path(self, user: str | None) -> pathlib.Path:
        base = pathlib.Path(self._cfg.cache_path)
        peer = self._peer(user)
        if peer == self._cfg.user_peer_id:
            return base
        return base.with_name(f"{base.stem}-{cache_key(peer)}{base.suffix}")

    def _encoded_peer(self, peer_id: str) -> str:
        return assert_honcho_safe(encode_honcho_id(peer_id))

    def _encoded_session(self, session_id: str) -> str:
        return assert_honcho_safe(encode_honcho_id(session_id))

    def _decode_conclusion(self, item: dict[str, Any]) -> ConclusionRecord:
        cid = str(item.get("id", ""))
        metadata = dict(item.get("metadata") or {})
        observer_id = decode_honcho_id(str(item.get("observer_id", item.get("observer", ""))))
        observed_id = decode_honcho_id(str(item.get("observed_id", item.get("observed", ""))))
        sidecar_metadata = self._sidecar.get(self._ws, cid)
        if not sidecar_metadata and cid:
            sidecar_metadata = self._sidecar.materialize_pending(
                self._ws,
                cid,
                observer_id=observer_id,
                observed_id=observed_id,
                content=str(item.get("content", "")),
            )
        metadata.update(sidecar_metadata)
        return ConclusionRecord(
            id=cid,
            content=str(item.get("content", "")),
            observer_id=observer_id,
            observed_id=observed_id,
            level=str(item.get("level") or metadata.get("level") or "explicit"),  # type: ignore[arg-type]
            session_id=decode_honcho_id(str(item["session_id"])) if item.get("session_id") else None,
            metadata=metadata,
        )

    def _normalise_conclusion_metadata(self, metadata: dict[str, Any] | None) -> dict[str, Any]:
        normalised = dict(metadata or {})
        level = str(normalised.get("level") or "explicit")
        if level not in _CONCLUSION_LEVELS:
            raise ValueError(f"unsupported conclusion level: {level!r}")
        if level == "explicit" and not normalised.get("observed_at"):
            raise ValueError("explicit conclusions require metadata['observed_at']")
        normalised["level"] = level
        return normalised

    def _conclusion_items_from_response(self, data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            items = data.get("items", data.get("conclusions", []))
            if isinstance(items, list):
                return [item for item in items if isinstance(item, dict)]
        return []

    def _next_conclusion_cursor(self, data: Any) -> tuple[Any | None, bool]:
        if not isinstance(data, dict):
            return None, True
        pagination = data.get("pagination") if isinstance(data.get("pagination"), dict) else {}
        cursor = (
            data.get("next_cursor")
            or data.get("next_page_token")
            or data.get("next")
            or pagination.get("next_cursor")
            or pagination.get("next_page_token")
            or pagination.get("next")
        )
        has_more = bool(data.get("has_more") or data.get("has_next") or pagination.get("has_more"))
        return cursor, not has_more or cursor is not None

    def _list_conclusion_items(
        self,
        client: httpx.Client,
        payload: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], bool]:
        items: list[dict[str, Any]] = []
        next_cursor: Any | None = None
        known_complete = True
        while True:
            page_payload = dict(payload)
            if next_cursor is not None:
                page_payload["cursor"] = next_cursor
            response = client.post(self._url(self._ws_path("/conclusions/list")), json=page_payload)
            response.raise_for_status()
            data = response.json() or {}
            items.extend(self._conclusion_items_from_response(data))
            next_cursor, page_complete = self._next_conclusion_cursor(data)
            known_complete = known_complete and page_complete
            if next_cursor is None:
                return items, known_complete

    def _find_exact_conclusion(
        self,
        client: httpx.Client,
        *,
        observer_id: str,
        observed_id: str,
        content: str,
        level: ConclusionLevel,
    ) -> ConclusionRecord | None:
        payload = {
            "filters": {
                "observer_id": self._encoded_peer(observer_id),
                "observed_id": self._encoded_peer(observed_id),
                "level": level,
            }
        }
        items, _known_complete = self._list_conclusion_items(client, payload)
        records = [self._decode_conclusion(item) for item in items]
        for record in records:
            if (
                record.observer_id == observer_id
                and record.observed_id == observed_id
                and record.content == content
                and record.level == level
            ):
                return record
        return None

    def _filter_conclusions(
        self,
        records: list[ConclusionRecord],
        *,
        observed_id: str | None = None,
        observer_id: str | None = None,
        session_id: str | None = None,
        level: ConclusionLevel | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[ConclusionRecord]:
        filtered = records
        if observed_id is not None:
            filtered = [r for r in filtered if r.observed_id == observed_id]
        if observer_id is not None:
            filtered = [r for r in filtered if r.observer_id == observer_id]
        if session_id is not None:
            filtered = [r for r in filtered if r.session_id == session_id]
        if level is not None:
            filtered = [r for r in filtered if r.level == level]
        if metadata:
            filtered = [
                r for r in filtered if all(r.metadata.get(key) == value for key, value in metadata.items())
            ]
        return filtered

    def _ensure_workspace(self, client: httpx.Client) -> None:
        if self._ensured_workspace:
            return
        client.post(
            self._url("/v3/workspaces"),
            json={"id": self._encoded_ws, "metadata": {"jarvis_id": self._ws}},
        ).raise_for_status()
        self._ensured_workspace = True

    def _ensure_peer(self, client: httpx.Client, peer_id: str, *, observe_me: bool | None = None) -> None:
        if peer_id in self._ensured_peers and observe_me is None:
            return
        encoded = self._encoded_peer(peer_id)
        payload: dict[str, Any] = {"id": encoded, "metadata": {"jarvis_id": peer_id}}
        if observe_me is not None:
            payload["configuration"] = {"observe_me": observe_me}
        client.post(self._url(self._ws_path("/peers")), json=payload).raise_for_status()
        self._ensured_peers.add(peer_id)

    def _ensure_session(
        self,
        client: httpx.Client,
        session_id: str,
        peers: list[SessionPeer],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        peer_key = ",".join(sorted(p.peer_id for p in peers))
        cache_key_for_session = f"{session_id}|{peer_key}"
        if cache_key_for_session in self._ensured_sessions:
            return
        for peer in peers:
            self._ensure_peer(client, peer.peer_id, observe_me=peer.observe_me)
        encoded_peers: dict[str, dict[str, bool]] = {}
        for peer in peers:
            config: dict[str, bool] = {}
            if peer.observe_me is not None:
                config["observe_me"] = peer.observe_me
            if peer.observe_others is not None:
                config["observe_others"] = peer.observe_others
            encoded_peers[self._encoded_peer(peer.peer_id)] = config
        payload: dict[str, Any] = {
            "id": self._encoded_session(session_id),
            "metadata": {"jarvis_id": session_id, **(metadata or {})},
            # Session-scoped reasoning features, matching the shape proven by
            # deploy/honcho-v3/validate.py — without this block a v3 cutover
            # could write turns that never produce conclusions or summaries.
            "configuration": _SESSION_CONFIGURATION,
        }
        if encoded_peers:
            payload["peers"] = encoded_peers
        client.post(self._url(self._ws_path("/sessions")), json=payload).raise_for_status()
        self._ensured_sessions.add(cache_key_for_session)

    def read_cached_representation(self, user: str | None = None) -> str:
        path = self._cache_path(user)
        if not path.exists():
            return ""
        try:
            return json.loads(path.read_text(encoding="utf-8")).get("representation", "")
        except (json.JSONDecodeError, OSError):
            return ""

    def _write_turn_sync(
        self,
        user_text: str,
        assistant_text: str,
        user: str | None = None,
        channel: str = "voice",
        device_id: str | None = None,
    ) -> None:
        session_id = self._session_id(user)
        metadata = _turn_metadata(channel=channel, device_id=device_id)
        messages = [
            MemoryMessage(peer_id=self._peer(user), content=user_text, metadata=metadata),
            MemoryMessage(peer_id=self._cfg.assistant_peer_id, content=assistant_text, metadata=metadata),
        ]
        self.create_messages(session_id, messages)

    def _refresh_cache_sync(self, min_interval_s: float = 0.0, user: str | None = None) -> str | None:
        if min_interval_s > 0 and self.read_cached_representation(user):
            if (time.monotonic() - self._last_refresh.get(self._peer(user), 0.0)) < min_interval_s:
                return None
        text = self.dialectic_chat(
            self._peer(user),
            _MEMORY_QUERY,
            session_id=self._session_id(user),
            reasoning_level="low",
        ).strip()
        path = self._cache_path(user)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"representation": text}), encoding="utf-8")
        self._last_refresh[self._peer(user)] = time.monotonic()
        return text

    async def write_turn(
        self,
        user_text: str,
        assistant_text: str,
        *,
        user: str | None = None,
        channel: str = "voice",
        device_id: str | None = None,
    ) -> None:
        await asyncio.to_thread(self._write_turn_sync, user_text, assistant_text, user, channel, device_id)

    async def refresh_cache(self, min_interval_s: float = 0.0, *, user: str | None = None) -> bool:
        result = await asyncio.to_thread(self._refresh_cache_sync, min_interval_s, user)
        return result is not None

    def get_or_create_peer(
        self,
        peer_id: str,
        *,
        observe_me: bool | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PeerRecord:
        with self._client() as client:
            self._ensure_workspace(client)
            encoded = self._encoded_peer(peer_id)
            payload: dict[str, Any] = {"id": encoded, "metadata": {"jarvis_id": peer_id, **(metadata or {})}}
            if observe_me is not None:
                payload["configuration"] = {"observe_me": observe_me}
            response = client.post(self._url(self._ws_path("/peers")), json=payload)
            response.raise_for_status()
            self._ensured_peers.add(peer_id)
            data = response.json() or payload
        return PeerRecord(
            id=decode_honcho_id(str(data.get("id", encoded))),
            metadata=dict(data.get("metadata") or metadata or {}),
            observe_me=observe_me,
        )

    def get_peer_card(self, peer_id: str, *, target: str | None = None) -> tuple[str, ...]:
        params = {"target": self._encoded_peer(target)} if target else None
        with self._client() as client:
            response = client.get(
                self._url(self._ws_path(f"/peers/{self._q(self._encoded_peer(peer_id))}/card")),
                params=params,
            )
            response.raise_for_status()
            data = response.json() or {}
        card = data.get("peer_card") or data.get("card") or (data if isinstance(data, list) else [])
        return tuple(str(item) for item in card)

    def set_peer_card(self, peer_id: str, card: list[str], *, target: str | None = None) -> tuple[str, ...]:
        payload: dict[str, Any] = {"peer_card": list(card)}
        params = {"target": self._encoded_peer(target)} if target else None
        with self._client() as client:
            self._ensure_workspace(client)
            self._ensure_peer(client, peer_id)
            if target:
                self._ensure_peer(client, target)
            response = client.put(
                self._url(self._ws_path(f"/peers/{self._q(self._encoded_peer(peer_id))}/card")),
                json=payload,
                params=params,
            )
            response.raise_for_status()
            data = response.json() or payload
        updated = data.get("peer_card") or data.get("card") or (data if isinstance(data, list) else card)
        return tuple(str(item) for item in updated)

    def create_session(
        self,
        session_id: str,
        *,
        peers: list[SessionPeer] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SessionRecord:
        peers = peers or []
        with self._client() as client:
            self._ensure_workspace(client)
            self._ensure_session(client, session_id, peers, metadata=metadata)
        return SessionRecord(id=session_id, peers=tuple(peers), metadata=dict(metadata or {}))

    def delete_session(self, session_id: str) -> None:
        encoded = self._encoded_session(session_id)
        with self._client() as client:
            response = client.delete(self._url(self._ws_path(f"/sessions/{self._q(encoded)}")))
            response.raise_for_status()
        self._ensured_sessions = {item for item in self._ensured_sessions if not item.startswith(f"{session_id}|")}

    def create_messages(self, session_id: str, messages: list[MemoryMessage]) -> list[dict[str, Any]]:
        peers = [
            SessionPeer(
                peer_id=message.peer_id,
                observe_me=False if message.peer_id == self._cfg.assistant_peer_id else True,
                observe_others=False,
            )
            for message in messages
        ]
        payload = {
            "messages": [
                {
                    "peer_id": self._encoded_peer(message.peer_id),
                    "content": message.content,
                    "metadata": dict(message.metadata),
                }
                for message in messages
            ]
        }
        with self._client() as client:
            self._ensure_workspace(client)
            self._ensure_session(client, session_id, peers)
            response = client.post(
                self._url(self._ws_path(f"/sessions/{self._q(self._encoded_session(session_id))}/messages")),
                json=payload,
            )
            response.raise_for_status()
            data = response.json() or []
        return data if isinstance(data, list) else list(data.get("items", []))

    def create_conclusion(
        self,
        *,
        observed_id: str,
        content: str,
        observer_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ConclusionRecord:
        observer = observer_id or self._cfg.assistant_peer_id
        metadata = dict(metadata or {})
        metadata = self._normalise_conclusion_metadata(metadata)
        level = metadata["level"]
        conclusion = {
            "observer_id": self._encoded_peer(observer),
            "observed_id": self._encoded_peer(observed_id),
            "content": content,
            "level": level,
            "metadata": metadata,
        }
        if session_id:
            conclusion["session_id"] = self._encoded_session(session_id)
        with self._client() as client:
            self._ensure_workspace(client)
            self._ensure_peer(client, observer, observe_me=False if observer == self._cfg.assistant_peer_id else True)
            self._ensure_peer(client, observed_id)
            content_hash = str(metadata.get("content_hash", ""))
            self._sidecar.put_pending(
                self._ws,
                content_hash,
                observer_id=observer,
                observed_id=observed_id,
                content=content,
                metadata=metadata,
            )
            if content_hash:
                existing = self._find_exact_conclusion(
                    client,
                    observer_id=observer,
                    observed_id=observed_id,
                    content=content,
                    level=level,  # type: ignore[arg-type]
                )
                if existing is not None:
                    return existing
            response = client.post(
                self._url(self._ws_path("/conclusions")),
                json={"conclusions": [conclusion]},
            )
            response.raise_for_status()
            data = response.json() or []
        item = data[0] if isinstance(data, list) else data
        cid = str(item["id"])
        self._sidecar.put(self._ws, cid, metadata)
        return self._decode_conclusion(item)

    def list_conclusions(
        self,
        *,
        observed_id: str | None = None,
        observer_id: str | None = None,
        session_id: str | None = None,
        level: ConclusionLevel | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[ConclusionRecord]:
        filters: dict[str, Any] = {}
        if observed_id:
            filters["observed_id"] = self._encoded_peer(observed_id)
        if observer_id:
            filters["observer_id"] = self._encoded_peer(observer_id)
        if session_id:
            filters["session_id"] = self._encoded_session(session_id)
        if level:
            filters["level"] = level
        payload = {"filters": filters} if filters else {}
        with self._client() as client:
            items, known_complete = self._list_conclusion_items(client, payload)
        records = [self._decode_conclusion(item) for item in items]
        if not filters and known_complete and items:
            self._sidecar.reconcile(self._ws, {str(item.get("id", "")) for item in items if item.get("id")})
        return self._filter_conclusions(
            records,
            observed_id=observed_id,
            observer_id=observer_id,
            session_id=session_id,
            level=level,
            metadata=metadata,
        )

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
        filters: dict[str, Any] = {}
        if observed_id:
            filters["observed"] = self._encoded_peer(observed_id)
        semantic_observer_id = observer_id or observed_id
        if semantic_observer_id:
            filters["observer"] = self._encoded_peer(semantic_observer_id)
        if session_id:
            filters["session_id"] = self._encoded_session(session_id)
        if level:
            filters["level"] = level
        payload: dict[str, Any] = {"query": query}
        if filters:
            payload["filters"] = filters
        if limit is not None:
            payload["top_k"] = limit
        with self._client() as client:
            response = client.post(self._url(self._ws_path("/conclusions/query")), json=payload)
            response.raise_for_status()
            data = response.json()
        items = self._conclusion_items_from_response(data)
        records = [self._decode_conclusion(item) for item in items]
        return self._filter_conclusions(
            records,
            observed_id=observed_id,
            observer_id=observer_id,
            session_id=session_id,
            level=level,
            metadata=metadata,
        )

    def delete_conclusion(self, conclusion_id: str) -> None:
        with self._client() as client:
            response = client.delete(self._url(self._ws_path(f"/conclusions/{self._q(conclusion_id)}")))
            response.raise_for_status()
        self._sidecar.delete(self._ws, conclusion_id)

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
        payload: dict[str, Any] = {}
        if session_id:
            payload["session_id"] = self._encoded_session(session_id)
        if search_query:
            payload["search_query"] = search_query
        if search_top_k is not None:
            payload["search_top_k"] = search_top_k
        if target:
            payload["target"] = self._encoded_peer(target)
        if max_conclusions is not None:
            payload["max_conclusions"] = max_conclusions
        with self._client() as client:
            response = client.post(
                self._url(self._ws_path(f"/peers/{self._q(self._encoded_peer(peer_id))}/representation")),
                json=payload,
            )
            response.raise_for_status()
            data = response.json() or {}
        card = data.get("peer_card") or data.get("card") or []
        return RepresentationRecord(
            peer_id=peer_id,
            target=target,
            representation=str(data.get("representation", "")),
            peer_card=tuple(str(item) for item in card),
        )

    def dialectic_chat(
        self,
        peer_id: str,
        query: str,
        *,
        session_id: str | None = None,
        target: str | None = None,
        reasoning_level: str = "low",
    ) -> str:
        payload: dict[str, Any] = {
            "query": query,
            "reasoning_level": reasoning_level,
            "stream": False,
        }
        if session_id:
            payload["session_id"] = self._encoded_session(session_id)
        if target:
            payload["target"] = self._encoded_peer(target)
        with self._client() as client:
            response = client.post(
                self._url(self._ws_path(f"/peers/{self._q(self._encoded_peer(peer_id))}/chat")),
                json=payload,
            )
            response.raise_for_status()
            data = response.json() or {}
        return str(data.get("content", ""))

    def queue_status(self) -> QueueStatus:
        with self._client() as client:
            response = client.get(self._url(self._ws_path("/queue/status")))
            response.raise_for_status()
            data = response.json() or {}
        return QueueStatus(
            pending_work_units=int(data.get("pending_work_units", 0)),
            in_progress_work_units=int(data.get("in_progress_work_units", 0)),
            raw=dict(data),
        )

    def ping(self) -> bool:
        try:
            with self._client() as client:
                response = client.get(self._url("/health"))
            return response.status_code == 200
        except httpx.HTTPError:
            return False

    def upload_file(
        self,
        session_id: str,
        *,
        peer_id: str,
        path: Path,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._client() as client:
            self._ensure_workspace(client)
            self._ensure_session(client, session_id, [SessionPeer(peer_id=peer_id, observe_me=True)])
            with Path(path).open("rb") as handle:
                response = client.post(
                    self._url(
                        self._ws_path(f"/sessions/{self._q(self._encoded_session(session_id))}/messages/upload")
                    ),
                    data={
                        "peer_id": self._encoded_peer(peer_id),
                        "metadata": json.dumps(metadata or {}),
                    },
                    files={"file": (Path(path).name, handle)},
                )
            response.raise_for_status()
            data = response.json() if response.content else {}
        return dict(data or {})
