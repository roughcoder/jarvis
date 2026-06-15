"""Memory client — talks to Honcho over its HTTP REST API (spec §3.1, §4).

Uses plain httpx against Honcho's /v2 endpoints at MEMORY_BASE_URL — an explicit
network boundary to a configurable host (§3.1), with no SDK/in-process coupling.
(The honcho-ai SDK 2.x targets /v3 while the stable v2.0.3 server serves /v2, so
calling the REST API directly is also the robust choice.)

Hot/cold split (spec §3.2):
  - read_cached_representation(): LOCAL file read. The ONLY memory call on the
    hot path.
  - write_turn(): cold path. POSTs the turn's messages; the deriver then updates
    the peer's working representation in the background.
  - refresh_cache(): cold path. POSTs to the cheap /representation endpoint (NOT
    the dialectic /chat reasoning endpoint) and caches the result locally.

Cold-path calls run blocking httpx in a worker thread, off the hot path.
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import time

from jarvis.config import MemoryConfig

# One rolling session for the single-user voice loop (Phase 1). It persists in
# Postgres, so recall survives app restarts ("across conversations", spec §1).
_SESSION_ID = "voice"

# Cold-path query that synthesises the cached memory document. Honcho's cheap
# /representation endpoint only fills in after many messages (it's summary-
# driven); the dialectic synthesises from stored facts immediately. Per spec
# §3.2 the dialectic is forbidden on the HOT path — this runs on the COLD path
# (after the reply) to refresh the local cache the hot path reads.
_MEMORY_QUERY = (
    "Summarise everything important you know about the user — their name, "
    "preferences, and any facts or ongoing context — in a few concise sentences. "
    "If you know nothing about them yet, reply with an empty string."
)


class MemoryClient:
    def __init__(self, cfg: MemoryConfig) -> None:
        self._cfg = cfg
        self._ws = cfg.workspace_id
        self._ensured: set[str] = set()  # peers already get-or-created
        self._last_refresh: dict[str, float] = {}  # per-peer debounce (monotonic)

    def _ws_url(self) -> str:
        return f"{self._cfg.base_url}/v2/workspaces/{self._ws}"

    def _headers(self) -> dict:
        key = self._cfg.api_key.get_secret_value()
        return {"Authorization": f"Bearer {key}"} if key else {}

    # --- per-principal scoping (Phase 3d) ----------------------------------
    # `user` is the speaking principal (Honcho peer). None / the configured default
    # peer keep the single-principal paths (back-compat with Phase 1 + the loop).
    def _peer(self, user: str | None) -> str:
        return user or self._cfg.user_peer_id

    def _session_id(self, user: str | None) -> str:
        peer = self._peer(user)
        return _SESSION_ID if peer == self._cfg.user_peer_id else f"{_SESSION_ID}-{peer}"

    def _cache_path(self, user: str | None) -> pathlib.Path:
        base = pathlib.Path(self._cfg.cache_path)
        peer = self._peer(user)
        if peer == self._cfg.user_peer_id:
            return base  # the default principal keeps the original cache file
        return base.with_name(f"{base.stem}-{peer}{base.suffix}")

    def _ensure(self, client, user: str | None) -> None:  # noqa: ANN001
        """Idempotently create workspace + this principal's peer + session, plus the
        shared assistant peer (get-or-create). Keyed per peer so each user is set up
        once."""
        peer = self._peer(user)
        if peer in self._ensured:
            return
        client.post(f"{self._cfg.base_url}/v2/workspaces", json={"id": self._ws})
        for p in (peer, self._cfg.assistant_peer_id):
            client.post(f"{self._ws_url()}/peers", json={"id": p})
        client.post(f"{self._ws_url()}/sessions", json={"id": self._session_id(user)})
        self._ensured.add(peer)

    # --- hot path (local only, no network) ---------------------------------
    def read_cached_representation(self, user: str | None = None) -> str:
        path = self._cache_path(user)
        if not path.exists():
            return ""
        try:
            return json.loads(path.read_text()).get("representation", "")
        except (json.JSONDecodeError, OSError):
            return ""

    # --- cold path (network, run off the hot path) -------------------------
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

        # Debounce per principal: skip the expensive dialectic if we refreshed this
        # peer recently — but always refresh when its cache is still empty.
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
        path.write_text(json.dumps({"representation": text}))
        self._last_refresh[self._peer(user)] = time.monotonic()
        return text

    async def write_turn(self, user_text: str, assistant_text: str, *, user: str | None = None) -> None:
        await asyncio.to_thread(self._write_turn_sync, user_text, assistant_text, user)

    async def refresh_cache(self, min_interval_s: float = 0.0, *, user: str | None = None) -> bool:
        """Refresh the cache; returns True if it actually ran (not debounced)."""
        result = await asyncio.to_thread(self._refresh_cache_sync, min_interval_s, user)
        return result is not None

    # --- helpers / gate support -------------------------------------------
    def deriver_idle(self) -> bool:
        """True when the deriver has no pending/in-progress work for our peer."""
        import httpx

        try:
            with httpx.Client(timeout=10.0, headers=self._headers()) as c:
                r = c.get(
                    f"{self._ws_url()}/deriver/status",
                    params={"peer_id": self._cfg.user_peer_id},
                )
                r.raise_for_status()
                s = r.json()
            return (s.get("pending_work_units", 0) + s.get("in_progress_work_units", 0)) == 0
        except httpx.HTTPError:
            return False

    def ping(self) -> bool:
        import httpx

        try:
            # v2.0.3 has no /health route; /openapi.json is a cheap liveness probe.
            r = httpx.get(f"{self._cfg.base_url}/openapi.json", timeout=5.0)
            return r.status_code == 200
        except httpx.HTTPError:
            return False
