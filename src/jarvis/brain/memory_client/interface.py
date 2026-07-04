"""Memory backend contract for Honcho-backed Jarvis memory.

The voice hot path is intentionally tiny: `read_cached_representation()` is a
local file read. Everything else is a cold-path or explicit-tool boundary call
to the configured memory service.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol


ConclusionLevel = Literal["explicit", "deductive", "inductive", "contradiction"]


@dataclass(frozen=True)
class PeerRecord:
    id: str
    metadata: dict[str, Any] = field(default_factory=dict)
    observe_me: bool | None = None


@dataclass(frozen=True)
class SessionPeer:
    peer_id: str
    observe_me: bool | None = None
    observe_others: bool | None = None


@dataclass(frozen=True)
class SessionRecord:
    id: str
    peers: tuple[SessionPeer, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MemoryMessage:
    peer_id: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ConclusionRecord:
    id: str
    content: str
    observer_id: str
    observed_id: str
    level: ConclusionLevel = "explicit"
    session_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RepresentationRecord:
    peer_id: str
    representation: str
    target: str | None = None
    peer_card: tuple[str, ...] = ()


@dataclass(frozen=True)
class QueueStatus:
    pending_work_units: int = 0
    in_progress_work_units: int = 0
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def idle(self) -> bool:
        return self.pending_work_units + self.in_progress_work_units == 0


class MemoryBackend(Protocol):
    """Common surface used by the brain and future memory tools."""

    def read_cached_representation(self, user: str | None = None) -> str: ...

    async def write_turn(self, user_text: str, assistant_text: str, *, user: str | None = None) -> None: ...

    async def refresh_cache(self, min_interval_s: float = 0.0, *, user: str | None = None) -> bool: ...

    def deriver_idle(self) -> bool: ...

    def ping(self) -> bool: ...

    def get_or_create_peer(
        self,
        peer_id: str,
        *,
        observe_me: bool | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PeerRecord: ...

    def get_peer_card(self, peer_id: str, *, target: str | None = None) -> tuple[str, ...]: ...

    def set_peer_card(self, peer_id: str, card: list[str], *, target: str | None = None) -> tuple[str, ...]: ...

    def create_session(
        self,
        session_id: str,
        *,
        peers: list[SessionPeer] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SessionRecord: ...

    def delete_session(self, session_id: str) -> None: ...

    def create_messages(self, session_id: str, messages: list[MemoryMessage]) -> list[dict[str, Any]]: ...

    def create_conclusion(
        self,
        *,
        observed_id: str,
        content: str,
        observer_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ConclusionRecord: ...

    def list_conclusions(
        self,
        *,
        observed_id: str | None = None,
        observer_id: str | None = None,
        session_id: str | None = None,
        level: ConclusionLevel | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[ConclusionRecord]: ...

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
    ) -> list[ConclusionRecord]: ...

    def delete_conclusion(self, conclusion_id: str) -> None: ...

    def read_representation(
        self,
        peer_id: str,
        *,
        session_id: str | None = None,
        search_query: str | None = None,
        search_top_k: int | None = None,
        target: str | None = None,
        max_conclusions: int | None = None,
    ) -> RepresentationRecord: ...

    def dialectic_chat(
        self,
        peer_id: str,
        query: str,
        *,
        session_id: str | None = None,
        target: str | None = None,
        reasoning_level: str = "low",
    ) -> str: ...

    def queue_status(self) -> QueueStatus: ...

    def upload_file(
        self,
        session_id: str,
        *,
        peer_id: str,
        path: Path,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]: ...
