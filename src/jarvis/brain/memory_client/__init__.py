"""Honcho v3 memory backend exports."""

from __future__ import annotations

from jarvis.brain.memory_client.encoding import cache_key, decode_honcho_id, encode_honcho_id
from jarvis.brain.memory_client.interface import (
    ConclusionLevel,
    ConclusionRecord,
    MemoryBackend,
    MemoryMessage,
    PeerRecord,
    QueueStatus,
    RepresentationRecord,
    SessionPeer,
    SessionRecord,
    UnsupportedMemoryOperation,
)
from jarvis.brain.memory_client.v3 import HonchoV3MemoryClient


class MemoryClient(HonchoV3MemoryClient):
    """Compatibility name used by the brain; Honcho v3 is the only backend."""


__all__ = [
    "ConclusionLevel",
    "ConclusionRecord",
    "HonchoV3MemoryClient",
    "MemoryBackend",
    "MemoryClient",
    "MemoryMessage",
    "PeerRecord",
    "QueueStatus",
    "RepresentationRecord",
    "SessionPeer",
    "SessionRecord",
    "UnsupportedMemoryOperation",
    "cache_key",
    "decode_honcho_id",
    "encode_honcho_id",
]
