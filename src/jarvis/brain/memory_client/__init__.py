"""Memory backend selector.

`MemoryClient(cfg)` remains the compatibility constructor used by the brain.
The default backend is v3 (production cut over 2026-07-05); setting
`MEMORY_BACKEND=v2` returns the legacy Honcho v2 client, kept for rollback,
without changing `BrainSession` or the turn loop.
"""

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
)
from jarvis.brain.memory_client.v2 import HonchoV2MemoryClient, UnsupportedMemoryOperation
from jarvis.config import MemoryConfig


class MemoryClient:
    def __new__(cls, cfg: MemoryConfig):  # noqa: ANN204
        if cfg.backend == "v3":
            from jarvis.brain.memory_client.v3 import HonchoV3MemoryClient

            return HonchoV3MemoryClient(cfg)
        return HonchoV2MemoryClient(cfg)


__all__ = [
    "ConclusionLevel",
    "ConclusionRecord",
    "HonchoV2MemoryClient",
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
