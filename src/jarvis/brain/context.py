"""RequestContext — the per-request identity/capability envelope (Phase 3 §4/§5).

Every request the brain handles flows down the resolution stack: who is speaking
(`identity`), from where (`device_id`), in what `scope` (house vs personal), and
which `capabilities` that combination grants. In Phase 3a this is single-
principal (built once from config); in 3b+ the brain server builds one per
connection/utterance. The object is immutable — a turn cannot widen its own
grants.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RequestContext:
    device_id: str  # which intercom/device the request arrived from
    identity: str  # who is speaking ("house" when unknown; a name when known)
    scope: str  # "house" | "personal"
    capabilities: frozenset[str]  # capabilities granted for THIS request
    # Phase 3d additions (defaulted so single-principal call sites are unchanged):
    channel: str = "voice"  # voice | whatsapp | … — which surface this came from
    confidence: str = "strong"  # strong | claimed | unknown (identity confidence)
    peer: str = ""  # memory principal (Honcho peer); empty => derive from identity

    def can(self, capability: str) -> bool:
        return capability in self.capabilities

    @property
    def memory_peer(self) -> str:
        """The Honcho peer this request's memory is scoped to (the privacy wall):
        the user's configured peer, else their identity."""
        return self.peer or self.identity
