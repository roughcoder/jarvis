"""ContextStore — per-`(device × user)` BrainSession registry (Phase 3d §9).

Phase 1/3a was single-principal: one session, one history, one memory cache. With
multiple devices and people, each principal on each device gets its OWN session —
its own rolling history and its own memory peer/cache — plus a shared **house**
context for unknown speakers. This is the structural half of the privacy wall (§5):
two contexts never share conversation state. Sessions are created lazily and reused
so a returning speaker keeps their thread.
"""

from __future__ import annotations

from collections.abc import Callable

from jarvis.runtime import RequestContext
from jarvis.brain.session import BrainSession


class ContextStore:
    def __init__(self, make_session: Callable[[RequestContext], BrainSession]) -> None:
        # make_session builds a BrainSession for a context (wiring the per-user
        # memory principal). The store owns lifetime + reuse.
        self._make = make_session
        self._sessions: dict[tuple[str, str], BrainSession] = {}

    def get(self, ctx: RequestContext) -> BrainSession:
        """The session for this `(device, identity)`, created on first use (the factory
        loads the soul) and reused thereafter."""
        key = (ctx.device_id, ctx.identity)
        session = self._sessions.get(key)
        # Profiles can change while the brain is running (new hardware grants, user
        # pairing, channel/scope changes). A BrainSession stores its RequestContext,
        # so reusing one with stale capabilities would keep old tool access.
        if session is None or getattr(session, "_ctx", ctx) != ctx:
            session = self._make(ctx)  # _make_session loads SOUL.md
            self._sessions[key] = session
        return session

    def __len__(self) -> int:
        return len(self._sessions)

    @property
    def keys(self) -> list[tuple[str, str]]:
        return list(self._sessions)
