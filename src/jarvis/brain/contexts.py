"""ContextStore — per-`(device × user)` BrainSession registry (Phase 3d §9).

Phase 1/3a was single-principal: one session, one history, one memory cache. With
multiple devices and people, each principal on each device gets its OWN session —
its own rolling history and its own memory peer/cache — plus a shared **house**
context for unknown speakers. This is the structural half of the privacy wall (§5):
two contexts never share conversation state. Sessions are created lazily and reused
so a returning speaker keeps their thread.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Callable

from jarvis.runtime import RequestContext
from jarvis.brain.session import BrainSession

_MAX_SESSIONS = 64


class ContextStore:
    def __init__(
        self,
        make_session: Callable[[RequestContext], BrainSession],
        *,
        max_sessions: int = _MAX_SESSIONS,
    ) -> None:
        # make_session builds a BrainSession for a context (wiring the per-user
        # memory principal). The store owns lifetime + reuse.
        self._make = make_session
        self._max_sessions = max(1, max_sessions)
        self._sessions: OrderedDict[tuple[str, str], BrainSession] = OrderedDict()
        # Retired sessions with pending cold-path writes stay strongly referenced
        # until those tasks finish; otherwise context replacement can detach the
        # last turn's memory write from the resident brain's lifetime accounting.
        self._retired: list[BrainSession] = []

    def get(self, ctx: RequestContext) -> BrainSession:
        """The session for this `(device, identity)`, created on first use (the factory
        loads the soul) and reused thereafter."""
        self._prune_retired()
        key = (ctx.device_id, ctx.identity)
        session = self._sessions.get(key)
        # Profiles can change while the brain is running (new hardware grants, user
        # pairing, channel/scope changes). A BrainSession stores its RequestContext,
        # so reusing one with stale capabilities would keep old tool access.
        if session is not None and getattr(session, "_ctx", ctx) == ctx:
            self._sessions.move_to_end(key)
            return session
        if session is not None:
            self._retire(session)
            self._sessions.pop(key, None)
        if session is None or getattr(session, "_ctx", ctx) != ctx:
            session = self._make(ctx)  # _make_session loads SOUL.md
            self._sessions[key] = session
            self._evict_if_needed()
        return session

    def __len__(self) -> int:
        return len(self._sessions)

    @property
    def keys(self) -> list[tuple[str, str]]:
        return list(self._sessions)

    def _evict_if_needed(self) -> None:
        while len(self._sessions) > self._max_sessions:
            _, session = self._sessions.popitem(last=False)
            self._retire(session)

    def _retire(self, session: BrainSession) -> None:
        tasks = [t for t in _pending_cold_tasks(session) if not t.done()]
        if not tasks:
            return
        self._retired.append(session)
        for task in tasks:
            task.add_done_callback(lambda _task: self._prune_retired())

    def _prune_retired(self) -> None:
        self._retired = [
            session
            for session in self._retired
            if any(not task.done() for task in _pending_cold_tasks(session))
        ]


def _pending_cold_tasks(session: BrainSession) -> tuple[asyncio.Task, ...]:
    tasks = getattr(session, "pending_cold_tasks", ())
    return tuple(task for task in tasks if isinstance(task, asyncio.Task))
