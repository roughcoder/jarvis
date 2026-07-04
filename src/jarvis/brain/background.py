"""Background-task lane — fire-and-forget agentic work (proactive completion).

A `BackgroundRunner` takes a task description + the asker's `RequestContext`, builds
an isolated `BrainSession` (same capabilities — never more), runs its headless
agentic loop to completion DETACHED from the hot path, and delivers the outcome
through a `notify` callback (the server's Proactive broadcast). The voice turn that
starts a job returns immediately ("on it"); the result arrives later as a proactive
message — so a slow task ("book a table at the pub", deep research, a long Mac job)
never makes the user wait through it.

Off the hot path by construction: `start()` creates an asyncio task and returns. A
concurrency cap rejects new jobs when too many are already running, and a hard
per-job timeout stops a runaway. The inner session is built with `background.run`
stripped, so a background task can't spawn more background tasks (no recursion).

This module is transport-free (it never touches a websocket) — the boundary
discipline that keeps the brain's tiers movable: it depends only on a session
factory and an async `notify(text)`.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from jarvis.runtime import RequestContext
from jarvis.brain.session import BrainSession
from jarvis.config import BackgroundConfig

SessionFactory = Callable[[RequestContext], BrainSession]
Notify = Callable[[str, str, str], Awaitable[None]]  # (text, identity, device_id)

_BACKGROUND_CAP = "background.run"


def _short(task: str, n: int = 60) -> str:
    t = " ".join((task or "").split())
    return t if len(t) <= n else t[: n - 1] + "…"


@dataclass
class Job:
    id: int
    task: str
    identity: str
    device_id: str = ""
    status: str = "running"  # running | done | error
    result: str = ""
    error_kind: str = ""


class BackgroundRunner:
    def __init__(
        self,
        cfg: BackgroundConfig,
        *,
        session_factory: SessionFactory,
        notify: Notify,
    ) -> None:
        self._cfg = cfg
        self._session_factory = session_factory
        self._notify = notify
        self._jobs: dict[int, Job] = {}
        self._tasks: set[asyncio.Task] = set()
        self._seq = 0

    @property
    def active(self) -> int:
        return sum(1 for j in self._jobs.values() if j.status == "running")

    def start(self, ctx: RequestContext, task: str) -> tuple[bool, str]:
        """Kick off a background job. Returns (accepted, message); never blocks and
        never raises. Rejects when the concurrency cap is already reached."""
        task = (task or "").strip()
        if not task:
            return (False, "a background task needs a description")
        if self.active >= max(1, self._cfg.max_concurrent):
            return (False, f"already running {self.active} background task(s) — wait for one to finish")
        self._seq += 1
        job = Job(id=self._seq, task=task, identity=ctx.identity, device_id=ctx.device_id)
        self._jobs[job.id] = job
        # Strip the background capability so a background task can't recurse into
        # spawning more background tasks; everything else (its real powers) carries.
        inner = dataclasses.replace(
            ctx, capabilities=ctx.capabilities - frozenset({_BACKGROUND_CAP})
        )
        t = asyncio.create_task(self._run(job, inner))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)
        return (True, f"started background task #{job.id}")

    async def _run(self, job: Job, ctx: RequestContext) -> None:
        try:
            session = self._session_factory(ctx)
            result = await asyncio.wait_for(
                session.run_task(job.task, max_rounds=self._cfg.max_rounds),
                timeout=self._cfg.timeout_s,
            )
            job.status, job.result = "done", (result or "").strip()
            if not job.result:
                job.result = f"I've finished what you asked — {_short(job.task)}."
        # Fallbacks stay DETERMINISTIC (failure reporting must never depend on
        # the stack that just failed) and spoken-safe: the raw exception goes to
        # the log, never to TTS.
        except asyncio.TimeoutError:
            job.status = "error"
            job.error_kind = "timeout"
            job.result = (
                f"I had to stop “{_short(job.task)}” — it was taking longer than "
                "I allow. Say the word if you'd like me to try again."
            )
        except Exception as exc:  # noqa: BLE001 - background work must never crash the brain
            job.status = "error"
            job.error_kind = exc.__class__.__name__
            job.result = (
                f"I couldn't finish “{_short(job.task)}” — something went wrong "
                "partway through. I can have another go if you like."
            )
            print(f"  [background] job #{job.id} failed ({job.error_kind}): {exc}")
        await self._deliver(job)

    async def _deliver(self, job: Job) -> None:
        print(f"  [background] job #{job.id} {job.status} → notifying: {job.result}")
        try:
            await self._notify(job.result, job.identity, job.device_id)
        except Exception as exc:  # noqa: BLE001 - delivery is best-effort
            print(f"  [background] notify failed for job #{job.id}: {exc}")
