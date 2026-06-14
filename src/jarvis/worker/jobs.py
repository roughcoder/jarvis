"""In-memory job tracking for long-running worker actions (Phase 3c).

Deep work (a coding-agent run) takes minutes, so the daemon starts it as a
background task and returns a job id immediately — the brain never blocks. No
aiohttp/brain imports, so it's unit-testable on its own.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable
from dataclasses import dataclass, field


@dataclass
class Job:
    id: str
    action: str
    label: str
    status: str = "running"  # running | done | error
    output: str = ""
    started: float = field(default_factory=time.time)
    ended: float | None = None

    def public(self) -> dict:
        return {
            "id": self.id,
            "action": self.action,
            "label": self.label,
            "status": self.status,
            "output": self.output,
            "started": round(self.started, 1),
            "ended": self.ended,
        }


class JobManager:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}
        self._tasks: set[asyncio.Task] = set()

    def start(self, action: str, label: str, coro: Awaitable[str]) -> Job:
        job = Job(id=uuid.uuid4().hex[:12], action=action, label=label)
        self._jobs[job.id] = job
        task = asyncio.create_task(self._run(job, coro))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return job

    async def _run(self, job: Job, coro: Awaitable[str]) -> None:
        try:
            job.output = await coro
            job.status = "done"
        except Exception as exc:  # noqa: BLE001 - a job failure must not crash the daemon
            job.output = f"error: {exc}"
            job.status = "error"
        finally:
            job.ended = round(time.time(), 1)

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def latest(self) -> Job | None:
        return next(reversed(self._jobs.values()), None)

    def recent(self, n: int = 20) -> list[Job]:
        return list(self._jobs.values())[-n:]
