"""Best-effort worker-to-orchestration change notifications.

The worker's durable session files remain the source of truth.  This module
only shortens the time before the orchestration API notices a change, so every
operation here is deliberately detached from the worker's request path.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx

logger = logging.getLogger(__name__)

NotifyPost = Callable[[str, dict, dict], Awaitable[object]]
_MAX_PENDING_CHANGES = 128


@dataclass(frozen=True)
class WorkerChange:
    worker_id: str
    kind: str
    session_id: str = ""
    job_id: str = ""

    @property
    def key(self) -> str:
        return f"session:{self.session_id}" if self.session_id else f"job:{self.job_id}"

    def body(self) -> dict[str, str]:
        body = {"worker_id": self.worker_id, "kind": self.kind}
        if self.session_id:
            body["session_id"] = self.session_id
        if self.job_id:
            body["job_id"] = self.job_id
        return body


class WorkerChangeNotifier:
    """Thread-safe, bounded, per-resource-coalescing notify sender.

    Session persistence is synchronous and can be reached from provider helper
    threads. ``enqueue`` therefore only schedules loop work; it never awaits or
    performs network I/O. A resource has one delivery task at most, while any
    later changes collapse into one follow-up request.
    """

    def __init__(
        self,
        *,
        url: str,
        token: str,
        worker_id: str,
        post: NotifyPost | None = None,
    ) -> None:
        self.url = url.rstrip("/")
        self.token = token
        self.worker_id = worker_id
        self._post = post or _post_notify
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pending: dict[str, WorkerChange] = {}
        self._pre_start: dict[str, WorkerChange] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self.url and self.token and self.worker_id)

    async def start(self) -> None:
        with self._lock:
            self._loop = asyncio.get_running_loop()
            pending = list(self._pre_start.values())
            self._pre_start.clear()
        for change in pending:
            self._enqueue_on_loop(change)

    def enqueue(self, *, kind: str, session_id: str = "", job_id: str = "") -> None:
        if not self.enabled or not (session_id or job_id):
            return
        change = WorkerChange(
            worker_id=self.worker_id,
            kind=kind,
            session_id=session_id,
            job_id=job_id,
        )
        with self._lock:
            loop = self._loop
        if loop is None or loop.is_closed():
            with self._lock:
                if change.key in self._pre_start or len(self._pre_start) < _MAX_PENDING_CHANGES:
                    self._pre_start[change.key] = change
                else:
                    logger.debug("dropping worker change notification: pending queue is full")
            return
        loop.call_soon_threadsafe(self._enqueue_on_loop, change)

    def _enqueue_on_loop(self, change: WorkerChange) -> None:
        key = change.key
        if key not in self._pending and key not in self._tasks and len(self._pending) + len(self._tasks) >= _MAX_PENDING_CHANGES:
            logger.debug("dropping worker change notification: pending queue is full")
            return
        self._pending[key] = change
        if key not in self._tasks:
            self._tasks[key] = asyncio.create_task(self._deliver(key), name=f"worker-notify-{key}")

    async def _deliver(self, key: str) -> None:
        try:
            while change := self._pending.pop(key, None):
                try:
                    response = await asyncio.wait_for(
                        self._post(
                            f"{self.url}/v1/worker/notify",
                            change.body(),
                            {"Authorization": f"Bearer {self.token}"},
                        ),
                        timeout=2.0,
                    )
                    if getattr(response, "status_code", 200) >= 400:
                        logger.debug("worker change notification rejected: status=%s", getattr(response, "status_code", 0))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001 - notification is never part of worker correctness
                    logger.debug("worker change notification failed: %s", exc)
        finally:
            self._tasks.pop(key, None)

    async def aclose(self) -> None:
        tasks = list(self._tasks.values())
        self._tasks.clear()
        self._pending.clear()
        self._pre_start.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def _post_notify(url: str, body: dict, headers: dict) -> httpx.Response:
    async with httpx.AsyncClient(timeout=2.0) as client:
        return await client.post(url, json=body, headers=headers)
