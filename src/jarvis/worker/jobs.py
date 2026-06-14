"""Job tracking for long-running worker actions, persisted to disk (Phase 3c).

Deep work (a coding-agent run) takes minutes, so the daemon starts it as a
background task and returns a job id immediately — the brain never blocks. Jobs
are persisted as one JSON file each under a store dir (no database — matches the
project's file-based operational data and keeps the worker self-contained), so
they survive a daemon restart. A job left "running" when the daemon died is
reloaded as "interrupted".
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import re
import time
import uuid
from collections.abc import Awaitable
from dataclasses import asdict, dataclass, field

_SESSION_ID = re.compile(r"session id:\s*(\S+)", re.IGNORECASE)


def slugify(text: str, max_words: int = 6) -> str:
    """A short, file- and speech-friendly handle from free text."""
    words = re.findall(r"[a-z0-9]+", text.lower())[:max_words]
    return "-".join(words) or "job"


@dataclass
class Job:
    id: str
    action: str
    label: str  # the full task/prompt (the "description")
    name: str = ""  # short human handle (user-given or auto-slugged)
    cwd: str = ""  # the working directory the job ran in (where file changes land)
    branch: str | None = None  # git branch for an isolated repo-job worktree
    repo: str = ""  # the source repo (for worktree jobs) — needed to clean up
    status: str = "running"  # running | done | error | interrupted
    output: str = ""
    session_id: str | None = None  # the coding agent's session (for `codex resume`)
    started: float = field(default_factory=time.time)
    ended: float | None = None

    def public(self) -> dict:
        d = asdict(self)
        d["started"] = round(self.started, 1)
        return d


class JobManager:
    def __init__(self, store_dir: str | None = None) -> None:
        self._jobs: dict[str, Job] = {}
        self._tasks: set[asyncio.Task] = set()
        self._store = pathlib.Path(store_dir) if store_dir else None
        if self._store is not None:
            self._store.mkdir(parents=True, exist_ok=True)
            self._load()

    # --- persistence -------------------------------------------------------
    def _load(self) -> None:
        for f in sorted(self._store.glob("*.json")):  # type: ignore[union-attr]
            try:
                d = json.loads(f.read_text())
            except (json.JSONDecodeError, OSError):
                continue
            job = Job(
                id=d["id"],
                action=d.get("action", "?"),
                label=d.get("label", ""),
                name=d.get("name", ""),
                cwd=d.get("cwd", ""),
                branch=d.get("branch"),
                repo=d.get("repo", ""),
                status=d.get("status", "done"),
                output=d.get("output", ""),
                session_id=d.get("session_id"),
                started=d.get("started", 0.0),
                ended=d.get("ended"),
            )
            if job.status == "running":  # the daemon died mid-job
                job.status = "interrupted"
            self._jobs[job.id] = job

    def _path(self, job: Job) -> pathlib.Path | None:
        # human-readable filename: <name>-<shortid>.json
        return None if self._store is None else self._store / f"{job.name}-{job.id[:6]}.json"

    def _persist(self, job: Job) -> None:
        path = self._path(job)
        if path is None:
            return
        try:
            path.write_text(json.dumps(job.public()))
        except OSError:
            pass  # persistence is best-effort; never break a job over it

    # --- lifecycle ---------------------------------------------------------
    def start(
        self,
        action: str,
        label: str,
        coro: Awaitable[str],
        name: str = "",
        cwd: str = "",
        branch: str | None = None,
        repo: str = "",
    ) -> Job:
        job = Job(
            id=uuid.uuid4().hex[:12],
            action=action,
            label=label,
            name=slugify(name or label),
            cwd=cwd,
            branch=branch,
            repo=repo,
        )
        self._jobs[job.id] = job
        self._persist(job)
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
            m = _SESSION_ID.search(job.output)
            if m:
                job.session_id = m.group(1)
            self._persist(job)

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def remove(self, job_id: str) -> None:
        """Drop a job from the list and delete its record file (after its worktree
        has been cleaned up by the caller)."""
        job = self._jobs.pop(job_id, None)
        if job is None:
            return
        path = self._path(job)
        if path is not None and path.exists():
            path.unlink()

    def find(self, query: str) -> Job | None:
        """Most recent job matching `query` by name or label (for 'check the
        polymarket job')."""
        q = query.lower().strip()
        qs = slugify(query)
        for job in reversed(self._jobs.values()):
            if (qs and qs in job.name) or (q and q in job.label.lower()):
                return job
        return None

    def latest(self) -> Job | None:
        return next(reversed(self._jobs.values()), None)

    def recent(self, n: int = 20) -> list[Job]:
        return list(self._jobs.values())[-n:]
