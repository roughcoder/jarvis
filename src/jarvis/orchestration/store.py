from __future__ import annotations

import base64
import copy
import contextlib
import fcntl
import hashlib
import hmac
import json
import pathlib
import re
import shutil
import threading
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from jarvis.ids import new_id, utc_now
from jarvis.jsonl_cache import JsonlCacheEntry, read_jsonl_projection
from jarvis.orchestration.models import (
    Artifact,
    OrchestrationRun,
    RunEvent,
    WorkItem,
    WorkItemLink,
    WorkerJobLink,
    WorkerSessionLink,
)
from jarvis.storage import atomic_write_json
from jarvis.worker_session_contract import ACTIVE_SESSION_STATUSES


_RUN_ID = re.compile(r"^[A-Za-z0-9_-]+$")
# Canonical session-ref HMAC scheme (see make_session_ref below).
# cockpit.make_session_ref() delegates here.
SESSION_REF_PREFIX = "sessref_"
_SESSION_REF_SIGNING_CONTEXT = b"jarvis-cockpit-session-ref-v1"
_SESSION_REF_SIGNATURE_BYTES = 12
_STORE_CACHE_MAX_RUNS = 500
# In-process only: the cockpit API is currently the store's single writer.
# Cross-process writers must either notify this generation or rely on the SSE
# hub's periodic forced refresh to make their changes visible.
_STORE_GENERATIONS: dict[pathlib.Path, int] = {}


class ActiveWorkItemError(RuntimeError):
    def __init__(self, owner: OrchestrationRun) -> None:
        super().__init__(f"work item is already owned by {owner.run_id}")
        self.owner = owner


class ActiveWorkerJobError(RuntimeError):
    def __init__(self, job: WorkerJobLink) -> None:
        super().__init__(f"worker job {job.job_id} is already running")
        self.job = job


class ActiveWorkerSessionError(RuntimeError):
    def __init__(self, session: WorkerSessionLink) -> None:
        super().__init__(f"worker session {session.session_id} is already active")
        self.session = session


class RunArchivedError(RuntimeError):
    def __init__(self, run_id: str) -> None:
        super().__init__(f"run {run_id} is archived")
        self.run_id = run_id


class OrchestrationStore:
    """File-backed run graph store.

    The current run graph is JSON for easy inspection. Events are append-only JSONL
    so Jarvis can explain what happened even if later state changes.
    """

    def __init__(
        self,
        root: str,
        *,
        thread_child_terminal_notifier: Callable[[str, OrchestrationRun], bool] | None = None,
        thread_children_promoter: Callable[[str], object] | None = None,
    ) -> None:
        self.root = pathlib.Path(root).expanduser()
        self._generation_key = self.root.resolve()
        _STORE_GENERATIONS.setdefault(self._generation_key, 0)
        self.runs_dir = self.root / "runs"
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self._lock_path = self.root / ".lock"
        self._archived_sessions_path = self.root / "archived-sessions.json"
        self._session_refs_path = self.root / "session-refs.json"
        self._deleted_runs_path = self.root / "deleted-runs.json"
        self._deleted_sessions_path = self.root / "deleted-sessions.json"
        self._thread_child_terminal_notifier = thread_child_terminal_notifier
        self._thread_children_promoter = thread_children_promoter
        # Cache for deleted_session_refs(): (mtime_ns, refs). The SSE hub calls
        # cockpit_snapshot() every sse_refresh_interval_s (~1s) while any
        # client is connected, which was recomputing an HMAC session-ref per
        # tombstone on every tick even though deleted-sessions.json only
        # changes on an explicit delete.
        self._deleted_session_refs_cache: tuple[int | None, set[str]] | None = None
        # The SSE hub reads these files every refresh while a cockpit client is
        # connected. Keep parsed values here and use file metadata to pick up
        # writes made by other processes.
        self._run_cache: OrderedDict[pathlib.Path, tuple[int, int, OrchestrationRun]] = OrderedDict()
        self._events_cache: OrderedDict[pathlib.Path, JsonlCacheEntry[RunEvent]] = OrderedDict()
        self._cache_lock = threading.Lock()

    @property
    def generation(self) -> int:
        """Monotonic in-process signal for cockpit snapshot invalidation.

        This assumes the cockpit API process is the single writer. File caches
        remain correct for other writers, while the hub's forced refresh bounds
        how long an external write can remain unseen by SSE clients.
        """

        return _STORE_GENERATIONS[self._generation_key]

    def bump_generation(self) -> int:
        """Record a store-visible change made outside the normal write helpers."""

        _STORE_GENERATIONS[self._generation_key] = self.generation + 1
        return self.generation

    def run_dir(self, run_id: str) -> pathlib.Path:
        if not _RUN_ID.fullmatch(run_id):
            raise ValueError(f"invalid run id {run_id!r}")
        root = self.runs_dir.resolve()
        path = (self.runs_dir / run_id).resolve(strict=False)
        if not path.is_relative_to(root):
            raise ValueError(f"run id escapes orchestration store: {run_id!r}")
        return path

    def run_path(self, run_id: str) -> pathlib.Path:
        return self.run_dir(run_id) / "run.json"

    def events_path(self, run_id: str) -> pathlib.Path:
        return self.run_dir(run_id) / "events.jsonl"

    def create_run(
        self,
        objective: str,
        *,
        work_items: list[WorkItem] | None = None,
        parent_run_id: str | None = None,
        parent_chat_id: str | None = None,
        project_id: str = "",
        engine: str = "",
        model: str = "",
        provider_instance_id: str = "",
    ) -> OrchestrationRun:
        items = work_items or []
        parent_chat_id = parent_chat_id or parent_run_id
        with self._locked():
            if items:
                owner = self._active_primary_owner_unlocked(items[0])
                if owner is not None:
                    raise ActiveWorkItemError(owner)
            run = OrchestrationRun(
                run_id=new_id("run"),
                objective=objective,
                parent_chat_id=parent_chat_id,
                parent_run_id=parent_run_id,
                child_run_ids=[],
                project_id=project_id,
                engine=engine,
                model=model,
                provider_instance_id=provider_instance_id,
                work_items=[
                    WorkItemLink(item=item, role="primary" if i == 0 else "related")
                    for i, item in enumerate(items)
                ],
            )
            self.save(run)
            self.append_event(run.run_id, "run_created", f"Created run: {objective}")
            if parent_chat_id:
                parent = self.get(parent_chat_id)
                if parent and run.run_id not in parent.child_run_ids:
                    parent.child_run_ids.append(run.run_id)
                    if run.run_id not in parent.child_chat_ids:
                        parent.child_chat_ids.append(run.run_id)
                    parent.updated_at = utc_now()
                    self.save(parent)
        return run

    def save(self, run: OrchestrationRun) -> None:
        run.updated_at = utc_now()
        d = self.run_dir(run.run_id)
        d.mkdir(parents=True, exist_ok=True)
        path = self.run_path(run.run_id)
        path.write_text(json.dumps(run.to_dict(), indent=2, sort_keys=True))
        with self._cache_lock:
            self._run_cache.pop(path, None)
        self.bump_generation()

    def get(self, run_id: str) -> OrchestrationRun | None:
        try:
            p = self.run_path(run_id)
        except ValueError:
            return None
        if not p.exists():
            matches = [x for x in self.runs_dir.glob(f"{run_id}*") if (x / "run.json").exists()]
            if len(matches) == 1:
                p = matches[0] / "run.json"
            else:
                return None
        return OrchestrationRun.from_dict(json.loads(p.read_text()))

    def list_runs(self) -> list[OrchestrationRun]:
        runs: list[OrchestrationRun] = []
        seen: set[pathlib.Path] = set()
        for p in sorted(self.runs_dir.glob("*/run.json")):
            try:
                stat = p.stat()
                with self._cache_lock:
                    cached = self._run_cache.get(p)
                    if cached is not None and cached[:2] == (stat.st_size, stat.st_mtime_ns):
                        run = cached[2]
                        self._run_cache.move_to_end(p)
                    else:
                        run = None
                if run is None:
                    run = OrchestrationRun.from_dict(json.loads(p.read_text()))
                    self._cache_run(p, stat.st_size, stat.st_mtime_ns, run)
                runs.append(copy.deepcopy(run))
                seen.add(p)
            except (json.JSONDecodeError, OSError, KeyError):
                continue
        with self._cache_lock:
            for path in tuple(self._run_cache):
                if path not in seen:
                    self._run_cache.pop(path, None)
        return sorted(runs, key=lambda r: r.updated_at)

    def append_event(
        self,
        run_id: str,
        event_type: str,
        message: str = "",
        data: dict | None = None,
    ) -> RunEvent:
        self.run_dir(run_id).mkdir(parents=True, exist_ok=True)
        event = RunEvent(type=event_type, run_id=run_id, message=message, data=data or {})
        path = self.events_path(run_id)
        with path.open("a") as f:
            f.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")
        with self._cache_lock:
            self._events_cache.pop(path, None)
        self.bump_generation()
        return event

    def append_event_if_run_visible(
        self,
        run_id: str,
        event_type: str,
        message: str = "",
        data: dict | None = None,
    ) -> RunEvent | None:
        """Append only while the run still exists and has not been archived."""

        with self._locked():
            run = self.get(run_id)
            if run is None or run.archived_at:
                return None
            return self.append_event(run_id, event_type, message, data)

    def persist_worker_session_events(
        self,
        run_id: str,
        session_id: str,
        events: list[dict[str, Any]],
    ) -> int:
        """Atomically deduplicate and append worker events shared by sync and dispatch writers."""

        with self._locked():
            return self._persist_worker_session_events_unlocked(run_id, session_id, events)

    def events(self, run_id: str) -> list[RunEvent]:
        try:
            p = self.events_path(run_id)
        except ValueError:
            return []
        try:
            stat = p.stat()
        except OSError:
            with self._cache_lock:
                self._events_cache.pop(p, None)
            return []
        with self._cache_lock:
            cached = self._events_cache.get(p)
            if cached is not None:
                self._events_cache.move_to_end(p)
        try:
            fingerprint, offset, events = read_jsonl_projection(
                p,
                stat,
                cached,
                clone=copy.deepcopy,
                merge=self._merge_event_record,
            )
        except OSError:
            return []
        self._cache_events(p, *fingerprint, offset, events)
        return copy.deepcopy(events)

    def set_phase(self, run_id: str, phase: str, message: str = "") -> OrchestrationRun:
        run = self.get(run_id)
        if run is None:
            raise KeyError(run_id)
        run.phase = phase  # type: ignore[assignment]
        if phase in {"done", "completed", "failed", "blocked", "cancelled", "needs_human"}:
            run.status = "terminal"
            run.terminal_reason = message
        else:
            run.status = "active"
            run.terminal_reason = ""
        self.save(run)
        self.append_event(run_id, "phase_changed", message or f"Phase changed to {phase}", {"phase": phase})
        if run.status == "terminal":
            self.notify_parent_child_terminal(run)
        return run

    def set_phase_if_execution_unchanged(
        self,
        run_id: str,
        phase: str,
        message: str,
        *,
        expected_phase: str,
        expected_status: str,
        expected_jobs: tuple[tuple[str, str, str], ...],
        expected_sessions: tuple[tuple[str, str, str, str], ...],
    ) -> OrchestrationRun | None:
        """Atomically finalize a run only while its observed execution set is unchanged."""

        with self._locked():
            run = self.get(run_id)
            if run is None or run.archived_at:
                return None
            jobs = tuple((job.worker_id, job.job_id, job.status) for job in run.jobs)
            sessions = tuple(
                (session.worker_id, session.session_id, session.status, session.archived_at)
                for session in run.sessions
            )
            if (
                run.phase != expected_phase
                or run.status != expected_status
                or jobs != expected_jobs
                or sessions != expected_sessions
            ):
                return None
            run.phase = phase  # type: ignore[assignment]
            if phase in {"done", "completed", "failed", "blocked", "cancelled", "needs_human"}:
                run.status = "terminal"
                run.terminal_reason = message
            else:
                run.status = "active"
                run.terminal_reason = ""
            self.save(run)
            self.append_event(run_id, "phase_changed", message or f"Phase changed to {phase}", {"phase": phase})
        if run.status == "terminal":
            self.notify_parent_child_terminal(run)
        return run

    def archive_run(self, run_id: str) -> OrchestrationRun:
        with self._locked():
            run = self.get(run_id)
            if run is None:
                raise KeyError(run_id)
            already_archived = bool(run.archived_at)
            self._promote_children_unlocked(run.run_id)
            self._promote_thread_children(run.run_id)
            if not already_archived:
                # _promote_children_unlocked() re-gets and saves this SAME run
                # (to clear its own child_chat_ids/child_run_ids as a promoted
                # ex-parent), so the `run` object fetched above is now stale.
                # Re-fetch and touch only archived_at, so this save can't
                # clobber whatever promote just persisted.
                run = self.get(run_id) or run
                run.archived_at = utc_now()
                self.save(run)
                self.append_event(run_id, "run_archived", "Run archived from cockpit views")
            return run

    def promote_children(self, parent_chat_id: str) -> list[OrchestrationRun]:
        with self._locked():
            promoted = self._promote_children_unlocked(parent_chat_id)
        self._promote_thread_children(parent_chat_id)
        return promoted

    def rename_run(self, run_id: str, title: str) -> OrchestrationRun:
        title = " ".join(title.split())
        if not title:
            raise ValueError("title is required")
        with self._locked():
            run = self.get(run_id)
            if run is None:
                raise KeyError(run_id)
            if run.objective != title:
                run.objective = title
                self.save(run)
                self.append_event(run_id, "run_renamed", f"Renamed run to {title}", {"title": title})
            return run

    def notify_parent_child_terminal(
        self,
        child: OrchestrationRun,
        *,
        thread_child_terminal_notifier: Callable[[str, OrchestrationRun], bool] | None = None,
    ) -> None:
        parent_chat_id = child.parent_chat_id or child.parent_run_id or ""
        if not parent_chat_id:
            return
        parent = self.get(parent_chat_id)
        if parent is None:
            notifier = thread_child_terminal_notifier or self._thread_child_terminal_notifier
            if notifier is not None:
                with contextlib.suppress(Exception):
                    notifier(parent_chat_id, child)
            return
        existing = {
            (event.type, str(event.data.get("child_chat_id") or ""), str(event.data.get("phase") or ""))
            for event in self.events(parent.run_id)
            if isinstance(event.data, dict)
        }
        key = ("child_terminal", child.run_id, child.phase)
        if key in existing:
            return
        self.append_event(
            parent.run_id,
            "child_terminal",
            f"Child {child.run_id} reached {child.phase}",
            {
                "child_chat_id": child.run_id,
                "child_run_id": child.run_id,
                "title": child.objective,
                "phase": child.phase,
                "status": child.status,
                "terminal_reason": child.terminal_reason,
            },
        )

    def archive_session(self, run_id: str, session_id: str, *, worker_id: str) -> OrchestrationRun:
        with self._locked():
            run = self.get(run_id)
            if run is None:
                raise KeyError(run_id)
            session = next((x for x in run.sessions if x.worker_id == worker_id and x.session_id == session_id), None)
            if session is None:
                raise KeyError(session_id)
            if not session.archived_at:
                session.archived_at = utc_now()
                self.save(run)
                self.append_event(
                    run_id,
                    "session_archived",
                    f"Worker session {worker_id}/{session_id} archived from cockpit views",
                    {"worker_id": worker_id, "session_id": session_id},
                )
            return run

    def archive_cockpit_session(self, worker_id: str, session_id: str) -> OrchestrationRun | dict[str, str]:
        """Archive a session consistently across run links and worker-only indexes."""

        with self._locked():
            archived = self._archive_worker_session_unlocked(worker_id, session_id)
            for run in self.list_runs():
                session = next((x for x in run.sessions if x.worker_id == worker_id and x.session_id == session_id), None)
                if session is None:
                    continue
                if not session.archived_at:
                    session.archived_at = archived["archived_at"]
                    self.save(run)
                    self.append_event(
                        run.run_id,
                        "session_archived",
                        f"Worker session {worker_id}/{session_id} archived from cockpit views",
                        {"worker_id": worker_id, "session_id": session_id},
                    )
                return run
            return archived

    def close_cockpit_session(self, worker_id: str, session_id: str) -> OrchestrationRun | dict[str, str]:
        """Archive a session and detach the owning run from the active chat tree."""

        with self._locked():
            archived = self._archive_worker_session_unlocked(worker_id, session_id)
            for run in self.list_runs():
                session = next((x for x in run.sessions if x.worker_id == worker_id and x.session_id == session_id), None)
                if session is None:
                    continue
                if not session.archived_at:
                    session.archived_at = archived["archived_at"]
                    self.append_event(
                        run.run_id,
                        "session_archived",
                        f"Worker session {worker_id}/{session_id} archived from cockpit views",
                        {"worker_id": worker_id, "session_id": session_id},
                    )
                self._detach_from_parent_unlocked(run)
                self.save(run)
                self._promote_children_unlocked(run.run_id)
                self._promote_thread_children(run.run_id)
                return self.get(run.run_id) or run
            return archived

    def unarchive_cockpit_session(self, worker_id: str, session_id: str) -> OrchestrationRun | dict[str, str]:
        """Restore a session consistently across run links and worker-only indexes."""

        with self._locked():
            # An archived run hides all of its sessions regardless of the
            # session-level flag; without a run unarchive endpoint, clearing
            # the session flag would report success while the row stays
            # invisible. Refuse instead of lying.
            for run in self.list_runs():
                if run.archived_at and any(x.worker_id == worker_id and x.session_id == session_id for x in run.sessions):
                    raise RunArchivedError(run.run_id)
            was_archived = self._unarchive_worker_session_unlocked(worker_id, session_id)
            for run in self.list_runs():
                session = next((x for x in run.sessions if x.worker_id == worker_id and x.session_id == session_id), None)
                if session is None:
                    continue
                if session.archived_at:
                    session.archived_at = ""
                    self.save(run)
                    self.append_event(
                        run.run_id,
                        "session_unarchived",
                        f"Worker session {worker_id}/{session_id} restored to cockpit views",
                        {"worker_id": worker_id, "session_id": session_id},
                    )
                return run
            if not was_archived:
                raise KeyError(session_id)
            return {"worker_id": worker_id, "session_id": session_id, "archived_at": ""}

    def archive_worker_session(self, worker_id: str, session_id: str) -> dict[str, str]:
        with self._locked():
            return self._archive_worker_session_unlocked(worker_id, session_id)

    def archived_worker_sessions(self) -> dict[str, dict[str, str]]:
        if not self._archived_sessions_path.exists():
            return {}
        try:
            data = json.loads(self._archived_sessions_path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}
        result: dict[str, dict[str, str]] = {}
        if not isinstance(data, list):
            return result
        for raw in data:
            if not isinstance(raw, dict):
                continue
            worker_id = str(raw.get("worker_id") or "")
            session_id = str(raw.get("session_id") or "")
            if worker_id and session_id:
                result[f"{worker_id}\0{session_id}"] = {
                    "worker_id": worker_id,
                    "session_id": session_id,
                    "archived_at": str(raw.get("archived_at") or ""),
                }
        return result

    def record_session_refs(self, rows: list[dict[str, str]]) -> None:
        clean_rows: dict[str, dict[str, str]] = {}
        for row in rows:
            session_ref = str(row.get("session_ref") or "")
            worker_id = str(row.get("worker_id") or "")
            session_id = str(row.get("session_id") or "")
            if session_ref and worker_id and session_id:
                clean_rows[session_ref] = {"session_ref": session_ref, "worker_id": worker_id, "session_id": session_id}
        if not clean_rows:
            return
        with self._locked():
            index = self.session_ref_index()
            changed = False
            updated_at = utc_now()
            for session_ref, row in clean_rows.items():
                existing = index.get(session_ref)
                if (
                    existing is not None
                    and existing.get("worker_id") == row["worker_id"]
                    and existing.get("session_id") == row["session_id"]
                ):
                    continue
                index[session_ref] = {**row, "updated_at": updated_at}
                changed = True
            if not changed:
                return
            self._write_index(self._session_refs_path, list(index.values()))

    def session_ref_index(self) -> dict[str, dict[str, str]]:
        if not self._session_refs_path.exists():
            return {}
        try:
            data = json.loads(self._session_refs_path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}
        result: dict[str, dict[str, str]] = {}
        if not isinstance(data, list):
            return result
        for raw in data:
            if not isinstance(raw, dict):
                continue
            session_ref = str(raw.get("session_ref") or "")
            worker_id = str(raw.get("worker_id") or "")
            session_id = str(raw.get("session_id") or "")
            if session_ref and worker_id and session_id:
                result[session_ref] = {
                    "session_ref": session_ref,
                    "worker_id": worker_id,
                    "session_id": session_id,
                    "updated_at": str(raw.get("updated_at") or ""),
                }
        return result

    def resolve_session_ref(self, session_ref: str) -> dict[str, str] | None:
        return self.session_ref_index().get(session_ref)

    def deleted_run(self, run_id: str) -> dict[str, str] | None:
        return self._deleted_records(self._deleted_runs_path).get(run_id)

    def deleted_worker_session(self, worker_id: str, session_id: str) -> dict[str, str] | None:
        return self._deleted_records(self._deleted_sessions_path).get(f"{worker_id}\0{session_id}")

    def deleted_worker_sessions(self) -> dict[str, dict[str, str]]:
        return self._deleted_records(self._deleted_sessions_path)

    def deleted_session_refs(self) -> set[str]:
        """Session refs for tombstoned sessions, cached until the file changes.

        See the comment on _deleted_session_refs_cache in __init__ for why this
        is cached rather than recomputed on every snapshot.
        """
        try:
            mtime = self._deleted_sessions_path.stat().st_mtime_ns
        except OSError:
            mtime = None
        cached = self._deleted_session_refs_cache
        if cached is not None and cached[0] == mtime:
            return cached[1]
        refs = {
            make_session_ref(str(item.get("worker_id") or ""), str(item.get("session_id") or ""))
            for item in self.deleted_worker_sessions().values()
            if item.get("worker_id") and item.get("session_id")
        }
        self._deleted_session_refs_cache = (mtime, refs)
        return refs

    def link_work_item(self, run_id: str, item: WorkItem, role: str = "related") -> OrchestrationRun:
        run = self.get(run_id)
        if run is None:
            raise KeyError(run_id)
        run.work_items.append(WorkItemLink(item=item, role=role))
        self.save(run)
        self.append_event(run_id, "work_item_linked", item.title, {"source": item.source, "id": item.id, "role": role})
        return run

    def link_job(self, run_id: str, job: WorkerJobLink) -> OrchestrationRun:
        run = self.get(run_id)
        if run is None:
            raise KeyError(run_id)
        run.jobs.append(job)
        if run.phase in {"created", "claimed", "provisioned"}:
            run.phase = "running"
        self.save(run)
        self.append_event(run_id, "job_started", f"Worker job {job.job_id} started", job.to_dict())
        return run

    def link_session(self, run_id: str, session: WorkerSessionLink) -> OrchestrationRun:
        run = self.get(run_id)
        if run is None:
            raise KeyError(run_id)
        existing = next((x for x in run.sessions if x.worker_id == session.worker_id and x.session_id == session.session_id), None)
        if existing is None:
            run.sessions.append(session)
        else:
            _merge_session_link(existing, session)
        if run.phase in {"created", "claimed", "provisioned", "completed", "done", "failed", "blocked"}:
            run.phase = "running"
            run.status = "active"
            run.terminal_reason = ""
        if session.project_id and not run.project_id:
            run.project_id = session.project_id
        if session.engine and not run.engine:
            run.engine = session.engine
        self.save(run)
        self.append_event(run_id, "session_started", f"Worker session {session.session_id} started", session.to_dict())
        return run

    def reserve_session_if_idle(self, run_id: str, session: WorkerSessionLink) -> OrchestrationRun:
        with self._locked():
            run = self.get(run_id)
            if run is None:
                raise KeyError(run_id)
            active = next((x for x in run.sessions if x.status in ACTIVE_SESSION_STATUSES), None)
            if active is not None:
                raise ActiveWorkerSessionError(active)
            existing = next((x for x in run.sessions if x.worker_id == session.worker_id and x.session_id == session.session_id), None)
            if existing is None:
                run.sessions.append(session)
            else:
                _merge_session_link(existing, session)
            run.phase = "running"
            run.status = "active"
            run.terminal_reason = ""
            if session.project_id and not run.project_id:
                run.project_id = session.project_id
            if session.engine and not run.engine:
                run.engine = session.engine
            self.save(run)
            self.append_event(run_id, "session_reserved", f"Reserved worker session {session.session_id}", session.to_dict())
            return run

    def update_session(self, run_id: str, session_id: str, *, worker_id: str, **updates: str | list[str]) -> OrchestrationRun:
        with self._locked():
            run = self.get(run_id)
            if run is None:
                raise KeyError(run_id)
            session = next((x for x in run.sessions if x.worker_id == worker_id and x.session_id == session_id), None)
            if session is None:
                raise KeyError(session_id)
            changed: dict[str, str | list[str]] = {}
            for field in ("status", "ended_reason", "provider", "engine", "project_id", "branch", "cwd", "last_event_id", "allowed_actions"):
                value = updates.get(field)
                if value is None:
                    continue
                if getattr(session, field) != value:
                    setattr(session, field, value)
                    changed[field] = value
            if changed:
                self.save(run)
                self.append_event(
                    run_id,
                    "session_updated",
                    f"Worker session {session_id} updated",
                    {"session_id": session_id, **changed},
                )
            return run

    def update_session_if_unchanged(
        self,
        run_id: str,
        session_id: str,
        *,
        worker_id: str,
        expected: WorkerSessionLink,
        events: list[dict[str, Any]] | None = None,
        **updates: str | list[str],
    ) -> OrchestrationRun | None:
        """Apply a worker observation only if the linked session is still the observed version."""

        with self._locked():
            run = self.get(run_id)
            if run is None or run.archived_at or run.status == "terminal":
                return None
            session = next((x for x in run.sessions if x.worker_id == worker_id and x.session_id == session_id), None)
            if session is None or session.to_dict() != expected.to_dict():
                return None
            if events:
                self._persist_worker_session_events_unlocked(run_id, session_id, events)
            changed: dict[str, str | list[str]] = {}
            for field in (
                "status",
                "ended_reason",
                "provider",
                "engine",
                "project_id",
                "branch",
                "cwd",
                "last_event_id",
                "allowed_actions",
            ):
                value = updates.get(field)
                if value is None:
                    continue
                if getattr(session, field) != value:
                    setattr(session, field, value)
                    changed[field] = value
            if changed:
                self.save(run)
                self.append_event(
                    run_id,
                    "session_updated",
                    f"Worker session {session_id} updated",
                    {"session_id": session_id, **changed},
                )
            return run

    def _persist_worker_session_events_unlocked(
        self,
        run_id: str,
        session_id: str,
        events: list[dict[str, Any]],
    ) -> int:
        run = self.get(run_id)
        if run is None or run.archived_at:
            return 0
        existing = {
            str(event.data.get("event_id") or "")
            for event in self.events(run_id)
            if isinstance(event.data, dict) and event.data.get("event_id")
        }
        persisted = 0
        for raw in events:
            event_id = str(raw.get("event_id") or "")
            if event_id and event_id in existing:
                continue
            data = raw.get("data") if isinstance(raw.get("data"), dict) else {}
            self.append_event(
                run_id,
                str(raw.get("type") or "session.event"),
                "",
                {
                    "session_id": session_id,
                    "event_id": event_id,
                    "turn_id": str(data.get("turn_id") or ""),
                    "message_id": str(data.get("message_id") or ""),
                    "time": str(raw.get("time") or ""),
                    "data": dict(data),
                },
            )
            persisted += 1
            if event_id:
                existing.add(event_id)
        return persisted

    def reserve_job_if_idle(self, run_id: str, job: WorkerJobLink) -> OrchestrationRun:
        with self._locked():
            run = self.get(run_id)
            if run is None:
                raise KeyError(run_id)
            running = next((x for x in run.jobs if x.status == "running"), None)
            if running is not None:
                raise ActiveWorkerJobError(running)
            run.jobs.append(job)
            run.phase = "running"
            run.status = "active"
            run.terminal_reason = ""
            self.save(run)
            self.append_event(run_id, "job_reserved", f"Reserved worker job {job.job_id}", job.to_dict())
            return run

    def replace_job(self, run_id: str, old_job_id: str, job: WorkerJobLink) -> OrchestrationRun:
        with self._locked():
            run = self.get(run_id)
            if run is None:
                raise KeyError(run_id)
            for idx, existing in enumerate(run.jobs):
                if existing.job_id == old_job_id:
                    run.jobs[idx] = job
                    break
            else:
                raise KeyError(old_job_id)
            self.save(run)
            self.append_event(run_id, "job_started", f"Worker job {job.job_id} started", job.to_dict())
            return run

    def remove_job_link(self, run_id: str, job_id: str) -> OrchestrationRun:
        with self._locked():
            run = self.get(run_id)
            if run is None:
                raise KeyError(run_id)
            run.jobs = [job for job in run.jobs if job.job_id != job_id]
            self.save(run)
            self.append_event(run_id, "job_removed", f"Removed worker job link {job_id}", {"job_id": job_id})
            return run

    def delete_run(self, run_id: str) -> dict[str, int | str | bool]:
        with self._locked():
            run = self.get(run_id)
            if run is None:
                deleted = self.deleted_run(run_id)
                if deleted is not None:
                    return {"deleted": False, "run_id": run_id, "records": 0, "events": 0}
                raise KeyError(run_id)
            events = len(self.events(run.run_id))
            records = 1 + len(run.sessions) + len(run.jobs) + len(run.artifacts)
            directory = self.run_dir(run.run_id)
            shutil.rmtree(directory, ignore_errors=True)
            with self._cache_lock:
                self._run_cache.pop(self.run_path(run.run_id), None)
                self._events_cache.pop(self.events_path(run.run_id), None)
            self.bump_generation()
            self._record_deleted_run_unlocked(run.run_id)
            self._record_sessions_deleted_unlocked(run.sessions)
            return {"deleted": True, "run_id": run.run_id, "records": records, "events": events}

    def delete_cockpit_session(self, worker_id: str, session_id: str) -> dict[str, int | str | bool]:
        with self._locked():
            deleted = self.deleted_worker_session(worker_id, session_id)
            if deleted is not None:
                return {"deleted": False, "worker_id": worker_id, "session_id": session_id, "records": 0, "events": 0}
            records = 0
            for run in self.list_runs():
                before = len(run.sessions)
                run.sessions = [
                    session
                    for session in run.sessions
                    if not (session.worker_id == worker_id and session.session_id == session_id)
                ]
                if len(run.sessions) != before:
                    records += before - len(run.sessions)
                    self.save(run)
                    self.append_event(
                        run.run_id,
                        "session_deleted",
                        f"Worker session {worker_id}/{session_id} deleted from cockpit",
                        {"worker_id": worker_id, "session_id": session_id},
                    )
            archived = self.archived_worker_sessions()
            if archived.pop(f"{worker_id}\0{session_id}", None) is not None:
                records += 1
                self._write_index(self._archived_sessions_path, list(archived.values()))
            self._record_deleted_session_unlocked(worker_id, session_id)
            return {"deleted": True, "worker_id": worker_id, "session_id": session_id, "records": records, "events": 0}

    def update_job(self, run_id: str, job_id: str, **updates: str) -> OrchestrationRun:
        with self._locked():
            run = self.get(run_id)
            if run is None:
                raise KeyError(run_id)
            job = next((x for x in run.jobs if x.job_id == job_id), None)
            if job is None:
                raise KeyError(job_id)
            changed: dict[str, str] = {}
            for field in ("status", "session_id", "session_name", "branch", "cwd"):
                value = updates.get(field)
                if value is None:
                    continue
                if getattr(job, field) != value:
                    setattr(job, field, value)
                    changed[field] = value
            if changed:
                self.save(run)
                self.append_event(
                    run_id,
                    "job_updated",
                    f"Worker job {job_id} updated",
                    {"job_id": job_id, **changed},
                )
            return run

    def update_job_if_unchanged(
        self,
        run_id: str,
        job_id: str,
        *,
        expected: WorkerJobLink,
        **updates: str,
    ) -> OrchestrationRun | None:
        """Apply a worker observation only if the linked job is still the observed version."""

        with self._locked():
            run = self.get(run_id)
            if run is None or run.archived_at or run.status == "terminal":
                return None
            job = next((x for x in run.jobs if x.job_id == job_id), None)
            if job is None or job.to_dict() != expected.to_dict():
                return None
            changed: dict[str, str] = {}
            for field in ("status", "session_id", "session_name", "branch", "cwd"):
                value = updates.get(field)
                if value is None:
                    continue
                if getattr(job, field) != value:
                    setattr(job, field, value)
                    changed[field] = value
            if changed:
                self.save(run)
                self.append_event(
                    run_id,
                    "job_updated",
                    f"Worker job {job_id} updated",
                    {"job_id": job_id, **changed},
                )
            return run

    def link_artifact(self, run_id: str, artifact: Artifact) -> OrchestrationRun:
        run = self.get(run_id)
        if run is None:
            raise KeyError(run_id)
        run.artifacts.append(artifact)
        self.save(run)
        self.append_event(run_id, "artifact_created", artifact.url or artifact.name, artifact.to_dict())
        return run

    def active_primary_owner(self, item: WorkItem) -> OrchestrationRun | None:
        with self._locked():
            return self._active_primary_owner_unlocked(item)

    def _active_primary_owner_unlocked(self, item: WorkItem) -> OrchestrationRun | None:
        for run in self.list_runs():
            if run.status == "terminal":
                continue
            for link in run.work_items:
                if link.role == "primary" and _same_work_item(link.item, item):
                    return run
        return None

    def _promote_children_unlocked(self, parent_chat_id: str) -> list[OrchestrationRun]:
        promoted: list[OrchestrationRun] = []
        for child in self.list_runs():
            if child.parent_chat_id != parent_chat_id and child.parent_run_id != parent_chat_id:
                continue
            child.parent_chat_id = None
            child.parent_run_id = None
            self.save(child)
            self.append_event(
                child.run_id,
                "chat_reparented",
                "Parent chat was removed; chat promoted to root",
                {"previous_parent_chat_id": parent_chat_id, "parent_chat_id": None},
            )
            promoted.append(child)
        parent = self.get(parent_chat_id)
        if parent is not None and (parent.child_chat_ids or parent.child_run_ids):
            parent.child_chat_ids = []
            parent.child_run_ids = []
            self.save(parent)
        return promoted

    def _detach_from_parent_unlocked(self, run: OrchestrationRun) -> None:
        parent_chat_id = run.parent_chat_id or run.parent_run_id or ""
        if not parent_chat_id:
            return
        parent = self.get(parent_chat_id)
        if parent is not None:
            parent.child_chat_ids = [child_id for child_id in parent.child_chat_ids if child_id != run.run_id]
            parent.child_run_ids = [child_id for child_id in parent.child_run_ids if child_id != run.run_id]
            self.save(parent)
        run.parent_chat_id = None
        run.parent_run_id = None

    def _promote_thread_children(self, parent_chat_id: str) -> None:
        if self._thread_children_promoter is not None:
            with contextlib.suppress(Exception):
                self._thread_children_promoter(parent_chat_id)

    def _cache_run(self, path: pathlib.Path, size: int, mtime_ns: int, run: OrchestrationRun) -> None:
        with self._cache_lock:
            self._run_cache[path] = (size, mtime_ns, run)
            self._run_cache.move_to_end(path)
            while len(self._run_cache) > _STORE_CACHE_MAX_RUNS:
                self._run_cache.popitem(last=False)

    def _cache_events(
        self,
        path: pathlib.Path,
        size: int,
        mtime_ns: int,
        device: int,
        inode: int,
        offset: int,
        events: list[RunEvent],
    ) -> None:
        with self._cache_lock:
            self._events_cache[path] = (size, mtime_ns, device, inode, offset, copy.deepcopy(events))
            self._events_cache.move_to_end(path)
            while len(self._events_cache) > _STORE_CACHE_MAX_RUNS:
                self._events_cache.popitem(last=False)

    @staticmethod
    def _merge_event_record(events: list[RunEvent], record: object) -> None:
        if isinstance(record, dict):
            events.append(RunEvent.from_dict(record))

    def _write_index(self, path: pathlib.Path, data: object) -> None:
        atomic_write_json(path, data)
        self.bump_generation()

    @contextlib.contextmanager
    def _locked(self):
        self.root.mkdir(parents=True, exist_ok=True)
        with self._lock_path.open("a+") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def _archive_worker_session_unlocked(self, worker_id: str, session_id: str) -> dict[str, str]:
        archived = self.archived_worker_sessions()
        key = f"{worker_id}\0{session_id}"
        existing = archived.get(key)
        if existing is not None:
            return existing
        item = {"worker_id": worker_id, "session_id": session_id, "archived_at": utc_now()}
        archived[key] = item
        self._write_index(self._archived_sessions_path, list(archived.values()))
        return item

    def _unarchive_worker_session_unlocked(self, worker_id: str, session_id: str) -> bool:
        archived = self.archived_worker_sessions()
        if archived.pop(f"{worker_id}\0{session_id}", None) is None:
            return False
        self._write_index(self._archived_sessions_path, list(archived.values()))
        return True
    def _deleted_records(self, path: pathlib.Path) -> dict[str, dict[str, str]]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            return {}
        result: dict[str, dict[str, str]] = {}
        if not isinstance(data, list):
            return result
        for raw in data:
            if not isinstance(raw, dict):
                continue
            key = str(raw.get("key") or "")
            if key:
                result[key] = {str(k): str(v) for k, v in raw.items()}
        return result

    def _record_deleted_run_unlocked(self, run_id: str) -> None:
        records = self._deleted_records(self._deleted_runs_path)
        records[run_id] = {"key": run_id, "run_id": run_id, "deleted_at": utc_now()}
        self._write_index(self._deleted_runs_path, list(records.values()))

    def _record_sessions_deleted_unlocked(self, sessions: list[WorkerSessionLink]) -> None:
        """Record session-refs + tombstones for every session on a deleted run.

        Reads/writes each index file once regardless of session count, instead
        of the per-session read+rewrite that _record_session_ref_unlocked and
        _record_deleted_session_unlocked do individually — deleting a run with
        many sessions used to be O(#sessions) full-file rewrites of both
        session-refs.json and deleted-sessions.json under the store lock.
        """

        if not sessions:
            return
        refs = self.session_ref_index()
        refs_changed = False
        updated_at = utc_now()
        for session in sessions:
            session_ref = make_session_ref(session.worker_id, session.session_id)
            existing = refs.get(session_ref)
            if (
                existing is not None
                and existing.get("worker_id") == session.worker_id
                and existing.get("session_id") == session.session_id
            ):
                continue
            refs[session_ref] = {
                "session_ref": session_ref,
                "worker_id": session.worker_id,
                "session_id": session.session_id,
                "updated_at": updated_at,
            }
            refs_changed = True
        if refs_changed:
            self._write_index(self._session_refs_path, list(refs.values()))

        deleted = self._deleted_records(self._deleted_sessions_path)
        deleted_at = utc_now()
        for session in sessions:
            key = f"{session.worker_id}\0{session.session_id}"
            deleted[key] = {
                "key": key,
                "worker_id": session.worker_id,
                "session_id": session.session_id,
                "deleted_at": deleted_at,
            }
        self._write_index(self._deleted_sessions_path, list(deleted.values()))

    def _record_session_ref_unlocked(self, worker_id: str, session_id: str) -> None:
        records = self.session_ref_index()
        session_ref = make_session_ref(worker_id, session_id)
        existing = records.get(session_ref)
        if (
            existing is not None
            and existing.get("worker_id") == worker_id
            and existing.get("session_id") == session_id
        ):
            return
        records[session_ref] = {
            "session_ref": session_ref,
            "worker_id": worker_id,
            "session_id": session_id,
            "updated_at": utc_now(),
        }
        self._write_index(self._session_refs_path, list(records.values()))

    def _record_deleted_session_unlocked(self, worker_id: str, session_id: str) -> None:
        records = self._deleted_records(self._deleted_sessions_path)
        key = f"{worker_id}\0{session_id}"
        records[key] = {
            "key": key,
            "worker_id": worker_id,
            "session_id": session_id,
            "deleted_at": utc_now(),
        }
        self._write_index(self._deleted_sessions_path, list(records.values()))


def _merge_session_link(existing: WorkerSessionLink, session: WorkerSessionLink) -> None:
    """Apply an updated session link's fields onto an already-linked one."""

    existing.status = session.status
    existing.provider = session.provider
    existing.engine = session.engine
    existing.project_id = session.project_id or existing.project_id
    existing.branch = session.branch
    existing.cwd = session.cwd
    existing.last_event_id = session.last_event_id
    existing.allowed_actions = list(session.allowed_actions)


def _same_work_item(left: WorkItem, right: WorkItem) -> bool:
    if left.source != right.source:
        return False
    if left.repo and right.repo and left.repo != right.repo:
        return False
    left_id = left.source_internal_id or left.id
    right_id = right.source_internal_id or right.id
    return left_id == right_id


def make_session_ref(worker_id: str, session_id: str) -> str:
    # Canonical session-ref HMAC scheme; cockpit.make_session_ref() delegates here.
    raw = f"{worker_id}\0{session_id}".encode("utf-8")
    digest = hmac.new(_SESSION_REF_SIGNING_CONTEXT, _SESSION_REF_SIGNING_CONTEXT + b"\0" + raw, hashlib.sha256).digest()
    token = base64.urlsafe_b64encode(digest[:_SESSION_REF_SIGNATURE_BYTES]).decode("ascii").rstrip("=")
    return f"{SESSION_REF_PREFIX}{token}"
