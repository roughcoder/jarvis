from __future__ import annotations

import json
import pathlib

from jarvis.orchestration.models import (
    Artifact,
    OrchestrationRun,
    RunEvent,
    WorkItem,
    WorkItemLink,
    WorkerJobLink,
    new_id,
    utc_now,
)


class OrchestrationStore:
    """File-backed run graph store.

    The current run graph is JSON for easy inspection. Events are append-only JSONL
    so Jarvis can explain what happened even if later state changes.
    """

    def __init__(self, root: str) -> None:
        self.root = pathlib.Path(root).expanduser()
        self.runs_dir = self.root / "runs"
        self.runs_dir.mkdir(parents=True, exist_ok=True)

    def run_dir(self, run_id: str) -> pathlib.Path:
        return self.runs_dir / run_id

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
    ) -> OrchestrationRun:
        run = OrchestrationRun(
            run_id=new_id("run"),
            objective=objective,
            parent_run_id=parent_run_id,
            work_items=[
                WorkItemLink(item=item, role="primary" if i == 0 else "related")
                for i, item in enumerate(work_items or [])
            ],
        )
        self.save(run)
        self.append_event(run.run_id, "run_created", f"Created run: {objective}")
        if parent_run_id:
            parent = self.get(parent_run_id)
            if parent and run.run_id not in parent.child_run_ids:
                parent.child_run_ids.append(run.run_id)
                parent.updated_at = utc_now()
                self.save(parent)
        return run

    def save(self, run: OrchestrationRun) -> None:
        run.updated_at = utc_now()
        d = self.run_dir(run.run_id)
        d.mkdir(parents=True, exist_ok=True)
        self.run_path(run.run_id).write_text(json.dumps(run.to_dict(), indent=2, sort_keys=True))

    def get(self, run_id: str) -> OrchestrationRun | None:
        p = self.run_path(run_id)
        if not p.exists():
            matches = [x for x in self.runs_dir.glob(f"{run_id}*") if (x / "run.json").exists()]
            if len(matches) == 1:
                p = matches[0] / "run.json"
            else:
                return None
        return OrchestrationRun.from_dict(json.loads(p.read_text()))

    def list_runs(self) -> list[OrchestrationRun]:
        runs: list[OrchestrationRun] = []
        for p in sorted(self.runs_dir.glob("*/run.json")):
            try:
                runs.append(OrchestrationRun.from_dict(json.loads(p.read_text())))
            except (json.JSONDecodeError, OSError, KeyError):
                continue
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
        with self.events_path(run_id).open("a") as f:
            f.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")
        return event

    def events(self, run_id: str) -> list[RunEvent]:
        p = self.events_path(run_id)
        if not p.exists():
            return []
        events: list[RunEvent] = []
        for line in p.read_text().splitlines():
            if not line.strip():
                continue
            try:
                events.append(RunEvent.from_dict(json.loads(line)))
            except json.JSONDecodeError:
                continue
        return events

    def set_phase(self, run_id: str, phase: str, message: str = "") -> OrchestrationRun:
        run = self.get(run_id)
        if run is None:
            raise KeyError(run_id)
        run.phase = phase  # type: ignore[assignment]
        if phase in {"done", "failed", "blocked", "cancelled", "needs_human"}:
            run.status = "terminal"
            run.terminal_reason = message
        self.save(run)
        self.append_event(run_id, "phase_changed", message or f"Phase changed to {phase}", {"phase": phase})
        return run

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

    def link_artifact(self, run_id: str, artifact: Artifact) -> OrchestrationRun:
        run = self.get(run_id)
        if run is None:
            raise KeyError(run_id)
        run.artifacts.append(artifact)
        self.save(run)
        self.append_event(run_id, "artifact_created", artifact.url or artifact.name, artifact.to_dict())
        return run

    def active_primary_owner(self, item: WorkItem) -> OrchestrationRun | None:
        for run in self.list_runs():
            if run.status == "terminal":
                continue
            for link in run.work_items:
                if link.role == "primary" and _same_work_item(link.item, item):
                    return run
        return None


def _same_work_item(left: WorkItem, right: WorkItem) -> bool:
    if left.source != right.source:
        return False
    if left.repo and right.repo and left.repo != right.repo:
        return False
    left_id = left.source_internal_id or left.id
    right_id = right.source_internal_id or right.id
    return left_id == right_id
