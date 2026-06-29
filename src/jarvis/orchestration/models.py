from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from typing import Any, Literal


Phase = Literal[
    "created",
    "claimed",
    "provisioned",
    "running",
    "verifying",
    "landing",
    "handoff",
    "done",
    "blocked",
    "stalled",
    "failed",
    "cancelled",
    "needs_human",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def new_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time())}_{uuid.uuid4().hex[:8]}"


def _coerce(cls: type, data: dict[str, Any]):
    names = {f.name for f in fields(cls)}
    return cls(**{k: v for k, v in data.items() if k in names})


@dataclass
class WorkItem:
    source: str
    id: str
    title: str
    url: str = ""
    body: str = ""
    repo: str = ""
    kind: str = "issue"
    source_internal_id: str = ""
    status: str = ""
    priority: str = ""
    labels: list[str] = field(default_factory=list)
    assignee: str = ""
    updated_at: str = ""
    acceptance_criteria: list[str] = field(default_factory=list)
    capability_requirements: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkItem:
        return _coerce(cls, data)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WorkItemLink:
    item: WorkItem
    role: str = "primary"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkItemLink:
        return cls(item=WorkItem.from_dict(data["item"]), role=data.get("role", "primary"))

    def to_dict(self) -> dict[str, Any]:
        return {"role": self.role, "item": self.item.to_dict()}


@dataclass
class WorkerJobLink:
    worker_id: str
    job_id: str
    status: str = "running"
    engine: str = "codex"
    session_id: str = ""
    branch: str = ""
    cwd: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerJobLink:
        return _coerce(cls, data)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Artifact:
    type: str
    id: str = ""
    url: str = ""
    name: str = ""
    status: str = ""
    public: bool = True

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Artifact:
        return _coerce(cls, data)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class VerificationPlan:
    minimum_rung: str = "repo_native"
    repo_native: bool = True
    task_proof: str = "Follow the repository's own checks and report evidence."
    suggested_commands: list[str] = field(default_factory=list)
    evidence_required: list[str] = field(
        default_factory=lambda: [
            "repo checks followed",
            "commands run",
            "observed behavior",
            "known gaps",
        ]
    )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> VerificationPlan:
        return _coerce(cls, data)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LandingPolicy:
    mode: str = "draft_pr"
    public_write_mode: str = "draft_then_confirm"
    allow_comments: str = "confirm"
    allow_merge: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LandingPolicy:
        return _coerce(cls, data)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ExecutionEnvelope:
    run_id: str
    repo: str
    prompt: str
    worker_id: str = "local-worker"
    engine: str = "codex"
    base_ref: str = "main"
    branch_name: str = ""
    allowed_actions: list[str] = field(default_factory=lambda: ["worker.job.start"])
    verification: VerificationPlan = field(default_factory=VerificationPlan)
    landing: LandingPolicy = field(default_factory=LandingPolicy)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExecutionEnvelope:
        data = dict(data)
        data["verification"] = VerificationPlan.from_dict(data.get("verification", {}))
        data["landing"] = LandingPolicy.from_dict(data.get("landing", {}))
        return _coerce(cls, data)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["verification"] = self.verification.to_dict()
        d["landing"] = self.landing.to_dict()
        return d


@dataclass
class OrchestrationRun:
    run_id: str
    objective: str
    phase: Phase = "created"
    status: str = "active"
    parent_run_id: str | None = None
    child_run_ids: list[str] = field(default_factory=list)
    work_items: list[WorkItemLink] = field(default_factory=list)
    jobs: list[WorkerJobLink] = field(default_factory=list)
    artifacts: list[Artifact] = field(default_factory=list)
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    terminal_reason: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> OrchestrationRun:
        return cls(
            run_id=data["run_id"],
            objective=data.get("objective", ""),
            phase=data.get("phase", "created"),
            status=data.get("status", "active"),
            parent_run_id=data.get("parent_run_id"),
            child_run_ids=list(data.get("child_run_ids", [])),
            work_items=[WorkItemLink.from_dict(x) for x in data.get("work_items", [])],
            jobs=[WorkerJobLink.from_dict(x) for x in data.get("jobs", [])],
            artifacts=[Artifact.from_dict(x) for x in data.get("artifacts", [])],
            created_at=data.get("created_at", utc_now()),
            updated_at=data.get("updated_at", utc_now()),
            terminal_reason=data.get("terminal_reason", ""),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "objective": self.objective,
            "phase": self.phase,
            "status": self.status,
            "parent_run_id": self.parent_run_id,
            "child_run_ids": self.child_run_ids,
            "work_items": [x.to_dict() for x in self.work_items],
            "jobs": [x.to_dict() for x in self.jobs],
            "artifacts": [x.to_dict() for x in self.artifacts],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "terminal_reason": self.terminal_reason,
        }


@dataclass
class RunEvent:
    type: str
    run_id: str
    message: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    time: str = field(default_factory=utc_now)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunEvent:
        return cls(
            type=data.get("type", ""),
            run_id=data.get("run_id", ""),
            message=data.get("message", ""),
            data=dict(data.get("data", {})),
            time=data.get("time", utc_now()),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WorkCommand:
    operation: str
    source: str = "direct"
    kind: str = ""
    filters: dict[str, Any] = field(default_factory=dict)
    autonomy: str = "read_only"
    target_worker_id: str = ""
    start: bool = False

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkCommand:
        return _coerce(cls, data)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WorkerProfile:
    worker_id: str
    display_name: str
    capabilities: list[str] = field(default_factory=list)
    base_url: str = ""
    token_env: str = ""
    token_set: bool = False
    max_concurrent_jobs: int = 1
    current_jobs: int = 0
    status: str = "unknown"
    agent: str = "codex"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerProfile:
        return _coerce(cls, data)

    def public(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "display_name": self.display_name,
            "capabilities": self.capabilities,
            "status": self.status,
            "capacity": {
                "max_concurrent_jobs": self.max_concurrent_jobs,
                "current_jobs": self.current_jobs,
            },
            "agent": self.agent,
            "token_set": self.token_set,
        }
