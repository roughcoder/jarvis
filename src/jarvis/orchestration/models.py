from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from typing import Any, Literal

from jarvis.capabilities import WORKER_SESSION_CREATE, WORKER_SESSION_TURN
from jarvis.engines import default_engine, engine_ids
from jarvis.ids import new_id, utc_now


Phase = Literal[
    "created",
    "claimed",
    "provisioned",
    "running",
    "verifying",
    "landing",
    "handoff",
    "done",
    "completed",
    "blocked",
    "stalled",
    "failed",
    "cancelled",
    "needs_human",
]


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
    session_name: str = ""
    branch: str = ""
    cwd: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerJobLink:
        return _coerce(cls, data)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class WorkerSessionLink:
    worker_id: str
    session_id: str
    status: str = "created"
    provider: str = "codex"
    engine: str = "codex"
    branch: str = ""
    cwd: str = ""
    last_event_id: str = ""
    archived_at: str = ""
    allowed_actions: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerSessionLink:
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
    summary: str = ""
    command: str = ""
    started_at: str = ""
    completed_at: str = ""

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
    dispatch_id: str = field(default_factory=lambda: new_id("dispatch"))
    worker_id: str = "local-worker"
    engine: str = "codex"
    engine_strategy: str = "single"
    base_ref: str = "main"
    branch_name: str = ""
    cwd: str = ""
    session_id: str = ""
    session_name: str = ""
    resume_session: bool = False
    allowed_actions: list[str] = field(default_factory=lambda: [WORKER_SESSION_CREATE, WORKER_SESSION_TURN])
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
    sessions: list[WorkerSessionLink] = field(default_factory=list)
    artifacts: list[Artifact] = field(default_factory=list)
    created_at: str = field(default_factory=utc_now)
    updated_at: str = field(default_factory=utc_now)
    terminal_reason: str = ""
    archived_at: str = ""

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
            sessions=[WorkerSessionLink.from_dict(x) for x in data.get("sessions", [])],
            artifacts=[Artifact.from_dict(x) for x in data.get("artifacts", [])],
            created_at=data.get("created_at", utc_now()),
            updated_at=data.get("updated_at", utc_now()),
            terminal_reason=data.get("terminal_reason", ""),
            archived_at=data.get("archived_at", ""),
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
            "sessions": [x.to_dict() for x in self.sessions],
            "artifacts": [x.to_dict() for x in self.artifacts],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "terminal_reason": self.terminal_reason,
            "archived_at": self.archived_at,
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
    target_engine_id: str = ""
    engine_strategy: str = "single"
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
    default_engine: str = ""
    default_repo: str = ""
    last_seen_at: str = ""
    supported_engines: list[str] = field(default_factory=list)
    engine_supports: dict[str, dict[str, bool]] = field(default_factory=dict)
    system: dict[str, Any] = field(default_factory=dict)
    repositories: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.default_engine = default_engine(self.default_engine or self.agent, self.supported_engines)
        self.supported_engines = engine_ids(self.supported_engines, default_engine=self.default_engine)
        self.agent = self.default_engine
        if not isinstance(self.system, dict):
            self.system = {}
        if not isinstance(self.repositories, list):
            self.repositories = []
        self.repositories = [dict(item) for item in self.repositories if isinstance(item, dict) and (item.get("repo") or item.get("name"))]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerProfile:
        if "supported_engines" in data and isinstance(data["supported_engines"], str):
            data = dict(data)
            data["supported_engines"] = engine_ids(
                data["supported_engines"],
                default_engine=data.get("default_engine") or data.get("agent", "codex"),
            )
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
            "default_engine": self.default_engine,
            "supported_engines": self.supported_engines,
            "engine_supports": self.engine_supports,
            "system": self.system,
            "token_set": self.token_set,
        }
