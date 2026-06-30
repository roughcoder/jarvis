from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from jarvis.engines import normalize_engine_id, worker_supports_engine
from jarvis.orchestration import executor
from jarvis.orchestration.authority import allowed
from jarvis.orchestration.models import ExecutionEnvelope, WorkCommand, WorkItem, WorkerJobLink
from jarvis.orchestration.policy import required_for_command, required_for_worker_dispatch
from jarvis.orchestration.sources import WorkSource
from jarvis.orchestration.store import ActiveWorkItemError, OrchestrationStore
from jarvis.orchestration.workers import WorkerProfile, WorkerRegistry


class SourceFactory(Protocol):
    def __call__(self, name: str, cfg: Any = None) -> WorkSource: ...


class MissingAuthorityError(RuntimeError):
    def __init__(self, actions: list[str]) -> None:
        self.actions = actions
        super().__init__(", ".join(actions))


class WorkAlreadyOwnedError(RuntimeError):
    def __init__(self, item: WorkItem, owner: Any) -> None:
        self.item = item
        self.owner = owner
        super().__init__(f"{item.source}:{item.id} is already owned by {owner.run_id}")


class NoEligibleWorkerError(RuntimeError):
    pass


class WorkerDispatchError(RuntimeError):
    def __init__(self, run_id: str, cause: Exception) -> None:
        self.run_id = run_id
        self.cause = cause
        super().__init__(str(cause))


class MissingWorkRepoError(RuntimeError):
    def __init__(self, item: WorkItem, run_id: str) -> None:
        self.item = item
        self.run_id = run_id
        super().__init__("work item has no repo/default repo; cannot start a coding worker")


@dataclass
class StartedWork:
    item: WorkItem
    worker: WorkerProfile
    envelope: ExecutionEnvelope
    job: WorkerJobLink


class OrchestrationService:
    def __init__(
        self,
        *,
        cfg: Any,
        capabilities: set[str],
        source_factory: SourceFactory,
    ) -> None:
        self.cfg = cfg
        self.capabilities = capabilities
        self.source_factory = source_factory

    def check_work(self, command: WorkCommand, *, limit: int = 10) -> list[WorkItem]:
        self._require(required_for_command(command.operation, command.source))
        source = self.source_factory(command.source, self.cfg)
        return source.list(repo=self._repo(command), filters=command.filters, limit=limit)

    def inspect_pr_comments(self, command: WorkCommand, *, number: int) -> list[dict[str, Any]]:
        if command.source != "github":
            raise ValueError("PR comments are currently a GitHub work source operation.")
        self._require(required_for_command(command.operation, command.source))
        source = self.source_factory(command.source, self.cfg)
        return source.pr_comments(self._repo(command), number)  # type: ignore[attr-defined]

    def next_work(self, command: WorkCommand, *, start: bool = False) -> WorkItem | StartedWork | None:
        self._require(required_for_command(command.operation, command.source))
        source = self.source_factory(command.source, self.cfg)
        item = source.next(repo=self._repo(command), filters=command.filters)
        if item is None:
            return None

        store = OrchestrationStore(self.cfg.orchestration.workspace)
        repo = self._repo(command)
        if repo and not item.repo:
            item.repo = repo
        existing = store.active_primary_owner(item)
        if existing:
            raise WorkAlreadyOwnedError(item, existing)
        if not start:
            return item

        self._require(required_for_worker_dispatch(self.cfg.orchestration.landing_mode))
        if not item.repo:
            run = store.create_run(str(item.title), work_items=[item])
            store.set_phase(run.run_id, "needs_human", "Work item has no repo/default repo; cannot start a coding worker")
            raise MissingWorkRepoError(item, run.run_id)
        registry = WorkerRegistry(self.cfg.worker, profiles_path=self.cfg.orchestration.workers_path)
        target_engine = normalize_engine_id(command.target_engine_id)
        if command.target_worker_id:
            worker = registry.get(command.target_worker_id, probe=True)
        elif target_engine:
            worker = registry.choose(item.capability_requirements, engine=target_engine)
        else:
            worker = registry.choose(item.capability_requirements)
        if worker is None or not _worker_is_eligible(worker, item.capability_requirements, engine=target_engine):
            raise NoEligibleWorkerError("No eligible worker found.")
        engine = target_engine or worker.default_engine or worker.agent

        try:
            envelope = executor.create_run_and_envelope(
                store=store,
                command=command,
                items=[item],
                worker=worker,
                landing_mode=self.cfg.orchestration.landing_mode,
                engine=engine,
            )
        except ActiveWorkItemError as exc:
            raise WorkAlreadyOwnedError(item, exc.owner) from exc

        try:
            job = executor.start_worker_job(envelope, worker_cfg=self.cfg.worker, worker=worker, store=store)
        except Exception as exc:  # noqa: BLE001 - dispatch failure must release the local claim
            store.set_phase(envelope.run_id, "failed", f"Worker dispatch failed: {exc}")
            raise WorkerDispatchError(envelope.run_id, exc) from exc

        return StartedWork(item=item, worker=worker, envelope=envelope, job=job)

    def _require(self, actions: list[str]) -> None:
        denied = [
            action
            for action in actions
            if not allowed(
                action,
                self.capabilities,
                public_write_mode=self.cfg.orchestration.landing_mode,
            )
        ]
        if denied:
            raise MissingAuthorityError(denied)

    def _repo(self, command: WorkCommand) -> str:
        return str(command.filters.get("repo") or self.cfg.orchestration.default_repo)


def _worker_is_eligible(worker: WorkerProfile, required: list[str] | None = None, *, engine: str = "") -> bool:
    if worker.status == "offline":
        return False
    if worker.current_jobs >= worker.max_concurrent_jobs:
        return False
    if engine and not worker_supports_engine(worker.supported_engines, engine):
        return False
    return set(required or []).issubset(set(worker.capabilities))
