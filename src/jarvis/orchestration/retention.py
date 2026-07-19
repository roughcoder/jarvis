"""Conversation retention policy for the orchestration store.

The store grows without bound: every cockpit chat keeps a thread record, a JSONL
transcript, and — for conversations that spawned work — child run directories and
worker worktrees. Archive only hides them, so nothing ever reclaims the disk.

This module owns the *decision* (which conversations are dead, and why the rest
are kept) as a pure function over the thread index and the run graph. The
*deletion* is injected: callers pass the existing lifecycle delete callables so
retention can never become a second delete path that drifts from the real one.

Classes and their TTLs are independent, and a TTL of 0 disables its class
outright — the same refusal stance the worker takes for `WORKER_WORKTREE_GC`,
where an unbounded threshold is a sane answer for an explicit sweep but a
foot-gun on a timer.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jarvis.orchestration.models import OrchestrationRun
from jarvis.worker_session_contract import ACTIVE_SESSION_STATUSES

# Conversation classes, in the order they are reported.
CLASS_ARCHIVED = "archived"
CLASS_CHAT = "chat"
CLASS_TREE = "tree"
RETENTION_CLASSES = (CLASS_ARCHIVED, CLASS_CHAT, CLASS_TREE)

# A conversation whose workspace sits in one of these states has a turn in
# flight (or an interrupt landing). Anything else is at rest.
LIVE_WORKSPACE_STATUSES = frozenset({"starting", "running", "interrupting", "waiting_input", "waiting_approval"})

# Runs are terminal when the store says so; everything else is live work.
TERMINAL_RUN_STATUSES = frozenset({"terminal"})

_DAY_S = 24 * 60 * 60


@dataclass(frozen=True)
class RetentionPolicy:
    """Resolved retention knobs. TTL of 0 (or less) disables that class."""

    enabled: bool = True
    interval_s: float = 6 * 60 * 60
    archived_ttl_days: float = 14.0
    chat_ttl_days: float = 7.0
    tree_ttl_days: float = 7.0

    @classmethod
    def from_config(cls, orchestration: Any) -> RetentionPolicy:
        return cls(
            enabled=bool(orchestration.retention_enabled),
            interval_s=float(orchestration.retention_interval_s),
            archived_ttl_days=float(orchestration.retention_archived_ttl_days),
            chat_ttl_days=float(orchestration.retention_chat_ttl_days),
            tree_ttl_days=float(orchestration.retention_tree_ttl_days),
        )

    def ttl_days(self, klass: str) -> float:
        return {
            CLASS_ARCHIVED: self.archived_ttl_days,
            CLASS_CHAT: self.chat_ttl_days,
            CLASS_TREE: self.tree_ttl_days,
        }.get(klass, 0.0)

    def class_enabled(self, klass: str) -> bool:
        return self.ttl_days(klass) > 0

    @property
    def any_class_enabled(self) -> bool:
        return any(self.class_enabled(klass) for klass in RETENTION_CLASSES)


@dataclass(frozen=True)
class RetentionCandidate:
    project_id: str
    thread_id: str
    klass: str
    title: str = ""
    age_days: float = 0.0
    child_run_ids: tuple[str, ...] = ()
    bytes_estimate: int = 0


@dataclass(frozen=True)
class RetentionKeep:
    project_id: str
    thread_id: str
    klass: str
    reason: str


@dataclass(frozen=True)
class RetentionPlan:
    candidates: tuple[RetentionCandidate, ...] = ()
    kept: tuple[RetentionKeep, ...] = ()

    def counts(self) -> dict[str, int]:
        counts = dict.fromkeys(RETENTION_CLASSES, 0)
        for candidate in self.candidates:
            counts[candidate.klass] = counts.get(candidate.klass, 0) + 1
        return counts

    def bytes_by_class(self) -> dict[str, int]:
        totals = dict.fromkeys(RETENTION_CLASSES, 0)
        for candidate in self.candidates:
            totals[candidate.klass] = totals.get(candidate.klass, 0) + candidate.bytes_estimate
        return totals

    @property
    def total_bytes(self) -> int:
        return sum(candidate.bytes_estimate for candidate in self.candidates)


@dataclass
class RetentionResult:
    deleted: dict[str, int] = field(default_factory=lambda: dict.fromkeys(RETENTION_CLASSES, 0))
    child_runs: int = 0
    bytes_reclaimed: int = 0
    kept: int = 0
    failures: list[str] = field(default_factory=list)

    def summary_line(self) -> str:
        classes = ", ".join(f"{klass}={self.deleted.get(klass, 0)}" for klass in RETENTION_CLASSES)
        line = (
            f"[cockpit] conversation retention: deleted {sum(self.deleted.values())} "
            f"({classes}), child runs {self.child_runs}, "
            f"{human_bytes(self.bytes_reclaimed)} reclaimed, kept {self.kept}"
        )
        if self.failures:
            line += f", failed {len(self.failures)}"
        return line


def human_bytes(size: int) -> str:
    """Readable size. Whole MB hides a 900 KB reclaim, which is most of them."""

    for unit, scale in (("MB", 1024 * 1024), ("KB", 1024)):
        if size >= scale:
            return f"{size / scale:.1f} {unit}"
    return f"{size} B"


def parse_timestamp(value: str) -> datetime | None:
    """Parse a store timestamp, tolerating the trailing-Z form and junk."""

    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_days(value: str, now: datetime) -> float | None:
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    return (now - parsed).total_seconds() / _DAY_S


def _latest(*values: str) -> str:
    """Newest of a set of timestamps, comparing parsed values not raw strings."""

    best_raw = ""
    best: datetime | None = None
    for value in values:
        parsed = parse_timestamp(value)
        if parsed is None:
            continue
        if best is None or parsed > best:
            best, best_raw = parsed, str(value)
    return best_raw


def directory_bytes(path: Path) -> int:
    total = 0
    try:
        for root, _dirs, files in os.walk(path):
            for name in files:
                try:
                    total += (Path(root) / name).stat().st_size
                except OSError:
                    continue
    except OSError:
        return total
    return total


def _run_is_live(run: OrchestrationRun) -> bool:
    if run.archived_at:
        return False
    return run.status not in TERMINAL_RUN_STATUSES


def _run_has_live_session(run: OrchestrationRun) -> bool:
    return any(session.status in ACTIVE_SESSION_STATUSES and not session.archived_at for session in run.sessions)


def _children_of(thread_id: str, runs: Iterable[OrchestrationRun]) -> list[OrchestrationRun]:
    return [run for run in runs if thread_id in {run.parent_chat_id or "", run.parent_run_id or ""}]


def _keep_reason(thread: Any, children: Sequence[OrchestrationRun], live_keys: frozenset[tuple[str, str]]) -> str:
    """Why this conversation must survive the sweep, or "" if it may go.

    Ordered cheapest-and-most-decisive first. Every rule here is local state —
    the sweep must never depend on a worker probe, because an unreachable worker
    would otherwise read as "nothing is running" and license a delete.
    """

    if (thread.project_id, thread.thread_id) in live_keys:
        return "turn in flight"
    if thread.queued_turns:
        return "queued turns"
    workspace = thread.workspace or {}
    if str(workspace.get("status") or "") in LIVE_WORKSPACE_STATUSES:
        return "live workspace execution"
    if workspace.get("pending_child_watch_ids"):
        return "pending child watch"
    for child in children:
        if _run_is_live(child):
            return f"live child run {child.run_id}"
        if _run_has_live_session(child):
            return f"live worker session in child run {child.run_id}"
        if child.jobs:
            # Run delete refuses a run that still has worker jobs (409); a tree
            # that cannot be fully collected must not be half-collected.
            return f"child run {child.run_id} still has worker jobs"
    return ""


def plan_retention(
    *,
    threads: Sequence[Any],
    runs: Sequence[OrchestrationRun],
    policy: RetentionPolicy,
    now: datetime | None = None,
    live_keys: frozenset[tuple[str, str]] = frozenset(),
    thread_bytes: Callable[[str, str], int] | None = None,
    run_bytes: Callable[[str], int] | None = None,
) -> RetentionPlan:
    """Classify every conversation and decide what a sweep would delete.

    Pure and side-effect free so the CLI's `--dry-run` reports exactly what the
    automatic sweep would do — same inputs, same plan.
    """

    now = now or datetime.now(timezone.utc)
    thread_bytes = thread_bytes or (lambda _project_id, _thread_id: 0)
    run_bytes = run_bytes or (lambda _run_id: 0)
    candidates: list[RetentionCandidate] = []
    kept: list[RetentionKeep] = []

    for thread in threads:
        children = _children_of(thread.thread_id, runs)
        klass = classify(thread, children)
        keep = _keep_reason(thread, children, live_keys)
        if keep:
            kept.append(RetentionKeep(thread.project_id, thread.thread_id, klass, keep))
            continue
        if not policy.class_enabled(klass):
            kept.append(RetentionKeep(thread.project_id, thread.thread_id, klass, f"{klass} retention disabled"))
            continue
        age = _age_days(_activity_reference(thread, children, klass), now)
        if age is None:
            kept.append(RetentionKeep(thread.project_id, thread.thread_id, klass, "no usable activity timestamp"))
            continue
        if age < policy.ttl_days(klass):
            kept.append(
                RetentionKeep(
                    thread.project_id,
                    thread.thread_id,
                    klass,
                    f"{age:.1f}d old, ttl {policy.ttl_days(klass):g}d",
                )
            )
            continue
        child_ids = tuple(child.run_id for child in children)
        size = thread_bytes(thread.project_id, thread.thread_id) + sum(run_bytes(run_id) for run_id in child_ids)
        candidates.append(
            RetentionCandidate(
                project_id=thread.project_id,
                thread_id=thread.thread_id,
                klass=klass,
                title=str(getattr(thread, "title", "") or ""),
                age_days=age,
                child_run_ids=child_ids,
                bytes_estimate=size,
            )
        )

    return RetentionPlan(candidates=tuple(candidates), kept=tuple(kept))


def classify(thread: Any, children: Sequence[OrchestrationRun]) -> str:
    """Which retention class a conversation belongs to.

    Archive wins over structure: it is an explicit operator signal that the
    conversation is finished, so an archived review tree ages out on the
    archived TTL rather than the tree TTL. Cascade is a mechanism, not a class —
    a tree is always deleted whole, whichever TTL retired it.
    """

    if thread.archived_at:
        return CLASS_ARCHIVED
    if children:
        return CLASS_TREE
    return CLASS_CHAT


def _activity_reference(thread: Any, children: Sequence[OrchestrationRun], klass: str) -> str:
    if klass == CLASS_ARCHIVED:
        return str(thread.archived_at or "")
    own = _latest(
        str(getattr(thread, "last_turn_at", "") or ""),
        str(getattr(thread, "updated_at", "") or ""),
        str(getattr(thread, "created_at", "") or ""),
    )
    if klass != CLASS_TREE:
        return own
    # A tree is only as idle as its most recently touched child: a review whose
    # last child finished an hour ago is not a week-old conversation.
    return _latest(own, *(str(child.updated_at or "") for child in children))


async def execute_plan(
    plan: RetentionPlan,
    *,
    delete_thread: Callable[[str, str], Awaitable[dict[str, Any]]],
    delete_run: Callable[[str], Awaitable[dict[str, Any]]],
) -> RetentionResult:
    """Delete every candidate via the injected lifecycle deletes.

    Children go first. A tree whose child delete fails keeps its parent, so a
    partial sweep can only ever leave *fewer* records than it found — never an
    orphaned child pointing at a parent that no longer exists.
    """

    result = RetentionResult(kept=len(plan.kept))
    for candidate in plan.candidates:
        try:
            child_bytes = 0
            for run_id in candidate.child_run_ids:
                packet = await delete_run(run_id)
                child_bytes += _reclaimed_bytes(packet)
                result.child_runs += 1
            packet = await delete_thread(candidate.project_id, candidate.thread_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - retention is best-effort; one bad tree must not stop the sweep
            result.failures.append(f"{candidate.thread_id}: {str(exc)[:200] or exc.__class__.__name__}")
            continue
        result.deleted[candidate.klass] = result.deleted.get(candidate.klass, 0) + 1
        # bytes_estimate is local store disk (transcript + run dirs); the packet
        # bytes are what the *worker* reclaimed (worktrees). Disjoint, so sum.
        result.bytes_reclaimed += child_bytes + _reclaimed_bytes(packet) + candidate.bytes_estimate
    return result


def _reclaimed_bytes(packet: dict[str, Any]) -> int:
    reclamation = packet.get("reclamation") if isinstance(packet, dict) else None
    if not isinstance(reclamation, dict):
        return 0
    try:
        return max(0, int(reclamation.get("bytes") or 0))
    except (TypeError, ValueError):
        return 0
