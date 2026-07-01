from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

from jarvis.orchestration.models import OrchestrationRun, WorkItem
from jarvis.orchestration.store import OrchestrationStore


@dataclass
class CampaignPolicy:
    max_items: int = 5
    max_duration_minutes: int = 120
    max_concurrent_runs: int = 1
    stop_when: list[str] = field(
        default_factory=lambda: ["queue_empty", "budget_exhausted", "blocked", "human_needed"]
    )


def create_campaign(
    store: OrchestrationStore,
    *,
    objective: str,
    candidates: list[WorkItem],
    policy: CampaignPolicy | None = None,
    start_child: Callable[[OrchestrationRun, WorkItem], object | None] | None = None,
) -> OrchestrationRun:
    policy = policy or CampaignPolicy()
    parent = store.create_run(objective)
    store.append_event(parent.run_id, "campaign_started", objective, {"policy": policy.__dict__})
    if not candidates:
        store.set_phase(parent.run_id, "done", "Campaign stopped: queue_empty")
        return store.get(parent.run_id) or parent
    active_children = 0
    for item in candidates[: policy.max_items]:
        if start_child is not None and active_children >= policy.max_concurrent_runs:
            store.append_event(
                parent.run_id,
                "campaign_stopped",
                "Campaign stopped: max_concurrency",
                {"active_children": active_children, "max_concurrent_runs": policy.max_concurrent_runs},
            )
            break
        child = store.create_run(item.title, work_items=[item], parent_run_id=parent.run_id)
        if start_child is None:
            continue
        try:
            session = start_child(child, item)
        except Exception as exc:  # noqa: BLE001 - campaign must leave an inspectable child run
            store.set_phase(child.run_id, "blocked", f"Campaign child dispatch failed: {exc}")
            store.append_event(
                parent.run_id,
                "campaign_child_blocked",
                f"Child {child.run_id} blocked during dispatch.",
                {"child_run_id": child.run_id, "error": str(exc)},
            )
            if "blocked" in policy.stop_when:
                break
            continue
        if session is not None:
            active_children += 1
            store.append_event(
                parent.run_id,
                "campaign_child_session_started",
                f"Child {child.run_id} started a worker session.",
                {
                    "child_run_id": child.run_id,
                    "session_id": getattr(session, "session_id", ""),
                    "worker_id": getattr(session, "worker_id", ""),
                },
            )
    parent = store.get(parent.run_id) or parent
    store.append_event(
        parent.run_id,
        "campaign_children_created",
        f"Created {len(parent.child_run_ids)} child run(s)",
        {"child_run_ids": parent.child_run_ids},
    )
    return parent
