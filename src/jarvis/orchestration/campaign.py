from __future__ import annotations

from dataclasses import dataclass, field

from jarvis.orchestration.models import OrchestrationRun, WorkItem
from jarvis.orchestration.store import OrchestrationStore


@dataclass
class CampaignPolicy:
    max_items: int = 5
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
) -> OrchestrationRun:
    policy = policy or CampaignPolicy()
    parent = store.create_run(objective)
    store.append_event(parent.run_id, "campaign_started", objective, {"policy": policy.__dict__})
    if not candidates:
        store.set_phase(parent.run_id, "done", "Campaign stopped: queue_empty")
        return store.get(parent.run_id) or parent
    for item in candidates[: policy.max_items]:
        store.create_run(item.title, work_items=[item], parent_run_id=parent.run_id)
    parent = store.get(parent.run_id) or parent
    store.append_event(
        parent.run_id,
        "campaign_children_created",
        f"Created {len(parent.child_run_ids)} child run(s)",
        {"child_run_ids": parent.child_run_ids},
    )
    return parent
