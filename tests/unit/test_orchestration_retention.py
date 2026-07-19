"""Conversation retention: classification, protections, cascade, dry-run."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from jarvis.connectors.cockpit import CockpitThread
from jarvis.orchestration.models import OrchestrationRun, WorkerJobLink, WorkerSessionLink
from jarvis.orchestration.retention import (
    CLASS_ARCHIVED,
    CLASS_CHAT,
    CLASS_TREE,
    RetentionPolicy,
    execute_plan,
    plan_retention,
)

NOW = datetime(2026, 7, 19, 12, 0, 0, tzinfo=timezone.utc)
POLICY = RetentionPolicy(archived_ttl_days=14, chat_ttl_days=7, tree_ttl_days=7)


def ts(days_ago: float) -> str:
    return (NOW - timedelta(days=days_ago)).isoformat()


def thread(thread_id: str, *, days_idle: float = 30, **kwargs) -> CockpitThread:
    stamp = ts(days_idle)
    fields = {
        "thread_id": thread_id,
        "project_id": "jarvis",
        "session_id": f"project:jarvis:{thread_id}",
        "title": thread_id,
        "created_at": stamp,
        "updated_at": stamp,
        "created_by": "neil",
        "last_turn_at": stamp,
    }
    fields.update(kwargs)
    return CockpitThread(**fields)


def run(run_id: str, parent: str, *, status: str = "terminal", days_idle: float = 30, **kwargs) -> OrchestrationRun:
    return OrchestrationRun(
        run_id=run_id,
        objective=run_id,
        status=status,
        phase="done" if status == "terminal" else "running",
        parent_chat_id=parent,
        parent_run_id=parent,
        project_id="jarvis",
        created_at=ts(days_idle),
        updated_at=ts(days_idle),
        **kwargs,
    )


def plan(threads, runs=(), policy=POLICY, **kwargs):
    return plan_retention(threads=list(threads), runs=list(runs), policy=policy, now=NOW, **kwargs)


# --- classification -------------------------------------------------------


def test_classifies_each_conversation_class() -> None:
    result = plan(
        [
            thread("t_chat"),
            thread("t_archived", archived_at=ts(20)),
            thread("t_tree"),
        ],
        [run("run_child", "t_tree")],
    )
    assert {c.thread_id: c.klass for c in result.candidates} == {
        "t_chat": CLASS_CHAT,
        "t_archived": CLASS_ARCHIVED,
        "t_tree": CLASS_TREE,
    }
    assert result.counts() == {CLASS_ARCHIVED: 1, CLASS_CHAT: 1, CLASS_TREE: 1}


def test_archive_wins_over_tree_structure() -> None:
    """An archived review tree ages on the archived TTL, not the tree TTL."""
    result = plan(
        [thread("t", archived_at=ts(10))],
        [run("run_child", "t")],
        policy=RetentionPolicy(archived_ttl_days=14, chat_ttl_days=1, tree_ttl_days=1),
    )
    assert not result.candidates  # 10d archived < 14d TTL, despite a 1d tree TTL
    assert result.kept[0].klass == CLASS_ARCHIVED


def test_archived_ages_from_archived_at_not_last_activity() -> None:
    aged = thread("t_old", days_idle=90, archived_at=ts(2))
    assert not plan([aged]).candidates


def test_tree_age_follows_its_most_recent_child() -> None:
    """A parent idle for a month is not collectable if a child finished today."""
    result = plan([thread("t", days_idle=30)], [run("run_child", "t", days_idle=0.5)])
    assert not result.candidates
    assert "0.5d old" in result.kept[0].reason


def test_young_conversation_is_kept() -> None:
    result = plan([thread("t", days_idle=3)])
    assert not result.candidates
    assert result.kept[0].reason.startswith("3.0d old")


# --- TTL = 0 semantics ----------------------------------------------------


@pytest.mark.parametrize(
    ("policy", "disabled"),
    [
        (RetentionPolicy(archived_ttl_days=0), CLASS_ARCHIVED),
        (RetentionPolicy(chat_ttl_days=0), CLASS_CHAT),
        (RetentionPolicy(tree_ttl_days=0), CLASS_TREE),
    ],
)
def test_ttl_zero_disables_only_its_own_class(policy: RetentionPolicy, disabled: str) -> None:
    threads = [thread("t_chat"), thread("t_archived", archived_at=ts(30)), thread("t_tree")]
    runs = [run("run_child", "t_tree")]
    result = plan(threads, runs, policy=policy)

    assert not policy.class_enabled(disabled)
    assert disabled not in {c.klass for c in result.candidates}
    assert len(result.candidates) == 2  # the other two classes still collect
    assert any(k.reason == f"{disabled} retention disabled" for k in result.kept)


def test_all_ttls_zero_leaves_nothing_enabled() -> None:
    policy = RetentionPolicy(archived_ttl_days=0, chat_ttl_days=0, tree_ttl_days=0)
    assert not policy.any_class_enabled
    assert not plan([thread("t")], policy=policy).candidates


# --- protections ----------------------------------------------------------


def test_in_flight_turn_protects_the_conversation() -> None:
    result = plan([thread("t")], live_keys=frozenset({("jarvis", "t")}))
    assert not result.candidates
    assert result.kept[0].reason == "turn in flight"


def test_queued_turns_protect_the_conversation() -> None:
    aged = thread("t", queued_turns=({"queue_id": "q1", "status": "queued"},))
    result = plan([aged])
    assert not result.candidates
    assert result.kept[0].reason == "queued turns"


@pytest.mark.parametrize("status", ["starting", "running", "interrupting", "waiting_input", "waiting_approval"])
def test_live_workspace_execution_protects_the_conversation(status: str) -> None:
    aged = thread("t", workspace={"session_id": "s1", "worker_id": "w1", "status": status})
    result = plan([aged])
    assert not result.candidates
    assert result.kept[0].reason == "live workspace execution"


def test_pending_child_watch_protects_the_conversation() -> None:
    aged = thread("t", workspace={"pending_child_watch_ids": ["watch_1"]})
    result = plan([aged], [run("run_child", "t")])
    assert not result.candidates
    assert result.kept[0].reason == "pending child watch"


def test_live_child_run_protects_the_whole_tree() -> None:
    result = plan([thread("t")], [run("run_done", "t"), run("run_live", "t", status="active")])
    assert not result.candidates
    assert result.kept[0].reason == "live child run run_live"


def test_live_worker_session_protects_the_whole_tree() -> None:
    child = run("run_child", "t", sessions=[WorkerSessionLink(worker_id="w1", session_id="s1", status="running")])
    result = plan([thread("t")], [child])
    assert not result.candidates
    assert result.kept[0].reason == "live worker session in child run run_child"


def test_child_with_worker_jobs_protects_the_whole_tree() -> None:
    """Run delete 409s on a run with jobs, so the tree cannot be collected whole."""
    child = run("run_child", "t", jobs=[WorkerJobLink(worker_id="w1", job_id="j1", status="running")])
    result = plan([thread("t")], [child])
    assert not result.candidates
    assert "still has worker jobs" in result.kept[0].reason


def test_archived_session_on_a_terminal_child_does_not_protect() -> None:
    child = run(
        "run_child",
        "t",
        sessions=[WorkerSessionLink(worker_id="w1", session_id="s1", status="running", archived_at=ts(20))],
    )
    result = plan([thread("t")], [child])
    assert [c.thread_id for c in result.candidates] == ["t"]


def test_unparseable_activity_timestamp_is_kept() -> None:
    result = plan([thread("t", last_turn_at="", updated_at="not-a-date", created_at="")])
    assert not result.candidates
    assert result.kept[0].reason == "no usable activity timestamp"


# --- dry-run accuracy + byte accounting -----------------------------------


def test_plan_reports_children_and_bytes_per_class() -> None:
    result = plan(
        [thread("t_chat"), thread("t_tree")],
        [run("run_a", "t_tree"), run("run_b", "t_tree")],
        thread_bytes=lambda _p, thread_id: 1000 if thread_id == "t_chat" else 200,
        run_bytes=lambda _run_id: 4000,
    )
    by_id = {c.thread_id: c for c in result.candidates}
    assert by_id["t_chat"].bytes_estimate == 1000
    assert by_id["t_chat"].child_run_ids == ()
    assert sorted(by_id["t_tree"].child_run_ids) == ["run_a", "run_b"]
    assert by_id["t_tree"].bytes_estimate == 200 + 4000 + 4000
    assert result.bytes_by_class() == {CLASS_ARCHIVED: 0, CLASS_CHAT: 1000, CLASS_TREE: 8200}
    assert result.total_bytes == 9200


def test_dry_run_plan_matches_what_the_sweep_deletes() -> None:
    """The CLI's report and the automatic sweep must agree — same plan object."""
    threads = [thread("t_chat"), thread("t_tree"), thread("t_young", days_idle=1)]
    runs = [run("run_child", "t_tree")]
    result = plan(threads, runs)
    deleted_threads: list[str] = []
    deleted_runs: list[str] = []

    async def delete_thread(project_id: str, thread_id: str) -> dict:
        deleted_threads.append(thread_id)
        return {"reclamation": {"bytes": 0}}

    async def delete_run(run_id: str) -> dict:
        deleted_runs.append(run_id)
        return {"reclamation": {"bytes": 0}}

    outcome = asyncio.run(execute_plan(result, delete_thread=delete_thread, delete_run=delete_run))
    assert set(deleted_threads) == {c.thread_id for c in result.candidates} == {"t_chat", "t_tree"}
    assert deleted_runs == ["run_child"]
    assert outcome.deleted == {CLASS_ARCHIVED: 0, CLASS_CHAT: 1, CLASS_TREE: 1}
    assert outcome.kept == 1
    assert "t_young" in {k.thread_id for k in result.kept}


# --- cascade atomicity ----------------------------------------------------


def test_tree_cascade_deletes_children_before_the_parent() -> None:
    result = plan([thread("t")], [run("run_a", "t"), run("run_b", "t")])
    order: list[str] = []

    async def delete_thread(project_id: str, thread_id: str) -> dict:
        order.append(f"thread:{thread_id}")
        return {}

    async def delete_run(run_id: str) -> dict:
        order.append(f"run:{run_id}")
        return {}

    asyncio.run(execute_plan(result, delete_thread=delete_thread, delete_run=delete_run))
    assert order[-1] == "thread:t"
    assert sorted(order[:2]) == ["run:run_a", "run:run_b"]


def test_failed_child_delete_keeps_the_parent_so_children_are_never_orphaned() -> None:
    result = plan([thread("t_bad"), thread("t_ok")], [run("run_bad", "t_bad"), run("run_ok", "t_ok")])
    deleted_threads: list[str] = []

    async def delete_thread(project_id: str, thread_id: str) -> dict:
        deleted_threads.append(thread_id)
        return {}

    async def delete_run(run_id: str) -> dict:
        if run_id == "run_bad":
            raise RuntimeError("worker unreachable")
        return {}

    outcome = asyncio.run(execute_plan(result, delete_thread=delete_thread, delete_run=delete_run))
    assert deleted_threads == ["t_ok"]  # the failed tree kept its parent
    assert outcome.deleted[CLASS_TREE] == 1
    assert outcome.failures == ["t_bad: worker unreachable"]
    assert "failed 1" in outcome.summary_line()


def test_sweep_reports_bytes_from_both_the_store_and_the_worker() -> None:
    result = plan(
        [thread("t")],
        [run("run_child", "t")],
        thread_bytes=lambda _p, _t: 100,
        run_bytes=lambda _r: 900,
    )

    async def delete_thread(project_id: str, thread_id: str) -> dict:
        return {"reclamation": {"bytes": 0}}

    async def delete_run(run_id: str) -> dict:
        return {"reclamation": {"bytes": 5000}}  # worker-side worktree

    outcome = asyncio.run(execute_plan(result, delete_thread=delete_thread, delete_run=delete_run))
    assert outcome.bytes_reclaimed == 1000 + 5000
    assert outcome.child_runs == 1


# --- policy plumbing ------------------------------------------------------


def test_sweep_end_to_end_against_a_real_store(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    """Proof the sweep drives the real lifecycle delete, not a parallel one."""
    import json

    from jarvis.config import Config
    from jarvis.connectors.cockpit import THREAD_INDEX_FILENAME, CockpitThreadIndex
    from jarvis.orchestration import api as api_module
    from jarvis.orchestration.store import OrchestrationStore

    env = tmp_path / ".env"
    workspace = tmp_path / "orchestration"
    env.write_text(
        "\n".join(
            [
                f"ORCHESTRATION_WORKSPACE={workspace}",
                f"MEMORY_CACHE_PATH={tmp_path / 'memory-cache.json'}",
                "ORCHESTRATION_RETENTION_CHAT_TTL_DAYS=7",
                "ORCHESTRATION_RETENTION_TREE_TTL_DAYS=7",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("JARVIS_ENV_FILE", str(env))
    cfg = Config()

    class FakeMemory:
        def __init__(self, _cfg) -> None:  # noqa: ANN001
            pass

        def delete_session(self, _session_id: str) -> None:
            return None

    monkeypatch.setattr(api_module, "MemoryClient", FakeMemory)

    index = CockpitThreadIndex(workspace / THREAD_INDEX_FILENAME)
    store = OrchestrationStore(str(workspace))
    for name, days in (("t_dead", 40), ("t_live", 1)):
        index.save(thread(name, days_idle=days))
    parent = index.save(thread("t_tree", days_idle=40))
    child = store.create_run("child work", parent_chat_id=parent.thread_id)
    store.set_phase(child.run_id, "done", "child finished")
    # save() re-stamps updated_at, so age the child on disk the way a real
    # month-old run looks — the tree is only as idle as its newest child.
    aged_run = json.loads(store.run_path(child.run_id).read_text())
    aged_run["created_at"] = aged_run["updated_at"] = ts(40)
    store.run_path(child.run_id).write_text(json.dumps(aged_run))
    protected = index.save(thread("t_busy", days_idle=40, queued_turns=({"queue_id": "q1"},)))

    before = {t.thread_id for t in index.list_all()}
    assert before == {"t_dead", "t_live", "t_tree", protected.thread_id}
    assert store.get(child.run_id) is not None

    ctx = api_module.cockpit_context(cfg)
    policy = RetentionPolicy.from_config(cfg.orchestration)
    result = asyncio.run(api_module.run_retention_sweep(ctx, policy))

    after = {t.thread_id for t in index.list_all()}
    assert after == {"t_live", "t_busy"}  # young chat + protected chat survive
    assert store.get(child.run_id) is None  # tree cascaded through run delete
    assert result.deleted[CLASS_CHAT] == 1
    assert result.deleted[CLASS_TREE] == 1
    assert result.child_runs == 1
    assert not result.failures
    assert "kept 2" in result.summary_line()


def test_policy_reads_the_orchestration_config() -> None:
    from jarvis.config import OrchestrationConfig

    policy = RetentionPolicy.from_config(OrchestrationConfig(_env_file=None))
    assert policy.enabled is True
    assert policy.interval_s == 6 * 60 * 60
    assert (policy.archived_ttl_days, policy.chat_ttl_days, policy.tree_ttl_days) == (14.0, 7.0, 7.0)
