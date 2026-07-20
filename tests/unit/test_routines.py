from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from jarvis.orchestration import routines as routines_module
from jarvis.orchestration.routines import (
    RoutineCatalog,
    RoutineDefinition,
    RoutineParameter,
    RoutineScheduleStore,
    resolve_routine,
)


def test_builtin_routine_catalog_exposes_first_class_library() -> None:
    routines = {routine.routine_id: routine for routine in RoutineCatalog().list()}

    assert set(routines) == {
        "morning-brief",
        "pull-request-review",
        "issue-triage",
        "system-health-check",
        "draft-release-notes",
    }
    pr_review = routines["pull-request-review"].to_dict()
    assert pr_review["version"] == 1
    assert pr_review["target_types"] == ["pull_request"]
    reviewers = next(item for item in pr_review["parameters"] if item["name"] == "reviewers")
    assert reviewers == {
        "name": "reviewers",
        "label": "Reviewers",
        "type": "model_ref",
        "description": "Exactly two provider/model combinations that independently review the pull request.",
        "required": True,
        "default": None,
        "options_source": "runtime.models",
        "allow_multiple": True,
        "sensitive": False,
        "choices": [],
        "min_items": 2,
        "max_items": 2,
    }
    dimensions = next(item for item in pr_review["parameters"] if item["name"] == "dimensions")
    assert dimensions["choices"] == [
        "correctness",
        "security",
        "performance",
        "tests",
        "maintainability",
        "style",
    ]


def test_builtin_routine_catalog_parses_once_per_catalog(monkeypatch) -> None:  # noqa: ANN001
    catalog = RoutineCatalog()
    builtin_path = catalog.path
    original_read_text = Path.read_text
    reads: list[Path] = []

    def tracked_read_text(path: Path, *args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        if path == builtin_path:
            reads.append(path)
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", tracked_read_text)

    assert len(catalog.list()) == 5
    assert len(catalog.list()) == 5
    assert reads == [builtin_path]


def test_pr_review_resolution_validates_two_models_and_renders_target() -> None:
    routine = RoutineCatalog().get("pull-request-review")
    assert routine is not None

    resolution = resolve_routine(
        routine,
        target={"repository": "roughcoder/jarvis", "number": 123},
        params={
            "reviewers": [
                {"engine": "codex", "model": "gpt-5"},
                {"engine": "claude", "model": "claude-opus"},
            ]
        },
    )

    assert resolution.ready is True
    assert resolution.missing == ()
    assert resolution.values["post_comments"] is True
    assert '"number":123' in resolution.rendered_prompt
    assert "gpt-5" in resolution.rendered_prompt

    with pytest.raises(ValueError, match="requires at least 2 items"):
        resolve_routine(
            routine,
            target={"repository": "roughcoder/jarvis", "number": 123},
            params={"reviewers": [{"engine": "codex", "model": "gpt-5"}]},
        )


def test_routine_resolution_reports_missing_and_uses_deterministic_day() -> None:
    pr_review = RoutineCatalog().get("pull-request-review")
    morning = RoutineCatalog().get("morning-brief")
    assert pr_review is not None and morning is not None

    missing = resolve_routine(pr_review)
    brief = resolve_routine(morning, today="2026-07-20")

    assert missing.ready is False
    assert missing.missing == ("pull_request", "reviewers")
    assert missing.rendered_prompt == ""
    assert brief.ready is True
    assert brief.values["day"] == "2026-07-20"
    assert "2026-07-20" in brief.rendered_prompt
    assert "Focus on Priorities, deadlines, blockers, and decisions that need attention." in brief.rendered_prompt

    health = RoutineCatalog().get("system-health-check")
    release = RoutineCatalog().get("draft-release-notes")
    assert health is not None and release is not None
    health_prompt = resolve_routine(health).rendered_prompt
    release_prompt = resolve_routine(
        release,
        target={"repository": "roughcoder/jarvis"},
    ).rendered_prompt
    assert "Worker selection: []" in health_prompt
    assert "workers  using" not in health_prompt
    assert "the repository's documented release range" in release_prompt


def test_optional_prompt_sentence_is_removed_without_dropping_mixed_context() -> None:
    routine = RoutineDefinition(
        routine_id="prompt-cleanup",
        version=1,
        name="Prompt cleanup",
        summary="",
        description="",
        prompt_template=(
            "Review {{repository}} carefully. "
            "Focus on {{focus}}. "
            "Preserve {{repository}} context across {{release_range}}."
        ),
        parameters=(
            RoutineParameter(
                name="repository",
                label="Repository",
                type="repository_ref",
                required=True,
            ),
            RoutineParameter(name="focus", label="Focus", type="text"),
            RoutineParameter(name="release_range", label="Range", type="string"),
        ),
        execution={"supported_engines": ["codex"]},
    )

    resolution = resolve_routine(
        routine,
        params={"repository": "roughcoder/jarvis"},
    )

    assert resolution.ready is True
    assert resolution.rendered_prompt == (
        "Review roughcoder/jarvis carefully. "
        "Preserve roughcoder/jarvis context across not specified."
    )
    assert " ." not in resolution.rendered_prompt


def test_optional_prompt_sentence_removal_preserves_paragraph_breaks() -> None:
    routine = RoutineDefinition(
        routine_id="paragraph-cleanup",
        version=1,
        name="Paragraph cleanup",
        summary="",
        description="",
        prompt_template=(
            "Review {{repository}} carefully.\n\n"
            "Focus on {{focus}}. "
            "Report the result."
        ),
        parameters=(
            RoutineParameter(
                name="repository",
                label="Repository",
                type="repository_ref",
                required=True,
            ),
            RoutineParameter(name="focus", label="Focus", type="text"),
        ),
        execution={"supported_engines": ["codex"]},
    )

    resolution = resolve_routine(
        routine,
        params={"repository": "roughcoder/jarvis"},
    )

    assert resolution.rendered_prompt == (
        "Review roughcoder/jarvis carefully.\n\nReport the result."
    )


def test_routine_schedule_store_lifecycle_and_daily_ack(tmp_path) -> None:  # noqa: ANN001
    store = RoutineScheduleStore(tmp_path / "routine-schedules.json")
    schedule = store.create(
        name="Morning brief",
        routine_id="morning-brief",
        routine_version=1,
        project_id="jarvis",
        created_by="neil",
        params={"day": "2026-07-20"},
        target={},
        hour=9,
        minute=30,
        timezone="Europe/London",
        first_eligible_date="2026-07-20",
    )
    due_at = datetime(2026, 7, 20, 8, 30, tzinfo=UTC)

    assert store.get(schedule.schedule_id) == schedule
    assert [item.schedule_id for item in store.due(due_at)] == [schedule.schedule_id]

    acked = store.ack(schedule.schedule_id, due_at)
    assert acked is not None
    assert acked.last_fired_date == "2026-07-20"
    assert store.due(due_at) == []

    updated = store.update(schedule.schedule_id, enabled=False, name="Weekday brief")
    assert updated is not None
    assert updated.enabled is False
    assert updated.name == "Weekday brief"
    assert store.delete(schedule.schedule_id) is True
    assert store.delete(schedule.schedule_id) is False


def test_routine_schedule_remains_due_after_a_missed_tick_until_acknowledged(tmp_path) -> None:  # noqa: ANN001
    store = RoutineScheduleStore(tmp_path / "routine-schedules.json")
    schedule = store.create(
        name="Morning brief",
        routine_id="morning-brief",
        routine_version=1,
        project_id="jarvis",
        created_by="neil",
        hour=9,
        minute=15,
        timezone="Europe/London",
        first_eligible_date="2026-07-20",
    )
    before_target = datetime(2026, 7, 20, 8, 14, tzinfo=UTC)
    missed_tick = datetime(2026, 7, 20, 8, 16, tzinfo=UTC)
    next_day_before_target = datetime(2026, 7, 21, 8, 14, tzinfo=UTC)

    assert store.due(before_target) == []
    assert [item.schedule_id for item in store.due(missed_tick)] == [schedule.schedule_id]

    acked = store.ack(schedule.schedule_id, missed_tick)
    assert acked is not None
    assert store.due(missed_tick) == []
    assert store.due(next_day_before_target) == []


def test_routine_schedule_created_at_or_after_target_waits_until_next_day(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        routines_module,
        "utc_now",
        lambda: "2026-07-20T09:15:30+00:00",
    )
    store = RoutineScheduleStore(tmp_path / "routine-schedules.json")
    schedule = store.create(
        name="Morning brief",
        routine_id="morning-brief",
        routine_version=1,
        project_id="jarvis",
        created_by="neil",
        hour=9,
        minute=15,
        timezone="UTC",
    )

    assert schedule.first_eligible_date == "2026-07-21"
    assert store.due(datetime(2026, 7, 20, 9, 15, tzinfo=UTC)) == []
    assert store.due(datetime(2026, 7, 21, 9, 14, tzinfo=UTC)) == []
    assert [item.schedule_id for item in store.due(datetime(2026, 7, 21, 9, 15, tzinfo=UTC))] == [
        schedule.schedule_id
    ]


def test_routine_schedule_material_timing_changes_reset_activation_watermark(
    tmp_path,
    monkeypatch,
) -> None:  # noqa: ANN001
    times = iter(
        [
            "2026-07-20T08:00:00+00:00",
            "2026-07-20T10:00:00+00:00",
            "2026-07-20T10:01:00+00:00",
        ]
    )
    monkeypatch.setattr(routines_module, "utc_now", lambda: next(times))
    store = RoutineScheduleStore(tmp_path / "routine-schedules.json")
    schedule = store.create(
        name="Morning brief",
        routine_id="morning-brief",
        routine_version=1,
        project_id="jarvis",
        created_by="neil",
        hour=11,
        minute=0,
        timezone="UTC",
    )
    assert schedule.first_eligible_date == "2026-07-20"

    rescheduled = store.update(schedule.schedule_id, hour=9)
    assert rescheduled is not None
    assert rescheduled.first_eligible_date == "2026-07-21"

    renamed = store.update(schedule.schedule_id, name="Renamed brief")
    assert renamed is not None
    assert renamed.first_eligible_date == "2026-07-21"


def test_routine_schedule_store_skips_invalid_records(tmp_path) -> None:  # noqa: ANN001
    path = tmp_path / "routine-schedules.json"
    path.write_text(json.dumps({"schedules": [{"schedule_id": "bad"}]}))

    assert RoutineScheduleStore(path).list() == []
