from __future__ import annotations

import json
import pathlib
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from jarvis.ids import new_id
from jarvis.orchestration.models import WorkCommand


@dataclass
class SchedulePolicy:
    max_concurrent_runs: int = 1
    skip_if_active: bool = True
    catch_up: str = "skip"
    report_on_no_work: bool = True
    public_write_mode: str = "draft_then_confirm"


@dataclass
class Schedule:
    schedule_id: str
    name: str
    command: WorkCommand
    hour: int
    minute: int
    weekdays: list[int] = field(default_factory=lambda: [0, 1, 2, 3, 4, 5, 6])
    timezone: str = "Europe/London"
    enabled: bool = True
    mode: str = "one_shot"
    policy: SchedulePolicy = field(default_factory=SchedulePolicy)
    last_fired_date: str = ""

    def __post_init__(self) -> None:
        _validate_time(self.hour, self.minute)
        _validate_weekdays(self.weekdays)
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone {self.timezone!r}") from exc

    @classmethod
    def from_dict(cls, data: dict) -> Schedule:
        return cls(
            schedule_id=data["schedule_id"],
            name=data.get("name", data["schedule_id"]),
            command=WorkCommand.from_dict(data.get("command", {})),
            hour=int(data.get("hour", 9)),
            minute=int(data.get("minute", 0)),
            weekdays=list(data.get("weekdays", [0, 1, 2, 3, 4, 5, 6])),
            timezone=data.get("timezone", "Europe/London"),
            enabled=bool(data.get("enabled", True)),
            mode=data.get("mode", "one_shot"),
            policy=SchedulePolicy(**data.get("policy", {})),
            last_fired_date=data.get("last_fired_date", ""),
        )

    def to_dict(self) -> dict:
        data = asdict(self)
        data["command"] = self.command.to_dict()
        data["policy"] = asdict(self.policy)
        return data

    def due(self, now: datetime) -> bool:
        if not self.enabled:
            return False
        try:
            local = now.astimezone(ZoneInfo(self.timezone))
        except Exception:
            return False
        if local.weekday() not in self.weekdays:
            return False
        if local.hour != self.hour or local.minute != self.minute:
            return False
        today = local.date().isoformat()
        return self.last_fired_date != today

    def mark_fired(self, now: datetime) -> None:
        self.last_fired_date = now.astimezone(ZoneInfo(self.timezone)).date().isoformat()


@dataclass
class ScheduleDispatchResult:
    schedule_id: str
    name: str
    status: str
    message: str = ""
    run_id: str = ""
    session_id: str = ""
    command: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class ScheduleStore:
    def __init__(self, path: str) -> None:
        self.path = pathlib.Path(path).expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def list(self) -> list[Schedule]:
        if not self.path.exists():
            return []
        try:
            raw = json.loads(self.path.read_text())
        except (OSError, json.JSONDecodeError):
            return []
        schedules: list[Schedule] = []
        for item in raw.get("schedules", []):
            try:
                schedules.append(Schedule.from_dict(item))
            except (KeyError, TypeError, ValueError):
                continue
        return schedules

    def save_all(self, schedules: list[Schedule]) -> None:
        self.path.write_text(
            json.dumps({"schedules": [x.to_dict() for x in schedules]}, indent=2, sort_keys=True)
        )

    def add(
        self,
        name: str,
        command: WorkCommand,
        *,
        hour: int,
        minute: int,
        weekdays: list[int] | None = None,
        timezone: str = "Europe/London",
        mode: str = "one_shot",
    ) -> Schedule:
        schedules = self.list()
        schedule = Schedule(
            schedule_id=new_id("sched"),
            name=name,
            command=command,
            hour=hour,
            minute=minute,
            weekdays=weekdays or [0, 1, 2, 3, 4, 5, 6],
            timezone=timezone,
            mode=mode,
        )
        schedules.append(schedule)
        self.save_all(schedules)
        return schedule

    def due(self, now: datetime) -> list[Schedule]:
        return [x for x in self.list() if x.due(now)]

    def ack(self, schedule_id: str, now: datetime) -> Schedule | None:
        schedules = self.list()
        acked: Schedule | None = None
        for schedule in schedules:
            if schedule.schedule_id == schedule_id:
                schedule.mark_fired(now)
                acked = schedule
                break
        if acked is not None:
            self.save_all(schedules)
        return acked


def dispatch_due_schedules(
    schedule_store: ScheduleStore,
    *,
    now: datetime,
    service: Any,
    run_store: Any,
) -> list[ScheduleDispatchResult]:
    results: list[ScheduleDispatchResult] = []
    for schedule in schedule_store.due(now):
        if schedule.policy.skip_if_active and _active_schedule_run(run_store, schedule.schedule_id):
            schedule_store.ack(schedule.schedule_id, now)
            results.append(
                ScheduleDispatchResult(
                    schedule_id=schedule.schedule_id,
                    name=schedule.name,
                    status="skipped_active",
                    message="Skipped because an earlier scheduled run is still active.",
                    command=schedule.command.to_dict(),
                )
            )
            continue
        try:
            started = service.next_work(schedule.command, start=bool(schedule.command.start))
        except Exception as exc:  # noqa: BLE001 - a failed dispatch must not ack/drop the schedule
            results.append(
                ScheduleDispatchResult(
                    schedule_id=schedule.schedule_id,
                    name=schedule.name,
                    status="failed",
                    message=str(exc),
                    command=schedule.command.to_dict(),
                )
            )
            continue
        if not schedule.command.start and started is not None:
            schedule_store.ack(schedule.schedule_id, now)
            results.append(
                ScheduleDispatchResult(
                    schedule_id=schedule.schedule_id,
                    name=schedule.name,
                    status="inspected",
                    message="Scheduled command inspected work without starting a worker session.",
                    command=schedule.command.to_dict(),
                )
            )
            continue
        if started is None:
            run_id = ""
            if schedule.policy.report_on_no_work:
                run = run_store.create_run(f"Schedule {schedule.name}: no work")
                run_store.append_event(
                    run.run_id,
                    "schedule_no_work",
                    "Schedule fired and found no work.",
                    {"schedule_id": schedule.schedule_id, "command": schedule.command.to_dict()},
                )
                run_store.set_phase(run.run_id, "done", "No work found for schedule.")
                run_id = run.run_id
            schedule_store.ack(schedule.schedule_id, now)
            results.append(
                ScheduleDispatchResult(
                    schedule_id=schedule.schedule_id,
                    name=schedule.name,
                    status="no_work",
                    message="No work found.",
                    run_id=run_id,
                    command=schedule.command.to_dict(),
                )
            )
            continue
        run_id = getattr(getattr(started, "envelope", None), "run_id", "")
        session_id = getattr(getattr(started, "session", None), "session_id", "")
        if run_id:
            run_store.append_event(
                run_id,
                "schedule_fired",
                f"Schedule {schedule.name} dispatched.",
                {"schedule_id": schedule.schedule_id, "command": schedule.command.to_dict()},
            )
        schedule_store.ack(schedule.schedule_id, now)
        results.append(
            ScheduleDispatchResult(
                schedule_id=schedule.schedule_id,
                name=schedule.name,
                status="started",
                message="Scheduled work started.",
                run_id=run_id,
                session_id=session_id,
                command=schedule.command.to_dict(),
            )
        )
    return results


def _validate_time(hour: int, minute: int) -> None:
    if hour < 0 or hour > 23:
        raise ValueError("hour must be between 0 and 23")
    if minute < 0 or minute > 59:
        raise ValueError("minute must be between 0 and 59")


def _validate_weekdays(weekdays: list[int]) -> None:
    if not weekdays:
        raise ValueError("weekdays must not be empty")
    if any(day < 0 or day > 6 for day in weekdays):
        raise ValueError("weekdays must be between 0 and 6")


def _active_schedule_run(run_store: Any, schedule_id: str) -> bool:
    for run in run_store.list_runs():
        if getattr(run, "status", "") == "terminal":
            continue
        for event in run_store.events(run.run_id):
            if event.type == "schedule_fired" and event.data.get("schedule_id") == schedule_id:
                return True
    return False
