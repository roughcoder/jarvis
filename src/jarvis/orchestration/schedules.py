from __future__ import annotations

import json
import pathlib
from dataclasses import asdict, dataclass, field
from datetime import datetime
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
