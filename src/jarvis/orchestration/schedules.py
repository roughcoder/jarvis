from __future__ import annotations

import json
import pathlib
from dataclasses import asdict, dataclass, field
from datetime import datetime
from zoneinfo import ZoneInfo

from jarvis.orchestration.models import WorkCommand, new_id


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
        local = now.astimezone(ZoneInfo(self.timezone))
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
        return [Schedule.from_dict(x) for x in raw.get("schedules", [])]

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
        schedules = self.list()
        due = [x for x in schedules if x.due(now)]
        if due:
            for schedule in schedules:
                if any(schedule.schedule_id == x.schedule_id for x in due):
                    schedule.mark_fired(now)
            self.save_all(schedules)
        return due
