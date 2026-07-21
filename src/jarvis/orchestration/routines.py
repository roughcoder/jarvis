from __future__ import annotations

import json
import re
import threading
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from jarvis.ids import new_id, utc_now
from jarvis.storage import atomic_write_json

PARAMETER_TYPES = frozenset(
    {
        "string",
        "text",
        "boolean",
        "integer",
        "date",
        "enum",
        "repository_ref",
        "pull_request_ref",
        "model_ref",
        "worker_ref",
    }
)
DEFAULT_SOURCES = frozenset({"literal", "today", "target", "project", "requester"})
OPTIONS_SOURCES = frozenset(
    {
        "",
        "github.repositories",
        "project.repositories",
        "runtime.models",
        "runtime.workers",
        "review.dimensions",
    }
)
TARGET_TYPES = frozenset(
    {"global", "project", "chat", "conversation", "repository", "pull_request", "issue", "fleet"}
)
_TEMPLATE_PATTERN = re.compile(r"\{\{([a-z][a-z0-9_]*)\}\}")
_STORE_LOCK = threading.RLock()
_CATALOG_LOCK = threading.Lock()


@dataclass(frozen=True)
class RoutineParameter:
    name: str
    label: str
    type: str
    description: str = ""
    required: bool = False
    default: dict[str, Any] | None = None
    options_source: str = ""
    allow_multiple: bool = False
    sensitive: bool = False
    choices: tuple[str, ...] = ()
    min_items: int = 0
    max_items: int = 0

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", self.name):
            raise ValueError(f"invalid routine parameter name: {self.name!r}")
        if self.type not in PARAMETER_TYPES:
            raise ValueError(f"unsupported routine parameter type: {self.type!r}")
        if self.options_source not in OPTIONS_SOURCES:
            raise ValueError(f"unsupported routine options source: {self.options_source!r}")
        if self.default is not None:
            source = str(self.default.get("source") or "")
            if source not in DEFAULT_SOURCES:
                raise ValueError(f"unsupported routine default source: {source!r}")
            if source == "literal" and "value" not in self.default:
                raise ValueError(f"literal default for {self.name!r} requires a value")
        if self.type == "enum" and not self.choices and not self.options_source:
            raise ValueError(f"enum parameter {self.name!r} requires choices or options_source")
        if self.min_items < 0 or self.max_items < 0 or (
            self.max_items and self.min_items > self.max_items
        ):
            raise ValueError(f"invalid item bounds for routine parameter {self.name!r}")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RoutineParameter:
        default = data.get("default")
        return cls(
            name=str(data.get("name") or ""),
            label=str(data.get("label") or data.get("name") or ""),
            type=str(data.get("type") or "string"),
            description=str(data.get("description") or ""),
            required=bool(data.get("required", False)),
            default=dict(default) if isinstance(default, dict) else None,
            options_source=str(data.get("options_source") or ""),
            allow_multiple=bool(data.get("allow_multiple", False)),
            sensitive=bool(data.get("sensitive", False)),
            choices=tuple(str(item) for item in data.get("choices") or []),
            min_items=int(data.get("min_items", 0)),
            max_items=int(data.get("max_items", 0)),
        )

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["choices"] = list(self.choices)
        return data


@dataclass(frozen=True)
class RoutineDefinition:
    routine_id: str
    version: int
    name: str
    summary: str
    description: str
    prompt_template: str
    parameters: tuple[RoutineParameter, ...] = ()
    target_types: tuple[str, ...] = ()
    builtin: bool = False
    execution: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9-]*", self.routine_id):
            raise ValueError(f"invalid routine id: {self.routine_id!r}")
        if self.version < 0:
            raise ValueError("routine version must be non-negative")
        if not self.name.strip() or not self.prompt_template.strip():
            raise ValueError("routine name and prompt_template are required")
        if len({parameter.name for parameter in self.parameters}) != len(self.parameters):
            raise ValueError(f"routine {self.routine_id!r} has duplicate parameter names")
        invalid_targets = set(self.target_types) - TARGET_TYPES
        if invalid_targets:
            raise ValueError(f"unsupported routine target types: {sorted(invalid_targets)!r}")
        parameter_names = {parameter.name for parameter in self.parameters}
        unknown_placeholders = set(_TEMPLATE_PATTERN.findall(self.prompt_template)) - parameter_names
        if unknown_placeholders:
            raise ValueError(
                f"routine {self.routine_id!r} has unknown prompt placeholders: {sorted(unknown_placeholders)!r}"
            )
        supported = self.execution.get("supported_engines", [])
        if not isinstance(supported, list) or any(engine not in {"codex", "claude"} for engine in supported):
            raise ValueError("routine supported_engines must contain only codex or claude")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RoutineDefinition:
        parameters = data.get("parameters") or []
        targets = data.get("target_types") or []
        execution = data.get("execution") or {}
        if not isinstance(parameters, list) or not isinstance(targets, list) or not isinstance(execution, dict):
            raise ValueError("invalid routine definition")
        return cls(
            routine_id=str(data.get("routine_id") or ""),
            version=int(data.get("version", 0)),
            name=str(data.get("name") or ""),
            summary=str(data.get("summary") or ""),
            description=str(data.get("description") or ""),
            prompt_template=str(data.get("prompt_template") or ""),
            parameters=tuple(RoutineParameter.from_dict(item) for item in parameters if isinstance(item, dict)),
            target_types=tuple(str(item) for item in targets),
            builtin=bool(data.get("builtin", False)),
            execution=dict(execution),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "routine_id": self.routine_id,
            "version": self.version,
            "name": self.name,
            "summary": self.summary,
            "description": self.description,
            "parameters": [parameter.to_dict() for parameter in self.parameters],
            "target_types": list(self.target_types),
            "builtin": self.builtin,
            "execution": dict(self.execution),
        }


@dataclass(frozen=True)
class RoutineResolution:
    values: dict[str, Any]
    missing: tuple[str, ...]
    rendered_prompt: str

    @property
    def ready(self) -> bool:
        return not self.missing

    def to_dict(self) -> dict[str, Any]:
        return {
            "values": dict(self.values),
            "missing": list(self.missing),
            "ready": self.ready,
            "rendered_prompt": self.rendered_prompt,
        }


class RoutineCatalog:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else Path(__file__).with_name("builtin_routines.json")
        self._cache_enabled = path is None
        self._cache: tuple[RoutineDefinition, ...] | None = None

    def list(self) -> list[RoutineDefinition]:
        if not self._cache_enabled:
            return self._load()
        with _CATALOG_LOCK:
            if self._cache is None:
                self._cache = tuple(self._load())
            return list(self._cache)

    def _load(self) -> list[RoutineDefinition]:
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"routine catalog is unavailable: {self.path}") from exc
        rows = raw.get("routines") if isinstance(raw, dict) else None
        if not isinstance(rows, list):
            raise RuntimeError("routine catalog must contain a routines array")
        routines = [RoutineDefinition.from_dict(item) for item in rows if isinstance(item, dict)]
        routines.sort(key=lambda routine: (routine.name.lower(), routine.routine_id, routine.version))
        return routines

    def get(self, routine_id: str, version: int | str | None = None) -> RoutineDefinition | None:
        matches = [routine for routine in self.list() if routine.routine_id == routine_id]
        if version not in (None, ""):
            try:
                requested = int(version)
            except (TypeError, ValueError):
                return None
            return next((routine for routine in matches if routine.version == requested), None)
        return matches[-1] if matches else None


def resolve_routine(
    routine: RoutineDefinition,
    *,
    params: dict[str, Any] | None = None,
    target: dict[str, Any] | None = None,
    project: dict[str, Any] | None = None,
    requester: dict[str, Any] | None = None,
    today: str = "",
) -> RoutineResolution:
    supplied = dict(params or {})
    known = {parameter.name for parameter in routine.parameters}
    unknown = sorted(set(supplied) - known)
    if unknown:
        raise ValueError(f"unknown routine parameters: {', '.join(unknown)}")
    values: dict[str, Any] = {}
    missing: list[str] = []
    for parameter in routine.parameters:
        value = supplied.get(parameter.name)
        if parameter.name not in supplied:
            value = _resolve_default(
                parameter.default,
                target=target,
                project=project,
                requester=requester,
                today=today,
            )
        if value is None or (isinstance(value, str) and not value.strip()):
            if parameter.required:
                missing.append(parameter.name)
            continue
        values[parameter.name] = _validate_parameter_value(parameter, value)
    unresolved_optional = {
        parameter.name
        for parameter in routine.parameters
        if not parameter.required and parameter.name not in values
    }
    rendered = (
        ""
        if missing
        else _render_prompt(
            routine.prompt_template,
            values,
            removable_placeholders=unresolved_optional,
        )
    )
    return RoutineResolution(values=values, missing=tuple(missing), rendered_prompt=rendered)


def _resolve_default(
    default: dict[str, Any] | None,
    *,
    target: dict[str, Any] | None,
    project: dict[str, Any] | None,
    requester: dict[str, Any] | None,
    today: str,
) -> Any:
    if default is None:
        return None
    source = str(default.get("source") or "")
    if source == "literal":
        return default.get("value")
    if source == "today":
        return today or datetime.now().astimezone().date().isoformat()
    if source == "target":
        return dict(target) if target else None
    if source == "project":
        return dict(project) if project else None
    if source == "requester":
        return dict(requester) if requester else None
    return None


def _validate_parameter_value(parameter: RoutineParameter, value: Any) -> Any:
    if parameter.allow_multiple:
        if not isinstance(value, list):
            raise ValueError(f"routine parameter {parameter.name!r} must be an array")
        if parameter.min_items and len(value) < parameter.min_items:
            raise ValueError(
                f"routine parameter {parameter.name!r} requires at least {parameter.min_items} items"
            )
        if parameter.max_items and len(value) > parameter.max_items:
            raise ValueError(
                f"routine parameter {parameter.name!r} allows at most {parameter.max_items} items"
            )
        return [_validate_scalar(parameter, item) for item in value]
    return _validate_scalar(parameter, value)


def _validate_scalar(parameter: RoutineParameter, value: Any) -> Any:
    if parameter.type == "boolean":
        if not isinstance(value, bool):
            raise ValueError(f"routine parameter {parameter.name!r} must be a boolean")
        return value
    if parameter.type == "integer":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"routine parameter {parameter.name!r} must be an integer")
        return value
    if parameter.type == "enum":
        if not isinstance(value, str) or (parameter.choices and value not in parameter.choices):
            raise ValueError(f"routine parameter {parameter.name!r} is not an allowed choice")
        return value
    if parameter.type in {"repository_ref", "pull_request_ref", "worker_ref"}:
        if not isinstance(value, (str, dict)):
            raise ValueError(f"routine parameter {parameter.name!r} must be a string or object")
        return dict(value) if isinstance(value, dict) else value.strip()
    if parameter.type == "model_ref":
        if not isinstance(value, dict):
            raise ValueError(f"routine parameter {parameter.name!r} must be an object")
        engine = str(value.get("engine") or "").strip()
        model = str(value.get("model") or "").strip()
        if engine not in {"codex", "claude"} or not model:
            raise ValueError(
                f"routine parameter {parameter.name!r} requires an engine and model"
            )
        return {"engine": engine, "model": model}
    if not isinstance(value, str):
        raise ValueError(f"routine parameter {parameter.name!r} must be a string")
    normalized = value.strip()
    if parameter.type == "date":
        try:
            date.fromisoformat(normalized)
        except ValueError as exc:
            raise ValueError(
                f"routine parameter {parameter.name!r} must be an ISO date"
            ) from exc
    return normalized


def _render_prompt(
    template: str,
    values: dict[str, Any],
    *,
    removable_placeholders: set[str] | None = None,
) -> str:
    removable = removable_placeholders or set()
    if removable:
        parts = re.split(r"((?<=[.!?])\s+|\n+)", template)
        kept: list[str] = []
        pending_separator = ""
        for index, part in enumerate(parts):
            if index % 2:
                if (part.count("\n"), len(part)) > (
                    pending_separator.count("\n"),
                    len(pending_separator),
                ):
                    pending_separator = part
                continue
            sentence = part.strip()
            if not sentence:
                continue
            placeholders = set(_TEMPLATE_PATTERN.findall(sentence))
            if placeholders and placeholders <= removable:
                continue
            if kept:
                kept.append(pending_separator or " ")
            kept.append(sentence)
            pending_separator = ""
        template = "".join(kept)

    def replace_match(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values and name in removable:
            return "not specified"
        value = values.get(name, "")
        if isinstance(value, bool):
            return "yes" if value else "no"
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True, separators=(",", ":"))
        return str(value)

    return _TEMPLATE_PATTERN.sub(replace_match, template).strip()


@dataclass(frozen=True)
class RoutineSchedule:
    schedule_id: str
    name: str
    routine_id: str
    routine_version: int
    project_id: str
    created_by: str
    hour: int
    minute: int
    creator_auth_mode: str = "legacy"
    first_eligible_date: str = ""
    weekdays: tuple[int, ...] = (0, 1, 2, 3, 4, 5, 6)
    timezone: str = "Europe/London"
    enabled: bool = True
    target: dict[str, Any] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    engine: str = ""
    model: str = ""
    effort: str = ""
    speed: str = ""
    worker_id: str = ""
    last_fired_date: str = ""
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self) -> None:
        if not self.routine_id or self.routine_version < 0 or not self.project_id or not self.created_by:
            raise ValueError("routine_id, routine_version, project_id, and created_by are required")
        if self.hour < 0 or self.hour > 23 or self.minute < 0 or self.minute > 59:
            raise ValueError("invalid schedule time")
        if not self.weekdays or any(day < 0 or day > 6 for day in self.weekdays):
            raise ValueError("weekdays must contain values between 0 and 6")
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"unknown timezone {self.timezone!r}") from exc
        if self.engine and self.engine not in {"codex", "claude"}:
            raise ValueError("schedule engine must be codex or claude")
        if self.creator_auth_mode not in {"legacy", "none", "oauth"}:
            raise ValueError("schedule creator auth mode must be legacy, none, or oauth")
        if self.first_eligible_date:
            try:
                date.fromisoformat(self.first_eligible_date)
            except ValueError as exc:
                raise ValueError("schedule first eligible date must be ISO-8601") from exc

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RoutineSchedule:
        return cls(
            schedule_id=str(data.get("schedule_id") or ""),
            name=str(data.get("name") or ""),
            routine_id=str(data.get("routine_id") or ""),
            routine_version=int(data.get("routine_version", 0)),
            project_id=str(data.get("project_id") or ""),
            created_by=str(data.get("created_by") or ""),
            hour=int(data.get("hour", 9)),
            minute=int(data.get("minute", 0)),
            creator_auth_mode=str(data.get("creator_auth_mode") or "legacy"),
            first_eligible_date=str(data.get("first_eligible_date") or ""),
            weekdays=tuple(int(day) for day in data.get("weekdays", [0, 1, 2, 3, 4, 5, 6])),
            timezone=str(data.get("timezone") or "Europe/London"),
            enabled=bool(data.get("enabled", True)),
            target=dict(data.get("target") or {}),
            params=dict(data.get("params") or {}),
            engine=str(data.get("engine") or ""),
            model=str(data.get("model") or ""),
            effort=str(data.get("effort") or ""),
            speed=str(data.get("speed") or ""),
            worker_id=str(data.get("worker_id") or ""),
            last_fired_date=str(data.get("last_fired_date") or ""),
            created_at=str(data.get("created_at") or ""),
            updated_at=str(data.get("updated_at") or ""),
        )

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["weekdays"] = list(self.weekdays)
        return data

    def local_date(self, now: datetime) -> str:
        return now.astimezone(ZoneInfo(self.timezone)).date().isoformat()

    def due(self, now: datetime) -> bool:
        if not self.enabled:
            return False
        local = now.astimezone(ZoneInfo(self.timezone))
        local_date = local.date().isoformat()
        return (
            local.weekday() in self.weekdays
            and (local.hour, local.minute) >= (self.hour, self.minute)
            and (not self.first_eligible_date or local_date >= self.first_eligible_date)
            and self.last_fired_date != local_date
        )


class RoutineScheduleStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()

    def list(self) -> list[RoutineSchedule]:
        with _STORE_LOCK:
            if not self.path.exists():
                return []
            try:
                raw = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return []
            rows = raw.get("schedules") if isinstance(raw, dict) else None
            if not isinstance(rows, list):
                return []
            schedules: list[RoutineSchedule] = []
            for row in rows:
                try:
                    if isinstance(row, dict):
                        schedules.append(RoutineSchedule.from_dict(row))
                except (TypeError, ValueError):
                    continue
            return sorted(schedules, key=lambda item: (item.name.lower(), item.schedule_id))

    def get(self, schedule_id: str) -> RoutineSchedule | None:
        return next((schedule for schedule in self.list() if schedule.schedule_id == schedule_id), None)

    def save(self, schedule: RoutineSchedule) -> RoutineSchedule:
        with _STORE_LOCK:
            schedules = self.list()
            schedules = [existing for existing in schedules if existing.schedule_id != schedule.schedule_id]
            schedules.append(schedule)
            self._save_all(schedules)
            return schedule

    def create(self, **values: Any) -> RoutineSchedule:
        now = utc_now()
        if not values.get("first_eligible_date"):
            values["first_eligible_date"] = _schedule_activation_date(
                now,
                hour=int(values.get("hour", 9)),
                minute=int(values.get("minute", 0)),
                timezone=str(values.get("timezone") or "Europe/London"),
            )
        return self.save(
            RoutineSchedule(
                schedule_id=new_id("sched"),
                created_at=now,
                updated_at=now,
                **values,
            )
        )

    def update(self, schedule_id: str, **changes: Any) -> RoutineSchedule | None:
        with _STORE_LOCK:
            schedule = self.get(schedule_id)
            if schedule is None:
                return None
            now = utc_now()
            activation_fields = {"hour", "minute", "timezone", "weekdays", "enabled"}
            if any(
                key in changes and changes[key] != getattr(schedule, key)
                for key in activation_fields
            ):
                changes["first_eligible_date"] = _schedule_activation_date(
                    now,
                    hour=int(changes.get("hour", schedule.hour)),
                    minute=int(changes.get("minute", schedule.minute)),
                    timezone=str(changes.get("timezone", schedule.timezone)),
                )
            updated = replace(schedule, **changes, updated_at=now)
            return self.save(updated)

    def delete(self, schedule_id: str) -> bool:
        with _STORE_LOCK:
            schedules = self.list()
            kept = [schedule for schedule in schedules if schedule.schedule_id != schedule_id]
            if len(kept) == len(schedules):
                return False
            self._save_all(kept)
            return True

    def due(self, now: datetime) -> list[RoutineSchedule]:
        return [schedule for schedule in self.list() if schedule.due(now)]

    def ack(self, schedule_id: str, now: datetime) -> RoutineSchedule | None:
        schedule = self.get(schedule_id)
        if schedule is None:
            return None
        return self.update(schedule_id, last_fired_date=schedule.local_date(now))

    def _save_all(self, schedules: list[RoutineSchedule]) -> None:
        atomic_write_json(
            self.path,
            {"schema_version": "1", "schedules": [schedule.to_dict() for schedule in schedules]},
        )


def _schedule_activation_date(
    now: str,
    *,
    hour: int,
    minute: int,
    timezone: str,
) -> str:
    try:
        zone = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone {timezone!r}") from exc
    local = datetime.fromisoformat(now.replace("Z", "+00:00")).astimezone(
        zone
    )
    # Creating or materially changing a schedule during its target minute is
    # treated as having missed that occurrence; it becomes eligible tomorrow.
    target_passed = (hour, minute) <= (local.hour, local.minute)
    eligible = local.date() + (timedelta(days=1) if target_passed else timedelta())
    return eligible.isoformat()


def routine_schedules_path(workspace: str | Path) -> Path:
    return Path(workspace).expanduser() / "routine-schedules.json"
