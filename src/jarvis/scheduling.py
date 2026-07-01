"""Alarm and timer scheduling primitives."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta


def _localnow(now: float, tz_name: str = "") -> datetime:
    if tz_name:
        try:
            from zoneinfo import ZoneInfo

            return datetime.fromtimestamp(now, ZoneInfo(tz_name))
        except Exception:
            pass
    return datetime.fromtimestamp(now).astimezone()


def next_at(now: float, hh: int, mm: int, tz_name: str = "") -> float:
    base = _localnow(now, tz_name)
    target = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target.timestamp() <= now:
        target = target + timedelta(days=1)
    return target.timestamp()


def due_from(
    now: float,
    *,
    minutes: float | None = None,
    seconds: float | None = None,
    at: str | None = None,
    tz_name: str = "",
) -> float | None:
    if seconds is not None:
        return now + float(seconds)
    if minutes is not None:
        return now + float(minutes) * 60.0
    if at:
        hh, mm = _parse_clock(at)
        if hh is not None:
            return next_at(now, hh, mm, tz_name)
    return None


def in_quiet_hours(now: float, start: str, end: str, tz_name: str = "") -> bool:
    sh, sm = _parse_clock(start)
    eh, em = _parse_clock(end)
    if sh is None or eh is None:
        return False
    cur = _localnow(now, tz_name)
    mins = cur.hour * 60 + cur.minute
    start_mins, end_mins = sh * 60 + sm, eh * 60 + em
    if start_mins == end_mins:
        return False
    return (
        start_mins <= mins < end_mins
        if start_mins < end_mins
        else mins >= start_mins or mins < end_mins
    )


def _parse_clock(text: str) -> tuple[int | None, int]:
    import re

    m = re.match(r"\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\s*$", text.strip(), re.IGNORECASE)
    if not m:
        return None, 0
    hh = int(m.group(1))
    mm = int(m.group(2) or 0)
    ap = (m.group(3) or "").lower()
    if ap == "pm" and hh < 12:
        hh += 12
    elif ap == "am" and hh == 12:
        hh = 0
    if 0 <= hh <= 23 and 0 <= mm <= 59:
        return hh, mm
    return None, 0


@dataclass
class Alarm:
    label: str
    due: float
    device_id: str
    ring_s: float
    quiet_s: float
    max_s: float
    channel: str = "voice"
    identity: str = "house"
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    _last_cycle: int = -1

    @property
    def cycle(self) -> float:
        return max(0.5, self.ring_s + self.quiet_s)


@dataclass
class Ring:
    alarm_id: str
    label: str
    device_id: str
    channel: str
    identity: str
    ring_s: float
    first: bool


class Scheduler:
    def __init__(self) -> None:
        self._alarms: dict[str, Alarm] = {}

    def add(self, alarm: Alarm) -> str:
        self._alarms[alarm.id] = alarm
        return alarm.id

    def cancel(self, ref: str) -> str | None:
        alarm = self._alarms.get(ref)
        if alarm is None:
            ref_l = ref.strip().lower()
            alarm = next((item for item in self._alarms.values() if item.label.lower() == ref_l), None)
        if alarm is None:
            return None
        del self._alarms[alarm.id]
        return alarm.label

    def acknowledge(self, device_id: str) -> Alarm | None:
        stopped = self.acknowledge_all(device_id)
        return stopped[0] if stopped else None

    def acknowledge_all(self, device_id: str) -> list[Alarm]:
        stopped: list[Alarm] = []
        for alarm in list(self._alarms.values()):
            if alarm.device_id == device_id and alarm._last_cycle >= 0:
                del self._alarms[alarm.id]
                stopped.append(alarm)
        return stopped

    def ringing_on(self, device_id: str) -> bool:
        return any(alarm.device_id == device_id and alarm._last_cycle >= 0 for alarm in self._alarms.values())

    def all(self) -> list[Alarm]:
        return sorted(self._alarms.values(), key=lambda alarm: alarm.due)

    def tick(self, now: float) -> list[Ring]:
        rings: list[Ring] = []
        for alarm in list(self._alarms.values()):
            elapsed = now - alarm.due
            if elapsed < 0:
                continue
            if elapsed >= alarm.max_s:
                del self._alarms[alarm.id]
                continue
            cycle_index = int(elapsed // alarm.cycle)
            if cycle_index > alarm._last_cycle:
                first = alarm._last_cycle < 0
                alarm._last_cycle = cycle_index
                rings.append(
                    Ring(
                        alarm_id=alarm.id,
                        label=alarm.label,
                        device_id=alarm.device_id,
                        channel=alarm.channel,
                        identity=alarm.identity,
                        ring_s=alarm.ring_s,
                        first=first,
                    )
                )
        return rings
