"""Scheduler — alarms & timers as scheduled proactive events.

Pure scheduling logic (clock + delivery are injected, so it's fully unit-testable).
An alarm fires at its due time and then REPEATS on a ring/quiet cadence until it's
acknowledged ('stop') or `max_s` elapses (a safety auto-stop). Each ring is emitted
once per cycle; the delivery layer turns a ring into a sound + (on the first ring) a
spoken label on the device that set it.

The same shape later covers reminders and recurring jobs — an alarm is just a
proactive event with a repeat cadence and a device target.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta


def _localnow(now: float, tz_name: str = "") -> datetime:
    if tz_name:
        try:
            from zoneinfo import ZoneInfo

            return datetime.fromtimestamp(now, ZoneInfo(tz_name))
        except Exception:  # noqa: BLE001 - bad tz → local
            pass
    return datetime.fromtimestamp(now).astimezone()


def next_at(now: float, hh: int, mm: int, tz_name: str = "") -> float:
    """Epoch of the next occurrence of clock time hh:mm (today if still ahead, else
    tomorrow)."""
    base = _localnow(now, tz_name)
    target = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target.timestamp() <= now:
        target = target + timedelta(days=1)
    return target.timestamp()


def due_from(
    now: float, *, minutes: float | None = None, seconds: float | None = None,
    at: str | None = None, tz_name: str = "",
) -> float | None:
    """Resolve a due epoch from a relative `seconds`/`minutes` OR an absolute `at`
    ('10:20', '10:20am', '22:05'). Returns None if none is usable."""
    if seconds is not None:
        return now + float(seconds)
    if minutes is not None:
        return now + float(minutes) * 60.0
    if at:
        hh, mm = _parse_clock(at)
        if hh is not None:
            return next_at(now, hh, mm, tz_name)
    return None


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
    _last_cycle: int = -1  # highest ring-cycle index already emitted

    @property
    def cycle(self) -> float:
        return max(0.5, self.ring_s + self.quiet_s)


@dataclass
class Ring:
    """One emitted ring of an alarm — the delivery layer plays the tone (and, on the
    first ring, speaks the label) on `device_id`."""

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

    # --- mutation -----------------------------------------------------------
    def add(self, alarm: Alarm) -> str:
        self._alarms[alarm.id] = alarm
        return alarm.id

    def cancel(self, ref: str) -> str | None:
        """Cancel by id or (case-insensitive) label. Returns the cancelled label."""
        a = self._alarms.get(ref)
        if a is None:
            ref_l = ref.strip().lower()
            a = next((x for x in self._alarms.values() if x.label.lower() == ref_l), None)
        if a is None:
            return None
        del self._alarms[a.id]
        return a.label

    def acknowledge(self, device_id: str) -> Alarm | None:
        """Stop the alarm currently ringing on `device_id` (what 'stop' acknowledges).
        Returns it, or None if nothing was ringing."""
        for a in list(self._alarms.values()):
            if a.device_id == device_id and a._last_cycle >= 0:  # has started ringing
                del self._alarms[a.id]
                return a
        return None

    def ringing_on(self, device_id: str) -> bool:
        return any(a.device_id == device_id and a._last_cycle >= 0 for a in self._alarms.values())

    def all(self) -> list[Alarm]:
        return sorted(self._alarms.values(), key=lambda a: a.due)

    # --- the clock tick -----------------------------------------------------
    def tick(self, now: float) -> list[Ring]:
        """Advance to `now`; return the rings to deliver this tick. Auto-removes alarms
        that have exceeded their max ringing duration."""
        rings: list[Ring] = []
        for a in list(self._alarms.values()):
            elapsed = now - a.due
            if elapsed < 0:
                continue  # not due yet
            if elapsed >= a.max_s:
                del self._alarms[a.id]  # safety auto-stop
                continue
            cycle_index = int(elapsed // a.cycle)
            if cycle_index > a._last_cycle:
                first = a._last_cycle < 0
                a._last_cycle = cycle_index
                rings.append(
                    Ring(
                        alarm_id=a.id, label=a.label, device_id=a.device_id,
                        channel=a.channel, identity=a.identity, ring_s=a.ring_s, first=first,
                    )
                )
        return rings
