"""Alarm/timer tools — the brain's voice into the scheduler (gated `alarms.set`).

`set_alarm` handles both 'in 30 minutes' (minutes) and 'at 10:20' (at). The alarm is
bound to the asking device (it rings there), repeats on the configured ring/quiet
cadence until acknowledged, and is delivered by the scheduler's tick loop. These are
instant, local registry operations — no network.
"""

from __future__ import annotations

import time

from jarvis.runtime import RequestContext
from jarvis.config import Config
from jarvis.scheduling import Alarm, Scheduler, due_from
from jarvis.tools.base import Tool

_CAP = "alarms.set"


def _humanize(due: float, now: float) -> str:
    from datetime import datetime

    delta = due - now
    if delta < 60:
        secs = max(1, round(delta))
        return f"in {secs} second{'s' if secs != 1 else ''}"
    if delta < 3600:
        mins = max(1, round(delta / 60))
        return f"in {mins} minute{'s' if mins != 1 else ''}"
    return datetime.fromtimestamp(due).astimezone().strftime("%-I:%M %p").lower()


def make_alarm_tools(scheduler: Scheduler, cfg: Config) -> list[Tool]:
    acfg = cfg.alarm
    tz = cfg.persona.timezone

    async def set_alarm(ctx: RequestContext, args: dict) -> str:
        label = (args.get("label") or "").strip()
        minutes = args.get("minutes")
        seconds = args.get("seconds")
        at = (args.get("at") or "").strip() or None
        now = time.time()
        due = due_from(
            now,
            minutes=float(minutes) if minutes not in (None, "") else None,
            seconds=float(seconds) if seconds not in (None, "") else None,
            at=at, tz_name=tz,
        )
        if due is None:
            return "error: tell me when — a number of minutes, or a clock time like '10:20' or '7am'"
        alarm = Alarm(
            label=label or "alarm", due=due, device_id=ctx.device_id, channel=ctx.channel,
            identity=ctx.identity, ring_s=acfg.ring_s, quiet_s=acfg.quiet_s, max_s=acfg.max_s,
        )
        scheduler.add(alarm)
        when = _humanize(due, now)
        return f"Alarm set for {when}" + (f" — {label}." if label else ".") + " It'll ring until you say stop."

    async def cancel_alarm(ctx: RequestContext, args: dict) -> str:
        ref = (args.get("which") or "").strip()
        if not ref:  # cancel whatever is ringing on this device, else the soonest pending one
            stopped = scheduler.acknowledge_all(ctx.device_id)
            if stopped:
                if len(stopped) == 1:
                    return f"Stopped the {stopped[0].label!r} alarm."
                return f"Stopped {len(stopped)} ringing alarms."
            mine = [a for a in scheduler.all() if a.device_id == ctx.device_id]
            if not mine:
                return "There are no alarms set."
            scheduler.cancel(mine[0].id)
            return f"Cancelled the {mine[0].label!r} alarm."
        label = scheduler.cancel(ref)
        return f"Cancelled the {label!r} alarm." if label else f"No alarm called {ref!r}."

    async def list_alarms(ctx: RequestContext, args: dict) -> str:
        mine = [a for a in scheduler.all() if a.device_id == ctx.device_id]
        if not mine:
            return "No alarms set."
        now = time.time()
        return "; ".join(f"{a.label} ({_humanize(a.due, now)})" for a in mine) + "."

    obj = "object"
    return [
        Tool(
            "set_alarm",
            "Set an alarm or timer that rings on this device. Give either `minutes` "
            "(e.g. 'in 30 minutes' → 30) or `at` (a clock time like '10:20', '7am', "
            "'10:20pm'), plus an optional short label. It repeats until the user says stop.",
            {
                "type": obj,
                "properties": {
                    "label": {"type": "string", "description": "Short label, e.g. 'tea', 'leave for the train'."},
                    "seconds": {"type": "number", "description": "Fire this many seconds from now (a short timer)."},
                    "minutes": {"type": "number", "description": "Fire this many minutes from now (a timer)."},
                    "at": {"type": "string", "description": "Clock time to fire at, e.g. '10:20', '7am'."},
                },
            },
            _CAP, set_alarm,
        ),
        Tool(
            "cancel_alarm",
            "Cancel or stop an alarm. With no argument, stops whatever is ringing now "
            "(or cancels the soonest one); or name one by its label.",
            {"type": obj, "properties": {"which": {"type": "string", "description": "Label to cancel (optional)."}}},
            _CAP, cancel_alarm,
        ),
        Tool(
            "list_alarms",
            "List the alarms/timers set on this device and when they'll fire.",
            {"type": obj, "properties": {}},
            _CAP, list_alarms,
        ),
    ]
