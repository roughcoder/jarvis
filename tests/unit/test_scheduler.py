"""Alarm scheduler — time math + the repeat-until-acknowledged cadence (pure logic)."""

from __future__ import annotations

from jarvis.brain.scheduler import Alarm, Scheduler, due_from, next_at


def _alarm(due: float, **kw) -> Alarm:
    base = dict(label="tea", due=due, device_id="mac", ring_s=10.0, quiet_s=10.0, max_s=300.0)
    base.update(kw)
    return Alarm(**base)


# --- time resolution -------------------------------------------------------

def test_due_from_relative_minutes() -> None:
    assert due_from(1000.0, minutes=30, at=None) == 1000.0 + 30 * 60


def test_due_from_absolute_clock_is_in_the_future() -> None:
    now = 1_000_000.0
    due = due_from(now, minutes=None, at="10:20")
    assert due is not None and due > now  # next occurrence is always ahead


def test_next_at_rolls_to_tomorrow_when_past() -> None:
    now = 1_000_000.0
    # whatever local time `now` is, both 00:00 and 23:59 resolve to a future epoch
    assert next_at(now, 0, 0) > now
    assert next_at(now, 23, 59) > now


def test_parse_clock_handles_am_pm() -> None:
    base = 1_000_000.0
    assert due_from(base, minutes=None, at="12am") is not None  # midnight
    assert due_from(base, minutes=None, at="nonsense") is None


# --- the ring/quiet cadence ------------------------------------------------

def test_no_ring_before_due() -> None:
    s = Scheduler()
    s.add(_alarm(due=100.0))
    assert s.tick(50.0) == []


def test_first_ring_at_due_then_once_per_cycle() -> None:
    s = Scheduler()
    s.add(_alarm(due=100.0))  # ring 10s, quiet 10s → cycle 20s
    assert [r.first for r in s.tick(100.0)] == [True]  # fires at due, marked first
    assert s.tick(105.0) == []  # still in the same ring window → no new ring
    assert s.tick(112.0) == []  # quiet part of cycle 0 → no ring
    r1 = s.tick(120.0)  # cycle 1 begins → ring again, not first
    assert len(r1) == 1 and r1[0].first is False
    assert s.tick(125.0) == []
    assert len(s.tick(140.0)) == 1  # cycle 2


def test_acknowledge_stops_the_ringing() -> None:
    s = Scheduler()
    s.add(_alarm(due=100.0))
    s.tick(100.0)  # it's now ringing
    assert s.ringing_on("mac") is True
    stopped = s.acknowledge("mac")
    assert stopped is not None and stopped.label == "tea"
    assert s.tick(120.0) == []  # gone — no more rings
    assert s.ringing_on("mac") is False


def test_acknowledge_only_a_ringing_alarm() -> None:
    s = Scheduler()
    s.add(_alarm(due=100.0))  # not yet ringing (pending)
    assert s.acknowledge("mac") is None  # nothing ringing to stop


def test_max_s_auto_stops() -> None:
    s = Scheduler()
    s.add(_alarm(due=100.0, max_s=60.0))
    s.tick(100.0)
    assert s.ringing_on("mac")
    assert s.tick(161.0) == []  # past max_s → auto-removed
    assert s.ringing_on("mac") is False


def test_cancel_by_label_and_id() -> None:
    s = Scheduler()
    a = _alarm(due=100.0, label="Tea Time")
    s.add(a)
    assert s.cancel("tea time") == "Tea Time"  # case-insensitive label
    assert s.cancel(a.id) is None  # already gone
    s.add(_alarm(due=200.0, label="x"))
    assert s.cancel("nope") is None


def test_rings_target_their_own_device() -> None:
    s = Scheduler()
    s.add(_alarm(due=100.0, device_id="mac"))
    s.add(_alarm(due=100.0, device_id="pi", label="pills"))
    rings = s.tick(100.0)
    assert {r.device_id for r in rings} == {"mac", "pi"}
