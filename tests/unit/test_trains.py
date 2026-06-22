"""train_times tool — RTT parsing/selection (pure) + the handler (mocked network).

The arrive-by logic is the whole point (LLM-browsing kept getting it wrong), so it's
pinned here: search a window before the deadline, keep services arriving in time,
prefer the latest. No network — RTTClient.journeys is stubbed for the handler test.
"""

from __future__ import annotations

import asyncio
import datetime as dt

import jarvis.tools.trains as T
from jarvis.brain.context import RequestContext
from jarvis.config import SecretStr, TrainsConfig


def test_resolve_crs() -> None:
    assert T.resolve_crs("BKA") == "BKA"
    assert T.resolve_crs("bka") == "BKA"  # any 3 letters = a code
    assert T.resolve_crs("bookham") == "BKA"
    assert T.resolve_crs("London Waterloo") == "WAT"
    assert T.resolve_crs("nowhere town") is None


def test_parse_clock() -> None:
    assert T.parse_clock("9am") == "0900"
    assert T.parse_clock("09:00") == "0900"
    assert T.parse_clock("0900") == "0900"
    assert T.parse_clock("5.30pm") == "1730"
    assert T.parse_clock("12am") == "0000"
    assert T.parse_clock("12pm") == "1200"
    assert T.parse_clock("") is None and T.parse_clock("teatime") is None


def test_resolve_date() -> None:
    today = dt.date(2026, 6, 22)
    assert T.resolve_date("today", today) == today
    assert T.resolve_date("", today) == today
    assert T.resolve_date("tomorrow", today) == dt.date(2026, 6, 23)
    assert T.resolve_date("2026-06-25", today) == dt.date(2026, 6, 25)
    assert T.resolve_date("nonsense", today) == today  # falls back, never crashes


def test_minus_minutes() -> None:
    assert T._minus_minutes("0900", 120) == "0700"
    assert T._minus_minutes("0030", 120) == "2230"  # wraps cleanly


_SEARCH = {
    "services": [
        {"serviceUid": "A1", "runDate": "2026-06-22", "atocName": "South Western Railway",
         "locationDetail": {"gbttBookedDeparture": "0750", "destination": [{"description": "London Waterloo"}]}},
        {"serviceUid": "A2", "runDate": "2026-06-22", "atocCode": "SW",
         "locationDetail": {"gbttBookedDeparture": "0820", "destination": [{"description": "London Waterloo"}]}},
        {"serviceUid": "skip", "locationDetail": {}},  # no departure -> dropped
    ]
}


def test_parse_search() -> None:
    svcs = T.parse_search(_SEARCH)
    assert [s["uid"] for s in svcs] == ["A1", "A2"]
    assert svcs[0]["dep"] == "0750" and svcs[0]["toc"] == "South Western Railway"
    assert T.parse_search({}) == []


def test_arrival_from_detail() -> None:
    detail = {"locations": [
        {"crs": "BKA", "gbttBookedDeparture": "0750"},
        {"crs": "WAT", "gbttBookedArrival": "0842"},
    ]}
    assert T.arrival_from_detail(detail, "WAT") == "0842"
    assert T.arrival_from_detail(detail, "wat") == "0842"  # case-insensitive
    assert T.arrival_from_detail(detail, "VXH") is None


def test_select_arrive_by_keeps_latest_in_time() -> None:
    svcs = [
        {"dep": "0700", "arr": "0752"},
        {"dep": "0750", "arr": "0842"},
        {"dep": "0820", "arr": "0901"},  # arrives after 09:00 — excluded
    ]
    picked = T.select(svcs, arrive_by="0900")
    assert [s["dep"] for s in picked] == ["0700", "0750"]  # the two that make it
    assert T.select(svcs, arrive_by="0700") == []  # none in time


def test_select_next_departures() -> None:
    svcs = [{"dep": f"{h:02d}00", "arr": f"{h:02d}50"} for h in range(7, 14)]
    assert len(T.select(svcs, arrive_by=None)) == 5  # next handful


def test_format_services() -> None:
    out = T.format_services([{"dep": "0750", "arr": "0842", "toc": "SWR"}], "BKA", "WAT")
    assert "07:50 dep" in out and "08:42 arr" in out and "SWR" in out
    assert "No direct trains" in T.format_services([], "BKA", "WAT")


def test_handler_arrive_by(monkeypatch) -> None:  # noqa: ANN001
    async def fake_journeys(self, origin, dest, date, *, search_time, arrive_by):  # noqa: ANN001
        assert (origin, dest) == ("BKA", "WAT")
        assert search_time == "0700"  # 09:00 deadline -> searched 2h earlier
        return [{"dep": "0750", "arr": "0842", "toc": "SWR"}]

    monkeypatch.setattr(T.RTTClient, "journeys", fake_journeys)
    cfg = TrainsConfig(rtt_username="u", rtt_password=SecretStr("p"))
    tool = T.make_trains_tools(cfg)[0]
    ctx = RequestContext("cli", "neil", "personal", frozenset({"web.search"}))
    out = asyncio.run(tool.handler(ctx, {"from": "bookham", "to": "waterloo", "arrive_by": "09:00"}))
    assert "arrive by 09:00" in out and "07:50 dep" in out and "08:42 arr" in out


def test_handler_unknown_station() -> None:
    cfg = TrainsConfig(rtt_username="u", rtt_password=SecretStr("p"))
    tool = T.make_trains_tools(cfg)[0]
    ctx = RequestContext("cli", "neil", "personal", frozenset({"web.search"}))
    out = asyncio.run(tool.handler(ctx, {"from": "nowhere town", "to": "WAT"}))
    assert out.startswith("error:") and "station code" in out


def test_disabled_registers_nothing() -> None:
    assert T.make_trains_tools(TrainsConfig()) == []  # no creds -> no tool
