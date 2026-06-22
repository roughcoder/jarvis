"""train_times — deterministic UK train times via the Realtime Trains Pull API.

Replaces LLM-browsing a departure board (flaky on "arrive by X") with a real JSON
API: search direct services between two stations, then read each service's detail
for the arrival time at the destination. Gated `web.search` (a read-only online
lookup, granted wherever search is). Registered only when RTT creds are set.

The HTTP lives in `RTTClient`; the parsing/selection are pure module functions so
they unit-test without a network. RTT's `/search/<from>/to/<to>` returns through
(direct) services that call at both stations — changes aren't modelled here.
"""

from __future__ import annotations

import datetime as _dt
import re

import httpx

from jarvis.brain.context import RequestContext
from jarvis.config import TrainsConfig
from jarvis.tools.base import Tool

_CAP = "web.search"

# Common stations near the user + major London termini. The model usually passes a
# CRS code directly; this just rescues plain names. Any 3-letter token is taken as a code.
_NAME_TO_CRS = {
    "effingham": "EFF", "effingham junction": "EFF", "bookham": "BKA",
    "guildford": "GLD", "woking": "WOK", "clandon": "CLA", "horsley": "HSY",
    "leatherhead": "LHD", "dorking": "DKG", "epsom": "EPS", "surbiton": "SUR",
    "cobham": "CSD", "clapham junction": "CLJ", "wimbledon": "WIM",
    "london waterloo": "WAT", "waterloo": "WAT", "london vauxhall": "VXH",
    "vauxhall": "VXH", "london victoria": "VIC", "victoria": "VIC",
    "london bridge": "LBG", "richmond": "RMD", "raynes park": "RAY",
}


def resolve_crs(s: str) -> str | None:
    """A 3-letter CRS code (returned upper-cased) or a known station name -> code."""
    s = (s or "").strip()
    if re.fullmatch(r"[A-Za-z]{3}", s):
        return s.upper()
    return _NAME_TO_CRS.get(s.lower())


def parse_clock(s: str) -> str | None:
    """A loose clock string -> 'HHMM' (24h). '9am'/'09:00'/'0900'/'5.30pm' -> '0900'."""
    s = (s or "").strip().lower()
    if not s:
        return None
    m = re.fullmatch(r"(\d{1,2})[:.]?(\d{2})?\s*(am|pm)?", s)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2) or 0)
    ap = m.group(3)
    if ap == "pm" and hh != 12:
        hh += 12
    if ap == "am" and hh == 12:
        hh = 0
    if hh > 23 or mm > 59:
        return None
    return f"{hh:02d}{mm:02d}"


def resolve_date(s: str, today: _dt.date) -> _dt.date:
    """'today'/'tomorrow'/'' -> a date; or an explicit yyyy-mm-dd / dd-mm-yyyy."""
    s = (s or "").strip().lower()
    if s in ("", "today"):
        return today
    if s == "tomorrow":
        return today + _dt.timedelta(days=1)
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return _dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return today


def _minus_minutes(hhmm: str, minutes: int) -> str:
    t = _dt.datetime.strptime(hhmm, "%H%M") - _dt.timedelta(minutes=minutes)
    return t.strftime("%H%M")


def _fmt(hhmm: str | None) -> str:
    return f"{hhmm[:2]}:{hhmm[2:]}" if hhmm and len(hhmm) == 4 else "??:??"


def parse_search(obj: dict) -> list[dict]:
    """Map an RTT /search response to candidate services (departure side)."""
    out: list[dict] = []
    for svc in (obj or {}).get("services") or []:
        ld = svc.get("locationDetail") or {}
        dep = ld.get("gbttBookedDeparture") or ld.get("realtimeDeparture")
        if not dep or not svc.get("serviceUid"):
            continue
        dests = ld.get("destination") or []
        out.append({
            "uid": svc["serviceUid"],
            "run_date": svc.get("runDate", ""),
            "dep": dep,
            "toc": svc.get("atocName") or svc.get("atocCode") or "",
            "final": dests[0].get("description", "") if dests else "",
        })
    return out


def arrival_from_detail(obj: dict, dest_crs: str) -> str | None:
    """From an RTT /service detail, the booked arrival time (HHMM) at `dest_crs`."""
    for loc in (obj or {}).get("locations") or []:
        if (loc.get("crs") or "").upper() == dest_crs.upper():
            return loc.get("gbttBookedArrival") or loc.get("realtimeArrival")
    return None


def select(services: list[dict], *, arrive_by: str | None) -> list[dict]:
    """Pick the services to report. arrive_by: keep those arriving in time, prefer the
    latest few (with one earlier backup). Otherwise the next handful of departures."""
    timed = [s for s in services if s.get("arr")]
    if arrive_by:
        ok = [s for s in timed if s["arr"] <= arrive_by]
        return ok[-3:] if ok else []
    return (timed or services)[:5]


def format_services(services: list[dict], origin: str, dest: str) -> str:
    if not services:
        return f"No direct trains found from {origin} to {dest} for that time."
    lines = []
    for s in services:
        arr = f" → {_fmt(s.get('arr'))} arr" if s.get("arr") else ""
        toc = f" ({s['toc']})" if s.get("toc") else ""
        lines.append(f"{_fmt(s['dep'])} dep{arr}{toc}")
    return "\n".join(lines)


class RTTClient:
    def __init__(self, cfg: TrainsConfig) -> None:
        self._cfg = cfg
        self._auth = (cfg.rtt_username, cfg.rtt_password.get_secret_value())

    async def _get(self, client: httpx.AsyncClient, path: str) -> dict:
        r = await client.get(f"{self._cfg.base_url}{path}", auth=self._auth)
        r.raise_for_status()
        return r.json()

    async def journeys(
        self, origin: str, dest: str, date: _dt.date, *, search_time: str | None, arrive_by: str | None
    ) -> list[dict]:
        d = date.strftime("%Y/%m/%d")
        path = f"/search/{origin}/to/{dest}/{d}" + (f"/{search_time}" if search_time else "")
        async with httpx.AsyncClient(timeout=self._cfg.timeout_s) as client:
            services = parse_search(await self._get(client, path))
            # Fill arrival times from each service's detail (bounded for latency).
            for s in services[: self._cfg.max_detail_lookups]:
                rd = (s.get("run_date") or date.isoformat()).replace("-", "/")
                try:
                    detail = await self._get(client, f"/service/{s['uid']}/{rd}")
                    s["arr"] = arrival_from_detail(detail, dest)
                except Exception:  # noqa: BLE001 - skip a bad service, keep the rest
                    s["arr"] = None
        return select(services, arrive_by=arrive_by)


def make_trains_tools(cfg: TrainsConfig) -> list[Tool]:
    if not cfg.enabled:
        return []
    client = RTTClient(cfg)

    async def train_times(ctx: RequestContext, args: dict) -> str:
        origin = resolve_crs(args.get("from") or args.get("origin") or "")
        dest = resolve_crs(args.get("to") or args.get("destination") or "")
        if not origin or not dest:
            bad = args.get("from") if not origin else args.get("to")
            return f"error: I don't know the station code for {bad!r} — give me the station name or its 3-letter CRS code."
        date = resolve_date(args.get("date") or "", _dt.date.today())
        arrive_by = parse_clock(args.get("arrive_by") or "")
        depart_after = parse_clock(args.get("depart_after") or args.get("time") or "")
        # arrive-by: search ~2h before the deadline so we actually catch trains that
        # arrive in time (RTT lists departures forward from the search time).
        search_time = depart_after or (_minus_minutes(arrive_by, 120) if arrive_by else None)
        try:
            services = await client.journeys(
                origin, dest, date, search_time=search_time, arrive_by=arrive_by
            )
        except Exception as exc:  # noqa: BLE001 - API down/auth/etc. — never break the turn
            return f"error: couldn't reach the train times service ({type(exc).__name__})."
        header = f"{origin} → {dest}, {date.strftime('%a %-d %b')}"
        if arrive_by:
            header += f" (arrive by {_fmt(arrive_by)})"
        return f"{header}:\n{format_services(services, origin, dest)}"

    return [
        Tool(
            name="train_times",
            description=(
                "Get real UK train times between two stations (live data from Realtime "
                "Trains). Use this for ANY train question — next trains, a specific "
                "departure time, or arriving somewhere by a deadline. Pass stations as "
                "names or 3-letter CRS codes. Prefer this over the browser for trains."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "from": {"type": "string", "description": "Origin station (name or CRS code)."},
                    "to": {"type": "string", "description": "Destination station (name or CRS code)."},
                    "date": {"type": "string", "description": "'today', 'tomorrow', or yyyy-mm-dd. Default today."},
                    "depart_after": {"type": "string", "description": "Earliest departure time, e.g. '17:30' (optional)."},
                    "arrive_by": {"type": "string", "description": "Arrival deadline, e.g. '09:00' — for 'get there by' (optional)."},
                },
                "required": ["from", "to"],
            },
            required_capability=_CAP,
            handler=train_times,
            announce=True,  # network round-trips — earn the "looking that up" pulse
        ),
    ]
