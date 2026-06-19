"""Browser tools — the brain's gated client to the worker's browser host (CDP/nodriver).

Granular actions the tool loop composes: open → snapshot → click/type → read, the
reliable snapshot-act-verify loop OpenClaw/Hermes use. Each is a thin HTTP call to
the worker (where the real Chrome lives), gated by `worker.browser` (deny-by-default).
`context` picks which browser — `device` (the machine's Chrome) or `jarvis` (his own
profile); omit it to use the per-device default.
"""

from __future__ import annotations

from typing import Any

import httpx

from jarvis.brain.context import RequestContext
from jarvis.config import BrowserConfig, WorkerConfig
from jarvis.tools.base import Tool

_CAP = "worker.browser"


def make_browser_tools(worker: WorkerConfig, browser: BrowserConfig) -> list[Tool]:
    def headers() -> dict[str, str]:
        tok = worker.token.get_secret_value()
        return {"Authorization": f"Bearer {tok}"} if tok else {}

    async def post(action: str, args: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=browser.request_timeout_s) as client:
            r = await client.post(
                f"{worker.base_url}/run", json={"action": action, "args": args}, headers=headers()
            )
            return r.json()

    def _ctx_arg(args: dict) -> dict:
        c = (args.get("context") or "").strip()
        return {"context": c} if c else {}

    async def _call(action: str, args: dict) -> dict:
        try:
            return await post(action, args)
        except Exception as exc:  # noqa: BLE001 - worker may be down
            return {"ok": False, "error": f"browser unreachable ({exc})"}

    async def open_url(ctx: RequestContext, args: dict) -> str:
        data = await _call("browser_open", {"url": args.get("url", ""), **_ctx_arg(args)})
        if not data.get("ok"):
            return f"error: {data.get('error', 'open failed')}"
        return (
            f"Opened {data.get('title') or '(untitled)'} — {data.get('url')}. "
            "To READ the page's text/answer use browser_read; to ACT (click/type) use browser_snapshot first."
        )

    async def snapshot(ctx: RequestContext, args: dict) -> str:
        data = await _call("browser_snapshot", _ctx_arg(args))
        if not data.get("ok"):
            return f"error: {data.get('error', 'snapshot failed')}"
        elements = data.get("elements") or ""
        hint = "" if elements.startswith("[") else "  (No clickable elements — to read the page's text, use browser_read.)"
        return f"{data.get('title') or ''} — {data.get('url')}\n{elements}{hint}"

    async def click(ctx: RequestContext, args: dict) -> str:
        data = await _call("browser_click", {"ref": args.get("ref"), **_ctx_arg(args)})
        if not data.get("ok"):
            return f"error: {data.get('error', 'click failed')}"
        return f"Clicked [{args.get('ref')}]. Now at {data.get('title') or data.get('url')}. Snapshot again to continue."

    async def type_text(ctx: RequestContext, args: dict) -> str:
        data = await _call(
            "browser_type",
            {"ref": args.get("ref"), "text": args.get("text", ""), "submit": bool(args.get("submit")), **_ctx_arg(args)},
        )
        if not data.get("ok"):
            return f"error: {data.get('error', 'type failed')}"
        return f"Typed into [{args.get('ref')}]. Now at {data.get('title') or data.get('url')}."

    async def read(ctx: RequestContext, args: dict) -> str:
        data = await _call("browser_read", _ctx_arg(args))
        if not data.get("ok"):
            return f"error: {data.get('error', 'read failed')}"
        return f"{data.get('title') or ''} — {data.get('url')}\n{data.get('text')}"

    obj = "object"
    context_param = {
        "context": {
            "type": "string",
            "enum": ["device", "jarvis"],
            "description": "Which browser: 'device' (the machine's Chrome + its logins) or "
            "'jarvis' (his own profile). Omit to use the device default.",
        }
    }
    return [
        Tool(
            "browser_open",
            "Open a URL in the browser (interactive web — forms, availability, bookings, "
            "anything behind a click). Returns the page title; then call browser_snapshot.",
            {"type": obj, "properties": {"url": {"type": "string"}, **context_param}, "required": ["url"]},
            _CAP, open_url, announce=True,
        ),
        Tool(
            "browser_snapshot",
            "List the page's interactive elements, each with a [ref] number you act on. "
            "Always snapshot before clicking/typing, and again after the page changes.",
            {"type": obj, "properties": {**context_param}},
            _CAP, snapshot, announce=True,
        ),
        Tool(
            "browser_click",
            "Click an element by its [ref] from the latest snapshot.",
            {"type": obj, "properties": {"ref": {"type": "integer"}, **context_param}, "required": ["ref"]},
            _CAP, click, announce=True,
        ),
        Tool(
            "browser_type",
            "Type text into an input/textarea by its [ref] from the latest snapshot. Set "
            "submit=true to press Enter after (e.g. a search box).",
            {
                "type": obj,
                "properties": {
                    "ref": {"type": "integer"},
                    "text": {"type": "string"},
                    "submit": {"type": "boolean", "description": "Press Enter after typing."},
                    **context_param,
                },
                "required": ["ref", "text"],
            },
            _CAP, type_text, announce=True,
        ),
        Tool(
            "browser_read",
            "Read the current page's visible text (to extract an answer — opening hours, "
            "availability, a result). Use after navigating to the right page.",
            {"type": obj, "properties": {**context_param}},
            _CAP, read, announce=True,
        ),
    ]
