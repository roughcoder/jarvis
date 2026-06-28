"""fetch_page — a general primitive: fetch a URL and return its readable text.

A reusable building block for skills (train times, directions, opening hours, …) so
those stay self-authored recipes instead of bespoke core tools. It strips a page to
plain text the model can read reliably — much cleaner than a browser DOM snapshot, and
no JavaScript/clicking. For interactive pages (forms, logins) the browser lane is still
the tool; this is for reading server-rendered content. Gated `web.search`.
"""

from __future__ import annotations

import html as _html
import re

import httpx

from jarvis.runtime import RequestContext
from jarvis.config import ToolsConfig
from jarvis.tools.base import Tool

_CAP = "web.search"
_MAX_CHARS = 6000

_DROP = re.compile(r"<(script|style|head|noscript|svg)\b.*?</\1>", re.DOTALL | re.IGNORECASE)
_COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
# Tags that should become a line break so text doesn't run together.
_BLOCK = re.compile(
    r"</?(div|p|br|tr|li|ul|ol|h[1-6]|section|article|header|footer|table|thead|tbody|nav)\b[^>]*>",
    re.IGNORECASE,
)
_TAG = re.compile(r"<[^>]+>")


def html_to_text(html: str, *, max_chars: int = _MAX_CHARS) -> str:
    """Strip HTML to readable, line-broken plain text (entities decoded, runs collapsed)."""
    s = _DROP.sub(" ", html or "")
    s = _COMMENT.sub(" ", s)
    s = _BLOCK.sub("\n", s)
    s = _TAG.sub("", s)
    s = _html.unescape(s).replace("\xa0", " ")  # nbsp -> normal space
    s = re.sub(r"[ \t\f\v]+", " ", s)
    s = re.sub(r" *\n[ \n]*", "\n", s)  # collapse blank lines + trim around breaks
    s = s.strip()
    return s[:max_chars] + ("\n…(truncated)" if len(s) > max_chars else "")


def make_fetch_tools(cfg: ToolsConfig) -> list[Tool]:
    async def fetch_page(ctx: RequestContext, args: dict) -> str:
        url = (args.get("url") or "").strip()
        if not re.match(r"https?://", url):
            return "error: give a full http(s) URL to fetch."
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                r = await client.get(url, headers={"User-Agent": "Mozilla/5.0 (Jarvis)"})
                r.raise_for_status()
                text = html_to_text(r.text)
        except Exception as exc:  # noqa: BLE001 - network/HTTP — never break the turn
            return f"error: couldn't fetch that page ({type(exc).__name__})."
        return f"[{url}]\n{text}" if text else f"[{url}] (no readable text)"

    return [
        Tool(
            name="fetch_page",
            description=(
                "Fetch a web page and return its readable text. Use to READ server-rendered "
                "content from a known URL — live data (timetables, opening hours, prices), "
                "articles, references — more reliably than the browser. Give the full URL "
                "(web_search first if you don't know it). For pages needing clicks, forms, or "
                "a login, use the browser instead."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "Full http(s) URL to fetch."},
                },
                "required": ["url"],
            },
            required_capability=_CAP,
            handler=fetch_page,
            announce=True,  # network round-trip — earn the "looking that up" pulse
        ),
    ]
