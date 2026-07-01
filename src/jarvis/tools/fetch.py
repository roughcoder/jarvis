"""fetch_page — a general primitive: fetch a URL and return its readable text.

A reusable building block for skills (train times, directions, opening hours, …) so
those stay self-authored recipes instead of bespoke core tools. It strips a page to
plain text the model can read reliably — much cleaner than a browser DOM snapshot, and
no JavaScript/clicking. For interactive pages (forms, logins) the browser lane is still
the tool; this is for reading server-rendered content. Gated `web.search`.
"""

from __future__ import annotations

import asyncio
import html as _html
import ipaddress
import re
import socket

import httpx
import httpcore

from jarvis.runtime import RequestContext
from jarvis.config import ToolsConfig
from jarvis.tools.base import Tool

_CAP = "web.search"
_MAX_CHARS = 6000
_MAX_REDIRECTS = 5

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


def _public_ip(address: str) -> bool:
    return ipaddress.ip_address(address).is_global


async def _resolve_public_address(host: str, port: int) -> str:
    def resolve() -> list[str]:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        return [info[4][0] for info in infos]

    addresses = await asyncio.to_thread(resolve)
    if not addresses or any(not _public_ip(addr) for addr in addresses):
        raise ValueError("blocked non-public network address")
    return addresses[0]


def _safe_fetch_url(url: str) -> httpx.URL:
    parsed = httpx.URL(url)
    if parsed.scheme not in {"http", "https"} or not parsed.host:
        raise ValueError("give a full http(s) URL to fetch")
    return parsed


class _PublicOnlyNetworkBackend(httpcore.AsyncNetworkBackend):
    def __init__(self) -> None:
        self._backend = httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options=None,  # noqa: ANN001 - httpcore socket option tuple shape
    ) -> httpcore.AsyncNetworkStream:
        address = await _resolve_public_address(host, port)
        return await self._backend.connect_tcp(
            address,
            port,
            timeout=timeout,
            local_address=local_address,
            socket_options=socket_options,
        )


class _PublicOnlyAsyncHTTPTransport(httpx.AsyncHTTPTransport):
    def __init__(self) -> None:
        super().__init__(trust_env=False)
        self._pool = httpcore.AsyncConnectionPool(network_backend=_PublicOnlyNetworkBackend())


def make_fetch_tools(cfg: ToolsConfig) -> list[Tool]:
    async def fetch_page(ctx: RequestContext, args: dict) -> str:
        url = (args.get("url") or "").strip()
        if not re.match(r"https?://", url):
            return "error: give a full http(s) URL to fetch."
        try:
            transport = _PublicOnlyAsyncHTTPTransport()
            async with httpx.AsyncClient(
                timeout=15.0,
                follow_redirects=False,
                transport=transport,
                trust_env=False,
            ) as client:
                current = url
                for _ in range(_MAX_REDIRECTS + 1):
                    safe_url = _safe_fetch_url(current)
                    r = await client.get(
                        str(safe_url), headers={"User-Agent": "Mozilla/5.0 (Jarvis)"}
                    )
                    if not r.is_redirect:
                        break
                    location = r.headers.get("location")
                    if not location:
                        break
                    current = str(r.url.join(location))
                else:
                    return "error: too many redirects."
                r.raise_for_status()
                text = html_to_text(r.text)
                final_url = str(r.url)
        except ValueError as exc:
            return f"error: {exc}."
        except Exception as exc:  # noqa: BLE001 - network/HTTP — never break the turn
            return f"error: couldn't fetch that page ({type(exc).__name__})."
        return f"[{final_url}]\n{text}" if text else f"[{final_url}] (no readable text)"

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
