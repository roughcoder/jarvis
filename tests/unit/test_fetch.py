"""fetch_page — the generic 'read a URL as text' primitive (html_to_text + handler)."""

from __future__ import annotations

import asyncio
import time

from jarvis.brain.context import RequestContext
from jarvis.config import ToolsConfig
from jarvis.tools.fetch import (
    _PublicOnlyNetworkBackend,
    _resolve_public_addresses,
    _safe_fetch_url,
    html_to_text,
    make_fetch_tools,
)


def test_html_to_text_strips_and_breaks() -> None:
    html = (
        "<html><head><title>x</title><style>.a{}</style></head>"
        "<body><script>var x=1</script>"
        "<h1>Departures</h1><div>0750 &amp; on&nbsp;time</div>"
        "<p>London <b>Waterloo</b></p><!-- note --></body></html>"
    )
    out = html_to_text(html)
    assert "var x=1" not in out and ".a{}" not in out  # script/style gone
    assert "<" not in out and ">" not in out  # tags gone
    assert "0750 & on time" in out  # entities decoded, &nbsp -> space
    assert "Departures" in out and "London Waterloo" in out
    # block tags became line breaks
    assert "Departures\n" in out


def test_html_to_text_truncates() -> None:
    out = html_to_text("<p>" + "x" * 100 + "</p>", max_chars=20)
    assert out.endswith("…(truncated)") and out.count("x") == 20


def test_fetch_handler_rejects_non_url() -> None:
    tool = make_fetch_tools(ToolsConfig())[0]
    ctx = RequestContext("dev", "house", "house", frozenset({"web.search"}))
    out = asyncio.run(tool.handler(ctx, {"url": "not a url"}))
    assert out.startswith("error:") and "http" in out


def test_fetch_rejects_private_local_and_link_local_hosts() -> None:
    blocked = [
        "http://127.0.0.1:4000",
        "http://localhost:4000",
        "http://192.168.1.20",
        "http://10.0.0.5",
        "http://100.64.0.1",
        "http://169.254.169.254/latest/meta-data",
        "http://224.0.0.1",
        "http://[ff02::1]",
    ]
    for url in blocked:
        parsed = _safe_fetch_url(url)
        port = parsed.port or 80
        try:
            asyncio.run(_resolve_public_addresses(parsed.host, port))
        except ValueError as exc:
            assert "blocked" in str(exc) or "non-public" in str(exc)
        else:  # pragma: no cover - explicit failure path
            raise AssertionError(f"{url} was not blocked")


def test_fetch_backend_connects_to_vetted_ip(monkeypatch) -> None:  # noqa: ANN001
    calls: list[str] = []

    def fake_getaddrinfo(host, port, **_kwargs):  # noqa: ANN001
        assert host == "example.com"
        assert port == 443
        return [(None, None, None, None, ("93.184.216.34", 443))]

    class FakeBackend:
        async def connect_tcp(self, host, port, **_kwargs):  # noqa: ANN001
            calls.append(host)
            return object()

    monkeypatch.setattr("jarvis.tools.fetch.socket.getaddrinfo", fake_getaddrinfo)
    backend = _PublicOnlyNetworkBackend()
    backend._backend = FakeBackend()

    asyncio.run(backend.connect_tcp("example.com", 443))

    assert calls == ["93.184.216.34"]


def test_fetch_backend_tries_remaining_vetted_addresses(monkeypatch) -> None:  # noqa: ANN001
    calls: list[str] = []

    def fake_getaddrinfo(host, port, **_kwargs):  # noqa: ANN001
        return [
            (None, None, None, None, ("93.184.216.34", port)),
            (None, None, None, None, ("93.184.216.35", port)),
        ]

    class FakeBackend:
        async def connect_tcp(self, host, port, **_kwargs):  # noqa: ANN001
            calls.append(host)
            if len(calls) == 1:
                raise OSError("first address unreachable")
            return object()

    monkeypatch.setattr("jarvis.tools.fetch.socket.getaddrinfo", fake_getaddrinfo)
    backend = _PublicOnlyNetworkBackend()
    backend._backend = FakeBackend()

    asyncio.run(backend.connect_tcp("example.com", 443))

    assert calls == ["93.184.216.34", "93.184.216.35"]


def test_fetch_dns_resolution_has_own_timeout(monkeypatch) -> None:  # noqa: ANN001
    def slow_getaddrinfo(host, port, **_kwargs):  # noqa: ANN001
        time.sleep(0.2)
        return [(None, None, None, None, ("93.184.216.34", port))]

    monkeypatch.setattr("jarvis.tools.fetch.socket.getaddrinfo", slow_getaddrinfo)

    try:
        asyncio.run(_resolve_public_addresses("example.com", 443, timeout_s=0.01))
    except TimeoutError:
        pass
    else:  # pragma: no cover - explicit failure path
        raise AssertionError("DNS resolution did not time out")


def test_fetch_tool_shape() -> None:
    tool = make_fetch_tools(ToolsConfig())[0]
    assert tool.name == "fetch_page"
    assert tool.required_capability == "web.search"
    assert "url" in tool.parameters["properties"]
