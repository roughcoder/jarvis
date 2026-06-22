"""fetch_page — the generic 'read a URL as text' primitive (html_to_text + handler)."""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.config import ToolsConfig
from jarvis.tools.fetch import html_to_text, make_fetch_tools


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


def test_fetch_tool_shape() -> None:
    tool = make_fetch_tools(ToolsConfig())[0]
    assert tool.name == "fetch_page"
    assert tool.required_capability == "web.search"
    assert "url" in tool.parameters["properties"]
