"""Tool layer — registry gating, files sandboxing, web-search parsing (Phase 3).

The gate is enforced twice: the registry only *offers* granted tools, and
`execute()` re-checks before running (defense in depth). Files are root-bounded.
"""

from __future__ import annotations

import asyncio

import pytest

from jarvis.brain.capabilities import CapabilityError
from jarvis.brain.context import RequestContext
from jarvis.config import ToolsConfig
from jarvis.tools import build_registry
from jarvis.tools.base import ToolError
from jarvis.tools.files import _resolve_within, make_files_tools
from jarvis.tools.web_search import _format_tavily


def _ctx(*caps: str) -> RequestContext:
    return RequestContext("dev", "house", "house", frozenset(caps))


def _cfg(tmp_path, **over) -> ToolsConfig:
    return ToolsConfig(_env_file=None, files_root=str(tmp_path), **over)


# --- registry gating -------------------------------------------------------


def test_available_for_filters_by_capability(tmp_path) -> None:
    reg = build_registry(_cfg(tmp_path))
    assert reg.available_for(_ctx()) == []  # deny-by-default
    read_only = {t.name for t in reg.available_for(_ctx("files.read"))}
    assert read_only == {"read_file", "list_files"}
    full = {t.name for t in reg.available_for(_ctx("files.read", "files.write", "web.search"))}
    assert full == {"read_file", "list_files", "write_file", "web_search"}


def test_execute_denies_ungranted_capability(tmp_path) -> None:
    reg = build_registry(_cfg(tmp_path))
    with pytest.raises(CapabilityError):
        asyncio.run(
            reg.execute(
                _ctx("files.read"),
                "write_file",
                {"path": "a.txt", "content": "x"},
                timeout_s=2,
            )
        )


def test_execute_unknown_tool_raises(tmp_path) -> None:
    reg = build_registry(_cfg(tmp_path))
    with pytest.raises(ToolError):
        asyncio.run(reg.execute(_ctx("files.read"), "nope", {}, timeout_s=2))


# --- files sandbox ---------------------------------------------------------


def test_files_write_read_list_roundtrip(tmp_path) -> None:
    tools = {t.name: t for t in make_files_tools(_cfg(tmp_path))}
    ctx = _ctx("files.read", "files.write")
    assert "wrote" in tools["write_file"].handler(ctx, {"path": "notes/x.md", "content": "hello"})
    assert tools["read_file"].handler(ctx, {"path": "notes/x.md"}) == "hello"
    assert "x.md" in tools["list_files"].handler(ctx, {"path": "notes"})


@pytest.mark.parametrize("bad", ["../escape.txt", "/etc/passwd", "a/../../b"])
def test_files_path_escape_rejected(tmp_path, bad) -> None:
    with pytest.raises(ToolError):
        _resolve_within(str(tmp_path), bad)


def test_files_within_root_ok(tmp_path) -> None:
    p = _resolve_within(str(tmp_path), "sub/dir/file.txt")
    assert str(p).startswith(str(tmp_path.resolve()))


# --- web-search parsing (pure) ---------------------------------------------


def test_format_tavily_with_answer_and_results() -> None:
    data = {
        "answer": "It is sunny.",
        "results": [{"title": "Weather", "url": "http://x", "content": "Sunny, 20C."}],
    }
    out = _format_tavily(data)
    assert "Answer: It is sunny." in out
    assert "Weather" in out and "http://x" in out


def test_format_tavily_empty_is_no_results() -> None:
    assert _format_tavily({}) == "No results."
