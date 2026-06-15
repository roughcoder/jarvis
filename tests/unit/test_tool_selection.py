"""Per-turn relevance prefilter (Phase 3 §9) — narrows MCP tools by utterance.

Pure logic, no SDK: builds Tool objects directly and checks which survive for a
given utterance.
"""

from __future__ import annotations

from jarvis.tools.base import Tool
from jarvis.tools.selection import offered_servers, select_tools


def _tool(name: str, cap: str) -> Tool:
    return Tool(name, "desc", {"type": "object", "properties": {}}, cap, lambda c, a: "", False)


def _fixture() -> list[Tool]:
    return [
        _tool("web_search", "web.search"),  # built-in — always offered
        _tool("write_file", "files.write"),  # built-in
        _tool("linear_list_issues", "mcp.linear"),
        _tool("linear_get_issue", "mcp.linear"),
        _tool("obsidian_daily_note", "mcp.obsidian"),
        _tool("obsidian_search_vault", "mcp.obsidian"),
        _tool("granola_get_meeting_transcript", "mcp.granola"),
    ]


def _names(tools: list[Tool]) -> set[str]:
    return {t.name for t in tools}


def test_builtins_always_offered_mcp_gated_by_relevance() -> None:
    out = _names(select_tools(_fixture(), "show me my open issues"))
    assert {"web_search", "write_file"} <= out  # built-ins always
    assert {"linear_list_issues", "linear_get_issue"} <= out  # "issues" -> linear
    assert "obsidian_daily_note" not in out  # unrelated server dropped
    assert "granola_get_meeting_transcript" not in out


def test_server_name_triggers_inclusion() -> None:
    out = _names(select_tools(_fixture(), "open my obsidian vault"))
    assert "obsidian_daily_note" in out
    assert "linear_list_issues" not in out


def test_keyword_from_tool_name_matches() -> None:
    out = _names(select_tools(_fixture(), "what was said in the meeting"))
    assert "granola_get_meeting_transcript" in out  # "meeting" derived from tool name


def test_extra_keywords_override() -> None:
    # "ticket" isn't in any tool name; supply it as a linear synonym
    out = _names(
        select_tools(_fixture(), "file a ticket about the bug", extra_keywords={"linear": {"ticket"}})
    )
    assert "linear_list_issues" in out


def test_disabled_or_no_utterance_offers_everything() -> None:
    everything = _names(_fixture())
    assert _names(select_tools(_fixture(), "anything", enabled=False)) == everything
    assert _names(select_tools(_fixture(), "")) == everything


def test_no_mcp_tools_passthrough() -> None:
    builtins = [_tool("web_search", "web.search"), _tool("write_file", "files.write")]
    assert select_tools(builtins, "irrelevant text") == builtins


def test_offered_servers_lists_distinct_mcp() -> None:
    out = select_tools(_fixture(), "issues and meeting notes", extra_keywords={"granola": {"notes"}})
    assert set(offered_servers(out)) == {"linear", "granola"}
