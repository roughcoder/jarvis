"""Skills (Phase 3 §7) — parsing, the capability-subset safety invariant, the
bounded runner, and self-authoring round-trip. No network (fake gateway)."""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.brain.skills import (
    Skill,
    load_skills,
    make_skill_tools,
    make_save_skill_tool,
    parse_skill,
    write_skill,
)
from jarvis.config import load_config
from jarvis.tools.base import Tool, ToolRegistry

_SKILL = """---
name: news_briefing
when_to_use: When the user asks for news.
allowed_tools: [web_search]
---

Search the web and summarise in three sentences.
"""


def _ctx(*caps: str) -> RequestContext:
    return RequestContext("mac", "neil", "personal", frozenset(caps))


def _registry() -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(Tool("web_search", "search", {"type": "object", "properties": {}}, "web.search", lambda c, a: "ok"))
    return reg


class _FakeFn:
    def __init__(self, name, arguments):  # noqa: ANN001
        self.name, self.arguments = name, arguments


class _FakeTC:
    def __init__(self, id, name, arguments):  # noqa: ANN001
        self.id, self.function = id, _FakeFn(name, arguments)


class _FakeMsg:
    def __init__(self, content=None, tool_calls=None):  # noqa: ANN001
        self.content, self.tool_calls = content, tool_calls


class _FakeGateway:
    def __init__(self, scripted):  # noqa: ANN001
        self._s, self.calls = scripted, 0

    async def complete_with_tools(self, messages, *, model=None, tools=None):  # noqa: ANN001
        m = self._s[self.calls]
        self.calls += 1
        return m


def test_parse_skill() -> None:
    s = parse_skill("news_briefing", _SKILL)
    assert s.name == "news_briefing"
    assert s.allowed_tools == ("web_search",)
    assert "summarise" in s.recipe
    assert s.description.startswith("When the user asks")


def test_skill_offered_only_when_all_tool_caps_granted() -> None:
    cfg = load_config()
    reg = _registry()
    (tool,) = make_skill_tools({"news": parse_skill("news", _SKILL)}, gateway=None, registry=reg, cfg=cfg)
    reg.register(tool)
    # needs skills.run AND web.search (the composed tool's cap) — the invariant
    assert tool.required_capability == "skills.run"
    assert tool.extra_capabilities == frozenset({"web.search"})
    assert tool.name == "news_briefing"  # name comes from the recipe's front-matter
    assert "news_briefing" not in {t.name for t in reg.available_for(_ctx("skills.run"))}  # no web.search
    assert "news_briefing" in {t.name for t in reg.available_for(_ctx("skills.run", "web.search"))}


def test_skill_runner_loops_tools_then_answers() -> None:
    cfg = load_config()
    reg = _registry()
    gw = _FakeGateway([
        _FakeMsg(tool_calls=[_FakeTC("c1", "web_search", "{}")]),
        _FakeMsg(content="Here is your briefing."),
    ])
    (tool,) = make_skill_tools({"news": parse_skill("news", _SKILL)}, gateway=gw, registry=reg, cfg=cfg)
    out = asyncio.run(tool.handler(_ctx("skills.run", "web.search"), {"request": "AI news"}))
    assert out == "Here is your briefing."
    assert gw.calls == 2  # one tool round, then the final answer


def test_save_skill_writes_and_registers_live(tmp_path) -> None:  # noqa: ANN001
    registered = []
    save = make_save_skill_tool(str(tmp_path), on_saved=lambda s: registered.append(s.name))
    out = asyncio.run(
        save.handler(
            _ctx("skills.author"),
            {"name": "Greet Boss", "when_to_use": "say hi", "recipe": "Say hello.", "allowed_tools": []},
        )
    )
    assert "greet_boss" in out
    assert registered == ["greet_boss"]
    # persisted + loadable + indexed
    assert (tmp_path / "greet_boss.md").exists()
    assert (tmp_path / "SKILLS.md").exists()
    assert "greet_boss" in load_skills(str(tmp_path))


def test_write_skill_roundtrip(tmp_path) -> None:  # noqa: ANN001
    write_skill(str(tmp_path), Skill("brief", "for briefings", "Summarise.", ("web_search",)))
    loaded = load_skills(str(tmp_path))["brief"]
    assert loaded.allowed_tools == ("web_search",)
    assert loaded.description == "for briefings"
