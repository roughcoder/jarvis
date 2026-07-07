from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from jarvis.brain.contexts import ContextStore
from jarvis.brain.project_tools import PROJECT_SWITCH_CAPABILITY, make_project_tools
from jarvis.brain.registry import ProjectEntry, RegistryStore
from jarvis.config import MemoryConfig
from jarvis.runtime import RequestContext, ToolRegistry
from conftest import request_context


def _ctx(
    *caps: str,
    identity: str = "neil",
    device_id: str = "office-mac",
) -> RequestContext:
    return request_context(
        *caps,
        device_id=device_id,
        identity=identity,
        scope="personal",
        peer=identity,
    )


def _memory_cfg(tmp_path: Path) -> MemoryConfig:
    return MemoryConfig(
        _env_file=None,
        backend="v3",
        cache_path=str(tmp_path / "cache.json"),
        curation_outbox_path=str(tmp_path / "outbox.jsonl"),
        tool_timeout_s=0.05,
    )


def _registry(tmp_path: Path) -> RegistryStore:
    store = RegistryStore(tmp_path / "registry.json")
    store.save_project(
        ProjectEntry(
            id="jarvis",
            name="Jarvis",
            aliases=("the jarvis project",),
            owner="neil",
            members=("neil",),
            visibility="shared",
        )
    )
    store.save_project(
        ProjectEntry(
            id="harvest",
            name="Harvest",
            aliases=("the harvest project",),
            owner="neil",
            members=("neil",),
            visibility="shared",
        )
    )
    store.save_project(
        ProjectEntry(
            id="alice-private",
            name="Alice Private",
            aliases=("alice private project",),
            owner="alice",
            members=("alice",),
            visibility="private",
        )
    )
    return store


class FakeMemory:
    def __init__(self, *, fail_refresh: bool = False) -> None:
        self.fail_refresh = fail_refresh
        self.refreshes: list[tuple[str | None, float]] = []
        self.cached: dict[str | None, str] = {}

    def read_cached_representation(self, user: str | None = None) -> str:
        return self.cached.get(user, "")

    async def refresh_cache(self, min_interval_s: float = 0.0, *, user: str | None = None) -> bool:
        self.refreshes.append((user, min_interval_s))
        if self.fail_refresh:
            raise TimeoutError("memory unavailable")
        self.cached[user] = f"fresh {user}"
        return True


def _tools(
    tmp_path: Path,
    memory: FakeMemory | None = None,
) -> tuple[dict[str, Any], ContextStore, FakeMemory]:
    memory = memory or FakeMemory()
    contexts = ContextStore(lambda ctx: object())
    tools = {
        tool.name: tool
        for tool in make_project_tools(
            _memory_cfg(tmp_path),
            memory=memory,
            registry=_registry(tmp_path),
            contexts=contexts,
        )
    }
    return tools, contexts, memory


def test_project_switch_tool_is_capability_gated(tmp_path) -> None:
    tools, _contexts, _memory = _tools(tmp_path)
    registry = ToolRegistry()
    for tool in tools.values():
        registry.register(tool)

    assert registry.available_for(_ctx()) == []
    assert {tool.name for tool in registry.available_for(_ctx(PROJECT_SWITCH_CAPABILITY))} == {
        "close_project",
        "current_project",
        "switch_project",
    }


def test_switch_project_fuzzy_alias_sets_active_project_and_refreshes_live(tmp_path) -> None:
    tools, contexts, memory = _tools(tmp_path)
    ctx = _ctx(PROJECT_SWITCH_CAPABILITY)

    result = asyncio.run(tools["switch_project"].handler(ctx, {"project": "javis"}))

    active = contexts.active_project(ctx)
    assert result == "Opening Jarvis project."
    assert active is not None
    assert active.id == "jarvis"
    assert active.peer_id == "project:jarvis"
    assert memory.refreshes == [("project:jarvis", 0.0)]


def test_switch_project_ambiguous_and_missing_ask_for_clarification(tmp_path) -> None:
    tools, contexts, memory = _tools(tmp_path)
    ctx = _ctx(PROJECT_SWITCH_CAPABILITY)

    ambiguous = asyncio.run(tools["switch_project"].handler(ctx, {"project": "project"}))
    missing = asyncio.run(tools["switch_project"].handler(ctx, {"project": "not real"}))

    assert ambiguous.startswith("confirmation required: which project?")
    assert "Jarvis" in ambiguous
    assert "Harvest" in ambiguous
    assert missing.startswith("confirmation required:")
    assert contexts.active_project(ctx) is None
    assert memory.refreshes == []


def test_switch_project_denies_non_visible_private_project(tmp_path) -> None:
    tools, contexts, memory = _tools(tmp_path)
    ctx = _ctx(PROJECT_SWITCH_CAPABILITY, identity="neil")

    result = asyncio.run(
        tools["switch_project"].handler(ctx, {"project": "alice private project"})
    )

    assert result.startswith("confirmation required:")
    assert contexts.active_project(ctx) is None
    assert memory.refreshes == []


def test_switch_project_tolerates_empty_cache_when_live_refresh_fails(tmp_path) -> None:
    tools, contexts, memory = _tools(tmp_path, FakeMemory(fail_refresh=True))
    ctx = _ctx(PROJECT_SWITCH_CAPABILITY)

    result = asyncio.run(tools["switch_project"].handler(ctx, {"project": "jarvis"}))

    assert result.startswith("Opening Jarvis project.")
    assert "no cached project memory yet" in result
    assert contexts.active_project(ctx).peer_id == "project:jarvis"  # type: ignore[union-attr]
    assert memory.refreshes == [("project:jarvis", 0.0)]


def test_close_project_clears_only_that_device_user_session(tmp_path) -> None:
    tools, contexts, _memory = _tools(tmp_path)
    office = _ctx(PROJECT_SWITCH_CAPABILITY, device_id="office-mac")
    kitchen = _ctx(PROJECT_SWITCH_CAPABILITY, device_id="kitchen-pi")
    asyncio.run(tools["switch_project"].handler(office, {"project": "jarvis"}))
    asyncio.run(tools["switch_project"].handler(kitchen, {"project": "harvest"}))

    closed = tools["close_project"].handler(office, {})
    current = tools["current_project"].handler(kitchen, {})

    assert closed == "Closed Jarvis project."
    assert contexts.active_project(office) is None
    assert contexts.active_project(kitchen).id == "harvest"  # type: ignore[union-attr]
    assert current == "The active project is Harvest."
