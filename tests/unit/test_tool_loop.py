"""BrainSession tool loop — the path that silently broke once (Phase 3 W3/W4).

Drives _run_tool_loop with a scripted fake gateway (no network): the model asks
for a tool, the (real, local) files tool runs, the model then answers. Guards:
- the final answer lands in result.raw,
- the tool actually executed,
- a tool-search earcon is emitted into the audio stream,
- the per-turn trace records a `tool` event (the exact call that raised
  TypeError: event() got multiple values for 'name' and ate the whole turn).
"""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.brain.tracing import TurnTrace
from jarvis.config import ToolsConfig, load_config
from jarvis.tools import build_registry


class _FakeFn:
    def __init__(self, name: str, arguments: str) -> None:
        self.name = name
        self.arguments = arguments


class _FakeToolCall:
    def __init__(self, id: str, name: str, arguments: str) -> None:
        self.id = id
        self.function = _FakeFn(name, arguments)


class _FakeMsg:
    def __init__(self, content=None, tool_calls=None) -> None:  # noqa: ANN001
        self.content = content
        self.tool_calls = tool_calls


class _FakeGateway:
    """Returns scripted assistant messages, one per complete_with_tools call."""

    def __init__(self, scripted: list[_FakeMsg]) -> None:
        self._scripted = scripted
        self.calls = 0

    async def complete_with_tools(self, messages, *, model=None, tools=None, usage_out=None):  # noqa: ANN001
        msg = self._scripted[self.calls]
        self.calls += 1
        if usage_out is not None:
            usage_out.update({"prompt_tokens": 100, "cached_tokens": 0})
        return msg


def _session(tmp_path, gateway) -> BrainSession:  # noqa: ANN001
    cfg = load_config()
    ctx = RequestContext(
        "dev", "house", "house", frozenset({"files.read", "files.write"})
    )
    registry = build_registry(ToolsConfig(_env_file=None, files_root=str(tmp_path)))
    return BrainSession(
        cfg, ctx, gateway=gateway, tts=None, memory=None, tracer=None, registry=registry
    )


def _run(session: BrainSession, trace: TurnTrace, result: TurnResult) -> list[bytes]:
    schemas = [t.openai_schema() for t in session._registry.available_for(session._ctx)]
    chunks: list[bytes] = []

    async def go() -> None:
        async for pcm in session._run_tool_loop(
            [{"role": "user", "content": "save a note"}], "fast", trace, schemas, result
        ):
            chunks.append(pcm)

    asyncio.run(go())
    return chunks


def test_tool_loop_executes_then_answers(tmp_path) -> None:
    gateway = _FakeGateway([
        _FakeMsg(tool_calls=[
            _FakeToolCall("c1", "write_file", '{"path": "note.md", "content": "buy milk"}')
        ]),
        _FakeMsg(content="Saved your note."),
    ])
    session = _session(tmp_path, gateway)
    trace = TurnTrace(room="x", speaker="house")
    result = TurnResult()

    chunks = _run(session, trace, result)

    assert result.raw == "Saved your note."
    assert (tmp_path / "note.md").read_text() == "buy milk"  # tool really ran
    assert chunks == []  # files are instant => no earcon beep
    # the regression: this trace event used to raise and kill the turn
    assert {"name": "tool", "tool": "write_file"} in trace.data["events"]
    assert "llm" in trace.data["stages"]


def _announced_registry(handler):  # noqa: ANN001
    from jarvis.tools.base import Tool, ToolRegistry

    reg = ToolRegistry()
    reg.register(
        Tool("lookup", "desc", {"type": "object", "properties": {}}, "web.search", handler, announce=True)
    )
    return reg


def test_announced_tool_emits_heartbeat(tmp_path) -> None:
    async def handler(ctx, args):  # noqa: ANN001
        return "looked up"

    gateway = _FakeGateway([
        _FakeMsg(tool_calls=[_FakeToolCall("c1", "lookup", "{}")]),
        _FakeMsg(content="Here you go."),
    ])
    ctx = RequestContext("dev", "house", "house", frozenset({"web.search"}))
    session = BrainSession(
        load_config(), ctx, gateway=gateway, tts=None, memory=None, tracer=None,
        registry=_announced_registry(handler),
    )
    result = TurnResult()
    chunks = _run(session, TurnTrace(room="x", speaker="house"), result)

    assert result.raw == "Here you go."
    assert chunks and chunks[0]  # announced (remote) tool => at least one pulse


def test_slow_tool_emits_repeating_heartbeats(tmp_path) -> None:
    async def slow(ctx, args):  # noqa: ANN001
        await asyncio.sleep(0.12)
        return "done searching"

    gateway = _FakeGateway([
        _FakeMsg(tool_calls=[_FakeToolCall("c1", "lookup", "{}")]),
        _FakeMsg(content="Found it."),
    ])
    cfg = load_config()
    cfg.tools.heartbeat_interval_s = 0.03  # fast cadence so the test stays quick
    ctx = RequestContext("dev", "house", "house", frozenset({"web.search"}))
    session = BrainSession(
        cfg, ctx, gateway=gateway, tts=None, memory=None, tracer=None,
        registry=_announced_registry(slow),
    )
    result = TurnResult()
    chunks = _run(session, TurnTrace(room="x", speaker="house"), result)

    assert result.raw == "Found it."
    assert len(chunks) >= 2  # pulses kept coming while the tool ran


def test_tool_calls_are_logged_to_console(tmp_path, capsys) -> None:  # noqa: ANN001
    gateway = _FakeGateway([
        _FakeMsg(tool_calls=[
            _FakeToolCall("c1", "write_file", '{"path": "n.md", "content": "hi"}')
        ]),
        _FakeMsg(content="Done."),
    ])
    session = _session(tmp_path, gateway)
    _run(session, TurnTrace(room="x", speaker="house"), TurnResult())

    out = capsys.readouterr().out
    assert "tool: write_file" in out
    assert "[files.write]" in out  # the gating capability is shown (= server for mcp.*)


def test_tool_logging_can_be_disabled(tmp_path, capsys) -> None:  # noqa: ANN001
    gateway = _FakeGateway([
        _FakeMsg(tool_calls=[
            _FakeToolCall("c1", "write_file", '{"path": "n.md", "content": "hi"}')
        ]),
        _FakeMsg(content="Done."),
    ])
    session = _session(tmp_path, gateway)
    session._cfg.tools.log_calls = False
    _run(session, TurnTrace(room="x", speaker="house"), TurnResult())

    assert "tool: write_file" not in capsys.readouterr().out


def test_produces_image_tool_injects_image_and_switches_to_vision(tmp_path) -> None:  # noqa: ANN001
    from jarvis.tools.base import Tool, ToolRegistry

    captured: list = []

    class _RecordingGateway:
        def __init__(self, scripted) -> None:  # noqa: ANN001
            self._s, self.calls = scripted, 0

        async def complete_with_tools(self, messages, *, model=None, tools=None, usage_out=None):  # noqa: ANN001
            captured.append({"model": model, "messages": list(messages)})
            m = self._s[self.calls]
            self.calls += 1
            return m

    async def look(ctx, args):  # noqa: ANN001 - returns base64 image, not text
        return "FAKEB64DATA"

    reg = ToolRegistry()
    reg.register(Tool(
        "look_at_screen", "see", {"type": "object", "properties": {}}, "worker.gui",
        look, announce=False, produces_image=True,
    ))
    gw = _RecordingGateway([
        _FakeMsg(tool_calls=[_FakeToolCall("c1", "look_at_screen", "{}")]),
        _FakeMsg(content="I can see a calculator showing 300."),
    ])
    cfg = load_config()
    ctx = RequestContext("dev", "neil", "personal", frozenset({"worker.gui"}))
    session = BrainSession(cfg, ctx, gateway=gw, tts=None, memory=None, tracer=None, registry=reg)
    result = TurnResult()
    schemas = [t.openai_schema() for t in reg.available_for(ctx)]

    async def go() -> None:
        async for _ in session._run_tool_loop(
            [{"role": "user", "content": "what's on screen"}],
            "fast", TurnTrace(room="x", speaker="neil"), schemas, result,
        ):
            pass

    asyncio.run(go())

    assert result.raw == "I can see a calculator showing 300."
    second = captured[1]["messages"]  # the call AFTER the image was injected
    imgs = [
        m for m in second
        if isinstance(m.get("content"), list)
        and any(p.get("type") == "image_url" for p in m["content"])
    ]
    assert imgs, "the captured screen image was injected as a user message"
    assert "data:image/jpeg;base64,FAKEB64DATA" in imgs[0]["content"][1]["image_url"]["url"]
    assert captured[1]["model"] == cfg.gateway.vision_model  # switched to the vision route
    # the big image is NOT carried into long-term history
    assert not any(isinstance(m.get("content"), list) for m in result.tool_messages)


def test_no_tool_call_sets_reply_without_earcon(tmp_path) -> None:
    gateway = _FakeGateway([_FakeMsg(content="Two plus two is four.")])
    session = _session(tmp_path, gateway)
    trace = TurnTrace(room="x", speaker="house")
    result = TurnResult()

    chunks = _run(session, trace, result)

    assert result.raw == "Two plus two is four."
    assert chunks == []  # no tool fired => no earcon
    assert trace.data["events"] == []
