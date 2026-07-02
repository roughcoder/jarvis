"""The streaming think/speak core (latency work) — sentence-streamed answers.

Drives _run_tool_loop with a scripted STREAMING fake gateway: text deltas arrive
incrementally, tool calls appear mid-stream, and the loop must (a) start TTS per
sentence as it completes, (b) stop treating text as speakable the moment a tool
call starts, (c) carry a spoken preamble into result.raw, and (d) survive a
barge-in (generator close) with result.raw holding what was actually streamed.
Also pins the history compaction of big tool results and the sticky MCP offer.
"""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.config import load_config
from jarvis.tools.base import Tool, ToolRegistry


class _FakeTTS:
    """Records each synthesis text; yields one PCM chunk per call."""

    def __init__(self) -> None:
        self.texts: list[str] = []

    async def synthesize_stream(self, text: str, *, voice=None):  # noqa: ANN001
        self.texts.append(text)
        yield b"PCM:" + text.encode()


class _StreamGateway:
    """Scripted streaming rounds. Each round is a list of ("text", str) and
    ("call", dict) items, replayed in order — calls land in tool_calls_out
    mid-stream, exactly like live tool-call deltas."""

    def __init__(self, rounds) -> None:  # noqa: ANN001
        self._rounds = rounds
        self.round = 0
        self.models: list[str] = []

    async def stream_with_tools(
        self, messages, *, model=None, tools=None, usage_out=None, tool_calls_out=None
    ):  # noqa: ANN001
        items = self._rounds[self.round]
        self.round += 1
        self.models.append(model)
        if usage_out is not None:
            usage_out.update({"prompt_tokens": 100, "cached_tokens": 50})
        for kind, payload in items:
            if kind == "text":
                yield payload
            else:
                tool_calls_out.append(dict(payload))
        await asyncio.sleep(0)


def _ping_registry(record: list) -> ToolRegistry:
    async def ping(ctx, args):  # noqa: ANN001
        record.append(args)
        return "pong"

    reg = ToolRegistry()
    reg.register(
        Tool("ping", "ping", {"type": "object", "properties": {}}, "ping.use", ping)
    )
    return reg


def _session(gateway, tts=None, registry=None) -> BrainSession:  # noqa: ANN001
    cfg = load_config()
    ctx = RequestContext("dev", "neil", "personal", frozenset({"ping.use"}), channel="voice")
    return BrainSession(
        cfg, ctx, gateway=gateway, tts=tts, memory=None, tracer=None,
        registry=registry or ToolRegistry(),
    )


def _drive(session, tool_schemas, result, *, speak=True, stop_after=None):  # noqa: ANN001
    chunks: list[bytes] = []

    async def go() -> None:
        gen = session._run_tool_loop(
            [{"role": "user", "content": "hi"}], "fast", None, tool_schemas, result,
            speak=speak,
        )
        try:
            async for pcm in gen:
                chunks.append(pcm)
                if stop_after is not None and len(chunks) >= stop_after:
                    break  # barge-in: the caller closes the generator
        finally:
            await gen.aclose()

    asyncio.run(go())
    return chunks


def test_streamed_answer_is_spoken_sentence_by_sentence() -> None:
    tts = _FakeTTS()
    gw = _StreamGateway([
        [("text", "Hello there, my friend. "), ("text", "How are you today?")],
    ])
    result = TurnResult()
    chunks = _drive(_session(gw, tts), [], result)

    assert result.raw == "Hello there, my friend. How are you today?"
    assert tts.texts == ["Hello there, my friend.", "How are you today?"]
    assert chunks == [b"PCM:Hello there, my friend.", b"PCM:How are you today?"]


def test_streamed_tool_round_then_spoken_final_answer() -> None:
    tts = _FakeTTS()
    called: list = []
    gw = _StreamGateway([
        [("call", {"id": "c1", "name": "ping", "arguments": "{}"})],
        [("text", "Pong came back, all good.")],
    ])
    session = _session(gw, tts, _ping_registry(called))
    result = TurnResult()
    schemas = [t.openai_schema() for t in session._registry.available_for(session._ctx)]
    chunks = _drive(session, schemas, result)

    assert called == [{}]  # the tool really ran
    assert result.raw == "Pong came back, all good."
    assert chunks == [b"PCM:Pong came back, all good."]
    cfg = session._cfg
    assert gw.models == [cfg.gateway.fast_model, cfg.gateway.strong_model]  # escalated
    # the tool round is in the carried context for the next turn
    assert any(m.get("tool_calls") for m in result.tool_messages)


def test_preamble_before_tool_call_is_spoken_and_kept_in_raw() -> None:
    tts = _FakeTTS()
    called: list = []
    gw = _StreamGateway([
        [
            ("text", "Let me check that for you. "),
            ("call", {"id": "c1", "name": "ping", "arguments": "{}"}),
            ("text", "internal scratch text"),  # post-call text is NOT spoken
        ],
        [("text", "All done, it looks sunny.")],
    ])
    session = _session(gw, tts, _ping_registry(called))
    result = TurnResult()
    schemas = [t.openai_schema() for t in session._registry.available_for(session._ctx)]
    _drive(session, schemas, result)

    assert tts.texts == ["Let me check that for you.", "All done, it looks sunny."]
    assert result.raw == "Let me check that for you. All done, it looks sunny."
    # the full round content (incl. the unspoken tail) went into the tool context
    assistant = result.tool_messages[0]
    assert "internal scratch text" in assistant["content"]


def test_bargein_mid_stream_leaves_partial_raw() -> None:
    tts = _FakeTTS()
    gw = _StreamGateway([
        [("text", "First sentence spoken aloud. "), ("text", "Second one never plays.")],
    ])
    result = TurnResult()
    chunks = _drive(_session(gw, tts), [], result, stop_after=1)

    assert chunks == [b"PCM:First sentence spoken aloud."]
    assert result.raw.startswith("First sentence spoken aloud.")


def test_speak_false_streams_text_without_tts() -> None:
    gw = _StreamGateway([[("text", "Text console reply, no audio here.")]])
    result = TurnResult()
    chunks = _drive(_session(gw, tts=None), [], result, speak=False)

    assert chunks == []
    assert result.raw == "Text console reply, no audio here."


def test_big_tool_results_are_compacted_into_history() -> None:
    session = _session(_StreamGateway([]))
    session._cfg.persona.history_tool_result_chars = 50
    big = "x" * 500
    result = TurnResult(
        reply="done",
        tool_messages=[
            {"role": "assistant", "content": "", "tool_calls": [
                {"id": "c1", "type": "function",
                 "function": {"name": "browser_read", "arguments": "{}"}},
            ]},
            {"role": "tool", "tool_call_id": "c1", "content": big},
        ],
    )
    session._remember("read the page", result)

    tool_msg = next(m for m in session._history if m.get("role") == "tool")
    assert len(tool_msg["content"]) < 100
    assert "truncated 450 chars" in tool_msg["content"]
    # the live turn's message list is untouched — only the carried copy shrinks
    assert result.tool_messages[1]["content"] == big


def test_mcp_offer_is_sticky_within_a_conversation() -> None:
    async def noop(ctx, args):  # noqa: ANN001
        return "ok"

    reg = ToolRegistry()
    reg.register(Tool(
        "notion_search", "search notion", {"type": "object", "properties": {}},
        "mcp.notion", noop,
    ))
    cfg = load_config()
    cfg.tools.relevance_filter = True
    ctx = RequestContext("dev", "neil", "personal", frozenset({"mcp.notion"}), channel="voice")
    session = BrainSession(
        cfg, ctx, gateway=None, tts=None, memory=None, tracer=None, registry=reg,
    )

    async def offers() -> tuple[list[str], list[str], list[str]]:
        first = [s["function"]["name"] for s in await session._offer_tools("search my notion for cake")]
        second = [s["function"]["name"] for s in await session._offer_tools("what time is it")]
        session._sticky_servers.clear()  # what finalize() does when the conversation ends
        third = [s["function"]["name"] for s in await session._offer_tools("what time is it")]
        return first, second, third

    first, second, third = asyncio.run(offers())

    assert "notion_search" in first  # keyword match brought it in
    assert "notion_search" in second  # sticky kept it (stable cached prefix)
    assert "notion_search" not in third  # cleared at conversation end
