"""Text console harness — protocol routing + the brain's text-only turn.

The text console is how the whole brain is driven headlessly (no mic/STT/TTS):
- text_turn sends TextIn(text_only=True) and collects ReplyText up to ReplyEnd,
  surfacing any Proactive push that lands first;
- BrainSession.respond_text runs the same think core as respond() but returns text
  and plays no audio, reusing the tool loop so tools work in text mode.
"""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.config import ToolsConfig, load_config
from jarvis.connectors.text import text_turn
from jarvis.protocol.messages import (
    Proactive,
    ReplyEnd,
    ReplyText,
    TextIn,
    decode,
    encode,
)
from jarvis.tools import build_registry


class _ScriptedWS:
    """A fake brain socket: when a TextIn is sent it queues an optional Proactive,
    then the matching ReplyText + ReplyEnd (turn_id echoed from the TextIn)."""

    def __init__(self, reply: str = "ok", ended: bool = False, proactive: str | None = None) -> None:
        self.sent: list = []
        self._out: list = []
        self._reply, self._ended, self._proactive = reply, ended, proactive

    async def send(self, data) -> None:  # noqa: ANN001
        self.sent.append(data)
        m = decode(data)
        if isinstance(m, TextIn):
            if self._proactive is not None:
                self._out.append(encode(Proactive(text=self._proactive)))
            self._out.append(encode(ReplyText(turn_id=m.turn_id, text=self._reply)))
            self._out.append(encode(ReplyEnd(turn_id=m.turn_id, ended=self._ended)))

    def __aiter__(self):  # noqa: ANN204
        return self

    async def __anext__(self):  # noqa: ANN204
        if self._out:
            return self._out.pop(0)
        raise StopAsyncIteration


def test_text_turn_sends_text_only_and_collects_reply() -> None:
    ws = _ScriptedWS(reply="Booked it.", ended=True)
    reply, ended = asyncio.run(text_turn(ws, "book the pub"))
    assert reply == "Booked it."
    assert ended is True
    sent = decode(ws.sent[0])
    assert isinstance(sent, TextIn)
    assert sent.text == "book the pub"
    assert sent.text_only is True  # the brain skips TTS for this turn


def test_text_turn_surfaces_proactive_push(capsys) -> None:  # noqa: ANN001
    ws = _ScriptedWS(reply="done", proactive="Your table is booked for eight.")
    reply, _ended = asyncio.run(text_turn(ws, "anything?"))
    assert reply == "done"
    assert "Your table is booked for eight." in capsys.readouterr().out


# --- BrainSession.respond_text ---------------------------------------------

class _Fn:
    def __init__(self, name: str, arguments: str) -> None:
        self.name, self.arguments = name, arguments


class _Call:
    def __init__(self, id: str, name: str, arguments: str) -> None:
        self.id, self.function = id, _Fn(name, arguments)


class _Msg:
    def __init__(self, content=None, tool_calls=None) -> None:  # noqa: ANN001
        self.content, self.tool_calls = content, tool_calls


class _Gateway:
    def __init__(self, scripted: list) -> None:
        self._s, self.calls = scripted, 0

    async def complete(self, messages, *, model=None) -> str:  # noqa: ANN001
        m = self._s[self.calls]
        self.calls += 1
        return m.content or ""

    async def complete_with_tools(self, messages, *, model=None, tools=None, usage_out=None):  # noqa: ANN001
        m = self._s[self.calls]
        self.calls += 1
        return m


def _session(tmp_path, gateway) -> BrainSession:  # noqa: ANN001
    ctx = RequestContext("dev", "house", "house", frozenset({"files.read", "files.write"}))
    registry = build_registry(ToolsConfig(_env_file=None, files_root=str(tmp_path)))
    return BrainSession(
        load_config(), ctx, gateway=gateway, tts=None, memory=None, tracer=None, registry=registry
    )


def test_respond_text_no_tools_returns_text(tmp_path) -> None:
    session = _session(tmp_path, _Gateway([_Msg(content="Two and two is four.")]))
    result = TurnResult()
    out = asyncio.run(session.respond_text("what's two plus two", None, result))
    assert out == "Two and two is four."
    assert result.raw == "Two and two is four."


def test_respond_text_runs_tools(tmp_path) -> None:
    gateway = _Gateway([
        _Msg(tool_calls=[_Call("c1", "write_file", '{"path": "n.md", "content": "hi"}')]),
        _Msg(content="Saved it."),
    ])
    session = _session(tmp_path, gateway)
    result = TurnResult()
    out = asyncio.run(session.respond_text("save a note", None, result))
    assert out == "Saved it."
    assert (tmp_path / "n.md").read_text() == "hi"  # the tool ran in text mode
