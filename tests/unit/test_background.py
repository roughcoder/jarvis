"""Background-task lane (fire-and-forget) — runner, gating, and headless run_task.

Covers the contract that makes it safe + useful:
- the `run_in_background` tool is gated by `background.run` (deny-by-default),
- start() returns instantly, runs detached, and delivers the outcome via notify,
- the inner session is built WITHOUT `background.run` (no recursion),
- a failing/timing-out job never crashes — it notifies a friendly message,
- the concurrency cap rejects new jobs,
- BrainSession.run_task drives the tool loop to a final summary without touching
  the live conversation history.
"""

from __future__ import annotations

import asyncio

from jarvis.brain.background import BackgroundRunner
from jarvis.brain.context import RequestContext
from jarvis.brain.session import BrainSession
from jarvis.config import BackgroundConfig, ToolsConfig, load_config
from jarvis.tools import build_registry
from jarvis.tools.background import make_background_tool


def _ctx(*caps: str) -> RequestContext:
    return RequestContext("dev", "neil", "personal", frozenset(caps))


class _FakeSession:
    def __init__(self, *, result: str = "done", raises: Exception | None = None, delay: float = 0.0) -> None:
        self._result, self._raises, self._delay = result, raises, delay
        self.ran_with: str | None = None

    async def run_task(self, task: str, *, max_rounds: int) -> str:
        self.ran_with = task
        if self._delay:
            await asyncio.sleep(self._delay)
        if self._raises is not None:
            raise self._raises
        return self._result


def _runner(cfg=None, *, session: _FakeSession | None = None):  # noqa: ANN001
    notes: list[str] = []
    seen_ctx: list[RequestContext] = []
    sess = session or _FakeSession()

    def factory(ctx: RequestContext):  # noqa: ANN202
        seen_ctx.append(ctx)
        return sess

    async def notify(text: str, identity: str = "", device_id: str = "") -> None:
        notes.append(text)

    runner = BackgroundRunner(
        cfg or BackgroundConfig(_env_file=None),
        session_factory=factory,
        notify=notify,
    )
    return runner, notes, seen_ctx, sess


async def _drain(runner: BackgroundRunner) -> None:
    """Await whatever jobs start() spawned (snapshot before the done-callback prunes)."""
    for _ in range(50):
        tasks = list(runner._tasks)
        if not tasks:
            return
        await asyncio.gather(*tasks)


# --- the tool: gating ------------------------------------------------------

def test_background_tool_gated_by_capability() -> None:
    runner, *_ = _runner()
    tool = make_background_tool(runner)
    assert tool.required_capability == "background.run"
    assert tool.name == "run_in_background"
    assert _ctx().can("background.run") is False  # deny-by-default
    assert _ctx("background.run").can("background.run") is True


# --- the runner ------------------------------------------------------------

def test_start_runs_detached_and_notifies_result() -> None:
    runner, notes, seen, sess = _runner(session=_FakeSession(result="Booked a table for two at eight."))

    async def go() -> None:
        ok, msg = runner.start(_ctx("background.run", "worker.gui"), "book the pub")
        assert ok and "#1" in msg
        await _drain(runner)

    asyncio.run(go())
    assert notes == ["Booked a table for two at eight."]
    assert sess.ran_with == "book the pub"


def test_inner_session_cannot_recurse() -> None:
    """The session that runs the job must NOT carry background.run (no nesting),
    but keeps every other granted capability."""
    runner, _notes, seen, _sess = _runner()

    async def go() -> None:
        runner.start(_ctx("background.run", "worker.gui", "web.search"), "do a thing")
        await _drain(runner)

    asyncio.run(go())
    assert seen, "the session factory was called"
    inner = seen[0]
    assert "background.run" not in inner.capabilities
    assert {"worker.gui", "web.search"} <= inner.capabilities


def test_empty_task_rejected() -> None:
    runner, *_ = _runner()
    ok, msg = runner.start(_ctx("background.run"), "   ")
    assert not ok and "description" in msg


def test_concurrency_cap_rejects() -> None:
    cfg = BackgroundConfig(_env_file=None, max_concurrent=1)
    # A slow job keeps slot #1 busy so the second start is rejected.
    runner, notes, _seen, _sess = _runner(cfg, session=_FakeSession(delay=0.05, result="r"))

    async def go() -> None:
        ok1, _ = runner.start(_ctx("background.run"), "first")
        ok2, msg2 = runner.start(_ctx("background.run"), "second")
        assert ok1 is True
        assert ok2 is False and "wait" in msg2
        await _drain(runner)

    asyncio.run(go())
    assert notes == ["r"]  # only the first ever ran


def test_failing_job_notifies_friendly_message_not_crash() -> None:
    runner, notes, *_ = _runner(session=_FakeSession(raises=RuntimeError("kaboom")))

    async def go() -> None:
        ok, _ = runner.start(_ctx("background.run"), "explode please")
        assert ok
        await _drain(runner)

    asyncio.run(go())
    assert len(notes) == 1
    assert "snag" in notes[0] and "kaboom" in notes[0]


def test_timeout_job_notifies_and_recovers() -> None:
    cfg = BackgroundConfig(_env_file=None, timeout_s=0.02)
    runner, notes, *_ = _runner(cfg, session=_FakeSession(delay=0.2, result="never"))

    async def go() -> None:
        runner.start(_ctx("background.run"), "slow task")
        await _drain(runner)

    asyncio.run(go())
    assert len(notes) == 1
    assert "ran out of time" in notes[0]


# --- BrainSession.run_task (headless agentic execution) --------------------

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
    def __init__(self, scripted: list[_Msg]) -> None:
        self._s, self.calls = scripted, 0

    async def complete_with_tools(self, messages, *, model=None, tools=None, usage_out=None):  # noqa: ANN001
        m = self._s[self.calls]
        self.calls += 1
        return m


def test_run_task_executes_tools_then_summarises(tmp_path) -> None:
    cfg = load_config()
    ctx = _ctx("files.read", "files.write")
    registry = build_registry(ToolsConfig(_env_file=None, files_root=str(tmp_path)))
    gateway = _Gateway([
        _Msg(tool_calls=[_Call("c1", "write_file", '{"path": "n.md", "content": "ok"}')]),
        _Msg(content="Done — I saved the note for you."),
    ])
    session = BrainSession(
        cfg, ctx, gateway=gateway, tts=None, memory=None, tracer=None, registry=registry
    )

    out = asyncio.run(session.run_task("save a note saying ok", max_rounds=4))

    assert out == "Done — I saved the note for you."
    assert (tmp_path / "n.md").read_text() == "ok"  # the tool really ran
    assert session._history == []  # background work never touches the live conversation
