"""Worker tools — the brain's gated HTTP dispatch to the daemon (Phase 3c).

Covers capability gating, graceful handling when the worker is down, and a live
brain-tool -> daemon round-trip over HTTP.
"""

from __future__ import annotations

import asyncio

import pytest

from jarvis.brain.context import RequestContext
from jarvis.config import WorkerConfig, load_config
from jarvis.tools import build_registry
from jarvis.tools.worker import make_worker_tools


def _ctx(*caps: str) -> RequestContext:
    return RequestContext("neil-mac", "neil", "personal", frozenset(caps))


def test_worker_tools_registered_and_gated() -> None:
    cfg = load_config()
    reg = build_registry(cfg.tools, worker=cfg.worker)
    # deny-by-default: no worker caps => no worker tools offered
    assert "run_shell" not in {t.name for t in reg.available_for(_ctx())}
    # granting a capability reveals exactly its tools
    assert "run_shell" in {t.name for t in reg.available_for(_ctx("worker.shell"))}
    code_tools = {t.name for t in reg.available_for(_ctx("worker.code"))}
    assert {"start_coding_job", "check_coding_job"} <= code_tools
    assert "run_shell" not in code_tools  # different capability

    tools = {t.name: t for t in make_worker_tools(cfg.worker)}
    assert tools["run_shell"].required_capability == "worker.shell"
    assert tools["start_coding_job"].required_capability == "worker.code"
    assert tools["run_applescript"].required_capability == "worker.applescript"
    assert tools["run_shell"].announce is True
    assert tools["take_screenshot"].announce is False


def test_shell_tool_unreachable_returns_error() -> None:
    cfg = WorkerConfig(_env_file=None, host="localhost", port=1, request_timeout_s=2.0)
    shell = {t.name: t for t in make_worker_tools(cfg)}["run_shell"]
    out = asyncio.run(shell.handler(_ctx("worker.shell"), {"command": "echo hi"}))
    assert out.startswith("error: worker unreachable")


def test_shell_tool_against_live_daemon() -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web

    from jarvis.worker.server import make_app

    async def go() -> str:
        daemon = WorkerConfig(_env_file=None, token="", workspace="jarvis-workspace/worker")
        runner = web.AppRunner(make_app(daemon))
        await runner.setup()
        site = web.TCPSite(runner, "localhost", 8804)
        await site.start()
        try:
            tcfg = WorkerConfig(_env_file=None, host="localhost", port=8804, request_timeout_s=10.0)
            shell = {t.name: t for t in make_worker_tools(tcfg)}["run_shell"]
            return await shell.handler(_ctx("worker.shell"), {"command": "echo brain-to-worker"})
        finally:
            await runner.cleanup()

    assert asyncio.run(go()) == "brain-to-worker"
