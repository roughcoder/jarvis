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
from conftest import request_context


def _ctx(*caps: str) -> RequestContext:
    return request_context(*caps, device_id="neil-mac", identity="neil", scope="personal")


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
    assert tools["clean_up_coding_jobs"].required_capability == "worker.code"
    assert tools["run_shell"].announce is True
    assert tools["take_screenshot"].announce is False


def test_shell_tool_unreachable_returns_error() -> None:
    cfg = WorkerConfig(_env_file=None, host="localhost", port=1, request_timeout_s=2.0)
    shell = {t.name: t for t in make_worker_tools(cfg)}["run_shell"]
    out = asyncio.run(shell.handler(_ctx("worker.shell"), {"command": "echo hi"}))
    assert out.startswith("error: worker unreachable")


def test_clean_output_strips_codex_noise() -> None:
    from jarvis.tools.worker import _clean_output

    raw = (
        "OpenAI Codex v0.1\n--------\nworkdir: /x\nmodel: gpt\n--------\n"
        "user\nsay hi\nhook: SessionStart\ncodex\npong\ntokens used\n21000\npong"
    )
    out = _clean_output(raw)
    assert "hook:" not in out
    assert "tokens used" not in out
    assert "OpenAI Codex" not in out
    assert "pong" in out


def test_shell_tool_against_live_daemon(tmp_path) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web

    from jarvis.worker.server import make_app

    async def go() -> str:
        daemon = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"))
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


def test_check_latest_and_list_against_live_daemon(tmp_path) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web

    from jarvis.worker.server import make_app

    async def go():  # noqa: ANN202
        # `echo` stands in for the coding agent so the job finishes instantly.
        daemon = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"), codex_bin="echo")
        runner = web.AppRunner(make_app(daemon))
        await runner.setup()
        site = web.TCPSite(runner, "localhost", 8805)
        await site.start()
        try:
            tcfg = WorkerConfig(_env_file=None, host="localhost", port=8805, request_timeout_s=10.0)
            tools = {t.name: t for t in make_worker_tools(tcfg)}
            ctx = _ctx("worker.code")
            started = await tools["start_coding_job"].handler(ctx, {"task": "say hi"})
            checked = ""
            for _ in range(300):
                checked = await tools["check_coding_job"].handler(ctx, {})  # no id => latest
                if "still running" not in checked:
                    break
                await asyncio.sleep(0.05)
            listed = await tools["list_coding_jobs"].handler(ctx, {})
            return started, checked, listed
        finally:
            await runner.cleanup()

    started, checked, listed = asyncio.run(go())
    assert "Started the coding job" in started
    assert "say hi" in checked  # checked the latest job, result echoed
    assert "total" in listed
