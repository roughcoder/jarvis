"""Remote (Managed Agents) client + tools — no network (Phase 3 cloud lane).

The client is verified against an httpx MockTransport so the exact endpoints,
beta header, and request bodies are pinned (the repo-path lesson: don't guess an
external API's shape). The tools are checked with stubbed client methods.
"""

from __future__ import annotations

import asyncio
import json

import httpx

from jarvis.brain.context import RequestContext
from jarvis.config import RemoteConfig
from jarvis.remote.client import RemoteClient
from jarvis.tools.remote import make_remote_tools
from conftest import request_context


def _ctx(*caps: str) -> RequestContext:
    return request_context(*caps, device_id="neil-mac", identity="neil", scope="personal")


def test_client_builds_correct_requests(monkeypatch) -> None:
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen[request.url.path] = json.loads(request.content) if request.content else {}
        seen["beta"] = request.headers.get("anthropic-beta")
        seen["key"] = request.headers.get("x-api-key")
        return httpx.Response(200, json={"id": "x1", "version": 1})

    transport = httpx.MockTransport(handler)
    orig = httpx.AsyncClient
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: orig(transport=transport, **kw))

    cfg = RemoteConfig(_env_file=None, api_key="sk-test", agent_id="a1", environment_id="e1")
    client = RemoteClient(cfg)

    asyncio.run(client.create_agent("A", "be helpful"))
    assert seen["/v1/agents"]["tools"] == [{"type": "agent_toolset_20260401"}]
    assert seen["/v1/agents"]["model"] == "claude-opus-4-8"
    assert seen["beta"] == "managed-agents-2026-04-01"
    assert seen["key"] == "sk-test"

    asyncio.run(client.create_environment("env"))
    assert seen["/v1/environments"]["config"] == {"type": "cloud", "networking": {"type": "unrestricted"}}

    asyncio.run(client.create_session("title"))
    assert seen["/v1/sessions"] == {"agent": "a1", "environment_id": "e1", "title": "title"}

    asyncio.run(client.send_task("sess_9", "do x"))
    ev = seen["/v1/sessions/sess_9/events"]["events"][0]
    assert ev["type"] == "user.message"
    assert ev["content"][0]["text"] == "do x"


def test_remote_tools_gating_and_dispatch(monkeypatch) -> None:
    async def fake_create_session(self, title):  # noqa: ANN001
        return {"id": "sess_123"}

    async def fake_send_task(self, sid, text):  # noqa: ANN001
        return {}

    monkeypatch.setattr(RemoteClient, "create_session", fake_create_session)
    monkeypatch.setattr(RemoteClient, "send_task", fake_send_task)

    cfg = RemoteConfig(_env_file=None, api_key="k", agent_id="a1", environment_id="e1")
    tools = {t.name: t for t in make_remote_tools(cfg)}
    assert tools["start_remote_coding_job"].required_capability == "remote.code"
    out = asyncio.run(tools["start_remote_coding_job"].handler(_ctx("remote.code"), {"task": "build a thing"}))
    assert "sess_123" in out

    # not configured -> points the user at remote-setup, doesn't crash
    bare = RemoteConfig(_env_file=None, api_key="", agent_id="", environment_id="")
    t2 = {t.name: t for t in make_remote_tools(bare)}
    out2 = asyncio.run(t2["start_remote_coding_job"].handler(_ctx("remote.code"), {"task": "x"}))
    assert "remote-setup" in out2
