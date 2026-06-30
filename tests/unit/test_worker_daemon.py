"""Worker daemon HTTP surface — tested in isolation (Phase 3c).

Spins up the real aiohttp app on a local port and drives it over HTTP: health,
auth, a shell dispatch, an unknown action, and a `code` job lifecycle (using
`echo` as a stand-in agent so it's instant). Self-contained — no gateway/keys.
Skips if aiohttp (the `worker` extra) isn't installed.
"""

from __future__ import annotations

import asyncio
import pathlib
import subprocess
import threading

import httpx
import pytest

pytest.importorskip("aiohttp")
from aiohttp import web  # noqa: E402

from jarvis.capabilities import (  # noqa: E402
    FORGE_BRANCH_PUSH,
    FORGE_PR_CREATE,
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_STOP,
    WORKER_SESSION_TURN,
)
from jarvis.config import WorkerConfig  # noqa: E402
from jarvis.worker.server import make_app  # noqa: E402
from jarvis.worker.authority import WorkerSessionAuthority  # noqa: E402
from jarvis.worker.sessions import WorkerSession  # noqa: E402
from jarvis.worker.sessions import SessionManager  # noqa: E402


async def _with_server(cfg: WorkerConfig, port: int, fn):  # noqa: ANN001
    runner = web.AppRunner(make_app(cfg))
    await runner.setup()
    site = web.TCPSite(runner, "localhost", port)
    await site.start()
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            return await fn(f"http://localhost:{port}", client)
    finally:
        await runner.cleanup()


def _authority_metadata(engine: str = "codex", extra_actions: list[str] | None = None) -> dict:
    allowed_actions = ["worker.session.create", "worker.session.turn", "forge.github.branch.push"]
    allowed_actions.extend(extra_actions or [])
    landing = {"mode": "branch_only", "allow_merge": False}
    return {
        "execution_envelope": {
            "run_id": f"run_{engine}",
            "engine": engine,
            "allowed_actions": allowed_actions,
            "landing": landing,
        },
        "allowed_actions": allowed_actions,
        "landing": landing,
    }


def _session_with_authority(
    *,
    provider: str = "codex",
    allowed_actions: list[str] | None = None,
    landing: dict | None = None,
) -> WorkerSession:
    return WorkerSession(
        session_id="sess_authority",
        provider=provider,
        engine=provider,
        metadata={
            "execution_envelope": {
                "allowed_actions": allowed_actions or [WORKER_SESSION_TURN],
                "landing": landing or {"mode": "read_only", "allow_merge": False},
            }
        },
    )


def test_worker_session_authority_maps_read_only_to_codex_read_only() -> None:
    authority = WorkerSessionAuthority.from_session(_session_with_authority())

    assert authority.codex_sandbox == "read-only"
    assert authority.codex_approval_policy == "never"


def test_worker_session_authority_maps_branch_only_to_workspace_write() -> None:
    authority = WorkerSessionAuthority.from_session(
        _session_with_authority(
            allowed_actions=[WORKER_SESSION_TURN, FORGE_BRANCH_PUSH],
            landing={"mode": "branch_only", "allow_merge": False},
        )
    )

    assert authority.codex_sandbox == "workspace-write"
    assert authority.codex_approval_policy == "never"
    assert authority.claude_permission_mode == "dontAsk"


def test_worker_session_authority_maps_draft_pr_and_approval_modes() -> None:
    authority = WorkerSessionAuthority.from_session(
        _session_with_authority(
            allowed_actions=[
                WORKER_SESSION_TURN,
                WORKER_SESSION_INPUT,
                WORKER_SESSION_APPROVE,
                FORGE_BRANCH_PUSH,
                FORGE_PR_CREATE,
            ],
            landing={"mode": "draft_pr", "allow_merge": False},
        )
    )

    assert authority.codex_sandbox == "workspace-write"
    assert authority.codex_approval_policy == "on-request"
    assert authority.claude_permission_mode == "default"


def test_worker_session_authority_fails_closed_for_unsupported_landing() -> None:
    with pytest.raises(RuntimeError, match=FORGE_PR_CREATE):
        WorkerSessionAuthority.from_session(
            _session_with_authority(
                allowed_actions=[WORKER_SESSION_TURN, FORGE_BRANCH_PUSH],
                landing={"mode": "draft_pr", "allow_merge": False},
            )
        )

    with pytest.raises(RuntimeError, match="read-only"):
        WorkerSessionAuthority.from_session(_session_with_authority(provider="claude"), provider="claude")


def test_session_manager_serializes_concurrent_session_writes(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create({"provider": "fake", "engine": "fake"})

    def append_events() -> None:
        for idx in range(40):
            sessions.append_event(session.session_id, "provider.log", {"idx": idx})

    def update_metadata() -> None:
        for idx in range(40):
            sessions.update_metadata(session.session_id, {f"k{idx}": str(idx)})

    def update_status() -> None:
        for _ in range(40):
            sessions.update_status(session.session_id, "running")

    threads = [
        threading.Thread(target=append_events),
        threading.Thread(target=update_metadata),
        threading.Thread(target=update_status),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    fetched = sessions.get(session.session_id)
    assert fetched is not None
    assert fetched.status == "running"
    assert fetched.metadata["k39"] == "39"
    assert len([event for event in sessions.events(session.session_id) if event.type == "provider.log"]) == 40


def test_daemon_health_shell_and_auth(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    h = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        health = (await c.get(base + "/health")).json()
        noauth = await c.post(base + "/run", json={"action": "shell", "args": {"command": "echo x"}})
        shell = (
            await c.post(base + "/run", json={"action": "shell", "args": {"command": "echo worker-ok"}}, headers=h)
        ).json()
        bad = await c.post(base + "/run", json={"action": "shell"}, headers={"Authorization": "Bearer nope"})
        unknown = await c.post(base + "/run", json={"action": "frobnicate"}, headers=h)
        return health, noauth.status_code, shell, bad.status_code, unknown.status_code

    health, noauth, shell, bad, unknown = asyncio.run(_with_server(cfg, 8802, calls))
    assert health["ok"] is True
    assert noauth == 401  # missing token
    assert bad == 401  # wrong token
    assert shell["output"] == "worker-ok"
    assert unknown == 400  # unknown action


def test_daemon_shell_uses_expanded_default_workspace(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    cfg = WorkerConfig(_env_file=None, token="", workspace="~/jarvis-worker")

    async def calls(base, c):  # noqa: ANN001
        health = (await c.get(base + "/health")).json()
        shell = (
            await c.post(base + "/run", json={"action": "shell", "args": {"command": "pwd"}})
        ).json()
        return health, shell

    health, shell = asyncio.run(_with_server(cfg, 8817, calls))

    assert health["workspace"] == str(home / "jarvis-worker")
    assert shell["output"] == str(home / "jarvis-worker")


def test_daemon_code_dispatch_runs_a_job(tmp_path) -> None:
    # `echo` stands in for the coding agent so the job finishes instantly.
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        disp = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "hello-job"}})).json()
        jid = disp["job_id"]
        status = "running"
        for _ in range(100):
            status = (await c.get(f"{base}/jobs/{jid}")).json()["status"]
            if status != "running":
                break
            await asyncio.sleep(0.02)
        listed = (await c.get(base + "/jobs")).json()
        return disp, status, listed

    disp, status, listed = asyncio.run(_with_server(cfg, 8803, calls))
    assert disp["ok"] and disp["job_id"]
    assert status == "done"
    assert len(listed["jobs"]) >= 1


def test_daemon_session_api_records_structured_events(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        noauth = await c.get(base + "/sessions")
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "run_id": "run_123",
                    "provider": "fake",
                    "engine": "fake",
                    "repo": "roughcoder/jarvis",
                    "branch": "jarvis/live-session",
                    "title": "Fix worker sessions",
                    "metadata": _authority_metadata(
                        "fake",
                        extra_actions=[WORKER_SESSION_APPROVE, WORKER_SESSION_INTERRUPT],
                    ),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        turn = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"prompt": "inspect the repo", "metadata": {"surface": "test"}},
                headers=headers,
            )
        ).json()
        retry = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"prompt": "inspect the repo again", "idempotency_key": "idem_fake"},
                headers=headers,
            )
        ).json()
        retry_again = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"prompt": "inspect the repo a third time", "idempotency_key": "idem_fake"},
                headers=headers,
            )
        ).json()
        approval = (
            await c.post(
                f"{base}/sessions/{session_id}/approval",
                json={"request_id": "approval_1", "decision": "approved"},
                headers=headers,
            )
        ).json()
        interrupted = (await c.post(f"{base}/sessions/{session_id}/interrupt", json={}, headers=headers)).json()
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        listed = (await c.get(base + "/sessions", headers=headers)).json()["sessions"]
        fetched = (await c.get(f"{base}/sessions/{session_id}", headers=headers)).json()
        return noauth.status_code, created, turn, retry, retry_again, approval, interrupted, events, listed, fetched

    noauth, created, turn, retry, retry_again, approval, interrupted, events, listed, fetched = asyncio.run(
        _with_server(cfg, 8828, calls)
    )

    assert noauth == 401
    assert created["ok"] is True
    assert created["session"]["run_id"] == "run_123"
    assert turn["turn_id"]
    assert [event["type"] for event in turn["events"]] == [
        "turn.started",
        "assistant.delta",
        "assistant.message",
        "checkpoint.created",
        "turn.completed",
    ]
    assert retry.get("idempotent") is not True
    assert retry_again["idempotent"] is True
    assert [event["type"] for event in retry_again["events"]] == [
        "turn.started",
        "assistant.delta",
        "assistant.message",
        "checkpoint.created",
        "turn.completed",
    ]
    assert approval["event"]["type"] == "approval.resolved"
    assert interrupted["session"]["status"] == "interrupted"
    assert [event["type"] for event in events] == [
        "session.created",
        "turn.started",
        "assistant.delta",
        "assistant.message",
        "checkpoint.created",
        "turn.completed",
        "turn.started",
        "assistant.delta",
        "assistant.message",
        "checkpoint.created",
        "turn.completed",
        "approval.resolved",
        "session.interrupted",
    ]
    assert listed[0]["session_id"] == created["session"]["session_id"]
    assert fetched["status"] == "interrupted"


def test_daemon_session_controls_require_envelope_authority(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "provider": "fake",
                    "engine": "fake",
                    "title": "Missing control authority",
                    "metadata": _authority_metadata("fake"),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        approval = await c.post(
            f"{base}/sessions/{session_id}/approval",
            json={"request_id": "approval_1", "decision": "approved"},
            headers=headers,
        )
        user_input = await c.post(
            f"{base}/sessions/{session_id}/input",
            json={"request_id": "input_1", "text": "continue"},
            headers=headers,
        )
        interrupted = await c.post(f"{base}/sessions/{session_id}/interrupt", json={}, headers=headers)
        stopped = await c.post(f"{base}/sessions/{session_id}/stop", json={}, headers=headers)
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return approval, user_input, interrupted, stopped, events

    approval, user_input, interrupted, stopped, events = asyncio.run(_with_server(cfg, 8838, calls))

    assert approval.status_code == 400
    assert user_input.status_code == 400
    assert interrupted.status_code == 400
    assert stopped.status_code == 400
    assert WORKER_SESSION_APPROVE in approval.json()["error"]
    assert WORKER_SESSION_INPUT in user_input.json()["error"]
    assert WORKER_SESSION_INTERRUPT in interrupted.json()["error"]
    assert WORKER_SESSION_STOP in stopped.json()["error"]
    assert [event["type"] for event in events] == ["session.created"]


def test_daemon_session_creation_provisions_repo_worktree(tmp_path) -> None:
    dev = tmp_path / "dev"
    repo = dev / "jarvis"
    repo.mkdir(parents=True)
    subprocess.run(["git", "init"], cwd=repo, check=True, capture_output=True)
    (repo / "README.md").write_text("hello\n")
    subprocess.run(["git", "add", "README.md"], cwd=repo, check=True, capture_output=True)
    subprocess.run(
        ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "initial"],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    cfg = WorkerConfig(
        _env_file=None,
        token="tkn",
        workspace=str(tmp_path / "worker"),
        repo_root=str(dev),
        clone_missing=False,
    )
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        return (
            await c.post(
                base + "/sessions",
                json={
                    "run_id": "run_repo",
                    "provider": "fake",
                    "engine": "fake",
                    "repo": "roughcoder/jarvis",
                    "branch": "jarvis/session",
                    "title": "Fix worker sessions",
                    "metadata": {**_authority_metadata("fake"), "provision_workspace": True},
                },
                headers=headers,
            )
        ).json()

    created = asyncio.run(_with_server(cfg, 8833, calls))

    assert created["ok"] is True
    cwd = pathlib.Path(created["session"]["cwd"])
    assert cwd.exists()
    assert cwd != repo
    assert created["session"]["branch"].startswith("jarvis/")
    assert created["session"]["metadata"]["source_repo"] == str(repo)


def test_daemon_session_pending_requests_and_checkpoints_are_projected(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "provider": "fake",
                    "engine": "fake",
                    "title": "Pending request",
                    "metadata": _authority_metadata(
                        "fake",
                        extra_actions=[WORKER_SESSION_APPROVE, WORKER_SESSION_INPUT],
                    ),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        approval_turn = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"turn_id": "turn_need_approval", "prompt": "request approval"},
                headers=headers,
            )
        ).json()
        pending_before = (await c.get(f"{base}/sessions/{session_id}/requests", headers=headers)).json()
        global_pending = (await c.get(f"{base}/sessions/requests", headers=headers)).json()
        denied = (
            await c.post(
                f"{base}/sessions/{session_id}/approval",
                json={"request_id": "approval_turn_need_approval", "decision": "denied"},
                headers=headers,
            )
        ).json()
        pending_after = (await c.get(f"{base}/sessions/{session_id}/requests", headers=headers)).json()
        input_turn = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"turn_id": "turn_need_input", "prompt": "request input"},
                headers=headers,
            )
        ).json()
        input_pending = (await c.get(f"{base}/sessions/{session_id}/requests", headers=headers)).json()
        input_reply = (
            await c.post(
                f"{base}/sessions/{session_id}/input",
                json={"request_id": "input_turn_need_input", "text": "continue"},
                headers=headers,
            )
        ).json()
        input_pending_after = (await c.get(f"{base}/sessions/{session_id}/requests", headers=headers)).json()
        checkpoint_turn = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"turn_id": "turn_checkpoint", "prompt": "complete normally"},
                headers=headers,
            )
        ).json()
        checkpoints = (await c.get(f"{base}/sessions/{session_id}/checkpoints", headers=headers)).json()
        restored = (
            await c.post(
                f"{base}/sessions/{session_id}/checkpoints/restore",
                json={"checkpoint_id": "ckpt_turn_checkpoint"},
                headers=headers,
            )
        ).json()
        checkpoints_after_restore = (await c.get(f"{base}/sessions/{session_id}/checkpoints", headers=headers)).json()
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return (
            approval_turn,
            pending_before,
            global_pending,
            denied,
            pending_after,
            input_turn,
            input_pending,
            input_reply,
            input_pending_after,
            checkpoint_turn,
            checkpoints,
            restored,
            checkpoints_after_restore,
            events,
        )

    (
        approval_turn,
        pending_before,
        global_pending,
        denied,
        pending_after,
        input_turn,
        input_pending,
        input_reply,
        input_pending_after,
        checkpoint_turn,
        checkpoints,
        restored,
        checkpoints_after_restore,
        events,
    ) = asyncio.run(_with_server(cfg, 8830, calls))

    assert [event["type"] for event in approval_turn["events"]] == ["turn.started", "approval.requested"]
    assert pending_before["requests"][0]["kind"] == "approval"
    assert pending_before["requests"][0]["request_id"] == "approval_turn_need_approval"
    assert global_pending["requests"][0]["session_id"] == pending_before["requests"][0]["session_id"]
    assert denied["event"]["type"] == "approval.resolved"
    assert pending_after["requests"] == []
    assert [event["type"] for event in input_turn["events"]] == ["turn.started", "input.requested"]
    assert input_pending["requests"][0]["request_id"] == "input_turn_need_input"
    assert input_reply["event"]["type"] == "input.received"
    assert input_pending_after["requests"] == []
    assert [event["type"] for event in checkpoint_turn["events"]] == [
        "turn.started",
        "assistant.delta",
        "assistant.message",
        "checkpoint.created",
        "turn.completed",
    ]
    assert checkpoints["checkpoints"][0]["checkpoint_id"] == "ckpt_turn_checkpoint"
    assert restored["event"]["type"] == "checkpoint.restored"
    assert checkpoints_after_restore["checkpoints"][0]["restored"] is True
    assert "turn.failed" in [event["type"] for event in events]


def test_daemon_codex_provider_projects_app_server_events(tmp_path) -> None:
    agent = tmp_path / "fake-codex"
    agent.write_text(
        """#!/usr/bin/env python3
import json
import sys


def emit(payload):
    print(json.dumps(payload), flush=True)


for line in sys.stdin:
    if not line.strip():
        continue
    payload = json.loads(line)
    method = payload.get("method")
    request_id = payload.get("id")
    if method == "initialize":
        emit({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method in {"thread/start", "thread/resume"}:
        emit({
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "thread": {
                    "id": "thread_fake",
                    "sessionId": "session_fake",
                    "path": "/tmp/thread_fake.json",
                }
            },
        })
    elif method == "turn/start":
        emit({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn_fake"}}})
        emit({"jsonrpc": "2.0", "method": "turn/started", "params": {"turn": {"id": "turn_fake"}}})
        emit({"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": {"delta": "he"}})
        emit({"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": {"delta": "llo"}})
        emit({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {"item": {"type": "agentMessage", "text": "hello"}},
        })
        emit({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {"turn": {"id": "turn_fake", "status": "completed"}},
        })
"""
    )
    agent.chmod(0o755)
    cfg = WorkerConfig(
        _env_file=None,
        token="tkn",
        workspace=str(tmp_path / "worker"),
        codex_bin=str(agent),
        job_timeout_s=5,
    )
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "run_id": "run_codex",
                    "provider": "codex",
                    "engine": "codex",
                    "repo": "roughcoder/jarvis",
                    "branch": "jarvis/codex-session",
                    "title": "Codex app-server projection",
                    "metadata": _authority_metadata("codex"),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        turn = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"prompt": "reply with hello", "idempotency_key": "idem_codex"},
                headers=headers,
            )
        ).json()
        events = []
        for _ in range(100):
            events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            if any(event["type"] == "turn.completed" for event in events):
                break
            await asyncio.sleep(0.05)
        fetched = (await c.get(f"{base}/sessions/{session_id}", headers=headers)).json()
        return created, turn, events, fetched

    created, turn, events, fetched = asyncio.run(_with_server(cfg, 8829, calls))

    event_types = [event["type"] for event in events]
    assert created["ok"] is True
    assert [event["type"] for event in turn["events"]] == ["turn.started", "provider.started"]
    assert "provider.process.started" in event_types
    assert "provider.thread.ready" in event_types
    assert "provider.turn.started" in event_types
    assert event_types.count("assistant.delta") == 2
    assert "assistant.message" in event_types
    assert "turn.completed" in event_types
    assert fetched["status"] == "completed"
    assert fetched["metadata"]["codex_thread_id"] == "thread_fake"
    assert fetched["metadata"]["provider_session_id"] == "session_fake"


def test_daemon_codex_interrupt_preserves_cancelled_status(tmp_path) -> None:
    agent = tmp_path / "fake-codex-slow"
    agent.write_text(
        """#!/usr/bin/env python3
import json
import sys
import time


def emit(payload):
    print(json.dumps(payload), flush=True)


for line in sys.stdin:
    if not line.strip():
        continue
    payload = json.loads(line)
    method = payload.get("method")
    request_id = payload.get("id")
    if method == "initialize":
        emit({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method in {"thread/start", "thread/resume"}:
        emit({"jsonrpc": "2.0", "id": request_id, "result": {"thread": {"id": "thread_slow"}}})
    elif method == "turn/start":
        emit({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn_slow"}}})
        emit({"jsonrpc": "2.0", "method": "turn/started", "params": {"turn": {"id": "turn_slow"}}})
        while True:
            time.sleep(1)
"""
    )
    agent.chmod(0o755)
    cfg = WorkerConfig(
        _env_file=None,
        token="tkn",
        workspace=str(tmp_path / "worker"),
        codex_bin=str(agent),
        job_timeout_s=5,
    )
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "run_id": "run_codex",
                    "provider": "codex",
                    "engine": "codex",
                    "title": "Interrupt Codex",
                    "metadata": _authority_metadata("codex", extra_actions=[WORKER_SESSION_INTERRUPT]),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={"prompt": "wait", "idempotency_key": "idem_codex_interrupt"},
            headers=headers,
        )
        events = []
        for _ in range(100):
            events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            if any(event["type"] == "provider.process.started" for event in events):
                break
            await asyncio.sleep(0.05)
        interrupted = (await c.post(f"{base}/sessions/{session_id}/interrupt", json={}, headers=headers)).json()
        await asyncio.sleep(0.2)
        fetched = (await c.get(f"{base}/sessions/{session_id}", headers=headers)).json()
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return interrupted, fetched, events

    interrupted, fetched, events = asyncio.run(_with_server(cfg, 8834, calls))

    assert interrupted["session"]["status"] == "interrupted"
    assert fetched["status"] == "interrupted"
    assert "turn.failed" not in [event["type"] for event in events]


def test_daemon_codex_stop_preserves_stopped_status(tmp_path) -> None:
    agent = tmp_path / "fake-codex-stop"
    agent.write_text(
        """#!/usr/bin/env python3
import json
import sys
import time


def emit(payload):
    print(json.dumps(payload), flush=True)


for line in sys.stdin:
    if not line.strip():
        continue
    payload = json.loads(line)
    method = payload.get("method")
    request_id = payload.get("id")
    if method == "initialize":
        emit({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method in {"thread/start", "thread/resume"}:
        emit({"jsonrpc": "2.0", "id": request_id, "result": {"thread": {"id": "thread_stop"}}})
    elif method == "turn/start":
        emit({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn_stop"}}})
        emit({"jsonrpc": "2.0", "method": "turn/started", "params": {"turn": {"id": "turn_stop"}}})
        while True:
            time.sleep(1)
"""
    )
    agent.chmod(0o755)
    cfg = WorkerConfig(
        _env_file=None,
        token="tkn",
        workspace=str(tmp_path / "worker"),
        codex_bin=str(agent),
        job_timeout_s=5,
    )
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "run_id": "run_codex",
                    "provider": "codex",
                    "engine": "codex",
                    "title": "Stop Codex",
                    "metadata": _authority_metadata("codex", extra_actions=[WORKER_SESSION_STOP]),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={"prompt": "wait", "idempotency_key": "idem_codex_stop"},
            headers=headers,
        )
        events = []
        for _ in range(100):
            events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            if any(event["type"] == "provider.process.started" for event in events):
                break
            await asyncio.sleep(0.05)
        stopped = (await c.post(f"{base}/sessions/{session_id}/stop", json={}, headers=headers)).json()
        await asyncio.sleep(0.2)
        fetched = (await c.get(f"{base}/sessions/{session_id}", headers=headers)).json()
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return stopped, fetched, events

    stopped, fetched, events = asyncio.run(_with_server(cfg, 8835, calls))

    assert stopped["session"]["status"] == "stopped"
    assert fetched["status"] == "stopped"
    assert "turn.failed" not in [event["type"] for event in events]


def test_daemon_codex_approval_waits_for_endpoint_resolution(tmp_path) -> None:
    agent = tmp_path / "fake-codex-approval"
    agent.write_text(
        """#!/usr/bin/env python3
import json
import sys


def emit(payload):
    print(json.dumps(payload), flush=True)


for line in sys.stdin:
    if not line.strip():
        continue
    payload = json.loads(line)
    method = payload.get("method")
    request_id = payload.get("id")
    if method == "initialize":
        emit({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method in {"thread/start", "thread/resume"}:
        emit({"jsonrpc": "2.0", "id": request_id, "result": {"thread": {"id": "thread_approval"}}})
    elif method == "turn/start":
        emit({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn_approval"}}})
        emit({"jsonrpc": "2.0", "method": "turn/started", "params": {"turn": {"id": "turn_approval"}}})
        emit({
            "jsonrpc": "2.0",
            "id": "approval_rpc",
            "method": "item/commandExecution/requestApproval",
            "params": {"command": "pytest", "cwd": "/tmp"},
        })
    elif request_id == "approval_rpc":
        emit({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {"item": {"type": "agentMessage", "text": payload["result"]["decision"]}},
        })
        emit({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {"turn": {"id": "turn_approval", "status": "completed"}},
        })
"""
    )
    agent.chmod(0o755)
    cfg = WorkerConfig(
        _env_file=None,
        token="tkn",
        workspace=str(tmp_path / "worker"),
        codex_bin=str(agent),
        job_timeout_s=5,
    )
    headers = {"Authorization": "Bearer tkn"}
    metadata = _authority_metadata("codex")
    metadata["execution_envelope"]["allowed_actions"].append(WORKER_SESSION_APPROVE)
    metadata["allowed_actions"].append(WORKER_SESSION_APPROVE)

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "run_id": "run_codex",
                    "provider": "codex",
                    "engine": "codex",
                    "title": "Approve Codex",
                    "metadata": metadata,
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={"prompt": "ask approval", "idempotency_key": "idem_codex_approval"},
            headers=headers,
        )
        pending = {}
        events = []
        for _ in range(100):
            pending = (await c.get(f"{base}/sessions/{session_id}/requests", headers=headers)).json()
            events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            if pending["requests"]:
                break
            await asyncio.sleep(0.05)
        before_resolve_types = [event["type"] for event in events]
        approval = (
            await c.post(
                f"{base}/sessions/{session_id}/approval",
                json={"request_id": pending["requests"][0]["request_id"], "decision": "approved"},
                headers=headers,
            )
        ).json()
        for _ in range(100):
            events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            if any(event["type"] == "turn.completed" for event in events):
                break
            await asyncio.sleep(0.05)
        fetched = (await c.get(f"{base}/sessions/{session_id}", headers=headers)).json()
        return pending, before_resolve_types, approval, events, fetched

    pending, before_resolve_types, approval, events, fetched = asyncio.run(_with_server(cfg, 8836, calls))

    event_types = [event["type"] for event in events]
    assert pending["requests"][0]["kind"] == "approval"
    assert pending["requests"][0]["request_id"] == "approval_rpc"
    assert "turn.completed" not in before_resolve_types
    assert approval["event"]["type"] == "approval.resolved"
    assert "approval.resolved" in event_types
    assert "assistant.message" in event_types
    assert "turn.completed" in event_types
    assert fetched["status"] == "completed"


def test_daemon_codex_input_waits_for_endpoint_response(tmp_path) -> None:
    agent = tmp_path / "fake-codex-input"
    agent.write_text(
        """#!/usr/bin/env python3
import json
import sys


def emit(payload):
    print(json.dumps(payload), flush=True)


for line in sys.stdin:
    if not line.strip():
        continue
    payload = json.loads(line)
    method = payload.get("method")
    request_id = payload.get("id")
    if method == "initialize":
        emit({"jsonrpc": "2.0", "id": request_id, "result": {}})
    elif method in {"thread/start", "thread/resume"}:
        emit({"jsonrpc": "2.0", "id": request_id, "result": {"thread": {"id": "thread_input"}}})
    elif method == "turn/start":
        emit({"jsonrpc": "2.0", "id": request_id, "result": {"turn": {"id": "turn_input"}}})
        emit({"jsonrpc": "2.0", "method": "turn/started", "params": {"turn": {"id": "turn_input"}}})
        emit({
            "jsonrpc": "2.0",
            "id": "input_rpc",
            "method": "item/tool/requestUserInput",
            "params": {"prompt": "Need more context"},
        })
    elif request_id == "input_rpc":
        emit({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {"item": {"type": "agentMessage", "text": payload["result"]["answers"]["text"]}},
        })
        emit({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {"turn": {"id": "turn_input", "status": "completed"}},
        })
"""
    )
    agent.chmod(0o755)
    cfg = WorkerConfig(
        _env_file=None,
        token="tkn",
        workspace=str(tmp_path / "worker"),
        codex_bin=str(agent),
        job_timeout_s=5,
    )
    headers = {"Authorization": "Bearer tkn"}
    metadata = _authority_metadata("codex")
    metadata["execution_envelope"]["allowed_actions"].append(WORKER_SESSION_INPUT)
    metadata["allowed_actions"].append(WORKER_SESSION_INPUT)

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "run_id": "run_codex",
                    "provider": "codex",
                    "engine": "codex",
                    "title": "Input Codex",
                    "metadata": metadata,
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={"prompt": "ask input", "idempotency_key": "idem_codex_input"},
            headers=headers,
        )
        pending = {}
        events = []
        for _ in range(100):
            pending = (await c.get(f"{base}/sessions/{session_id}/requests", headers=headers)).json()
            events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            if pending["requests"]:
                break
            await asyncio.sleep(0.05)
        before_resolve_types = [event["type"] for event in events]
        response = (
            await c.post(
                f"{base}/sessions/{session_id}/input",
                json={"request_id": pending["requests"][0]["request_id"], "text": "continue with tests"},
                headers=headers,
            )
        ).json()
        for _ in range(100):
            events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            if any(event["type"] == "turn.completed" for event in events):
                break
            await asyncio.sleep(0.05)
        fetched = (await c.get(f"{base}/sessions/{session_id}", headers=headers)).json()
        return pending, before_resolve_types, response, events, fetched

    pending, before_resolve_types, response, events, fetched = asyncio.run(_with_server(cfg, 8837, calls))

    event_types = [event["type"] for event in events]
    assert pending["requests"][0]["kind"] == "input"
    assert pending["requests"][0]["request_id"] == "input_rpc"
    assert "turn.completed" not in before_resolve_types
    assert response["event"]["type"] == "input.received"
    assert "input.received" in event_types
    assert "assistant.message" in event_types
    assert "turn.completed" in event_types
    assert fetched["status"] == "completed"


def test_daemon_rejects_unknown_session_provider(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "run_id": "run_unknown",
                    "provider": "not-a-provider",
                    "engine": "not-a-provider",
                    "metadata": _authority_metadata("not-a-provider"),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        turn = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"prompt": "should not run"},
                headers=headers,
            )
        )
        return turn.status_code, turn.json(), (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()

    status_code, body, events = asyncio.run(_with_server(cfg, 8831, calls))

    assert status_code == 400
    assert body["ok"] is False
    assert "unsupported worker session provider" in body["error"]
    assert [event["type"] for event in events["events"]] == ["session.created"]


def test_daemon_real_provider_requires_execution_envelope_authority(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"), codex_bin="/missing/codex")
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={"run_id": "run_no_auth", "provider": "codex", "engine": "codex"},
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        turn = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"prompt": "should fail before spawn", "idempotency_key": "idem_no_auth"},
                headers=headers,
            )
        )
        session = (await c.get(f"{base}/sessions/{session_id}", headers=headers)).json()
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return turn.status_code, turn.json(), session, events

    status_code, body, session, events = asyncio.run(_with_server(cfg, 8832, calls))

    assert status_code == 400
    assert body["ok"] is False
    assert "worker.session.turn" in body["error"]
    assert session["status"] == "failed"
    assert [event["type"] for event in events] == ["session.created", "turn.started", "turn.failed"]


def test_daemon_claude_provider_projects_stream_json_events_and_resumes(tmp_path) -> None:
    agent = tmp_path / "fake-claude"
    agent.write_text(
        """#!/usr/bin/env python3
import json
import sys


def arg_value(flag):
    if flag not in sys.argv:
        return ""
    index = sys.argv.index(flag)
    return sys.argv[index + 1] if index + 1 < len(sys.argv) else ""


def emit(payload):
    print(json.dumps(payload), flush=True)


session_id = arg_value("--session-id") or arg_value("--resume") or "11111111-1111-4111-8111-111111111111"
emit({
    "type": "system",
    "subtype": "init",
    "session_id": session_id,
    "model": "claude-test",
    "cwd": "/tmp",
})
emit({"type": "assistant", "message": {"content": [{"type": "text", "text": "hello"}]}})
emit({"type": "result", "subtype": "success", "session_id": session_id, "total_cost_usd": 0})
"""
    )
    agent.chmod(0o755)
    cfg = WorkerConfig(
        _env_file=None,
        token="tkn",
        workspace=str(tmp_path / "worker"),
        claude_bin=str(agent),
        job_timeout_s=5,
    )
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "run_id": "run_claude",
                    "provider": "claude",
                    "engine": "claude",
                    "repo": "roughcoder/jarvis",
                    "branch": "jarvis/claude-session",
                    "title": "Claude stream-json projection",
                    "metadata": _authority_metadata("claude"),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        first = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"prompt": "reply with hello", "idempotency_key": "idem_claude_1"},
                headers=headers,
            )
        ).json()
        first_events = []
        for _ in range(100):
            first_events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            if any(event["type"] == "turn.completed" for event in first_events):
                break
            await asyncio.sleep(0.05)
        second = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={"prompt": "resume and reply again", "idempotency_key": "idem_claude_2"},
                headers=headers,
            )
        ).json()
        events = []
        for _ in range(100):
            events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            completed = [event for event in events if event["type"] == "turn.completed"]
            if len(completed) >= 2:
                break
            await asyncio.sleep(0.05)
        fetched = (await c.get(f"{base}/sessions/{session_id}", headers=headers)).json()
        return created, first, second, events, fetched

    created, first, second, events, fetched = asyncio.run(_with_server(cfg, 8830, calls))

    event_types = [event["type"] for event in events]
    process_events = [event for event in events if event["type"] == "provider.process.started"]
    assert created["ok"] is True
    assert [event["type"] for event in first["events"]] == ["turn.started", "provider.started"]
    assert [event["type"] for event in second["events"]] == ["turn.started", "provider.started"]
    assert "provider.session.ready" in event_types
    assert "assistant.message" in event_types
    assert event_types.count("turn.completed") == 2
    assert [event["data"]["resume"] for event in process_events] == [False, True]
    assert fetched["status"] == "completed"
    assert fetched["metadata"]["provider_session_id"]
    assert fetched["metadata"]["claude_session_started"] == "true"


def test_daemon_code_dispatch_marks_nonzero_agent_exit_as_error(tmp_path) -> None:
    agent = tmp_path / "bad-agent"
    agent.write_text("#!/usr/bin/env python3\nimport sys\nprint('bad auth')\nsys.exit(1)\n")
    agent.chmod(0o755)
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"), codex_bin=str(agent))

    async def calls(base, c):  # noqa: ANN001
        disp = (
            await c.post(
                base + "/run",
                json={"action": "code", "args": {"prompt": "hello"}},
            )
        ).json()
        jid = disp["job_id"]
        job = {}
        for _ in range(100):
            job = (await c.get(f"{base}/jobs/{jid}")).json()
            if job["status"] != "running":
                break
            await asyncio.sleep(0.02)
        return job

    job = asyncio.run(_with_server(cfg, 8820, calls))

    assert job["status"] == "error"
    assert "command exited with 1" in job["output"]
    assert "bad auth" in job["output"]


def test_daemon_health_advertises_supported_engines(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        agent="codex",
        supported_engines="codex,claude",
    )

    async def calls(base, c):  # noqa: ANN001
        return (await c.get(base + "/health")).json()

    health = asyncio.run(_with_server(cfg, 8818, calls))

    assert health["default_engine"] == "codex"
    assert health["supported_engines"] == ["codex", "claude"]


def test_daemon_code_dispatch_persists_engine_session_metadata(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        claude_bin="echo",
        supported_engines="codex,claude",
    )
    session_id = "550e8400-e29b-41d4-a716-446655440000"

    async def calls(base, c):  # noqa: ANN001
        disp = (
            await c.post(
                base + "/run",
                json={
                    "action": "code",
                    "args": {
                        "prompt": "hello",
                        "agent": "claude",
                        "session_id": session_id,
                        "session_name": "jarvis-hello",
                    },
                },
            )
        ).json()
        jid = disp["job_id"]
        job = {}
        for _ in range(100):
            job = (await c.get(f"{base}/jobs/{jid}")).json()
            if job["status"] != "running":
                break
            await asyncio.sleep(0.02)
        return disp, job

    disp, job = asyncio.run(_with_server(cfg, 8821, calls))

    assert disp["ok"]
    assert disp["engine"] == "claude"
    assert disp["session_id"] == session_id
    assert disp["session_name"] == "jarvis-hello"
    assert job["engine"] == "claude"
    assert job["session_id"] == session_id
    assert job["session_name"] == "jarvis-hello"
    assert "--session-id 550e8400-e29b-41d4-a716-446655440000 --name jarvis-hello -p hello" in job["output"]


def test_daemon_code_dispatch_resumes_claude_session(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        claude_bin="echo",
        supported_engines="codex,claude",
    )
    session_id = "550e8400-e29b-41d4-a716-446655440000"

    async def calls(base, c):  # noqa: ANN001
        disp = (
            await c.post(
                base + "/run",
                json={
                    "action": "code",
                    "args": {
                        "prompt": "follow up",
                        "agent": "claude",
                        "session_id": session_id,
                        "resume_session": True,
                    },
                },
            )
        ).json()
        jid = disp["job_id"]
        job = {}
        for _ in range(100):
            job = (await c.get(f"{base}/jobs/{jid}")).json()
            if job["status"] != "running":
                break
            await asyncio.sleep(0.02)
        return job

    job = asyncio.run(_with_server(cfg, 8822, calls))

    assert job["session_id"] == session_id
    assert "-p --resume 550e8400-e29b-41d4-a716-446655440000 follow up" in job["output"]


def test_daemon_resume_uses_worker_owned_cwd_without_cleanup_ownership(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        claude_bin="echo",
        supported_engines="codex,claude",
    )
    reused = tmp_path / "worker" / "worktrees" / "jarvis-existing"
    reused.mkdir(parents=True)

    async def calls(base, c):  # noqa: ANN001
        disp = (
            await c.post(
                base + "/run",
                json={
                    "action": "code",
                    "args": {
                        "prompt": "follow up",
                        "agent": "claude",
                        "session_id": "550e8400-e29b-41d4-a716-446655440000",
                        "resume_session": True,
                        "cwd": str(reused),
                        "branch": "jarvis/existing",
                    },
                },
            )
        ).json()
        jid = disp["job_id"]
        job = {}
        for _ in range(100):
            job = (await c.get(f"{base}/jobs/{jid}")).json()
            if job["status"] != "running":
                break
            await asyncio.sleep(0.02)
        cleaned = (await c.post(base + "/run", json={"action": "cleanup", "args": {"job": jid}})).json()
        return disp, job, cleaned

    disp, job, cleaned = asyncio.run(_with_server(cfg, 8823, calls))

    assert disp["cwd"] == str(reused)
    assert disp["branch"] == "jarvis/existing"
    assert job["cwd"] == str(reused)
    assert job["cleanup_owned"] is False
    assert cleaned["cleaned"] == [job["name"]]
    assert reused.exists()


def test_daemon_resume_cwd_requires_session_id(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        claude_bin="echo",
        supported_engines="codex,claude",
    )
    reused = tmp_path / "worker" / "worktrees" / "jarvis-existing"
    reused.mkdir(parents=True)

    async def calls(base, c):  # noqa: ANN001
        return await c.post(
            base + "/run",
            json={
                "action": "code",
                "args": {
                    "prompt": "follow up",
                    "agent": "claude",
                    "resume_session": True,
                    "cwd": str(reused),
                },
            },
        )

    response = asyncio.run(_with_server(cfg, 8827, calls))

    assert response.status_code == 400
    assert "resume cwd requires session_id" in response.json()["error"]


def test_daemon_prune_keeps_workspace_used_by_running_resume(tmp_path) -> None:
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty", "-m", "init"], cwd=repo, check=True)
    slow_agent = tmp_path / "slow-agent"
    slow_agent.write_text("#!/usr/bin/env python3\nimport time\nprint('started')\ntime.sleep(2)\nprint('done')\n")
    slow_agent.chmod(0o755)
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        codex_bin="echo",
        claude_bin=str(slow_agent),
        supported_engines="codex,claude",
    )

    async def calls(base, c):  # noqa: ANN001
        original = (
            await c.post(
                base + "/run",
                json={"action": "code", "args": {"prompt": "first", "agent": "codex", "repo": str(repo)}},
            )
        ).json()
        original_job = {}
        for _ in range(100):
            original_job = (await c.get(f"{base}/jobs/{original['job_id']}")).json()
            if original_job["status"] != "running":
                break
            await asyncio.sleep(0.02)
        resumed = (
            await c.post(
                base + "/run",
                json={
                    "action": "code",
                    "args": {
                        "prompt": "follow up",
                        "agent": "claude",
                        "session_id": "550e8400-e29b-41d4-a716-446655440000",
                        "resume_session": True,
                        "cwd": original_job["cwd"],
                    },
                },
            )
        ).json()
        cleaned = (await c.post(base + "/run", json={"action": "cleanup", "args": {"job": "all"}})).json()
        listed = (await c.get(base + "/jobs")).json()["jobs"]
        for _ in range(100):
            resumed_job = (await c.get(f"{base}/jobs/{resumed['job_id']}")).json()
            if resumed_job["status"] != "running":
                break
            await asyncio.sleep(0.03)
        return original_job, resumed, cleaned, listed

    original_job, resumed, cleaned, listed = asyncio.run(_with_server(cfg, 8825, calls))

    assert resumed["ok"]
    assert cleaned["cleaned"] == []
    assert any(job["id"] == original_job["id"] for job in listed)
    assert any(job["id"] == resumed["job_id"] and job["status"] == "running" for job in listed)
    assert pathlib.Path(original_job["cwd"]).exists()


def test_daemon_resume_rejects_cwd_outside_worker_workspace(tmp_path) -> None:
    cfg = WorkerConfig(
        _env_file=None,
        token="",
        workspace=str(tmp_path / "worker"),
        claude_bin="echo",
        supported_engines="codex,claude",
    )
    outside = tmp_path / "outside"
    outside.mkdir()

    async def calls(base, c):  # noqa: ANN001
        return await c.post(
            base + "/run",
            json={
                "action": "code",
                "args": {
                    "prompt": "follow up",
                    "agent": "claude",
                    "session_id": "550e8400-e29b-41d4-a716-446655440000",
                    "resume_session": True,
                    "cwd": str(outside),
                },
            },
        )

    response = asyncio.run(_with_server(cfg, 8824, calls))

    assert response.status_code == 400
    assert "refusing to resume outside worker-owned workspace" in response.json()["error"]


def test_daemon_rejects_unsupported_code_engine(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        return await c.post(base + "/run", json={"action": "code", "args": {"prompt": "hello", "agent": "claude"}})

    response = asyncio.run(_with_server(cfg, 8819, calls))

    assert response.status_code == 400
    assert "does not support engine 'claude'" in response.json()["error"]


def test_daemon_refuses_worker_workspace_inside_git_checkout(tmp_path) -> None:
    import subprocess

    repo = tmp_path / "repo"
    workspace = repo / "jarvis-workspace" / "worker"
    workspace.mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(workspace))

    with pytest.raises(ValueError, match="inside a git checkout"):
        make_app(cfg)


def test_daemon_rejects_orchestration_code_job_without_start_action(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        return await c.post(
            base + "/run",
            json={
                "action": "code",
                "args": {
                    "prompt": "hello-job",
                    "execution_envelope": {
                        "run_id": "run_1",
                        "allowed_actions": [],
                        "landing": {"mode": "draft_pr", "allow_merge": False},
                    },
                },
            },
        )

    response = asyncio.run(_with_server(cfg, 8814, calls))
    assert response.status_code == 403
    assert "worker.job.start" in response.json()["error"]


def test_no_repo_jobs_get_isolated_run_dirs(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        r1 = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "a", "name": "job one"}})).json()
        r2 = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "b", "name": "job two"}})).json()
        await asyncio.sleep(0.3)
        j1 = (await c.get(f"{base}/jobs/{r1['job_id']}")).json()
        j2 = (await c.get(f"{base}/jobs/{r2['job_id']}")).json()
        return j1, j2

    j1, j2 = asyncio.run(_with_server(cfg, 8810, calls))
    assert j1["cwd"] != j2["cwd"]  # isolated per job
    assert "/runs/" in j1["cwd"]
    assert "job-one" in j1["cwd"]


def test_repo_job_isolates_on_a_worktree_branch(tmp_path) -> None:
    import pathlib
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run([*git, "init", "-q"], cwd=repo, check=True)
    subprocess.run([*git, "commit", "--allow-empty", "-qm", "init"], cwd=repo, check=True)
    (repo / "original.txt").write_text("untouched")

    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "ws"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        r = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "do x", "name": "refactor", "repo": str(repo)}})).json()
        await asyncio.sleep(0.3)
        j = (await c.get(f"{base}/jobs/{r['job_id']}")).json()
        return r, j

    r, j = asyncio.run(_with_server(cfg, 8812, calls))
    assert r["branch"].startswith("jarvis/refactor-")
    assert "/worktrees/" in j["cwd"] and pathlib.Path(j["cwd"]).exists()
    assert j["cwd"] != str(repo)  # NOT the user's checkout
    assert (repo / "original.txt").read_text() == "untouched"  # checkout untouched
    branches = subprocess.run(["git", "-C", str(repo), "branch", "--list", r["branch"]], capture_output=True, text=True).stdout
    assert r["branch"] in branches


def test_non_git_repo_input_is_copied_to_worker_scratch(tmp_path) -> None:
    import pathlib

    user_dir = tmp_path / "plain-input"
    user_dir.mkdir()
    (user_dir / "note.txt").write_text("original")
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "ws"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        r = (
            await c.post(
                base + "/run",
                json={"action": "code", "args": {"prompt": "x", "name": "plain", "repo": str(user_dir)}},
            )
        ).json()
        await asyncio.sleep(0.3)
        j = (await c.get(f"{base}/jobs/{r['job_id']}")).json()
        existed_before_cleanup = pathlib.Path(j["cwd"]).exists()
        clean = (await c.post(base + "/run", json={"action": "cleanup", "args": {"job": "plain"}})).json()
        return r, j, existed_before_cleanup, clean

    r, j, existed_before_cleanup, clean = asyncio.run(_with_server(cfg, 8813, calls))
    assert r["branch"] is None
    assert j["cwd"] != str(user_dir)
    assert "/worktrees/" in j["cwd"] and j["cwd"].endswith("-scratch")
    assert existed_before_cleanup
    assert (user_dir / "note.txt").read_text() == "original"
    assert "plain" in clean["cleaned"]
    assert not pathlib.Path(j["cwd"]).exists()
    assert user_dir.exists()


def test_cleanup_removes_worktree_and_branch(tmp_path) -> None:
    import pathlib
    import subprocess

    repo = tmp_path / "repo"
    repo.mkdir()
    git = ["git", "-c", "user.email=t@t", "-c", "user.name=t"]
    subprocess.run([*git, "init", "-q"], cwd=repo, check=True)
    subprocess.run([*git, "commit", "--allow-empty", "-qm", "init"], cwd=repo, check=True)
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "ws"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        r = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "x", "name": "cleanme", "repo": str(repo)}})).json()
        await asyncio.sleep(0.3)
        wt = (await c.get(f"{base}/jobs/{r['job_id']}")).json()["cwd"]
        clean = (await c.post(base + "/run", json={"action": "cleanup", "args": {"job": "cleanme"}})).json()
        after = (await c.get(base + "/jobs")).json()["jobs"]
        return r["branch"], wt, clean, after

    branch, wt, clean, after = asyncio.run(_with_server(cfg, 8814, calls))
    assert "cleanme" in clean["cleaned"]
    assert not pathlib.Path(wt).exists()  # worktree removed
    branches = subprocess.run(["git", "-C", str(repo), "branch", "--list", branch], capture_output=True, text=True).stdout
    assert branch not in branches  # branch deleted
    assert after == []  # job dropped from the list


def test_unknown_repo_returns_helpful_error(tmp_path) -> None:
    import subprocess

    dev = tmp_path / "dev"
    (dev / "realrepo").mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=dev / "realrepo", check=True)
    cfg = WorkerConfig(
        _env_file=None, token="", workspace=str(tmp_path / "ws"),
        repo_root=str(dev), clone_missing=False, codex_bin="echo",
    )

    async def calls(base, c):  # noqa: ANN001
        r = await c.post(base + "/run", json={"action": "code", "args": {"prompt": "x", "repo": "missing"}})
        return r.status_code, r.json()

    status, data = asyncio.run(_with_server(cfg, 8817, calls))
    assert status == 404
    assert "couldn't find" in data["error"]
    assert "realrepo" in data["error"]  # the error lists the repos it can see


def test_list_repos_action(tmp_path) -> None:
    import subprocess

    dev = tmp_path / "dev"
    (dev / "alpha").mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=dev / "alpha", check=True)
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "ws"), repo_root=str(dev))

    async def calls(base, c):  # noqa: ANN001
        return (await c.post(base + "/run", json={"action": "list_repos"})).json()

    data = asyncio.run(_with_server(cfg, 8818, calls))
    assert data["repos"] == ["alpha"]


def test_cleanup_all_removes_scratch_dir(tmp_path) -> None:
    import pathlib

    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "ws"), codex_bin="echo")

    async def calls(base, c):  # noqa: ANN001
        r = (await c.post(base + "/run", json={"action": "code", "args": {"prompt": "x", "name": "scratchy"}})).json()
        await asyncio.sleep(0.3)
        cwd = (await c.get(f"{base}/jobs/{r['job_id']}")).json()["cwd"]
        clean = (await c.post(base + "/run", json={"action": "cleanup", "args": {"job": ""}})).json()
        return cwd, clean

    cwd, clean = asyncio.run(_with_server(cfg, 8815, calls))
    assert "scratchy" in clean["cleaned"]
    assert not pathlib.Path(cwd).exists()
