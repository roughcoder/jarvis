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
import time

import httpx
import pytest

pytest.importorskip("aiohttp")
from aiohttp import web  # noqa: E402

from jarvis.capabilities import (  # noqa: E402
    FORGE_BRANCH_PUSH,
    FORGE_PR_CREATE,
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_CREATE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_RESTORE,
    WORKER_SESSION_STOP,
    WORKER_SESSION_TURN,
)
from jarvis.config import WorkerConfig  # noqa: E402
from jarvis.worker.server import make_app  # noqa: E402
from jarvis.worker.authority import WorkerSessionAuthority  # noqa: E402
from jarvis.worker.providers.codex import (  # noqa: E402
    _deliver_pending_request,
    _approval_result,
    _input_result,
    _project_jsonrpc_message,
    _read_until_turn_done,
    _restore_running_if_waiting,
    _run_codex_turn,
    _session_cwd as codex_session_cwd,
    _track_pending_request,
    _terminate_provider_process as codex_terminate_provider_process,
)
from jarvis.worker.providers import claude  # noqa: E402
from jarvis.worker.providers.claude import _claude_session_id  # noqa: E402
from jarvis.worker.providers.base import ProviderTurn  # noqa: E402
from jarvis.worker.sessions import WorkerSession  # noqa: E402
from jarvis.worker.sessions import SessionManager  # noqa: E402
from jarvis.worker_session_contract import (  # noqa: E402
    EVENT_APPROVAL_RESOLVED,
    EVENT_TURN_COMPLETED,
    REQUEST_KIND_APPROVAL,
    SESSION_COMPLETED,
    SESSION_RUNNING,
    SESSION_STOPPED,
    SESSION_WAITING_APPROVAL,
    SESSION_WAITING_INPUT,
)


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


def _control_metadata(action: str, **extra: object) -> dict:
    metadata = {"allowed_actions": [action]}
    metadata.update(extra)
    return metadata


def _owned_worker_cwd(tmp_path, name: str = "session") -> str:  # noqa: ANN001
    cwd = tmp_path / "worker" / "worktrees" / name
    cwd.mkdir(parents=True, exist_ok=True)
    return str(cwd)


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


class _FakeClaudeOptions:
    def __init__(self, **kwargs):  # noqa: ANN003
        self.kwargs = dict(kwargs)
        for key, value in kwargs.items():
            setattr(self, key, value)


class _FakePermissionResultAllow:
    def __init__(self, updated_input=None, updated_permissions=None):  # noqa: ANN001
        self.behavior = "allow"
        self.updated_input = updated_input
        self.updated_permissions = updated_permissions


class _FakePermissionResultDeny:
    def __init__(self, message: str = "", interrupt: bool = False) -> None:
        self.behavior = "deny"
        self.message = message
        self.interrupt = interrupt


class _FakeTextBlock:
    def __init__(self, text: str) -> None:
        self.text = text


class _FakeToolUseBlock:
    def __init__(self, id: str, name: str, input: dict):  # noqa: A002
        self.id = id
        self.name = name
        self.input = input


class _FakeToolResultBlock:
    def __init__(self, tool_use_id: str, content=None, is_error: bool | None = None):  # noqa: ANN001
        self.tool_use_id = tool_use_id
        self.content = content
        self.is_error = is_error


class _FakeAssistantMessage:
    def __init__(self, content: list, model: str = "claude-test", session_id: str = "11111111-1111-4111-8111-111111111111") -> None:
        self.content = content
        self.model = model
        self.session_id = session_id


class _FakeUserMessage:
    def __init__(self, content: list) -> None:
        self.content = content


class _FakeSystemMessage:
    def __init__(self, subtype: str, data: dict) -> None:
        self.subtype = subtype
        self.data = data


class _FakeResultMessage:
    def __init__(self, *, subtype: str = "success", is_error: bool = False, session_id: str = "11111111-1111-4111-8111-111111111111") -> None:
        self.subtype = subtype
        self.is_error = is_error
        self.session_id = session_id


class _FakePermissionAsk:
    def __init__(self, tool_name: str, tool_input: dict, request_id: str = "request_1") -> None:
        self.tool_name = tool_name
        self.tool_input = tool_input
        self.request_id = request_id


class _FakeClaudeContext:
    def __init__(self, tool_use_id: str) -> None:
        self.tool_use_id = tool_use_id
        self.title = "Claude wants to use a tool"
        self.display_name = "Tool"
        self.description = "Fake permission request"
        self.decision_reason = "test"
        self.blocked_path = None


class _FakeClaudeClient:
    response_batches: list[list[list[object]]] = []
    instances: list["_FakeClaudeClient"] = []
    connect_error: Exception | None = None

    def __init__(self, options):  # noqa: ANN001
        self.options = options
        self.connected = False
        self.disconnected = False
        self.interrupted = False
        self.response_exhausted = 0
        self.permission_modes: list[str] = []
        self.queries: list[str] = []
        self.permission_results: list[object] = []
        self._batches = _FakeClaudeClient.response_batches.pop(0)
        self._active: list[object] = []
        _FakeClaudeClient.instances.append(self)

    async def connect(self, prompt=None) -> None:  # noqa: ANN001
        if _FakeClaudeClient.connect_error is not None:
            raise _FakeClaudeClient.connect_error
        self.connected = True
        if prompt is not None:
            async for _ in prompt:
                break

    async def disconnect(self) -> None:
        self.disconnected = True

    async def set_permission_mode(self, mode: str) -> None:
        self.permission_modes.append(mode)

    async def query(self, prompt: str) -> None:
        self.queries.append(prompt)
        self._active = self._batches[len(self.queries) - 1]

    async def interrupt(self) -> None:
        self.interrupted = True

    async def receive_response(self):
        for item in self._active:
            if isinstance(item, _FakePermissionAsk):
                result = await self.options.can_use_tool(
                    item.tool_name,
                    item.tool_input,
                    _FakeClaudeContext(item.request_id),
                )
                self.permission_results.append(result)
                continue
            yield item
        self.response_exhausted += 1


def _fake_claude_sdk() -> object:
    return type(
        "FakeClaudeSDK",
        (),
        {
            "AssistantMessage": _FakeAssistantMessage,
            "ClaudeAgentOptions": _FakeClaudeOptions,
            "ClaudeSDKClient": _FakeClaudeClient,
            "PermissionResultAllow": _FakePermissionResultAllow,
            "PermissionResultDeny": _FakePermissionResultDeny,
            "ResultMessage": _FakeResultMessage,
            "ServerToolResultBlock": _FakeToolResultBlock,
            "ServerToolUseBlock": _FakeToolUseBlock,
            "SystemMessage": _FakeSystemMessage,
            "TextBlock": _FakeTextBlock,
            "ToolResultBlock": _FakeToolResultBlock,
            "ToolUseBlock": _FakeToolUseBlock,
            "UserMessage": _FakeUserMessage,
        },
    )()


def _install_fake_claude_sdk(monkeypatch, batches: list[list[list[object]]]) -> None:  # noqa: ANN001
    _FakeClaudeClient.response_batches = batches
    _FakeClaudeClient.instances = []
    _FakeClaudeClient.connect_error = None
    monkeypatch.setattr(claude, "_SDK", _fake_claude_sdk())
    with claude._RUNTIME_LOCK:
        for runtime in list(claude._RUNTIMES.values()):
            runtime.stop()
        claude._RUNTIMES.clear()


def _stop_fake_claude_runtimes() -> None:
    with claude._RUNTIME_LOCK:
        runtimes = list(claude._RUNTIMES.values())
    for runtime in runtimes:
        runtime.stop()
    with claude._RUNTIME_LOCK:
        claude._RUNTIMES.clear()


def test_worker_session_authority_maps_read_only_to_codex_read_only() -> None:
    authority = WorkerSessionAuthority.from_session(_session_with_authority())

    assert authority.codex_sandbox == "read-only"
    assert authority.codex_approval_policy == "never"
    assert authority.claude_permission_mode == "plan"
    assert authority.claude_tool_denial("Bash")
    assert authority.claude_tool_denial("mcp__github__create_issue")
    assert authority.claude_tool_denial("FutureClaudeTool")
    assert authority.claude_tool_denial("")
    assert authority.claude_tool_denial("Read") == ""
    assert authority.claude_tool_denial("Grep") == ""
    assert authority.claude_tool_denial("AskUserQuestion") == ""


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


def test_worker_session_authority_keeps_input_separate_from_codex_approval() -> None:
    authority = WorkerSessionAuthority.from_session(
        _session_with_authority(
            allowed_actions=[WORKER_SESSION_TURN, WORKER_SESSION_INPUT, FORGE_BRANCH_PUSH],
            landing={"mode": "branch_only", "allow_merge": False},
        )
    )

    assert authority.codex_approval_policy == "never"
    assert authority.can_receive_input is True
    assert authority.can_resolve_approval is False


def test_worker_session_authority_requires_envelope_for_real_providers() -> None:
    metadata = {
        "allowed_actions": [WORKER_SESSION_CREATE, WORKER_SESSION_TURN, FORGE_BRANCH_PUSH],
        "landing": {"mode": "branch_only", "allow_merge": False},
    }

    with pytest.raises(RuntimeError, match="execution_envelope is required"):
        WorkerSessionAuthority.from_metadata(metadata, provider="codex")


def test_worker_session_authority_does_not_override_envelope_denial() -> None:
    metadata = {
        "execution_envelope": {
            "allowed_actions": [],
            "landing": {"mode": "branch_only", "allow_merge": False},
        },
        "allowed_actions": [WORKER_SESSION_CREATE, WORKER_SESSION_TURN, FORGE_BRANCH_PUSH],
        "landing": {"mode": "branch_only", "allow_merge": False},
    }

    with pytest.raises(RuntimeError, match=FORGE_BRANCH_PUSH):
        WorkerSessionAuthority.from_metadata(metadata, provider="codex")


def test_codex_protocol_response_shapes_match_app_server_contract() -> None:
    assert _approval_result({"decision": "approved"}) == {"decision": "accept"}
    assert _approval_result({"decision": "denied"}) == {"decision": "decline"}
    assert _approval_result({"decision": "acceptForSession"}) == {"decision": "acceptForSession"}

    assert _input_result(
        {"text": "continue"},
        {"questions": [{"id": "details"}]},
    ) == {"answers": {"details": {"answers": ["continue"]}}}
    assert _input_result(
        {"answers": {"details": "use pytest"}},
        {"questions": [{"id": "details"}]},
    ) == {"answers": {"details": {"answers": ["use pytest"]}}}


def test_codex_server_request_resolved_clears_pending_request(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "codex",
            "engine": "codex",
            "metadata": _authority_metadata("codex", extra_actions=[WORKER_SESSION_APPROVE]),
        }
    )
    process = object()
    turn = ProviderTurn(turn_id="turn_1", prompt="x", idempotency_key="idem_1")
    _track_pending_request(
        session.session_id,
        "approval_rpc",
        kind=REQUEST_KIND_APPROVAL,
        process=process,  # type: ignore[arg-type]
        rpc_id="approval_rpc",
        params={"command": "pytest"},
    )
    sessions.update_status(session.session_id, SESSION_WAITING_APPROVAL)

    done = _project_jsonrpc_message(
        process,  # type: ignore[arg-type]
        {"jsonrpc": "2.0", "method": "serverRequest/resolved", "params": {"requestId": "approval_rpc"}},
        session_id=session.session_id,
        turn=turn,
        sessions=sessions,
    )

    assert done is False
    assert sessions.pending_requests(session.session_id) == []
    assert sessions.get(session.session_id).status == SESSION_RUNNING  # type: ignore[union-attr]
    resolved = [event for event in sessions.events(session.session_id) if event.type == EVENT_APPROVAL_RESOLVED]
    assert resolved[-1].data["request_id"] == "approval_rpc"
    assert resolved[-1].data["provider_resolved"] is True


def test_codex_restore_running_does_not_revive_terminal_turn(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "codex",
            "engine": "codex",
            "metadata": _authority_metadata("codex", extra_actions=[WORKER_SESSION_INPUT]),
        }
    )
    sessions.append_event_with_status(session.session_id, SESSION_COMPLETED, EVENT_TURN_COMPLETED)
    sessions.update_status(session.session_id, SESSION_WAITING_INPUT)

    _restore_running_if_waiting(sessions, session.session_id, SESSION_WAITING_INPUT)

    assert sessions.get(session.session_id).status == SESSION_COMPLETED  # type: ignore[union-attr]


def test_codex_turn_rechecks_cancellation_before_launch(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    def fail_popen(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("cancelled sessions must not launch codex")

    monkeypatch.setattr("jarvis.worker.providers.codex.subprocess.Popen", fail_popen)
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "codex",
            "engine": "codex",
            "cwd": _owned_worker_cwd(tmp_path, "cancel-before-launch"),
            "metadata": _authority_metadata("codex"),
        }
    )
    sessions.update_status(session.session_id, SESSION_STOPPED)
    authority = WorkerSessionAuthority.from_session(session, provider="codex")

    _run_codex_turn(
        session.session_id,
        ProviderTurn(turn_id="turn_cancelled", prompt="x"),
        sessions,
        WorkerConfig(_env_file=None, workspace=str(tmp_path / "worker")),
        authority,
    )

    assert "provider.process.started" not in [event.type for event in sessions.events(session.session_id)]


def test_codex_turn_rechecks_cancellation_after_process_tracking(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.worker.providers import codex

    class Process:
        pid = 12345
        stdin = None
        stdout = None
        stderr = None
        terminated = False

        def poll(self):
            return -15 if self.terminated else None

        def terminate(self) -> None:
            self.terminated = True

        def wait(self, timeout=None):  # noqa: ANN001, ANN202
            self.terminated = True
            return -15

    process = Process()
    monkeypatch.setattr("jarvis.worker.providers.codex.subprocess.Popen", lambda *_args, **_kwargs: process)
    original_track = codex._track_provider_process

    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "codex",
            "engine": "codex",
            "cwd": _owned_worker_cwd(tmp_path, "cancel-after-track"),
            "metadata": _authority_metadata("codex"),
        }
    )

    def cancel_after_tracking(session_id, tracked_process):  # noqa: ANN001
        original_track(session_id, tracked_process)
        sessions.update_status(session_id, SESSION_STOPPED)

    monkeypatch.setattr(codex, "_track_provider_process", cancel_after_tracking)
    authority = WorkerSessionAuthority.from_session(session, provider="codex")

    _run_codex_turn(
        session.session_id,
        ProviderTurn(turn_id="turn_cancelled", prompt="x"),
        sessions,
        WorkerConfig(_env_file=None, workspace=str(tmp_path / "worker")),
        authority,
    )

    assert process.terminated is True
    assert "provider.process.started" not in [event.type for event in sessions.events(session.session_id)]


def test_claude_session_id_ignores_caller_metadata_session_id() -> None:
    session = WorkerSession(
        session_id="sess_untrusted",
        provider="claude",
        engine="claude",
        metadata={"session_id": "caller-native-session"},
    )

    assert _claude_session_id(session) != "caller-native-session"


def test_claude_interrupt_ignores_caller_supplied_provider_pid(monkeypatch) -> None:  # noqa: ANN001
    def fail_if_called(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("provider interrupt must use tracked runtime state")

    monkeypatch.setattr(claude, "_runtime_for_existing_session", lambda _session_id: None)
    monkeypatch.setattr("os.kill", fail_if_called)
    claude._terminate_provider_process(
        WorkerSession(
            session_id="sess_untrusted_pid",
            provider="claude",
            engine="claude",
            metadata={"provider_pid": "1"},
        )
    )


def test_claude_start_turn_rejects_missing_sdk(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):  # noqa: ANN001
        if name == "claude_agent_sdk":
            raise ModuleNotFoundError("No module named 'claude_agent_sdk'", name="claude_agent_sdk")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(claude, "_SDK", None)
    monkeypatch.setattr(builtins, "__import__", fake_import)
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "claude",
            "engine": "claude",
            "cwd": _owned_worker_cwd(tmp_path, "claude-missing-sdk"),
            "metadata": _authority_metadata("claude"),
        }
    )

    with pytest.raises(RuntimeError, match=r"install jarvis\[worker-claude\]"):
        claude.ClaudeProviderAdapter().start_turn(
            session=session,
            turn=ProviderTurn(turn_id="turn_missing_sdk", prompt="x"),
            sessions=sessions,
            worker_cfg=WorkerConfig(_env_file=None, workspace=str(tmp_path / "worker")),
        )


def test_codex_stop_ignores_caller_supplied_provider_pid(monkeypatch) -> None:  # noqa: ANN001
    def fail_if_called(*_args, **_kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("provider stop must not trust metadata provider_pid")

    monkeypatch.setattr("os.kill", fail_if_called)
    codex_terminate_provider_process(
        WorkerSession(
            session_id="sess_untrusted_pid",
            provider="codex",
            engine="codex",
            metadata={"provider_pid": "1"},
        )
    )


def test_worker_session_authority_fails_closed_for_unsupported_landing() -> None:
    with pytest.raises(RuntimeError, match=FORGE_PR_CREATE):
        WorkerSessionAuthority.from_session(
            _session_with_authority(
                allowed_actions=[WORKER_SESSION_TURN, FORGE_BRANCH_PUSH],
                landing={"mode": "draft_pr", "allow_merge": False},
            )
        )

    claude_authority = WorkerSessionAuthority.from_session(_session_with_authority(provider="claude"), provider="claude")
    assert claude_authority.claude_permission_mode == "plan"


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


def test_session_manager_strips_provider_owned_metadata_on_create(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))

    session, _ = sessions.create(
        {
            "provider": "codex",
            "engine": "codex",
            "metadata": {
                **_authority_metadata("codex"),
                "provider_pid": "1",
                "codex_thread_id": "thread_123",
                "provider_session_id": "provider_123",
            },
        }
    )

    assert "provider_pid" not in session.metadata
    assert "codex_thread_id" not in session.metadata
    assert "provider_session_id" not in session.metadata
    assert "execution_envelope" in session.metadata


def test_session_manager_hides_pending_requests_for_terminal_sessions(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create({"provider": "fake", "engine": "fake", "metadata": _authority_metadata("fake")})
    sessions.append_event(session.session_id, "approval.requested", {"request_id": "approval_1"})

    assert sessions.pending_requests(session.session_id)[0]["request_id"] == "approval_1"

    sessions.update_status(session.session_id, "stopped")

    assert sessions.pending_requests(session.session_id) == []
    assert sessions.pending_requests() == []


def test_session_manager_rejects_duplicate_session_id_without_overwriting(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    original, _ = sessions.create(
        {
            "session_id": "sess_duplicate",
            "provider": "codex",
            "engine": "codex",
            "cwd": "/worker/worktrees/original",
            "metadata": {"allowed_actions": ["worker.session.turn"]},
        }
    )

    with pytest.raises(ValueError, match="already exists"):
        sessions.create(
            {
                "session_id": original.session_id,
                "provider": "claude",
                "engine": "claude",
                "cwd": "/worker/worktrees/replaced",
                "metadata": {"allowed_actions": ["worker.session.stop"]},
            }
        )

    fetched = sessions.get(original.session_id)
    assert fetched is not None
    assert fetched.provider == "codex"
    assert fetched.engine == "codex"
    assert fetched.cwd == "/worker/worktrees/original"
    assert fetched.metadata == {"allowed_actions": ["worker.session.turn"]}
    assert [event.type for event in sessions.events(original.session_id)] == ["session.created"]


def test_session_manager_lists_past_corrupt_or_legacy_session_json(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    good, _ = sessions.create({"provider": "fake", "engine": "fake"})
    bad = sessions.root / "bad" / "session.json"
    bad.parent.mkdir(parents=True)
    bad.write_text('{"session_id": "../bad"}')

    listed = sessions.list()

    assert [session.session_id for session in listed] == [good.session_id]


def test_session_manager_reserves_idempotent_turn_once_under_concurrency(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create({"provider": "fake", "engine": "fake"})
    results = []

    def reserve() -> None:
        results.append(
            sessions.reserve_turn(
                session.session_id,
                {
                    "turn_id": "turn_shared",
                    "prompt": "same",
                    "idempotency_key": "idem_shared",
                },
            )
        )

    threads = [threading.Thread(target=reserve), threading.Thread(target=reserve)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(created for _session, _event, created in results) == [False, True]
    assert [event.type for event in sessions.events(session.session_id)].count("turn.started") == 1


def test_session_manager_rejects_overlapping_active_turns(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create({"provider": "fake", "engine": "fake"})

    first_session, first_event, created = sessions.reserve_turn(
        session.session_id,
        {"turn_id": "turn_one", "prompt": "first", "idempotency_key": "idem_one"},
    )

    assert created is True
    assert first_session.status == "running"
    same_session, same_event, same_created = sessions.reserve_turn(
        session.session_id,
        {"turn_id": "turn_one", "prompt": "first", "idempotency_key": "idem_one"},
    )
    assert same_session.status == "running"
    assert same_event.event_id == first_event.event_id
    assert same_created is False
    with pytest.raises(RuntimeError, match="active turn"):
        sessions.reserve_turn(
            session.session_id,
            {"turn_id": "turn_two", "prompt": "second", "idempotency_key": "idem_two"},
        )


def test_session_manager_rejects_turns_for_terminal_sessions(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    for status in ("stopped", "failed", "interrupted", "blocked"):
        session, _ = sessions.create({"session_id": f"sess_{status}", "provider": "fake", "engine": "fake"})
        sessions.update_status(session.session_id, status)

        with pytest.raises(RuntimeError, match="does not accept new turns"):
            sessions.reserve_turn(session.session_id, {"turn_id": f"turn_{status}"})

        assert [event.type for event in sessions.events(session.session_id)] == ["session.created"]


def test_session_manager_requires_resume_metadata_for_completed_session_turns(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create({"provider": "fake", "engine": "fake"})
    sessions.update_status(session.session_id, "completed")

    with pytest.raises(RuntimeError, match="does not accept new turns"):
        sessions.reserve_turn(session.session_id, {"turn_id": "turn_without_resume"})

    resumed, event, created = sessions.reserve_turn(
        session.session_id,
        {"turn_id": "turn_resume", "metadata": {"resume_session": True}},
    )

    assert resumed.status == "running"
    assert event.type == "turn.started"
    assert created is True


def test_session_manager_interrupts_stale_active_sessions_on_startup(tmp_path) -> None:
    root = tmp_path / "sessions"
    sessions = SessionManager(str(root))
    session, _ = sessions.create({"provider": "codex", "engine": "codex"})
    sessions.update_status(session.session_id, "waiting_input")

    reloaded = SessionManager(str(root))

    stale = reloaded.get(session.session_id)
    assert stale is not None
    assert stale.status == "interrupted"
    events = reloaded.events(session.session_id)
    assert events[-1].type == "session.interrupted"
    assert events[-1].data["previous_status"] == "waiting_input"


def test_codex_provider_revalidates_worker_owned_cwd(tmp_path) -> None:
    session = WorkerSession(
        session_id="sess_bad_cwd",
        provider="codex",
        engine="codex",
        cwd=str(tmp_path),
        metadata=_authority_metadata("codex"),
    )
    cfg = WorkerConfig(_env_file=None, workspace=str(tmp_path / "worker"))

    with pytest.raises(RuntimeError, match="worker-owned"):
        codex_session_cwd(session, cfg)


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
    assert "system" not in health
    assert noauth == 401  # missing token
    assert bad == 401  # wrong token
    assert shell["output"] == "worker-ok"
    assert unknown == 400  # unknown action


def test_daemon_health_includes_best_effort_system_block(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        "jarvis.worker.server.system_info_cached",
        lambda: {
            "hostname": "neil-laptop",
            "platform": "darwin",
            "arch": "arm64",
            "os_name": "macOS",
            "os_version": "15.5",
            "kernel_version": "24.5.0",
            "cpu_model": "Apple M4 Pro",
            "cpu_cores_physical": 12,
            "cpu_cores_logical": 12,
            "memory_total_bytes": 51539607552,
            "memory_available_bytes": 21474836480,
            "memory_used_bytes": 30064771072,
            "memory_used_percent": 58.3,
            "load_average": [2.12, 2.44, 2.19],
            "uptime_seconds": 384220,
            "disk": [
                {
                    "mount": "/",
                    "filesystem": "apfs",
                    "total_bytes": 994662584320,
                    "available_bytes": 420118257664,
                    "used_bytes": 574544326656,
                    "used_percent": 57.8,
                }
            ],
            "gpu": [{"name": "Apple M4 Pro", "memory_total_bytes": None}],
            "checked_at": "2026-07-02T23:35:00Z",
        },
    )
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"))

    async def calls(base, c):  # noqa: ANN001
        return (await c.get(base + "/health")).json()

    health = asyncio.run(_with_server(cfg, 8818, calls))

    assert health["system"]["hostname"] == "neil-laptop"
    assert health["system"]["disk"][0]["filesystem"] == "apfs"
    assert health["system"]["gpu"][0]["name"] == "Apple M4 Pro"
    assert health["system"]["checked_at"] == "2026-07-02T23:35:00Z"


def test_daemon_health_returns_system_block_only_for_authorized_callers(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        "jarvis.worker.server.system_info_cached",
        lambda: {"hostname": "worker-laptop", "checked_at": "2026-07-02T23:35:00Z"},
    )
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))

    async def calls(base, c):  # noqa: ANN001
        public_health = (await c.get(base + "/health")).json()
        private_health = (await c.get(base + "/health", headers={"Authorization": "Bearer tkn"})).json()
        return public_health, private_health

    public_health, private_health = asyncio.run(_with_server(cfg, 8819, calls))

    assert public_health["ok"] is True
    assert "system" not in public_health
    assert private_health["system"]["hostname"] == "worker-laptop"


def test_daemon_health_reports_diagnostics_error_without_500(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        "jarvis.worker.server.diagnostics",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("diagnostics failed")),
    )
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"))

    async def calls(base, c):  # noqa: ANN001
        first = await c.get(base + "/health")
        for _ in range(50):
            second = await c.get(base + "/health")
            body = second.json()
            if body.get("diagnostics", {}).get("error"):
                return first.status_code, first.json(), second.status_code, body
            await asyncio.sleep(0.01)
        return first.status_code, first.json(), second.status_code, second.json()

    first_status, first_health, second_status, second_health = asyncio.run(_with_server(cfg, 8848, calls))

    assert first_status == 200
    assert first_health["ok"] is True
    assert first_health["diagnostics"]["status"] == "refreshing"
    assert second_status == 200
    assert second_health["ok"] is True
    assert second_health["diagnostics"]["error"] == "diagnostics failed"
    assert second_health["repositories"] == []


def test_daemon_health_does_not_wait_for_slow_diagnostics(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    import time

    def slow_diagnostics(**_kwargs):  # noqa: ANN001
        time.sleep(0.25)
        return {"repositories": [{"repo": "jarvis", "status": "ready"}]}

    monkeypatch.setattr("jarvis.worker.server.diagnostics", slow_diagnostics)
    cfg = WorkerConfig(_env_file=None, token="", workspace=str(tmp_path / "worker"))

    async def calls(base, c):  # noqa: ANN001
        started = time.monotonic()
        response = await c.get(base + "/health")
        elapsed = time.monotonic() - started
        return elapsed, response.json()

    elapsed, health = asyncio.run(_with_server(cfg, 8849, calls))

    assert elapsed < 0.20
    assert health["diagnostics"] == {"status": "refreshing", "repositories": []}


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
                json={"prompt": "inspect the repo", "metadata": _control_metadata(WORKER_SESSION_TURN, surface="test")},
                headers=headers,
            )
        ).json()
        retry = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={
                    "prompt": "inspect the repo again",
                    "idempotency_key": "idem_fake",
                    "metadata": _control_metadata(WORKER_SESSION_TURN, resume_session=True),
                },
                headers=headers,
            )
        ).json()
        retry_again = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={
                    "prompt": "inspect the repo a third time",
                    "idempotency_key": "idem_fake",
                    "metadata": _control_metadata(WORKER_SESSION_TURN, resume_session=True),
                },
                headers=headers,
            )
        ).json()
        approval = (
            await c.post(
                f"{base}/sessions/{session_id}/approval",
                json={
                    "request_id": "approval_1",
                    "decision": "approved",
                    "metadata": _control_metadata(WORKER_SESSION_APPROVE),
                },
                headers=headers,
            )
        ).json()
        interrupted = (
            await c.post(
                f"{base}/sessions/{session_id}/interrupt",
                json={"metadata": _control_metadata(WORKER_SESSION_INTERRUPT)},
                headers=headers,
            )
        ).json()
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


def test_daemon_session_create_requires_authority_before_provisioning(tmp_path) -> None:
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
    allowed_actions = [WORKER_SESSION_TURN, FORGE_BRANCH_PUSH]
    metadata = {
        "execution_envelope": {
            "allowed_actions": allowed_actions,
            "landing": {"mode": "branch_only", "allow_merge": False},
        },
        "allowed_actions": allowed_actions,
        "landing": {"mode": "branch_only", "allow_merge": False},
    }

    async def calls(base, c):  # noqa: ANN001
        created = await c.post(
            base + "/sessions",
            json={
                "provider": "fake",
                "engine": "fake",
                "repo": "roughcoder/jarvis",
                "metadata": {**metadata, "provision_workspace": True},
            },
            headers=headers,
        )
        listed = (await c.get(f"{base}/sessions", headers=headers)).json()
        return created.status_code, created.json(), listed

    status_code, body, listed = asyncio.run(_with_server(cfg, 8839, calls))

    assert status_code == 400
    assert WORKER_SESSION_CREATE in body["error"]
    assert listed["sessions"] == []
    assert not (tmp_path / "worker" / "worktrees").exists()


def test_daemon_session_turn_requires_authority_before_event_append(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}
    allowed_actions = [WORKER_SESSION_CREATE, FORGE_BRANCH_PUSH]
    metadata = {
        "execution_envelope": {
            "allowed_actions": allowed_actions,
            "landing": {"mode": "branch_only", "allow_merge": False},
        },
        "allowed_actions": allowed_actions,
        "landing": {"mode": "branch_only", "allow_merge": False},
    }

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={"provider": "fake", "engine": "fake", "metadata": metadata},
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        turn = await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={"prompt": "should not append", "idempotency_key": "idem_missing_turn"},
            headers=headers,
        )
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return turn.status_code, turn.json(), events

    status_code, body, events = asyncio.run(_with_server(cfg, 8840, calls))

    assert status_code == 400
    assert WORKER_SESSION_TURN in body["error"]
    assert [event["type"] for event in events] == ["session.created"]


def test_daemon_session_turn_requires_current_request_authority(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={"provider": "fake", "engine": "fake", "metadata": _authority_metadata("fake")},
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        turn = await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={"prompt": "caller did not carry turn authority", "idempotency_key": "idem_missing_caller_turn"},
            headers=headers,
        )
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return turn.status_code, turn.json(), events

    status_code, body, events = asyncio.run(_with_server(cfg, 8844, calls))

    assert status_code == 400
    assert "caller control authority denied" in body["error"]
    assert WORKER_SESSION_TURN in body["error"]
    assert [event["type"] for event in events] == ["session.created"]


def test_daemon_session_create_rejects_caller_supplied_non_worker_cwd(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = await c.post(
            base + "/sessions",
            json={
                "provider": "fake",
                "engine": "fake",
                "cwd": str(tmp_path),
                "metadata": _authority_metadata("fake"),
            },
            headers=headers,
        )
        listed = (await c.get(f"{base}/sessions", headers=headers)).json()
        return created.status_code, created.json(), listed

    status_code, body, listed = asyncio.run(_with_server(cfg, 8841, calls))

    assert status_code == 400
    assert "worker-owned workspace" in body["error"]
    assert listed["sessions"] == []


def test_daemon_session_turn_rejects_missing_provider_cwd_before_event_append(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}
    cwd = pathlib.Path(_owned_worker_cwd(tmp_path, "codex-missing-cwd"))

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "provider": "codex",
                    "engine": "codex",
                    "cwd": str(cwd),
                    "metadata": _authority_metadata("codex"),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        cwd.rmdir()
        turn = await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={
                "prompt": "should fail before append",
                "idempotency_key": "idem_missing_cwd",
                "metadata": _control_metadata(WORKER_SESSION_TURN),
            },
            headers=headers,
        )
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return turn.status_code, turn.json(), events

    status_code, body, events = asyncio.run(_with_server(cfg, 8842, calls))

    assert status_code == 400
    assert "cwd does not exist" in body["error"]
    assert [event["type"] for event in events] == ["session.created"]


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


def test_daemon_session_controls_require_caller_authority(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "provider": "fake",
                    "engine": "fake",
                    "metadata": _authority_metadata("fake", extra_actions=[WORKER_SESSION_STOP]),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        stopped = await c.post(f"{base}/sessions/{session_id}/stop", json={}, headers=headers)
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return stopped.status_code, stopped.json(), events

    status_code, body, events = asyncio.run(_with_server(cfg, 8845, calls))

    assert status_code == 400
    assert "caller control authority denied" in body["error"]
    assert WORKER_SESSION_STOP in body["error"]
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


def test_daemon_duplicate_session_id_rejects_before_worktree_provisioning(tmp_path) -> None:
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
    metadata = {**_authority_metadata("fake"), "provision_workspace": True}

    async def calls(base, c):  # noqa: ANN001
        first = (
            await c.post(
                base + "/sessions",
                json={
                    "session_id": "sess_duplicate_repo",
                    "run_id": "run_repo",
                    "provider": "fake",
                    "engine": "fake",
                    "repo": "roughcoder/jarvis",
                    "title": "Fix worker sessions",
                    "metadata": metadata,
                },
                headers=headers,
            )
        ).json()
        duplicate = await c.post(
            base + "/sessions",
            json={
                "session_id": "sess_duplicate_repo",
                "run_id": "run_repo",
                "provider": "fake",
                "engine": "fake",
                "repo": "roughcoder/jarvis",
                "title": "Fix worker sessions duplicate",
                "metadata": metadata,
            },
            headers=headers,
        )
        return first, duplicate.status_code, duplicate.json()

    first, status_code, duplicate = asyncio.run(_with_server(cfg, 8846, calls))

    assert first["ok"] is True
    assert status_code == 400
    assert "already exists" in duplicate["error"]
    worktrees = [path for path in (tmp_path / "worker" / "worktrees").iterdir() if path.is_dir()]
    assert len(worktrees) == 1


def test_daemon_session_pending_requests_and_checkpoints_are_projected(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        metadata = _authority_metadata(
            "fake",
            extra_actions=[WORKER_SESSION_APPROVE, WORKER_SESSION_INPUT, WORKER_SESSION_RESTORE],
        )
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "provider": "fake",
                    "engine": "fake",
                    "title": "Pending request",
                    "metadata": metadata,
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        approval_turn = (
            await c.post(
                f"{base}/sessions/{session_id}/turns",
                json={
                    "turn_id": "turn_need_approval",
                    "prompt": "request approval",
                    "metadata": _control_metadata(WORKER_SESSION_TURN),
                },
                headers=headers,
            )
        ).json()
        pending_before = (await c.get(f"{base}/sessions/{session_id}/requests", headers=headers)).json()
        global_pending = (await c.get(f"{base}/sessions/requests", headers=headers)).json()
        denied = (
            await c.post(
                f"{base}/sessions/{session_id}/approval",
                json={
                    "request_id": "approval_turn_need_approval",
                    "decision": "denied",
                    "metadata": _control_metadata(WORKER_SESSION_APPROVE),
                },
                headers=headers,
            )
        ).json()
        pending_after = (await c.get(f"{base}/sessions/{session_id}/requests", headers=headers)).json()
        approval_events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        input_created = (
            await c.post(
                base + "/sessions",
                json={
                    "provider": "fake",
                    "engine": "fake",
                    "title": "Input request",
                    "metadata": metadata,
                },
                headers=headers,
            )
        ).json()
        input_session_id = input_created["session"]["session_id"]
        input_turn = (
            await c.post(
                f"{base}/sessions/{input_session_id}/turns",
                json={
                    "turn_id": "turn_need_input",
                    "prompt": "request input",
                    "metadata": _control_metadata(WORKER_SESSION_TURN),
                },
                headers=headers,
            )
        ).json()
        input_pending = (await c.get(f"{base}/sessions/{input_session_id}/requests", headers=headers)).json()
        input_reply = (
            await c.post(
                f"{base}/sessions/{input_session_id}/input",
                json={
                    "request_id": "input_turn_need_input",
                    "text": "continue",
                    "metadata": _control_metadata(WORKER_SESSION_INPUT),
                },
                headers=headers,
            )
        ).json()
        input_pending_after = (await c.get(f"{base}/sessions/{input_session_id}/requests", headers=headers)).json()
        checkpoint_turn = (
            await c.post(
                f"{base}/sessions/{input_session_id}/turns",
                json={
                    "turn_id": "turn_checkpoint",
                    "prompt": "complete normally",
                    "metadata": _control_metadata(WORKER_SESSION_TURN, resume_session=True),
                },
                headers=headers,
            )
        ).json()
        checkpoints = (await c.get(f"{base}/sessions/{input_session_id}/checkpoints", headers=headers)).json()
        restored = (
            await c.post(
                f"{base}/sessions/{input_session_id}/checkpoints/restore",
                json={
                    "checkpoint_id": "ckpt_turn_checkpoint",
                    "metadata": _control_metadata(WORKER_SESSION_RESTORE),
                },
                headers=headers,
            )
        ).json()
        checkpoints_after_restore = (await c.get(f"{base}/sessions/{input_session_id}/checkpoints", headers=headers)).json()
        events = (await c.get(f"{base}/sessions/{input_session_id}/events", headers=headers)).json()["events"]
        return (
            approval_turn,
            pending_before,
            global_pending,
            denied,
            pending_after,
            approval_events,
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
        approval_events,
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
    assert "turn.failed" in [event["type"] for event in approval_events]
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


def test_daemon_checkpoint_restore_requires_envelope_authority(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={"provider": "fake", "engine": "fake", "metadata": _authority_metadata("fake")},
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        restored = await c.post(
            f"{base}/sessions/{session_id}/checkpoints/restore",
            json={"checkpoint_id": "ckpt_any"},
            headers=headers,
        )
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return restored.status_code, restored.json(), events

    status_code, body, events = asyncio.run(_with_server(cfg, 8843, calls))

    assert status_code == 400
    assert WORKER_SESSION_RESTORE in body["error"]
    assert [event["type"] for event in events] == ["session.created"]


def test_daemon_checkpoint_restore_returns_unsupported_without_provider_handler(tmp_path) -> None:
    cfg = WorkerConfig(_env_file=None, token="tkn", workspace=str(tmp_path / "worker"))
    headers = {"Authorization": "Bearer tkn"}

    async def calls(base, c):  # noqa: ANN001
        created = (
            await c.post(
                base + "/sessions",
                json={
                    "provider": "claude",
                    "engine": "claude",
                    "cwd": _owned_worker_cwd(tmp_path, "claude-no-restore"),
                    "metadata": _authority_metadata("claude", extra_actions=[WORKER_SESSION_RESTORE]),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        direct = SessionManager(str(tmp_path / "worker" / "sessions"))
        direct.append_event(session_id, "checkpoint.created", {"checkpoint_id": "ckpt_manual"})
        restored = await c.post(
            f"{base}/sessions/{session_id}/checkpoints/restore",
            json={
                "checkpoint_id": "ckpt_manual",
                "metadata": _control_metadata(WORKER_SESSION_RESTORE),
            },
            headers=headers,
        )
        events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
        return restored.status_code, restored.json(), events

    status_code, body, events = asyncio.run(_with_server(cfg, 8844, calls))

    assert status_code == 501
    assert "does not support checkpoint restore" in body["error"]
    assert [event["type"] for event in events] == ["session.created", "checkpoint.created"]


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
                    "cwd": _owned_worker_cwd(tmp_path, "codex-projection"),
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
                json={
                    "prompt": "reply with hello",
                    "idempotency_key": "idem_codex",
                    "metadata": _control_metadata(WORKER_SESSION_TURN),
                },
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


def test_codex_turn_completed_does_not_overwrite_cancelled_session(tmp_path) -> None:
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _event = sessions.create(
        {
            "session_id": "sess_cancelled",
            "provider": "codex",
            "engine": "codex",
            "metadata": _authority_metadata("codex"),
        }
    )
    sessions.update_status(session.session_id, SESSION_STOPPED)

    done = _project_jsonrpc_message(
        object(),  # type: ignore[arg-type]
        {"jsonrpc": "2.0", "method": "turn/completed", "params": {"turn": {"status": "completed"}}},
        session_id=session.session_id,
        turn=ProviderTurn(turn_id="turn_cancelled", prompt="x", idempotency_key="idem_cancelled"),
        sessions=sessions,
    )

    assert done is True
    assert sessions.get(session.session_id).status == SESSION_STOPPED  # type: ignore[union-attr]
    assert all(event.type != EVENT_TURN_COMPLETED for event in sessions.events(session.session_id))


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
                    "cwd": _owned_worker_cwd(tmp_path, "codex-interrupt"),
                    "title": "Interrupt Codex",
                    "metadata": _authority_metadata("codex", extra_actions=[WORKER_SESSION_INTERRUPT]),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={
                "prompt": "wait",
                "idempotency_key": "idem_codex_interrupt",
                "metadata": _control_metadata(WORKER_SESSION_TURN),
            },
            headers=headers,
        )
        events = []
        for _ in range(100):
            events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            if any(event["type"] == "provider.process.started" for event in events):
                break
            await asyncio.sleep(0.05)
        interrupted = (
            await c.post(
                f"{base}/sessions/{session_id}/interrupt",
                json={"metadata": _control_metadata(WORKER_SESSION_INTERRUPT)},
                headers=headers,
            )
        ).json()
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
                    "cwd": _owned_worker_cwd(tmp_path, "codex-stop"),
                    "title": "Stop Codex",
                    "metadata": _authority_metadata("codex", extra_actions=[WORKER_SESSION_STOP]),
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={
                "prompt": "wait",
                "idempotency_key": "idem_codex_stop",
                "metadata": _control_metadata(WORKER_SESSION_TURN),
            },
            headers=headers,
        )
        events = []
        for _ in range(100):
            events = (await c.get(f"{base}/sessions/{session_id}/events", headers=headers)).json()["events"]
            if any(event["type"] == "provider.process.started" for event in events):
                break
            await asyncio.sleep(0.05)
        stopped = (
            await c.post(
                f"{base}/sessions/{session_id}/stop",
                json={"metadata": _control_metadata(WORKER_SESSION_STOP)},
                headers=headers,
            )
        ).json()
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
        if payload["result"]["decision"] != "accept":
            raise SystemExit(f"unexpected approval decision {payload['result']!r}")
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
                    "cwd": _owned_worker_cwd(tmp_path, "codex-approval"),
                    "title": "Approve Codex",
                    "metadata": metadata,
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={
                "prompt": "ask approval",
                "idempotency_key": "idem_codex_approval",
                "metadata": _control_metadata(WORKER_SESSION_TURN),
            },
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
                json={
                    "request_id": pending["requests"][0]["request_id"],
                    "decision": "approved",
                    "metadata": _control_metadata(WORKER_SESSION_APPROVE),
                },
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


def test_codex_records_approval_resolution_only_after_delivery() -> None:
    class ClosedStdin:
        def write(self, _payload: str) -> None:
            raise BrokenPipeError("closed")

        def flush(self) -> None:
            return None

    class CaptureStdin:
        def __init__(self) -> None:
            self.payloads: list[str] = []

        def write(self, payload: str) -> None:
            self.payloads.append(payload)

        def flush(self) -> None:
            return None

    class Process:
        stdin = ClosedStdin()

    process = Process()
    recorded: list[str] = []
    _track_pending_request(
        "sess_delivery",
        "approval_rpc",
        kind=REQUEST_KIND_APPROVAL,
        process=process,  # type: ignore[arg-type]
        rpc_id="approval_rpc",
    )

    with pytest.raises(BrokenPipeError):
        _deliver_pending_request(
            "sess_delivery",
            "approval_rpc",
            kind=REQUEST_KIND_APPROVAL,
            request={"request_id": "approval_rpc", "decision": "approved"},
            before_send=lambda: recorded.append("recorded"),
        )

    assert recorded == []

    capture = CaptureStdin()
    process.stdin = capture
    assert _deliver_pending_request(
        "sess_delivery",
        "approval_rpc",
        kind=REQUEST_KIND_APPROVAL,
        request={"request_id": "approval_rpc", "decision": "approved"},
        before_send=lambda: recorded.append("recorded"),
    )
    assert capture.payloads
    assert recorded == ["recorded"]


def test_codex_turn_timeout_pauses_while_request_pending(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    from jarvis.worker.providers import codex

    class Process:
        def poll(self):
            return None

    process = Process()
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _event = sessions.create(
        {
            "session_id": "sess_waiting",
            "provider": "codex",
            "engine": "codex",
            "metadata": _authority_metadata("codex"),
        }
    )
    turn = ProviderTurn(turn_id="turn_waiting", prompt="x", idempotency_key="idem_waiting")
    _track_pending_request(
        session.session_id,
        "approval_rpc",
        kind=REQUEST_KIND_APPROVAL,
        process=process,  # type: ignore[arg-type]
        rpc_id="approval_rpc",
    )
    calls = 0

    def fake_read_message(*_args, **_kwargs):  # noqa: ANN002, ANN003, ANN202
        nonlocal calls
        calls += 1
        if calls < 3:
            return None
        codex._forget_pending_request(session.session_id, "approval_rpc", process=process)  # type: ignore[arg-type]
        return {"jsonrpc": "2.0", "method": "turn/completed", "params": {"turn": {"status": "completed"}}}

    monkeypatch.setattr(codex, "_read_message", fake_read_message)

    _read_until_turn_done(
        process,  # type: ignore[arg-type]
        session_id=session.session_id,
        turn=turn,
        sessions=sessions,
        line_queue=asyncio.Queue(),  # type: ignore[arg-type]
        timeout_s=0,
    )

    assert calls == 3
    assert sessions.get(session.session_id).status == "completed"  # type: ignore[union-attr]


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
            "params": {"prompt": "Need more context", "questions": [{"id": "details"}]},
        })
    elif request_id == "input_rpc":
        answer = payload["result"]["answers"]["details"]["answers"][0]
        emit({
            "jsonrpc": "2.0",
            "method": "item/completed",
            "params": {"item": {"type": "agentMessage", "text": answer}},
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
                    "cwd": _owned_worker_cwd(tmp_path, "codex-input"),
                    "title": "Input Codex",
                    "metadata": metadata,
                },
                headers=headers,
            )
        ).json()
        session_id = created["session"]["session_id"]
        await c.post(
            f"{base}/sessions/{session_id}/turns",
            json={
                "prompt": "ask input",
                "idempotency_key": "idem_codex_input",
                "metadata": _control_metadata(WORKER_SESSION_TURN),
            },
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
                json={
                    "request_id": pending["requests"][0]["request_id"],
                    "text": "continue with tests",
                    "metadata": _control_metadata(WORKER_SESSION_INPUT),
                },
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
                json={"prompt": "should not run", "metadata": _control_metadata(WORKER_SESSION_TURN)},
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
        created = await c.post(
            base + "/sessions",
            json={"run_id": "run_no_auth", "provider": "codex", "engine": "codex"},
            headers=headers,
        )
        listed = (await c.get(f"{base}/sessions", headers=headers)).json()
        return created.status_code, created.json(), listed

    status_code, body, listed = asyncio.run(_with_server(cfg, 8832, calls))

    assert status_code == 400
    assert body["ok"] is False
    assert "execution_envelope is required" in body["error"]
    assert listed["sessions"] == []


def test_daemon_claude_provider_projects_sdk_events_and_reuses_stream(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    native_id = "11111111-1111-4111-8111-111111111111"
    _install_fake_claude_sdk(
        monkeypatch,
        [
            [
                [
                    _FakeSystemMessage(
                        "init",
                        {"type": "system", "subtype": "init", "session_id": native_id, "model": "claude-test", "cwd": "/tmp"},
                    ),
                    _FakeAssistantMessage([_FakeTextBlock("hello"), _FakeToolUseBlock("tool_1", "Read", {"file_path": "README.md"})], session_id=native_id),
                    _FakeUserMessage([_FakeToolResultBlock("tool_1", "ok")]),
                    _FakeResultMessage(session_id=native_id),
                ],
                [
                    _FakeAssistantMessage([_FakeTextBlock("hello again")], session_id=native_id),
                    _FakeResultMessage(session_id=native_id),
                ],
            ]
        ],
    )
    cfg = WorkerConfig(
        _env_file=None,
        token="tkn",
        workspace=str(tmp_path / "worker"),
        claude_bin="fake-claude",
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
                    "cwd": _owned_worker_cwd(tmp_path, "claude-projection"),
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
                json={
                    "prompt": "reply with hello",
                    "idempotency_key": "idem_claude_1",
                    "metadata": _control_metadata(WORKER_SESSION_TURN),
                },
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
                json={
                    "prompt": "resume and reply again",
                    "idempotency_key": "idem_claude_2",
                    "metadata": _control_metadata(WORKER_SESSION_TURN, resume_session=True),
                },
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

    try:
        created, first, second, events, fetched = asyncio.run(_with_server(cfg, 8830, calls))
    finally:
        _stop_fake_claude_runtimes()

    event_types = [event["type"] for event in events]
    process_events = [event for event in events if event["type"] == "provider.process.started"]
    assert created["ok"] is True
    assert [event["type"] for event in first["events"]] == ["turn.started", "provider.started"]
    assert [event["type"] for event in second["events"]] == ["turn.started", "provider.started"]
    assert "provider.session.ready" in event_types
    assert "assistant.message" in event_types
    assert "tool.call" in event_types
    assert "tool.result" in event_types
    assert event_types.count("turn.completed") == 2
    assert [event["data"]["resume"] for event in process_events] == [False]
    assert fetched["status"] == "completed"
    assert fetched["metadata"]["provider_session_id"]
    assert fetched["metadata"]["claude_session_started"] == "true"
    assert len(_FakeClaudeClient.instances) == 1
    assert _FakeClaudeClient.instances[0].queries == ["reply with hello", "resume and reply again"]
    assert _FakeClaudeClient.instances[0].options.kwargs["session_id"]
    assert _FakeClaudeClient.instances[0].options.kwargs["resume"] is None
    assert _FakeClaudeClient.instances[0].options.kwargs["system_prompt"] == {"type": "preset", "preset": "claude_code"}
    assert _FakeClaudeClient.instances[0].options.kwargs["setting_sources"] is None
    assert _FakeClaudeClient.instances[0].response_exhausted == 2


def test_claude_provider_resumes_after_runtime_restart(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    native_id = "22222222-2222-4222-8222-222222222222"
    _install_fake_claude_sdk(
        monkeypatch,
        [
            [
                [
                    _FakeAssistantMessage([_FakeTextBlock("resumed")], session_id=native_id),
                    _FakeResultMessage(session_id=native_id),
                ]
            ]
        ],
    )
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "claude",
            "engine": "claude",
            "cwd": _owned_worker_cwd(tmp_path, "claude-resume"),
            "metadata": _authority_metadata("claude"),
        }
    )
    sessions.update_metadata(session.session_id, {"claude_session_id": native_id, "claude_session_started": "true"})
    session = sessions.get(session.session_id)
    assert session is not None

    try:
        claude.ClaudeProviderAdapter().start_turn(
            session=session,
            turn=ProviderTurn(turn_id="turn_resume", prompt="continue"),
            sessions=sessions,
            worker_cfg=WorkerConfig(_env_file=None, workspace=str(tmp_path / "worker"), claude_bin="fake-claude", job_timeout_s=5),
        )
        for _ in range(100):
            if any(event.type == "turn.completed" for event in sessions.events(session.session_id)):
                break
            time.sleep(0.02)
    finally:
        _stop_fake_claude_runtimes()

    assert _FakeClaudeClient.instances[0].options.kwargs["session_id"] is None
    assert _FakeClaudeClient.instances[0].options.kwargs["resume"] == native_id


def test_claude_connection_failure_fails_queued_turn(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    _install_fake_claude_sdk(monkeypatch, [[[]]])
    _FakeClaudeClient.connect_error = RuntimeError("bad claude auth")
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "claude",
            "engine": "claude",
            "cwd": _owned_worker_cwd(tmp_path, "claude-connect-failure"),
            "metadata": _authority_metadata("claude"),
        }
    )

    try:
        claude.ClaudeProviderAdapter().start_turn(
            session=session,
            turn=ProviderTurn(turn_id="turn_connect_failure", prompt="hello"),
            sessions=sessions,
            worker_cfg=WorkerConfig(_env_file=None, workspace=str(tmp_path / "worker"), claude_bin="fake-claude", job_timeout_s=5),
        )
        for _ in range(100):
            if any(event.type == "turn.failed" for event in sessions.events(session.session_id)):
                break
            time.sleep(0.02)
    finally:
        _stop_fake_claude_runtimes()

    failed = [event for event in sessions.events(session.session_id) if event.type == "turn.failed"]
    assert failed[-1].data["turn_id"] == "turn_connect_failure"
    assert "bad claude auth" in failed[-1].data["error"]
    assert sessions.get(session.session_id).status == "failed"  # type: ignore[union-attr]


def test_claude_approval_request_resolves_through_adapter(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    native_id = "33333333-3333-4333-8333-333333333333"
    _install_fake_claude_sdk(
        monkeypatch,
        [
            [
                [
                    _FakePermissionAsk("Bash", {"command": "pytest"}, request_id="approval_1"),
                    _FakeResultMessage(session_id=native_id),
                ]
            ]
        ],
    )
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "claude",
            "engine": "claude",
            "cwd": _owned_worker_cwd(tmp_path, "claude-approval"),
            "metadata": _authority_metadata("claude", extra_actions=[WORKER_SESSION_APPROVE]),
        }
    )
    adapter = claude.ClaudeProviderAdapter()

    try:
        adapter.start_turn(
            session=session,
            turn=ProviderTurn(turn_id="turn_approval", prompt="run tests"),
            sessions=sessions,
            worker_cfg=WorkerConfig(_env_file=None, workspace=str(tmp_path / "worker"), claude_bin="fake-claude", job_timeout_s=5),
        )
        for _ in range(100):
            if sessions.pending_requests(session.session_id):
                break
            time.sleep(0.02)
        assert sessions.pending_requests(session.session_id)[0]["request_id"] == "approval_1"

        event = adapter.resolve_approval(
            session=sessions.get(session.session_id),  # type: ignore[arg-type]
            request={"request_id": "approval_1", "decision": "approved"},
            sessions=sessions,
        )
        for _ in range(100):
            if any(item.type == "turn.completed" for item in sessions.events(session.session_id)):
                break
            time.sleep(0.02)
    finally:
        _stop_fake_claude_runtimes()

    assert event.type == "approval.resolved"
    assert isinstance(_FakeClaudeClient.instances[0].permission_results[0], _FakePermissionResultAllow)
    assert sessions.pending_requests(session.session_id) == []
    assert sessions.get(session.session_id).status == "completed"  # type: ignore[union-attr]


def test_claude_approval_timeout_denies_and_fails_turn(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    _install_fake_claude_sdk(
        monkeypatch,
        [[[_FakePermissionAsk("Bash", {"command": "pytest"}, request_id="approval_timeout")]]],
    )
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "claude",
            "engine": "claude",
            "cwd": _owned_worker_cwd(tmp_path, "claude-approval-timeout"),
            "metadata": _authority_metadata("claude", extra_actions=[WORKER_SESSION_APPROVE]),
        }
    )

    try:
        claude.ClaudeProviderAdapter().start_turn(
            session=session,
            turn=ProviderTurn(turn_id="turn_timeout", prompt="run tests"),
            sessions=sessions,
            worker_cfg=WorkerConfig(_env_file=None, workspace=str(tmp_path / "worker"), claude_bin="fake-claude", job_timeout_s=0.2),
        )
        for _ in range(100):
            if any(event.type == "turn.failed" for event in sessions.events(session.session_id)):
                break
            time.sleep(0.02)
    finally:
        _stop_fake_claude_runtimes()

    event_types = [event.type for event in sessions.events(session.session_id)]
    assert "approval.requested" in event_types
    assert "approval.resolved" in event_types
    assert "turn.failed" in event_types
    resolved = [event for event in sessions.events(session.session_id) if event.type == "approval.resolved"]
    assert resolved[-1].data["decision"] == "denied"
    assert "timed out" in resolved[-1].data["message"]


def test_claude_ask_user_question_uses_input_endpoint(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    native_id = "44444444-4444-4444-8444-444444444444"
    _install_fake_claude_sdk(
        monkeypatch,
        [
            [
                [
                    _FakePermissionAsk("AskUserQuestion", {"question": "Proceed?", "questions": [{"id": "choice"}]}, request_id="input_1"),
                    _FakeResultMessage(session_id=native_id),
                ]
            ]
        ],
    )
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "claude",
            "engine": "claude",
            "cwd": _owned_worker_cwd(tmp_path, "claude-input"),
            "metadata": _authority_metadata("claude", extra_actions=[WORKER_SESSION_INPUT]),
        }
    )
    adapter = claude.ClaudeProviderAdapter()

    try:
        adapter.start_turn(
            session=session,
            turn=ProviderTurn(turn_id="turn_input", prompt="ask me"),
            sessions=sessions,
            worker_cfg=WorkerConfig(_env_file=None, workspace=str(tmp_path / "worker"), claude_bin="fake-claude", job_timeout_s=5),
        )
        for _ in range(100):
            if sessions.pending_requests(session.session_id):
                break
            time.sleep(0.02)
        assert sessions.pending_requests(session.session_id)[0]["kind"] == "input"
        event = adapter.receive_input(
            session=sessions.get(session.session_id),  # type: ignore[arg-type]
            request={"request_id": "input_1", "answer": "yes"},
            sessions=sessions,
        )
        for _ in range(100):
            if any(item.type == "turn.completed" for item in sessions.events(session.session_id)):
                break
            time.sleep(0.02)
    finally:
        _stop_fake_claude_runtimes()

    result = _FakeClaudeClient.instances[0].permission_results[0]
    assert event.type == "input.received"
    assert isinstance(result, _FakePermissionResultAllow)
    assert result.updated_input["answers"] == {"choice": "yes"}


def test_claude_interrupt_and_stop_call_live_client(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    _install_fake_claude_sdk(monkeypatch, [[[ _FakePermissionAsk("AskUserQuestion", {"question": "Wait?"}, request_id="input_wait") ]]])
    sessions = SessionManager(str(tmp_path / "sessions"))
    session, _ = sessions.create(
        {
            "provider": "claude",
            "engine": "claude",
            "cwd": _owned_worker_cwd(tmp_path, "claude-stop"),
            "metadata": _authority_metadata("claude", extra_actions=[WORKER_SESSION_INPUT, WORKER_SESSION_INTERRUPT, WORKER_SESSION_STOP]),
        }
    )
    adapter = claude.ClaudeProviderAdapter()

    adapter.start_turn(
        session=session,
        turn=ProviderTurn(turn_id="turn_stop", prompt="wait"),
        sessions=sessions,
        worker_cfg=WorkerConfig(_env_file=None, workspace=str(tmp_path / "worker"), claude_bin="fake-claude", job_timeout_s=5),
    )
    for _ in range(100):
        if sessions.pending_requests(session.session_id):
            break
        time.sleep(0.02)
    interrupted, interrupt_event = adapter.interrupt(session=sessions.get(session.session_id), sessions=sessions)  # type: ignore[arg-type]
    stopped, stop_event = adapter.stop(session=sessions.get(session.session_id), sessions=sessions)  # type: ignore[arg-type]
    _stop_fake_claude_runtimes()

    assert interrupt_event.type == "session.interrupted"
    assert interrupted.status == "interrupted"
    assert stop_event.type == "session.stopped"
    assert stopped.status == "stopped"
    assert _FakeClaudeClient.instances[0].interrupted is True
    assert _FakeClaudeClient.instances[0].disconnected is True
    resolved = [event for event in sessions.events(session.session_id) if event.type == "input.received"]
    assert resolved[-1].data["request_id"] == "input_wait"
    assert resolved[-1].data["decision"] == "denied"


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
    assert health["engine_supports"]["codex"]["approval_requests"] is True
    assert health["engine_supports"]["codex"]["input_requests"] is True
    assert health["engine_supports"]["codex"]["checkpoints"] is True
    assert health["engine_supports"]["claude"]["resume"] is True
    assert health["engine_supports"]["claude"]["checkpoints"] is False


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
