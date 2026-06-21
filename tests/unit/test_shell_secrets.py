"""Allowlisted secret injection into worker shell commands (the credential boundary).

Jarvis can USE a named secret in a shell command ($OPENAI_API_KEY) without the model
ever receiving its value: the worker resolves an allowlist into the subprocess env,
and the system prompt tells the model the NAMES (not values).
"""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.brain.session import BrainSession
from jarvis.config import WorkerConfig, load_config
from jarvis.tools.base import ToolRegistry
from jarvis.worker.actions import run_shell
from jarvis.worker.server import _shell_env


def test_shell_env_resolves_only_the_allowlist(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setenv("JARVIS_TEST_SECRET", "s3kr3t")
    env = _shell_env(WorkerConfig(_env_file=None, shell_secrets="JARVIS_TEST_SECRET,NOT_SET"))
    assert env == {"JARVIS_TEST_SECRET": "s3kr3t"}  # unset names are skipped


def test_shell_env_empty_by_default() -> None:
    assert _shell_env(WorkerConfig(_env_file=None, shell_secrets="")) == {}


def test_run_shell_injects_env_so_commands_can_reference_it() -> None:
    out = asyncio.run(run_shell("echo got:$JARVIS_X", None, 5.0, env={"JARVIS_X": "yes"}))
    assert "got:yes" in out  # the shell expanded the injected secret by name


def test_run_shell_does_not_leak_non_allowlisted_env(monkeypatch) -> None:  # noqa: ANN001
    """Deny-by-default: a host env secret that wasn't allowlisted must never reach the
    shell — even with no allowlist, and even when OTHER names are allowlisted."""
    monkeypatch.setenv("JARVIS_LEAK_TEST", "should-not-appear")
    out = asyncio.run(run_shell("echo [$JARVIS_LEAK_TEST]", None, 5.0, env=None))
    assert "should-not-appear" not in out and "[]" in out  # scrubbed: the var is unset
    out2 = asyncio.run(run_shell("echo [$JARVIS_LEAK_TEST]", None, 5.0, env={"OPENAI_API_KEY": "v"}))
    assert "should-not-appear" not in out2  # an allowlist of other names doesn't open it up


def test_system_prompt_lists_secret_names_only_with_worker_shell() -> None:
    cfg = load_config()
    cfg.worker = WorkerConfig(_env_file=None, shell_secrets="FOO_KEY,BAR_KEY")

    def prompt(*caps: str) -> str:
        s = BrainSession(
            cfg, RequestContext("d", "neil", "personal", frozenset(caps)),
            gateway=None, tts=None, memory=None, tracer=None, registry=ToolRegistry(),
        )
        return s._system_prompt("")

    p = prompt("worker.shell")
    assert "FOO_KEY" in p and "BAR_KEY" in p
    assert "never print" in p.lower()  # the don't-leak instruction
    assert "FOO_KEY" not in prompt("files.read")  # not surfaced without worker.shell
