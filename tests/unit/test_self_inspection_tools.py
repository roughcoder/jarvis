"""Self-inspection tools — device awareness and fixed terminal diagnostics."""

from __future__ import annotations

import asyncio
import socket

from jarvis.brain.context import RequestContext
from jarvis.config import CapabilityConfig, ToolsConfig
from jarvis.tools import build_registry
from jarvis.tools.self_inspection import make_self_tools


def _ctx(*caps: str) -> RequestContext:
    return RequestContext("local-mac", "neil", "personal", frozenset(caps), channel="text")


def test_self_tools_registered_and_gated() -> None:
    reg = build_registry(ToolsConfig(_env_file=None))

    assert "describe_device" not in {t.name for t in reg.available_for(_ctx())}
    inspect = {t.name for t in reg.available_for(_ctx("self.inspect"))}
    assert inspect == {"describe_device"}
    assert "run_self_diagnostics" not in inspect
    diagnostics = {t.name for t in reg.available_for(_ctx("self.diagnostics"))}
    assert {
        "run_self_diagnostics",
        "get_ip_address",
        "ping_host",
        "resolve_dns",
        "check_tcp_port",
    } <= diagnostics


def test_describe_device_uses_request_context() -> None:
    tools = {
        t.name: t
        for t in make_self_tools(
            ToolsConfig(_env_file=None),
            CapabilityConfig(_env_file=None, device_id="configured-mac"),
        )
    }

    out = tools["describe_device"].handler(_ctx("self.inspect", "worker.shell"), {})

    assert "request_device_id: local-mac" in out
    assert "identity: neil" in out
    assert "configured_device_id: configured-mac" in out
    assert "worker.shell" in out


def test_diagnostics_are_fixed_read_only_checks() -> None:
    tools = {
        t.name: t
        for t in make_self_tools(
            ToolsConfig(_env_file=None, self_diagnostic_timeout_s=1.0),
            CapabilityConfig(_env_file=None),
        )
    }

    out = asyncio.run(tools["run_self_diagnostics"].handler(_ctx("self.diagnostics"), {}))

    assert "basic_runtime:" in out
    assert "terminal_checks:" in out
    assert "$ uname -a" in out


def test_get_ip_address_can_skip_public_lookup() -> None:
    tools = {
        t.name: t
        for t in make_self_tools(
            ToolsConfig(_env_file=None),
            CapabilityConfig(_env_file=None),
        )
    }

    out = tools["get_ip_address"].handler(_ctx("self.diagnostics"), {"include_public": False})

    assert "host:" in out
    assert "local_ipv4:" in out
    assert "public_ipv4:" not in out


def test_resolve_dns_uses_local_resolver() -> None:
    tools = {
        t.name: t
        for t in make_self_tools(
            ToolsConfig(_env_file=None),
            CapabilityConfig(_env_file=None),
        )
    }

    out = tools["resolve_dns"].handler(_ctx("self.diagnostics"), {"host": "localhost"})

    assert "localhost resolves to:" in out
    assert "127.0.0.1" in out


def test_ping_host_rejects_shell_metacharacters() -> None:
    tools = {
        t.name: t
        for t in make_self_tools(
            ToolsConfig(_env_file=None),
            CapabilityConfig(_env_file=None),
        )
    }

    out = tools["ping_host"].handler(_ctx("self.diagnostics"), {"host": "example.com;rm -rf /"})

    assert out == "error: host contains unsupported characters"


def test_check_tcp_port_reports_reachability() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        tools = {
            t.name: t
            for t in make_self_tools(
                ToolsConfig(_env_file=None),
                CapabilityConfig(_env_file=None),
            )
        }

        out = tools["check_tcp_port"].handler(
            _ctx("self.diagnostics"), {"host": "127.0.0.1", "port": port}
        )

    assert f"127.0.0.1:{port} is reachable." == out


def test_execute_rechecks_self_capability() -> None:
    reg = build_registry(ToolsConfig(_env_file=None))

    out = asyncio.run(
        reg.execute(_ctx("self.inspect"), "describe_device", {}, timeout_s=2.0)
    )

    assert "request_device_id: local-mac" in out
