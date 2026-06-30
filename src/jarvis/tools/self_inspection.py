"""Self-inspection tools for device and local terminal diagnostics.

These tools are intentionally read-only and capability-gated. Arbitrary command
execution stays behind the worker shell boundary; diagnostics here run a small
fixed allow-list only.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
import os
import platform
import socket
import sys
from pathlib import Path
from typing import Any

from jarvis.config import CapabilityConfig, ToolsConfig
from jarvis.device_diagnostics import (
    check_tcp_port as check_tcp_port_local,
    get_ip_address as get_ip_address_local,
    host_arg,
    int_arg,
    ping_host as ping_host_local,
    repo_root,
    resolve_dns as resolve_dns_local,
    run_self_diagnostics as run_self_diagnostics_local,
)
from jarvis.runtime import RequestContext
from jarvis.tools.base import Tool


CAP_INSPECT = "self.inspect"
CAP_DIAGNOSTICS = "self.diagnostics"

DeviceAction = Callable[[RequestContext, str, dict[str, Any], float], Awaitable[dict[str, Any]]]


def make_self_tools(
    cfg: ToolsConfig,
    capabilities: CapabilityConfig,
    *,
    device_action: DeviceAction | None = None,
) -> list[Tool]:
    root = repo_root()

    async def device_or_local(
        ctx: RequestContext,
        action: str,
        args: dict[str, Any],
        timeout_s: float,
        local: Callable[[], Awaitable[str]],
    ) -> str:
        if device_action is not None and ctx.device_id != capabilities.device_id:
            result = await device_action(ctx, action, args, timeout_s)
            return str(result.get("text") or result)
        return await local()

    def describe_device(ctx: RequestContext, args: dict[str, Any]) -> str:
        del args
        caps = ", ".join(sorted(ctx.capabilities)) or "(none)"
        return "\n".join(
            [
                f"request_device_id: {ctx.device_id}",
                f"identity: {ctx.identity}",
                f"scope: {ctx.scope}",
                f"channel: {ctx.channel}",
                f"capabilities: {caps}",
                f"configured_device_id: {capabilities.device_id}",
                f"host: {socket.gethostname()}",
                f"platform: {platform.platform()}",
                f"machine: {platform.machine()}",
                f"python: {sys.version.split()[0]} ({sys.executable})",
                f"pid: {os.getpid()}",
                f"cwd: {Path.cwd()}",
            ]
        )

    async def run_diagnostics(ctx: RequestContext, args: dict[str, Any]) -> str:
        del args
        timeout = cfg.self_diagnostic_timeout_s
        max_bytes = cfg.self_max_bytes
        diagnostics = await device_or_local(
            ctx,
            "run_self_diagnostics",
            {},
            max(cfg.timeout_s, timeout * 6),
            lambda: run_self_diagnostics_local(
                request_device_id=ctx.device_id,
                configured_device_id=capabilities.device_id,
                timeout_s=timeout,
                max_bytes=max_bytes,
                root=root,
            ),
        )
        return "\n".join(
            [
                diagnostics,
                "",
                "tool_config:",
                f"- timeout_s={cfg.timeout_s}",
                f"- max_rounds={cfg.max_rounds}",
                f"- diagnostic_timeout_s={cfg.self_diagnostic_timeout_s}",
            ]
        )

    async def get_ip_address(ctx: RequestContext, args: dict[str, Any]) -> str:
        include_public = bool(args.get("include_public", True))
        timeout = min(max(cfg.self_diagnostic_timeout_s, 0.5), 3.0)
        return await device_or_local(
            ctx,
            "get_ip_address",
            {"include_public": include_public},
            timeout + 1.0,
            lambda: get_ip_address_local(include_public=include_public, timeout_s=timeout),
        )

    async def ping_host(ctx: RequestContext, args: dict[str, Any]) -> str:
        try:
            host = host_arg(args.get("host"))
        except ValueError as exc:
            return f"error: {exc}"
        count = int_arg(args.get("count"), default=4, min_value=1, max_value=10)
        timeout = max(cfg.self_diagnostic_timeout_s * count, cfg.self_diagnostic_timeout_s + 2.0)
        return await device_or_local(
            ctx,
            "ping_host",
            {"host": host, "count": count},
            timeout + 1.0,
            lambda: ping_host_local(
                host=host,
                count=count,
                timeout_s=timeout,
                max_bytes=cfg.self_max_bytes,
                root=root,
            ),
        )

    async def resolve_dns(ctx: RequestContext, args: dict[str, Any]) -> str:
        try:
            host = host_arg(args.get("host"))
        except ValueError as exc:
            return f"error: {exc}"
        return await device_or_local(
            ctx,
            "resolve_dns",
            {"host": host},
            max(cfg.timeout_s, cfg.self_diagnostic_timeout_s + 1.0),
            lambda: resolve_dns_local(host=host),
        )

    async def check_tcp_port(ctx: RequestContext, args: dict[str, Any]) -> str:
        try:
            host = host_arg(args.get("host"))
        except ValueError as exc:
            return f"error: {exc}"
        port = int_arg(args.get("port"), default=443, min_value=1, max_value=65535)
        timeout = min(max(cfg.self_diagnostic_timeout_s, 0.5), 5.0)
        return await device_or_local(
            ctx,
            "check_tcp_port",
            {"host": host, "port": port},
            timeout + 1.0,
            lambda: check_tcp_port_local(host=host, port=port, timeout_s=timeout),
        )

    obj = "object"
    return [
        Tool(
            "describe_device",
            "Describe the current Jarvis request device, identity, host runtime, and granted capabilities.",
            {"type": obj, "properties": {}},
            CAP_INSPECT,
            describe_device,
        ),
        Tool(
            "run_self_diagnostics",
            "Run fixed, read-only terminal diagnostics for the current device and Jarvis process.",
            {"type": obj, "properties": {}},
            CAP_DIAGNOSTICS,
            run_diagnostics,
            announce=True,
            timeout_s=max(cfg.timeout_s, cfg.self_diagnostic_timeout_s * 6),
        ),
        Tool(
            "get_ip_address",
            "Show this device's local IP address, and optionally its public IP address.",
            {
                "type": obj,
                "properties": {
                    "include_public": {
                        "type": "boolean",
                        "description": "Whether to also fetch the public internet-facing IPv4 address.",
                    }
                },
            },
            CAP_DIAGNOSTICS,
            get_ip_address,
            announce=False,
            timeout_s=max(cfg.timeout_s, cfg.self_diagnostic_timeout_s + 3),
        ),
        Tool(
            "ping_host",
            "Ping a hostname or IP address and return packet loss and round-trip timings.",
            {
                "type": obj,
                "properties": {
                    "host": {"type": "string", "description": "Hostname or IP address to ping."},
                    "count": {
                        "type": "integer",
                        "description": "Ping packet count, one to ten. Defaults to four.",
                    },
                },
                "required": ["host"],
            },
            CAP_DIAGNOSTICS,
            ping_host,
            announce=True,
            timeout_s=max(cfg.timeout_s, cfg.self_diagnostic_timeout_s * 10),
        ),
        Tool(
            "resolve_dns",
            "Resolve a hostname to IP addresses using this device's DNS configuration.",
            {
                "type": obj,
                "properties": {"host": {"type": "string", "description": "Hostname to resolve."}},
                "required": ["host"],
            },
            CAP_DIAGNOSTICS,
            resolve_dns,
        ),
        Tool(
            "check_tcp_port",
            "Check whether a TCP host:port is reachable from this device.",
            {
                "type": obj,
                "properties": {
                    "host": {"type": "string", "description": "Hostname or IP address."},
                    "port": {"type": "integer", "description": "TCP port, one to 65535."},
                },
                "required": ["host", "port"],
            },
            CAP_DIAGNOSTICS,
            check_tcp_port,
            announce=True,
            timeout_s=max(cfg.timeout_s, cfg.self_diagnostic_timeout_s + 3),
        ),
    ]
