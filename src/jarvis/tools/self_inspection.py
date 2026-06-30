"""Self-inspection tools for device and local terminal diagnostics.

These tools are intentionally read-only and capability-gated. Arbitrary command
execution stays behind the worker shell boundary; diagnostics here run a small
fixed allow-list only.
"""

from __future__ import annotations

import asyncio
import os
import platform
import re
import socket
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any

from jarvis.config import CapabilityConfig, ToolsConfig
from jarvis.runtime import RequestContext
from jarvis.tools.base import Tool


CAP_INSPECT = "self.inspect"
CAP_DIAGNOSTICS = "self.diagnostics"
_HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,252}$")


def _repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists() and (parent / "src" / "jarvis").exists():
            return parent
    return Path.cwd()


def _run(argv: list[str], *, cwd: Path, timeout_s: float, max_bytes: int) -> str:
    try:
        proc = subprocess.run(
            argv,
            cwd=cwd,
            text=True,
            capture_output=True,
            timeout=timeout_s,
            check=False,
        )
    except FileNotFoundError:
        return f"$ {' '.join(argv)}\nnot installed"
    except subprocess.TimeoutExpired:
        return f"$ {' '.join(argv)}\ntimed out after {timeout_s:.1f}s"
    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    body = "\n".join(part for part in (out, err) if part).strip() or "(no output)"
    if len(body.encode("utf-8")) > max_bytes:
        body = body.encode("utf-8")[:max_bytes].decode("utf-8", errors="replace") + "\n...[truncated]"
    return f"$ {' '.join(argv)}\nexit {proc.returncode}\n{body}"


def _host_arg(raw: Any) -> str:
    host = str(raw or "").strip()
    if not host:
        raise ValueError("empty host")
    if "://" in host:
        raise ValueError("use a hostname or IP address, not a URL")
    if not _HOST_RE.match(host):
        raise ValueError("host contains unsupported characters")
    return host


def _int_arg(raw: Any, *, default: int, min_value: int, max_value: int) -> int:
    try:
        value = int(raw if raw is not None else default)
    except (TypeError, ValueError):
        value = default
    return max(min_value, min(max_value, value))


def _local_ips() -> list[str]:
    ips: set[str] = set()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            ips.add(sock.getsockname()[0])
    except OSError:
        pass
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, family=socket.AF_INET):
            ips.add(info[4][0])
    except OSError:
        pass
    return sorted(ip for ip in ips if not ip.startswith("127."))


def _public_ip(timeout_s: float) -> str:
    try:
        with urllib.request.urlopen("https://api.ipify.org", timeout=timeout_s) as response:
            return response.read(64).decode("ascii", errors="replace").strip()
    except Exception as exc:  # noqa: BLE001 - external network may be unavailable
        return f"unavailable ({exc})"


def make_self_tools(
    cfg: ToolsConfig,
    capabilities: CapabilityConfig,
) -> list[Tool]:
    root = _repo_root()

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
        commands = [
            ["uname", "-a"],
            ["uptime"],
            ["df", "-h", str(Path.cwd())],
            ["sh", "-lc", "command -v pmset >/dev/null && pmset -g batt || true"],
            ["sh", "-lc", "command -v vm_stat >/dev/null && vm_stat | head -20 || true"],
            ["sh", "-lc", "ps -o pid,ppid,%cpu,%mem,comm -p $$"],
        ]
        command_results = await asyncio.to_thread(
            lambda: [
                _run(command, cwd=root, timeout_s=timeout, max_bytes=max_bytes)
                for command in commands
            ]
        )
        return "\n".join(
            [
                "basic_runtime:",
                f"- host={socket.gethostname()} platform={platform.platform()} python={sys.version.split()[0]}",
                f"- request_device_id={ctx.device_id} configured_device_id={capabilities.device_id}",
                f"- cwd={Path.cwd()}",
                "",
                "tool_config:",
                f"- timeout_s={cfg.timeout_s}",
                f"- max_rounds={cfg.max_rounds}",
                f"- diagnostic_timeout_s={cfg.self_diagnostic_timeout_s}",
                "",
                "terminal_checks:",
                *command_results,
            ]
        )

    def get_ip_address(ctx: RequestContext, args: dict[str, Any]) -> str:
        del ctx
        include_public = bool(args.get("include_public", True))
        timeout = min(max(cfg.self_diagnostic_timeout_s, 0.5), 3.0)
        local = _local_ips()
        lines = [
            f"host: {socket.gethostname()}",
            "local_ipv4: " + (", ".join(local) if local else "unavailable"),
        ]
        if include_public:
            lines.append(f"public_ipv4: {_public_ip(timeout)}")
        return "\n".join(lines)

    def ping_host(ctx: RequestContext, args: dict[str, Any]) -> str:
        del ctx
        try:
            host = _host_arg(args.get("host"))
        except ValueError as exc:
            return f"error: {exc}"
        count = _int_arg(args.get("count"), default=4, min_value=1, max_value=10)
        timeout = max(cfg.self_diagnostic_timeout_s * count, cfg.self_diagnostic_timeout_s + 2.0)
        return _run(
            ["ping", "-c", str(count), host],
            cwd=root,
            timeout_s=timeout,
            max_bytes=cfg.self_max_bytes,
        )

    def resolve_dns(ctx: RequestContext, args: dict[str, Any]) -> str:
        del ctx
        try:
            host = _host_arg(args.get("host"))
        except ValueError as exc:
            return f"error: {exc}"
        try:
            infos = socket.getaddrinfo(host, None)
        except OSError as exc:
            return f"error: DNS lookup failed for {host}: {exc}"
        addresses = sorted({item[4][0] for item in infos})
        return f"{host} resolves to: " + (", ".join(addresses) if addresses else "(no addresses)")

    def check_tcp_port(ctx: RequestContext, args: dict[str, Any]) -> str:
        del ctx
        try:
            host = _host_arg(args.get("host"))
        except ValueError as exc:
            return f"error: {exc}"
        port = _int_arg(args.get("port"), default=443, min_value=1, max_value=65535)
        timeout = min(max(cfg.self_diagnostic_timeout_s, 0.5), 5.0)
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return f"{host}:{port} is reachable."
        except OSError as exc:
            return f"{host}:{port} is not reachable ({exc})."

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
