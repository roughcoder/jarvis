"""Bounded local diagnostics shared by the brain and intercom edge."""

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

HOST_RE = re.compile(r"^[A-Za-z0-9][-A-Za-z0-9_.:]{0,252}$")


def repo_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists() and (parent / "src" / "jarvis").exists():
            return parent
    return Path.cwd()


def run_command(argv: list[str], *, cwd: Path, timeout_s: float, max_bytes: int) -> str:
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


def host_arg(raw: Any) -> str:
    host = str(raw or "").strip()
    if not host:
        raise ValueError("empty host")
    if "://" in host:
        raise ValueError("use a hostname or IP address, not a URL")
    if not HOST_RE.match(host):
        raise ValueError("host contains unsupported characters")
    return host


def int_arg(raw: Any, *, default: int, min_value: int, max_value: int) -> int:
    try:
        value = int(raw if raw is not None else default)
    except (TypeError, ValueError):
        value = default
    return max(min_value, min(max_value, value))


def local_ips() -> list[str]:
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


def public_ip(timeout_s: float) -> str:
    try:
        with urllib.request.urlopen("https://api.ipify.org", timeout=timeout_s) as response:
            return response.read(64).decode("ascii", errors="replace").strip()
    except Exception as exc:  # noqa: BLE001 - external network may be unavailable
        return f"unavailable ({exc})"


async def run_self_diagnostics(
    *,
    request_device_id: str,
    configured_device_id: str,
    timeout_s: float,
    max_bytes: int,
    root: Path | None = None,
) -> str:
    cwd = root or repo_root()
    commands = [
        ["uname", "-a"],
        ["uptime"],
        ["df", "-h", str(Path.cwd())],
        ["sh", "-lc", "command -v pmset >/dev/null && pmset -g batt || true"],
        ["sh", "-lc", "command -v vm_stat >/dev/null && vm_stat | head -20 || true"],
        ["ps", "-o", "pid,ppid,%cpu,%mem,comm", "-p", str(os.getpid())],
    ]
    command_results = await asyncio.to_thread(
        lambda: [
            run_command(command, cwd=cwd, timeout_s=timeout_s, max_bytes=max_bytes)
            for command in commands
        ]
    )
    return "\n".join(
        [
            "basic_runtime:",
            f"- host={socket.gethostname()} platform={platform.platform()} python={sys.version.split()[0]}",
            f"- request_device_id={request_device_id} configured_device_id={configured_device_id}",
            f"- cwd={Path.cwd()}",
            "",
            "terminal_checks:",
            *command_results,
        ]
    )


async def get_ip_address(*, include_public: bool, timeout_s: float) -> str:
    def gather() -> str:
        local = local_ips()
        lines = [
            f"host: {socket.gethostname()}",
            "local_ipv4: " + (", ".join(local) if local else "unavailable"),
        ]
        if include_public:
            lines.append(f"public_ipv4: {public_ip(timeout_s)}")
        return "\n".join(lines)

    return await asyncio.to_thread(gather)


async def ping_host(
    *,
    host: str,
    count: int,
    timeout_s: float,
    max_bytes: int,
    root: Path | None = None,
) -> str:
    cwd = root or repo_root()
    return await asyncio.to_thread(
        run_command,
        ["ping", "-c", str(count), host],
        cwd=cwd,
        timeout_s=timeout_s,
        max_bytes=max_bytes,
    )


async def resolve_dns(*, host: str) -> str:
    try:
        infos = await asyncio.to_thread(socket.getaddrinfo, host, None)
    except OSError as exc:
        return f"error: DNS lookup failed for {host}: {exc}"
    addresses = sorted({item[4][0] for item in infos})
    return f"{host} resolves to: " + (", ".join(addresses) if addresses else "(no addresses)")


async def check_tcp_port(*, host: str, port: int, timeout_s: float) -> str:
    try:
        sock = await asyncio.to_thread(socket.create_connection, (host, port), timeout_s)
        sock.close()
        return f"{host}:{port} is reachable."
    except OSError as exc:
        return f"{host}:{port} is not reachable ({exc})."
