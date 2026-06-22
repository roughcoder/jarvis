"""Deployment helpers for packaged Jarvis installs.

The development checkout can still use `uv run ...`, but fleet installs need a
stable surface that the Mac app, Homebrew formula, and Pi installer can call
without reimplementing launchd/systemd details.
"""

from __future__ import annotations

import os
import platform
import secrets
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


ROLES = ("brain", "intercom", "worker")
ROLE_COMMANDS = {
    "brain": ["brain"],
    "intercom": ["run"],
    "worker": ["worker"],
}
ROLE_EXTRAS = {
    "brain": ["gateway", "tts", "stt", "vad", "wake", "memory", "mcp"],
    "intercom": ["stt", "vad", "wake"],
    "worker": ["worker", "browser"],
}


@dataclass(frozen=True)
class ServicePaths:
    role: str
    platform_name: str
    destination: Path
    log_dir: Path


def role_extras(roles: list[str] | tuple[str, ...] | set[str]) -> list[str]:
    """Return role extras in a deterministic order, de-duplicated."""
    seen: set[str] = set()
    out: list[str] = []
    for role in ROLES:
        if role not in roles:
            continue
        for extra in ROLE_EXTRAS[role]:
            if extra not in seen:
                seen.add(extra)
                out.append(extra)
    return out


def issue_pairing_entry(device_id: str, *, identity: str = "") -> tuple[str, str]:
    """Return a fresh token and a BRAIN_DEVICES JSON object fragment."""
    token = secrets.token_urlsafe(32)
    parts = [f'"token":"{token}"', f'"device_id":"{device_id}"']
    if identity:
        parts.append(f'"identity":"{identity}"')
    return token, "{" + ",".join(parts) + "}"


def render_pi_installer_command(
    *,
    device_id: str,
    token: str,
    brain_host: str,
    brain_port: str = "8700",
    repo: str = "roughcoder/jarvis",
    ref: str = "main",
) -> str:
    """Return copy/paste commands for installing a paired Raspberry Pi intercom."""
    if not brain_host:
        raise ValueError("brain_host is required")

    assignments = {
        "JARVIS_BRAIN_HOST": brain_host,
        "JARVIS_BRAIN_PORT": brain_port,
        "JARVIS_INTERCOM_TOKEN": token,
        "JARVIS_DEVICE_ID": device_id,
        "JARVIS_REPO": repo,
        "JARVIS_REF": ref,
    }
    env = " ".join(f"{key}={shlex.quote(value)}" for key, value in assignments.items() if value)
    return "\n".join(
        [
            "curl -fsSL https://raw.githubusercontent.com/"
            f"{shlex.quote(repo)}/{shlex.quote(ref)}/scripts/install_pi.sh "
            "-o /tmp/install_jarvis_pi.sh",
            f"sudo {env} bash /tmp/install_jarvis_pi.sh",
        ]
    )


def detect_platform() -> str:
    system = platform.system().lower()
    if system == "darwin":
        return "launchd"
    if system == "linux":
        return "systemd"
    return system


def default_jarvis_bin() -> str:
    return shutil.which("jarvis") or sys.argv[0]


def default_workdir() -> Path:
    return Path.cwd()


def default_log_dir() -> Path:
    if detect_platform() == "launchd":
        return Path.home() / "Library" / "Logs" / "Jarvis"
    return Path("/var/log/jarvis")


def service_paths(
    role: str,
    *,
    platform_name: str | None = None,
    destination: str | None = None,
    log_dir: str | None = None,
) -> ServicePaths:
    _validate_role(role)
    target = platform_name or detect_platform()
    logs = Path(log_dir).expanduser() if log_dir else default_log_dir()
    if destination:
        dest = Path(destination).expanduser()
    elif target == "launchd":
        dest = Path.home() / "Library" / "LaunchAgents" / f"com.jarvis.{role}.plist"
    elif target == "systemd":
        dest = Path("/etc/systemd/system") / f"jarvis-{role}.service"
    else:
        raise ValueError(f"unsupported service platform: {target}")
    return ServicePaths(role=role, platform_name=target, destination=dest, log_dir=logs)


def render_service(
    role: str,
    *,
    platform_name: str | None = None,
    jarvis_bin: str | None = None,
    workdir: str | None = None,
    log_dir: str | None = None,
) -> str:
    _validate_role(role)
    target = platform_name or detect_platform()
    bin_path = jarvis_bin or default_jarvis_bin()
    cwd = str(Path(workdir).expanduser() if workdir else default_workdir())
    logs = str(Path(log_dir).expanduser() if log_dir else default_log_dir())
    args = ROLE_COMMANDS[role]
    if target == "launchd":
        arg_xml = "\n".join(f"    <string>{_xml_escape(a)}</string>" for a in [bin_path, *args])
        return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.jarvis.{role}</string>
  <key>WorkingDirectory</key>
  <string>{_xml_escape(cwd)}</string>
  <key>ProgramArguments</key>
  <array>
{arg_xml}
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
  </dict>
  <key>StandardOutPath</key>
  <string>{_xml_escape(logs)}/{role}.out.log</string>
  <key>StandardErrorPath</key>
  <string>{_xml_escape(logs)}/{role}.err.log</string>
</dict>
</plist>
"""
    if target == "systemd":
        command = " ".join([_shell_quote(bin_path), *map(_shell_quote, args)])
        return f"""[Unit]
Description=Jarvis {role}
After=network-online.target sound.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={cwd}
ExecStart={command}
Restart=always
RestartSec=3
Environment=PATH=/usr/local/bin:/usr/bin:/bin

[Install]
WantedBy=multi-user.target
"""
    raise ValueError(f"unsupported service platform: {target}")


def install_service(
    role: str,
    *,
    platform_name: str | None = None,
    jarvis_bin: str | None = None,
    workdir: str | None = None,
    log_dir: str | None = None,
    destination: str | None = None,
    dry_run: bool = False,
) -> tuple[Path, str]:
    paths = service_paths(
        role,
        platform_name=platform_name,
        destination=destination,
        log_dir=log_dir,
    )
    text = render_service(
        role,
        platform_name=paths.platform_name,
        jarvis_bin=jarvis_bin,
        workdir=workdir,
        log_dir=str(paths.log_dir),
    )
    if not dry_run:
        paths.log_dir.mkdir(parents=True, exist_ok=True)
        paths.destination.parent.mkdir(parents=True, exist_ok=True)
        paths.destination.write_text(text, encoding="utf-8")
    return paths.destination, text


def control_service(role: str, action: str, *, platform_name: str | None = None) -> subprocess.CompletedProcess[str]:
    _validate_role(role)
    target = platform_name or detect_platform()
    if target == "launchd":
        domain = f"gui/{os.getuid()}"
        label = f"com.jarvis.{role}"
        service = f"{domain}/{label}"
        plist = str(Path.home() / "Library" / "LaunchAgents" / f"{label}.plist")
        if action == "start":
            argv = ["launchctl", "bootstrap", domain, plist]
        elif action == "stop":
            argv = ["launchctl", "bootout", domain, plist]
        elif action == "restart":
            argv = ["launchctl", "kickstart", "-k", service]
        elif action == "status":
            argv = ["launchctl", "print", service]
        else:
            raise ValueError(f"unsupported service action: {action}")
    elif target == "systemd":
        unit = f"jarvis-{role}.service"
        verb = {"start": "start", "stop": "stop", "restart": "restart", "status": "status"}[action]
        argv = ["systemctl", verb, unit]
    else:
        raise ValueError(f"unsupported service platform: {target}")
    return subprocess.run(argv, capture_output=True, text=True, check=False)


def _validate_role(role: str) -> None:
    if role not in ROLES:
        raise ValueError(f"unknown role {role!r}; expected one of {', '.join(ROLES)}")


def _xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _shell_quote(value: str) -> str:
    if not value:
        return "''"
    if all(c.isalnum() or c in "/._-:" for c in value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"
