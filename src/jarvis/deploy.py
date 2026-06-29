"""Deployment helpers for packaged Jarvis installs.

The development checkout can still use `uv run ...`, but fleet installs need a
stable surface that the Mac app, Homebrew formula, and Pi installer can call
without reimplementing launchd/systemd details.
"""

from __future__ import annotations

import json
import os
import platform
import re
import secrets
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from jarvis import __version__


ROLES = ("brain", "intercom", "worker", "whatsapp")
ROLE_COMMANDS = {
    "brain": ["brain"],
    "intercom": ["run"],
    "worker": ["worker"],
    "whatsapp": ["whatsapp"],
}
ROLE_EXTRAS = {
    "brain": ["gateway", "tts", "stt", "vad", "wake", "memory", "mcp"],
    "intercom": ["stt", "vad", "wake"],
    "worker": ["worker", "browser"],
    "whatsapp": [],
}


@dataclass(frozen=True)
class ServicePaths:
    role: str
    platform_name: str
    destination: Path
    log_dir: Path


CommandRunner = Callable[[list[str], float], subprocess.CompletedProcess[str]]
Which = Callable[[str], str | None]


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


def uv_sync_args_for_roles(roles: list[str] | tuple[str, ...] | set[str]) -> list[str]:
    """Return the role-scoped dependency sync arguments for packaged installs."""
    args = ["sync", "--no-dev", "--inexact", "--no-install-project", "--no-editable"]
    for extra in role_extras(roles):
        args.extend(["--extra", extra])
    return args


def _find_uv() -> str:
    candidates = [
        os.environ.get("UV_BIN", ""),
        shutil.which("uv") or "",
        "/opt/homebrew/bin/uv",
        "/usr/local/bin/uv",
        "/usr/bin/uv",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    raise FileNotFoundError("uv not found; install Homebrew uv or set UV_BIN")


def sync_role_dependencies(
    roles: list[str] | tuple[str, ...] | set[str],
    *,
    cwd: str | Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Sync optional runtime dependencies needed by the selected roles."""
    args = uv_sync_args_for_roles(roles)
    env = os.environ.copy()
    if "UV_PYTHON" not in env:
        for candidate in (
            "/opt/homebrew/opt/python@3.12/bin/python3.12",
            "/usr/local/opt/python@3.12/bin/python3.12",
        ):
            if Path(candidate).is_file():
                env["UV_PYTHON"] = candidate
                break
    return subprocess.run(
        [_find_uv(), *args],
        cwd=cwd,
        env=env,
        check=False,
        text=True,
    )


def issue_pairing_entry(device_id: str, *, identity: str = "") -> tuple[str, str]:
    """Return a fresh token and a BRAIN_DEVICES JSON object fragment."""
    token = secrets.token_urlsafe(32)
    entry = {"token": token, "device_id": device_id}
    if identity:
        entry["identity"] = identity
    return token, json.dumps(entry, separators=(",", ":"))


def upsert_brain_device_entry(
    env_file: str | Path,
    entry_json: str,
    *,
    brain_bind_host: str = "",
) -> list[dict[str, str]]:
    """Upsert one BRAIN_DEVICES entry into a dotenv file and return all entries."""
    entry = _parse_brain_device_entry(entry_json)
    path = Path(env_file).expanduser()
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True) if path.exists() else []
    devices = _read_brain_devices(lines)
    devices = [
        device
        for device in devices
        if str(device.get("device_id", "")) != entry["device_id"]
    ]
    devices.append(entry)

    serialized = json.dumps(devices, separators=(",", ":"))
    updated_line = f"BRAIN_DEVICES={_dotenv_quote(serialized)}\n"
    updated = _replace_dotenv_key(lines, "BRAIN_DEVICES", updated_line)
    if brain_bind_host:
        updated = _replace_dotenv_key(
            updated, "BRAIN_HOST", f"BRAIN_HOST={_dotenv_quote(brain_bind_host)}\n"
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(updated), encoding="utf-8")
    path.chmod(0o600)
    return devices


def _parse_brain_device_entry(entry_json: str) -> dict[str, str]:
    try:
        raw = json.loads(entry_json)
    except json.JSONDecodeError as exc:
        raise ValueError("brain device entry must be valid JSON") from exc
    if not isinstance(raw, dict):
        raise ValueError("brain device entry must be a JSON object")
    token = raw.get("token")
    device_id = raw.get("device_id")
    if not isinstance(token, str) or not token:
        raise ValueError("brain device entry requires a token")
    if not isinstance(device_id, str) or not device_id:
        raise ValueError("brain device entry requires a device_id")
    entry = {"token": token, "device_id": device_id}
    identity = raw.get("identity")
    if isinstance(identity, str) and identity:
        entry["identity"] = identity
    return entry


def _read_brain_devices(lines: list[str]) -> list[dict[str, str]]:
    value = ""
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("#") or not stripped.startswith("BRAIN_DEVICES="):
            continue
        value = line.split("=", 1)[1].strip()
    if not value:
        return []
    try:
        raw = json.loads(_dotenv_unquote(value))
    except json.JSONDecodeError as exc:
        raise ValueError("existing BRAIN_DEVICES is not valid JSON") from exc
    if not isinstance(raw, list):
        raise ValueError("existing BRAIN_DEVICES must be a JSON array")
    devices: list[dict[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("existing BRAIN_DEVICES entries must be JSON objects")
        devices.append(_parse_brain_device_entry(json.dumps(item)))
    return devices


def _replace_dotenv_key(lines: list[str], key: str, new_line: str) -> list[str]:
    replaced = False
    updated: list[str] = []
    prefix = f"{key}="
    for line in lines:
        stripped = line.lstrip()
        if not stripped.startswith("#") and stripped.startswith(prefix):
            if not replaced:
                if updated and not updated[-1].endswith(("\n", "\r")):
                    updated[-1] += "\n"
                updated.append(new_line)
                replaced = True
            continue
        updated.append(line)
    if not replaced:
        if updated and not updated[-1].endswith(("\n", "\r")):
            updated[-1] += "\n"
        updated.append(new_line)
    return updated


def _dotenv_unquote(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        quote = text[0]
        text = text[1:-1]
        if quote == '"':
            text = text.replace('\\"', '"').replace("\\\\", "\\")
    return text


def current_release_ref() -> str:
    """Return the runtime release tag that matches this installed package."""
    return f"v{__version__}"


def render_pi_installer_command(
    *,
    device_id: str,
    token: str,
    brain_host: str,
    brain_port: str = "8700",
    repo: str = "roughcoder/jarvis",
    ref: str | None = None,
) -> str:
    """Return copy/paste commands for installing a paired Raspberry Pi intercom."""
    if not brain_host:
        raise ValueError("brain_host is required")

    runtime_ref = ref or current_release_ref()
    assignments = {
        "JARVIS_BRAIN_HOST": brain_host,
        "JARVIS_BRAIN_PORT": brain_port,
        "JARVIS_INTERCOM_TOKEN": token,
        "JARVIS_DEVICE_ID": device_id,
        "JARVIS_REPO": repo,
        "JARVIS_REF": runtime_ref,
    }
    env = " ".join(
        f"{key}={shlex.quote(value)}" for key, value in assignments.items() if value
    )
    return "\n".join(
        [
            "curl -fsSL https://raw.githubusercontent.com/"
            f"{shlex.quote(repo)}/{shlex.quote(runtime_ref)}/scripts/install_pi.sh "
            "-o /tmp/install_jarvis_pi.sh",
            f"sudo {env} bash /tmp/install_jarvis_pi.sh",
        ]
    )


def render_mac_config_command(
    *,
    device_id: str,
    token: str,
    brain_host: str,
    brain_port: str = "8700",
    identity: str = "",
    workdir: str = "$HOME/.jarvis",
) -> str:
    """Return copy/paste commands for configuring a paired Mac intercom."""
    if not brain_host:
        raise ValueError("brain_host is required")

    scope = "personal" if identity else "house"
    local_identity = identity or "house"
    env_values = {
        "INTERCOM_BRAIN_HOST": brain_host,
        "INTERCOM_BRAIN_PORT": brain_port,
        "INTERCOM_TOKEN": token,
        "CAPS_DEVICE_ID": device_id,
        "CAPS_IDENTITY": local_identity,
        "CAPS_SCOPE": scope,
    }
    managed_keys = "|".join(env_values)
    managed_block = "\n".join(
        f"{key}={_dotenv_quote(value)}" for key, value in env_values.items()
    )
    return "\n".join(
        [
            f'JARVIS_WORKDIR="${{JARVIS_WORKDIR:-{workdir}}}"',
            'mkdir -p "$JARVIS_WORKDIR"',
            'JARVIS_ENV_FILE="$JARVIS_WORKDIR/.env"',
            'JARVIS_TMP_FILE="$(mktemp)"',
            'touch "$JARVIS_ENV_FILE"',
            f"grep -v -E '^({managed_keys})=' "
            '"$JARVIS_ENV_FILE" > "$JARVIS_TMP_FILE" || true',
            'cat >> "$JARVIS_TMP_FILE" <<\'JARVIS_ENV\'',
            managed_block,
            "JARVIS_ENV",
            'mv "$JARVIS_TMP_FILE" "$JARVIS_ENV_FILE"',
            'chmod 0600 "$JARVIS_ENV_FILE"',
            'echo "Jarvis Mac pairing config written to $JARVIS_ENV_FILE"',
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
    env_file = str(Path(cwd) / ".env")
    args = ROLE_COMMANDS[role]
    if target == "launchd":
        arg_xml = "\n".join(
            f"    <string>{_xml_escape(a)}</string>" for a in [bin_path, *args]
        )
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
    <key>JARVIS_ENV_FILE</key>
    <string>{_xml_escape(env_file)}</string>
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
Environment=JARVIS_ENV_FILE={_systemd_escape(env_file)}

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
        if workdir:
            Path(workdir).expanduser().mkdir(parents=True, exist_ok=True)
        paths.log_dir.mkdir(parents=True, exist_ok=True)
        paths.destination.parent.mkdir(parents=True, exist_ok=True)
        paths.destination.write_text(text, encoding="utf-8")
    return paths.destination, text


def control_service(
    role: str, action: str, *, platform_name: str | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        service_control_argv(role, action, platform_name=platform_name),
        capture_output=True,
        text=True,
        check=False,
    )


def service_control_argv(
    role: str, action: str, *, platform_name: str | None = None
) -> list[str]:
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
        elif action == "enable":
            argv = ["launchctl", "enable", service]
        elif action == "disable":
            argv = ["launchctl", "disable", service]
        else:
            raise ValueError(f"unsupported service action: {action}")
    elif target == "systemd":
        unit = f"jarvis-{role}.service"
        verb = {
            "start": "start",
            "stop": "stop",
            "restart": "restart",
            "status": "status",
            "enable": "enable",
            "disable": "disable",
        }[action]
        argv = ["systemctl", verb, unit]
    else:
        raise ValueError(f"unsupported service platform: {target}")
    return argv


def collect_bringup_evidence(
    roles: list[str] | tuple[str, ...] | set[str],
    *,
    include_hardware: bool = False,
    platform_name: str | None = None,
    runner: CommandRunner | None = None,
    which: Which | None = None,
) -> dict[str, object]:
    """Collect read-only deployment evidence for a physical fleet bring-up."""
    ordered_roles = [role for role in ROLES if role in set(roles)]
    for role in roles:
        _validate_role(role)

    target = platform_name or detect_platform()
    run = runner or _run_command
    find = which or shutil.which
    evidence: dict[str, object] = {
        "jarvis_version": __version__,
        "release_ref": current_release_ref(),
        "platform": target,
        "roles": ordered_roles,
        "role_extras": role_extras(set(ordered_roles)),
        "jarvis_bin": default_jarvis_bin(),
        "packages": {},
        "services": {},
        "hardware": {},
    }

    packages: dict[str, object] = {}
    brew = find("brew")
    if brew:
        packages["jarvis"] = _command_report(
            [brew, "list", "--formula", "--versions", "jarvis"], runner=run
        )
        packages["jarvis-app"] = _command_report(
            [brew, "list", "--cask", "--versions", "jarvis-app"], runner=run
        )
    else:
        packages["brew"] = {"available": False, "reason": "brew not found"}
    evidence["packages"] = packages

    evidence["services"] = {
        role: _command_report(
            service_control_argv(role, "status", platform_name=target), runner=run
        )
        for role in ordered_roles
    }

    if include_hardware:
        evidence["hardware"] = _hardware_evidence(target, runner=run, which=find)
    return evidence


def summarize_bringup_evidence(
    path: str | Path,
    *,
    expected_roles: list[str] | tuple[str, ...] | set[str] = (),
    expected_version: str = "",
    expected_release_ref: str = "",
    min_files: int = 0,
) -> dict[str, object]:
    """Summarize redacted bring-up JSON files without copying raw command output."""
    for role in expected_roles:
        _validate_role(role)

    target = Path(path).expanduser()
    files = [target] if target.is_file() else sorted(target.glob("*.json"))
    entries: list[dict[str, object]] = []
    issues: list[str] = []
    roles_seen: set[str] = set()
    platforms_seen: set[str] = set()
    versions_seen: set[str] = set()
    release_refs_seen: set[str] = set()

    if not target.exists():
        issues.append(f"evidence path does not exist: {target}")
    if target.is_dir() and not files:
        issues.append(f"no JSON evidence files found in {target}")

    for file in files:
        try:
            data = json.loads(file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            issues.append(f"{file}: could not read evidence JSON: {exc}")
            continue
        if not isinstance(data, dict):
            issues.append(f"{file}: evidence root is not an object")
            continue
        if _is_bringup_summary(data):
            continue

        roles = [str(role) for role in data.get("roles", []) if isinstance(role, str)]
        roles_seen.update(roles)
        platform_name = str(data.get("platform", "unknown"))
        platforms_seen.add(platform_name)
        version = str(data.get("jarvis_version", "unknown"))
        versions_seen.add(version)
        release_ref = str(data.get("release_ref", "unknown"))
        release_refs_seen.add(release_ref)

        services = data.get("services", {})
        service_ok = _reports_ok(services)
        packages = data.get("packages", {})
        package_ok = _reports_ok(packages) or (
            platform_name == "systemd" and _only_missing_brew(packages)
        )
        hardware = data.get("hardware", {})
        hardware_checked = isinstance(hardware, dict) and bool(hardware)
        hardware_ok = _hardware_summary_ok(hardware)
        brain_status = data.get("brain_status", {})
        brain_checked = isinstance(brain_status, dict) and bool(brain_status)
        brain_paired = bool(brain_status.get("paired")) if isinstance(brain_status, dict) else False
        brain_reachable = bool(brain_status.get("reachable")) if isinstance(brain_status, dict) else False

        if not package_ok:
            issues.append(f"{file.name}: package checks need attention")
        if not service_ok:
            issues.append(f"{file.name}: service checks need attention")
        if "intercom" in roles and not brain_checked:
            issues.append(f"{file.name}: intercom evidence is missing brain pairing check")
        if brain_checked and not brain_paired:
            state = "reachable but unpaired" if brain_reachable else "unreachable"
            issues.append(f"{file.name}: brain check is {state}")

        entries.append(
            {
                "file": str(file),
                "platform": platform_name,
                "jarvis_version": version,
                "release_ref": release_ref,
                "roles": roles,
                "packages_ok": package_ok,
                "services_ok": service_ok,
                "hardware_checked": hardware_checked,
                "hardware_ok": hardware_ok,
                "brain_checked": brain_checked,
                "brain_reachable": brain_reachable,
                "brain_paired": brain_paired,
            }
        )

    for role in expected_roles:
        if role not in roles_seen:
            issues.append(f"missing expected role evidence: {role}")
    if min_files and len(entries) < min_files:
        issues.append(f"expected at least {min_files} evidence file(s), found {len(entries)}")
    if len(versions_seen) > 1:
        issues.append(
            "mixed Jarvis versions in evidence: " + ", ".join(sorted(versions_seen))
        )
    if expected_version:
        if not versions_seen:
            issues.append(f"missing expected Jarvis version evidence: {expected_version}")
        elif versions_seen != {expected_version}:
            issues.append(
                "expected Jarvis version "
                f"{expected_version}, found: {', '.join(sorted(versions_seen))}"
            )
    if len(release_refs_seen) > 1:
        issues.append(
            "mixed Jarvis release refs in evidence: " + ", ".join(sorted(release_refs_seen))
        )
    if expected_release_ref:
        if not release_refs_seen:
            issues.append(
                f"missing expected Jarvis release ref evidence: {expected_release_ref}"
            )
        elif release_refs_seen != {expected_release_ref}:
            issues.append(
                "expected Jarvis release ref "
                f"{expected_release_ref}, found: {', '.join(sorted(release_refs_seen))}"
            )

    return {
        "path": str(target),
        "ok": not issues,
        "file_count": len(entries),
        "expected_roles": list(expected_roles),
        "expected_version": expected_version,
        "expected_release_ref": expected_release_ref,
        "roles_seen": [role for role in ROLES if role in roles_seen],
        "platforms_seen": sorted(platforms_seen),
        "versions_seen": sorted(versions_seen),
        "release_refs_seen": sorted(release_refs_seen),
        "entries": entries,
        "issues": issues,
    }


def _reports_ok(value: object) -> bool:
    if not isinstance(value, dict) or not value:
        return False
    reports = [report for report in value.values() if isinstance(report, dict)]
    return bool(reports) and all(bool(report.get("ok")) for report in reports)


def _only_missing_brew(value: object) -> bool:
    if not isinstance(value, dict) or set(value) != {"brew"}:
        return False
    report = value.get("brew")
    return isinstance(report, dict) and report.get("available") is False


def _is_bringup_summary(value: dict[str, object]) -> bool:
    return (
        "entries" in value
        and "file_count" in value
        and "issues" in value
        and "roles_seen" in value
        and "roles" not in value
    )


def _hardware_summary_ok(value: object) -> bool:
    if not isinstance(value, dict) or not value:
        return False
    reports = [report for report in value.values() if isinstance(report, dict)]
    if not reports:
        return False
    required = [
        report
        for name, report in value.items()
        if isinstance(report, dict) and name in {"audio", "microphones", "speakers"}
    ]
    checked = required or reports
    return all(bool(report.get("ok")) for report in checked)


def _validate_role(role: str) -> None:
    if role not in ROLES:
        raise ValueError(f"unknown role {role!r}; expected one of {', '.join(ROLES)}")


def _run_command(argv: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


def _command_report(
    argv: list[str],
    *,
    runner: CommandRunner,
    timeout: float = 15.0,
) -> dict[str, object]:
    try:
        result = runner(argv, timeout)
    except FileNotFoundError as exc:
        return {
            "argv": argv,
            "available": False,
            "returncode": 127,
            "stdout": "",
            "stderr": str(exc),
            "ok": False,
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "argv": argv,
            "available": True,
            "returncode": 124,
            "stdout": _redact_text((exc.stdout or "") if isinstance(exc.stdout, str) else ""),
            "stderr": _redact_text((exc.stderr or "") if isinstance(exc.stderr, str) else "command timed out"),
            "ok": False,
        }

    return {
        "argv": argv,
        "available": True,
        "returncode": result.returncode,
        "stdout": _redact_text(result.stdout),
        "stderr": _redact_text(result.stderr),
        "ok": result.returncode == 0,
    }


def _hardware_evidence(
    platform_name: str,
    *,
    runner: CommandRunner,
    which: Which,
) -> dict[str, object]:
    checks: dict[str, list[str]] = {}
    if platform_name == "systemd":
        camera_tool = "rpicam-hello" if which("rpicam-hello") else "libcamera-hello"
        checks = {
            "microphones": ["arecord", "-l"],
            "speakers": ["aplay", "-l"],
            "cameras": [camera_tool, "--list-cameras"],
            "display": [
                "sh",
                "-c",
                "if command -v vcgencmd >/dev/null 2>&1; then vcgencmd display_power; "
                "elif [ -e /dev/fb0 ]; then echo 'framebuffer: /dev/fb0 present'; "
                "elif ls /dev/dri/card* >/dev/null 2>&1; then ls -1 /dev/dri/card*; "
                "else echo 'display: no framebuffer or DRM card detected' >&2; exit 1; fi",
            ],
        }
    elif platform_name == "launchd":
        checks = {
            "audio": ["system_profiler", "SPAudioDataType"],
            "cameras": ["system_profiler", "SPCameraDataType"],
        }

    out: dict[str, object] = {}
    for name, argv in checks.items():
        if which(argv[0]):
            out[name] = _command_report(argv, runner=runner, timeout=25.0)
        else:
            out[name] = {
                "argv": argv,
                "available": False,
                "returncode": 127,
                "stdout": "",
                "stderr": f"{argv[0]} not found",
                "ok": False,
            }
    return out


def _redact_text(value: str, *, limit: int = 4000) -> str:
    text = value[:limit]
    patterns = [
        re.compile(r"(?i)(token|secret|password|api[_-]?key|authorization)(\s*[:=]\s*)([^\s,}]+)"),
        re.compile(r'(?i)("(?:token|secret|password|api[_-]?key|authorization)"\s*:\s*)"[^"]*"'),
    ]
    for pattern in patterns:
        if pattern.pattern.startswith('(?i)("'):
            text = pattern.sub('\\1"[redacted]"', text)
        else:
            text = pattern.sub(r"\1\2[redacted]", text)
    if len(value) > limit:
        text += "\n[truncated]"
    return text


def _xml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _systemd_escape(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _dotenv_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _shell_quote(value: str) -> str:
    if not value:
        return "''"
    if all(c.isalnum() or c in "/._-:" for c in value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"
