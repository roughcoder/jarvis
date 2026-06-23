from __future__ import annotations

import json
import subprocess

import pytest

from jarvis.deploy import (
    collect_bringup_evidence,
    current_release_ref,
    issue_pairing_entry,
    render_mac_config_command,
    render_pi_installer_command,
    render_service,
    role_extras,
    service_control_argv,
    summarize_bringup_evidence,
    upsert_brain_device_entry,
    uv_sync_args_for_roles,
)


def test_role_extras_are_ordered_and_deduplicated() -> None:
    assert role_extras({"worker", "intercom"}) == [
        "stt",
        "vad",
        "wake",
        "worker",
        "browser",
    ]


def test_uv_sync_args_for_roles_are_packaged_install_safe() -> None:
    assert uv_sync_args_for_roles({"worker", "intercom"}) == [
        "sync",
        "--no-dev",
        "--inexact",
        "--no-install-project",
        "--no-editable",
        "--extra",
        "stt",
        "--extra",
        "vad",
        "--extra",
        "wake",
        "--extra",
        "worker",
        "--extra",
        "browser",
    ]
    assert role_extras({"brain", "worker"}) == [
        "gateway",
        "tts",
        "stt",
        "vad",
        "wake",
        "memory",
        "mcp",
        "worker",
        "browser",
    ]


def test_render_launchd_service_uses_jarvis_command_not_uv() -> None:
    text = render_service(
        "brain",
        platform_name="launchd",
        jarvis_bin="/opt/homebrew/bin/jarvis",
        workdir="/opt/homebrew/var/jarvis",
        log_dir="/Users/example/Library/Logs/Jarvis",
    )

    assert "<string>/opt/homebrew/bin/jarvis</string>" in text
    assert "<string>brain</string>" in text
    assert "uv" not in text
    assert "com.jarvis.brain" in text
    assert "<key>JARVIS_ENV_FILE</key>" in text
    assert "<string>/opt/homebrew/var/jarvis/.env</string>" in text


def test_render_systemd_service_for_pi_intercom() -> None:
    text = render_service(
        "intercom",
        platform_name="systemd",
        jarvis_bin="/usr/local/bin/jarvis",
        workdir="/opt/jarvis",
    )

    assert "ExecStart=/usr/local/bin/jarvis run" in text
    assert "After=network-online.target sound.target" in text
    assert 'Environment=JARVIS_ENV_FILE="/opt/jarvis/.env"' in text


def test_service_control_argv_uses_platform_supervisors() -> None:
    assert service_control_argv("intercom", "status", platform_name="systemd") == [
        "systemctl",
        "status",
        "jarvis-intercom.service",
    ]
    assert service_control_argv("worker", "restart", platform_name="launchd")[:3] == [
        "launchctl",
        "kickstart",
        "-k",
    ]


def test_collect_bringup_evidence_redacts_and_filters_roles() -> None:
    calls: list[list[str]] = []

    def fake_runner(
        argv: list[str], _timeout: float
    ) -> subprocess.CompletedProcess[str]:
        calls.append(argv)
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout='token="secret-value"\n{"password":"hidden-value"}\njarvis 0.1.8\n',
            stderr="",
        )

    def fake_which(name: str) -> str | None:
        return (
            f"/usr/bin/{name}"
            if name in {"brew", "arecord", "aplay", "rpicam-hello", "sh"}
            else None
        )

    data = collect_bringup_evidence(
        ["intercom"],
        include_hardware=True,
        platform_name="systemd",
        runner=fake_runner,
        which=fake_which,
    )

    assert data["roles"] == ["intercom"]
    assert data["role_extras"] == ["stt", "vad", "wake"]
    assert set(data["services"]) == {"intercom"}
    assert data["packages"]["jarvis"]["ok"] is True
    assert "secret-value" not in json.dumps(data)
    assert "hidden-value" not in json.dumps(data)
    assert '"password":"[redacted]"' in data["packages"]["jarvis"]["stdout"]
    assert "systemctl" in calls[2]
    assert data["hardware"]["microphones"]["ok"] is True
    assert data["hardware"]["speakers"]["ok"] is True
    assert data["hardware"]["cameras"]["argv"] == ["rpicam-hello", "--list-cameras"]
    assert data["hardware"]["cameras"]["ok"] is True
    assert data["hardware"]["display"]["ok"] is True


def test_summarize_bringup_evidence_flags_missing_expected_roles(tmp_path) -> None:
    (tmp_path / "imac.json").write_text(
        json.dumps(
            {
                "jarvis_version": "0.1.test",
                "release_ref": "v0.1.test",
                "platform": "launchd",
                "roles": ["brain", "worker"],
                "packages": {
                    "jarvis": {"ok": True},
                    "jarvis-app": {"ok": True},
                },
                "services": {
                    "brain": {"ok": True},
                    "worker": {"ok": True},
                },
                "hardware": {"audio": {"ok": True}},
            }
        ),
        encoding="utf-8",
    )

    summary = summarize_bringup_evidence(
        tmp_path,
        expected_roles=["brain", "worker", "intercom"],
        min_files=2,
    )

    assert summary["ok"] is False
    assert summary["file_count"] == 1
    assert summary["roles_seen"] == ["brain", "worker"]
    assert "missing expected role evidence: intercom" in summary["issues"]
    assert "expected at least 2 evidence file(s), found 1" in summary["issues"]


def test_summarize_bringup_evidence_accepts_pi_without_brew(tmp_path) -> None:
    (tmp_path / "pi.json").write_text(
        json.dumps(
            {
                "jarvis_version": "0.1.test",
                "release_ref": "v0.1.test",
                "platform": "systemd",
                "roles": ["intercom"],
                "packages": {"brew": {"available": False, "reason": "brew not found"}},
                "services": {"intercom": {"ok": True}},
                "hardware": {
                    "microphones": {"ok": True},
                    "speakers": {"ok": True},
                    "cameras": {"available": False, "ok": False},
                },
                "brain_status": {"reachable": True, "paired": True},
            }
        ),
        encoding="utf-8",
    )

    summary = summarize_bringup_evidence(
        tmp_path,
        expected_roles=["intercom"],
        min_files=1,
    )

    assert summary["ok"] is True
    assert summary["entries"][0]["packages_ok"] is True
    assert summary["entries"][0]["hardware_ok"] is True
    assert summary["entries"][0]["brain_paired"] is True


def test_summarize_bringup_evidence_requires_intercom_brain_check(tmp_path) -> None:
    (tmp_path / "laptop.json").write_text(
        json.dumps(
            {
                "jarvis_version": "0.1.test",
                "release_ref": "v0.1.test",
                "platform": "launchd",
                "roles": ["intercom", "worker"],
                "packages": {
                    "jarvis": {"ok": True},
                    "jarvis-app": {"ok": True},
                },
                "services": {
                    "intercom": {"ok": True},
                    "worker": {"ok": True},
                },
                "hardware": {"audio": {"ok": True}},
            }
        ),
        encoding="utf-8",
    )

    summary = summarize_bringup_evidence(tmp_path)

    assert summary["ok"] is False
    assert (
        "laptop.json: intercom evidence is missing brain pairing check"
        in summary["issues"]
    )


def test_summarize_bringup_evidence_ignores_previous_summary_files(tmp_path) -> None:
    (tmp_path / "imac.json").write_text(
        json.dumps(
            {
                "jarvis_version": "0.1.test",
                "release_ref": "v0.1.test",
                "platform": "launchd",
                "roles": ["brain"],
                "packages": {"jarvis": {"ok": True}},
                "services": {"brain": {"ok": True}},
                "hardware": {},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "jarvis-fleet-summary.json").write_text(
        json.dumps(
            {
                "path": str(tmp_path),
                "ok": True,
                "file_count": 1,
                "roles_seen": ["brain"],
                "entries": [],
                "issues": [],
            }
        ),
        encoding="utf-8",
    )

    summary = summarize_bringup_evidence(tmp_path)

    assert summary["file_count"] == 1
    assert summary["roles_seen"] == ["brain"]


def test_summarize_bringup_evidence_flags_mixed_versions_and_refs(tmp_path) -> None:
    (tmp_path / "imac.json").write_text(
        json.dumps(
            {
                "jarvis_version": "0.1.16",
                "release_ref": "v0.1.16",
                "platform": "launchd",
                "roles": ["brain", "worker"],
                "packages": {"jarvis": {"ok": True}, "jarvis-app": {"ok": True}},
                "services": {"brain": {"ok": True}, "worker": {"ok": True}},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "pi.json").write_text(
        json.dumps(
            {
                "jarvis_version": "0.1.15",
                "release_ref": "v0.1.15",
                "platform": "systemd",
                "roles": ["intercom"],
                "packages": {"brew": {"available": False, "reason": "brew not found"}},
                "services": {"intercom": {"ok": True}},
                "brain_status": {"reachable": True, "paired": True},
            }
        ),
        encoding="utf-8",
    )

    summary = summarize_bringup_evidence(tmp_path)

    assert summary["ok"] is False
    assert summary["versions_seen"] == ["0.1.15", "0.1.16"]
    assert summary["release_refs_seen"] == ["v0.1.15", "v0.1.16"]
    assert "mixed Jarvis versions in evidence: 0.1.15, 0.1.16" in summary["issues"]
    assert "mixed Jarvis release refs in evidence: v0.1.15, v0.1.16" in summary["issues"]


def test_summarize_bringup_evidence_flags_unexpected_version_and_ref(tmp_path) -> None:
    (tmp_path / "imac.json").write_text(
        json.dumps(
            {
                "jarvis_version": "0.1.16",
                "release_ref": "v0.1.16",
                "platform": "launchd",
                "roles": ["brain", "worker"],
                "packages": {"jarvis": {"ok": True}, "jarvis-app": {"ok": True}},
                "services": {"brain": {"ok": True}, "worker": {"ok": True}},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "pi.json").write_text(
        json.dumps(
            {
                "jarvis_version": "0.1.16",
                "release_ref": "v0.1.16",
                "platform": "systemd",
                "roles": ["intercom"],
                "packages": {"brew": {"available": False, "reason": "brew not found"}},
                "services": {"intercom": {"ok": True}},
                "brain_status": {"reachable": True, "paired": True},
            }
        ),
        encoding="utf-8",
    )

    summary = summarize_bringup_evidence(
        tmp_path,
        expected_version="0.1.17",
        expected_release_ref="v0.1.17",
    )

    assert summary["ok"] is False
    assert summary["versions_seen"] == ["0.1.16"]
    assert summary["release_refs_seen"] == ["v0.1.16"]
    assert "expected Jarvis version 0.1.17, found: 0.1.16" in summary["issues"]
    assert "expected Jarvis release ref v0.1.17, found: v0.1.16" in summary["issues"]


def test_issue_pairing_entry_returns_brain_devices_fragment() -> None:
    token, fragment = issue_pairing_entry("kitchen-pi")
    entry = json.loads(fragment)

    assert len(token) >= 32
    assert entry == {"token": token, "device_id": "kitchen-pi"}


def test_issue_pairing_entry_json_escapes_values() -> None:
    token, fragment = issue_pairing_entry('kitchen "pi"', identity='Neil "home"')
    entry = json.loads(fragment)

    assert entry == {
        "token": token,
        "device_id": 'kitchen "pi"',
        "identity": 'Neil "home"',
    }


def test_upsert_brain_device_entry_writes_env_file(tmp_path) -> None:
    env_file = tmp_path / ".env"
    entry = '{"token":"tok","device_id":"kitchen-pi","identity":"house"}'

    devices = upsert_brain_device_entry(env_file, entry)

    assert devices == [
        {"token": "tok", "device_id": "kitchen-pi", "identity": "house"}
    ]
    text = env_file.read_text(encoding="utf-8")
    assert text.startswith("BRAIN_DEVICES=")
    value = text.split("=", 1)[1].strip().strip('"').replace('\\"', '"')
    assert json.loads(value) == devices
    assert oct(env_file.stat().st_mode & 0o777) == "0o600"


def test_upsert_brain_device_entry_can_set_bind_host(tmp_path) -> None:
    env_file = tmp_path / ".env"

    devices = upsert_brain_device_entry(
        env_file,
        '{"token":"tok","device_id":"kitchen-pi"}',
        brain_bind_host="0.0.0.0",
    )

    text = env_file.read_text(encoding="utf-8")
    assert devices == [{"token": "tok", "device_id": "kitchen-pi"}]
    assert 'BRAIN_HOST="0.0.0.0"\n' in text
    assert "BRAIN_DEVICES=" in text


def test_upsert_brain_device_entry_preserves_other_env_and_replaces_device(
    tmp_path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "BRAIN_HOST=0.0.0.0",
                'BRAIN_DEVICES="[{\\"token\\":\\"old\\",\\"device_id\\":\\"kitchen-pi\\"},{\\"token\\":\\"mac\\",\\"device_id\\":\\"laptop\\"}]"',
                "MEMORY_HOST=localhost",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    devices = upsert_brain_device_entry(
        env_file, '{"token":"new","device_id":"kitchen-pi"}'
    )

    assert devices == [
        {"token": "mac", "device_id": "laptop"},
        {"token": "new", "device_id": "kitchen-pi"},
    ]
    text = env_file.read_text(encoding="utf-8")
    assert "BRAIN_HOST=0.0.0.0\n" in text
    assert "MEMORY_HOST=localhost\n" in text
    assert text.count("BRAIN_DEVICES=") == 1


def test_render_pi_installer_command_quotes_pairing_values() -> None:
    command = render_pi_installer_command(
        device_id="kitchen pi",
        token="tok en",
        brain_host="imac.private",
        brain_port="8701",
        repo="roughcoder/jarvis",
        ref="v0.1.0",
    )

    assert (
        "curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/v0.1.0/scripts/install_pi.sh"
        in command
    )
    assert "sudo JARVIS_BRAIN_HOST=imac.private JARVIS_BRAIN_PORT=8701" in command
    assert "JARVIS_INTERCOM_TOKEN='tok en'" in command
    assert "JARVIS_DEVICE_ID='kitchen pi'" in command


def test_render_pi_installer_command_defaults_to_current_release_ref() -> None:
    command = render_pi_installer_command(
        device_id="kitchen-pi",
        token="token",
        brain_host="imac.private",
    )

    release_ref = current_release_ref()
    assert (
        f"https://raw.githubusercontent.com/roughcoder/jarvis/{release_ref}/scripts/install_pi.sh"
        in command
    )
    assert f"JARVIS_REF={release_ref}" in command
    assert "JARVIS_REF=main" not in command


def test_render_mac_config_command_upserts_service_env_values() -> None:
    command = render_mac_config_command(
        device_id="neil laptop",
        token="tok en",
        brain_host="imac.private",
        brain_port="8701",
        identity="neil",
        workdir="$HOME/.jarvis",
    )

    assert 'JARVIS_WORKDIR="${JARVIS_WORKDIR:-$HOME/.jarvis}"' in command
    assert 'JARVIS_ENV_FILE="$JARVIS_WORKDIR/.env"' in command
    assert "grep -v -E '^(INTERCOM_BRAIN_HOST|INTERCOM_BRAIN_PORT|INTERCOM_TOKEN|CAPS_DEVICE_ID|CAPS_IDENTITY|CAPS_SCOPE)='" in command
    assert 'INTERCOM_BRAIN_HOST="imac.private"' in command
    assert 'INTERCOM_BRAIN_PORT="8701"' in command
    assert 'INTERCOM_TOKEN="tok en"' in command
    assert 'CAPS_DEVICE_ID="neil laptop"' in command
    assert 'CAPS_IDENTITY="neil"' in command
    assert 'CAPS_SCOPE="personal"' in command
    assert 'chmod 0600 "$JARVIS_ENV_FILE"' in command


def test_render_mac_config_command_defaults_shared_device_identity() -> None:
    command = render_mac_config_command(
        device_id="kitchen-mac",
        token="token",
        brain_host="imac.private",
    )

    assert 'CAPS_IDENTITY="house"' in command
    assert 'CAPS_SCOPE="house"' in command


def test_render_pi_installer_command_requires_brain_host() -> None:
    with pytest.raises(ValueError, match="brain_host"):
        render_pi_installer_command(
            device_id="kitchen-pi", token="token", brain_host=""
        )


def test_render_mac_config_command_requires_brain_host() -> None:
    with pytest.raises(ValueError, match="brain_host"):
        render_mac_config_command(
            device_id="neil-laptop", token="token", brain_host=""
        )
