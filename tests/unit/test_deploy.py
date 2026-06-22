from __future__ import annotations

import json

import pytest

from jarvis.deploy import (
    current_release_ref,
    issue_pairing_entry,
    render_mac_config_command,
    render_pi_installer_command,
    render_service,
    role_extras,
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
