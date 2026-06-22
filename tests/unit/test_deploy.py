from __future__ import annotations

import json

import pytest

from jarvis.deploy import (
    issue_pairing_entry,
    render_pi_installer_command,
    render_service,
    role_extras,
)


def test_role_extras_are_ordered_and_deduplicated() -> None:
    assert role_extras({"worker", "intercom"}) == [
        "stt",
        "vad",
        "wake",
        "worker",
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


def test_render_systemd_service_for_pi_intercom() -> None:
    text = render_service(
        "intercom",
        platform_name="systemd",
        jarvis_bin="/usr/local/bin/jarvis",
        workdir="/opt/jarvis",
    )

    assert "ExecStart=/usr/local/bin/jarvis run" in text
    assert "After=network-online.target sound.target" in text


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


def test_render_pi_installer_command_requires_brain_host() -> None:
    with pytest.raises(ValueError, match="brain_host"):
        render_pi_installer_command(
            device_id="kitchen-pi", token="token", brain_host=""
        )
