import json

from jarvis.cli import main
from jarvis.deploy import current_release_ref


def test_pair_json_can_include_pi_installer_command(capsys) -> None:
    code = main(
        [
            "pair",
            "kitchen-pi",
            "--json",
            "--pi-installer",
            "--brain-host",
            "imac.private",
            "--ref",
            "v0.1.0",
        ]
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)
    entry = json.loads(payload["brain_devices_entry"])

    assert code == 0
    assert output.err == ""
    assert payload["token"] == entry["token"]
    assert entry["device_id"] == "kitchen-pi"
    assert (
        "curl -fsSL https://raw.githubusercontent.com/roughcoder/jarvis/v0.1.0/scripts/install_pi.sh"
        in payload["pi_installer_command"]
    )
    assert (
        f"JARVIS_INTERCOM_TOKEN={payload['token']}" in payload["pi_installer_command"]
    )
    assert "JARVIS_DEVICE_ID=kitchen-pi" in payload["pi_installer_command"]


def test_pair_json_can_include_mac_config_command(capsys) -> None:
    code = main(
        [
            "pair",
            "neil-laptop",
            "--json",
            "--mac-config",
            "--brain-host",
            "imac.private",
            "--identity",
            "neil",
        ]
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)
    entry = json.loads(payload["brain_devices_entry"])

    assert code == 0
    assert output.err == ""
    assert payload["token"] == entry["token"]
    assert entry["device_id"] == "neil-laptop"
    assert entry["identity"] == "neil"
    assert 'INTERCOM_BRAIN_HOST="imac.private"' in payload["mac_config_command"]
    assert f'INTERCOM_TOKEN="{payload["token"]}"' in payload["mac_config_command"]
    assert 'CAPS_DEVICE_ID="neil-laptop"' in payload["mac_config_command"]
    assert 'CAPS_IDENTITY="neil"' in payload["mac_config_command"]
    assert 'CAPS_SCOPE="personal"' in payload["mac_config_command"]


def test_pair_json_pi_installer_defaults_to_current_release_ref(capsys) -> None:
    code = main(
        [
            "pair",
            "kitchen-pi",
            "--json",
            "--pi-installer",
            "--brain-host",
            "imac.private",
        ]
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)
    release_ref = current_release_ref()

    assert code == 0
    assert output.err == ""
    assert (
        f"https://raw.githubusercontent.com/roughcoder/jarvis/{release_ref}/scripts/install_pi.sh"
        in payload["pi_installer_command"]
    )
    assert f"JARVIS_REF={release_ref}" in payload["pi_installer_command"]
    assert "JARVIS_REF=main" not in payload["pi_installer_command"]


def test_pair_json_pi_installer_requires_brain_host(capsys) -> None:
    code = main(["pair", "kitchen-pi", "--json", "--pi-installer"])

    output = capsys.readouterr()

    assert code == 2
    assert output.out == ""
    assert "--brain-host is required with --pi-installer or --mac-config" in output.err


def test_pair_json_mac_config_requires_brain_host(capsys) -> None:
    code = main(["pair", "neil-laptop", "--json", "--mac-config"])

    output = capsys.readouterr()

    assert code == 2
    assert output.out == ""
    assert "--brain-host is required with --pi-installer or --mac-config" in output.err
