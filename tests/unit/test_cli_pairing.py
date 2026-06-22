import json

from jarvis.cli import main


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


def test_pair_json_pi_installer_requires_brain_host(capsys) -> None:
    code = main(["pair", "kitchen-pi", "--json", "--pi-installer"])

    output = capsys.readouterr()

    assert code == 2
    assert output.out == ""
    assert "--brain-host is required with --pi-installer" in output.err
