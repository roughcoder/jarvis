import json

from jarvis.cli import main


def test_bringup_json_uses_selected_roles_and_brain_probe(monkeypatch, capsys) -> None:
    seen = {}

    def fake_collect(roles, **kwargs):  # noqa: ANN001, ANN202
        seen["roles"] = roles
        seen["kwargs"] = kwargs
        return {
            "jarvis_version": "0.1.test",
            "release_ref": "v0.1.test",
            "platform": kwargs["platform_name"],
            "roles": roles,
            "role_extras": ["worker", "browser"],
            "packages": {},
            "services": {},
            "hardware": {},
        }

    async def fake_probe(cfg):  # noqa: ANN001, ANN202
        seen["brain_host"] = cfg.intercom.brain_host
        seen["brain_port"] = cfg.intercom.brain_port
        return {"reachable": True, "paired": True, "identity": "neil"}

    monkeypatch.setattr("jarvis.deploy.collect_bringup_evidence", fake_collect)
    monkeypatch.setattr("jarvis.fleet.probe_brain", fake_probe)

    code = main(
        [
            "bringup",
            "--json",
            "--role",
            "worker",
            "--platform",
            "launchd",
            "--hardware",
            "--brain-host",
            "imac.private",
            "--brain-port",
            "8701",
        ]
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert code == 0
    assert output.err == ""
    assert seen["roles"] == ["worker"]
    assert seen["kwargs"] == {"include_hardware": True, "platform_name": "launchd"}
    assert seen["brain_host"] == "imac.private"
    assert seen["brain_port"] == 8701
    assert payload["brain_status"]["paired"] is True
    assert payload["roles"] == ["worker"]


def test_bringup_output_writes_timestamped_evidence_file(
    monkeypatch, capsys, tmp_path
) -> None:
    def fake_collect(roles, **kwargs):  # noqa: ANN001, ANN202
        return {
            "jarvis_version": "0.1.test",
            "release_ref": "v0.1.test",
            "platform": kwargs["platform_name"],
            "roles": roles,
            "role_extras": ["worker", "browser"],
            "packages": {"jarvis": {"stdout": "token=[redacted]", "ok": True}},
            "services": {},
            "hardware": {},
        }

    monkeypatch.setattr("jarvis.deploy.collect_bringup_evidence", fake_collect)

    code = main(
        [
            "bringup",
            "--json",
            "--role",
            "worker",
            "--platform",
            "launchd",
            "--output",
            str(tmp_path),
        ]
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)
    evidence_path = tmp_path / payload["evidence_path"].split("/")[-1]
    saved = json.loads(evidence_path.read_text(encoding="utf-8"))

    assert code == 0
    assert output.err == ""
    assert payload["evidence_path"] == str(evidence_path)
    assert saved == payload
    assert evidence_path.name.startswith("jarvis-bringup-")
    assert evidence_path.name.endswith(".json")
