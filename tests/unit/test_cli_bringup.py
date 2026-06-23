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


def test_bringup_summary_json_returns_nonzero_for_missing_role(capsys, tmp_path) -> None:
    (tmp_path / "imac.json").write_text(
        json.dumps(
            {
                "jarvis_version": "0.1.test",
                "release_ref": "v0.1.test",
                "platform": "launchd",
                "roles": ["brain"],
                "packages": {"jarvis": {"ok": True}, "jarvis-app": {"ok": True}},
                "services": {"brain": {"ok": True}},
                "hardware": {},
            }
        ),
        encoding="utf-8",
    )

    code = main(
        [
            "bringup-summary",
            str(tmp_path),
            "--json",
            "--expect-role",
            "brain",
            "--expect-role",
            "worker",
            "--min-files",
            "2",
        ]
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert code == 1
    assert output.err == ""
    assert payload["ok"] is False
    assert "missing expected role evidence: worker" in payload["issues"]
    assert "expected at least 2 evidence file(s), found 1" in payload["issues"]


def test_bringup_summary_output_writes_summary_file(capsys, tmp_path) -> None:
    (tmp_path / "imac.json").write_text(
        json.dumps(
            {
                "jarvis_version": "0.1.test",
                "release_ref": "v0.1.test",
                "platform": "launchd",
                "roles": ["brain", "worker", "intercom"],
                "packages": {"jarvis": {"ok": True}, "jarvis-app": {"ok": True}},
                "services": {
                    "brain": {"ok": True},
                    "worker": {"ok": True},
                    "intercom": {"ok": True},
                },
                "hardware": {"audio": {"ok": True}},
            }
        ),
        encoding="utf-8",
    )

    code = main(
        [
            "bringup-summary",
            str(tmp_path),
            "--json",
            "--expect-role",
            "brain",
            "--expect-role",
            "worker",
            "--expect-role",
            "intercom",
            "--min-files",
            "1",
            "--output",
            str(tmp_path),
        ]
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)
    summary_path = tmp_path / "jarvis-fleet-summary.json"
    saved = json.loads(summary_path.read_text(encoding="utf-8"))

    assert code == 0
    assert payload["summary_path"] == str(summary_path)
    assert saved == payload
    assert saved["file_count"] == 1

    code = main(["bringup-summary", str(tmp_path), "--json", "--min-files", "1"])
    repeated = json.loads(capsys.readouterr().out)

    assert code == 0
    assert repeated["file_count"] == 1
