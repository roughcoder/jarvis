import json

from jarvis.cli import main


def test_status_json_accepts_brain_host_override(monkeypatch, capsys) -> None:
    seen = {}

    async def fake_probe(cfg):
        seen["brain_host"] = cfg.intercom.brain_host
        seen["brain_port"] = cfg.intercom.brain_port
        return {"reachable": True, "paired": False, "error": "missing token"}

    monkeypatch.setattr("jarvis.fleet.probe_brain", fake_probe)

    code = main(
        [
            "status",
            "--json",
            "--brain-host",
            "imac.private",
            "--brain-port",
            "8701",
        ]
    )

    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert code == 1
    assert output.err == ""
    assert seen == {"brain_host": "imac.private", "brain_port": 8701}
    assert payload["brain_url"] == "ws://imac.private:8701"
    assert payload["reachable"] is True
    assert payload["paired"] is False
