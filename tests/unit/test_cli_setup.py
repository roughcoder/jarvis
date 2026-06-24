import json

from jarvis.cli import main


def test_whatsapp_auth_accepts_json_flag(capsys) -> None:
    code = main(["whatsapp-auth", "--json", "--wacli-bin", "/usr/bin/false"])

    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert code == 1
    assert output.err == ""
    assert payload["ok"] is False
    assert payload["argv"] == ["/usr/bin/false", "auth", "--qr-format", "text"]
