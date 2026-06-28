import json
from types import SimpleNamespace

from jarvis.config import AccountConfig, GoogleConfig
from jarvis.cli import main


def test_whatsapp_auth_accepts_json_flag(capsys) -> None:
    code = main(["whatsapp-auth", "--json", "--wacli-bin", "/usr/bin/false"])

    output = capsys.readouterr()
    payload = json.loads(output.out)

    assert code == 1
    assert output.err == ""
    assert payload["ok"] is False
    assert payload["argv"] == ["/usr/bin/false", "auth", "--qr-format", "text"]


def test_google_setup_creates_default_house_bindings(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    calls: list[list[str]] = []

    def fake_run(argv: list[str]):  # noqa: ANN202
        calls.append(argv)
        return SimpleNamespace(returncode=0)

    cfg = SimpleNamespace(
        google=GoogleConfig(_env_file=None, gogcli_bin="gog"),
        accounts=AccountConfig(_env_file=None, bindings_dir=str(tmp_path / ".accounts")),
    )
    monkeypatch.setattr("jarvis.cli.load_config", lambda: cfg)
    monkeypatch.setattr("shutil.which", lambda _bin: "/usr/bin/gog")
    monkeypatch.setattr("subprocess.run", fake_run)

    code = main(["google-setup", "--account", "house"])

    assert code == 0
    assert calls == [["gog", "--account", "house", "auth", "login"]]
    email = json.loads((tmp_path / ".accounts" / "house" / "house-email.json").read_text(encoding="utf-8"))
    calendar = json.loads((tmp_path / ".accounts" / "house" / "house-calendar.json").read_text(encoding="utf-8"))
    assert email == {
        "account": "house",
        "grants": ["email.read", "email.draft", "email.send"],
        "kind": "email",
        "provider": "gogcli",
    }
    assert calendar == {
        "account": "house",
        "calendar_id": "primary",
        "grants": ["calendar.freebusy", "calendar.read"],
        "kind": "calendar",
        "provider": "gogcli",
    }


def test_google_setup_preserves_existing_bindings(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    root = tmp_path / ".accounts" / "house"
    root.mkdir(parents=True)
    existing = root / "house-email.json"
    existing.write_text('{"kind":"email","provider":"gogcli","account":"custom"}\n', encoding="utf-8")

    cfg = SimpleNamespace(
        google=GoogleConfig(_env_file=None, gogcli_bin="gog"),
        accounts=AccountConfig(_env_file=None, bindings_dir=str(tmp_path / ".accounts")),
    )
    monkeypatch.setattr("jarvis.cli.load_config", lambda: cfg)
    monkeypatch.setattr("shutil.which", lambda _bin: "/usr/bin/gog")
    monkeypatch.setattr("subprocess.run", lambda _argv: SimpleNamespace(returncode=0))

    code = main(["google-setup"])

    assert code == 0
    assert existing.read_text(encoding="utf-8") == '{"kind":"email","provider":"gogcli","account":"custom"}\n'
    assert (root / "house-calendar.json").exists()
