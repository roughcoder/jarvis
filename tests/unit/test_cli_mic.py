import subprocess

from jarvis.cli import main


def test_mic_off_disables_and_stops_only_intercom(monkeypatch, capsys) -> None:
    calls: list[tuple[str, str, str | None]] = []

    def fake_control(role: str, action: str, *, platform_name: str | None = None):
        calls.append((role, action, platform_name))
        return subprocess.CompletedProcess([role, action], 0, stdout="", stderr="")

    monkeypatch.setattr("jarvis.deploy.control_service", fake_control)

    code = main(["mic", "off", "--platform", "launchd"])

    output = capsys.readouterr()
    assert code == 0
    assert calls == [
        ("intercom", "disable", "launchd"),
        ("intercom", "stop", "launchd"),
    ]
    assert "worker" not in output.out
    assert "mic/listener off" in output.out


def test_mic_on_enables_and_starts_only_intercom(monkeypatch, capsys) -> None:
    calls: list[tuple[str, str, str | None]] = []

    def fake_control(role: str, action: str, *, platform_name: str | None = None):
        calls.append((role, action, platform_name))
        return subprocess.CompletedProcess([role, action], 0, stdout="", stderr="")

    monkeypatch.setattr("jarvis.deploy.control_service", fake_control)

    code = main(["mic", "on", "--platform", "systemd"])

    output = capsys.readouterr()
    assert code == 0
    assert calls == [
        ("intercom", "enable", "systemd"),
        ("intercom", "start", "systemd"),
    ]
    assert "worker" not in output.out
    assert "mic/listener on" in output.out


def test_mic_off_reports_stop_failure(monkeypatch, capsys) -> None:
    def fake_control(role: str, action: str, *, platform_name: str | None = None):
        code = 1 if action == "stop" else 0
        return subprocess.CompletedProcess(
            [role, action], code, stdout="", stderr=f"{action} failed\n" if code else ""
        )

    monkeypatch.setattr("jarvis.deploy.control_service", fake_control)

    code = main(["mic", "off", "--platform", "launchd"])

    output = capsys.readouterr()
    assert code == 1
    assert "mic/listener off" not in output.out
    assert "stop failed" in output.err
