from __future__ import annotations

import io
import json
import os
import subprocess
import tarfile
from pathlib import Path

import pytest

from jarvis.dogfood import (
    DogfoodHost,
    _host_activate,
    _host_prepare,
    _host_rollback,
    _host_status,
    load_inventory,
)


SHA = "a" * 40


def _host(tmp_path: Path, *, extras: tuple[str, ...] = ()) -> DogfoodHost:
    production = tmp_path / "production-jarvis"
    production.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    production.chmod(0o755)
    uv = tmp_path / "fake-uv"
    uv.write_text(
        "#!/bin/sh\n"
        'mkdir -p "$UV_PROJECT_ENVIRONMENT/bin"\n'
        'printf \'#!/bin/sh\\nexit 0\\n\' > "$UV_PROJECT_ENVIRONMENT/bin/jarvis"\n'
        'chmod 755 "$UV_PROJECT_ENVIRONMENT/bin/jarvis"\n',
        encoding="utf-8",
    )
    uv.chmod(0o755)
    workdir = tmp_path / "runtime-home"
    workdir.mkdir()
    return DogfoodHost(
        name="local-review-worker",
        local=True,
        ssh="",
        roles=("worker",),
        extras=extras,
        workdir=str(workdir),
        runtime_root=str(tmp_path / "dogfood"),
        production_bin=str(production),
        platform="launchd",
        uv_bin=str(uv),
        python="3.12",
    )


def _archive(tmp_path: Path) -> Path:
    archive = tmp_path / "source.tar.gz"
    files = {
        "pyproject.toml": b'[project]\nname = "jarvis"\nversion = "9.9.9"\n',
        "uv.lock": b"version = 1\n",
    }
    with tarfile.open(archive, "w:gz") as tar:
        for name, content in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(content)
            tar.addfile(info, io.BytesIO(content))
    return archive


def test_inventory_requires_private_valid_hosts(tmp_path) -> None:
    inventory = tmp_path / "dogfood.json"
    inventory.write_text(
        json.dumps(
            {
                "hosts": [
                    {
                        "name": "review-worker",
                        "local": True,
                        "roles": ["worker"],
                        "extras": ["worker-claude"],
                        "probes": [
                            {"role": "worker", "url": "http://127.0.0.1:8780/health"}
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    hosts = load_inventory(inventory)

    assert hosts[0].roles == ("worker",)
    assert hosts[0].extras == ("worker-claude",)


@pytest.mark.parametrize(
    "host",
    [
        {"name": "missing-transport", "roles": ["worker"]},
        {"name": "two-transports", "local": True, "ssh": "review", "roles": ["worker"]},
        {"name": "unknown-role", "local": True, "roles": ["database"]},
        {"name": "unknown-extra", "local": True, "roles": ["worker"], "extras": ["secret-sdk"]},
        {
            "name": "missing-probe",
            "local": True,
            "roles": ["worker"],
            "probes": [],
        },
        {
            "name": "credential-exfiltration",
            "local": True,
            "roles": ["worker"],
            "probes": [
                {
                    "role": "worker",
                    "url": "http://attacker.example/health",
                    "token_env": "WORKER_TOKEN",
                }
            ],
        },
    ],
)
def test_inventory_rejects_unsafe_or_unknown_configuration(tmp_path, host) -> None:  # noqa: ANN001
    inventory = tmp_path / "dogfood.json"
    inventory.write_text(json.dumps({"hosts": [host]}), encoding="utf-8")

    with pytest.raises(ValueError):
        load_inventory(inventory)


def test_prepare_activate_status_and_rollback_without_homebrew(tmp_path) -> None:
    host = _host(tmp_path, extras=("worker-claude",))

    prepared = _host_prepare(host, sha=SHA, archive=str(_archive(tmp_path)))
    activated = _host_activate(host, sha=SHA)

    root = Path(host.runtime_root)
    manifest = json.loads((root / "builds" / SHA / "manifest.json").read_text(encoding="utf-8"))
    assert prepared == {"ok": True, "action": "prepare", "git_sha": SHA, "reused": False}
    assert manifest["extras"] == ["worker", "browser", "worker-claude"]
    assert activated["status"]["channel"] == "dogfood"
    assert activated["status"]["git_sha"] == SHA
    assert os.readlink(root / "current").endswith(f"builds/{SHA}/bin/jarvis")
    assert os.readlink(root / "previous") == host.production_bin
    assert subprocess.run([str(root / "builds" / SHA / "bin" / "jarvis")], check=False).returncode == 0

    rolled_back = _host_rollback(host)

    assert rolled_back["status"]["channel"] == "production"
    assert rolled_back["status"]["git_sha"] == ""
    assert os.readlink(root / "current") == host.production_bin
    assert _host_status(host)["ok"] is True


def test_prepare_reuses_identical_build_but_rejects_changed_extras(tmp_path) -> None:
    archive = str(_archive(tmp_path))
    host = _host(tmp_path)
    _host_prepare(host, sha=SHA, archive=archive)

    assert _host_prepare(host, sha=SHA, archive=archive)["reused"] is True

    changed = DogfoodHost(**{**host.__dict__, "extras": ("worker-claude",)})
    with pytest.raises(RuntimeError, match="different roles or extras"):
        _host_prepare(changed, sha=SHA, archive=archive)


def test_activation_restores_previous_runtime_when_health_never_converges(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    host = _host(tmp_path)
    _host_prepare(host, sha=SHA, archive=str(_archive(tmp_path)))
    monkeypatch.setattr(
        "jarvis.dogfood._wait_for_status",
        lambda _host: {"ok": False, "issues": ["worker process identity does not match"]},
    )

    with pytest.raises(RuntimeError, match="activation health failed"):
        _host_activate(host, sha=SHA)

    root = Path(host.runtime_root)
    assert os.readlink(root / "current") == host.production_bin
    assert not (root / "state.json").exists()


def test_rollback_restores_current_runtime_when_previous_target_is_unhealthy(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    host = _host(tmp_path)
    _host_prepare(host, sha=SHA, archive=str(_archive(tmp_path)))
    _host_activate(host, sha=SHA)
    root = Path(host.runtime_root)
    dogfood_target = os.readlink(root / "current")
    monkeypatch.setattr(
        "jarvis.dogfood._wait_for_status",
        lambda _host: {"ok": False, "issues": ["production probe failed"]},
    )

    with pytest.raises(RuntimeError, match="rollback health failed"):
        _host_rollback(host)

    assert os.readlink(root / "current") == dogfood_target
    state = json.loads((root / "state.json").read_text(encoding="utf-8"))
    assert state["channel"] == "dogfood"
    assert state["git_sha"] == SHA
