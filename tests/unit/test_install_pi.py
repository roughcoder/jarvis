from __future__ import annotations

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts" / "install_pi.sh"


def run_installer(**env: str) -> subprocess.CompletedProcess[str]:
    merged = {
        **os.environ,
        "JARVIS_DRY_RUN": "1",
        "JARVIS_BRAIN_HOST": "imac.private",
        "JARVIS_INTERCOM_TOKEN": "issued-token",
        "JARVIS_DEVICE_ID": "kitchen-pi",
        "JARVIS_INSTALL_DIR": "/opt/jarvis-test",
        "JARVIS_DRY_RUN_TMP_DIR": "/tmp/jarvis-pi-test",
        **env,
    }
    return subprocess.run(
        ["bash", str(INSTALLER)],
        check=False,
        cwd=ROOT,
        env=merged,
        text=True,
        capture_output=True,
    )


def test_pi_installer_requires_brain_host_and_token() -> None:
    result = run_installer(JARVIS_BRAIN_HOST="", JARVIS_INTERCOM_TOKEN="")

    assert result.returncode == 2
    assert "Set JARVIS_BRAIN_HOST and JARVIS_INTERCOM_TOKEN" in result.stderr


def test_pi_installer_dry_run_models_intercom_install() -> None:
    result = run_installer()

    assert result.returncode == 0, result.stderr
    assert "+ apt-get update" in result.stdout
    assert "+ apt-get install -y --no-install-recommends" in result.stdout
    assert "+ env UV_INSTALL_DIR=/usr/local/bin sh -c" in result.stdout
    assert "+ curl -fsSL https://github.com/roughcoder/jarvis/archive/main.tar.gz -o /tmp/jarvis-pi-test/jarvis.tar.gz" in result.stdout
    assert "+ mkdir -p /opt/jarvis-test" in result.stdout
    assert "+ tar -xzf /tmp/jarvis-pi-test/jarvis.tar.gz --strip-components=1 -C /opt/jarvis-test" in result.stdout
    assert "+ cd /opt/jarvis-test" in result.stdout
    assert "+ uv sync --no-dev --extra stt --extra vad --extra wake" in result.stdout
    assert "+ write /opt/jarvis-test/.env" in result.stdout
    assert "+ chmod 0600 /opt/jarvis-test/.env" in result.stdout
    assert "+ write /usr/local/bin/jarvis" in result.stdout
    assert "+ jarvis service install intercom --platform systemd --jarvis-bin /usr/local/bin/jarvis --workdir /opt/jarvis-test" in result.stdout
    assert "+ systemctl daemon-reload" in result.stdout
    assert "+ systemctl enable --now jarvis-intercom.service" in result.stdout


def test_pi_installer_dry_run_skips_uv_install_when_present() -> None:
    result = run_installer(JARVIS_DRY_RUN_UV_INSTALLED="1")

    assert result.returncode == 0, result.stderr
    assert "astral.sh/uv/install.sh" not in result.stdout
    assert "+ uv sync --no-dev --extra stt --extra vad --extra wake" in result.stdout

