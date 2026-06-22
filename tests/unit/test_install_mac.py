from __future__ import annotations

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts" / "install_mac.sh"


def run_installer(**env: str) -> subprocess.CompletedProcess[str]:
    merged = {
        **os.environ,
        "JARVIS_ASSUME_MAC": "1",
        "JARVIS_BREW_PATH": "/tmp/jarvis-test-brew",
        "JARVIS_DRY_RUN": "1",
        "JARVIS_OPEN_APP": "0",
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


def test_mac_installer_dry_run_models_fresh_install() -> None:
    result = run_installer()

    assert result.returncode == 0, result.stderr
    assert "+ /tmp/jarvis-test-brew update" in result.stdout
    assert "+ /tmp/jarvis-test-brew tap roughcoder/infinite-stack" in result.stdout
    assert "+ /tmp/jarvis-test-brew install jarvis" in result.stdout
    assert "--HEAD jarvis" not in result.stdout
    assert "+ /tmp/jarvis-test-brew install --cask jarvis-app" in result.stdout
    assert "open -a Jarvis" not in result.stdout


def test_mac_installer_dry_run_models_existing_install_update() -> None:
    result = run_installer(
        JARVIS_DRY_RUN_RUNTIME_INSTALLED="1",
        JARVIS_DRY_RUN_APP_INSTALLED="1",
    )

    assert result.returncode == 0, result.stderr
    assert "+ /tmp/jarvis-test-brew upgrade jarvis" in result.stdout
    assert "--fetch-HEAD jarvis" not in result.stdout
    assert "+ /tmp/jarvis-test-brew upgrade --cask jarvis-app" in result.stdout


def test_mac_installer_head_fallback_is_explicit() -> None:
    result = run_installer(JARVIS_ALLOW_HEAD_FALLBACK="1")

    assert result.returncode == 0, result.stderr
    stable = result.stdout.index("+ /tmp/jarvis-test-brew install jarvis")
    head = result.stdout.index("+ /tmp/jarvis-test-brew install --HEAD jarvis")
    assert stable < head


def test_mac_installer_dry_run_installs_and_starts_roles() -> None:
    result = run_installer(
        JARVIS_ROLES="brain worker",
        JARVIS_START_SERVICES="1",
    )

    assert result.returncode == 0, result.stderr
    assert "+ jarvis service install brain" in result.stdout
    assert "+ jarvis service start brain" in result.stdout
    assert "+ jarvis service restart brain" in result.stdout
    assert "+ jarvis service install worker" in result.stdout
    assert "+ jarvis service start worker" in result.stdout
    assert "+ jarvis service restart worker" in result.stdout
