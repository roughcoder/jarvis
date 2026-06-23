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
    assert "+ /tmp/jarvis-test-brew tap roughcoder/infinite-stack" in result.stdout
    assert "+ /tmp/jarvis-test-brew trust --formula roughcoder/infinite-stack/jarvis" in result.stdout
    assert "+ /tmp/jarvis-test-brew trust --cask roughcoder/infinite-stack/jarvis-app" in result.stdout
    assert "+ /tmp/jarvis-test-brew install jarvis" in result.stdout
    assert "+ /tmp/jarvis-test-brew install --cask jarvis-app" in result.stdout
    assert "+ /bin/mkdir -p " in result.stdout
    assert "+ /usr/bin/xattr -dr com.apple.quarantine /Applications/Jarvis.app" in result.stdout
    assert "open -a Jarvis" not in result.stdout
    assert "Press Install Services" in result.stdout
    assert "scripts/uninstall_mac.sh | bash" in result.stdout


def test_mac_installer_can_open_app() -> None:
    result = run_installer(JARVIS_OPEN_APP="1")

    assert result.returncode == 0, result.stderr
    assert "+ /usr/bin/open -a Jarvis" in result.stdout
