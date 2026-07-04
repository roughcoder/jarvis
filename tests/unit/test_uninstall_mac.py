from __future__ import annotations

import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
UNINSTALLER = ROOT / "scripts" / "uninstall_mac.sh"


def run_uninstaller(**env: str) -> subprocess.CompletedProcess[str]:
    merged = {
        **os.environ,
        "JARVIS_ASSUME_MAC": "1",
        "JARVIS_BREW_PATH": "/tmp/jarvis-test-brew",
        "JARVIS_DRY_RUN": "1",
        **env,
    }
    return subprocess.run(
        ["bash", str(UNINSTALLER)],
        check=False,
        cwd=ROOT,
        env=merged,
        text=True,
        capture_output=True,
    )


def test_mac_uninstaller_dry_run_removes_services_state_and_packages() -> None:
    result = run_uninstaller(
        HOME="/Users/tester",
        JARVIS_WORKDIR="/Users/tester/.jarvis",
        JARVIS_LOG_DIR="/Users/tester/Library/Logs/Jarvis",
    )

    assert result.returncode == 0, result.stderr
    assert "+ /usr/bin/osascript -e tell\\ application\\ \\\"Jarvis\\\"\\ to\\ quit" in result.stdout
    assert "+ /bin/launchctl bootout gui/" in result.stdout
    assert "+ rm -rf /Users/tester/Library/LaunchAgents/com.jarvis.brain.plist" in result.stdout
    assert "+ rm -rf /Users/tester/Library/LaunchAgents/com.jarvis.api.plist" in result.stdout
    assert "+ rm -rf /Users/tester/.jarvis" in result.stdout
    assert "+ rm -rf /Users/tester/Library/Logs/Jarvis" in result.stdout
    assert "+ rm -rf /Users/tester/Library/Preferences/dev.infinitestack.jarvis.mac.plist" in result.stdout
    assert "+ /tmp/jarvis-test-brew uninstall --cask --zap jarvis-app" in result.stdout
    assert "+ /tmp/jarvis-test-brew uninstall --formula jarvis" in result.stdout
    assert "+ rm -rf /Applications/Jarvis.app" in result.stdout
    assert "Local source checkouts were not touched." in result.stdout
    assert "scripts/install_mac.sh | bash" in result.stdout


def test_mac_uninstaller_can_keep_homebrew_packages() -> None:
    result = run_uninstaller(HOME="/Users/tester", JARVIS_UNINSTALL_PACKAGES="0")

    assert result.returncode == 0, result.stderr
    assert "uninstall --cask" not in result.stdout
    assert "uninstall --formula" not in result.stdout
    assert "+ rm -rf /Applications/Jarvis.app" not in result.stdout
