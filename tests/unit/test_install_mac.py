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
    assert "+ /usr/bin/git -C \\<homebrew\\ tap\\ repo\\> pull --ff-only" in result.stdout
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


def test_mac_installer_does_not_let_brew_consume_piped_script(tmp_path: Path) -> None:
    fake_brew = tmp_path / "brew"
    log = tmp_path / "brew.log"
    consumed = tmp_path / "stdin-consumed.txt"
    fake_brew.write_text(
        f"""#!/usr/bin/env bash
set -euo pipefail
printf '%s\\n' "$*" >> {log}
case "$1" in
  shellenv)
    printf 'export PATH=%q:$PATH\\n' {tmp_path}
    exit 0
    ;;
  --repo)
    printf '%s\\n' {tmp_path}
    exit 0
    ;;
  help)
    exit 0
    ;;
  install)
    cat >> {consumed}
    exit 0
    ;;
  *)
    exit 0
    ;;
esac
""",
        encoding="utf-8",
    )
    fake_brew.chmod(0o755)

    env = {
        **os.environ,
        "JARVIS_ASSUME_MAC": "1",
        "JARVIS_BREW_PATH": str(fake_brew),
        "JARVIS_OPEN_APP": "0",
        "JARVIS_WORKDIR": str(tmp_path / ".jarvis"),
    }
    result = subprocess.run(
        ["bash", "-c", "cat scripts/install_mac.sh | bash"],
        check=False,
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0, result.stderr
    assert "Installing Jarvis app" in result.stdout
    assert "Jarvis is installed." in result.stdout
    assert "install jarvis" in log.read_text(encoding="utf-8")
    assert "install --cask jarvis-app" in log.read_text(encoding="utf-8")
    assert consumed.read_text(encoding="utf-8") == ""
