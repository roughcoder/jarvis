from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_homebrew_formula_helper_writes_non_recursive_wrapper() -> None:
    text = (ROOT / "scripts" / "update_homebrew_formula.sh").read_text()

    assert "'      #!/usr/bin/env bash'" in text
    assert 'exec "#{libexec}/.venv/bin/python" -m jarvis.cli "$@"' in text
    assert 'uv" run --no-sync jarvis "$@"' not in text


def test_homebrew_formula_helper_restores_user_tap_remote() -> None:
    text = (ROOT / "scripts" / "update_homebrew_formula.sh").read_text()

    assert "restore_brew_tap_remote()" in text
    assert 'trap restore_brew_tap_remote EXIT' in text
    assert 'remote set-url origin "$BREW_TAP_ORIGINAL_REMOTE"' in text
