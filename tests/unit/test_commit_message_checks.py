from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CHECK_SCRIPT = ROOT / "scripts" / "check_conventional_commits.sh"


def _git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        check=check,
        text=True,
        capture_output=True,
    )


def _init_repo(repo: Path) -> str:
    _git(repo, "init")
    _git(repo, "config", "user.name", "Test User")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "commit", "--allow-empty", "-m", "chore: init", "-m", "Release-note: skip")
    return _git(repo, "rev-parse", "HEAD").stdout.strip()


def test_commit_check_requires_release_note_for_fix_commits(tmp_path: Path) -> None:
    base = _init_repo(tmp_path)
    _git(tmp_path, "commit", "--allow-empty", "-m", "fix(voice): improve lifecycle")

    result = subprocess.run(
        [str(CHECK_SCRIPT), base, "HEAD"],
        cwd=tmp_path,
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 5
    assert "Missing release-note trailer" in result.stderr


def test_commit_check_rejects_escaped_newline_trailer_text(tmp_path: Path) -> None:
    base = _init_repo(tmp_path)
    _git(
        tmp_path,
        "commit",
        "--allow-empty",
        "-m",
        "fix(voice): improve lifecycle",
        "-m",
        "Constraint: voice release metadata matters.\\nRelease-note: skip",
    )

    result = subprocess.run(
        [str(CHECK_SCRIPT), base, "HEAD"],
        cwd=tmp_path,
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 4
    assert "Malformed commit trailers" in result.stderr
    assert "literal escaped newline" in result.stderr


def test_commit_check_accepts_real_release_note_trailer_lines(tmp_path: Path) -> None:
    base = _init_repo(tmp_path)
    _git(
        tmp_path,
        "commit",
        "--allow-empty",
        "-m",
        "fix(voice): improve lifecycle",
        "-m",
        "Release-note: Voice lifecycle now handles follow-ups more reliably.",
    )

    result = subprocess.run(
        [str(CHECK_SCRIPT), base, "HEAD"],
        cwd=tmp_path,
        check=False,
        text=True,
        capture_output=True,
    )

    assert result.returncode == 0
    assert "Conventional commit check passed" in result.stdout
