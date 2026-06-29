from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "generate_release_notes",
    ROOT / "scripts" / "generate_release_notes.py",
)
release_notes = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = release_notes
SPEC.loader.exec_module(release_notes)


def test_parse_commit_release_and_env_trailers() -> None:
    commit = release_notes.parse_commit(
        "abc123",
        """feat(intercom): add per-room routing

Release-note: Added per-room intercom routing.
Env: JARVIS_ROOM_ID added; set this on each room device.
Breaking Change: Existing room configs need a room id.
""",
    )

    assert commit.type == "feat"
    assert commit.scope == "intercom"
    assert commit.breaking is True
    assert commit.release_notes == ["Added per-room intercom routing."]
    assert commit.env_notes == ["JARVIS_ROOM_ID added; set this on each room device."]
    assert commit.breaking_notes == ["Existing room configs need a room id."]


def test_deterministic_notes_include_changes_fixes_and_env() -> None:
    commits = [
        release_notes.parse_commit(
            "abc123",
            """feat(worker): queue background jobs

Release-note: Added queued background jobs for long-running work.
""",
        ),
        release_notes.parse_commit(
            "def456",
            """fix(tts): avoid repeated sentence starts

Release-note: Fixed duplicate TTS starts during streaming playback.
Env: TTS_STREAM_TIMEOUT changed; increase it for slow networks.
Upgrade-note: Restart Jarvis after changing TTS_STREAM_TIMEOUT.
Docs: docs/TESTING.md covers streaming playback checks.
""",
        ),
    ]
    payload = release_notes.facts_payload(
        "1.2.3",
        "v1.2.3",
        "v1.2.2",
        commits,
        {"added": ["JARVIS_BACKGROUND_ENABLED"], "removed": []},
    )

    notes = release_notes.deterministic_notes(payload)

    assert "# Jarvis Runtime v1.2.3" in notes
    assert "## Changed" in notes
    assert "- Added queued background jobs for long-running work." in notes
    assert "## Fixed" in notes
    assert "- Fixed duplicate TTS starts during streaming playback." in notes
    assert "## Operator Action" in notes
    assert "- Restart Jarvis after changing TTS_STREAM_TIMEOUT." in notes
    assert "## Environment" in notes
    assert "Env vars added to `.env.example`: `JARVIS_BACKGROUND_ENABLED`." in notes
    assert "TTS_STREAM_TIMEOUT changed; increase it for slow networks." in notes
    assert "## Documentation" in notes
    assert "- docs/TESTING.md covers streaming playback checks." in notes
    assert "brew upgrade jarvis" in notes


def test_env_diff_reads_env_example_keys(monkeypatch) -> None:
    def fake_run_git(args: list[str], *, check: bool = True) -> str:
        assert args[0] == "show"
        if args[1] == "v1.0.0:.env.example":
            return "A=1\nOLD_SETTING=true\n"
        if args[1] == "HEAD:.env.example":
            return "A=1\nNEW_SETTING=true\n"
        raise AssertionError(args)

    monkeypatch.setattr(release_notes, "run_git", fake_run_git)

    assert release_notes.env_diff("v1.0.0", "HEAD") == {
        "added": ["NEW_SETTING"],
        "removed": ["OLD_SETTING"],
    }


def test_ai_auto_disables_without_key_or_model(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("JARVIS_RELEASE_NOTES_MODEL", raising=False)

    assert release_notes.ai_enabled("auto") is False


def test_release_note_skip_does_not_emit_other_change() -> None:
    commit = release_notes.parse_commit(
        "abc123",
        """chore(version): sync runtime metadata to 1.2.3

Release-note: skip
""",
    )

    assert release_notes.grouped_notes([commit])["other"] == []


def test_missing_release_note_does_not_fall_back_to_commit_subject() -> None:
    commit = release_notes.parse_commit(
        "abc123",
        """fix(voice): preserve paired identity baseline

Keep pairing identity stable after a voice reset.
""",
    )

    groups = release_notes.grouped_notes([commit])

    assert groups["fixes"] == []


def test_strict_release_trailers_require_note_or_skip() -> None:
    missing = release_notes.parse_commit(
        "abc123",
        """fix(voice): preserve paired identity baseline

Keep pairing identity stable after a voice reset.
""",
    )
    skipped = release_notes.parse_commit(
        "def456",
        """fix(tts): adjust internal buffering

Release-note: skip
""",
    )

    errors = release_notes.validate_release_trailers([missing, skipped])

    assert len(errors) == 1
    assert "abc123" in errors[0]
    assert "Release-note: <text> or Release-note: skip" in errors[0]


def test_strict_release_trailers_reject_escaped_newline_trailer_text() -> None:
    malformed = release_notes.parse_commit(
        "abc123",
        """fix(voice): keep follow-up open

Constraint: voice release metadata matters.\\nRelease-note: Voice follow-up lifecycle improved.
""",
    )

    errors = release_notes.validate_release_trailers([malformed])

    assert len(errors) == 1
    assert "abc123" in errors[0]
    assert "contains escaped newline trailer text" in errors[0]
    assert "use real commit-message lines" in errors[0]


def test_release_note_overrides_satisfy_strict_trailers(tmp_path) -> None:
    missing = release_notes.parse_commit(
        "abc123def456",
        """fix(voice): preserve paired identity baseline

Keep pairing identity stable after a voice reset.
""",
    )
    path = tmp_path / "overrides.json"
    path.write_text('{"abc123": {"Release-note": "skip"}}', encoding="utf-8")

    overrides = release_notes.load_release_note_overrides(path)
    release_notes.apply_release_note_overrides([missing], overrides)

    assert missing.has_release_note_decision is True
    assert missing.release_notes == []
    assert release_notes.validate_release_trailers([missing]) == []


def test_strict_release_trailers_require_breaking_detail_for_any_type() -> None:
    missing = release_notes.parse_commit(
        "abc123",
        """refactor(architecture)!: move account contracts

Release-note: skip
""",
    )
    documented = release_notes.parse_commit(
        "def456",
        """chore(config)!: remove legacy env alias

Breaking Change: OLD_SETTING is no longer read; use NEW_SETTING instead.
""",
    )

    errors = release_notes.validate_release_trailers([missing, documented])

    assert len(errors) == 1
    assert "abc123" in errors[0]
    assert "is breaking" in errors[0]
    assert "Breaking Change: <migration impact>" in errors[0]


def test_release_markdown_quality_rejects_raw_scope_bullets() -> None:
    notes = """# Jarvis Runtime v1.2.3

## Changed

- ops: make fleet runtime controls easier

## Operator Action

- No operator action required.

## Environment

- No env changes detected.

## Install

```bash
brew install jarvis
```

## Update

```bash
brew upgrade jarvis
```
"""

    errors = release_notes.validate_release_markdown(notes)

    assert errors == ["raw commit-style bullet leaked into release notes: - ops: make fleet runtime controls easier"]


def test_release_markdown_quality_caps_noisy_sections() -> None:
    bullets = "\n".join(f"- Fix {index}" for index in range(9))
    notes = f"""# Jarvis Runtime v1.2.3

## Fixed

{bullets}

## Operator Action

- No operator action required.

## Environment

- No env changes detected.

## Install

```bash
brew install jarvis
```

## Update

```bash
brew upgrade jarvis
```
"""

    errors = release_notes.validate_release_markdown(notes)

    assert errors == ["Fixed has 9 bullets; group related changes down to 8 or fewer"]


def test_release_runtime_refuses_local_publish(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("GITHUB_WORKFLOW", raising=False)

    result = subprocess.run(
        [str(ROOT / "scripts" / "release_runtime.sh"), "9.9.9"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "must be published by the GitHub Actions" in result.stderr
