from __future__ import annotations

import importlib.util
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
    assert "## Environment" in notes
    assert "New env vars: `JARVIS_BACKGROUND_ENABLED`." in notes
    assert "TTS_STREAM_TIMEOUT changed; increase it for slow networks." in notes
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
