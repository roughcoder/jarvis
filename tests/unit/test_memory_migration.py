from __future__ import annotations

from pathlib import Path
from typing import Any

from jarvis.brain.memory_client import ConclusionRecord
from jarvis.cli import main
from jarvis.migration.profile_facts import (
    EXPLICIT_LEVEL,
    load_profile_fact_seeds,
    seed_profile_facts,
    verify_profile_fact_seed,
)


class FakeBackend:
    def __init__(self, records: list[ConclusionRecord] | None = None) -> None:
        self.records = list(records or [])
        self.created: list[dict[str, Any]] = []

    def create_conclusion(
        self,
        *,
        observed_id: str,
        content: str,
        observer_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ConclusionRecord:
        record = ConclusionRecord(
            id=f"c{len(self.records) + 1}",
            content=content,
            observer_id=observer_id or "jarvis",
            observed_id=observed_id,
            session_id=session_id,
            level=(metadata or {}).get("level", EXPLICIT_LEVEL),
            metadata=dict(metadata or {}),
        )
        self.created.append(
            {
                "observed_id": observed_id,
                "observer_id": observer_id,
                "content": content,
                "metadata": dict(metadata or {}),
            }
        )
        self.records.append(record)
        return record

    def list_conclusions(
        self,
        *,
        observed_id: str | None = None,
        observer_id: str | None = None,
        session_id: str | None = None,
        level: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[ConclusionRecord]:
        del session_id, metadata
        records = self.records
        if observed_id:
            records = [record for record in records if record.observed_id == observed_id]
        if observer_id:
            records = [record for record in records if record.observer_id == observer_id]
        if level:
            records = [record for record in records if record.level == level]
        return records


def test_profile_fact_maps_to_explicit_conclusion_payload(tmp_path: Path) -> None:
    users_dir = _write_profile(tmp_path, honcho_peer="principal-neil")

    seeds = load_profile_fact_seeds(users_dir, as_of="2026-07-05")

    assert len(seeds) == 1
    seed = seeds[0]
    assert seed.peer_id == "principal-neil"
    assert seed.content == "email: neil@example.test"
    assert seed.metadata["level"] == "explicit"
    assert seed.metadata["recorded_by"] == "neil"
    assert seed.metadata["source"] == "profile-migration"
    assert seed.metadata["observed_at"] == "2026-07-05"
    assert seed.metadata["content_hash"].startswith("sha256:")

    backend = FakeBackend()
    seed_profile_facts(backend, seeds, workspace="jarvis-migration-dev")

    assert backend.created == [
        {
            "observed_id": "principal-neil",
            "observer_id": "principal-neil",
            "content": "email: neil@example.test",
            "metadata": seed.metadata,
        }
    ]


def test_profile_fact_seed_rerun_skips_existing_content_hash(tmp_path: Path) -> None:
    users_dir = _write_profile(tmp_path)
    seeds = load_profile_fact_seeds(users_dir, as_of="2026-07-05")
    backend = FakeBackend()

    first = seed_profile_facts(backend, seeds, workspace="jarvis-migration-dev")
    second = seed_profile_facts(backend, seeds, workspace="jarvis-migration-dev")

    assert first.created == 1
    assert second.created == 0
    assert second.skipped == 1
    assert len(backend.records) == 1


def test_profile_fact_verify_passes_when_every_fact_is_present(tmp_path: Path) -> None:
    users_dir = _write_profile(tmp_path)
    seeds = load_profile_fact_seeds(users_dir, as_of="2026-07-05")
    backend = FakeBackend()
    seed_profile_facts(backend, seeds, workspace="jarvis-migration-dev")

    summary = verify_profile_fact_seed(backend, seeds, workspace="jarvis-migration-dev")

    assert summary.ok
    assert summary.expected == 1


def test_profile_fact_verify_fails_when_fact_is_missing(tmp_path: Path) -> None:
    users_dir = _write_profile(tmp_path)
    seeds = load_profile_fact_seeds(users_dir, as_of="2026-07-05")

    summary = verify_profile_fact_seed(FakeBackend(), seeds, workspace="jarvis-migration-dev")

    assert not summary.ok
    assert "missing 'email: neil@example.test'" in summary.discrepancies[0]


def test_memory_migrate_dry_run_does_not_build_backend(tmp_path: Path, monkeypatch, capsys) -> None:  # noqa: ANN001
    users_dir = _write_profile(tmp_path)
    monkeypatch.setenv("JARVIS_ENV_FILE", str(tmp_path / "missing.env"))

    def fail_backend(_cfg):  # noqa: ANN001, ANN202
        raise AssertionError("dry-run should not create a backend")

    monkeypatch.setattr("jarvis.brain.memory_client.MemoryClient", fail_backend)

    status = main(
        [
            "memory-migrate",
            "--users-dir",
            str(users_dir),
            "--workspace",
            "jarvis-migration-dryrun",
            "--as-of",
            "2026-07-05",
            "--dry-run",
        ]
    )

    assert status == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "email: neil@example.test" in out


def test_memory_migrate_blocks_non_dev_workspace_without_ack(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    users_dir = _write_profile(tmp_path)
    monkeypatch.setenv("JARVIS_ENV_FILE", str(tmp_path / "missing.env"))

    def fail_backend(_cfg):  # noqa: ANN001, ANN202
        raise AssertionError("guard should run before backend creation")

    monkeypatch.setattr("jarvis.brain.memory_client.MemoryClient", fail_backend)

    status = main(
        [
            "memory-migrate",
            "--users-dir",
            str(users_dir),
            "--workspace",
            "jarvis-home",
            "--as-of",
            "2026-07-05",
        ]
    )

    assert status == 2


def _write_profile(tmp_path: Path, *, honcho_peer: str = "neil") -> Path:
    users_dir = tmp_path / "users"
    users_dir.mkdir()
    (users_dir / "neil.md").write_text(
        f"---\nhoncho_peer: {honcho_peer}\nscope: personal\n---\n\n"
        "# Neil\n\n"
        "## What Jarvis knows\n"
        "<!-- managed by Jarvis: facts you've asked me to remember -->\n"
        "- email: neil@example.test\n",
        encoding="utf-8",
    )
    return users_dir
