"""Seed authoritative profile facts into Honcho v3 explicit conclusions."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Protocol

from jarvis.brain.memory_client import ConclusionRecord, MemoryBackend
from jarvis.users import load_users, read_facts

DEFAULT_MIGRATION_WORKSPACE = "jarvis-migration-dev"
PROFILE_MIGRATION_SOURCE = "profile-migration"
EXPLICIT_LEVEL = "explicit"

_DEV_WORKSPACE_MARKERS = frozenset(
    {"dev", "test", "testing", "stage", "staging", "local", "dryrun", "scratch", "sandbox"}
)
_NON_DEV_WORKSPACE_MARKERS = frozenset({"home", "prod", "production", "main", "live"})


class WorkspaceSafetyError(ValueError):
    """Raised when the target workspace is not safe for an unattended write."""


@dataclass(frozen=True)
class ProfileFactSeed:
    profile_name: str
    peer_id: str
    key: str
    value: str
    content: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class SeedSummary:
    workspace: str
    expected: int
    created: int = 0
    skipped: int = 0
    dry_run: bool = False
    discrepancies: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.discrepancies


class _ConclusionBackend(Protocol):
    def create_conclusion(
        self,
        *,
        observed_id: str,
        content: str,
        observer_id: str | None = None,
        session_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ConclusionRecord: ...

    def list_conclusions(
        self,
        *,
        observed_id: str | None = None,
        observer_id: str | None = None,
        session_id: str | None = None,
        level: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> list[ConclusionRecord]: ...


def default_as_of() -> str:
    return date.today().isoformat()


def is_dev_workspace(workspace: str) -> bool:
    value = (workspace or "").strip().lower()
    tokens = {token for token in re.split(r"[-_]+", value) if token}
    if not tokens or tokens & _NON_DEV_WORKSPACE_MARKERS:
        return False
    return bool(tokens & _DEV_WORKSPACE_MARKERS)


def validate_workspace_target(
    workspace: str,
    *,
    explicit_workspace: bool,
    acknowledgement: str = "",
) -> None:
    """Require a deliberate acknowledgement before writing outside dev/staging."""

    target = (workspace or "").strip()
    if not target:
        raise WorkspaceSafetyError("workspace is required")
    if is_dev_workspace(target):
        return
    if not explicit_workspace:
        raise WorkspaceSafetyError(
            "non-dev workspace targets require an explicit --workspace value"
        )
    if acknowledgement != target:
        raise WorkspaceSafetyError(
            "non-dev workspace targets require --i-understand-this-writes-to "
            f"{target!r}"
        )


def load_profile_fact_seeds(users_dir: str | Path, *, as_of: str) -> list[ProfileFactSeed]:
    users_path = Path(users_dir).expanduser()
    users = load_users(str(users_path))
    seeds: list[ProfileFactSeed] = []
    for profile_name, user in sorted(users.items()):
        facts = read_facts(users_path / f"{profile_name}.md")
        for key, value in facts.items():
            content = f"{key}: {value}"
            metadata = {
                "level": EXPLICIT_LEVEL,
                "recorded_by": profile_name,
                "source": PROFILE_MIGRATION_SOURCE,
                "observed_at": as_of,
                "content_hash": _content_hash(user.peer, key, value),
            }
            seeds.append(
                ProfileFactSeed(
                    profile_name=profile_name,
                    peer_id=user.peer,
                    key=key,
                    value=value,
                    content=content,
                    metadata=metadata,
                )
            )
    return seeds


def dry_run_plan(seeds: Iterable[ProfileFactSeed]) -> list[dict[str, Any]]:
    return [
        {
            "peer": seed.peer_id,
            "observer": seed.peer_id,
            "content": seed.content,
            "metadata": dict(seed.metadata),
        }
        for seed in seeds
    ]


def print_dry_run(seeds: Iterable[ProfileFactSeed]) -> None:
    print(json.dumps(dry_run_plan(seeds), indent=2, sort_keys=True))


def seed_profile_facts(
    backend: MemoryBackend | _ConclusionBackend,
    seeds: Iterable[ProfileFactSeed],
    *,
    workspace: str,
) -> SeedSummary:
    seed_list = list(seeds)
    created = 0
    skipped = 0
    for peer_id, peer_seeds in _by_peer(seed_list).items():
        existing = backend.list_conclusions(
            observed_id=peer_id,
            observer_id=peer_id,
            level=EXPLICIT_LEVEL,  # type: ignore[arg-type]
        )
        existing_hashes = {
            str(record.metadata.get("content_hash"))
            for record in existing
            if record.metadata.get("content_hash")
        }
        for seed in peer_seeds:
            if seed.metadata["content_hash"] in existing_hashes:
                skipped += 1
                continue
            backend.create_conclusion(
                observed_id=seed.peer_id,
                observer_id=seed.peer_id,
                content=seed.content,
                metadata=dict(seed.metadata),
            )
            existing_hashes.add(seed.metadata["content_hash"])
            created += 1
    return SeedSummary(workspace=workspace, expected=len(seed_list), created=created, skipped=skipped)


def verify_profile_fact_seed(
    backend: MemoryBackend | _ConclusionBackend,
    seeds: Iterable[ProfileFactSeed],
    *,
    workspace: str,
) -> SeedSummary:
    seed_list = list(seeds)
    discrepancies: list[str] = []
    for peer_id, peer_seeds in _by_peer(seed_list).items():
        records = backend.list_conclusions(
            observed_id=peer_id,
            observer_id=peer_id,
            level=EXPLICIT_LEVEL,  # type: ignore[arg-type]
        )
        migration_records = [
            record for record in records if record.metadata.get("source") == PROFILE_MIGRATION_SOURCE
        ]
        expected_hashes = {str(seed.metadata["content_hash"]) for seed in peer_seeds}
        seen_expected_hashes: set[str] = set()
        for seed in peer_seeds:
            matches = [
                record
                for record in migration_records
                if _matches_seed(record, seed)
            ]
            if not matches:
                discrepancies.append(f"{peer_id}: missing {seed.content!r}")
                continue
            if len(matches) > 1:
                discrepancies.append(f"{peer_id}: duplicate {seed.content!r} ({len(matches)} matches)")
            seen_expected_hashes.add(str(seed.metadata["content_hash"]))

        for record in migration_records:
            content_hash = str(record.metadata.get("content_hash", ""))
            if content_hash not in expected_hashes:
                discrepancies.append(
                    f"{peer_id}: unexpected migrated conclusion {record.content!r}"
                )

        if len(seen_expected_hashes) != len(peer_seeds):
            discrepancies.append(
                f"{peer_id}: matched {len(seen_expected_hashes)} of {len(peer_seeds)} expected facts"
            )
    return SeedSummary(workspace=workspace, expected=len(seed_list), discrepancies=tuple(discrepancies))


def _matches_seed(record: ConclusionRecord, seed: ProfileFactSeed) -> bool:
    return (
        record.content == seed.content
        and record.observed_id == seed.peer_id
        and record.observer_id == seed.peer_id
        and record.level == EXPLICIT_LEVEL
        and record.metadata.get("source") == PROFILE_MIGRATION_SOURCE
        and record.metadata.get("recorded_by") == seed.profile_name
        and record.metadata.get("content_hash") == seed.metadata["content_hash"]
    )


def _by_peer(seeds: Iterable[ProfileFactSeed]) -> dict[str, list[ProfileFactSeed]]:
    grouped: dict[str, list[ProfileFactSeed]] = {}
    for seed in seeds:
        grouped.setdefault(seed.peer_id, []).append(seed)
    return grouped


def _content_hash(peer_id: str, key: str, value: str) -> str:
    payload = "\0".join(
        [
            "jarvis-profile-fact-migration-v1",
            PROFILE_MIGRATION_SOURCE,
            peer_id,
            key,
            value,
        ]
    )
    return f"sha256:{hashlib.sha256(payload.encode('utf-8')).hexdigest()}"
