"""Jarvis-owned project/contact registry.

The registry is local state, deliberately separate from Honcho: Honcho stores
reasoned memory for peers, while this file stores identity, access metadata,
and external pointers for those peers.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass, field, replace
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Literal


Visibility = Literal["household", "private", "shared"]
ProjectStatus = Literal["active", "paused", "archived"]
ContactStatus = Literal["active", "merged"]
CreatedFrom = Literal["curated", "auto_created_channel_sender"]
ResolutionStatus = Literal["matched", "ambiguous", "not_found"]
RepoResolutionStatus = Literal["matched", "ambiguous", "not_found"]

_VISIBILITIES = {"household", "private", "shared"}
_PROJECT_STATUSES = {"active", "paused", "archived"}
_CONTACT_STATUSES = {"active", "merged"}
_CREATED_FROM = {"curated", "auto_created_channel_sender"}
_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


class RegistryError(ValueError):
    """Base error for invalid registry operations."""


class RegistryConflict(RegistryError):
    """Raised when a registry entry conflicts with an existing entry."""


@dataclass(frozen=True)
class RepoEntry:
    name: str
    remote: str
    default: bool = False

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise RegistryError("repo name is required")
        if not self.remote.strip():
            raise RegistryError("repo remote is required")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RepoEntry:
        return cls(
            name=str(data.get("name", "")).strip(),
            remote=str(data.get("remote", "")).strip(),
            default=bool(data.get("default", False)),
        )

    def as_dict(self) -> dict[str, Any]:
        data = {"name": self.name, "remote": self.remote}
        if self.default:
            data["default"] = True
        return data


@dataclass(frozen=True)
class ProjectLinks:
    jira: str = ""
    urls: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> ProjectLinks:
        data = data or {}
        return cls(
            jira=str(data.get("jira", "")).strip(),
            urls=_unique_strings(data.get("urls", ())),
        )

    def as_dict(self) -> dict[str, Any]:
        return {"jira": self.jira, "urls": list(self.urls)}


@dataclass(frozen=True)
class ProjectEntry:
    id: str
    name: str
    owner: str
    members: tuple[str, ...]
    visibility: Visibility = "household"
    status: ProjectStatus = "active"
    aliases: tuple[str, ...] = ()
    repos: tuple[RepoEntry, ...] = ()
    links: ProjectLinks = field(default_factory=ProjectLinks)
    files_root: str = ""

    def __post_init__(self) -> None:
        _validate_slug(self.id, "project id")
        if not self.name.strip():
            raise RegistryError("project name is required")
        if not self.owner.strip():
            raise RegistryError("project owner is required")
        if self.visibility not in _VISIBILITIES:
            raise RegistryError(f"invalid project visibility: {self.visibility}")
        if self.status not in _PROJECT_STATUSES:
            raise RegistryError(f"invalid project status: {self.status}")
        members = _with_owner(self.owner, self.members)
        object.__setattr__(self, "members", members)
        object.__setattr__(self, "aliases", _unique_strings(self.aliases))
        _validate_repos(self.repos)

    @property
    def peer_id(self) -> str:
        return f"project:{self.id}"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ProjectEntry:
        entry = cls(
            id=str(data.get("id", "")).strip(),
            name=str(data.get("name", "")).strip(),
            aliases=_unique_strings(data.get("aliases", ())),
            owner=str(data.get("owner", "")).strip(),
            members=_unique_strings(data.get("members", ())),
            visibility=str(data.get("visibility", "household")),
            status=str(data.get("status", "active")),
            repos=tuple(RepoEntry.from_dict(item) for item in data.get("repos", ())),
            links=ProjectLinks.from_dict(data.get("links")),
            files_root=str(data.get("files_root", "")).strip(),
        )
        expected = entry.peer_id
        actual = data.get("peer_id", expected)
        if actual != expected:
            raise RegistryError(f"project peer_id must be {expected!r}")
        return entry

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "peer_id": self.peer_id,
            "aliases": list(self.aliases),
            "owner": self.owner,
            "members": list(self.members),
            "visibility": self.visibility,
            "status": self.status,
            "repos": [repo.as_dict() for repo in self.repos],
            "links": self.links.as_dict(),
            "files_root": self.files_root,
        }


@dataclass(frozen=True)
class ContactIdentifiers:
    phones: tuple[str, ...] = ()
    emails: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "phones", _unique_strings(_normalize_phone(p) for p in self.phones))
        object.__setattr__(
            self,
            "emails",
            _unique_strings(str(e).strip().lower() for e in self.emails),
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> ContactIdentifiers:
        data = data or {}
        return cls(
            phones=tuple(data.get("phones", ())),
            emails=tuple(data.get("emails", ())),
        )

    @property
    def keys(self) -> tuple[str, ...]:
        return tuple(f"phone:{phone}" for phone in self.phones) + tuple(
            f"email:{email}" for email in self.emails
        )

    def as_dict(self) -> dict[str, Any]:
        return {"phones": list(self.phones), "emails": list(self.emails)}

    def merged(self, other: ContactIdentifiers) -> ContactIdentifiers:
        return ContactIdentifiers(
            phones=self.phones + other.phones,
            emails=self.emails + other.emails,
        )


@dataclass(frozen=True)
class ContactEntry:
    id: str
    display_name: str
    owner: str
    visibility: Visibility = "private"
    members: tuple[str, ...] = ()
    relationship: str = ""
    aliases: tuple[str, ...] = ()
    identifiers: ContactIdentifiers = field(default_factory=ContactIdentifiers)
    created_from: CreatedFrom = "curated"
    status: ContactStatus = "active"
    merged_into: str = ""

    def __post_init__(self) -> None:
        _validate_slug(self.id, "contact id")
        if not self.display_name.strip():
            raise RegistryError("contact display_name is required")
        if not self.owner.strip():
            raise RegistryError("contact owner is required")
        if self.visibility not in _VISIBILITIES:
            raise RegistryError(f"invalid contact visibility: {self.visibility}")
        if self.created_from not in _CREATED_FROM:
            raise RegistryError(f"invalid contact created_from: {self.created_from}")
        if self.status not in _CONTACT_STATUSES:
            raise RegistryError(f"invalid contact status: {self.status}")
        if self.status == "merged" and not self.merged_into:
            raise RegistryError("merged contact must include merged_into")
        members = _with_owner(self.owner, self.members)
        object.__setattr__(self, "members", members)
        object.__setattr__(self, "aliases", _unique_strings(self.aliases))

    @property
    def peer_id(self) -> str:
        return f"contact:{self.id}"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ContactEntry:
        entry = cls(
            id=str(data.get("id", "")).strip(),
            display_name=str(data.get("display_name", data.get("name", ""))).strip(),
            aliases=_unique_strings(data.get("aliases", ())),
            relationship=str(data.get("relationship", "")).strip(),
            identifiers=ContactIdentifiers.from_dict(data.get("identifiers")),
            owner=str(data.get("owner", "")).strip(),
            visibility=str(data.get("visibility", "private")),
            members=_unique_strings(data.get("members", ())),
            created_from=str(data.get("created_from", "curated")),
            status=str(data.get("status", "active")),
            merged_into=str(data.get("merged_into", "")).strip(),
        )
        expected = entry.peer_id
        actual = data.get("peer_id", expected)
        if actual != expected:
            raise RegistryError(f"contact peer_id must be {expected!r}")
        return entry

    def as_dict(self) -> dict[str, Any]:
        data = {
            "id": self.id,
            "peer_id": self.peer_id,
            "display_name": self.display_name,
            "aliases": list(self.aliases),
            "relationship": self.relationship,
            "identifiers": self.identifiers.as_dict(),
            "owner": self.owner,
            "visibility": self.visibility,
            "members": list(self.members),
            "created_from": self.created_from,
            "status": self.status,
        }
        if self.merged_into:
            data["merged_into"] = self.merged_into
        return data


@dataclass(frozen=True)
class EntityResolution:
    status: ResolutionStatus
    entry: ProjectEntry | ContactEntry | None = None
    candidates: tuple[ProjectEntry | ContactEntry, ...] = ()
    speakable_names: tuple[str, ...] = ()
    score: float = 0.0


@dataclass(frozen=True)
class RepoResolution:
    status: RepoResolutionStatus
    repo: RepoEntry | None = None
    speakable_names: tuple[str, ...] = ()
    reason: str = ""


class RegistryStore:
    """JSON-backed single-writer registry store."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser()
        self._projects: dict[str, ProjectEntry] = {}
        self._contacts: dict[str, ContactEntry] = {}
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            self._projects = {}
            self._contacts = {}
            return
        raw = self.path.read_text(encoding="utf-8")
        if not raw.strip():
            # A provisioned-but-empty file (e.g. `touch`) is a valid empty registry.
            self._projects = {}
            self._contacts = {}
            return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as exc:
            # Fail loudly rather than starting empty: a later save would
            # overwrite the damaged system of record.
            raise RegistryError(f"registry file {self.path} is not valid JSON: {exc}") from exc
        if not isinstance(data, dict):
            raise RegistryError(f"registry file {self.path} must contain a JSON object")
        self._projects = {
            item["id"]: ProjectEntry.from_dict(item) for item in data.get("projects", ())
        }
        self._contacts = {
            item["id"]: ContactEntry.from_dict(item) for item in data.get("contacts", ())
        }
        self._validate_contact_identifiers()

    def save(self) -> None:
        _atomic_write_json(self.path, self.as_dict())

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": 1,
            "projects": [
                entry.as_dict() for entry in sorted(self._projects.values(), key=lambda e: e.id)
            ],
            "contacts": [
                entry.as_dict() for entry in sorted(self._contacts.values(), key=lambda e: e.id)
            ],
        }

    def save_project(self, entry: ProjectEntry) -> ProjectEntry:
        self._projects[entry.id] = entry
        self.save()
        return entry

    def create_project(self, entry: ProjectEntry) -> ProjectEntry:
        if entry.id in self._projects:
            raise RegistryConflict(f"project {entry.id!r} already exists")
        return self.save_project(entry)

    def update_project(self, entry: ProjectEntry) -> ProjectEntry:
        if entry.id not in self._projects:
            raise RegistryError(f"missing project: {entry.id}")
        return self.save_project(entry)

    def get_project(self, project_id: str) -> ProjectEntry | None:
        return self._projects.get(project_id)

    def get_visible_project(self, project_id: str, requester_id: str) -> ProjectEntry | None:
        project = self.get_project(project_id)
        if project is None or not is_visible_to(project, requester_id):
            return None
        return project

    def delete_project(self, project_id: str) -> bool:
        existed = self._projects.pop(project_id, None) is not None
        if existed:
            self.save()
        return existed

    def list_projects(self, requester_id: str, *, include_archived: bool = False) -> list[ProjectEntry]:
        projects = [
            project
            for project in self._projects.values()
            if is_visible_to(project, requester_id)
            and (include_archived or project.status != "archived")
        ]
        return sorted(projects, key=lambda p: p.name.lower())

    def resolve_project(self, query: str, requester_id: str) -> EntityResolution:
        return _resolve_entity(query, self.list_projects(requester_id))

    def resolve_project_repo(
        self,
        project_id: str,
        requester_id: str,
        repo_name: str | None = None,
    ) -> RepoResolution:
        project = self.get_visible_project(project_id, requester_id)
        if project is None:
            return RepoResolution(status="not_found", reason="project not visible")
        return resolve_repo(project, repo_name)

    def save_contact(self, entry: ContactEntry) -> ContactEntry:
        self._assert_contact_identifiers_available(entry)
        self._contacts[entry.id] = entry
        self.save()
        return entry

    def create_contact(self, entry: ContactEntry) -> ContactEntry:
        if entry.id in self._contacts:
            raise RegistryConflict(f"contact {entry.id!r} already exists")
        return self.save_contact(entry)

    def update_contact(self, entry: ContactEntry) -> ContactEntry:
        if entry.id not in self._contacts:
            raise RegistryError(f"missing contact: {entry.id}")
        return self.save_contact(entry)

    def get_contact(self, contact_id: str) -> ContactEntry | None:
        return self._contacts.get(contact_id)

    def get_visible_contact(self, contact_id: str, requester_id: str) -> ContactEntry | None:
        contact = self.get_contact(contact_id)
        if contact is None or contact.status != "active" or not is_visible_to(contact, requester_id):
            return None
        return contact

    def delete_contact(self, contact_id: str) -> bool:
        existed = self._contacts.pop(contact_id, None) is not None
        if existed:
            self.save()
        return existed

    def list_contacts(self, requester_id: str, *, include_merged: bool = False) -> list[ContactEntry]:
        contacts = [
            contact
            for contact in self._contacts.values()
            if is_visible_to(contact, requester_id)
            and (include_merged or contact.status == "active")
        ]
        return sorted(contacts, key=lambda c: c.display_name.lower())

    def resolve_contact(self, query: str, requester_id: str) -> EntityResolution:
        return _resolve_entity(query, self.list_contacts(requester_id))

    def resolve_contact_identifier(
        self, kind: Literal["phone", "email"], value: str, requester_id: str
    ) -> ContactEntry | None:
        key = _identifier_key(kind, value)
        for contact in self.list_contacts(requester_id):
            if key in contact.identifiers.keys:
                return contact
        return None

    def merge_contacts(self, survivor_id: str, loser_id: str) -> ContactEntry:
        if survivor_id == loser_id:
            raise RegistryError("cannot merge a contact into itself")
        survivor = self._contacts.get(survivor_id)
        loser = self._contacts.get(loser_id)
        if survivor is None:
            raise RegistryError(f"missing surviving contact: {survivor_id}")
        if loser is None:
            raise RegistryError(f"missing merged contact: {loser_id}")
        if survivor.status != "active":
            raise RegistryError("surviving contact must be active")

        merged_identifiers = survivor.identifiers.merged(loser.identifiers)
        merged_aliases = _unique_strings(
            survivor.aliases + loser.aliases + (loser.display_name,)
        )
        updated_survivor = replace(
            survivor,
            aliases=merged_aliases,
            identifiers=merged_identifiers,
            members=_unique_strings(survivor.members + loser.members),
        )
        inert_loser = replace(
            loser,
            aliases=(),
            identifiers=ContactIdentifiers(),
            status="merged",
            merged_into=survivor.id,
        )

        self._contacts[survivor.id] = updated_survivor
        self._contacts[loser.id] = inert_loser
        self._validate_contact_identifiers()
        self.save()
        self._after_contact_merge(updated_survivor, inert_loser)
        return updated_survivor

    def _after_contact_merge(self, survivor: ContactEntry, loser: ContactEntry) -> None:
        # TODO(memory-step): copy explicit Honcho conclusions from loser.peer_id
        # to survivor.peer_id once the curation/outbox lane exists.
        _ = (survivor, loser)

    def _validate_contact_identifiers(self) -> None:
        seen: dict[str, str] = {}
        for contact in self._contacts.values():
            if contact.status != "active":
                continue
            for key in contact.identifiers.keys:
                other = seen.setdefault(key, contact.id)
                if other != contact.id:
                    raise RegistryConflict(
                        f"identifier {key!r} belongs to both {other!r} and {contact.id!r}"
                    )

    def _assert_contact_identifiers_available(self, entry: ContactEntry) -> None:
        if entry.status != "active":
            return
        for contact in self._contacts.values():
            if contact.id == entry.id or contact.status != "active":
                continue
            overlap = set(entry.identifiers.keys) & set(contact.identifiers.keys)
            if overlap:
                key = sorted(overlap)[0]
                raise RegistryConflict(f"identifier {key!r} already belongs to {contact.id!r}")


def is_visible_to(entry: ProjectEntry | ContactEntry, requester_id: str) -> bool:
    requester = (requester_id or "").strip()
    if not requester:
        return False
    if entry.visibility == "household":
        return True
    if requester == entry.owner:
        return True
    if entry.visibility == "shared":
        return requester in entry.members
    return False


def resolve_repo(project: ProjectEntry, repo_name: str | None = None) -> RepoResolution:
    repos = project.repos
    speakable = tuple(repo.name for repo in repos)
    if not repos:
        return RepoResolution(status="not_found", reason="project has no repos")

    if repo_name and repo_name.strip():
        resolution = _resolve_named_repo(repo_name, repos)
        if resolution.status == "matched":
            return resolution
        return RepoResolution(
            status="not_found",
            speakable_names=speakable,
            reason="repo name did not match",
        )

    defaults = [repo for repo in repos if repo.default]
    if defaults:
        return RepoResolution(status="matched", repo=defaults[0], reason="default repo")
    if len(repos) == 1:
        return RepoResolution(status="matched", repo=repos[0], reason="only repo")
    return RepoResolution(
        status="ambiguous",
        speakable_names=speakable,
        reason="project has multiple repos and no default",
    )


def _resolve_named_repo(query: str, repos: tuple[RepoEntry, ...]) -> RepoResolution:
    ranked = sorted(
        ((_phrase_score(query, repo.name), repo) for repo in repos),
        key=lambda item: item[0],
        reverse=True,
    )
    if not ranked or ranked[0][0] < 0.70:
        return RepoResolution(status="not_found", speakable_names=tuple(repo.name for repo in repos))
    if len(ranked) > 1 and ranked[0][0] - ranked[1][0] <= 0.03:
        return RepoResolution(
            status="ambiguous",
            speakable_names=tuple(repo.name for _, repo in ranked[:3]),
            reason="repo name is ambiguous",
        )
    return RepoResolution(status="matched", repo=ranked[0][1], reason="explicit repo")


def _resolve_entity(
    query: str, entries: list[ProjectEntry] | list[ContactEntry]
) -> EntityResolution:
    if not query.strip():
        return EntityResolution(status="not_found")
    ranked: list[tuple[float, ProjectEntry | ContactEntry]] = []
    for entry in entries:
        score = max(_phrase_score(query, name) for name in _entity_names(entry))
        if score >= 0.68:
            ranked.append((score, entry))
    ranked.sort(key=lambda item: item[0], reverse=True)
    if not ranked:
        return EntityResolution(status="not_found")
    top_score = ranked[0][0]
    near = tuple(entry for score, entry in ranked if top_score - score <= 0.03)
    if len(near) > 1:
        return EntityResolution(
            status="ambiguous",
            candidates=near,
            speakable_names=tuple(_display_name(entry) for entry in near),
            score=top_score,
        )
    return EntityResolution(status="matched", entry=ranked[0][1], score=top_score)


def _entity_names(entry: ProjectEntry | ContactEntry) -> tuple[str, ...]:
    if isinstance(entry, ProjectEntry):
        return (entry.name, *entry.aliases)
    return (entry.display_name, *entry.aliases)


def _display_name(entry: ProjectEntry | ContactEntry) -> str:
    return entry.name if isinstance(entry, ProjectEntry) else entry.display_name


def _phrase_score(query: str, candidate: str) -> float:
    left = _normalize_phrase(query)
    right = _normalize_phrase(candidate)
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    if len(left) >= 3 and (left in right or right in left):
        return 0.94
    return max(
        SequenceMatcher(None, left, right).ratio(),
        SequenceMatcher(None, _token_sort(left), _token_sort(right)).ratio(),
    )


def _normalize_phrase(value: str) -> str:
    value = value.lower().replace("&", " and ")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def _token_sort(value: str) -> str:
    return " ".join(sorted(value.split()))


def _validate_slug(value: str, label: str) -> None:
    if not _SLUG_RE.match(value):
        raise RegistryError(f"{label} must be a stable slug")


def _validate_repos(repos: tuple[RepoEntry, ...]) -> None:
    defaults = [repo for repo in repos if repo.default]
    if len(defaults) > 1:
        raise RegistryError("project repos may contain at most one default")
    names: set[str] = set()
    for repo in repos:
        name = repo.name.lower()
        if name in names:
            raise RegistryError(f"duplicate repo name: {repo.name}")
        names.add(name)


def _with_owner(owner: str, members: tuple[str, ...]) -> tuple[str, ...]:
    return _unique_strings((owner.strip(), *members))


def _unique_strings(values: Any) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values or ():
        item = str(value).strip()
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return tuple(result)


def _identifier_key(kind: Literal["phone", "email"], value: str) -> str:
    if kind == "phone":
        return f"phone:{_normalize_phone(value)}"
    return f"email:{value.strip().lower()}"


def _normalize_phone(value: str) -> str:
    value = str(value).strip()
    prefix = "+" if value.startswith("+") else ""
    digits = re.sub(r"\D+", "", value)
    return f"{prefix}{digits}" if digits else ""


def _atomic_write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        tmp = Path(handle.name)
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    try:
        dir_fd = os.open(path.parent, os.O_DIRECTORY)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
