from __future__ import annotations

import json

import pytest

from jarvis.brain.registry import (
    ContactEntry,
    ContactIdentifiers,
    ProjectEntry,
    ProjectLinks,
    RegistryConflict,
    RegistryError,
    RegistryStore,
    RepoEntry,
    resolve_repo,
)


def _project(**overrides: object) -> ProjectEntry:
    values = {
        "id": "jarvis",
        "name": "Jarvis",
        "aliases": ("the jarvis project", "jarvis"),
        "owner": "neil",
        "members": ("neil",),
        "visibility": "household",
        "status": "active",
        "repos": (
            RepoEntry("runtime", "roughcoder/jarvis", default=True),
            RepoEntry("cockpit", "roughcoder/jarvis-cockpit"),
        ),
        "links": ProjectLinks(jira="JARV", urls=("https://example.test/jarvis",)),
        "files_root": "jarvis-workspace/projects/jarvis/files",
    }
    values.update(overrides)
    return ProjectEntry(**values)


def _contact(**overrides: object) -> ContactEntry:
    values = {
        "id": "klaus",
        "display_name": "Klaus Schmidt",
        "aliases": ("Klaus from work",),
        "relationship": "colleague",
        "identifiers": ContactIdentifiers(phones=("+1 555 0100",)),
        "owner": "neil",
        "visibility": "private",
        "members": ("neil",),
        "created_from": "curated",
    }
    values.update(overrides)
    return ContactEntry(**values)


def test_project_entry_shape_derives_peer_id_and_validates_default_repo() -> None:
    project = _project()

    assert project.peer_id == "project:jarvis"
    assert project.as_dict() == {
        "id": "jarvis",
        "name": "Jarvis",
        "peer_id": "project:jarvis",
        "aliases": ["the jarvis project", "jarvis"],
        "owner": "neil",
        "members": ["neil"],
        "visibility": "household",
        "status": "active",
        "repos": [
            {"name": "runtime", "remote": "roughcoder/jarvis", "default": True},
            {"name": "cockpit", "remote": "roughcoder/jarvis-cockpit"},
        ],
        "links": {"jira": "JARV", "urls": ["https://example.test/jarvis"]},
        "files_root": "jarvis-workspace/projects/jarvis/files",
    }

    with pytest.raises(RegistryError, match="at most one default"):
        _project(
            repos=(
                RepoEntry("runtime", "roughcoder/jarvis", default=True),
                RepoEntry("infra", "roughcoder/jarvis-infra", default=True),
            )
        )

    with pytest.raises(RegistryError, match="project peer_id"):
        ProjectEntry.from_dict({**project.as_dict(), "peer_id": "project:wrong"})


def test_contact_entry_shape_normalizes_identifiers_and_peer_id() -> None:
    contact = _contact()

    assert contact.peer_id == "contact:klaus"
    assert contact.identifiers.phones == ("+15550100",)
    assert contact.as_dict()["identifiers"] == {"phones": ["+15550100"], "emails": []}

    with pytest.raises(RegistryError, match="contact peer_id"):
        ContactEntry.from_dict({**contact.as_dict(), "peer_id": "contact:wrong"})


def test_alias_resolution_is_fuzzy_and_membership_filtered(tmp_path) -> None:
    store = RegistryStore(tmp_path / "registry.json")
    store.save_project(
        _project(
            id="bird-story",
            name="Bird-Time Story",
            aliases=("the bird time story project",),
            visibility="shared",
            members=("neil", "alice"),
            repos=(),
        )
    )
    store.save_project(
        _project(
            id="work-plan",
            name="Work Plan",
            aliases=("work planning",),
            visibility="private",
            members=("neil",),
            repos=(),
        )
    )

    assert store.resolve_project("bird time storey", "alice").entry.id == "bird-story"
    assert store.resolve_project("work plan", "alice").status == "not_found"
    assert [project.id for project in store.list_projects("alice")] == ["bird-story"]


def test_contact_identifier_dedupes_and_resolves_exactly(tmp_path) -> None:
    store = RegistryStore(tmp_path / "registry.json")
    store.save_contact(_contact())

    resolved = store.resolve_contact_identifier("phone", "+1 (555) 0100", "neil")
    assert resolved is not None
    assert resolved.id == "klaus"

    with pytest.raises(
        RegistryConflict,
        match="identifier already belongs to another contact",
    ) as exc_info:
        store.save_contact(
            _contact(
                id="klaus-duplicate",
                display_name="Klaus Duplicate",
                identifiers=ContactIdentifiers(phones=("+15550100",)),
            )
        )
    assert "klaus" not in str(exc_info.value)
    assert exc_info.value.conflicting_entry_id == "klaus"


def test_contact_name_resolution_fuzzy_and_visibility_filtered(tmp_path) -> None:
    store = RegistryStore(tmp_path / "registry.json")
    store.save_contact(_contact(visibility="shared", members=("neil", "jules")))
    store.save_contact(
        _contact(
            id="maria",
            display_name="Maria Garcia",
            aliases=("Maria from school",),
            identifiers=ContactIdentifiers(phones=("+15550101",)),
            visibility="private",
        )
    )

    assert store.resolve_contact("claus from werk", "jules").entry.id == "klaus"
    assert store.resolve_contact("maria", "jules").status == "not_found"


def test_visibility_semantics_for_project_and_contact_lists(tmp_path) -> None:
    store = RegistryStore(tmp_path / "registry.json")
    store.save_project(_project(id="home", name="Home", visibility="household", repos=()))
    store.save_project(
        _project(
            id="shared",
            name="Shared",
            visibility="shared",
            members=("neil", "jules"),
            repos=(),
        )
    )
    store.save_project(_project(id="private", name="Private", visibility="private", repos=()))
    store.save_contact(_contact(id="household", display_name="Household", visibility="household"))
    store.save_contact(
        _contact(
            id="shared-contact",
            display_name="Shared Contact",
            visibility="shared",
            members=("neil", "jules"),
            identifiers=ContactIdentifiers(phones=("+15550102",)),
        )
    )
    store.save_contact(
        _contact(
            id="private-contact",
            display_name="Private Contact",
            visibility="private",
            identifiers=ContactIdentifiers(phones=("+15550103",)),
        )
    )

    assert {project.id for project in store.list_projects("jules")} == {"home", "shared"}
    assert {contact.id for contact in store.list_contacts("jules")} == {
        "household",
        "shared-contact",
    }
    assert store.list_projects("") == []
    assert store.list_contacts("") == []


def test_membership_grants_private_visibility_to_lists_details_and_resolvers(tmp_path) -> None:
    store = RegistryStore(tmp_path / "registry.json")
    store.save_project(
        _project(
            id="private-project",
            name="Private Project",
            aliases=("secret workspace",),
            owner="alice",
            members=("alice", "neil"),
            visibility="private",
            repos=(RepoEntry("notes", "roughcoder/private-notes", default=True),),
        )
    )
    store.save_contact(
        _contact(
            id="private-contact",
            display_name="Private Contact",
            aliases=("private colleague",),
            owner="alice",
            members=("alice", "neil"),
            visibility="private",
            identifiers=ContactIdentifiers(emails=("private@example.test",)),
        )
    )

    assert [project.id for project in store.list_projects("neil")] == ["private-project"]
    assert store.get_visible_project("private-project", "neil").id == "private-project"
    assert store.resolve_project("secret workspace", "neil").entry.id == "private-project"
    assert store.resolve_project_repo("private-project", "neil").repo.name == "notes"

    assert [contact.id for contact in store.list_contacts("neil")] == ["private-contact"]
    assert store.get_visible_contact("private-contact", "neil").id == "private-contact"
    assert store.resolve_contact("private colleague", "neil").entry.id == "private-contact"
    assert (
        store.resolve_contact_identifier("email", "PRIVATE@example.test", "neil").id
        == "private-contact"
    )

    assert store.list_projects("jules") == []
    assert store.get_visible_project("private-project", "jules") is None
    assert store.resolve_project("secret workspace", "jules").status == "not_found"
    assert store.resolve_project_repo("private-project", "jules").status == "not_found"
    assert store.list_contacts("jules") == []
    assert store.get_visible_contact("private-contact", "jules") is None
    assert store.resolve_contact("private colleague", "jules").status == "not_found"
    assert store.resolve_contact_identifier("email", "private@example.test", "jules") is None


def test_repo_resolution_precedence_and_ambiguity() -> None:
    project = _project(
        repos=(
            RepoEntry("runtime", "roughcoder/jarvis", default=True),
            RepoEntry("cockpit", "roughcoder/jarvis-cockpit"),
            RepoEntry("infra", "roughcoder/jarvis-infra"),
        )
    )

    explicit = resolve_repo(project, "cock pit")
    assert explicit.status == "matched"
    assert explicit.repo.name == "cockpit"

    default = resolve_repo(project)
    assert default.status == "matched"
    assert default.repo.name == "runtime"

    ambiguous = resolve_repo(_project(repos=project.repos[1:]))
    assert ambiguous.status == "ambiguous"
    assert ambiguous.speakable_names == ("cockpit", "infra")


def test_shallow_contact_merge_repoints_identifiers_and_aliases(tmp_path) -> None:
    store = RegistryStore(tmp_path / "registry.json")
    store.save_contact(
        _contact(
            id="sarah",
            display_name="Sarah Jones",
            aliases=("Sarah from Berlin",),
            identifiers=ContactIdentifiers(phones=("+15550104",)),
        )
    )
    store.save_contact(
        _contact(
            id="sarah-wa",
            display_name="Sarah J",
            aliases=("Sarah WhatsApp",),
            identifiers=ContactIdentifiers(phones=("+15550105",), emails=("SARAH@example.test",)),
            created_from="auto_created_channel_sender",
        )
    )

    survivor = store.merge_contacts("sarah", "sarah-wa")
    loser = store.get_contact("sarah-wa")

    assert survivor.identifiers.phones == ("+15550104", "+15550105")
    assert survivor.identifiers.emails == ("sarah@example.test",)
    assert "Sarah WhatsApp" in survivor.aliases
    assert "Sarah J" in survivor.aliases
    assert loser.status == "merged"
    assert loser.merged_into == "sarah"
    assert loser.identifiers.keys == ()
    assert store.resolve_contact_identifier("phone", "+15550105", "neil").id == "sarah"
    assert store.resolve_contact("Sarah WhatsApp", "neil").entry.id == "sarah"


def test_atomic_persistence_round_trip(tmp_path) -> None:
    path = tmp_path / "registry.json"
    store = RegistryStore(path)
    store.save_project(_project())
    store.save_contact(_contact())

    loaded = RegistryStore(path)

    assert loaded.get_project("jarvis").peer_id == "project:jarvis"
    assert loaded.get_contact("klaus").peer_id == "contact:klaus"
    assert not list(tmp_path.glob("*.tmp"))
    json.loads(path.read_text(encoding="utf-8"))


def test_load_tolerates_empty_file_and_rejects_corruption(tmp_path) -> None:
    path = tmp_path / "registry.json"

    path.write_text("", encoding="utf-8")
    store = RegistryStore(path)
    assert store.list_projects("neil") == []

    path.write_text('{"projects": [', encoding="utf-8")
    with pytest.raises(RegistryError, match="not valid JSON"):
        RegistryStore(path)

    path.write_text("[]", encoding="utf-8")
    with pytest.raises(RegistryError, match="JSON object"):
        RegistryStore(path)
