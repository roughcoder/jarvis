"""Capability gate — deny-by-default enforcement + profile resolution (Phase 3).

The privacy/capability wall the review (HIGH #1) required to exist before any
tool. These tests lock deny-by-default: nothing is granted implicitly.
"""

from __future__ import annotations

import pytest

from jarvis.brain.capabilities import (
    CapabilityError,
    build_request_context,
    can_query_memory_peer,
    can_write_memory_peer,
    parse_profile_capabilities,
    require,
    resolve_capabilities,
)
from jarvis.brain.context import RequestContext
from jarvis.brain.registry import ContactEntry, ProjectEntry, RegistryStore
from jarvis.brain.server import BrainServer
from jarvis.config import CapabilityConfig
from jarvis.users import User


def _ctx(*caps: str) -> RequestContext:
    return RequestContext(
        device_id="dev", identity="house", scope="house", capabilities=frozenset(caps)
    )


# --- the gate --------------------------------------------------------------


def test_require_passes_when_granted() -> None:
    assert require(_ctx("web.search"), "web.search") is None  # grants by not raising


def test_require_denies_when_not_granted() -> None:
    with pytest.raises(CapabilityError):
        require(_ctx("web.search"), "files.write")


def test_empty_context_denies_everything() -> None:
    ctx = _ctx()  # deny-by-default
    assert ctx.can("web.search") is False
    for cap in ("web.search", "files.read", "files.write"):
        with pytest.raises(CapabilityError):
            require(ctx, cap)


# --- profile parsing -------------------------------------------------------


def test_parse_block_form() -> None:
    text = "---\ncapabilities:\n  - web.search\n  - files.read\n---\n\n# notes\n"
    assert parse_profile_capabilities(text) == {"web.search", "files.read"}


def test_parse_inline_form() -> None:
    text = "---\ncapabilities: [web.search, files.read]\n---\n"
    assert parse_profile_capabilities(text) == {"web.search", "files.read"}


def test_parse_no_frontmatter_is_empty() -> None:
    assert parse_profile_capabilities("# just a heading\nno front matter\n") == set()


def test_parse_block_stops_at_next_key() -> None:
    text = "---\ncapabilities:\n  - web.search\nnotes: hello\n---\n"
    assert parse_profile_capabilities(text) == {"web.search"}


# --- resolution / build ----------------------------------------------------


def test_resolve_from_profile_file(tmp_path) -> None:
    (tmp_path / "kitchen-pi.md").write_text(
        "---\ncapabilities:\n  - web.search\n---\n"
    )
    cfg = CapabilityConfig(
        _env_file=None, device_id="kitchen-pi", profiles_dir=str(tmp_path)
    )
    assert resolve_capabilities(cfg) == {"web.search"}


def test_resolve_falls_back_to_csv_default(tmp_path) -> None:
    cfg = CapabilityConfig(
        _env_file=None,
        device_id="no-profile",
        profiles_dir=str(tmp_path),
        default_capabilities="web.search, files.read",
    )
    assert resolve_capabilities(cfg) == {"web.search", "files.read"}


def test_resolve_deny_by_default_when_nothing_configured(tmp_path) -> None:
    cfg = CapabilityConfig(_env_file=None, device_id="bare", profiles_dir=str(tmp_path))
    assert resolve_capabilities(cfg) == set()


def test_build_request_context_carries_identity_and_caps(tmp_path) -> None:
    cfg = CapabilityConfig(
        _env_file=None,
        device_id="neil-mac",
        identity="neil",
        scope="personal",
        profiles_dir=str(tmp_path),
        default_capabilities="web.search",
    )
    ctx = build_request_context(cfg)
    assert ctx.device_id == "neil-mac"
    assert ctx.identity == "neil"
    assert ctx.scope == "personal"
    assert ctx.can("web.search")
    assert not ctx.can("files.write")


def test_live_hardware_filters_intercom_caps() -> None:
    ctx = RequestContext(
        "kitchen-pi",
        "house",
        "house",
        frozenset({"web.search", "intercom.camera", "intercom.display"}),
    )

    with_camera = BrainServer._with_live_hardware(ctx, {"camera"})
    assert with_camera.capabilities == frozenset({"web.search", "intercom.camera"})

    without_hardware = BrainServer._with_live_hardware(ctx, set())
    assert without_hardware.capabilities == frozenset({"web.search"})


# --- memory access matrix -------------------------------------------------


def _personal(identity: str, peer: str | None = None) -> RequestContext:
    return RequestContext(
        "dev",
        identity,
        "personal",
        frozenset({"memory.query", "memory.curate"}),
        peer=peer or identity,
    )


def _memory_registry(tmp_path) -> RegistryStore:  # noqa: ANN001
    store = RegistryStore(tmp_path / "registry.json")
    store.save_contact(
        ContactEntry(
            id="klaus",
            display_name="Klaus",
            owner="neil",
            visibility="shared",
            members=("neil", "jules"),
        )
    )
    store.save_contact(
        ContactEntry(
            id="private",
            display_name="Private",
            owner="neil",
            visibility="private",
            members=("neil",),
        )
    )
    store.save_project(
        ProjectEntry(
            id="jarvis",
            name="Jarvis",
            owner="neil",
            visibility="shared",
            members=("neil", "jules"),
        )
    )
    store.save_project(
        ProjectEntry(
            id="private",
            name="Private",
            owner="neil",
            visibility="private",
            members=("neil",),
        )
    )
    return store


def test_memory_access_matrix_allows_own_contacts_projects_and_guardian(tmp_path) -> None:
    registry = _memory_registry(tmp_path)
    users = {
        "neil": User("neil", trust_tier="guardian"),
        "alice": User("alice", trust_tier="minor", guardians=("neil",)),
        "jules": User("jules", trust_tier="adult"),
    }

    assert can_query_memory_peer(_personal("neil"), "neil", registry=registry, users=users).allowed
    assert can_query_memory_peer(_personal("jules"), "contact:klaus", registry=registry, users=users).allowed
    assert can_query_memory_peer(_personal("jules"), "project:jarvis", registry=registry, users=users).allowed
    assert can_query_memory_peer(_personal("neil"), "alice", registry=registry, users=users).allowed


def test_memory_access_matrix_denies_by_default_and_unowned_target_views(tmp_path) -> None:
    registry = _memory_registry(tmp_path)
    users = {
        "neil": User("neil", trust_tier="guardian"),
        "alice": User("alice", trust_tier="minor", guardians=("neil",)),
        "jules": User("jules", trust_tier="adult"),
    }

    assert not can_query_memory_peer(_ctx(), "neil", registry=registry, users=users).allowed
    assert not can_query_memory_peer(_personal("jules"), "contact:private", registry=registry, users=users).allowed
    assert not can_query_memory_peer(_personal("jules"), "project:private", registry=registry, users=users).allowed
    assert not can_query_memory_peer(_personal("jules"), "alice", registry=registry, users=users).allowed
    assert not can_query_memory_peer(
        _personal("neil"),
        "project:jarvis",
        registry=registry,
        users=users,
        target="jules",
    ).allowed


def test_memory_write_matrix_excludes_guardian_read_right(tmp_path) -> None:
    registry = _memory_registry(tmp_path)
    neil = _personal("neil")

    assert can_write_memory_peer(neil, "neil", registry=registry).allowed
    assert can_write_memory_peer(neil, "contact:klaus", registry=registry).allowed
    assert can_write_memory_peer(neil, "project:jarvis", registry=registry).allowed
    assert not can_write_memory_peer(neil, "alice", registry=registry).allowed
