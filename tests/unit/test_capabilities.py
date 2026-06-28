"""Capability gate — deny-by-default enforcement + profile resolution (Phase 3).

The privacy/capability wall the review (HIGH #1) required to exist before any
tool. These tests lock deny-by-default: nothing is granted implicitly.
"""

from __future__ import annotations

import pytest

from jarvis.brain.capabilities import (
    CapabilityError,
    build_request_context,
    parse_profile_capabilities,
    require,
    resolve_capabilities,
)
from jarvis.brain.context import RequestContext
from jarvis.brain.server import BrainServer
from jarvis.config import CapabilityConfig


def _ctx(*caps: str) -> RequestContext:
    return RequestContext(
        device_id="dev", identity="house", scope="house", capabilities=frozenset(caps)
    )


# --- the gate --------------------------------------------------------------


def test_require_passes_when_granted() -> None:
    require(_ctx("web.search"), "web.search")  # no raise


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
