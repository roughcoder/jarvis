"""Identity resolution + per-user context (Phase 3d §5).

Pure logic: users parsed from markdown front-matter, the trust-tier resolver
(strong / claimed / unknown), and the per-utterance RequestContext built from a
resolution. Plus the know-or-ask sticky-claim behaviour.
"""

from __future__ import annotations

from jarvis.brain.capabilities import context_for_resolution
from jarvis.brain.identity import (
    HOUSE,
    IdentityResolver,
    Resolution,
    load_users,
    parse_user,
)
from jarvis.config import CapabilityConfig

_NEIL = """---
devices: [neil-mac]
whatsapp: ["+441234567890"]
claims: ["it's neil", "neil here"]
capabilities: [mcp.notion, mcp.linear]
scope: personal
honcho_peer: neil
---
# Neil
"""

_JULES = """---
devices: []
claims: ["it's jules", "this is jules"]
capabilities: [mcp.granola]
scope: personal
---
# Jules
"""


def _resolver() -> IdentityResolver:
    return IdentityResolver({"neil": parse_user("neil", _NEIL), "jules": parse_user("jules", _JULES)})


def test_parse_user_front_matter() -> None:
    u = parse_user("neil", _NEIL)
    assert u.devices == frozenset({"neil-mac"})
    assert "+441234567890" in u.whatsapp
    assert u.capabilities == frozenset({"mcp.notion", "mcp.linear"})
    assert u.scope == "personal"
    assert u.peer == "neil"


def test_strong_identity_from_bound_device() -> None:
    r = _resolver().resolve(device_id="neil-mac", channel="voice", utterance="what's the weather")
    assert r == Resolution("neil", "personal", "strong", r.user)
    assert r.confidence == "strong"


def test_strong_identity_from_whatsapp_number() -> None:
    r = _resolver().resolve(device_id="wa", channel="whatsapp", asserted="+441234567890")
    assert r.identity == "neil"
    assert r.confidence == "strong"


def test_claimed_identity_on_shared_device() -> None:
    r = _resolver().resolve(device_id="room-pi", channel="voice", utterance="it's Jules, what's on today")
    assert r.identity == "jules"
    assert r.scope == "personal"
    assert r.confidence == "claimed"


def test_unknown_speaker_falls_back_to_house() -> None:
    r = _resolver().resolve(device_id="room-pi", channel="voice", utterance="what time is it")
    assert r.identity == HOUSE
    assert r.scope == HOUSE
    assert r.confidence == "unknown"
    assert r.known is False


def test_load_users_from_dir(tmp_path) -> None:  # noqa: ANN001
    (tmp_path / "neil.md").write_text(_NEIL)
    (tmp_path / "jules.md").write_text(_JULES)
    users = load_users(str(tmp_path))
    assert set(users) == {"neil", "jules"}
    assert load_users(str(tmp_path / "missing")) == {}  # no dir => house-only


def test_context_adds_user_grants_only_in_personal_scope(tmp_path) -> None:  # noqa: ANN001
    # device profile grants files.read; the user adds their MCP servers
    (tmp_path / "neil-mac.md").write_text("---\ncapabilities: [files.read]\n---\n")
    cfg = CapabilityConfig(_env_file=None, device_id="neil-mac", profiles_dir=str(tmp_path))
    neil = parse_user("neil", _NEIL)

    personal = context_for_resolution(cfg, Resolution("neil", "personal", "strong", neil))
    assert {"files.read", "mcp.notion", "mcp.linear"} <= personal.capabilities
    assert personal.memory_peer == "neil"

    # house scope (unknown speaker) gets only the device's caps, none of Neil's
    house = context_for_resolution(cfg, Resolution(HOUSE, HOUSE, "unknown", None))
    assert house.capabilities == frozenset({"files.read"})
    assert "mcp.notion" not in house.capabilities
    assert house.memory_peer == HOUSE
