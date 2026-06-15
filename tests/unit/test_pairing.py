"""Per-device pairing (Phase 3d §3) — tokens bound to devices, pinned identities."""

from __future__ import annotations

from jarvis.brain.server import authorise_device
from jarvis.config import BrainConfig, DeviceAuth


def _brain(**kw) -> BrainConfig:  # noqa: ANN003
    return BrainConfig(_env_file=None, **kw)


def test_no_tokens_is_open_dev_mode() -> None:
    ok, ident = authorise_device(_brain(), "anything", "")
    assert ok is True
    assert ident == ""


def test_shared_token_accepts_any_device() -> None:
    brain = _brain(pairing_token="shared")
    assert authorise_device(brain, "room-pi", "shared") == (True, "")
    assert authorise_device(brain, "room-pi", "wrong") == (False, "")


def test_per_device_token_is_bound_and_pins_identity() -> None:
    brain = _brain(
        devices=[
            DeviceAuth(token="pi-secret", device_id="room-pi"),
            DeviceAuth(token="mac-secret", device_id="local-mac", identity="neil"),
        ]
    )
    # right token + right device → ok, with the pinned default identity
    assert authorise_device(brain, "local-mac", "mac-secret") == (True, "neil")
    assert authorise_device(brain, "room-pi", "pi-secret") == (True, "")
    # a leaked Pi token cannot impersonate the Mac (token bound to its device)
    assert authorise_device(brain, "local-mac", "pi-secret") == (False, "")
    # unknown token rejected
    assert authorise_device(brain, "room-pi", "nope") == (False, "")
