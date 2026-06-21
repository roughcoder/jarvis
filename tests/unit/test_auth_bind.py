"""Bind-time auth guard — don't expose unauthenticated access on a non-loopback bind."""

from __future__ import annotations

from jarvis.config import insecure_bind, is_loopback


def test_is_loopback() -> None:
    assert is_loopback("localhost") and is_loopback("127.0.0.1") and is_loopback("::1")
    assert not is_loopback("0.0.0.0")  # all interfaces — reachable from the network
    assert not is_loopback("192.168.1.5") and not is_loopback("hive.local")


def test_insecure_bind_refuses_only_unauth_network() -> None:
    # loopback is always fine, token or not
    assert insecure_bind("127.0.0.1", has_token=False, allow_insecure=False) is False
    # non-loopback with no token and no override → refuse (True)
    assert insecure_bind("0.0.0.0", has_token=False, allow_insecure=False) is True
    assert insecure_bind("192.168.1.5", has_token=False, allow_insecure=False) is True
    # a token makes a non-loopback bind safe
    assert insecure_bind("0.0.0.0", has_token=True, allow_insecure=False) is False
    # explicit override permits it (local dev)
    assert insecure_bind("0.0.0.0", has_token=False, allow_insecure=True) is False
