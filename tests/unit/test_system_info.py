from __future__ import annotations

import threading
import time

from jarvis import system_info as system_info_module


def test_system_info_cached_refreshes_in_background(monkeypatch) -> None:  # noqa: ANN001
    started = threading.Event()
    release = threading.Event()
    monkeypatch.setattr(system_info_module, "_CACHE", None)
    monkeypatch.setattr(system_info_module, "_CACHE_REFRESHING", False)

    def collect() -> dict:
        started.set()
        release.wait(timeout=2)
        return {
            "hostname": "worker-laptop",
            "platform": "darwin",
            "arch": "arm64",
            "disk": [],
            "checked_at": None,
        }

    monkeypatch.setattr(system_info_module, "_collect_system_info", collect)

    before = time.monotonic()
    first = system_info_module.system_info_cached(cache_ttl_s=10)
    elapsed = time.monotonic() - before

    assert elapsed < 0.5
    assert first["hostname"] is None
    assert first["checked_at"]
    assert started.wait(timeout=1)

    release.set()
    for _ in range(20):
        second = system_info_module.system_info_cached(cache_ttl_s=10)
        if second["hostname"] == "worker-laptop":
            break
        time.sleep(0.05)

    assert second["hostname"] == "worker-laptop"
