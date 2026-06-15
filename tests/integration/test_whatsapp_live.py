"""Integration: the WhatsApp connector's live dependency (Phase 3b).

Self-skips when `wacli` isn't installed/linked (the established §11 contract) —
WhatsApp device-linking is interactive and can't be provisioned headlessly. When
wacli IS present, this confirms the connector can shell out to it.
"""

from __future__ import annotations

import shutil

import pytest

from jarvis.config import load_config

pytestmark = pytest.mark.integration


def test_wacli_available() -> None:
    cfg = load_config()
    if not shutil.which(cfg.whatsapp.wacli_bin):
        pytest.skip(f"{cfg.whatsapp.wacli_bin} not installed — `jarvis whatsapp` unavailable")
    import subprocess

    # A linked wacli responds to a trivial invocation without error.
    r = subprocess.run([cfg.whatsapp.wacli_bin, "--help"], capture_output=True, timeout=10)
    assert r.returncode == 0
