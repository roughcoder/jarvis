from __future__ import annotations

import logging
import sys

from jarvis.config import VADConfig
from jarvis.intercom.vad import SileroVAD


def test_webrtc_vad_accepts_existing_intercom_frame_size() -> None:
    vad = SileroVAD(VADConfig(engine="webrtc"))

    assert vad.prob(b"\0" * 1024) == 0.0


def test_webrtc_vad_fallback_logs_warning(caplog, monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setitem(sys.modules, "webrtcvad", None)
    vad = SileroVAD(VADConfig(engine="webrtc"))

    with caplog.at_level(logging.WARNING, logger="jarvis.intercom.vad"):
        assert vad.prob(b"\0" * 1024) == 0.0

    assert "webrtcvad is unavailable" in caplog.text
    assert "not production audio devices" in caplog.text
