from __future__ import annotations

from jarvis.config import VADConfig
from jarvis.intercom.vad import SileroVAD


def test_webrtc_vad_accepts_existing_intercom_frame_size() -> None:
    vad = SileroVAD(VADConfig(engine="webrtc"))

    assert vad.prob(b"\0" * 1024) == 0.0
