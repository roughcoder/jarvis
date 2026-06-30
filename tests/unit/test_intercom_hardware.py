from jarvis.config import IntercomDeviceConfig
from jarvis.intercom.hardware import IntercomHardware


def test_display_capability_advertised_for_preview_panel_url() -> None:
    cfg = IntercomDeviceConfig(
        _env_file=None,
        camera="false",
        pi_panel="false",
        pi_panel_url="http://127.0.0.1:8787",
    )

    assert "display" in IntercomHardware(cfg).capabilities()
