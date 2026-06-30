from jarvis.intercom.pi_panel import _configure_fullscreen_root


class FakeRoot:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def title(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("title", args, kwargs))

    def configure(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("configure", args, kwargs))

    def overrideredirect(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("overrideredirect", args, kwargs))

    def attributes(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("attributes", args, kwargs))

    def geometry(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("geometry", args, kwargs))

    def update_idletasks(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("update_idletasks", args, kwargs))

    def winfo_screenwidth(self) -> int:
        return 800

    def winfo_screenheight(self) -> int:
        return 480

    def lift(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("lift", args, kwargs))

    def focus_force(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("focus_force", args, kwargs))

    def withdraw(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("withdraw", args, kwargs))

    def deiconify(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("deiconify", args, kwargs))


def test_pi_panel_window_defaults_to_fullscreen_then_screen_sized_borderless() -> None:
    root = FakeRoot()

    _configure_fullscreen_root(root)

    assert ("title", ("Jarvis",), {}) in root.calls
    assert ("configure", (), {"bg": "#05070a"}) in root.calls
    assert ("overrideredirect", (True,), {}) in root.calls
    assert ("attributes", ("-fullscreen", True), {}) in root.calls
    assert ("geometry", ("800x480+0+0",), {}) in root.calls
    assert root.calls.index(("attributes", ("-fullscreen", True), {})) < root.calls.index(
        ("overrideredirect", (True,), {})
    )


def test_pi_panel_window_can_pin_geometry_for_dsi_panel() -> None:
    root = FakeRoot()

    _configure_fullscreen_root(root, geometry="800x480+0+0")

    assert ("overrideredirect", (True,), {}) in root.calls
    assert ("geometry", ("800x480+0+0",), {}) in root.calls
    assert not any(call[0] == "attributes" for call in root.calls)


def test_pi_panel_control_reports_visibility(monkeypatch) -> None:  # noqa: ANN001
    from jarvis.config import IntercomDeviceConfig
    from jarvis.intercom.pi_panel import PiPanel

    monkeypatch.setenv("DISPLAY", ":0")
    panel = PiPanel(
        IntercomDeviceConfig(
            _env_file=None,
            pi_panel="true",
            pi_panel_show_cmd="",
            pi_panel_hide_cmd="",
            pi_panel_status_cmd="",
        )
    )

    assert panel.control("status")["status"] == "visible"
    assert panel.control("hide")["status"] == "hidden"
    assert panel.control("show")["status"] == "visible"
    assert panel.control("toggle")["status"] == "hidden"


def test_pi_panel_control_runs_configured_command(tmp_path) -> None:
    from jarvis.config import IntercomDeviceConfig
    from jarvis.intercom.pi_panel import PiPanel

    marker = tmp_path / "shown"
    panel = PiPanel(
        IntercomDeviceConfig(
            _env_file=None,
            pi_panel="false",
            pi_panel_show_cmd=f"touch {marker}",
            pi_panel_hide_cmd="",
            pi_panel_status_cmd="",
        )
    )

    result = panel.control("show")

    assert result["status"] == "visible"
    assert marker.exists()
    assert "exit 0" in result["command"]


def test_pi_panel_control_reports_command_failure() -> None:
    from jarvis.config import IntercomDeviceConfig
    from jarvis.intercom.pi_panel import PiPanel

    panel = PiPanel(
        IntercomDeviceConfig(
            _env_file=None,
            pi_panel="false",
            pi_panel_show_cmd="false",
            pi_panel_hide_cmd="",
            pi_panel_status_cmd="",
        )
    )

    result = panel.control("show")

    assert result["status"] == "command_failed"
    assert "exit 1" in result["command"]
