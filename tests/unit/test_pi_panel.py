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

    def winfo_screenwidth(self) -> int:
        return 800

    def winfo_screenheight(self) -> int:
        return 480

    def lift(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("lift", args, kwargs))

    def focus_force(self, *args, **kwargs):  # noqa: ANN001, ANN202
        self.calls.append(("focus_force", args, kwargs))


def test_pi_panel_window_is_borderless_and_fullscreen_by_default() -> None:
    root = FakeRoot()

    _configure_fullscreen_root(root)

    assert ("title", ("Jarvis",), {}) in root.calls
    assert ("configure", (), {"bg": "#05070a"}) in root.calls
    assert ("overrideredirect", (True,), {}) in root.calls
    assert ("attributes", ("-fullscreen", True), {}) in root.calls
    assert not any(call[0] == "geometry" for call in root.calls)


def test_pi_panel_window_can_pin_geometry_for_dsi_panel() -> None:
    root = FakeRoot()

    _configure_fullscreen_root(root, geometry="800x480+0+0")

    assert ("overrideredirect", (True,), {}) in root.calls
    assert ("geometry", ("800x480+0+0",), {}) in root.calls
    assert not any(call[0] == "attributes" for call in root.calls)
