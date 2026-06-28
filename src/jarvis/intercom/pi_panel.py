"""Optional Raspberry Pi touch panel for room intercoms.

Uses tkinter from the standard library so the Pi image does not need a heavy UI
dependency. If there is no graphical display, it simply does not start. The
ambient eyes are one view inside the panel, not the panel itself.
"""

from __future__ import annotations

import math
import os
import queue
import random
import threading
import time
from contextlib import suppress
from dataclasses import dataclass
from typing import Literal

from jarvis.config import IntercomDeviceConfig
from jarvis.intercom.hardware import IntercomHardware, _enabled

PanelMode = Literal["eyes", "status", "camera", "debug"]


@dataclass(frozen=True)
class EyeState:
    name: str
    openness: float
    pupil_y: float
    pupil_x: float = 0.0


_STATES = {
    "idle": EyeState("idle", 0.82, 0.0),
    "sleep": EyeState("sleep", 0.05, 0.0),
    "awake": EyeState("awake", 1.0, -0.04),
    "listening": EyeState("listening", 0.95, -0.08),
    "thinking": EyeState("thinking", 0.72, 0.03),
    "speaking": EyeState("speaking", 0.86, 0.0),
}


class PiPanel:
    def __init__(self, cfg: IntercomDeviceConfig, *, hardware: IntercomHardware | None = None) -> None:
        self._cfg = cfg
        self._hardware = hardware or IntercomHardware(cfg)
        self._events: queue.Queue[str] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    @property
    def enabled(self) -> bool:
        has_display = bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))
        return _enabled(self._cfg.pi_panel_setting, auto=has_display)

    def start(self) -> None:
        if self._thread is not None or not self.enabled:
            return
        self._thread = threading.Thread(target=self._run, name="jarvis-pi-panel", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._events.put("stop")

    def set(self, state: str) -> None:
        if self._thread is not None:
            self._events.put(state)

    def _run(self) -> None:
        try:
            import tkinter as tk
        except Exception as exc:  # noqa: BLE001
            print(f"  [pi-panel] unavailable: {exc}")
            return

        try:
            root = tk.Tk()
        except Exception as exc:  # noqa: BLE001
            print(f"  [pi-panel] couldn't open display: {exc}")
            return
        _configure_fullscreen_root(root, geometry=self._cfg.pi_panel_geometry)
        canvas = tk.Canvas(root, bg="#05070a", highlightthickness=0)
        canvas.pack(fill="both", expand=True)

        mode: PanelMode = "eyes"
        current = _STATES["idle"]
        target = current
        last_active = time.monotonic()
        last_mode_change = time.monotonic()
        blink_until = 0.0
        next_blink = time.monotonic() + random.uniform(3.0, 7.0)
        pointer_down = 0.0
        camera_state = "ready" if self._hardware.camera_available() else "not detected"
        camera_detail = "tap test capture" if camera_state == "ready" else "rpicam/libcamera missing"
        camera_busy = False

        def set_mode(next_mode: PanelMode) -> None:
            nonlocal mode, last_mode_change
            mode = next_mode
            last_mode_change = time.monotonic()

        def cycle_mode() -> None:
            modes: tuple[PanelMode, ...] = ("eyes", "status", "camera", "debug")
            set_mode(modes[(modes.index(mode) + 1) % len(modes)])

        def run_camera_test() -> None:
            nonlocal camera_busy, camera_state, camera_detail
            if camera_busy:
                return
            camera_busy = True
            camera_state = "capturing"
            camera_detail = "please wait"

            def worker() -> None:
                nonlocal camera_busy, camera_state, camera_detail
                try:
                    result = self._hardware.capture_photo_sync({"reason": "pi-panel-test"})
                    width = result.get("width") or self._cfg.camera_width
                    height = result.get("height") or self._cfg.camera_height
                    camera_state = "ok"
                    camera_detail = f"{width}x{height} captured"
                except Exception as exc:  # noqa: BLE001 - surfaced in the local debug panel
                    camera_state = "error"
                    camera_detail = str(exc)[:80] or "capture failed"
                finally:
                    camera_busy = False

            threading.Thread(target=worker, name="jarvis-pi-panel-camera", daemon=True).start()

        def pointer_start(_event) -> None:  # noqa: ANN001
            nonlocal pointer_down
            pointer_down = time.monotonic()

        def pointer_end(event) -> None:  # noqa: ANN001
            if pointer_down and time.monotonic() - pointer_down >= 0.7:
                set_mode("eyes")
            elif mode == "camera" and event.y >= root.winfo_height() * 0.68:
                run_camera_test()
            else:
                cycle_mode()

        root.bind("<ButtonPress-1>", pointer_start)
        root.bind("<ButtonRelease-1>", pointer_end)
        root.bind("<space>", lambda _event: cycle_mode())
        root.bind("<Escape>", lambda _event: set_mode("eyes"))

        def draw() -> None:
            nonlocal current, target, last_active, blink_until, next_blink
            now = time.monotonic()
            try:
                while True:
                    name = self._events.get_nowait()
                    if name == "stop":
                        root.destroy()
                        return
                    target = _STATES.get(name, target)
                    if name != "sleep":
                        last_active = now
            except queue.Empty:
                pass

            if mode != "eyes" and now - last_mode_change > 60.0:
                set_mode("eyes")
            if now - last_active > max(5.0, self._cfg.pi_panel_sleep_s):
                target = _STATES["sleep"]
            if now >= next_blink and target.name != "sleep":
                blink_until = now + 0.12
                next_blink = now + random.uniform(3.0, 7.0)

            ease = 0.18
            current = EyeState(
                target.name,
                current.openness + (target.openness - current.openness) * ease,
                current.pupil_y + (target.pupil_y - current.pupil_y) * ease,
                current.pupil_x + (target.pupil_x - current.pupil_x) * ease,
            )
            openness = 0.03 if now < blink_until else current.openness
            canvas.delete("all")
            w = max(1, canvas.winfo_width())
            h = max(1, canvas.winfo_height())
            if mode == "status":
                _draw_text_view(
                    canvas,
                    w,
                    h,
                    title="Jarvis PiPanel",
                    rows=(
                        ("view", "status"),
                        ("voice", target.name),
                        ("screen", "active"),
                    ),
                )
                root.after(33, draw)
                return
            if mode == "debug":
                _draw_text_view(
                    canvas,
                    w,
                    h,
                    title="Debug",
                    rows=(
                        ("view", "debug"),
                        ("voice", target.name),
                        ("display", os.environ.get("WAYLAND_DISPLAY") or os.environ.get("DISPLAY") or "unknown"),
                        ("sleep", f"{self._cfg.pi_panel_sleep_s:g}s"),
                    ),
                )
                root.after(33, draw)
                return
            if mode == "camera":
                _draw_camera_view(
                    canvas,
                    w,
                    h,
                    rows=(
                        ("camera", camera_state),
                        ("resolution", f"{self._cfg.camera_width}x{self._cfg.camera_height}"),
                        ("timeout", f"{self._cfg.camera_timeout_s:g}s"),
                        ("last", camera_detail),
                    ),
                    busy=camera_busy,
                )
                root.after(33, draw)
                return

            eye_w = min(w * 0.28, h * 0.34)
            eye_h = max(6.0, eye_w * 0.72 * openness)
            y = h * 0.48
            xs = [w * 0.34, w * 0.66]
            accent = "#74d2ff" if target.name in {"awake", "listening"} else "#d9f3ff"
            if target.name == "thinking":
                accent = "#f6cf5c"
            if target.name == "listening":
                _draw_listening_beacons(canvas, w, h, now)
            for x in xs:
                canvas.create_oval(
                    x - eye_w / 2,
                    y - eye_h / 2,
                    x + eye_w / 2,
                    y + eye_h / 2,
                    fill=accent,
                    outline="",
                )
                if openness > 0.18:
                    px = x + eye_w * 0.13 * math.sin(now * 0.8) + eye_w * current.pupil_x
                    py = y + eye_h * current.pupil_y
                    pupil = max(5.0, eye_w * 0.13)
                    canvas.create_oval(
                        px - pupil,
                        py - pupil,
                        px + pupil,
                        py + pupil,
                        fill="#091018",
                        outline="",
                    )
            root.after(33, draw)

        root.after(33, draw)
        try:
            root.mainloop()
        finally:
            self._stop.set()


def _configure_fullscreen_root(root, *, geometry: str = "") -> None:  # noqa: ANN001
    root.title("Jarvis")
    root.configure(bg="#05070a")
    if geometry:
        # On multi-output Pi desktops, Tk may report the whole virtual desktop.
        # Let operators pin the panel to the DSI output, e.g. 800x480+0+0.
        with suppress(Exception):
            root.overrideredirect(True)
        with suppress(Exception):
            root.geometry(geometry)
    else:
        # Ask the window manager for fullscreen sizing first. Then make Tk
        # borderless and pin the window to the reported screen size so
        # override-redirect sessions do not fall back to a tiny default window.
        with suppress(Exception):
            root.attributes("-fullscreen", True)
        with suppress(Exception):
            root.update_idletasks()
        with suppress(Exception):
            root.overrideredirect(True)
        with suppress(Exception):
            root.geometry(f"{root.winfo_screenwidth()}x{root.winfo_screenheight()}+0+0")
    with suppress(Exception):
        root.lift()
    with suppress(Exception):
        root.focus_force()


def _draw_listening_beacons(canvas, width: int, height: int, now: float) -> None:  # noqa: ANN001
    pulse = (math.sin(now * 4.2) + 1.0) / 2.0
    base = max(8.0, min(width, height) * 0.025)
    ring = base * (1.6 + pulse * 0.9)
    core = base * (0.72 + pulse * 0.18)
    margin = max(18.0, min(width, height) * 0.07)
    for x in (margin, width - margin):
        canvas.create_oval(
            x - ring,
            margin - ring,
            x + ring,
            margin + ring,
            outline="#74d2ff",
            width=2,
        )
        canvas.create_oval(
            x - core,
            margin - core,
            x + core,
            margin + core,
            fill="#74d2ff",
            outline="",
        )


def _draw_camera_view(  # noqa: ANN001
    canvas,
    width: int,
    height: int,
    *,
    rows: tuple[tuple[str, str], ...],
    busy: bool,
) -> None:
    _draw_text_view(canvas, width, height, title="Camera", rows=rows)
    x0 = width * 0.18
    y0 = height * 0.72
    x1 = width * 0.82
    y1 = height * 0.88
    fill = "#163041" if busy else "#0f5f7c"
    outline = "#74d2ff" if busy else ""
    canvas.create_rectangle(x0, y0, x1, y1, fill=fill, outline=outline, width=2)
    canvas.create_text(
        width * 0.5,
        (y0 + y1) / 2,
        anchor="center",
        text="capturing" if busy else "test capture",
        fill="#f5fbff",
        font=("TkDefaultFont", max(18, int(height * 0.05)), "bold"),
    )


def _draw_text_view(canvas, width: int, height: int, *, title: str, rows: tuple[tuple[str, str], ...]) -> None:  # noqa: ANN001
    canvas.create_text(
        width * 0.08,
        height * 0.16,
        anchor="w",
        text=title,
        fill="#d9f3ff",
        font=("TkDefaultFont", max(22, int(height * 0.075)), "bold"),
    )
    y = height * 0.34
    for label, value in rows:
        canvas.create_text(
            width * 0.1,
            y,
            anchor="w",
            text=label,
            fill="#74d2ff",
            font=("TkDefaultFont", max(14, int(height * 0.042)), "bold"),
        )
        canvas.create_text(
            width * 0.42,
            y,
            anchor="w",
            text=value,
            fill="#f5fbff",
            font=("TkDefaultFont", max(14, int(height * 0.042))),
        )
        y += max(34, int(height * 0.105))
