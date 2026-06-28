"""Tiny optional eyes display for Pi intercoms.

Uses tkinter from the standard library so the Pi image does not need a heavy UI
dependency. If there is no graphical display, it simply does not start.
"""

from __future__ import annotations

import math
import os
import queue
import random
import threading
import time
from dataclasses import dataclass

from jarvis.config import IntercomDeviceConfig
from jarvis.intercom.hardware import _enabled


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


class EyeDisplay:
    def __init__(self, cfg: IntercomDeviceConfig) -> None:
        self._cfg = cfg
        self._events: queue.Queue[str] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    @property
    def enabled(self) -> bool:
        has_display = bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))
        return _enabled(self._cfg.eyes, auto=has_display)

    def start(self) -> None:
        if self._thread is not None or not self.enabled:
            return
        self._thread = threading.Thread(target=self._run, name="jarvis-eyes", daemon=True)
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
            print(f"  [eyes] unavailable: {exc}")
            return

        try:
            root = tk.Tk()
        except Exception as exc:  # noqa: BLE001
            print(f"  [eyes] couldn't open display: {exc}")
            return
        root.title("Jarvis")
        root.configure(bg="#05070a")
        root.attributes("-fullscreen", True)
        canvas = tk.Canvas(root, bg="#05070a", highlightthickness=0)
        canvas.pack(fill="both", expand=True)

        current = _STATES["idle"]
        target = current
        last_active = time.monotonic()
        blink_until = 0.0
        next_blink = time.monotonic() + random.uniform(3.0, 7.0)

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

            if now - last_active > max(5.0, self._cfg.eyes_sleep_after_s):
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
            eye_w = min(w * 0.28, h * 0.34)
            eye_h = max(6.0, eye_w * 0.72 * openness)
            y = h * 0.48
            xs = [w * 0.34, w * 0.66]
            accent = "#74d2ff" if target.name in {"awake", "listening"} else "#d9f3ff"
            if target.name == "thinking":
                accent = "#f6cf5c"
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
