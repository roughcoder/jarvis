"""Optional intercom-local hardware.

The intercom stays a thin boundary peer: it may own a camera or small display,
but the brain can only ask for bounded actions over the WebSocket protocol. No
provider credentials or model logic live here.
"""

from __future__ import annotations

import base64
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from jarvis.config import IntercomDeviceConfig


def _enabled(value: str, *, auto: bool) -> bool:
    v = (value or "auto").strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return auto


class IntercomHardware:
    def __init__(self, cfg: IntercomDeviceConfig) -> None:
        self._cfg = cfg
        self._camera_bin = self._find_camera_bin()

    def capabilities(self) -> list[str]:
        caps: list[str] = []
        if _enabled(self._cfg.camera, auto=bool(self._camera_bin)):
            caps.append("camera")
        if _enabled(self._cfg.eyes, auto=self.display_available()):
            caps.append("display")
        return caps

    def display_available(self) -> bool:
        return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))

    async def handle(self, action: str, args: dict[str, Any]) -> dict[str, Any]:
        if action == "capture_photo":
            return await self.capture_photo(args)
        raise ValueError(f"unsupported device action {action!r}")

    async def capture_photo(self, args: dict[str, Any] | None = None) -> dict[str, Any]:
        import asyncio

        return await asyncio.to_thread(self._capture_photo_sync, args or {})

    def _find_camera_bin(self) -> str:
        if self._cfg.camera_bin:
            return self._cfg.camera_bin
        return shutil.which("rpicam-still") or shutil.which("libcamera-still") or ""

    def _capture_photo_sync(self, args: dict[str, Any]) -> dict[str, Any]:
        bin_path = self._find_camera_bin()
        if not bin_path:
            raise RuntimeError("camera capture tool not found (rpicam-still/libcamera-still)")
        width = int(args.get("width") or self._cfg.camera_width)
        height = int(args.get("height") or self._cfg.camera_height)
        warmup_ms = int(args.get("warmup_ms") or self._cfg.camera_warmup_ms)
        with tempfile.TemporaryDirectory(prefix="jarvis-photo-") as td:
            out = Path(td) / "capture.jpg"
            cmd = [
                bin_path,
                "-n",
                "-t",
                str(max(1, warmup_ms)),
                "--width",
                str(width),
                "--height",
                str(height),
                "-o",
                str(out),
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=max(1.0, self._cfg.camera_timeout_s),
                check=False,
            )
            if result.returncode != 0:
                detail = (result.stderr or result.stdout or "").strip()
                raise RuntimeError(detail or f"{Path(bin_path).name} exited {result.returncode}")
            data = out.read_bytes()
        return {
            "image_b64": base64.b64encode(data).decode("ascii"),
            "mime_type": "image/jpeg",
            "width": width,
            "height": height,
        }
