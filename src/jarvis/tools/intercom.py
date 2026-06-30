"""Intercom-local hardware tools.

The brain owns tool selection and capability checks; the intercom owns physical
hardware. Calls cross the brain<->intercom WebSocket boundary.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from jarvis.runtime import RequestContext
from jarvis.tools.base import Tool

CAP_CAMERA = "intercom.camera"
CAP_DISPLAY = "intercom.display"

DeviceAction = Callable[[RequestContext, str, dict[str, Any], float], Awaitable[dict[str, Any]]]


def make_intercom_tools(action: DeviceAction) -> list[Tool]:
    async def take_photo(ctx: RequestContext, args: dict[str, Any]) -> str:
        result = await action(ctx, "capture_photo", args, 10.0)
        image = result.get("image_b64")
        if not image:
            return "error: no image captured"
        return str(image)

    async def control_display(ctx: RequestContext, args: dict[str, Any]) -> str:
        requested = str(args.get("action") or "status").strip().lower()
        result = await action(ctx, "control_display", {"action": requested}, 5.0)
        status = result.get("status") or "unknown"
        command = str(result.get("command") or "").strip()
        if requested == "status" and command:
            return f"PiPanel screen status:\n{command}"
        if status == "visible":
            return "PiPanel screen is on."
        if status == "hidden":
            return "PiPanel screen is off."
        if status == "unavailable":
            return "PiPanel screen is unavailable on this device."
        if status == "command_failed" and command:
            return f"PiPanel screen command failed:\n{command}"
        return f"PiPanel screen status: {status}."

    return [
        Tool(
            "take_photo",
            "Take a fresh photo from this intercom's local camera and look at it. "
            "Use this when the user asks what they are holding, showing, wearing, "
            "or what is in front of the room device.",
            {
                "type": "object",
                "properties": {
                    "reason": {
                        "type": "string",
                        "description": "Why the photo is needed, in a few words.",
                    },
                    "width": {
                        "type": "integer",
                        "description": "Optional capture width in pixels.",
                    },
                    "height": {
                        "type": "integer",
                        "description": "Optional capture height in pixels.",
                    },
                },
            },
            CAP_CAMERA,
            take_photo,
            announce=False,
            produces_image=True,
            timeout_s=12.0,
        ),
        Tool(
            "control_pi_panel",
            "Show, hide, toggle, or check the Raspberry Pi PiPanel screen. Use when "
            "the user says turn the screen on/off, hide/show the screen, or asks "
            "whether the PiPanel display is on.",
            {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["show", "hide", "toggle", "status"],
                        "description": "show/on, hide/off, toggle, or status.",
                    }
                },
                "required": ["action"],
            },
            CAP_DISPLAY,
            control_display,
            announce=False,
            timeout_s=6.0,
        ),
    ]
