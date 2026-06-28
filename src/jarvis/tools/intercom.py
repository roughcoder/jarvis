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

DeviceAction = Callable[[RequestContext, str, dict[str, Any], float], Awaitable[dict[str, Any]]]


def make_intercom_tools(action: DeviceAction) -> list[Tool]:
    async def take_photo(ctx: RequestContext, args: dict[str, Any]) -> str:
        result = await action(ctx, "capture_photo", args, 10.0)
        image = result.get("image_b64")
        if not image:
            return "error: no image captured"
        return str(image)

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
        )
    ]
