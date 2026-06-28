from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.tools.intercom import CAP_CAMERA, make_intercom_tools


def test_take_photo_tool_is_camera_gated_and_image_producing() -> None:
    async def action(ctx, name, args, timeout_s):  # noqa: ANN001
        assert ctx.device_id == "kitchen-pi"
        assert name == "capture_photo"
        assert args["reason"] == "holding"
        assert timeout_s == 10.0
        return {"image_b64": "JPEGDATA"}

    tool = make_intercom_tools(action)[0]
    ctx = RequestContext("kitchen-pi", "house", "house", frozenset({CAP_CAMERA}))

    result = asyncio.run(tool.handler(ctx, {"reason": "holding"}))

    assert tool.required_capability == CAP_CAMERA
    assert tool.produces_image is True
    assert result == "JPEGDATA"
