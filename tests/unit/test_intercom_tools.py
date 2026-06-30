from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.tools.intercom import CAP_CAMERA, CAP_DISPLAY, DISPLAY_TIMEOUT_S, make_intercom_tools


def test_take_photo_tool_is_camera_gated_and_image_producing() -> None:
    async def action(ctx, name, args, timeout_s):  # noqa: ANN001
        assert ctx.device_id == "kitchen-pi"
        assert name == "capture_photo"
        assert args["reason"] == "holding"
        assert timeout_s == 10.0
        return {"image_b64": "JPEGDATA"}

    tool = {t.name: t for t in make_intercom_tools(action)}["take_photo"]
    ctx = RequestContext("kitchen-pi", "house", "house", frozenset({CAP_CAMERA}))

    result = asyncio.run(tool.handler(ctx, {"reason": "holding"}))

    assert tool.required_capability == CAP_CAMERA
    assert tool.produces_image is True
    assert result == "JPEGDATA"


def test_control_pi_panel_tool_is_display_gated() -> None:
    async def action(ctx, name, args, timeout_s):  # noqa: ANN001
        assert ctx.device_id == "kitchen-pi"
        assert name == "control_display"
        assert args == {"action": "hide"}
        assert timeout_s == DISPLAY_TIMEOUT_S
        return {"status": "hidden", "visible": False}

    tool = {t.name: t for t in make_intercom_tools(action)}["control_pi_panel"]
    ctx = RequestContext("kitchen-pi", "house", "house", frozenset({CAP_DISPLAY}))

    result = asyncio.run(tool.handler(ctx, {"action": "hide"}))

    assert tool.required_capability == CAP_DISPLAY
    assert tool.timeout_s == DISPLAY_TIMEOUT_S + 1.0
    assert result == "PiPanel screen is off."


def test_control_pi_panel_status_returns_command_output() -> None:
    async def action(ctx, name, args, timeout_s):  # noqa: ANN001
        assert name == "control_display"
        assert args == {"action": "status"}
        return {"status": "visible", "command": "sudo jarvis-pi panel-status: active"}

    tool = {t.name: t for t in make_intercom_tools(action)}["control_pi_panel"]
    ctx = RequestContext("kitchen-pi", "house", "house", frozenset({CAP_DISPLAY}))

    result = asyncio.run(tool.handler(ctx, {"action": "status"}))

    assert result == "PiPanel screen status:\nsudo jarvis-pi panel-status: active"
