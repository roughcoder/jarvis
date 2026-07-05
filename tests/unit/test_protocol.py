"""Protocol — message round-trips + discriminator (Phase 3 W4 contract).

Both the brain and any intercom (Python now, native later) must agree on these
schemas, so the round-trip and the `type` discrimination are contract.
"""

from __future__ import annotations

import pytest

from jarvis.protocol.messages import (
    AudioEnd,
    AudioStart,
    BargeIn,
    DeviceRequest,
    DeviceResponse,
    Hello,
    ProjectOperationRequest,
    ProjectOperationResponse,
    REPLY_AUDIO_BINARY_V1,
    ReplyEnd,
    TextIn,
    UPLINK_AUDIO_BINARY_V1,
    Welcome,
    decode_binary_audio,
    decode,
    encode_reply_audio_binary,
    encode_uplink_audio_binary,
    encode,
)


def test_audio_control_round_trips() -> None:
    start = decode(encode(AudioStart(turn_id="t1", sample_rate=16000, voice_mode="stay")))
    assert isinstance(start, AudioStart)
    assert start.turn_id == "t1"
    assert start.sample_rate == 16000
    assert start.voice_mode == "stay"

    end = decode(encode(AudioEnd(turn_id="t1")))
    assert isinstance(end, AudioEnd)
    assert end.turn_id == "t1"


def test_binary_uplink_audio_round_trip() -> None:
    pcm = b"\x00\x01\x02\x03" * 10
    frame = encode_uplink_audio_binary("t1", 16000, pcm)
    back = decode_binary_audio(frame)
    assert back is not None
    assert back.kind == "uplink_audio"
    assert back.turn_id == "t1"
    assert back.sample_rate == 16000
    assert back.pcm == pcm
    assert UPLINK_AUDIO_BINARY_V1 == "uplink_audio_binary_v1"


def test_binary_reply_audio_round_trip() -> None:
    pcm = b"\x00\x01\x02\x03" * 10
    frame = encode_reply_audio_binary("t9", pcm)
    back = decode_binary_audio(frame)
    assert back is not None
    assert back.kind == "reply_audio"
    assert back.turn_id == "t9"
    assert back.pcm == pcm
    assert REPLY_AUDIO_BINARY_V1 == "reply_audio_binary_v1"


def test_binary_audio_decoder_ignores_json_bytes() -> None:
    assert decode_binary_audio(encode(ReplyEnd(turn_id="t1")).encode("utf-8")) is None


@pytest.mark.parametrize(
    "msg",
    [
        Hello(device_id="kitchen-pi", token="x"),
        AudioStart(turn_id="t1", sample_rate=16000),
        AudioEnd(turn_id="t1"),
        BargeIn(turn_id="t2"),
        TextIn(turn_id="t3", text="hello"),
        DeviceRequest(request_id="r1", action="capture_photo", args={"width": 640}),
        DeviceResponse(request_id="r1", ok=True, result={"image_b64": "abc"}),
        ProjectOperationRequest(
            request_id="op1",
            op="project.update",
            requester={"identity": "neil"},
            payload={"project_id": "jarvis", "name": "Jarvis"},
        ),
        ProjectOperationResponse(request_id="op1", ok=True, result={"project_id": "jarvis"}),
        Welcome(identity="house", scope="house", capabilities=["web.search"]),
        ReplyEnd(turn_id="t4", ended=True),
    ],
)
def test_discriminator_recovers_exact_type(msg) -> None:
    back = decode(encode(msg))
    assert type(back) is type(msg)
    assert back.model_dump() == msg.model_dump()


def test_decode_picks_by_type_field() -> None:
    assert isinstance(decode('{"type":"barge_in","turn_id":"z"}'), BargeIn)
    assert isinstance(decode('{"type":"reply_end","turn_id":"z","ended":true}'), ReplyEnd)
