"""Protocol — message round-trips + discriminator (Phase 3 W4 contract).

Both the brain and any intercom (Python now, native later) must agree on these
schemas, so the round-trip and the `type` discrimination are contract.
"""

from __future__ import annotations

import pytest

from jarvis.protocol.messages import (
    BargeIn,
    Hello,
    ReplyAudio,
    ReplyEnd,
    TextIn,
    Utterance,
    Welcome,
    decode,
    encode,
)


def test_utterance_pcm_round_trip() -> None:
    pcm = bytes(range(256))
    u = Utterance.of("t1", 16000, pcm)
    back = decode(encode(u))
    assert isinstance(back, Utterance)
    assert back.turn_id == "t1"
    assert back.sample_rate == 16000
    assert back.pcm() == pcm


def test_reply_audio_pcm_round_trip() -> None:
    pcm = b"\x00\x01\x02\x03" * 10
    back = decode(encode(ReplyAudio.of("t9", pcm)))
    assert isinstance(back, ReplyAudio)
    assert back.pcm() == pcm


@pytest.mark.parametrize(
    "msg",
    [
        Hello(device_id="kitchen-pi", token="x"),
        BargeIn(turn_id="t2"),
        TextIn(turn_id="t3", text="hello"),
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
