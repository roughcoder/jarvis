"""Brain <-> intercom WebSocket protocol (Phase 3 W4).

Language-neutral message schemas so a Python intercom (now) and a native client
(later) are interchangeable on the brain side. All messages are JSON text frames
with a `type` discriminator; audio PCM travels base64-encoded inside them (simple
and uniform for 3a — a binary-frame fast path is a later optimisation).

  up   (intercom -> brain): Hello, Utterance, BargeIn, TextIn
  down (brain -> intercom): Welcome / Reject, ReplyAudio, ReplyText, ReplyEnd,
                            Cancel, Proactive
"""

from __future__ import annotations

import base64
from typing import Annotated, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter

# --- up: intercom -> brain -------------------------------------------------


class Hello(BaseModel):
    type: Literal["hello"] = "hello"
    device_id: str
    token: str = ""
    profile: str = ""


class Utterance(BaseModel):
    type: Literal["utterance"] = "utterance"
    turn_id: str
    sample_rate: int
    pcm_b64: str

    def pcm(self) -> bytes:
        return base64.b64decode(self.pcm_b64)

    @classmethod
    def of(cls, turn_id: str, sample_rate: int, pcm: bytes) -> "Utterance":
        return cls(turn_id=turn_id, sample_rate=sample_rate, pcm_b64=_b64(pcm))


class BargeIn(BaseModel):
    type: Literal["barge_in"] = "barge_in"
    turn_id: str


class TextIn(BaseModel):
    type: Literal["text_in"] = "text_in"
    turn_id: str
    text: str


# --- down: brain -> intercom -----------------------------------------------


class Welcome(BaseModel):
    type: Literal["welcome"] = "welcome"
    identity: str
    scope: str
    capabilities: list[str]


class Reject(BaseModel):
    type: Literal["reject"] = "reject"
    reason: str


class ReplyAudio(BaseModel):
    type: Literal["reply_audio"] = "reply_audio"
    turn_id: str
    pcm_b64: str

    def pcm(self) -> bytes:
        return base64.b64decode(self.pcm_b64)

    @classmethod
    def of(cls, turn_id: str, pcm: bytes) -> "ReplyAudio":
        return cls(turn_id=turn_id, pcm_b64=_b64(pcm))


class ReplyText(BaseModel):
    type: Literal["reply_text"] = "reply_text"
    turn_id: str
    text: str


class ReplyEnd(BaseModel):
    type: Literal["reply_end"] = "reply_end"
    turn_id: str
    ended: bool = False


class Cancel(BaseModel):
    type: Literal["cancel"] = "cancel"
    turn_id: str


class Proactive(BaseModel):
    type: Literal["proactive"] = "proactive"
    text: str


Message = Union[
    Hello, Utterance, BargeIn, TextIn,
    Welcome, Reject, ReplyAudio, ReplyText, ReplyEnd, Cancel, Proactive,
]
_ADAPTER: TypeAdapter = TypeAdapter(Annotated[Message, Field(discriminator="type")])


def _b64(pcm: bytes) -> str:
    return base64.b64encode(pcm).decode("ascii")


def encode(msg: BaseModel) -> str:
    """Serialise any protocol message to a JSON text frame."""
    return msg.model_dump_json()


def decode(data: str | bytes) -> Message:
    """Parse a JSON frame into the matching message type (by `type`)."""
    return _ADAPTER.validate_json(data)
