"""Brain <-> intercom WebSocket protocol (Phase 3 W4).

Language-neutral message schemas so a Python intercom (now) and a native client
(later) are interchangeable on the brain side. Control messages are JSON text
frames with a `type` discriminator. Downlink audio PCM travels in binary
WebSocket frames. Uplink voice PCM streams as binary WebSocket frames bracketed
by JSON control frames; there is one voice-audio transport in this home setup.

  up   (intercom -> brain): Hello, AudioStart, AudioEnd, BargeIn, TextIn, ConversationIdle,
                            DeviceResponse
  down (brain -> intercom): Welcome / Reject, ReplyText, ReplyEnd,
                            Cancel, Proactive, DeviceRequest
"""

from __future__ import annotations

import dataclasses
import struct
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, TypeAdapter

REPLY_AUDIO_BINARY_V1 = "reply_audio_binary_v1"
UPLINK_AUDIO_BINARY_V1 = "uplink_audio_binary_v1"

_BINARY_MAGIC = b"JARVIS1"
_BINARY_HEADER = struct.Struct("!7sBBHI")
_BINARY_TYPE_REPLY_AUDIO = 1
_BINARY_TYPE_UPLINK_AUDIO = 2


@dataclasses.dataclass(frozen=True)
class BinaryAudio:
    """Decoded binary audio WebSocket frame."""

    kind: Literal["reply_audio", "uplink_audio"]
    turn_id: str
    pcm: bytes
    sample_rate: int = 0

# --- up: intercom -> brain -------------------------------------------------


class Hello(BaseModel):
    type: Literal["hello"] = "hello"
    device_id: str
    token: str = ""
    profile: str = ""
    # Phase 3d: a strong channel may assert WHO it is at pairing (e.g. your own
    # Mac/WhatsApp). Empty => the brain resolves identity from the device's default
    # (a shared room device defaults to the house principal until a speaker is
    # confirmed). `channel` distinguishes voice/whatsapp/etc. for the resolver.
    identity: str = ""
    channel: str = "voice"
    # Live resources on the intercom, e.g. ["camera", "display"]. These do not
    # grant authority by themselves; the brain intersects them with the device
    # profile before exposing tools.
    hardware: list[str] = Field(default_factory=list)


class Identify(BaseModel):
    """Up: a speaker claims an identity mid-session ("it's Jules"). For voice this
    is usually inferred from the utterance, but a non-voice client can send it
    explicitly. The brain re-resolves scope/credentials from it (know-or-ask)."""

    type: Literal["identify"] = "identify"
    identity: str


class AudioStart(BaseModel):
    type: Literal["audio_start"] = "audio_start"
    turn_id: str
    sample_rate: int
    voice_mode: str = "default"


class AudioEnd(BaseModel):
    type: Literal["audio_end"] = "audio_end"
    turn_id: str


class BargeIn(BaseModel):
    type: Literal["barge_in"] = "barge_in"
    turn_id: str


class ConversationIdle(BaseModel):
    """Up: a voice follow-up window timed out on the edge without another utterance.

    The brain uses this to end connection-scoped state such as temporary identity
    claims. Stay mode does not send this while it remains active.
    """

    type: Literal["conversation_idle"] = "conversation_idle"
    reason: str = "timeout"


class TextIn(BaseModel):
    type: Literal["text_in"] = "text_in"
    turn_id: str
    text: str
    # A text client (the terminal console, scripted tests) wants ReplyText only —
    # the brain skips TTS for the turn, so no audio stack / TTS key is needed.
    text_only: bool = False


class DeviceResponse(BaseModel):
    """Up: response to a bounded device-local action requested by the brain.

    Result payloads are action-specific. For `capture_photo`, result contains
    `image_b64`, `mime_type`, and optional capture metadata.
    """

    type: Literal["device_response"] = "device_response"
    request_id: str
    ok: bool
    result: dict[str, Any] = Field(default_factory=dict)
    error: str = ""


ProjectOperationName = Literal[
    "project.create",
    "project.update",
    "project.repos.set",
    "project.members.set",
    "project.visibility.set",
    "project.archive",
    "project.delete",
    "project.file.upload",
    "project.file.retract",
]


class ProjectOperationRequest(BaseModel):
    """Up: authenticated boundary peers ask the brain to perform project writes."""

    type: Literal["project_operation_request"] = "project_operation_request"
    request_id: str
    op: ProjectOperationName
    requester: dict[str, Any] = Field(default_factory=dict)
    payload: dict[str, Any] = Field(default_factory=dict)


# --- down: brain -> intercom -----------------------------------------------


class Welcome(BaseModel):
    type: Literal["welcome"] = "welcome"
    identity: str
    scope: str
    capabilities: list[str]


class Reject(BaseModel):
    type: Literal["reject"] = "reject"
    reason: str


class Transcript(BaseModel):
    """Down: what the brain heard for this turn (STT runs brain-side, so the thin
    intercom can't print the user's words without this)."""

    type: Literal["transcript"] = "transcript"
    turn_id: str
    text: str


class ReplyText(BaseModel):
    type: Literal["reply_text"] = "reply_text"
    turn_id: str
    text: str


class ReplyEnd(BaseModel):
    type: Literal["reply_end"] = "reply_end"
    turn_id: str
    ended: bool = False
    continue_listening: bool = False
    voice_mode: str = "default"
    close_reason: str = ""


class Cancel(BaseModel):
    type: Literal["cancel"] = "cancel"
    turn_id: str


class Proactive(BaseModel):
    """Down: a server-initiated message (alarm, background-job result, heartbeat). For a
    voice device the brain follows this with binary reply-audio frames (tone + spoken text)
    under the same `turn_id`, then ReplyEnd; text clients just show `text`. `open_mic`
    asks the intercom to listen for a reply after speaking."""

    type: Literal["proactive"] = "proactive"
    text: str
    turn_id: str = ""
    kind: str = "notification"  # notification | alarm
    open_mic: bool = False
    to: str = ""  # for a forwarding connector (e.g. WhatsApp): the recipient address/jid


class WhoAreYou(BaseModel):
    """Down: the brain needs the speaker's identity to serve a personal request on
    an uncertain channel (know-or-ask, §5). The reply comes as `Identify` or, for
    voice, as the next utterance ("it's Neil")."""

    type: Literal["who_are_you"] = "who_are_you"
    turn_id: str = ""
    prompt: str = "Who am I talking to?"


class DeviceRequest(BaseModel):
    """Down: ask the intercom to perform a tightly-scoped local hardware action."""

    type: Literal["device_request"] = "device_request"
    request_id: str
    action: str
    args: dict[str, Any] = Field(default_factory=dict)


class ProjectOperationResponse(BaseModel):
    """Down: structured response for a project write/upload operation."""

    type: Literal["project_operation_response"] = "project_operation_response"
    request_id: str
    ok: bool
    result: dict[str, Any] = Field(default_factory=dict)
    error: dict[str, Any] = Field(default_factory=dict)


Message = Union[
    Hello, AudioStart, AudioEnd, BargeIn, ConversationIdle, TextIn, Identify, DeviceResponse,
    ProjectOperationRequest,
    Welcome, Reject, ReplyText, ReplyEnd, Cancel, Proactive, WhoAreYou,
    Transcript, DeviceRequest, ProjectOperationResponse,
]
_ADAPTER: TypeAdapter = TypeAdapter(Annotated[Message, Field(discriminator="type")])


def encode(msg: BaseModel) -> str:
    """Serialise any protocol message to a JSON text frame."""
    return msg.model_dump_json()


def decode(data: str | bytes) -> Message:
    """Parse a JSON frame into the matching message type (by `type`)."""
    return _ADAPTER.validate_json(data)


def encode_reply_audio_binary(turn_id: str, pcm: bytes) -> bytes:
    """Serialise reply PCM as a binary WebSocket frame."""
    turn = turn_id.encode("utf-8")
    if len(turn) > 65535:
        raise ValueError("turn_id is too long for a binary audio frame")
    return (
        _BINARY_HEADER.pack(
            _BINARY_MAGIC,
            _BINARY_TYPE_REPLY_AUDIO,
            0,  # flags reserved
            len(turn),
            0,  # reply audio sample rate is implicit from config
        )
        + turn
        + pcm
    )


def encode_uplink_audio_binary(turn_id: str, sample_rate: int, pcm: bytes) -> bytes:
    """Serialise captured mic PCM as a binary WebSocket frame."""
    turn = turn_id.encode("utf-8")
    if len(turn) > 65535:
        raise ValueError("turn_id is too long for a binary audio frame")
    return (
        _BINARY_HEADER.pack(
            _BINARY_MAGIC,
            _BINARY_TYPE_UPLINK_AUDIO,
            0,  # flags reserved
            len(turn),
            sample_rate,
        )
        + turn
        + pcm
    )


def decode_binary_audio(data: bytes) -> BinaryAudio | None:
    """Return a binary audio frame, or None when `data` is not this protocol."""
    if len(data) < _BINARY_HEADER.size:
        return None
    magic, frame_type, _flags, turn_len, sample_rate = _BINARY_HEADER.unpack_from(data)
    if magic != _BINARY_MAGIC:
        return None
    start = _BINARY_HEADER.size
    end = start + turn_len
    if len(data) < end:
        raise ValueError("truncated binary audio frame")
    turn_id = data[start:end].decode("utf-8")
    pcm = data[end:]
    if frame_type == _BINARY_TYPE_REPLY_AUDIO:
        return BinaryAudio(
            kind="reply_audio", turn_id=turn_id, pcm=pcm, sample_rate=sample_rate
        )
    if frame_type == _BINARY_TYPE_UPLINK_AUDIO:
        return BinaryAudio(
            kind="uplink_audio", turn_id=turn_id, pcm=pcm, sample_rate=sample_rate
        )
    raise ValueError(f"unknown binary audio frame type: {frame_type}")
