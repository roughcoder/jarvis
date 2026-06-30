"""Build the frames for a server-initiated (proactive) delivery to a voice device.

A proactive delivery is: a `Proactive` header (text + kind + open_mic, under a "pa-"
turn id), then binary reply-audio frames — the alarm/notification tone, then the spoken
text — then ReplyEnd. Text clients use the header's text and ignore the audio. Factored
out of the server so it's testable with a fake TTS.
"""

from __future__ import annotations

from jarvis.brain.tones import make_tone
from jarvis.protocol.messages import (
    Proactive,
    ReplyEnd,
    encode,
    encode_reply_audio_binary,
)


async def proactive_frames(
    tts,  # noqa: ANN001 - InworldTTS | fake (has synthesize_stream)
    sample_rate: int,
    text: str,
    *,
    turn_id: str,
    kind: str = "notification",
    open_mic: bool = False,
    speak: bool = True,
    tone: bool = True,
    sound: str = "chime",
    freq: float = 880.0,
) -> list[str | bytes]:
    """Return the ordered, encoded frames for one proactive delivery."""
    frames: list[str | bytes] = [
        encode(Proactive(text=text, turn_id=turn_id, kind=kind, open_mic=open_mic))
    ]
    if tone:
        frames.append(
            encode_reply_audio_binary(turn_id, make_tone(sample_rate, sound=sound, freq=freq))
        )
    if speak and text and tts is not None:
        async for chunk in tts.synthesize_stream(text):
            frames.append(encode_reply_audio_binary(turn_id, chunk))
    frames.append(encode(ReplyEnd(turn_id=turn_id, ended=False)))
    return frames
