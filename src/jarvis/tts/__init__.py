"""TTS — cloud streaming synthesis behind a config-driven URL (spec §4, Step 2).

Inworld streaming endpoint: POST {base_url}/tts/v1/voice:stream
  Auth:  Authorization: Basic <api_key>   (the key is already a base64 token)
  Body:  {text, voiceId, modelId, audioConfig{audioEncoding, sampleRateHertz}}
  Resp:  a stream of JSON objects, each {"result": {"audioContent": <base64>}}

synthesize_stream() yields raw little-endian 16-bit PCM as soon as each chunk
arrives, so playback can start before the full sentence is synthesized. The
HTTP request is cancellable (close the context / cancel the task) for barge-in.
"""

from __future__ import annotations

import base64
import json
from collections.abc import AsyncIterator

import httpx

from jarvis.config import TTSConfig


def _extract_json_objects(buffer: str) -> tuple[list[str], str]:
    """Pull complete top-level {...} objects out of a partial text buffer.

    Tolerant of both newline-delimited objects and a JSON array stream — it
    tracks brace depth outside of strings and ignores array punctuation.
    Returns (complete_object_strings, unconsumed_remainder).
    """
    objs: list[str] = []
    depth = 0
    in_str = False
    esc = False
    start: int | None = None
    consumed = 0
    for i, ch in enumerate(buffer):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                objs.append(buffer[start : i + 1])
                consumed = i + 1
                start = None
    return objs, buffer[consumed:]


def _strip_wav_header(pcm: bytes) -> bytes:
    """Inworld LINEAR16 chunks may carry a WAV/RIFF header — drop it if present."""
    if pcm[:4] == b"RIFF":
        idx = pcm.find(b"data")
        if idx != -1:
            return pcm[idx + 8 :]  # skip 'data' + 4-byte size
    return pcm


class InworldTTS:
    def __init__(self, cfg: TTSConfig) -> None:
        self._cfg = cfg

    async def synthesize_stream(
        self, text: str, *, voice: str | None = None
    ) -> AsyncIterator[bytes]:
        """Yield raw 16-bit PCM chunks as they stream back from Inworld."""
        url = f"{self._cfg.base_url}/tts/v1/voice:stream"
        headers = {
            "Authorization": f"Basic {self._cfg.api_key.get_secret_value()}",
            "Content-Type": "application/json",
        }
        body = {
            "text": text,
            "voiceId": voice or self._cfg.voice,
            "modelId": self._cfg.model_id,
            "audioConfig": {
                "audioEncoding": "LINEAR16",
                "sampleRateHertz": self._cfg.sample_rate,
            },
            "language": self._cfg.language,
            # STABLE | BALANCED | CREATIVE — overall expressiveness; fine-grained
            "deliveryMode": self._cfg.delivery_mode,
        }

        buffer = ""
        timeout = httpx.Timeout(
            self._cfg.request_timeout_s, connect=self._cfg.connect_timeout_s
        )
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream("POST", url, headers=headers, json=body) as resp:
                if resp.status_code != 200:
                    detail = (await resp.aread()).decode("utf-8", "replace")
                    raise RuntimeError(f"Inworld TTS {resp.status_code}: {detail[:300]}")
                async for raw in resp.aiter_bytes():
                    buffer += raw.decode("utf-8", "replace")
                    objs, buffer = _extract_json_objects(buffer)
                    for obj in objs:
                        try:
                            data = json.loads(obj)
                        except json.JSONDecodeError:
                            continue
                        b64 = (data.get("result") or {}).get("audioContent")
                        if not b64:
                            continue
                        pcm = _strip_wav_header(base64.b64decode(b64))
                        if pcm:
                            yield pcm
