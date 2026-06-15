"""WhatsApp connector (Phase 3b) — wraps `wacli` and bridges it to the brain.

Inbound WhatsApp messages become brain turns over the WebSocket protocol
(channel=whatsapp; the sender's number is the asserted identity, which the brain's
resolver maps to a user via `users/<name>.md`); the brain's reply text goes back
out through `wacli`. The connector is a thin boundary peer — it imports nothing from
the brain and holds only a pairing token (the credential boundary, §3).

`wacli` is an external dependency (not vendored); the live path is exercised by an
integration test that self-skips when the binary is absent. The routing logic
(`handle_message`) is pure and unit-tested against a fake brain socket.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

from jarvis.config import Config, WhatsAppConfig
from jarvis.protocol.messages import (
    Hello,
    Identify,
    ReplyEnd,
    ReplyText,
    TextIn,
    Welcome,
    decode,
    encode,
)


@dataclass(frozen=True)
class InboundMessage:
    sender: str  # the WhatsApp number / jid
    text: str


class WacliClient:
    """Thin wrapper over the `wacli` WhatsApp CLI. `listen` streams inbound messages
    as line-delimited JSON ({"from": …, "text": …}); `send` posts a reply."""

    def __init__(self, cfg: WhatsAppConfig) -> None:
        self._cfg = cfg

    async def listen(self) -> AsyncIterator[InboundMessage]:
        proc = await asyncio.create_subprocess_exec(
            self._cfg.wacli_bin, "listen", "--json", stdout=asyncio.subprocess.PIPE
        )
        assert proc.stdout is not None
        try:
            async for raw in proc.stdout:
                line = raw.decode("utf-8", "replace").strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                sender = obj.get("from") or obj.get("sender")
                text = obj.get("text") or obj.get("body") or ""
                if sender and text:
                    yield InboundMessage(sender=sender, text=text)
        finally:
            if proc.returncode is None:
                proc.terminate()

    async def send(self, to: str, text: str) -> None:
        proc = await asyncio.create_subprocess_exec(
            self._cfg.wacli_bin, "send", "--to", to, "--text", text
        )
        await proc.wait()


async def handle_message(ws, msg: InboundMessage) -> str:  # noqa: ANN001
    """Drive ONE inbound message through the brain and return the reply text. Sends
    an Identify (the sender is the asserted identity) then a TextIn, and collects
    ReplyText up to ReplyEnd. Pure routing — unit-tested with a fake socket."""
    turn_id = uuid.uuid4().hex
    await ws.send(encode(Identify(identity=msg.sender)))
    await ws.send(encode(TextIn(turn_id=turn_id, text=msg.text)))
    reply = ""
    async for raw in ws:
        m = decode(raw)
        if isinstance(m, ReplyText) and m.turn_id == turn_id:
            reply = m.text
        elif isinstance(m, ReplyEnd) and m.turn_id == turn_id:
            break
    return reply


class WhatsAppConnector:
    def __init__(self, cfg: Config, *, wacli: WacliClient | None = None) -> None:
        self._cfg = cfg
        self._wacli = wacli or WacliClient(cfg.whatsapp)

    async def run(self) -> None:
        import websockets

        url = self._cfg.intercom.brain_url
        print(f"WhatsApp connector → brain {url}")
        async with websockets.connect(url) as ws:
            await ws.send(
                encode(
                    Hello(
                        device_id=self._cfg.whatsapp.device_id,
                        token=self._cfg.whatsapp.token.get_secret_value(),
                        channel="whatsapp",
                    )
                )
            )
            welcome = decode(await ws.recv())
            if not isinstance(welcome, Welcome):
                print(f"pairing rejected: {welcome}")
                return
            print("Paired. Listening for WhatsApp messages…")
            async for msg in self._wacli.listen():
                reply = await handle_message(ws, msg)
                if reply:
                    await self._wacli.send(msg.sender, reply)
