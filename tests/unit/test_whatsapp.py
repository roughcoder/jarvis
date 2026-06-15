"""WhatsApp connector routing (Phase 3b) — inbound message → brain turn → reply.

The brain socket is faked; this proves the connector identifies the sender, sends
the text as a turn, and returns the brain's reply text. No wacli/network.
"""

from __future__ import annotations

import asyncio

from jarvis.connectors.whatsapp import InboundMessage, handle_message
from jarvis.protocol.messages import (
    Identify,
    ReplyEnd,
    ReplyText,
    TextIn,
    decode,
    encode,
)


class _FakeWS:
    """Records sent frames; yields scripted brain replies keyed to the turn id."""

    def __init__(self) -> None:
        self.sent: list = []
        self._turn_id: str | None = None

    async def send(self, raw: str) -> None:
        msg = decode(raw)
        self.sent.append(msg)
        if isinstance(msg, TextIn):
            self._turn_id = msg.turn_id

    def __aiter__(self):  # noqa: ANN204
        async def gen():  # noqa: ANN202
            yield encode(ReplyText(turn_id=self._turn_id, text="Hi from the brain."))
            yield encode(ReplyEnd(turn_id=self._turn_id, ended=False))

        return gen()


def test_handle_message_routes_and_returns_reply() -> None:
    ws = _FakeWS()
    reply = asyncio.run(handle_message(ws, InboundMessage(sender="+44123", text="hello")))

    assert reply == "Hi from the brain."
    # the sender is asserted as the identity, then the text is sent as a turn
    ident = next(m for m in ws.sent if isinstance(m, Identify))
    textin = next(m for m in ws.sent if isinstance(m, TextIn))
    assert ident.identity == "+44123"
    assert textin.text == "hello"
