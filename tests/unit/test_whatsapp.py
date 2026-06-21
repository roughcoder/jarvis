"""WhatsApp connector routing (Phase 3b) — inbound turn + outbound proactive forward.

The brain socket + wacli are faked; this proves the connector identifies the sender,
sends the text as a turn and returns the brain's reply (consuming the router queue), and
forwards a proactive notification (Proactive with a `to`) OUT via wacli. No network.
"""

from __future__ import annotations

import asyncio

from jarvis.connectors.whatsapp import InboundMessage, forward_proactive, handle_message
from jarvis.protocol.messages import (
    Identify,
    Proactive,
    ReplyEnd,
    ReplyText,
    TextIn,
    decode,
)


class _FakeWS:
    def __init__(self) -> None:
        self.sent: list = []

    async def send(self, raw: str) -> None:
        self.sent.append(decode(raw))


class _FakeWacli:
    def __init__(self) -> None:
        self.sent: list = []

    async def send(self, to: str, text: str) -> None:
        self.sent.append((to, text))


def test_handle_message_routes_and_returns_reply() -> None:
    ws = _FakeWS()
    inbound: asyncio.Queue = asyncio.Queue()

    async def go() -> str:
        # the router would queue these; simulate it after the turn sends its TextIn
        async def feed() -> None:
            await asyncio.sleep(0)  # let handle_message send first
            tid = next(m for m in ws.sent if isinstance(m, TextIn)).turn_id
            inbound.put_nowait(ReplyText(turn_id=tid, text="Hi from the brain."))
            inbound.put_nowait(ReplyEnd(turn_id=tid, ended=False))

        task = asyncio.create_task(feed())
        reply = await handle_message(ws, inbound, InboundMessage(sender="+44123", text="hello"))
        await task
        return reply

    reply = asyncio.run(go())
    assert reply == "Hi from the brain."
    assert next(m for m in ws.sent if isinstance(m, Identify)).identity == "+44123"
    sent = next(m for m in ws.sent if isinstance(m, TextIn))
    assert sent.text == "hello"
    assert sent.text_only is True  # WhatsApp wants text — the brain must skip TTS


def test_forward_proactive_sends_out_via_wacli() -> None:
    wacli = _FakeWacli()
    # a proactive addressed to a number → forwarded out
    handled = asyncio.run(forward_proactive(wacli, Proactive(text="your table is booked", to="+44123")))
    assert handled is True
    assert wacli.sent == [("+44123", "your table is booked")]


def test_forward_proactive_ignores_non_addressed() -> None:
    wacli = _FakeWacli()
    # a device-only proactive (no `to`) is NOT forwarded to WhatsApp
    assert asyncio.run(forward_proactive(wacli, Proactive(text="device only"))) is False
    assert asyncio.run(forward_proactive(wacli, ReplyText(turn_id="t", text="x"))) is False
    assert wacli.sent == []
