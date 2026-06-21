"""WhatsApp connector routing (Phase 3b) — inbound turn + outbound proactive forward.

The brain socket + wacli are faked; this proves the connector identifies the sender,
sends the text as a turn and returns the brain's reply (consuming the router queue), and
forwards a proactive notification (Proactive with a `to`) OUT via wacli. No network.
"""

from __future__ import annotations

import asyncio

from jarvis.connectors.whatsapp import (
    InboundMessage,
    _parse_messages,
    chunk_text,
    forward_proactive,
    handle_message,
    is_allowed,
)
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


def test_parse_messages_maps_wacli_schema() -> None:
    # mirrors `wacli messages list --json` (data.messages[], PascalCase fields)
    obj = {
        "success": True,
        "data": {
            "messages": [
                {"SenderJID": "447921815819@s.whatsapp.net", "ChatJID": "447921815819@s.whatsapp.net",
                 "MsgID": "abc", "Timestamp": "2026-06-21T10:00:00Z", "FromMe": False, "Text": "hello jarvis"},
                {"SenderJID": "x@s.whatsapp.net", "MsgID": "own", "FromMe": True, "Text": "my own msg"},
                {"SenderJID": "y@s.whatsapp.net", "MsgID": "empty", "FromMe": False, "Text": ""},
                {"SenderJID": "z@s.whatsapp.net", "ChatJID": "z@s.whatsapp.net", "MsgID": "d2",
                 "Timestamp": "2026-06-21T10:01:00Z", "FromMe": False, "Text": "", "DisplayText": "via display"},
            ]
        },
    }
    msgs = _parse_messages(obj)
    assert [m.text for m in msgs] == ["hello jarvis", "via display"]  # own + empty skipped
    assert msgs[0].sender == "447921815819@s.whatsapp.net" and msgs[0].msg_id == "abc"
    assert msgs[0].chat == "447921815819@s.whatsapp.net" and msgs[0].ts == "2026-06-21T10:00:00Z"
    assert _parse_messages({}) == []  # empty/garbage → no rows


def test_is_allowed_deny_by_default() -> None:
    allow = "447921815819, 447999246830"
    # allowlist (default): only listed numbers, any format
    assert is_allowed("447921815819@s.whatsapp.net", "allowlist", allow) is True
    assert is_allowed("+44 7921 815819", "allowlist", allow) is True
    assert is_allowed("447000000000@s.whatsapp.net", "allowlist", allow) is False
    assert is_allowed("447921815819", "allowlist", "") is False  # empty list → nobody
    # open lets anyone; disabled blocks everyone
    assert is_allowed("447000000000", "open", "") is True
    assert is_allowed("447921815819", "disabled", allow) is False


def test_chunk_text_splits_long_replies() -> None:
    assert chunk_text("short", 4000) == ["short"]
    assert chunk_text("", 4000) == []
    body = "word " * 500  # 2500 chars
    chunks = chunk_text(body, 1000)
    assert len(chunks) >= 3 and all(len(c) <= 1000 for c in chunks)
    assert "".join(c.replace(" ", "") for c in chunks) == body.replace(" ", "")  # no data lost


def test_forward_proactive_ignores_non_addressed() -> None:
    wacli = _FakeWacli()
    # a device-only proactive (no `to`) is NOT forwarded to WhatsApp
    assert asyncio.run(forward_proactive(wacli, Proactive(text="device only"))) is False
    assert asyncio.run(forward_proactive(wacli, ReplyText(turn_id="t", text="x"))) is False
    assert wacli.sent == []
