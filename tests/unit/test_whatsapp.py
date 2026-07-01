"""WhatsApp connector routing (Phase 3b) — inbound turn + outbound proactive forward.

The brain socket + wacli are faked; this proves the connector identifies the sender,
sends the text as a turn and returns the brain's reply (consuming the router queue), and
forwards a proactive notification (Proactive with a `to`) OUT via wacli. No network.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict

from jarvis.connectors.whatsapp import (
    InboundMessage,
    _remember_seen,
    _parse_messages,
    _user_numbers,
    add_whatsapp_number,
    chunk_text,
    forward_proactive,
    handle_message,
    is_allowed,
    parse_admin_cmd,
    route_inbound,
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


def _dm(text="hi", sender="447921815819@s.whatsapp.net"):  # noqa: ANN001, ANN202
    return InboundMessage(sender=sender, text=text, chat=sender)


def _grp(text, sender="447921815819@s.whatsapp.net", chat="123-456@g.us"):  # noqa: ANN001, ANN202
    return InboundMessage(sender=sender, text=text, chat=chat)


def _route(msg, **kw):  # noqa: ANN001, ANN202
    base = dict(dm_policy="allowlist", allow_from="447921815819",
                group_policy="ignore", group_allow="", trigger="jarvis")
    base.update(kw)
    return route_inbound(msg, **base)


def test_route_dm_uses_sender_allowlist() -> None:
    assert _route(_dm())[0] is True
    assert _route(_dm(sender="447000000000@s.whatsapp.net"))[0] is False


def test_route_group_ignored_by_default() -> None:
    assert _route(_grp("jarvis what's up"))[0] is False  # group_policy=ignore


def test_route_group_mention_only_when_called_out() -> None:
    # not called out → ignored
    assert _route(_grp("morning everyone"), group_policy="mention")[0] is False
    # called out → handled, trigger stripped from the text
    ok, text = _route(_grp("Jarvis, what's the weather?"), group_policy="mention")
    assert ok is True and text == "what's the weather?"
    # trigger mid-sentence still counts; full text passed through
    ok2, text2 = _route(_grp("can you ask jarvis about trains"), group_policy="mention")
    assert ok2 is True and "trains" in text2


def test_route_group_allowlist_restricts() -> None:
    msg = _grp("jarvis hi", chat="999-000@g.us")
    assert _route(msg, group_policy="mention", group_allow="123-456@g.us")[0] is False  # other group
    assert _route(msg, group_policy="mention", group_allow="999-000@g.us")[0] is True


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


def test_seen_messages_evict_oldest_not_random_recent() -> None:
    seen: OrderedDict[str, None] = OrderedDict()

    for i in range(1001):
        _remember_seen(seen, f"m{i}")

    assert "m0" not in seen
    assert "m1" in seen
    assert "m1000" in seen
    _remember_seen(seen, "m1")
    for i in range(1001, 2000):
        _remember_seen(seen, f"m{i}")
    assert "m1" in seen


# --- Remote pairing / onboarding ------------------------------------------


def test_parse_admin_cmd() -> None:
    # approve with a name, deny without, case/space-insensitive; code uppercased
    assert parse_admin_cmd("approve A1B2 Alice") == ("approve", "A1B2", "Alice")
    assert parse_admin_cmd("  APPROVE a1b2  Alice Smith ") == ("approve", "A1B2", "Alice Smith")
    assert parse_admin_cmd("deny A1B2") == ("deny", "A1B2", "")
    assert parse_admin_cmd("approve A1B2") == ("approve", "A1B2", "")  # name optional
    # not a command
    assert parse_admin_cmd("hello there") is None
    assert parse_admin_cmd("approve") is None  # no code
    assert parse_admin_cmd("") is None


def test_add_whatsapp_number_create_merge_exists(tmp_path) -> None:  # noqa: ANN001
    d = str(tmp_path)
    # 1. fresh user → file created with personal scope + own honcho peer
    assert add_whatsapp_number(d, "Alice", "+44 7000 000001") == "created"
    alice = (tmp_path / "alice.md").read_text()
    assert 'whatsapp: ["447000000001"]' in alice
    assert "scope: personal" in alice and "honcho_peer: alice" in alice
    # 2. same number again (any format) → idempotent, no change
    assert add_whatsapp_number(d, "Alice", "447000000001@s.whatsapp.net") == "exists"
    # 3. a second number for Alice → merged into the existing list, file preserved
    assert add_whatsapp_number(d, "Alice", "447000000002") == "merged"
    alice2 = (tmp_path / "alice.md").read_text()
    assert "447000000001" in alice2 and "447000000002" in alice2
    assert "scope: personal" in alice2  # everything else preserved


def test_add_whatsapp_number_merges_into_existing_file(tmp_path) -> None:  # noqa: ANN001
    # an existing hand-written user.md (no whatsapp line) → number inserted, body kept
    f = tmp_path / "bob.md"
    f.write_text("---\nscope: personal\ncapabilities: [web.search]\n---\n\n# Bob\nLikes trains.\n")
    assert add_whatsapp_number(str(tmp_path), "Bob", "447000000003") == "merged"
    out = f.read_text()
    assert "447000000003" in out
    assert "capabilities: [web.search]" in out and "Likes trains." in out  # preserved


def test_user_numbers_collects_across_files(tmp_path) -> None:  # noqa: ANN001
    (tmp_path / "alice.md").write_text('---\nwhatsapp: ["447000000001", "447000000002"]\n---\n')
    (tmp_path / "bob.md").write_text('---\nwhatsapp: ["+44 7000 000003"]\n---\n')
    (tmp_path / "nobody.md").write_text("---\nscope: house\n---\n")  # no number
    nums = _user_numbers(str(tmp_path))
    assert nums == {"447000000001", "447000000002", "447000000003"}
    assert _user_numbers(str(tmp_path / "missing")) == set()  # absent dir → empty
