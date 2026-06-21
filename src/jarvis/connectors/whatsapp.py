"""WhatsApp connector (Phase 3b) — wraps `wacli` and bridges it to the brain.

Inbound WhatsApp messages become brain turns over the WebSocket protocol
(channel=whatsapp; the sender's number is the asserted identity, which the brain's
resolver maps to a user via `users/<name>.md`); the brain's reply text goes back
out through `wacli`. The connector is a thin boundary peer — it imports nothing from
the brain and holds only a pairing token (the credential boundary, §3).

`wacli` (wacli.sh) is an external dependency (not vendored). Its model is poll-based:
`sync --follow` keeps a local DB warm, `messages list --from-them --after <ts>` reads
new inbound, `send text --to --message` sends. The integration test self-skips when the
binary is absent; the parsing + routing are pure and unit-tested.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from jarvis.config import Config, WhatsAppConfig
from jarvis.protocol.messages import (
    Hello,
    Identify,
    Proactive,
    ReplyEnd,
    ReplyText,
    TextIn,
    Welcome,
    decode,
    encode,
)


@dataclass(frozen=True)
class InboundMessage:
    sender: str  # SenderJID — the WhatsApp jid/number the brain resolves to a user
    text: str
    chat: str = ""  # ChatJID — where to send the reply (the conversation)
    ts: str = ""  # Timestamp (RFC3339) — the poll cursor
    msg_id: str = ""  # MsgID — for dedup across polls


def _parse_messages(obj: dict) -> list[InboundMessage]:
    """Parse `wacli messages list --json` into inbound messages (skip own + empty)."""
    rows = ((obj or {}).get("data") or {}).get("messages") or []
    out: list[InboundMessage] = []
    for m in rows:
        if not isinstance(m, dict) or m.get("FromMe"):
            continue
        text = (m.get("Text") or m.get("DisplayText") or "").strip()
        sender = m.get("SenderJID") or ""
        if text and sender:
            out.append(
                InboundMessage(
                    sender=sender, text=text,
                    chat=m.get("ChatJID") or sender, ts=m.get("Timestamp") or "",
                    msg_id=m.get("MsgID") or "",
                )
            )
    return out


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _digits(num: str) -> str:
    """Phone/jid → digits only (drop '@…' suffix, '+', spaces) for allowlist matching."""
    return re.sub(r"\D", "", (num or "").split("@", 1)[0])


def is_allowed(sender: str, policy: str, allow_from: str) -> bool:
    """Whether an inbound sender may drive a turn (OpenClaw's dmPolicy/allowFrom). Default
    'allowlist' is deny-by-default: only numbers in `allow_from` (any format) get through."""
    if policy == "open":
        return True
    if policy == "disabled":
        return False
    allowed = {_digits(n) for n in allow_from.split(",") if n.strip()}
    return _digits(sender) in allowed


def chunk_text(text: str, limit: int) -> list[str]:
    """Split a reply into <=limit-char chunks (prefer a newline/space boundary) for
    WhatsApp's message-length cap. Returns [text] when it already fits."""
    text = text or ""
    if limit <= 0 or len(text) <= limit:
        return [text] if text else []
    chunks, rest = [], text
    while len(rest) > limit:
        cut = rest.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = rest.rfind(" ", 0, limit)
        if cut < limit // 2:
            cut = limit
        chunks.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip()
    if rest:
        chunks.append(rest)
    return chunks


class WacliClient:
    """Thin wrapper over the `wacli` CLI (wacli.sh)."""

    def __init__(self, cfg: WhatsAppConfig) -> None:
        self._cfg = cfg

    def _base(self) -> list[str]:
        argv = [self._cfg.wacli_bin]
        if getattr(self._cfg, "account", ""):
            argv += ["--account", self._cfg.account]
        return argv

    async def start_sync(self):  # noqa: ANN202 - returns the background sync process
        """Keep the local DB warm (`wacli sync --follow`), so polls see new messages."""
        return await asyncio.create_subprocess_exec(
            *self._base(), "sync", "--follow",
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )

    async def poll(self, after: str, limit: int = 20) -> list[InboundMessage]:
        """New inbound messages since `after` (RFC3339), oldest first."""
        proc = await asyncio.create_subprocess_exec(
            *self._base(), "messages", "list", "--json", "--from-them",
            "--after", after, "--asc", "--limit", str(limit),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        out, _err = await proc.communicate()
        try:
            return _parse_messages(json.loads(out.decode("utf-8", "replace")))
        except (json.JSONDecodeError, ValueError):
            return []

    async def send(self, to: str, text: str) -> None:
        for chunk in chunk_text(text, self._cfg.text_chunk_limit):
            proc = await asyncio.create_subprocess_exec(
                *self._base(), "send", "text", "--to", to, "--message", chunk,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
            out, _ = await proc.communicate()
            if proc.returncode != 0:
                print(f"  [whatsapp] send to {to} failed: {out.decode('utf-8', 'replace').strip()}")


async def forward_proactive(wacli: WacliClient, m) -> bool:  # noqa: ANN001
    """If `m` is a proactive notification addressed to a number, push it OUT via wacli
    (so a background result / heartbeat reaches the user on WhatsApp). Returns True if it
    was a (handled) outbound proactive. Pure routing — unit-tested with a fake wacli."""
    if isinstance(m, Proactive) and m.to:
        await wacli.send(m.to, m.text)
        return True
    return False


async def handle_message(ws, inbound, msg: InboundMessage) -> str:  # noqa: ANN001
    """Drive ONE inbound message through the brain and return the reply text. Sends an
    Identify (the sender is the asserted identity) then a TextIn, and collects ReplyText
    up to ReplyEnd from the router queue."""
    import uuid

    turn_id = uuid.uuid4().hex
    await ws.send(encode(Identify(identity=msg.sender)))
    # text_only → the brain skips TTS (WhatsApp wants text; no wasted/blocking synthesis).
    await ws.send(encode(TextIn(turn_id=turn_id, text=msg.text, text_only=True)))
    reply = ""
    while True:
        m = await inbound.get()
        if isinstance(m, ReplyText) and m.turn_id == turn_id:
            reply = m.text
        elif isinstance(m, ReplyEnd) and m.turn_id == turn_id:
            return reply


class WhatsAppConnector:
    def __init__(self, cfg: Config, *, wacli: WacliClient | None = None) -> None:
        self._cfg = cfg
        self._wacli = wacli or WacliClient(cfg.whatsapp)

    async def _router(self, ws, inbound: asyncio.Queue) -> None:  # noqa: ANN001
        """Read the brain socket: forward outbound proactives via wacli; queue turn
        reply frames for handle_message."""
        try:
            async for raw in ws:
                with contextlib.suppress(Exception):
                    m = decode(raw)
                    if await forward_proactive(self._wacli, m):
                        continue
                    if isinstance(m, (ReplyText, ReplyEnd)):
                        inbound.put_nowait(m)
        except Exception:  # noqa: BLE001 - socket closed
            pass

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
            print("Paired. Syncing WhatsApp + polling for messages…")
            wa = self._cfg.whatsapp
            inbound: asyncio.Queue = asyncio.Queue()
            router = asyncio.create_task(self._router(ws, inbound))
            sync = await self._wacli.start_sync()
            cursor = _now_rfc3339()  # only react to messages from now on (no history replay)
            seen: set[str] = set()
            try:
                while True:
                    await asyncio.sleep(max(1.0, self._cfg.whatsapp.poll_interval_s))
                    for msg in await self._wacli.poll(cursor):
                        if msg.msg_id and msg.msg_id in seen:
                            continue
                        seen.add(msg.msg_id)
                        if msg.ts:
                            cursor = max(cursor, msg.ts)
                        # deny-by-default: only allowed numbers drive a turn (don't let a
                        # stranger texting the line spend LLM/tools).
                        if not is_allowed(msg.sender, wa.dm_policy, wa.allow_from):
                            print(f"  [whatsapp] ignored message from non-allowed sender {msg.sender}")
                            continue
                        reply = await handle_message(ws, inbound, msg)
                        if reply:
                            await self._wacli.send(msg.chat or msg.sender, reply)
                    if len(seen) > 1000:  # bound the dedup set
                        seen = set(list(seen)[-500:])
            finally:
                router.cancel()
                with contextlib.suppress(Exception):
                    sync.terminate()
