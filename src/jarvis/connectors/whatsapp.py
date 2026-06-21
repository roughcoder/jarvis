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
import pathlib
import random
import re
import string
import time
from dataclasses import dataclass, replace
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
    name: str = ""  # SenderName (WhatsApp push name) — for the pairing prompt


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
                    msg_id=m.get("MsgID") or "", name=m.get("SenderName") or "",
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


_NAME_OK = re.compile(r"[^a-z0-9_-]")


def _slug(name: str) -> str:
    s = _NAME_OK.sub("_", (name or "").strip().lower().replace(" ", "_")).strip("_")
    return s[:48] or "user"


def parse_admin_cmd(text: str):  # noqa: ANN201 - ('approve'|'deny', code, name) | None
    """Parse an admin pairing command: 'approve <code> [name]' or 'deny <code>'."""
    m = re.match(r"\s*(approve|deny)\s+([A-Za-z0-9]{2,12})\s*(.*)$", text or "", re.IGNORECASE)
    if not m:
        return None
    return m.group(1).lower(), m.group(2).upper(), m.group(3).strip()


def _user_numbers(users_dir: str) -> set[str]:
    """Digits of every whatsapp number across users/*.md — seeds the connector's
    known-user allowlist so a paired user is recognised (incl. after a restart)."""
    out: set[str] = set()
    d = pathlib.Path(users_dir)
    if not d.is_dir():
        return out
    for f in d.glob("*.md"):
        for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
            m = re.match(r"\s*whatsapp:\s*\[(.*)\]", line)
            if m:
                out |= {_digits(e) for e in m.group(1).split(",") if _digits(e)}
    return out


def add_whatsapp_number(users_dir: str, name: str, number: str) -> str:
    """Add `number` to users/<slug>.md's whatsapp list. MERGES into an existing file
    (preserving capabilities/scope/memory/body; idempotent) or creates a fresh personal
    user. Returns 'created' | 'merged' | 'exists'."""
    d = pathlib.Path(users_dir)
    d.mkdir(parents=True, exist_ok=True)
    slug = _slug(name)
    path = d / f"{slug}.md"
    nd = _digits(number) or number.strip()
    if not path.exists():
        path.write_text(
            f'---\n# {name} — paired via WhatsApp\nwhatsapp: ["{nd}"]\n'
            f"scope: personal\nhoncho_peer: {slug}\n---\n\n# {name}\n",
            encoding="utf-8",
        )
        return "created"
    text = path.read_text(encoding="utf-8", errors="replace")
    m = re.search(r"^(whatsapp:\s*)\[(.*)\]\s*$", text, re.MULTILINE)
    if m:
        existing = [e.strip().strip("'\"") for e in m.group(2).split(",") if e.strip()]
        if any(_digits(e) == _digits(number) for e in existing):
            return "exists"
        existing.append(nd)
        new_line = m.group(1) + "[" + ", ".join(f'"{e}"' for e in existing) + "]"
        path.write_text(text[: m.start()] + new_line + text[m.end():], encoding="utf-8")
        return "merged"
    # No whatsapp line — insert one right after the opening front-matter '---'.
    path.write_text(re.sub(r"(?m)^---\s*$", f'---\nwhatsapp: ["{nd}"]', text, count=1), encoding="utf-8")
    return "merged"


def _is_group(chat: str) -> bool:
    return (chat or "").endswith("@g.us")


def _called_out(text: str, trigger: str) -> tuple[bool, str]:
    """In a group: is the bot addressed by `trigger` (as a word)? Returns (called,
    cleaned) where a leading 'Jarvis,' prefix is stripped so the brain gets the request."""
    t = (trigger or "").strip().lower()
    if not t:
        return True, text
    if not re.search(rf"\b{re.escape(t)}\b", (text or "").lower()):
        return False, ""
    cleaned = re.sub(rf"^\s*{re.escape(t)}\b[\s,:.!?-]*", "", text or "", flags=re.IGNORECASE).strip()
    return True, cleaned or text


def route_inbound(
    msg: "InboundMessage", *, dm_policy: str, allow_from: str,
    group_policy: str, group_allow: str, trigger: str,
) -> tuple[bool, str]:
    """Decide whether to act on an inbound message and with what text. DMs use the
    sender allowlist; groups use group_policy ('ignore' | 'mention' — only when called
    out | 'open'), optionally restricted to allowed group JIDs. Pure + unit-tested."""
    if _is_group(msg.chat):
        if group_policy == "ignore":
            return False, ""
        groups = {g.strip() for g in group_allow.split(",") if g.strip()}
        if groups and msg.chat not in groups and msg.chat.split("@", 1)[0] not in groups:
            return False, ""
        if group_policy == "open":
            return True, msg.text
        return _called_out(msg.text, trigger)  # "mention"
    return is_allowed(msg.sender, dm_policy, allow_from), msg.text


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
        account = getattr(self._cfg, "account", "").strip()
        if account:
            argv += ["--account", account]
        return argv

    async def start_sync(self):  # noqa: ANN202 - returns the background sync process
        """Keep the local DB warm (`wacli sync --follow`), so polls see new messages.
        stdin is detached to /dev/null: inheriting the connector's closed background
        stdin makes `wacli sync` see EOF and shut itself straight back down."""
        return await asyncio.create_subprocess_exec(
            *self._base(), "sync", "--follow",
            stdin=asyncio.subprocess.DEVNULL,
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
        self._allowed: set[str] = set()  # digits allowed to DM (allow_from + users + paired)
        self._pending: dict[str, tuple[str, str, float]] = {}  # code -> (number, name, expiry)

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

    async def _handle_dm(self, ws, inbound: asyncio.Queue, msg: InboundMessage) -> None:  # noqa: ANN001
        """A 1:1 message: admin pairing commands first, then allowed senders get a turn,
        then (under 'pairing') an unknown sender starts an admin-approved onboarding."""
        wa = self._cfg.whatsapp
        admin = _digits(wa.admin)
        sd = _digits(msg.sender)
        if admin and sd == admin:
            cmd = parse_admin_cmd(msg.text)
            if cmd:
                await self._do_admin_cmd(*cmd)
                return
        if sd in self._allowed:
            reply = await handle_message(ws, inbound, msg)
            if reply:
                await self._wacli.send(msg.chat or msg.sender, reply)
            return
        if wa.dm_policy == "pairing":
            await self._start_pairing(msg)
            return
        print(f"  [whatsapp] ignored DM from non-allowed {msg.sender}")

    async def _start_pairing(self, msg: InboundMessage) -> None:
        self._pending = {c: v for c, v in self._pending.items() if v[2] > time.time()}  # prune
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=4))
        self._pending[code] = (msg.sender, msg.name or "", time.time() + 3600)
        who = msg.name or _digits(msg.sender)
        await self._wacli.send(
            msg.sender, "Hi! You're not set up with Jarvis yet — I've asked the admin to "
            "approve you. Hang tight.",
        )
        admin = self._cfg.whatsapp.admin
        if admin:
            await self._wacli.send(
                admin, f"📲 {who} ({_digits(msg.sender)}) wants to connect.\n"
                f"Reply: approve {code} <name>   (or: deny {code})",
            )
        print(f"  [whatsapp] pairing requested by {msg.sender} code={code}")

    async def _do_admin_cmd(self, action: str, code: str, name: str) -> None:
        admin = self._cfg.whatsapp.admin
        pending = self._pending.pop(code, None)
        if pending is None or pending[2] < time.time():
            await self._wacli.send(admin, f"No pending pairing for code {code}.")
            return
        number, pname, _ = pending
        if action == "deny":
            await self._wacli.send(admin, f"Denied pairing {code}.")
            await self._wacli.send(number, "Sorry — you weren't approved to use Jarvis.")
            return
        final_name = name or pname or "user"
        result = add_whatsapp_number(self._cfg.capabilities.users_dir, final_name, number)
        self._allowed.add(_digits(number))  # recognised immediately, no restart
        await self._wacli.send(admin, f"✓ Added {final_name} ({result}).")
        await self._wacli.send(number, f"You're all set — welcome, {final_name}! You can talk to Jarvis now.")
        print(f"  [whatsapp] paired {number} as {final_name} ({result})")

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
            # DM allowlist seed: explicit numbers + everyone already in users/*.md.
            self._allowed = {_digits(n) for n in wa.allow_from.split(",") if _digits(n)}
            self._allowed |= _user_numbers(self._cfg.capabilities.users_dir)
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
                        if _is_group(msg.chat):
                            # groups: reply only when called out (route_inbound).
                            ok, text = route_inbound(
                                msg, dm_policy=wa.dm_policy, allow_from=wa.allow_from,
                                group_policy=wa.group_policy, group_allow=wa.group_allow, trigger=wa.trigger,
                            )
                            if not ok:
                                continue
                            reply = await handle_message(ws, inbound, replace(msg, text=text))
                            if reply:
                                await self._wacli.send(msg.chat, reply)
                        else:
                            await self._handle_dm(ws, inbound, msg)  # DM: allowlist + pairing
                    if len(seen) > 1000:  # bound the dedup set
                        seen = set(list(seen)[-500:])
            finally:
                router.cancel()
                with contextlib.suppress(Exception):
                    sync.terminate()
