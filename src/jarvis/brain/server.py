"""Brain WebSocket server (Phase 3 W4).

Intercoms connect and pair; each connection gets its own BrainSession. Per
Utterance: STT -> think(+tools) -> TTS -> stream ReplyAudio frames -> ReplyEnd.
A BargeIn cancels the in-flight turn (mirrors the single-process stop_playback
cancelling the feed task). STT/TTS run in-process here (services co-located in
3a). Provider credentials live only on the brain — the intercom holds none.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import uuid

import websockets

from jarvis.brain.background import BackgroundRunner
from jarvis.brain.capabilities import context_for_resolution
from jarvis.brain.context import RequestContext
from jarvis.brain.contexts import ContextStore
from jarvis.brain.gateway_client import GatewayClient
from jarvis.brain.heartbeat import HeartbeatScheduler, make_heartbeat_think
from jarvis.brain.identity import HOUSE, IdentityResolver, load_users
from jarvis.brain.memory_client import MemoryClient
from jarvis.brain.proactive import proactive_frames
from jarvis.brain.scheduler import Ring, Scheduler
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.brain.skills import register_skills
from jarvis.brain.tracing import Tracer
from jarvis.config import Config
from jarvis.mcp import MCPBridge
from jarvis.protocol.messages import (
    BargeIn,
    Cancel,
    Hello,
    Identify,
    Proactive,
    Reject,
    ReplyAudio,
    ReplyEnd,
    ReplyText,
    TextIn,
    Transcript,
    Utterance,
    Welcome,
    decode,
    encode,
)
from jarvis.services.stt import Transcriber
from jarvis.services.tts import InworldTTS
from jarvis.tools import build_registry
from jarvis.tools.alarm import make_alarm_tools
from jarvis.tools.background import make_background_tool
from jarvis.tools.mcp import make_mcp_tools
from jarvis.tools.selection import build_relevance


import re

# Only consulted while an alarm is actually ringing, so it can be liberal — any of
# these silences it.
_ALARM_ACK = re.compile(
    r"\b(stop|dismiss|turn (it |the alarm )?off|cancel|enough|quiet|silence|shut up|"
    r"got it|okay|ok|alright|thanks)\b",
    re.IGNORECASE,
)


def _is_alarm_ack(text: str) -> bool:
    return bool(_ALARM_ACK.search(text or ""))


def _can_bind(host: str, port: int) -> bool:
    """True if (host, port) is free — a fast pre-flight so the brain gives a friendly
    'port in use' message instead of a raw bind traceback after loading models."""
    import socket

    addr = "127.0.0.1" if host == "localhost" else host
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((addr, port))
        return True
    except OSError:
        return False


def authorise_device(brain, device_id: str, token: str) -> tuple[bool, str]:  # noqa: ANN001
    """Authorise a pairing → (ok, device_default_identity). A per-device token
    (BRAIN_DEVICES) is bound to its device_id and may pin a default identity; the
    shared pairing_token is the fallback; no tokens configured => open (dev/local).
    Module-level + dependency-free so it's unit-testable without a full server."""
    for d in brain.devices:
        if d.token and token == d.token:
            if d.device_id and device_id != d.device_id:
                return (False, "")  # token bound to a different device
            return (True, d.identity)
    shared = brain.pairing_token.get_secret_value()
    if shared and token == shared:
        return (True, "")
    # Open only when NOTHING is configured (dev/local). If any token (shared or
    # per-device) is set, an unmatched token is rejected.
    if not shared and not brain.devices:
        return (True, "")
    return (False, "")


class BrainServer:
    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._stt = Transcriber(cfg.stt)
        self._tracer = Tracer(cfg.trace)
        self._registry = build_registry(
            cfg.tools, worker=cfg.worker, remote=cfg.remote, google=cfg.google, browser=cfg.browser
        )
        users = load_users(cfg.capabilities.users_dir)  # dict name -> User
        self._users = users  # for outbound (WhatsApp) routing
        # MCP servers are connected at startup (async, off the hot path); OAuth
        # servers connect per principal (house + each user) so credentials isolate.
        self._mcp = MCPBridge(cfg.mcp, principals=list(users))
        # The gateway/tts/memory clients are effectively stateless; share one set
        # across connections. Per-`(device × user)` state (history/memory peer)
        # lives in each BrainSession, owned by the ContextStore.
        self._gateway = GatewayClient(cfg.gateway)
        self._tts = InworldTTS(cfg.tts)
        self._memory = MemoryClient(cfg.memory)
        self._relevance = build_relevance(cfg, self._gateway)  # embedding scorer or None
        # Identity resolution (§5): who is speaking, per utterance.
        self._resolver = IdentityResolver(users)
        self._contexts = ContextStore(self._make_session)
        # Open intercom connections (for proactive heartbeat push, §3b). Also indexed by
        # device so an alarm/notification can be routed to the device that set it.
        self._connections: set = set()
        self._device_conns: dict[str, set] = {}
        # Background-task lane (fire-and-forget): start() returns instantly; the
        # outcome is pushed via the same proactive broadcast the heartbeat uses.
        self._background = BackgroundRunner(
            cfg.background, session_factory=self._make_session, notify=self._notify_completion
        )
        if cfg.background.enabled:
            self._registry.register(make_background_tool(self._background))
        # Alarms & timers: the scheduler fires rings on the device that set them.
        self._scheduler = Scheduler()
        if cfg.alarm.enabled:
            for tool in make_alarm_tools(self._scheduler, cfg):
                self._registry.register(tool)

    def _make_session(self, ctx: RequestContext) -> BrainSession:
        return BrainSession(
            self._cfg,
            ctx,
            gateway=self._gateway,
            tts=self._tts,
            memory=self._memory,
            tracer=self._tracer,
            registry=self._registry,
            memory_user=ctx.memory_peer,
            relevance=self._relevance,
        )

    async def serve(self) -> None:
        host, port = self._cfg.brain.host, self._cfg.brain.port
        if not _can_bind(host, port):  # fail fast, before loading the STT model
            print(
                f"\n✗ Port {port} is already in use — is another `jarvis brain` "
                f"running?\n  Free it with:  lsof -ti tcp:{port} | xargs kill\n"
            )
            return
        print("Loading STT model…")
        self._stt.load()
        await self._connect_mcp()
        host, port = self._cfg.brain.host, self._cfg.brain.port
        heartbeat: asyncio.Task | None = None
        alarms: asyncio.Task | None = None
        try:
            async with websockets.serve(self._handle, host, port):
                print(f"Brain listening on ws://{host}:{port}")
                if self._cfg.heartbeat.enabled:
                    sched = HeartbeatScheduler(
                        self._cfg.heartbeat,
                        think=make_heartbeat_think(self._cfg),
                        broadcast=self._broadcast,
                    )
                    heartbeat = asyncio.create_task(sched.run())
                    print(f"Heartbeat on (every {self._cfg.heartbeat.interval_s:.0f}s).")
                if self._cfg.alarm.enabled:
                    alarms = asyncio.create_task(self._alarm_loop())
                    print("Alarms on.")
                await asyncio.Future()  # run forever
        finally:
            for t in (heartbeat, alarms):
                if t is not None:
                    t.cancel()
            await self._mcp.aclose()

    async def _alarm_loop(self) -> None:
        """Tick the scheduler; deliver any rings to the device that set them. Guarded —
        an alarm failure must never crash the brain."""
        import time

        while True:
            await asyncio.sleep(max(0.2, self._cfg.alarm.tick_s))
            try:
                for ring in self._scheduler.tick(time.time()):
                    await self._deliver_ring(ring)
            except Exception as exc:  # noqa: BLE001 - proactive work is best-effort
                print(f"  [alarm] tick skipped: {exc}")

    async def _deliver_ring(self, ring: Ring) -> None:
        """Deliver one alarm ring to the device that set it: the tone every cycle, the
        spoken label only on the first ring (so it doesn't repeat the words each time)."""
        label = ring.label if ring.label != "alarm" else ""
        text = (f"Alarm: {label}." if label else "Your alarm.") if ring.first else (f"Alarm: {label}." if label else "Alarm.")
        conns = self._device_conns.get(ring.device_id, set())
        sent = await self._deliver_proactive(
            conns, text, kind="alarm", open_mic=False, speak=ring.first, tone=True
        )
        print(f"  [alarm] ring → device={ring.device_id} ({sent} conn){' (first)' if ring.first else ''}: {text}")

    async def _broadcast(self, text: str) -> None:
        """Push a proactive notification (heartbeat / background completion) to every
        connected intercom — chime + spoken text, and open the mic so the user can reply
        (turn it into a chat). Best-effort; a dead socket is skipped, never fatal."""
        print(f"  [proactive] notify → {len(self._connections)} intercom(s): {text}")
        await self._deliver_proactive(
            self._connections, text, kind="notification", open_mic=True, speak=True, tone=True
        )

    async def _deliver_proactive(self, conns, text, *, kind="notification", open_mic=False,  # noqa: ANN001
                                 speak=True, tone=True) -> int:
        """Send one proactive delivery (header + tone + spoken audio + end) to a set of
        connections. The TTS is synthesised once and reused across connections; text
        clients ignore the audio and just show the header text."""
        targets = list(conns)
        if not targets:
            return 0
        turn_id = "pa-" + uuid.uuid4().hex[:8]
        frames = await proactive_frames(
            self._tts, self._cfg.tts.sample_rate, text, turn_id=turn_id, kind=kind,
            open_mic=open_mic, speak=speak, tone=tone,
            sound=self._cfg.alarm.sound, freq=self._cfg.alarm.tone_freq,
        )
        n = 0
        for ws in targets:
            with contextlib.suppress(Exception):
                for f in frames:
                    await ws.send(f)
                n += 1
        return n

    async def _notify_completion(self, text: str, identity: str, device_id: str) -> None:
        """A background job finished → tell the person who asked: on their device, and
        (if NOTIFY_ALSO_WHATSAPP) on WhatsApp too. Opens the mic so they can reply."""
        await self._notify(text, device_id=device_id, identity=identity, kind="notification", open_mic=True)

    async def _notify(self, text: str, *, device_id: str = "", identity: str = "",
                      kind: str = "notification", open_mic: bool = True) -> None:
        conns = self._device_conns.get(device_id) if device_id else self._connections
        sent = await self._deliver_proactive(conns or set(), text, kind=kind, open_mic=open_mic)
        print(f"  [proactive] notify → device={device_id or 'all'} ({sent} conn): {text}")
        if self._cfg.notify.also_whatsapp and identity:
            await self._notify_whatsapp(identity, text)

    async def _notify_whatsapp(self, identity: str, text: str) -> None:
        """Forward a notification to the user's WhatsApp number(s) via the connector
        (it sends Proactive(to=number) out through wacli). No-op if they have no number
        or the connector isn't connected."""
        user = self._users.get(identity)
        wa_conns = self._device_conns.get(self._cfg.whatsapp.device_id, set())
        if not user or not user.whatsapp or not wa_conns:
            return
        for number in user.whatsapp:
            msg = encode(Proactive(text=text, to=number, kind="notification"))
            for ws in list(wa_conns):
                with contextlib.suppress(Exception):
                    await ws.send(msg)

    async def _connect_mcp(self) -> None:
        """Connect configured MCP servers and register their tools (best-effort —
        a failed server is skipped, never fatal), then load skills that compose them."""
        await self._mcp.start()
        for tool in make_mcp_tools(self._mcp):
            self._registry.register(tool)
        register_skills(self._registry, gateway=self._gateway, cfg=self._cfg)

    # --- pairing -----------------------------------------------------------
    def _authorise(self, hello: Hello) -> tuple[bool, str]:
        return authorise_device(self._cfg.brain, hello.device_id, hello.token)

    def _resolve(
        self, device_id: str, channel: str, asserted: str, utterance: str, device_default: str = HOUSE
    ) -> RequestContext:
        """Resolve who's speaking for this device/channel/utterance (§5) and build
        the per-utterance RequestContext (device profile + the speaker's grants)."""
        resolution = self._resolver.resolve(
            device_id=device_id, channel=channel, asserted=asserted,
            utterance=utterance, device_default=device_default,
        )
        caps_cfg = self._cfg.capabilities.model_copy(update={"device_id": device_id})
        ctx = context_for_resolution(caps_cfg, resolution)
        return dataclasses.replace(ctx, channel=channel)

    async def _handle(self, ws) -> None:  # noqa: ANN001
        try:
            first = decode(await ws.recv())
        except Exception:
            return
        if not isinstance(first, Hello):
            with contextlib.suppress(Exception):
                await ws.send(encode(Reject(reason="unauthorized")))
            return
        ok, device_default = self._authorise(first)
        if not ok:
            with contextlib.suppress(Exception):
                await ws.send(encode(Reject(reason="unauthorized")))
            return
        device_id = first.device_id
        channel = first.channel or "voice"
        # `asserted` is the connection's sticky claimed identity: a strong device may
        # assert at pairing, and a spoken claim ("it's Jules") updates it for the rest
        # of the conversation. Re-resolved per utterance (claims need the transcript).
        # `device_default` is the device's pinned principal (a personal device) — used
        # only when nobody is otherwise identified.
        conn = {"asserted": first.identity, "device_default": device_default or HOUSE}
        base = self._resolve(device_id, channel, conn["asserted"], "", conn["device_default"])
        await ws.send(
            encode(
                Welcome(
                    identity=base.identity, scope=base.scope, capabilities=sorted(base.capabilities)
                )
            )
        )
        print(f"intercom paired: device={device_id} channel={channel} identity={base.identity}")

        self._connections.add(ws)  # eligible for proactive heartbeat push
        self._device_conns.setdefault(device_id, set()).add(ws)  # for device-routed alarms
        turn: asyncio.Task | None = None
        try:
            async for raw in ws:
                try:
                    msg = decode(raw)
                except Exception:
                    continue
                if isinstance(msg, (Utterance, TextIn)):
                    turn = await self._cancel(turn)
                    turn = asyncio.create_task(self._run_turn(ws, device_id, channel, conn, msg))
                elif isinstance(msg, Identify):
                    if msg.identity:  # explicit claim from a non-voice client
                        conn["asserted"] = msg.identity
                elif isinstance(msg, BargeIn):
                    turn = await self._cancel(turn)
                    with contextlib.suppress(Exception):
                        await ws.send(encode(Cancel(turn_id=msg.turn_id)))
        finally:
            self._connections.discard(ws)
            conns = self._device_conns.get(device_id)
            if conns is not None:
                conns.discard(ws)
                if not conns:
                    self._device_conns.pop(device_id, None)
            await self._cancel(turn)

    @staticmethod
    async def _cancel(turn: asyncio.Task | None) -> None:
        if turn and not turn.done():
            turn.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await turn
        return None

    async def _run_turn(self, ws, device_id: str, channel: str, conn: dict, msg) -> None:  # noqa: ANN001
        if isinstance(msg, Utterance):
            text = await asyncio.to_thread(
                self._stt.transcribe, msg.pcm(), sample_rate=msg.sample_rate
            )
        else:  # TextIn
            text = msg.text
        turn_id = msg.turn_id
        if not text:
            await ws.send(encode(ReplyEnd(turn_id=turn_id, ended=False)))
            return
        print(f"  you: {text!r}")
        with contextlib.suppress(Exception):  # let the intercom print what was heard
            await ws.send(encode(Transcript(turn_id=turn_id, text=text)))
        text_only = isinstance(msg, TextIn) and msg.text_only
        # Alarm acknowledgement: if one is ringing on this device and the user says
        # stop/dismiss/etc, silence it without a full LLM turn.
        if self._scheduler.ringing_on(device_id) and _is_alarm_ack(text):
            stopped = self._scheduler.acknowledge(device_id)
            reply = "Alarm off." if stopped else "Okay."
            print(f"  [alarm] acknowledged on device={device_id}")
            if not text_only:
                with contextlib.suppress(Exception):
                    async for pcm in self._tts.synthesize_stream(reply):
                        await ws.send(encode(ReplyAudio.of(turn_id, pcm)))
            with contextlib.suppress(Exception):
                await ws.send(encode(ReplyText(turn_id=turn_id, text=reply)))
                await ws.send(encode(ReplyEnd(turn_id=turn_id, ended=False)))
            return
        # Resolve WHO this utterance is from (claim detection needs the transcript),
        # then route to that principal's session. A spoken claim sticks for the rest
        # of the conversation.
        ctx = self._resolve(device_id, channel, conn["asserted"], text, conn["device_default"])
        if ctx.confidence == "claimed" and ctx.identity != HOUSE:
            conn["asserted"] = ctx.identity
        session = self._contexts.get(ctx)
        trace = self._tracer.turn(room=self._cfg.gateway.room, speaker=ctx.identity)
        result = TurnResult()
        try:
            if text_only:  # text console / scripted test — reply text only, no TTS
                await session.respond_text(text, trace, result)
            else:
                async for pcm in session.respond(text, trace, result):
                    await ws.send(encode(ReplyAudio.of(turn_id, pcm)))
        except asyncio.CancelledError:
            session.finalize(text, result)  # remember what was actually said
            self._tracer.emit(trace)
            raise
        session.finalize(text, result)
        self._tracer.emit(trace)
        with contextlib.suppress(Exception):
            await ws.send(encode(ReplyText(turn_id=turn_id, text=result.reply)))
            await ws.send(encode(ReplyEnd(turn_id=turn_id, ended=result.ended)))


async def serve(cfg: Config) -> None:
    await BrainServer(cfg).serve()
