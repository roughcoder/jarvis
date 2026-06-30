"""Brain WebSocket server (Phase 3 W4).

Intercoms connect and pair; each connection gets its own BrainSession. Per
voice turn: stream binary uplink audio -> STT -> think(+tools) -> TTS ->
stream binary reply-audio frames -> ReplyEnd.
A BargeIn cancels the in-flight turn (mirrors the single-process stop_playback
cancelling the feed task). STT/TTS run in-process here (services co-located in
3a). Provider credentials live only on the brain — the intercom holds none.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import time
import uuid

import websockets

from jarvis.brain.background import BackgroundRunner
from jarvis.brain.capabilities import context_for_resolution
from jarvis.runtime import RequestContext
from jarvis.brain.contexts import ContextStore
from jarvis.brain.gateway_client import GatewayClient
from jarvis.brain.heartbeat import HeartbeatScheduler, make_heartbeat_think
from jarvis.brain.identity import HOUSE, IdentityResolver, load_users
from jarvis.brain.memory_client import MemoryClient
from jarvis.brain.proactive import proactive_frames
from jarvis.brain.scheduler import Ring, Scheduler, in_quiet_hours
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.brain.skills import register_skills
from jarvis.brain.tracing import Tracer
from jarvis.brain.voice_modes import (
    DEFAULT_MODE,
    alarm_ack_transition,
    cancelled_voice_transition,
    empty_transcript_transition,
    normalize_mode,
    voice_result_transition,
)
from jarvis.config import Config, insecure_bind
from jarvis.mcp import MCPBridge
from jarvis.protocol.messages import (
    AudioEnd,
    AudioStart,
    BargeIn,
    BinaryAudio,
    Cancel,
    ConversationIdle,
    DeviceRequest,
    DeviceResponse,
    Hello,
    Identify,
    Proactive,
    Reject,
    REPLY_AUDIO_BINARY_V1,
    ReplyEnd,
    ReplyText,
    TextIn,
    Transcript,
    UPLINK_AUDIO_BINARY_V1,
    Welcome,
    decode_binary_audio,
    decode,
    encode_reply_audio_binary,
    encode,
)
from jarvis.services.stt import Transcriber
from jarvis.services.tts import InworldTTS
from jarvis.tools import build_registry
from jarvis.tools.alarm import make_alarm_tools
from jarvis.tools.background import make_background_tool
from jarvis.tools.intercom import make_intercom_tools
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

_HARDWARE_CAPS = {
    "intercom.camera": "camera",
    "intercom.display": "display",
}

_TURN_ERROR_REPLY = "I hit an error before I could answer that."
_MAX_UPLINK_AUDIO_S = 30.0
_MAX_UPLINK_SAMPLE_RATE = 48_000


@dataclasses.dataclass
class BufferedAudioTurn:
    turn_id: str
    sample_rate: int
    pcm: bytes
    chunks: int
    frame_bytes: int
    stream_ms: float
    voice_mode: str = DEFAULT_MODE


def _is_alarm_ack(text: str) -> bool:
    return bool(_ALARM_ACK.search(text or ""))


def _dir_mtime(users_dir: str) -> float:
    """Newest mtime among users/*.md (0 if none) — cheap change-detector so a WhatsApp
    pairing that writes a new user file is picked up live, without a brain restart."""
    import pathlib

    d = pathlib.Path(users_dir)
    return max((f.stat().st_mtime for f in d.glob("*.md")), default=0.0) if d.is_dir() else 0.0


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
        # The gateway/tts/memory clients are effectively stateless; share one set
        # across connections. Per-`(device × user)` state (history/memory peer)
        # lives in each BrainSession, owned by the ContextStore. Memory is built
        # before the registry so the profile tools can seed Honcho on save (cold).
        self._gateway = GatewayClient(cfg.gateway)
        self._tts = InworldTTS(cfg.tts)
        self._memory = MemoryClient(cfg.memory)
        self._registry = build_registry(
            cfg.tools, worker=cfg.worker, remote=cfg.remote, google=cfg.google,
            accounts=cfg.accounts, browser=cfg.browser, capabilities=cfg.capabilities,
            memory=self._memory,
        )
        users = load_users(cfg.capabilities.users_dir)  # dict name -> User
        self._users = users  # for outbound (WhatsApp) routing
        # MCP servers are connected at startup (async, off the hot path); OAuth
        # servers connect per principal (house + each user) so credentials isolate.
        self._mcp = MCPBridge(cfg.mcp, principals=list(users))
        self._relevance = build_relevance(cfg, self._gateway)  # embedding scorer or None
        # Identity resolution (§5): who is speaking, per utterance.
        self._resolver = IdentityResolver(users)
        self._users_mtime = _dir_mtime(cfg.capabilities.users_dir)  # for live reload (WhatsApp pairing)
        self._contexts = ContextStore(self._make_session)
        # Open intercom connections (for proactive heartbeat push, §3b). Also indexed by
        # device so an alarm/notification can be routed to the device that set it.
        self._connections: set = set()
        self._device_conns: dict[str, set] = {}
        self._conn_meta: dict = {}
        # Idle-aware notification timing: devices mid-turn, and notifications held until
        # the gap (busy clears / quiet hours end). Alarms bypass this.
        self._busy: set[str] = set()
        self._pending: dict[str, list] = {}
        # Background-task lane (fire-and-forget): start() returns instantly; the
        # outcome is pushed via the same proactive broadcast the heartbeat uses.
        self._background = BackgroundRunner(
            cfg.background, session_factory=self._make_session, notify=self._notify_completion
        )
        if cfg.background.enabled:
            self._registry.register(make_background_tool(self._background))
        for tool in make_intercom_tools(self._request_device_action):
            self._registry.register(tool)
        # Alarms & timers: the scheduler fires rings on the device that set them.
        self._scheduler = Scheduler()
        if cfg.alarm.enabled:
            for tool in make_alarm_tools(self._scheduler, cfg):
                self._registry.register(tool)

    def _make_session(self, ctx: RequestContext) -> BrainSession:
        session = BrainSession(
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
        session.load_soul()  # personality is authoritative for ALL sessions (incl. background)
        return session

    async def serve(self) -> None:
        host, port = self._cfg.brain.host, self._cfg.brain.port
        b = self._cfg.brain
        has_token = bool(b.pairing_token.get_secret_value()) or bool(b.devices)
        if insecure_bind(host, has_token, b.allow_insecure):  # don't expose unauth access on a LAN
            print(
                f"\n✗ Refusing to start: brain is bound to {host!r} (non-loopback) with no "
                "pairing token — that's unauthenticated network access.\n  Set "
                "BRAIN_PAIRING_TOKEN or BRAIN_DEVICES, or BRAIN_ALLOW_INSECURE=true to override.\n"
            )
            return
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
            async with websockets.serve(
                self._handle,
                host,
                port,
                max_size=self._cfg.brain.websocket_max_size,
                ping_interval=self._cfg.brain.websocket_ping_interval_s,
                ping_timeout=self._cfg.brain.websocket_ping_timeout_s,
            ):
                print(f"Brain listening on ws://{host}:{port}")
                if self._cfg.heartbeat.enabled:
                    sched = HeartbeatScheduler(
                        self._cfg.heartbeat,
                        think=make_heartbeat_think(self._cfg),
                        broadcast=self._broadcast,
                        tracer=self._tracer,
                        room=self._cfg.gateway.room,
                    )
                    heartbeat = asyncio.create_task(sched.run())
                    print(f"Heartbeat on (every {self._cfg.heartbeat.interval_s:.0f}s).")
                alarms = asyncio.create_task(self._proactive_loop())
                if self._cfg.alarm.enabled:
                    print("Alarms on.")
                await asyncio.Future()  # run forever
        finally:
            for t in (heartbeat, alarms):
                if t is not None:
                    t.cancel()
            await self._mcp.aclose()

    async def _proactive_loop(self) -> None:
        """Tick alarms (deliver rings to the setting device) and flush held
        notifications to devices that are now idle. Guarded — must never crash the brain."""
        import time

        while True:
            await asyncio.sleep(max(0.2, self._cfg.alarm.tick_s))
            try:
                if self._cfg.alarm.enabled:
                    for ring in self._scheduler.tick(time.time()):
                        await self._deliver_ring(ring)
                await self._flush_pending()
                self._maybe_reload_users()
            except Exception as exc:  # noqa: BLE001 - proactive work is best-effort
                print(f"  [proactive] tick skipped: {exc}")

    def _maybe_reload_users(self) -> None:
        """Pick up a newly paired/edited user (e.g. WhatsApp pairing wrote users/alice.md)
        without a restart: rebuild the resolver + outbound map when the dir changes."""
        mt = _dir_mtime(self._cfg.capabilities.users_dir)
        if mt <= self._users_mtime:
            return
        self._users_mtime = mt
        users = load_users(self._cfg.capabilities.users_dir)
        self._users = users
        self._resolver = IdentityResolver(users)
        print(f"  [users] reloaded — {len(users)} principal(s)")

    async def _deliver_ring(self, ring: Ring) -> None:
        """Deliver one alarm ring to the device that set it: the tone every cycle, the
        spoken label only on the first ring (so it doesn't repeat the words each time)."""
        # Don't ring into an active reply — the intercom would discard the proactive
        # frames as a foreign turn. The alarm repeats, so the next ring lands once the
        # device is idle (between turns, seconds away).
        if ring.device_id in self._busy:
            print(f"  [alarm] ring skipped (device busy) device={ring.device_id}")
            return
        label = ring.label if ring.label != "alarm" else ""
        text = (f"Alarm: {label}." if label else "Your alarm.") if ring.first else (f"Alarm: {label}." if label else "Alarm.")
        conns = self._device_conns.get(ring.device_id, set())
        # open_mic so a ringing alarm listens for the ack right after the tone — you can
        # just say "stop"/"dismiss", no wake word needed.
        sent = await self._deliver_proactive(
            conns, text, kind="alarm", open_mic=True, speak=ring.first, tone=True
        )
        print(f"  [alarm] ring → device={ring.device_id} ({sent} conn){' (first)' if ring.first else ''}: {text}")

    async def _broadcast(self, text: str) -> None:
        """Push a proactive notification (heartbeat / background completion) to every
        connected intercom — chime + spoken text, and open the mic so the user can reply
        (turn it into a chat). Best-effort; a dead socket is skipped, never fatal."""
        if self._quiet_now():
            print(f"  [proactive] heartbeat suppressed (quiet hours): {text}")
            return
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

    def _quiet_now(self) -> bool:
        import time

        return in_quiet_hours(
            time.time(), self._cfg.notify.quiet_start, self._cfg.notify.quiet_end,
            self._cfg.persona.timezone,
        )

    async def _notify(self, text: str, *, device_id: str = "", identity: str = "",
                      kind: str = "notification", open_mic: bool = True) -> None:
        # WhatsApp delivery isn't held (a silent text that reaches them when out).
        if self._cfg.notify.also_whatsapp and identity:
            await self._notify_whatsapp(identity, text)
        # Idle-aware: hold a notification if the device is mid-conversation or it's quiet
        # hours; the proactive tick flushes it once idle / quiet ends. (Alarms bypass —
        # they come via _deliver_ring.)
        if device_id and (device_id in self._busy or self._quiet_now()):
            self._pending.setdefault(device_id, []).append((text, kind, open_mic))
            print(f"  [proactive] held for device={device_id} (busy/quiet): {text}")
            return
        conns = self._device_conns.get(device_id) if device_id else self._connections
        sent = await self._deliver_proactive(conns or set(), text, kind=kind, open_mic=open_mic)
        print(f"  [proactive] notify → device={device_id or 'all'} ({sent} conn): {text}")

    async def _flush_pending(self) -> None:
        """Deliver held notifications to devices that are now idle (and not in quiet
        hours). Called from the proactive tick."""
        if not self._pending:
            return
        quiet = self._quiet_now()
        for device_id in list(self._pending):
            if device_id in self._busy or quiet:
                continue
            held = self._pending.pop(device_id, [])
            conns = self._device_conns.get(device_id, set())
            for text, kind, open_mic in held:
                await self._deliver_proactive(conns, text, kind=kind, open_mic=open_mic)
                print(f"  [proactive] flushed → device={device_id}: {text}")

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
        self,
        device_id: str,
        channel: str,
        asserted: str,
        utterance: str,
        device_default: str = HOUSE,
        hardware: set[str] | None = None,
    ) -> RequestContext:
        """Resolve who's speaking for this device/channel/utterance (§5) and build
        the per-utterance RequestContext (device profile + the speaker's grants)."""
        resolution = self._resolver.resolve(
            device_id=device_id, channel=channel, asserted=asserted,
            utterance=utterance, device_default=device_default,
        )
        caps_cfg = self._cfg.capabilities.model_copy(update={"device_id": device_id})
        ctx = context_for_resolution(caps_cfg, resolution)
        ctx = self._with_live_hardware(ctx, hardware or set())
        return dataclasses.replace(ctx, channel=channel)

    @staticmethod
    def _with_live_hardware(ctx: RequestContext, hardware: set[str]) -> RequestContext:
        caps = set(ctx.capabilities)
        for cap, required_hardware in _HARDWARE_CAPS.items():
            if cap in caps and required_hardware not in hardware:
                caps.remove(cap)
        return dataclasses.replace(ctx, capabilities=frozenset(caps))

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
        hardware = {h.strip().lower() for h in first.hardware if h.strip()}
        conn = {
            "asserted": first.identity,
            "base_asserted": first.identity,
            "device_default": device_default or HOUSE,
            "hardware": hardware,
            "voice_mode": "default",
            "waiters": {},
            "audio_buffers": {},
        }
        base = self._resolve(
            device_id, channel, conn["asserted"], "", conn["device_default"], hardware
        )
        await ws.send(
            encode(
                Welcome(
                    identity=base.identity,
                    scope=base.scope,
                    capabilities=sorted(base.capabilities),
                )
            )
        )
        hw = ",".join(sorted(hardware)) or "none"
        print(
            f"intercom paired: device={device_id} channel={channel} "
            f"identity={base.identity} hardware={hw} audio={REPLY_AUDIO_BINARY_V1}"
        )

        self._connections.add(ws)  # eligible for proactive heartbeat push
        self._device_conns.setdefault(device_id, set()).add(ws)  # for device-routed alarms
        self._conn_meta[ws] = conn
        turn: asyncio.Task | None = None
        try:
            async for raw in ws:
                if isinstance(raw, bytes):
                    try:
                        binary = decode_binary_audio(raw)
                    except ValueError:
                        continue
                    if binary is not None and binary.kind == "uplink_audio":
                        ok = self._buffer_audio_chunk(conn, binary, frame_bytes=len(raw))
                        if not ok:
                            await ws.close(code=1009, reason="uplink audio too large")
                            break
                    continue
                try:
                    msg = decode(raw)
                except Exception:
                    continue
                if isinstance(msg, AudioStart):
                    sample_rate = max(1, min(msg.sample_rate, _MAX_UPLINK_SAMPLE_RATE))
                    conn["audio_buffers"][msg.turn_id] = {
                        "sample_rate": sample_rate,
                        "voice_mode": normalize_mode(msg.voice_mode),
                        "chunks": [],
                        "pcm_bytes": 0,
                        "frame_bytes": 0,
                        "max_pcm_bytes": int(sample_rate * 2 * _MAX_UPLINK_AUDIO_S),
                        "started_at": time.perf_counter(),
                    }
                elif isinstance(msg, AudioEnd):
                    buffered = self._finish_audio_buffer(conn, msg.turn_id)
                    if buffered is not None:
                        turn = await self._cancel(turn)
                        turn = asyncio.create_task(
                            self._run_turn(ws, device_id, channel, conn, buffered)
                        )
                elif isinstance(msg, TextIn):
                    turn = await self._cancel(turn)
                    turn = asyncio.create_task(self._run_turn(ws, device_id, channel, conn, msg))
                elif isinstance(msg, ConversationIdle):
                    self._reset_voice_conversation(channel, conn)
                elif isinstance(msg, Identify):
                    if msg.identity:  # explicit claim from a non-voice client
                        conn["asserted"] = msg.identity
                elif isinstance(msg, DeviceResponse):
                    fut = conn["waiters"].pop(msg.request_id, None)
                    if fut is not None and not fut.done():
                        fut.set_result(msg)
                elif isinstance(msg, BargeIn):
                    turn = await self._cancel(turn)
                    with contextlib.suppress(Exception):
                        await ws.send(encode(Cancel(turn_id=msg.turn_id)))
        finally:
            self._connections.discard(ws)
            self._conn_meta.pop(ws, None)
            for fut in list(conn.get("waiters", {}).values()):
                if not fut.done():
                    fut.set_exception(ConnectionError("intercom disconnected"))
            conns = self._device_conns.get(device_id)
            if conns is not None:
                conns.discard(ws)
                if not conns:
                    self._device_conns.pop(device_id, None)
            await self._cancel(turn)

    @staticmethod
    def _buffer_audio_chunk(conn: dict, binary: BinaryAudio, *, frame_bytes: int) -> bool:
        buffers = conn.get("audio_buffers", {})
        buf = buffers.get(binary.turn_id)
        if buf is None:
            return True
        pcm_bytes = int(buf.get("pcm_bytes") or 0) + len(binary.pcm)
        max_pcm_bytes = int(buf.get("max_pcm_bytes") or 0)
        if max_pcm_bytes and pcm_bytes > max_pcm_bytes:
            buffers.pop(binary.turn_id, None)
            return False
        buf["chunks"].append(binary.pcm)
        buf["pcm_bytes"] = pcm_bytes
        buf["frame_bytes"] += frame_bytes
        if binary.sample_rate:
            buf["sample_rate"] = binary.sample_rate
        return True

    @staticmethod
    def _finish_audio_buffer(conn: dict, turn_id: str) -> BufferedAudioTurn | None:
        buf = conn.get("audio_buffers", {}).pop(turn_id, None)
        if buf is None:
            return None
        chunks = list(buf.get("chunks", []))
        pcm = b"".join(chunks)
        return BufferedAudioTurn(
            turn_id=turn_id,
            sample_rate=int(buf.get("sample_rate") or 16000),
            pcm=pcm,
            chunks=len(chunks),
            frame_bytes=int(buf.get("frame_bytes") or len(pcm)),
            stream_ms=(time.perf_counter() - float(buf.get("started_at", time.perf_counter())))
            * 1000,
            voice_mode=normalize_mode(buf.get("voice_mode") or DEFAULT_MODE),
        )

    @staticmethod
    async def _cancel(turn: asyncio.Task | None) -> None:
        if turn and not turn.done():
            turn.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await turn
        return None

    async def _run_turn(self, ws, device_id: str, channel: str, conn: dict, msg) -> None:  # noqa: ANN001
        """Mark the device busy for the turn so notifications hold until the gap; the
        proactive tick flushes them once it's idle again."""
        self._busy.add(device_id)
        try:
            await self._do_turn(ws, device_id, channel, conn, msg)
        finally:
            self._busy.discard(device_id)

    @staticmethod
    def _reset_voice_conversation(channel: str, conn: dict) -> None:
        if channel != "voice":
            return
        conn["asserted"] = conn.get("base_asserted", "")
        conn["voice_mode"] = DEFAULT_MODE

    @staticmethod
    def _alarm_ack_reply_end(turn_id: str, channel: str, conn: dict) -> ReplyEnd:
        active_mode = conn.get("voice_mode") if channel == "voice" else DEFAULT_MODE
        transition = alarm_ack_transition(active_mode)
        return ReplyEnd(
            turn_id=turn_id,
            ended=transition.ended,
            continue_listening=transition.continue_listening,
            voice_mode=transition.mode,
            close_reason=transition.reason,
        )

    @staticmethod
    def _empty_transcript_reply_end(turn_id: str, channel: str) -> ReplyEnd:
        transition = empty_transcript_transition(channel)
        return ReplyEnd(
            turn_id=turn_id,
            ended=transition.ended,
            continue_listening=transition.continue_listening,
            voice_mode=transition.mode,
            close_reason=transition.reason,
        )

    @staticmethod
    def _apply_turn_result(channel: str, conn: dict, result: TurnResult) -> None:
        transition = voice_result_transition(
            ended=result.ended,
            voice_mode=result.voice_mode,
            continue_listening=result.continue_listening,
            close_reason=result.close_reason,
        )
        conn["voice_mode"] = transition.mode
        if transition.reset_conversation:
            BrainServer._reset_voice_conversation(channel, conn)

    @staticmethod
    def _apply_cancelled_turn_result(channel: str, conn: dict, result: TurnResult) -> None:
        transition = cancelled_voice_transition(
            voice_mode=result.voice_mode,
            close_reason=result.close_reason,
        )
        if transition is None:
            return
        conn["voice_mode"] = transition.mode
        if transition.reset_conversation:
            BrainServer._reset_voice_conversation(channel, conn)

    async def _do_turn(self, ws, device_id: str, channel: str, conn: dict, msg) -> None:  # noqa: ANN001
        turn_id = msg.turn_id
        trace = self._tracer.turn(
            room=self._cfg.gateway.room,
            speaker=conn.get("asserted") or conn.get("device_default", HOUSE),
            channel=channel,
            device_id=device_id,
        )
        trace.set(audio_downlink=REPLY_AUDIO_BINARY_V1)
        if isinstance(msg, BufferedAudioTurn):
            if channel == "voice":
                conn["voice_mode"] = msg.voice_mode
            pcm = msg.pcm
            secs = len(pcm) / 2 / msg.sample_rate
            trace.stage(
                "uplink",
                msg.stream_ms,
                protocol=UPLINK_AUDIO_BINARY_V1,
                pcm_bytes=len(pcm),
                frame_bytes=msg.frame_bytes,
                chunks=msg.chunks,
                audio_s=round(secs, 1),
            )
            trace.start("stt")
            text = await asyncio.to_thread(
                self._stt.transcribe, pcm, sample_rate=msg.sample_rate
            )
            trace.end("stt", audio_s=round(secs, 1), chars=len(text))
        else:  # TextIn
            text = msg.text
        if not text:
            end = self._empty_transcript_reply_end(turn_id, channel)
            await ws.send(encode(end))
            if end.ended:
                self._reset_voice_conversation(channel, conn)
            self._tracer.emit(trace)
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
            sent_audio_chunks = 0
            if not text_only:
                sent_audio_chunks, _sent_audio_bytes = await self._synthesize_and_send_text(
                    ws, turn_id, reply, trace, phase="alarm_reply"
                )
                if sent_audio_chunks == 0:
                    trace.event("reply_audio_missing", reply_chars=len(reply))
            end = self._alarm_ack_reply_end(turn_id, channel, conn)
            with contextlib.suppress(Exception):
                await ws.send(encode(ReplyText(turn_id=turn_id, text=reply)))
                await ws.send(encode(end))
            conn["voice_mode"] = end.voice_mode
            if end.ended:
                self._reset_voice_conversation(channel, conn)
            self._tracer.emit(trace)
            return
        # Resolve WHO this utterance is from (claim detection needs the transcript),
        # then route to that principal's session. A spoken claim sticks for the rest
        # of the conversation.
        ctx = self._resolve(
            device_id,
            channel,
            conn["asserted"],
            text,
            conn["device_default"],
            conn.get("hardware", set()),
        )
        if ctx.confidence == "claimed" and ctx.identity != HOUSE:
            conn["asserted"] = ctx.identity
        session = self._contexts.get(ctx)
        session.set_voice_mode(conn.get("voice_mode", "default"))
        trace.set(
            speaker=ctx.identity,
            channel=ctx.channel,
            device_id=ctx.device_id,
            scope=ctx.scope,
            confidence=ctx.confidence,
        )
        result = TurnResult()
        sent_audio_chunks = 0
        sent_audio_bytes = 0
        try:
            if text_only:  # text console / scripted test — reply text only, no TTS
                await session.respond_text(text, trace, result)
            else:
                async for pcm in session.respond(text, trace, result):
                    try:
                        await self._send_reply_audio(ws, turn_id, pcm)
                        sent_audio_chunks += 1
                        sent_audio_bytes += len(pcm)
                    except Exception as exc:  # noqa: BLE001 - close the turn cleanly below
                        trace.event("reply_audio_error", error=type(exc).__name__, phase="downlink")
                        print(f"  [tts] reply audio send failed: {exc!r}")
                        break
        except asyncio.CancelledError:
            session.finalize(text, result, trace)  # remember what was actually said
            self._apply_cancelled_turn_result(channel, conn, result)
            self._tracer.emit(trace)
            raise
        except Exception as exc:  # noqa: BLE001 - keep the intercom from hanging
            if not result.raw:
                trace.event("turn_error", error=type(exc).__name__, recovered=True)
                print(f"  [turn error] {exc!r}")
                result.raw = _TURN_ERROR_REPLY
                if not text_only:
                    fallback_chunks, fallback_bytes = await self._synthesize_and_send_text(
                        ws, turn_id, result.raw, trace, phase="fallback_reply"
                    )
                    sent_audio_chunks += fallback_chunks
                    sent_audio_bytes += fallback_bytes
            else:
                trace.event("reply_audio_error", error=type(exc).__name__, phase="synthesis")
                print(f"  [tts] reply audio failed: {exc!r}")
        session.finalize(text, result, trace)
        if not text_only and result.reply and sent_audio_chunks == 0:
            trace.event("reply_audio_missing", reply_chars=len(result.reply))
            print(
                "  [tts] reply completed without audio frames "
                f"(turn={turn_id}, chars={len(result.reply)})"
            )
        if not text_only:
            trace.set(reply_audio_chunks=sent_audio_chunks, reply_audio_bytes=sent_audio_bytes)
        self._tracer.emit(trace)
        with contextlib.suppress(Exception):
            await ws.send(encode(ReplyText(turn_id=turn_id, text=result.reply)))
            await ws.send(
                encode(
                    ReplyEnd(
                        turn_id=turn_id,
                        ended=result.ended,
                        continue_listening=result.continue_listening,
                        voice_mode=result.voice_mode,
                        close_reason=result.close_reason,
                    )
                )
            )
        self._apply_turn_result(channel, conn, result)

    @staticmethod
    async def _send_reply_audio(ws, turn_id: str, pcm: bytes) -> None:  # noqa: ANN001
        await ws.send(encode_reply_audio_binary(turn_id, pcm))

    async def _synthesize_and_send_text(
        self,
        ws,
        turn_id: str,
        text: str,
        trace,
        *,
        phase: str,
    ) -> tuple[int, int]:  # noqa: ANN001
        chunks = 0
        n_bytes = 0
        try:
            async for pcm in self._tts.synthesize_stream(text):
                await self._send_reply_audio(ws, turn_id, pcm)
                chunks += 1
                n_bytes += len(pcm)
        except Exception as exc:  # noqa: BLE001 - caller still sends ReplyText/ReplyEnd
            trace.event("reply_audio_error", error=type(exc).__name__, phase=phase)
            print(f"  [tts] {phase} audio failed: {exc!r}")
        return chunks, n_bytes

    async def _request_device_action(
        self, ctx: RequestContext, action: str, args: dict, timeout_s: float
    ) -> dict:
        conns = list(self._device_conns.get(ctx.device_id, set()))
        if not conns:
            raise RuntimeError(f"device {ctx.device_id!r} is not connected")
        ws = conns[0]
        meta = self._conn_meta.get(ws)
        if meta is None:
            raise RuntimeError(f"device {ctx.device_id!r} has no active control channel")
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        meta["waiters"][request_id] = fut
        try:
            await ws.send(encode(DeviceRequest(request_id=request_id, action=action, args=args)))
            resp = await asyncio.wait_for(fut, timeout=max(1.0, timeout_s))
        except Exception:
            meta["waiters"].pop(request_id, None)
            raise
        if not resp.ok:
            raise RuntimeError(resp.error or f"{action} failed")
        return dict(resp.result)


async def serve(cfg: Config) -> None:
    await BrainServer(cfg).serve()
