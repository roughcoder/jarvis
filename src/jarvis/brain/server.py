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

import websockets

from jarvis.brain.capabilities import build_request_context
from jarvis.brain.gateway_client import GatewayClient
from jarvis.brain.memory_client import MemoryClient
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.brain.tracing import Tracer
from jarvis.config import Config
from jarvis.protocol.messages import (
    BargeIn,
    Cancel,
    Hello,
    Reject,
    ReplyAudio,
    ReplyEnd,
    ReplyText,
    TextIn,
    Utterance,
    Welcome,
    decode,
    encode,
)
from jarvis.services.stt import Transcriber
from jarvis.services.tts import InworldTTS
from jarvis.tools import build_registry


class BrainServer:
    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._stt = Transcriber(cfg.stt)
        self._tracer = Tracer(cfg.trace)
        self._registry = build_registry(cfg.tools, worker=cfg.worker)
        # The gateway/tts/memory clients are effectively stateless; share one set
        # across connections. Per-connection state (history/ctx) lives in the
        # BrainSession.
        self._gateway = GatewayClient(cfg.gateway)
        self._tts = InworldTTS(cfg.tts)
        self._memory = MemoryClient(cfg.memory)

    async def serve(self) -> None:
        print("Loading STT model…")
        self._stt.load()
        host, port = self._cfg.brain.host, self._cfg.brain.port
        async with websockets.serve(self._handle, host, port):
            print(f"Brain listening on ws://{host}:{port}")
            await asyncio.Future()  # run forever

    # --- pairing -----------------------------------------------------------
    def _authorised(self, hello: Hello) -> bool:
        token = self._cfg.brain.pairing_token.get_secret_value()
        return (not token) or hello.token == token

    def _context_for(self, hello: Hello):
        # Build the connection's RequestContext from the paired device's profile.
        caps_cfg = self._cfg.capabilities.model_copy(update={"device_id": hello.device_id})
        return build_request_context(caps_cfg)

    async def _handle(self, ws) -> None:  # noqa: ANN001
        try:
            first = decode(await ws.recv())
        except Exception:
            return
        if not isinstance(first, Hello) or not self._authorised(first):
            with contextlib.suppress(Exception):
                await ws.send(encode(Reject(reason="unauthorized")))
            return
        ctx = self._context_for(first)
        session = BrainSession(
            self._cfg,
            ctx,
            gateway=self._gateway,
            tts=self._tts,
            memory=self._memory,
            tracer=self._tracer,
            registry=self._registry,
        )
        session.load_soul()
        await ws.send(
            encode(
                Welcome(
                    identity=ctx.identity,
                    scope=ctx.scope,
                    capabilities=sorted(ctx.capabilities),
                )
            )
        )
        print(f"intercom paired: device={first.device_id} caps={sorted(ctx.capabilities)}")

        turn: asyncio.Task | None = None
        try:
            async for raw in ws:
                try:
                    msg = decode(raw)
                except Exception:
                    continue
                if isinstance(msg, (Utterance, TextIn)):
                    turn = await self._cancel(turn)
                    turn = asyncio.create_task(self._run_turn(ws, session, msg))
                elif isinstance(msg, BargeIn):
                    turn = await self._cancel(turn)
                    with contextlib.suppress(Exception):
                        await ws.send(encode(Cancel(turn_id=msg.turn_id)))
        finally:
            await self._cancel(turn)

    @staticmethod
    async def _cancel(turn: asyncio.Task | None) -> None:
        if turn and not turn.done():
            turn.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await turn
        return None

    async def _run_turn(self, ws, session: BrainSession, msg) -> None:  # noqa: ANN001
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
        trace = self._tracer.turn(
            room=self._cfg.gateway.room, speaker=self._cfg.gateway.speaker
        )
        result = TurnResult()
        try:
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
