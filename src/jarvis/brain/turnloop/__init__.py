"""Turn loop — single-process orchestration of the voice state machine (spec §5).

  PASSIVE  -> (wake "Jarvis")     -> ACTIVE
  ACTIVE   -> (endpoint silence)  -> THINKING
  THINKING -> (first token)       -> SPEAKING
  SPEAKING -> (reply done)        -> PASSIVE
  SPEAKING -> (user speaks)       -> INTERRUPTED -> ACTIVE   (Step 7)

This owns the *edge* — the always-open mic, wake word, VAD, audio playback —
plus STT and the state machine. The think/speak core (prompt, tools, TTS,
end-detection, memory) lives in BrainSession (brain/session.py), reused
unchanged by the WebSocket brain server (Phase 3 W4). This is the in-process
path (`jarvis run`): capture an utterance, transcribe it, hand the text to the
session, and play the reply with hard-stop barge-in.
"""

from __future__ import annotations

import asyncio
import enum
import queue
import threading

from jarvis.brain.capabilities import context_for_resolution
from jarvis.brain.context import RequestContext
from jarvis.brain.contexts import ContextStore
from jarvis.brain.gateway_client import GatewayClient
from jarvis.brain.identity import HOUSE, IdentityResolver, load_users
from jarvis.brain.memory_client import MemoryClient
from jarvis.brain.session import BrainSession, TurnResult
from jarvis.brain.skills import register_skills
from jarvis.brain.tracing import Tracer
from jarvis.config import Config
from jarvis.intercom.audio import AudioIO, MicStream
from jarvis.intercom.vad import Endpointer, SileroVAD
from jarvis.intercom.wake import WakeWord
from jarvis.mcp import MCPBridge
from jarvis.services.stt import Transcriber
from jarvis.services.tts import InworldTTS
from jarvis.tools import build_registry
from jarvis.tools.mcp import make_mcp_tools
from jarvis.tools.selection import build_relevance

FRAME_SAMPLES = 512


class State(enum.Enum):
    PASSIVE = "passive_listening"
    ACTIVE = "active_listening"
    THINKING = "thinking"
    SPEAKING = "speaking"
    INTERRUPTED = "interrupted"


class TurnLoop:
    def __init__(
        self,
        cfg: Config,
        *,
        audio: AudioIO,
        stt: Transcriber,
        vad: SileroVAD,
        wake: WakeWord,
        gateway: GatewayClient,
        tts: InworldTTS,
        memory: MemoryClient,
        tracer: Tracer,
    ) -> None:
        self._cfg = cfg
        self._audio = audio
        self._stt = stt
        self._vad = vad
        self._wake = wake
        self._tts = tts  # kept for the wake acknowledgement ("speak" mode)
        self._tracer = tracer
        self._gateway = gateway  # kept so run() can aclose it
        self._sr = cfg.audio.sample_rate
        # Per-request identity/capability envelope (Phase 3 §4) — single-principal
        # in 3a. The think/speak core is shared with the brain server.
        self._registry = build_registry(
            cfg.tools, worker=cfg.worker, remote=cfg.remote, google=cfg.google,
            accounts=cfg.accounts, browser=cfg.browser, capabilities=cfg.capabilities,
            memory=memory,
        )
        users = load_users(cfg.capabilities.users_dir)
        # MCP servers connect at startup (off the hot path); OAuth servers connect
        # per principal (house + each user) so credentials isolate. See run().
        self._mcp = MCPBridge(cfg.mcp, principals=list(users))
        # Identity-aware like the brain server (§5): resolve the speaker per turn and
        # route to that principal's session (own history + memory peer). With no
        # users/ configured this stays house-only (single principal, base cache).
        self._memory = memory
        self._relevance = build_relevance(cfg, gateway)  # embedding scorer or None
        self._resolver = IdentityResolver(users)
        self._store = ContextStore(self._make_session)
        self._asserted = "" if cfg.capabilities.identity == "house" else cfg.capabilities.identity
        self.state = State.PASSIVE

    def _make_session(self, ctx: RequestContext) -> BrainSession:
        return BrainSession(
            self._cfg, ctx, gateway=self._gateway, tts=self._tts, memory=self._memory,
            tracer=self._tracer, registry=self._registry, memory_user=ctx.memory_peer,
            relevance=self._relevance,
        )

    def _resolve(self, utterance: str) -> RequestContext:
        resolution = self._resolver.resolve(
            device_id=self._cfg.capabilities.device_id,
            channel="voice",
            asserted=self._asserted,
            utterance=utterance,
        )
        return context_for_resolution(self._cfg.capabilities, resolution)

    # --- blocking frame loops (run via to_thread) --------------------------
    def _wait_for_wake(self, mic: MicStream) -> None:
        # If the wake loop falls behind (e.g. a CPU/thread spike from the
        # cold-path memory work just after a turn), the queue backs up and we'd
        # be detecting stale audio. Drop the backlog and reset so detection
        # always tracks live audio and stays responsive.
        backlog_frames = int(1.5 * self._sr / FRAME_SAMPLES)
        while True:
            if mic.qsize() > backlog_frames:
                mic.drain()
                self._wake.reset()
                continue
            frame = mic.read()
            if self._wake.process(frame):
                return

    def _capture_utterance(self, mic: MicStream, *, initial_wait_ms: float = 8000) -> bytes:
        frame_ms = FRAME_SAMPLES / self._sr * 1000.0
        self._vad.reset()
        ep = Endpointer(
            frame_ms=frame_ms,
            endpoint_silence_ms=self._cfg.vad.endpoint_silence_ms,
            speech_threshold=self._cfg.vad.speech_threshold,
            min_speech_ms=self._cfg.vad.min_speech_ms,
        )
        waited_ms = 0.0
        while True:
            frame = mic.read()
            done = ep.feed(frame, self._vad.prob(frame))
            if not ep.started:
                waited_ms += frame_ms
                if waited_ms >= initial_wait_ms:  # no speech within the window
                    return b""
            if done or len(ep.audio) / 2 / self._sr >= 30.0:
                return ep.audio

    # --- the loop ----------------------------------------------------------
    async def run(self) -> None:
        print("Loading models…")
        self._stt.load()
        self._vad.load()
        self._wake.load()
        await self._connect_mcp()
        try:
            with MicStream(
                self._cfg.audio, sample_rate=self._sr, frame_samples=FRAME_SAMPLES
            ) as mic:
                phrase = self._cfg.wake.keyword.replace("_", " ").title()
                print(f'\nJarvis is listening. Say "{phrase}".')
                while True:
                    await self._one_turn(mic)
        finally:
            self._wake.delete()
            await self._mcp.aclose()
            await self._gateway.aclose()

    async def _connect_mcp(self) -> None:
        """Connect configured MCP servers and register their tools (best-effort —
        a failed server is skipped, never fatal). No-op when MCP is disabled."""
        await self._mcp.start()
        for tool in make_mcp_tools(self._mcp):
            self._registry.register(tool)
        register_skills(self._registry, gateway=self._gateway, cfg=self._cfg)

    async def _one_turn(self, mic: MicStream) -> None:
        # PASSIVE → ACTIVE
        self.state = State.PASSIVE
        mic.drain()
        self._wake.reset()
        print('● idle — say "Hey Jarvis"')
        await asyncio.to_thread(self._wait_for_wake, mic)
        self.state = State.ACTIVE
        print("● wake")
        await self._acknowledge()
        mic.drain()  # discard the ack's own audio before listening
        print("  listening…")
        pcm = await asyncio.to_thread(self._capture_utterance, mic)

        # Conversation continues as long as the user barges in (spec §5): a
        # barge-in re-enters ACTIVE directly, NOT PASSIVE.
        while True:
            if not pcm:
                print("  (nothing said)")
                return
            trace = self._tracer.turn(
                room=self._cfg.gateway.room,
                speaker=self._asserted or "house",
                channel="voice",
                device_id=self._cfg.capabilities.device_id,
            )
            secs = len(pcm) / 2 / self._sr
            trace.start("stt")
            text = await asyncio.to_thread(self._stt.transcribe, pcm, sample_rate=self._sr)
            trace.end("stt", audio_s=round(secs, 1), chars=len(text))
            print(f"  you: {text!r}")
            if not text:
                return

            # Resolve WHO is speaking from this utterance (§5) and route to that
            # principal's session; a spoken claim ("it's Jules") sticks for the rest
            # of the conversation.
            ctx = self._resolve(text)
            if ctx.confidence == "claimed" and ctx.identity != HOUSE:
                self._asserted = ctx.identity
            trace.set(
                speaker=ctx.identity,
                channel=ctx.channel,
                device_id=ctx.device_id,
                scope=ctx.scope,
                confidence=ctx.confidence,
            )
            session = self._store.get(ctx)

            # THINKING/SPEAKING — the session does the work (hot path reads the
            # LOCAL memory cache only); _speak_with_bargein plays the PCM and
            # watches for a barge-in, cancelling the in-flight generation.
            self.state = State.THINKING
            result = TurnResult()
            interrupted = await self._speak_with_bargein(
                mic, session.respond(text, trace, result)
            )
            # finalize() runs even after a barge-in: result.raw is what was said.
            session.finalize(text, result)
            reply, ended = result.reply, result.ended
            print(f"  jarvis: {reply}{'  ⏹' if ended else ''}")
            if interrupted:
                trace.event("barge_in")
            self._tracer.emit(trace)

            if interrupted:
                # INTERRUPTED → re-listen immediately (ACTIVE), no wake word.
                print("⊘ interrupted — listening…")
                self.state = State.ACTIVE
                pcm = await asyncio.to_thread(self._capture_utterance, mic)
                continue
            if not reply:
                if ended:
                    print('  …(conversation closed — say "Hey Jarvis")')
                return

            # Normal completion. If the user signed off, close the conversation
            # and return to PASSIVE (wake word required again).
            if ended:
                print('  …(conversation closed — say "Hey Jarvis")')
                return
            # Conversation mode: keep listening briefly so the user can continue
            # without the wake word. Silence past the window drops to PASSIVE.
            if not self._cfg.vad.conversation_mode:
                return
            self.state = State.ACTIVE
            mic.drain()  # discard Jarvis's own reply tail before listening
            print("  …(listening — keep talking, or stay quiet to sleep)")
            pcm = await asyncio.to_thread(
                self._capture_utterance,
                mic,
                initial_wait_ms=self._cfg.vad.conversation_timeout_ms,
            )
            if not pcm:
                return  # conversation went idle → PASSIVE (wake word required)

    async def _acknowledge(self) -> None:
        """Confirm the wake word was heard before listening (configurable)."""
        mode = self._cfg.audio.ack_mode
        if mode == "none":
            return
        if mode == "speak":
            await self._audio.play_stream(
                self._tts.synthesize_stream(self._cfg.audio.ack_phrase),
                sample_rate=self._cfg.tts.sample_rate,
            )
        else:  # "beep"
            await asyncio.to_thread(self._audio.play_tone)

    async def _speak_with_bargein(self, mic: MicStream, pcm_source) -> bool:  # noqa: ANN001
        """Play a PCM source (a single audio stream, possibly spanning several
        streamed sentences) while watching the mic for the user talking over it.

        Returns True if the user barged in (playback was cut and the in-flight
        TTS + LLM stream cancelled), False on a clean finish. AEC is assumed in
        hardware (spec §2); a sustained-speech + grace-window guard reduces
        self-triggering without building software AEC.
        """
        self.state = State.SPEAKING
        if not self._cfg.vad.bargein_enabled:
            # No AEC input path: just speak, don't listen for interruptions.
            await self._audio.play_stream(pcm_source, sample_rate=self._cfg.tts.sample_rate)
            return False
        mic.drain()  # don't react to pre-speech frames
        wakeword_mode = self._cfg.vad.bargein_mode == "wakeword"
        # Reset the detector's rolling buffer so it doesn't re-fire on the
        # "Hey Jarvis" that started this turn (it was frozen in the buffer while
        # we captured/transcribed/thought, since we don't feed it then).
        self._wake.reset()
        self._vad.reset()
        frame_ms = FRAME_SAMPLES / self._sr * 1000.0
        stop_monitor = threading.Event()
        interrupted = threading.Event()

        def monitor_wakeword() -> None:
            # Interrupt only on the wake word — Jarvis's own voice never says it,
            # so this won't self-trigger even on speakers without AEC.
            elapsed_ms = 0.0
            while not stop_monitor.is_set():
                try:
                    frame = mic.read(timeout=0.1)
                except queue.Empty:
                    continue
                elapsed_ms += frame_ms
                # Grace: let the just-reset detector refill with fresh audio.
                if elapsed_ms < self._cfg.vad.bargein_grace_ms:
                    self._wake.process(frame)  # warm the buffer, ignore result
                    continue
                if self._wake.process(frame):
                    interrupted.set()
                    return

        def monitor_vad() -> None:
            elapsed_ms = 0.0
            speech_ms = 0.0
            while not stop_monitor.is_set():
                try:
                    frame = mic.read(timeout=0.1)
                except queue.Empty:
                    continue
                elapsed_ms += frame_ms
                if elapsed_ms < self._cfg.vad.bargein_grace_ms:
                    continue  # skip the playback onset window
                if self._vad.prob(frame) >= self._cfg.vad.bargein_threshold:
                    speech_ms += frame_ms
                    if speech_ms >= self._cfg.vad.bargein_min_ms:
                        interrupted.set()
                        return
                else:
                    speech_ms = 0.0

        monitor = monitor_wakeword if wakeword_mode else monitor_vad

        play_task = asyncio.create_task(
            self._audio.play_stream(pcm_source, sample_rate=self._cfg.tts.sample_rate)
        )
        mon_task = asyncio.create_task(asyncio.to_thread(monitor))
        try:
            while not play_task.done():
                if interrupted.is_set():
                    self._audio.stop_playback()  # cut audio + cancel TTS request
                    break
                await asyncio.sleep(0.02)
        finally:
            stop_monitor.set()
            results = await asyncio.gather(play_task, mon_task, return_exceptions=True)
            # Surface a real failure in the reply generator (LLM/tool/TTS) instead
            # of swallowing it into a silent empty turn.
            for r in results:
                if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                    print(f"  [turn error] {r!r}")
        return interrupted.is_set()
