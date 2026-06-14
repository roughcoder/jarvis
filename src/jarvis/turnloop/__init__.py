"""Turn loop — the five-state voice machine (spec §5).

  PASSIVE  -> (wake "Jarvis")     -> ACTIVE
  ACTIVE   -> (endpoint silence)  -> THINKING
  THINKING -> (first token)       -> SPEAKING
  SPEAKING -> (reply done)        -> PASSIVE
  SPEAKING -> (user speaks)       -> INTERRUPTED -> ACTIVE   (Step 7)

ONE always-open mic (MicStream) feeds whichever consumer the active state
needs: Porcupine in PASSIVE, Silero+Endpointer in ACTIVE. Both want 512-sample
16kHz frames, so the same stream serves both.

The loop runs in a single asyncio event loop (so the async gateway/TTS clients
stay on one loop); the blocking frame loops (wake wait, endpoint capture, STT)
run via asyncio.to_thread.

Step 6 implements PASSIVE→ACTIVE→THINKING→SPEAKING→PASSIVE. Barge-in (INTERRUPTED)
arrives in Step 7; memory (hot/cold) in Step 9.
"""

from __future__ import annotations

import asyncio
import enum
import queue
import threading

from jarvis.audio import AudioIO, MicStream
from jarvis.config import Config
from jarvis.gateway_client import GatewayClient
from jarvis.memory_client import MemoryClient
from jarvis.stt import Transcriber
from jarvis.tts import InworldTTS
from jarvis.vad import Endpointer, SileroVAD
from jarvis.wake import WakeWord

FRAME_SAMPLES = 512

_VOICE_SYSTEM_PROMPT = (
    "You are Jarvis, a concise spoken voice assistant. Answer in one or two "
    "short sentences meant to be read aloud. Use plain text only — no markdown, "
    "lists, code blocks, or emoji."
)


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
    ) -> None:
        self._cfg = cfg
        self._audio = audio
        self._stt = stt
        self._vad = vad
        self._wake = wake
        self._gateway = gateway
        self._tts = tts
        self._memory = memory
        self._sr = cfg.audio.sample_rate
        self._cold_tasks: set[asyncio.Task] = set()
        self.state = State.PASSIVE

    # --- blocking frame loops (run via to_thread) --------------------------
    def _wait_for_wake(self, mic: MicStream) -> None:
        while True:
            frame = mic.read()
            if self._wake.process(frame):
                return

    def _capture_utterance(self, mic: MicStream) -> bytes:
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
                if waited_ms >= 8000:  # gave the wake word but said nothing
                    return b""
            if done or len(ep.audio) / 2 / self._sr >= 30.0:
                return ep.audio

    # --- the loop ----------------------------------------------------------
    async def run(self) -> None:
        print("Loading models…")
        self._stt.load()
        self._vad.load()
        self._wake.load()
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
            await self._gateway.aclose()

    async def _one_turn(self, mic: MicStream) -> None:
        # PASSIVE → ACTIVE
        self.state = State.PASSIVE
        mic.drain()
        self._wake.reset()
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
            text = await asyncio.to_thread(
                self._stt.transcribe, pcm, sample_rate=self._sr
            )
            print(f"  you: {text!r}")
            if not text:
                return

            # THINKING — hot path: inject the LOCAL cached representation only
            # (a fast file read), never a live memory reasoning call (spec §3.2).
            self.state = State.THINKING
            model = (
                self._cfg.gateway.strong_model
                if len(text) > 120
                else self._cfg.gateway.fast_model
            )
            system = _VOICE_SYSTEM_PROMPT
            memory = self._memory.read_cached_representation()
            if memory:
                system += (
                    "\n\nWhat you already know about the user (use it naturally "
                    f"only if relevant; do not recite it):\n{memory}"
                )
            reply = await self._gateway.complete(
                [
                    {"role": "system", "content": system},
                    {"role": "user", "content": text},
                ],
                model=model,
            )
            print(f"  jarvis [{model}]: {reply}")
            if not reply:
                return

            # COLD path: fire-and-forget BEFORE speaking so the memory write +
            # background reasoning + cache refresh happen while Jarvis talks —
            # never blocking the hot path (spec §3.2).
            self._fire_cold_path(text, reply)

            # SPEAKING (barge-in armed)
            interrupted = await self._speak_with_bargein(mic, reply)
            if not interrupted:
                return  # normal completion → back to PASSIVE
            # INTERRUPTED → re-listen immediately (ACTIVE), no wake word
            print("⊘ interrupted — listening…")
            self.state = State.ACTIVE
            pcm = await asyncio.to_thread(self._capture_utterance, mic)

    def _fire_cold_path(self, user_text: str, assistant_text: str) -> None:
        """Detached background task — never awaited on the hot path."""
        task = asyncio.create_task(self._cold_path(user_text, assistant_text))
        self._cold_tasks.add(task)
        task.add_done_callback(self._cold_tasks.discard)

    async def _cold_path(self, user_text: str, assistant_text: str) -> None:
        # Write the turn to Honcho (deriver reasons in the background), then
        # refresh the local representation cache for the next turn. Resilient:
        # if memory is unreachable, the turn loop is unaffected.
        try:
            await self._memory.write_turn(user_text, assistant_text)
            await self._memory.refresh_cache()
        except Exception as exc:  # noqa: BLE001 - memory must never break a turn
            print(f"  [memory] cold-path skipped: {exc}")

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

    async def _speak_with_bargein(self, mic: MicStream, reply: str) -> bool:
        """Play the reply while watching the mic for the user talking over it.

        Returns True if the user barged in (playback was cut and the in-flight
        TTS request cancelled), False on a clean finish. AEC is assumed in
        hardware (spec §2); a sustained-speech + grace-window guard reduces
        self-triggering without building software AEC.
        """
        self.state = State.SPEAKING
        if not self._cfg.vad.bargein_enabled:
            # No AEC input path: just speak, don't listen for interruptions.
            await self._audio.play_stream(
                self._tts.synthesize_stream(reply),
                sample_rate=self._cfg.tts.sample_rate,
            )
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
            self._audio.play_stream(
                self._tts.synthesize_stream(reply),
                sample_rate=self._cfg.tts.sample_rate,
            )
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
            await asyncio.gather(play_task, mon_task, return_exceptions=True)
        return interrupted.is_set()
