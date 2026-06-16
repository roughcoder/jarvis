"""Intercom client (Phase 3 W4) — the thin edge.

Wake + VAD + endpoint capture locally; stream the utterance PCM to the brain;
play the reply PCM with hard-stop barge-in. Holds NO provider credentials —
authenticates to the brain with a pairing token only. Reuses the always-open
mic, wake word, VAD/Endpointer, and the streaming player from the intercom tier;
the think/speak work happens on the brain (BrainSession).

Edge capture/barge-in logic is intentionally parallel to the single-process
TurnLoop (we shared the BrainSession, not the edge). A future refactor could
factor the shared edge out; for 3a the small duplication keeps the loop stable.
"""

from __future__ import annotations

import asyncio
import queue
import threading
import uuid

import websockets

from jarvis.config import Config
from jarvis.intercom.audio import AudioIO, MicStream
from jarvis.intercom.vad import Endpointer, SileroVAD
from jarvis.intercom.wake import WakeWord
from jarvis.protocol.messages import (
    BargeIn,
    Cancel,
    Hello,
    ReplyAudio,
    ReplyEnd,
    ReplyText,
    Transcript,
    Utterance,
    Welcome,
    decode,
    encode,
)

FRAME_SAMPLES = 512


class IntercomClient:
    def __init__(
        self, cfg: Config, *, audio: AudioIO, vad: SileroVAD, wake: WakeWord
    ) -> None:
        self._cfg = cfg
        self._audio = audio
        self._vad = vad
        self._wake = wake
        self._sr = cfg.audio.sample_rate
        self._device_id = cfg.capabilities.device_id

    async def run(self) -> None:
        print("Loading models…")
        self._vad.load()
        self._wake.load()
        url = self._cfg.intercom.brain_url
        print(f"Connecting to brain at {url}…")
        async with websockets.connect(url) as ws:
            await ws.send(
                encode(
                    Hello(
                        device_id=self._device_id,
                        token=self._cfg.intercom.token.get_secret_value(),
                    )
                )
            )
            welcome = decode(await ws.recv())
            if not isinstance(welcome, Welcome):
                print(f"pairing rejected: {welcome}")
                return
            print(f"Paired with brain. Capabilities: {welcome.capabilities}")
            try:
                with MicStream(
                    self._cfg.audio, sample_rate=self._sr, frame_samples=FRAME_SAMPLES
                ) as mic:
                    phrase = self._cfg.wake.keyword.replace("_", " ").title()
                    print(f'\nJarvis is listening. Say "{phrase}".')
                    while True:
                        await self._one_turn(ws, mic)
            finally:
                self._wake.delete()

    async def _one_turn(self, ws, mic: MicStream) -> None:  # noqa: ANN001
        mic.drain()
        self._wake.reset()
        print('● idle — say "Hey Jarvis"')
        await asyncio.to_thread(self._wait_for_wake, mic)
        print("● wake")
        await self._acknowledge()
        mic.drain()
        print("  listening…")
        pcm = await asyncio.to_thread(self._capture_utterance, mic)

        while True:
            if not pcm:
                print("  (nothing said)")
                return
            turn_id = uuid.uuid4().hex
            await ws.send(encode(Utterance.of(turn_id, self._sr, pcm)))
            state = {"ended": False, "text": ""}
            interrupted = await self._play_reply(ws, mic, turn_id, state)
            print(f"  jarvis: {state['text']}{'  ⏹' if state['ended'] else ''}")

            if interrupted:
                print("⊘ interrupted — listening…")
                pcm = await asyncio.to_thread(self._capture_utterance, mic)
                continue
            if state["ended"]:
                print('  …(conversation closed — say "Hey Jarvis")')
                return
            if not self._cfg.vad.conversation_mode:
                return
            mic.drain()
            print("  …(listening — keep talking, or stay quiet to sleep)")
            pcm = await asyncio.to_thread(
                self._capture_utterance,
                mic,
                initial_wait_ms=self._cfg.vad.conversation_timeout_ms,
            )
            if not pcm:
                return

    # --- reply playback + barge-in -----------------------------------------
    async def _reply_audio(self, ws, turn_id, state):  # noqa: ANN001
        """Yield reply PCM from the brain until ReplyEnd; record text/ended."""
        async for raw in ws:
            try:
                msg = decode(raw)
            except Exception:
                continue
            if isinstance(msg, Transcript) and msg.turn_id == turn_id:
                print(f"  you: {msg.text!r}")
            elif isinstance(msg, ReplyAudio) and msg.turn_id == turn_id:
                yield msg.pcm()
            elif isinstance(msg, ReplyText) and msg.turn_id == turn_id:
                state["text"] = msg.text
            elif isinstance(msg, ReplyEnd) and msg.turn_id == turn_id:
                state["ended"] = msg.ended
                return
            elif isinstance(msg, Cancel) and msg.turn_id == turn_id:
                return

    async def _play_reply(self, ws, mic: MicStream, turn_id: str, state: dict) -> bool:  # noqa: ANN001
        if not self._cfg.vad.bargein_enabled:
            await self._audio.play_stream(
                self._reply_audio(ws, turn_id, state), sample_rate=self._cfg.tts.sample_rate
            )
            return False
        mic.drain()
        self._wake.reset()
        self._vad.reset()
        frame_ms = FRAME_SAMPLES / self._sr * 1000.0
        wakeword_mode = self._cfg.vad.bargein_mode == "wakeword"
        stop_monitor = threading.Event()
        interrupted = threading.Event()

        def monitor_wakeword() -> None:
            elapsed_ms = 0.0
            while not stop_monitor.is_set():
                try:
                    frame = mic.read(timeout=0.1)
                except queue.Empty:
                    continue
                elapsed_ms += frame_ms
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
                    continue
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
                self._reply_audio(ws, turn_id, state), sample_rate=self._cfg.tts.sample_rate
            )
        )
        mon_task = asyncio.create_task(asyncio.to_thread(monitor))
        try:
            while not play_task.done():
                if interrupted.is_set():
                    self._audio.stop_playback()  # cut local audio immediately
                    await ws.send(encode(BargeIn(turn_id=turn_id)))  # cancel brain-side
                    break
                await asyncio.sleep(0.02)
        finally:
            stop_monitor.set()
            results = await asyncio.gather(play_task, mon_task, return_exceptions=True)
            for r in results:
                if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError):
                    print(f"  [reply error] {r!r}")
        return interrupted.is_set()

    # --- local capture (parallel to TurnLoop) ------------------------------
    def _wait_for_wake(self, mic: MicStream) -> None:
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
                if waited_ms >= initial_wait_ms:
                    return b""
            if done or len(ep.audio) / 2 / self._sr >= 30.0:
                return ep.audio

    async def _acknowledge(self) -> None:
        # Local wake confirmation only — the intercom has no TTS, so "speak" mode
        # degrades to a beep here.
        if self._cfg.audio.ack_mode == "none":
            return
        await asyncio.to_thread(self._audio.play_tone)
