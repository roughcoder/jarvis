"""Audio I/O — the microphone NEVER closes (spec §5).

One always-open input stream feeds whichever consumer the state machine has
active: Porcupine (PASSIVE), STT+VAD endpointing (ACTIVE), or VAD barge-in
(SPEAKING). Output is streaming playback that can be hard-stopped within
~100ms for barge-in (spec Step 2/7).

AEC is assumed in hardware (spec §2) — no software AEC here.

Playback uses PortAudio in CALLBACK mode: all stream operations happen on
PortAudio's own thread, so a barge-in (CallbackAbort) cuts audio immediately
without the cross-thread deadlock that blocking writes invite, while a normal
end (CallbackStop after the buffer drains) plays the tail cleanly.
"""

from __future__ import annotations

import asyncio
import queue
import threading
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from jarvis.config import AudioConfig


@dataclass
class PlaybackMetrics:
    """Timing captured by the local output path for one streamed playback."""

    sample_rate: int
    blocksize: int
    prebuffer_ms: float
    preroll_ms: float
    started_at: float = field(default_factory=time.perf_counter)
    chunks: int = 0
    bytes: int = 0
    underruns: int = 0
    first_feed_ms: float | None = None
    producer_done_ms: float | None = None
    ready_ms: float | None = None
    first_output_ms: float | None = None
    first_speech_ms: float | None = None
    finished_ms: float | None = None
    cut_latency_ms: float | None = None
    aborted: bool = False

    def _elapsed_ms(self) -> float:
        return (time.perf_counter() - self.started_at) * 1000

    def mark_feed(self, n_bytes: int) -> None:
        if self.first_feed_ms is None:
            self.first_feed_ms = self._elapsed_ms()
        self.chunks += 1
        self.bytes += n_bytes

    def mark_producer_done(self) -> None:
        if self.producer_done_ms is None:
            self.producer_done_ms = self._elapsed_ms()

    def mark_ready(self) -> None:
        if self.ready_ms is None:
            self.ready_ms = self._elapsed_ms()

    def mark_output(self, *, speech: bool) -> None:
        if self.first_output_ms is None:
            self.first_output_ms = self._elapsed_ms()
        if speech and self.first_speech_ms is None:
            self.first_speech_ms = self._elapsed_ms()

    def mark_finished(self) -> None:
        if self.finished_ms is None:
            self.finished_ms = self._elapsed_ms()

    def as_dict(self) -> dict:
        return {
            "ms": round(self.finished_ms or self._elapsed_ms(), 1),
            "sample_rate": self.sample_rate,
            "blocksize": self.blocksize,
            "block_ms": round(self.blocksize / self.sample_rate * 1000, 1),
            "prebuffer_ms": round(self.prebuffer_ms, 1),
            "preroll_ms": round(self.preroll_ms, 1),
            "chunks": self.chunks,
            "bytes": self.bytes,
            "underruns": self.underruns,
            "first_feed_ms": _round_ms(self.first_feed_ms),
            "producer_done_ms": _round_ms(self.producer_done_ms),
            "ready_ms": _round_ms(self.ready_ms),
            "first_output_ms": _round_ms(self.first_output_ms),
            "first_speech_ms": _round_ms(self.first_speech_ms),
            "cut_latency_ms": _round_ms(self.cut_latency_ms),
            "aborted": self.aborted,
        }


def _round_ms(value: float | None) -> float | None:
    return round(value, 1) if value is not None else None


class _StreamingPlayer:
    """Plays 16-bit mono PCM fed incrementally. Callback-driven.

    - feed(pcm): append bytes (called as TTS chunks arrive).
    - mark_producer_done(): no more audio is coming; once the buffer drains the
      callback raises CallbackStop and playback ends cleanly (no truncation).
    - stop(): barge-in. The callback raises CallbackAbort on its next tick,
      discarding buffered audio immediately (well under 100ms).
    """

    def __init__(
        self,
        sample_rate: int,
        output_device: int | None = None,
        *,
        blocksize: int = 2048,
        preroll_ms: int = 300,
        prebuffer_ms: int = 200,
    ) -> None:
        from collections import deque

        import numpy as np
        import sounddevice as sd

        self._sd = sd
        self._np = np
        self._lock = threading.Lock()
        # Audio is held as a deque of int16 arrays (O(1) appends, tiny lock
        # holds) plus a partial leftover view — NOT one growing array that
        # would need an O(n) copy under the realtime lock and starve playback.
        self._chunks: deque = deque()
        self._empty = np.empty(0, dtype=np.int16)
        self._leftover = self._empty
        self._leftover_pos = 0
        self._buffered = 0  # queued REAL (fed) samples
        # Two gates hold output silent before the first audible sample:
        #  - preroll: the callback outputs zeros for at least this long so the
        #    cold-start warmup (the device offers only ~20ms of hardware buffer)
        #    lands on silence, not the first words.
        #  - prebuffer: at least this much REAL audio must be queued, to absorb
        #    network jitter at speech onset. Only fed samples count — silence
        #    must not satisfy it, or the guard silently does nothing.
        # Both overlap the TTS time-to-first-audio, so they add ~nothing to felt
        # latency, and neither affects barge-in: a stop aborts the software
        # buffer and only the tiny hardware buffer plays out.
        self._preroll = int(sample_rate * max(0, preroll_ms) / 1000)
        self._prebuffer = int(sample_rate * max(0, prebuffer_ms) / 1000)
        self._warmed = 0  # silent samples emitted while waiting to go ready
        self._ready = False
        self._stop = threading.Event()
        self._producer_done = threading.Event()
        self._finished = threading.Event()
        self._stop_at: float | None = None
        self.cut_latency_ms: float | None = None  # audible barge-in cut latency
        self.metrics = PlaybackMetrics(
            sample_rate=sample_rate,
            blocksize=blocksize,
            prebuffer_ms=self._prebuffer / sample_rate * 1000,
            preroll_ms=self._preroll / sample_rate * 1000,
        )
        # Large block so each callback has ~85ms to run (the device only offers
        # ~20ms of hardware buffer, which starves a cold low-latency callback).
        # abort() still discards pending audio, so barge-in stays under 100ms.
        self._stream = sd.OutputStream(
            samplerate=sample_rate,
            channels=1,
            dtype="int16",
            device=output_device,
            blocksize=blocksize,
            callback=self._callback,
            finished_callback=self._on_finished,
        )

    def _on_finished(self) -> None:
        if self._stop_at is not None:
            self.cut_latency_ms = (time.perf_counter() - self._stop_at) * 1000
            self.metrics.cut_latency_ms = self.cut_latency_ms
        self.metrics.mark_finished()
        self._finished.set()

    def _callback(self, outdata, frames, time_info, status) -> None:  # noqa: ANN001
        if self._stop.is_set():
            self.metrics.aborted = True
            raise self._sd.CallbackAbort
        idx = 0
        done = False
        while idx < frames:
            with self._lock:
                if not self._ready:
                    # Hold output silent until BOTH the warmup window has passed
                    # and enough real audio is buffered (or the input ended).
                    warmed = self._warmed >= self._preroll
                    buffered = (
                        self._buffered >= self._prebuffer or self._producer_done.is_set()
                    )
                    if warmed and buffered:
                        self._ready = True
                        self.metrics.mark_ready()
                    else:
                        outdata[:, 0] = 0
                        self._warmed += frames
                        return
                if self._leftover_pos >= len(self._leftover):
                    if self._chunks:
                        self._leftover = self._chunks.popleft()
                        self._leftover_pos = 0
                    else:
                        done = self._producer_done.is_set()
                        break
                start = self._leftover_pos
                take = min(frames - idx, len(self._leftover) - start)
                chunk = self._leftover
                self._leftover_pos += take
                if self._leftover_pos >= len(self._leftover):
                    self._leftover = self._empty
                    self._leftover_pos = 0
                self._buffered -= take
            outdata[idx : idx + take, 0] = chunk[start : start + take]
            self.metrics.mark_output(speech=True)
            idx += take
        if idx < frames:
            outdata[idx:, 0] = 0  # pad underflow with silence
            if done:
                raise self._sd.CallbackStop  # drained -> clean finish
            if self._ready:
                self.metrics.underruns += 1

    def start(self) -> None:
        self._stream.start()

    def feed(self, pcm: bytes) -> None:
        if self._stop.is_set():
            return
        arr = self._np.frombuffer(pcm, dtype=self._np.int16)
        self.metrics.mark_feed(len(pcm))
        with self._lock:
            self._chunks.append(arr)
            self._buffered += len(arr)

    def mark_producer_done(self) -> None:
        self.metrics.mark_producer_done()
        self._producer_done.set()

    def wait(self) -> None:
        self._finished.wait()

    def stop(self) -> None:
        """Hard-stop: next callback aborts, dropping buffered audio. < 100ms."""
        self._stop_at = time.perf_counter()
        self._stop.set()

    def close(self) -> None:
        try:
            self._stream.close()
        except Exception:
            pass


class MicStream:
    """The single always-open microphone (spec §5).

    One input stream that never closes for the life of the session; the state
    machine reads fixed-size 512-sample frames and routes them to whichever
    consumer is active (Porcupine in PASSIVE, VAD in ACTIVE/SPEAKING).
    """

    def __init__(
        self, cfg: AudioConfig, *, sample_rate: int, frame_samples: int = 512
    ) -> None:
        self._cfg = cfg
        self._sample_rate = sample_rate
        self._frame_samples = frame_samples
        self._queue: queue.Queue[bytes] = queue.Queue()
        self._stream = None

    def __enter__(self) -> MicStream:
        import sounddevice as sd

        def callback(indata, _n, _t, _status) -> None:  # noqa: ANN001
            self._queue.put(bytes(indata))

        self._stream = sd.RawInputStream(
            samplerate=self._sample_rate,
            channels=1,
            dtype="int16",
            blocksize=self._frame_samples,
            device=self._cfg.input_device,
            callback=callback,
        )
        self._stream.start()
        return self

    def read(self, timeout: float | None = None) -> bytes:
        """Next 512-sample PCM frame. Blocks until one is available."""
        return self._queue.get(timeout=timeout)

    def qsize(self) -> int:
        """Number of buffered (unread) frames — a backlog indicator."""
        return self._queue.qsize()

    def drain(self) -> None:
        """Discard any buffered frames (e.g. Jarvis's own audio tail)."""
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                return

    def __exit__(self, *exc) -> None:  # noqa: ANN002
        try:
            self._stream.stop()
            self._stream.close()
        except Exception:
            pass


class AudioIO:
    def __init__(self, cfg: AudioConfig) -> None:
        self._cfg = cfg
        self._player: _StreamingPlayer | None = None
        self._feed_task: asyncio.Task | None = None
        self.last_cut_latency_ms: float | None = None  # audible barge-in cut

    def frames(self) -> AsyncIterator[bytes]:
        """Continuous stream of fixed-size PCM frames from the open mic."""
        raise NotImplementedError("Step 3/5")

    def record(self, stop_event: threading.Event, *, sample_rate: int) -> bytes:
        """Push-to-talk capture: record 16-bit mono PCM until stop_event is set.

        Step 3 opens/closes the mic per utterance; Steps 5/6 switch to a single
        always-open input stream (spec §5). Returns raw 16-bit PCM bytes.
        """
        import sounddevice as sd

        frames: list = []

        def callback(indata, _n, _t, _status) -> None:  # noqa: ANN001
            frames.append(bytes(indata))

        with sd.RawInputStream(
            samplerate=sample_rate,
            channels=1,
            dtype="int16",
            device=self._cfg.input_device,
            callback=callback,
        ):
            stop_event.wait()
        return b"".join(frames)

    def record_until_silence(
        self,
        vad,  # noqa: ANN001 - .reset(), .prob(frame_bytes)
        *,
        sample_rate: int,
        endpoint_silence_ms: int,
        speech_threshold: float,
        min_speech_ms: int = 200,
        max_initial_wait_s: float = 8.0,
        max_utterance_s: float = 30.0,
    ) -> bytes:
        """VAD endpointing capture (spec Step 5): record until the user stops.

        Opens its own short-lived mic stream (used by `chat`). The always-open
        state machine uses MicStream + jarvis.intercom.vad.Endpointer directly.
        """
        from jarvis.intercom.vad import Endpointer

        FRAME = 512
        frame_ms = FRAME / sample_rate * 1000.0
        vad.reset()
        ep = Endpointer(
            frame_ms=frame_ms,
            endpoint_silence_ms=endpoint_silence_ms,
            speech_threshold=speech_threshold,
            min_speech_ms=min_speech_ms,
        )
        waited_ms = 0.0
        with MicStream(self._cfg, sample_rate=sample_rate, frame_samples=FRAME) as mic:
            while True:
                frame = mic.read()
                done = ep.feed(frame, vad.prob(frame))
                if not ep.started:
                    waited_ms += frame_ms
                    if waited_ms >= max_initial_wait_s * 1000:
                        return b""
                if done or len(ep.audio) / 2 / sample_rate >= max_utterance_s:
                    break
        return ep.audio

    async def play_stream(
        self, pcm_chunks: AsyncIterator[bytes], *, sample_rate: int
    ) -> PlaybackMetrics:
        """Stream PCM to the speaker as chunks arrive (start before complete).

        The producer runs as a cancellable task so a barge-in (stop_playback)
        both aborts the audio AND cancels the in-flight TTS request (spec §7),
        unwinding the network generator cleanly.
        """
        player = _StreamingPlayer(
            sample_rate,
            self._cfg.output_device,
            blocksize=self._cfg.playback_blocksize,
            preroll_ms=self._cfg.playback_preroll_ms,
            prebuffer_ms=self._cfg.playback_prebuffer_ms,
        )
        player.start()
        self._player = player

        async def feed_loop() -> None:
            try:
                async for chunk in pcm_chunks:
                    if player._stop.is_set():
                        break
                    player.feed(chunk)
            finally:
                player.mark_producer_done()

        self._feed_task = asyncio.create_task(feed_loop())
        try:
            try:
                await self._feed_task
            except asyncio.CancelledError:
                pass  # barge-in cancelled the producer; audio already aborting
            await asyncio.to_thread(player.wait)
            self.last_cut_latency_ms = player.cut_latency_ms
            return player.metrics
        finally:
            player.close()
            self._feed_task = None
            if self._player is player:
                self._player = None

    def stop_playback(self) -> None:
        """Hard-stop playback immediately (barge-in). Target < 100ms.

        Aborts buffered audio AND cancels the in-flight TTS request.
        """
        if self._player is not None:
            self._player.stop()  # CallbackAbort on next tick -> audio gone fast
        if self._feed_task is not None and not self._feed_task.done():
            self._feed_task.cancel()  # kill the in-flight TTS network request

    def play_tone(self, *, sample_rate: int = 24000) -> None:
        """Play a short two-note earcon (wake acknowledgement). Blocking.

        A pre-rendered buffer handed to sounddevice in one shot (like afplay),
        so it stays clean despite the device's tiny hardware buffer.
        """
        import numpy as np
        import sounddevice as sd

        def note(freq: float, ms: int):  # noqa: ANN202
            t = np.linspace(0, ms / 1000, int(sample_rate * ms / 1000), False)
            tone = 0.25 * np.sin(2 * np.pi * freq * t)
            fade = max(1, int(sample_rate * 0.008))  # 8ms fades kill clicks
            env = np.ones_like(tone)
            env[:fade] = np.linspace(0, 1, fade)
            env[-fade:] = np.linspace(1, 0, fade)
            return tone * env

        buf = np.concatenate([note(660, 90), note(990, 110)])
        pcm = (buf * 32767).astype(np.int16)
        sd.play(pcm, samplerate=sample_rate, device=self._cfg.output_device)
        sd.wait()
