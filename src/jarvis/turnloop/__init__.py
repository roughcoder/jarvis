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
import re
import threading
import time
from collections.abc import AsyncIterator

from jarvis.audio import AudioIO, MicStream
from jarvis.config import Config
from jarvis.gateway_client import GatewayClient
from jarvis.memory_client import MemoryClient
from jarvis.stt import Transcriber
from jarvis.tracing import Tracer
from jarvis.tts import InworldTTS
from jarvis.vad import Endpointer, SileroVAD
from jarvis.wake import WakeWord

FRAME_SAMPLES = 512

# Technical format layer (always present). Personality comes from the soul
# (SOUL.md); what Jarvis knows about the user comes from memory.
#
# Base TTS hygiene + (optionally) Inworld TTS-2 expressive steering, per
# Inworld's prompting/steering guides: one steering instruction at the START
# only (scopes the whole line), non-verbals inline, tags consumed not spoken.
_VOICE_FORMAT_BASE = (
    "Write for the ear, not the page: one or two short spoken sentences. Use "
    "contractions and natural phrasing. Write numbers as words ('twenty-three', "
    "not '23'). Never use markdown, bullet points, headings, emoji, or special "
    "characters."
)
_VOICE_FORMAT_EXPRESSIVE = (
    _VOICE_FORMAT_BASE + "\n\n"
    "Let real feeling colour your delivery when the moment calls for it, using "
    "Inworld TTS-2 cues with a light touch:\n"
    "- Delivery/emotion: at most ONE instruction in [square brackets] at the "
    "very START of the reply — it sets mood, pace and tone for the whole line. "
    "Describe it concretely and match it to the words, e.g. [say warmly with a "
    "relaxed, conversational pace] or [say gently, a little concerned]. Never "
    "put a steering tag mid-sentence, never use more than one, never contradict "
    "the words.\n"
    "- Non-verbals may go INLINE where the sound happens: [laugh], [sigh], "
    "[breathe], [clear throat], [yawn].\n"
    "- For stress, capitalise a whole word ('I did NOT'), rarely.\n"
    "Most lines need no tags at all — reach for them only when feeling genuinely "
    "colours what you're saying."
)

# Conversation control: how the model signals the user is done so the loop can
# return to PASSIVE (wake word required again). Detected + stripped before TTS.
_END_INSTRUCTION = (
    "Ending the conversation: end when the user clearly signals they're finished "
    "— a goodbye ('bye', 'goodnight', 'see you'), declining further help ('no "
    "thanks', \"no, that's good, thanks\", \"I'm good\", 'we're good'), or "
    "'that's all'/'stop'/'go to sleep'. To end, give a short, warm farewell of a "
    "few words and NOTHING else, then put [[END]] as the very last characters. "
    "IMPORTANT: if your reply is itself a goodbye, you MUST include [[END]]. If a "
    "message is only a vague acknowledgement and you can't tell whether they're "
    "done (a bare 'thanks', 'ok', 'cool', 'great'), do NOT end — give your reply "
    "and briefly ASK if there's anything else. When unsure, ask rather than end."
)
# Matches [[END]] / [END] (case-insensitive). Stripped from the spoken reply.
_END_RE = re.compile(r"\s*\[\[?\s*end\s*\]\]?\s*", re.IGNORECASE)

# --- Deterministic backstops (the model handles nuance; these guarantee the
# clear cases and never fire on a turn the user meant to continue) -----------

# Any request/question word → never a sign-off.
_REQUEST_CUE = re.compile(
    r"\b(tell|what|whats|how|why|when|where|who|which|show|give|explain|"
    r"recommend|suggest|find|search|list|define|describe|help)\b"
)
# Short command / closer phrases (matched after stripping filler words).
_CLEAR_SIGNOFFS = frozenset(
    {
        "goodbye", "bye", "bye bye", "good night", "goodnight",
        "stop", "stop listening", "go to sleep", "go to bed", "go away",
        "dismissed", "that is all", "thats all", "that is it", "thats it",
        "im done", "i am done", "were done", "we are done", "nothing else",
    }
)
# Farewell / "we're finished" phrasing anywhere in the utterance.
_USER_FAREWELL = re.compile(
    r"\b(goodbye|good ?night|see you|see ya|were good|we are good|were done|"
    r"we are done|were finished|im off|that(s| is) (all|it|everything))\b"
)
# "no … <specific done-phrase>" — a decline of further help. Uses specific
# phrases (never bare 'good') so "no, that's a good idea" is NOT a sign-off.
_DECLINE_CLOSER = re.compile(
    r"^(no|nope|nah)\b.*\b(thanks|thank you|cheers|im good|im fine|im done|"
    r"im all set|im set|all good|all set|all done|good thanks|fine thanks|"
    r"great thanks|were good|were done|thats all|thats it|thats fine|"
    r"thats everything|nothing else)\b"
)
_SIGNOFF_LEAD = re.compile(
    r"^(no|nope|nah|yeah|yep|yes|ok|okay|alright|right|well|so|um|uh|cool|great|"
    r"thanks|thank you|cheers|jarvis)\s+"
)
_SIGNOFF_TRAIL = re.compile(
    r"\s+(please|thanks|thank you|cheers|now|then|mate|jarvis|ok|okay|here)$"
)
# Jarvis's OWN reply is a goodbye → end even if it forgot the [[END]] marker.
_REPLY_FAREWELL = re.compile(
    r"\b(goodbye|good ?night|see you|see ya|sleep well|take care|farewell|"
    r"talk soon|bye)\b",
    re.IGNORECASE,
)
_REPLY_CONTINUE = re.compile(
    r"\?|anything else|let me know|give me a shout|what else|tell me|how about|"
    r"shall i|would you like|need anything",
    re.IGNORECASE,
)


def _norm(text: str) -> str:
    t = text.lower().replace("'", "")
    t = re.sub(r"[^\w\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def _is_clear_signoff(text: str) -> bool:
    """True only for an unambiguous goodbye / decline of further help."""
    base = _norm(text)
    if _REQUEST_CUE.search(base):
        return False
    if _USER_FAREWELL.search(base) or _DECLINE_CLOSER.search(base):
        return True
    t = base
    while True:
        stripped = _SIGNOFF_TRAIL.sub("", _SIGNOFF_LEAD.sub("", t))
        if stripped == t:
            break
        t = stripped
    return t in _CLEAR_SIGNOFFS


def _is_reply_farewell(reply: str) -> bool:
    """True if Jarvis's reply is a goodbye with no continuation cue."""
    return bool(_REPLY_FAREWELL.search(reply)) and not _REPLY_CONTINUE.search(reply)


# --- Streaming: sentence segmentation + steering reuse ----------------------

# Inworld non-verbals stay inline; everything else leading is a steering
# directive that must be reused across sentences (its scope is the whole call).
_NONVERBALS = frozenset(
    {"laugh", "sigh", "breathe", "cough", "yawn", "clear throat", "gasp"}
)
_LEAD_BRACKET = re.compile(r"^\s*\[([^\]]+)\]\s*")


def _extract_steering(text: str) -> tuple[str, str]:
    """Pull a leading steering directive off the first sentence so it can be
    re-applied to later sentences. Returns (steering_tag_or_empty, remainder)."""
    m = _LEAD_BRACKET.match(text)
    if not m:
        return "", text
    if m.group(1).strip().lower() in _NONVERBALS:
        return "", text  # a sound, not a directive — leave it inline
    return f"[{m.group(1).strip()}]", text[m.end() :]


_ABBREV = frozenset(
    {"mr", "mrs", "ms", "dr", "prof", "st", "sr", "jr", "vs", "etc", "eg", "ie", "mt"}
)
_LAST_WORD = re.compile(r"([A-Za-z]+)$")


def _next_sentence(buf: str, min_len: int = 12, max_len: int = 180) -> tuple[str, str] | None:
    """Split off the first complete sentence from a streaming buffer, never
    breaking inside [...] or <...>. Returns (sentence, rest) or None if not yet."""
    depth_sq = depth_ang = 0
    for i, ch in enumerate(buf):
        if ch == "[":
            depth_sq += 1
        elif ch == "]":
            depth_sq = max(0, depth_sq - 1)
        elif ch == "<":
            depth_ang += 1
        elif ch == ">":
            depth_ang = max(0, depth_ang - 1)
        elif ch in ".!?" and depth_sq == 0 and depth_ang == 0:
            if i + 1 < len(buf) and buf[i + 1] in " \n" and i + 1 >= min_len:
                if ch == ".":  # don't split after an abbreviation or initial
                    m = _LAST_WORD.search(buf[:i])
                    w = m.group(1).lower() if m else ""
                    if w in _ABBREV or len(w) == 1:
                        continue
                return buf[: i + 1].strip(), buf[i + 2 :].lstrip()
    # Force-flush an over-long clause (rare for short replies) at a space.
    if len(buf) >= max_len and depth_sq == 0 and depth_ang == 0:
        cut = buf.rfind(" ", min_len)
        if cut != -1:
            return buf[:cut].strip(), buf[cut + 1 :].lstrip()
    return None


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
        self._gateway = gateway
        self._tts = tts
        self._memory = memory
        self._tracer = tracer
        self._sr = cfg.audio.sample_rate
        self._cold_tasks: set[asyncio.Task] = set()
        self._soul = ""  # personality (SOUL.md), loaded at start
        self._history: list[dict] = []  # rolling shared conversation context
        self.state = State.PASSIVE

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
    def _load_soul(self) -> None:
        import pathlib

        path = pathlib.Path(self._cfg.persona.soul_path)
        if path.exists():
            self._soul = path.read_text(encoding="utf-8").strip()
            print(f"Soul loaded from {path} ({len(self._soul)} chars).")

    async def run(self) -> None:
        print("Loading models…")
        self._load_soul()
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
        print("● idle — say \"Hey Jarvis\"")
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
                room=self._cfg.gateway.room, speaker=self._cfg.gateway.speaker
            )
            secs = len(pcm) / 2 / self._sr
            trace.start("stt")
            text = await asyncio.to_thread(
                self._stt.transcribe, pcm, sample_rate=self._sr
            )
            trace.end("stt", audio_s=round(secs, 1), chars=len(text))
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
            memory = self._memory.read_cached_representation()
            messages = [
                {"role": "system", "content": self._system_prompt(memory)},
                *self._history,  # shared context: the conversation so far
                {"role": "user", "content": text},
            ]

            # Generate + speak. Streaming: speech starts on the first sentence
            # while the rest generates. The full raw reply lands in holder.
            self.state = State.SPEAKING
            if self._cfg.gateway.stream:
                holder: dict = {"reply": ""}
                interrupted = await self._speak_with_bargein(
                    mic, self._stream_speech(messages, model, trace, holder)
                )
                raw_reply = holder["reply"]
            else:
                trace.start("llm")
                raw_reply = await self._gateway.complete(messages, model=model)
                trace.end(
                    "llm", model=model, chars=len(raw_reply or ""), memory=bool(memory)
                )
                spoken = _END_RE.sub(" ", raw_reply or "").strip()
                interrupted = await self._speak_with_bargein(
                    mic, self._tts_source(spoken, trace)
                )

            # End-of-conversation: the model's [[END]] marker OR a deterministic
            # unmistakable sign-off OR Jarvis's own reply being a goodbye.
            ended = self._cfg.vad.conversation_mode and (
                bool(_END_RE.search(raw_reply or ""))
                or _is_clear_signoff(text)
                or _is_reply_farewell(raw_reply or "")
            )
            reply = _END_RE.sub(" ", raw_reply or "").strip()  # never store the marker
            print(f"  jarvis [{model}]: {reply}{'  ⏹' if ended else ''}")
            if interrupted:
                trace.event("barge_in")
            self._tracer.emit(trace)

            # Remember + cold-path the exchange (runs while/after speaking, never
            # blocking the hot path). On barge-in, `reply` is what was actually said.
            if reply:
                self._remember(text, reply)
                self._fire_cold_path(text, reply)

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

    def _system_prompt(self, memory: str) -> str:
        """Soul (who Jarvis is) + format + memory (what he knows about you)."""
        parts = []
        if self._soul:
            parts.append(self._soul)
        parts.append(
            _VOICE_FORMAT_EXPRESSIVE
            if self._cfg.persona.expressive
            else _VOICE_FORMAT_BASE
        )
        if self._cfg.vad.conversation_mode:
            parts.append(_END_INSTRUCTION)
        if memory:
            parts.append(
                "What you already know about the user (use it naturally only if "
                f"relevant; do not recite it):\n{memory}"
            )
        return "\n\n".join(parts)

    def _remember(self, user_text: str, assistant_text: str) -> None:
        """Append the exchange to the rolling shared-context window."""
        self._history.append({"role": "user", "content": user_text})
        self._history.append({"role": "assistant", "content": assistant_text})
        limit = max(0, self._cfg.persona.history_messages)
        if len(self._history) > limit:
            self._history = self._history[-limit:]

    def _fire_cold_path(self, user_text: str, assistant_text: str) -> None:
        """Detached background task — never awaited on the hot path."""
        task = asyncio.create_task(self._cold_path(user_text, assistant_text))
        self._cold_tasks.add(task)
        task.add_done_callback(self._cold_tasks.discard)

    async def _cold_path(self, user_text: str, assistant_text: str) -> None:
        # Write the turn to Honcho (deriver reasons in the background), then
        # refresh the local representation cache for the next turn. Resilient:
        # if memory is unreachable, the turn loop is unaffected.
        t0 = time.perf_counter()
        try:
            await self._memory.write_turn(user_text, assistant_text)
            refreshed = await self._memory.refresh_cache(
                min_interval_s=self._cfg.memory.refresh_interval_s
            )
            if refreshed:
                ms = (time.perf_counter() - t0) * 1000
                mt = self._tracer.turn(
                    room=self._cfg.gateway.room, speaker=self._cfg.gateway.speaker
                )
                mt.set(kind="memory")
                mt.stage("memory", ms)
                self._tracer.emit(mt)
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

    async def _tts_source(self, text: str, trace=None) -> AsyncIterator[bytes]:  # noqa: ANN001
        """Wrap the TTS stream to capture its timing (time-to-first-audio, total
        duration, bytes) into the turn trace — the Inworld call isn't visible in
        the gateway logs, so this is where it's measured."""
        t0 = time.perf_counter()
        first_ms: float | None = None
        total = 0
        try:
            async for chunk in self._tts.synthesize_stream(text):
                if first_ms is None:
                    first_ms = (time.perf_counter() - t0) * 1000
                total += len(chunk)
                yield chunk
        finally:
            if trace is not None:
                trace.stage(
                    "tts",
                    (time.perf_counter() - t0) * 1000,
                    ttfa_ms=round(first_ms, 1) if first_ms is not None else None,
                    bytes=total,
                    chars=len(text),
                    voice=self._cfg.tts.voice,
                    provider=self._cfg.tts.provider,
                )

    async def _stream_speech(self, messages, model, trace, holder) -> AsyncIterator[bytes]:  # noqa: ANN001
        """Stream the LLM, segment into sentences, synthesise each through TTS,
        and yield a single continuous PCM stream — so speech starts on sentence 1
        while later sentences are still generating. Accumulates the full reply
        into holder['reply'] (even on barge-in) and records LLM/TTS timings."""
        t0 = time.perf_counter()
        first_tok: float | None = None
        llm_done: float | None = None
        tts_first: float | None = None
        full: list[str] = []
        steering: str | None = None

        async def sentences() -> AsyncIterator[str]:
            nonlocal first_tok, llm_done
            buf = ""
            async for delta in self._gateway.stream(messages, model=model):
                if first_tok is None:
                    first_tok = time.perf_counter()
                full.append(delta)
                buf += delta
                while True:
                    split = _next_sentence(buf)
                    if split is None:
                        break
                    sent, buf = split
                    if sent.strip():
                        yield sent
            llm_done = time.perf_counter()
            if buf.strip():
                yield buf

        try:
            async for sent in sentences():
                if steering is None:  # capture the leading directive once
                    steering, sent = _extract_steering(sent)
                tts_text = _END_RE.sub(" ", sent).strip()  # never speak the marker
                if not tts_text:
                    continue
                if steering:
                    tts_text = f"{steering} {tts_text}"
                async for pcm in self._tts.synthesize_stream(tts_text):
                    if tts_first is None:
                        tts_first = time.perf_counter()
                    yield pcm
        finally:
            holder["reply"] = "".join(full)
            if trace is not None:
                end = time.perf_counter()
                if first_tok is not None:
                    trace.stage(
                        "llm",
                        ((llm_done or end) - t0) * 1000,
                        model=model,
                        ttft_ms=round((first_tok - t0) * 1000, 1),
                        chars=len(holder["reply"]),
                    )
                trace.stage(
                    "tts",
                    (end - (first_tok or t0)) * 1000,
                    ttfa_ms=round((tts_first - t0) * 1000, 1) if tts_first else None,
                    voice=self._cfg.tts.voice,
                    provider=self._cfg.tts.provider,
                )

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
            await self._audio.play_stream(
                pcm_source, sample_rate=self._cfg.tts.sample_rate
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
            await asyncio.gather(play_task, mon_task, return_exceptions=True)
        return interrupted.is_set()
