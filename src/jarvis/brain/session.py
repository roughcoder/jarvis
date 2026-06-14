"""BrainSession — the think/speak core for one conversation (Phase 3 W4).

Owns the part of a turn that is *not* audio I/O: read local memory, build the
prompt, run the gated tool loop or stream the reply, synthesise TTS, detect
conversation end, and fire the cold-path memory write. It yields PCM and records
the outcome in a `TurnResult`; it never touches the mic, the speaker, or barge-in
— those belong to the caller (the single-process TurnLoop, or the WebSocket
server). One copy of the logic, two transports.

Barge-in safety: `respond()` is a cancellable async generator. The caller cancels
it to interrupt; `result.raw` still holds what was actually said (captured in a
finally), and the caller then calls `finalize()` to remember exactly that.
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from jarvis.brain.context import RequestContext
from jarvis.brain.dialog import (
    _END_INSTRUCTION,
    _END_RE,
    _VOICE_FORMAT_BASE,
    _VOICE_FORMAT_EXPRESSIVE,
    _extract_steering,
    _is_clear_signoff,
    _is_reply_farewell,
    _next_sentence,
)
from jarvis.brain.gateway_client import GatewayClient
from jarvis.brain.memory_client import MemoryClient
from jarvis.brain.tracing import Tracer
from jarvis.config import Config
from jarvis.services.tts import InworldTTS
from jarvis.tools.base import ToolRegistry


def _now_line(tz_name: str) -> str:
    """A human 'right now' string injected so Jarvis knows the date/time without
    a tool or a search. `tz_name` is an IANA name; empty = host local time."""
    from datetime import datetime

    now = None
    if tz_name:
        try:
            from zoneinfo import ZoneInfo

            now = datetime.now(ZoneInfo(tz_name))
        except Exception:
            now = None
    if now is None:
        now = datetime.now().astimezone()
    tz = now.strftime("%Z") or "local time"
    # e.g. "Right now it's Saturday, 14 June 2026, 8:47 pm BST."
    return (
        f"Right now it's {now.strftime('%A, %-d %B %Y')}, "
        f"{now.strftime('%-I:%M %p').lower()} {tz}."
    )


def _make_heartbeat(sample_rate: int) -> bytes:
    """A soft 'lub-dub' as 16-bit PCM at the playback rate — the gentle pulse
    played periodically while a slow tool (web search) runs."""
    import numpy as np

    def thump(freq: float, ms: int, amp: float):  # noqa: ANN202
        t = np.linspace(0, ms / 1000, int(sample_rate * ms / 1000), False)
        tone = amp * np.sin(2 * np.pi * freq * t)
        fade = max(1, int(sample_rate * 0.012))  # 12ms fades kill clicks
        env = np.ones_like(tone)
        env[:fade] = np.linspace(0, 1, fade)
        env[-fade:] = np.linspace(1, 0, fade)
        return tone * env

    gap = np.zeros(int(sample_rate * 0.10))  # 100ms between lub and dub
    buf = np.concatenate([thump(150, 90, 0.16), gap, thump(120, 110, 0.12)])
    return (buf * 32767).astype(np.int16).tobytes()


@dataclass
class TurnResult:
    raw: str = ""  # reply incl. any [[END]] marker (may be partial on barge-in)
    reply: str = ""  # spoken/stored reply (marker stripped); set by finalize()
    ended: bool = False  # conversation closed; set by finalize()
    # The turn's tool calls + results (assistant tool_calls then tool messages),
    # kept so the NEXT turn knows what was done (e.g. a job id it just created).
    tool_messages: list = field(default_factory=list)


class BrainSession:
    def __init__(
        self,
        cfg: Config,
        ctx: RequestContext,
        *,
        gateway: GatewayClient,
        tts: InworldTTS,
        memory: MemoryClient,
        tracer: Tracer,
        registry: ToolRegistry,
    ) -> None:
        self._cfg = cfg
        self._ctx = ctx
        self._gateway = gateway
        self._tts = tts
        self._memory = memory
        self._tracer = tracer
        self._registry = registry
        self._soul = ""  # personality (SOUL.md), loaded at start
        self._history: list[dict] = []  # rolling shared conversation context
        self._cold_tasks: set[asyncio.Task] = set()
        self._heartbeat_pcm: bytes | None = None  # cached tool-search pulse

    def load_soul(self) -> None:
        path = pathlib.Path(self._cfg.persona.soul_path)
        if path.exists():
            self._soul = path.read_text(encoding="utf-8").strip()
            print(f"Soul loaded from {path} ({len(self._soul)} chars).")

    # --- the think/speak core ----------------------------------------------
    async def respond(
        self, user_text: str, trace, result: TurnResult
    ) -> AsyncIterator[bytes]:  # noqa: ANN001
        """Yield PCM for the spoken reply to `user_text`; record the raw reply in
        `result`. Hot path reads the LOCAL cached representation only (a fast file
        read), never a live memory reasoning call (spec §3.2). Call finalize()
        afterwards (even on barge-in) to detect end + remember."""
        model = (
            self._cfg.gateway.strong_model
            if len(user_text) > 120
            else self._cfg.gateway.fast_model
        )
        memory = self._memory.read_cached_representation()
        messages = [
            {"role": "system", "content": self._system_prompt(memory)},
            *self._history,  # shared context: the conversation so far
            {"role": "user", "content": user_text},
        ]
        tool_schemas = [t.openai_schema() for t in self._registry.available_for(self._ctx)]

        if tool_schemas:
            # Tool turn: run the tool loop (which yields a short "looking that up"
            # earcon into the audio stream when a tool fires), then speak the final
            # answer. Casual no-tool setups never enter this branch, so their
            # streaming TTFT is unchanged.
            async for pcm in self._run_tool_loop(messages, model, trace, tool_schemas, result):
                yield pcm
            async for pcm in self._tts_source(_END_RE.sub(" ", result.raw or "").strip(), trace):
                yield pcm
        elif self._cfg.gateway.stream:
            async for pcm in self._stream_speech(messages, model, trace, result):
                yield pcm  # result.raw set inside _stream_speech's finally
        else:
            trace.start("llm")
            raw = await self._gateway.complete(messages, model=model)
            trace.end("llm", model=model, chars=len(raw or ""), memory=bool(memory))
            result.raw = raw
            async for pcm in self._tts_source(_END_RE.sub(" ", raw or "").strip(), trace):
                yield pcm

    def finalize(self, user_text: str, result: TurnResult) -> None:
        """End-detection + remember + cold-path. Safe to call after a barge-in —
        `result.raw` is what was actually said."""
        raw = result.raw or ""
        result.reply = _END_RE.sub(" ", raw).strip()  # never store the marker
        result.ended = self._cfg.vad.conversation_mode and (
            bool(_END_RE.search(raw))
            or _is_clear_signoff(user_text)
            or _is_reply_farewell(raw)
        )
        self._remember(user_text, result)
        if result.reply:
            self._fire_cold_path(user_text, result.reply)

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
        # Most volatile (changes each minute) → last, so the stable prefix above
        # stays cacheable. Lets Jarvis answer time/date instantly, no tool needed.
        parts.append(_now_line(self._cfg.persona.timezone))
        return "\n\n".join(parts)

    async def _run_tool_loop(self, messages, model, trace, tool_schemas, result):  # noqa: ANN001
        """Tool-aware completion: let the model call gated tools, feed results
        back, repeat until it answers. Sets `result.raw` to the final text. While a
        slow/announced tool runs it yields a soft heartbeat pulse into the audio
        stream (instant local tools stay silent). Each tool is capability-checked
        and hard-timeout-bounded; a tool error is fed back rather than breaking the
        turn."""
        t0 = time.perf_counter()
        n_tools = 0
        for _ in range(max(1, self._cfg.tools.max_rounds)):
            msg = await self._gateway.complete_with_tools(
                messages, model=model, tools=tool_schemas
            )
            if not msg.tool_calls:
                result.raw = msg.content or ""
                self._record_llm(trace, t0, model, result.raw, n_tools)
                return
            assistant_msg = {
                "role": "assistant",
                "content": msg.content or "",
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                    }
                    for tc in msg.tool_calls
                ],
            }
            messages.append(assistant_msg)
            result.tool_messages.append(assistant_msg)  # carry into history
            for tc in msg.tool_calls:
                n_tools += 1
                tool = self._registry.get(tc.function.name)
                if tool is not None and tool.announce:
                    # Slow/remote tool (web search): a soft heartbeat pulses while
                    # it runs so the user hears the search is happening.
                    tool_result = ""
                    async for item in self._execute_with_heartbeat(tc):
                        if isinstance(item, bytes):
                            yield item
                        else:
                            tool_result = item
                else:
                    tool_result = await self._execute_call(tc)  # instant: no pulse
                if trace is not None:
                    trace.event("tool", tool=tc.function.name)
                tool_msg = {"role": "tool", "tool_call_id": tc.id, "content": tool_result}
                messages.append(tool_msg)
                result.tool_messages.append(tool_msg)  # carry into history
        # Out of tool rounds — force a final answer with no further tool calls.
        msg = await self._gateway.complete_with_tools(messages, model=model, tools=None)
        result.raw = msg.content or ""
        self._record_llm(trace, t0, model, result.raw, n_tools)

    @staticmethod
    def _record_llm(trace, t0: float, model: str, content: str, n_tools: int) -> None:  # noqa: ANN001
        if trace is not None:
            trace.stage(
                "llm",
                (time.perf_counter() - t0) * 1000,
                model=model,
                chars=len(content),
                tools=n_tools,
            )

    async def _execute_call(self, tc) -> str:  # noqa: ANN001
        """Run one tool call to its result string. Never raises — a tool error is
        returned as text and fed back to the model."""
        try:
            args = json.loads(tc.function.arguments or "{}")
            return await self._registry.execute(
                self._ctx, tc.function.name, args, timeout_s=self._cfg.tools.timeout_s
            )
        except Exception as exc:  # noqa: BLE001 - tools must never break a turn
            return f"error: {exc}"

    async def _execute_with_heartbeat(self, tc):  # noqa: ANN001
        """Run a slow tool, yielding a soft heartbeat pulse (bytes) every
        `heartbeat_interval_s` while it runs, then the result (str) last — the two
        are told apart by type. The task is cancelled if the caller stops (a
        barge-in closes the generator)."""
        task = asyncio.create_task(self._execute_call(tc))
        try:
            yield self._heartbeat()  # first pulse — the search has started
            while not task.done():
                done, _ = await asyncio.wait(
                    {task}, timeout=self._cfg.tools.heartbeat_interval_s
                )
                if not done:
                    yield self._heartbeat()
            yield task.result()
        finally:
            if not task.done():
                task.cancel()

    def _heartbeat(self) -> bytes:
        """A soft 'still working' pulse (cached at the TTS rate)."""
        if self._heartbeat_pcm is None:
            self._heartbeat_pcm = _make_heartbeat(self._cfg.tts.sample_rate)
        return self._heartbeat_pcm

    def _remember(self, user_text: str, result: TurnResult) -> None:
        """Append the full turn to the rolling shared-context window — user, any
        tool calls + results (so the next turn knows what was done), then the
        spoken reply."""
        if not (result.reply or result.tool_messages):
            return
        self._history.append({"role": "user", "content": user_text})
        self._history.extend(result.tool_messages)
        if result.reply:
            self._history.append({"role": "assistant", "content": result.reply})
        self._trim_history()

    def _trim_history(self) -> None:
        limit = max(0, self._cfg.persona.history_messages)
        if len(self._history) <= limit:
            return
        trimmed = self._history[-limit:]
        # A tool message orphaned from its assistant tool_calls is invalid, so
        # never start the window mid tool-group: drop leading non-user messages.
        while trimmed and trimmed[0].get("role") != "user":
            trimmed.pop(0)
        self._history = trimmed

    def _fire_cold_path(self, user_text: str, assistant_text: str) -> None:
        """Detached background task — never awaited on the hot path."""
        task = asyncio.create_task(self._cold_path(user_text, assistant_text))
        self._cold_tasks.add(task)
        task.add_done_callback(self._cold_tasks.discard)

    async def _cold_path(self, user_text: str, assistant_text: str) -> None:
        # Write the turn to Honcho (deriver reasons in the background), then
        # refresh the local representation cache for the next turn. Resilient:
        # if memory is unreachable, the turn is unaffected.
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

    async def _stream_speech(self, messages, model, trace, result) -> AsyncIterator[bytes]:  # noqa: ANN001
        """Stream the LLM, segment into sentences, synthesise each through TTS,
        and yield a single continuous PCM stream — so speech starts on sentence 1
        while later sentences are still generating. Captures the full reply into
        result.raw (even on barge-in) and records LLM/TTS timings."""
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
            result.raw = "".join(full)
            if trace is not None:
                end = time.perf_counter()
                if first_tok is not None:
                    trace.stage(
                        "llm",
                        ((llm_done or end) - t0) * 1000,
                        model=model,
                        ttft_ms=round((first_tok - t0) * 1000, 1),
                        chars=len(result.raw),
                    )
                trace.stage(
                    "tts",
                    (end - (first_tok or t0)) * 1000,
                    ttfa_ms=round((tts_first - t0) * 1000, 1) if tts_first else None,
                    voice=self._cfg.tts.voice,
                    provider=self._cfg.tts.provider,
                )
