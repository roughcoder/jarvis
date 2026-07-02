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
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from types import SimpleNamespace

from jarvis.runtime import RequestContext
from jarvis.brain.dialog import (
    _END_INSTRUCTION,
    _END_RE,
    _MESSAGING_FORMAT,
    _VOICE_FORMAT_BASE,
    _VOICE_FORMAT_EXPRESSIVE,
    _extract_steering,
    _is_clear_signoff,
    _is_reply_farewell,
    _next_sentence,
    _now_line,
)
from jarvis.brain.gateway_client import GatewayClient, LLMAttribution
from jarvis.brain.memory_client import MemoryClient
from jarvis.users import format_facts, read_facts
from jarvis.brain.tracing import Tracer
from jarvis.brain.voice_modes import (
    DEFAULT_MODE,
    STAY_MODE,
    classify_voice_turn,
    local_voice_action,
    normalize_mode,
    strip_voice_controls,
    voice_disabled_transition,
    voice_mode_instruction,
)
from jarvis.config import Config
from jarvis.services.tts import InworldTTS
from jarvis.tools.base import ToolRegistry
from jarvis.tools.selection import offered_servers, select_tools


_AGENCY = (
    "Act, don't advise: you're an assistant with hands. When the user asks for something "
    "you have a tool to do, DO it — never answer 'call them', 'use their website', "
    "'you'll need to…', or 'I can't access that' when a tool could do it. Persist down "
    "your options: try the right tool, and if it fails fix the obvious problem and try "
    "again, or take another route, before you conclude you can't. Stop only at a GENUINE "
    "wall — a login, two-factor, a payment, a captcha, or a destructive and irreversible "
    "step — and when you hit one, say specifically what you need from the user to get "
    "past it; don't refuse vaguely. Report only what truly happened: never claim you did "
    "something you didn't confirm, and don't call it a failure when you merely reached a "
    "permission prompt. NEVER invent information to fill a gap — a name, email, phone "
    "number, address, date, a payment detail, or which option to pick when it's ambiguous. "
    "Use only what the user actually told you; if a form or task needs a detail you don't "
    "have, STOP and ASK for that specific thing rather than guessing or making something up."
)

_BACKGROUND_GUIDANCE = (
    "Slow work goes to the background: for anything that takes more than a few seconds — "
    "driving the Mac through several steps, a multi-page web task, deep research, a "
    "booking — hand it to run_in_background and tell the user you're on it, rather than "
    "making them wait through it on this turn. That includes slow [skill] tools (news "
    "briefings, multi-step recipes that browse several pages): run those via "
    "run_in_background too. You'll report the outcome to them proactively when it's "
    "done. Do it inline only when it's genuinely quick."
)

_PROFILE_GUIDANCE = (
    "Remembering personal facts: when the user states a durable, structured fact about "
    "themselves — email, postal address, phone number, birthday, names of family or pets, "
    "a standing preference — or asks you to remember something, you MUST call the `remember` "
    "tool to persist it. Actually call the tool; never just say you've saved it. Use a short "
    "stable label (e.g. 'email', 'address') and the value verbatim, then confirm in a few "
    "words ('Got it — saved your email.'). "
    "IMPORTANT: only the explicit 'Facts the user has asked you to remember' list (below, if "
    "present) counts as saved. Vaguer background knowledge of the user is fuzzy and may be "
    "wrong or stale — do NOT treat it as already saved. If the user asks you to remember a "
    "durable fact, call `remember` even if you think you already know it; the tool is "
    "idempotent, so re-saving is harmless. Don't save fleeting or conversational remarks. "
    "Use `forget` to remove one, `list_facts` to read back what's actually saved."
)

_BROWSER_GUIDANCE = (
    "The web browser (when worker.browser is granted) is your hand for INTERACTIVE web — "
    "checking availability, filling forms, logging in, bookings, anything behind a click. "
    "For just reading facts, use web_search; to actually DO something on a site, use the "
    "browser. If you need to type but the snapshot shows only a 'Search' link or button "
    "(not an input/textbox), CLICK it first to reveal the field, then snapshot again and "
    "type into the input that appears — many sites hide the search box until you open it. "
    "NEVER guess a domain — if you don't already know the exact URL, web_search "
    "for it first (e.g. 'Old Crown Great Bookham booking') and open the real link from the "
    "results; if a page won't load (DNS error, can't be reached), search for the correct "
    "URL and try again rather than giving up. To READ a page (extract an answer, a code, "
    "opening hours, availability) "
    "call browser_read — not snapshot. To ACT, browser_snapshot to see the elements "
    "(each has a [ref]), then browser_click / browser_type by ref, snapshot again after "
    "the page changes. If a control won't respond to a click (a dropdown/combobox that "
    "stays shut, a date/time picker), use browser_press: focus it by ref and press "
    "ArrowDown to open it, ArrowDown/Enter to choose, Tab to move between fields, Escape "
    "to dismiss — many widgets are keyboard-driven, not click-driven. Never give up after "
    "a snapshot shows nothing clickable — call browser_read to read the text. If a ref is "
    "stale, snapshot again rather than "
    "guessing. Two browsers: 'device' (the machine's Chrome with its "
    "logins) and 'jarvis' (his own profile) — omit context for the default. If you hit a "
    "login, captcha, or payment you can't pass, stop and tell the user what to do in the "
    "browser window; don't pretend it failed. Only state a time, price, or availability "
    "you have ACTUALLY read off the page with browser_read — if you're not certain you read "
    "the right figure, read again or say you couldn't confirm it; never fill the gap with a "
    "plausible-sounding guess. "
    "LIVE DATA (train/bus times, opening hours, prices, availability): a web_search snippet "
    "is stale — NEVER answer from it; open a live source and browser_read the real values "
    "first. Prefer a simple, server-rendered page you can just READ over a heavy JavaScript "
    "app that needs a form filled in: blindly pressing Tab/Enter to drive a date-time picker "
    "(e.g. Trainline) is unreliable — if a result needs form-filling, you usually don't need "
    "that site at all. For UK train times use realtimetrains.co.uk: it has stable, readable "
    "URLs and server-rendered tables, e.g. realtimetrains.co.uk/search/detailed/gb-nr:<FROM>/"
    "to/gb-nr:<TO>/<yyyy-mm-dd>/<HHMM> with three-letter CRS codes (Effingham Junction=EFF, "
    "Guildford=GLD, London Vauxhall=VXH) — open that and browser_read the board rather than "
    "wrestling a booking site. Read the actual rows before you answer, and don't claim a "
    "fact is wrong or right until you've re-read the page."
)

_GUI_GUIDANCE = (
    "Controlling the Mac (when worker.gui is granted): control_mac is the only way to "
    "ACT on screen — open apps, click, type, drive any app ('open the BBC Sport site in "
    "Chrome', 'leave the Discord call'). It's an autonomous agent that plans, focuses the "
    "right window, acts, and verifies, but it works step by step and can take a while — "
    "so for anything beyond a quick single action, start it via run_in_background and say "
    "you're on it rather than waiting on the live turn. Report what it actually returns "
    "(don't claim success it didn't confirm); if it comes back with a QUESTION or asks to "
    "confirm, RELAY that to the user and act on their answer — never say you can't when "
    "the agent was only asking permission. To READ the screen without acting, use "
    "look_at_screen (it sends you the actual image); to look facts up, use web_search "
    "rather than driving a browser by hand. Use each app's exact name ('Google Chrome')."
)

_INTERCOM_CAMERA_GUIDANCE = (
    "This intercom has a camera. When the user asks what they are holding, showing, "
    "wearing, pointing at, or what is in front of the device, call `take_photo` and "
    "answer from the captured image. If they ask whether you have a camera, say yes: "
    "you can take a fresh photo from this room device when needed. Do not claim you "
    "cannot see when `take_photo` is available."
)

_INTERCOM_DISPLAY_GUIDANCE = (
    "This intercom has a PiPanel display. When the user asks to turn the screen off, "
    "hide the screen, turn the screen on, show the screen, toggle it, or check whether "
    "it is on, call `control_pi_panel`. Map 'off' to hide and 'on' to show."
)

_SELF_GUIDANCE = (
    "Device awareness (when self.inspect is granted): use describe_device to know which "
    "configured Jarvis device, identity, host runtime, and capability set this request is "
    "running under. Do not reveal secrets, token values, private user files, or hidden "
    "credentials."
)

_SELF_DIAGNOSTICS_GUIDANCE = (
    "Basic diagnostics (when self.diagnostics is granted): use run_self_diagnostics for "
    "quick fixed read-only terminal checks about this device and Jarvis process. Use "
    "get_ip_address when asked for your IP address, ping_host for latency or packet loss "
    "to a host, resolve_dns for DNS lookups, and check_tcp_port for simple reachability. "
    "For arbitrary non-destructive terminal commands use run_shell if worker.shell is "
    "granted."
)

_ENGINEERING_GUIDANCE = (
    "Terminal work: run_shell executes a bounded shell command on the worker Mac. Prefer "
    "read-only diagnostics first, and do not run destructive commands unless the user "
    "explicitly asked for that specific destructive action."
)


_BACKGROUND_FRAMING = (
    "You are completing this task in the BACKGROUND — the user has already been told "
    "you're on it and has moved on, so there is NO ONE to ask follow-up questions. Make "
    "sensible decisions yourself and use your tools to actually DO the task, end to end. "
    "When you're finished, reply with ONE or two natural spoken sentences reporting the "
    "outcome — what you did and the result, or, if you genuinely couldn't finish, what "
    "stopped you. That sentence is spoken to the user out of the blue, so make it sound "
    "like you're proactively letting them know it's done."
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
    continue_listening: bool = False  # voice edge should capture another utterance
    voice_mode: str = DEFAULT_MODE  # active voice mode after this turn
    close_reason: str = ""  # task_complete | user_closed | mode_enter | mode_exit | ...
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
        memory_user: str | None = None,
        relevance=None,  # noqa: ANN001 - optional EmbeddingRelevance (else keyword prefilter)
    ) -> None:
        self._cfg = cfg
        self._ctx = ctx
        self._relevance = relevance
        # The memory principal (Honcho peer). None => single-principal base cache
        # (the single-process loop / Phase 1). The brain server passes the resolved
        # speaker so each user's memory is isolated (the privacy wall, §5).
        self._memory_user = memory_user
        self._gateway = gateway
        self._tts = tts
        self._memory = memory
        self._tracer = tracer
        self._registry = registry
        self._soul = ""  # personality (SOUL.md), loaded at start
        # Per-server keyword overrides for the relevance prefilter (§9). Built once.
        self._server_keywords = {
            s.name: set(s.keywords) for s in cfg.mcp.servers if s.keywords
        }
        self._history: list[dict] = []  # rolling shared conversation context
        self._cold_tasks: set[asyncio.Task] = set()
        self._heartbeat_pcm: bytes | None = None  # cached tool-search pulse
        self._voice_mode = DEFAULT_MODE
        # MCP servers already offered this conversation stay offered (sticky): the
        # tool list is part of the provider's cached prompt prefix, so a set that
        # flickers turn-to-turn forces a full prompt-cache miss. Cleared when the
        # conversation ends.
        self._sticky_servers: set[str] = set()

    @property
    def pending_cold_tasks(self) -> tuple[asyncio.Task, ...]:
        return tuple(self._cold_tasks)

    def set_voice_mode(self, mode: str) -> None:
        self._voice_mode = normalize_mode(mode)

    def load_soul(self) -> None:
        path = pathlib.Path(self._cfg.persona.soul_path)
        if path.exists():
            self._soul = path.read_text(encoding="utf-8").strip()
            print(f"Soul loaded from {path} ({len(self._soul)} chars).")

    def _gateway_for(self, *, kind: str = "turn"):  # noqa: ANN202
        """Return the gateway with per-context LiteLLM attribution when supported.

        Tests often pass simple fake gateways; they intentionally don't need to
        implement attribution.
        """
        attr = LLMAttribution(
            kind=kind,
            channel=self._ctx.channel,
            speaker=self._ctx.identity,
            device_id=self._ctx.device_id,
        )
        if hasattr(self._gateway, "with_attribution"):
            return self._gateway.with_attribution(attr)
        return self._gateway

    # --- the think/speak core ----------------------------------------------
    async def respond(
        self, user_text: str, trace, result: TurnResult
    ) -> AsyncIterator[bytes]:  # noqa: ANN001
        """Yield PCM for the spoken reply to `user_text`; record the raw reply in
        `result`. Hot path reads the LOCAL cached representation only (a fast file
        read), never a live memory reasoning call (spec §3.2). Call finalize()
        afterwards (even on barge-in) to detect end + remember."""
        self._prewarm_tts()  # open the TTS connection while we think
        if self._ctx.channel == "voice":
            action = local_voice_action(user_text, self._voice_mode)
            if action is not None:
                if action.mode == STAY_MODE and not self._cfg.vad.conversation_mode:
                    reply = "I can't stay with you while follow-up listening is off."
                    result.raw = reply
                    result.reply = reply
                    result.voice_mode = DEFAULT_MODE
                    result.ended = True
                    result.continue_listening = False
                    result.close_reason = "conversation_disabled"
                    async for pcm in self._tts_source(reply, trace):
                        yield pcm
                    return
                self._voice_mode = action.mode
                result.raw = action.reply
                result.reply = action.reply
                result.voice_mode = action.mode
                result.ended = action.ended
                result.continue_listening = action.continue_listening
                result.close_reason = action.reason
                async for pcm in self._tts_source(action.reply, trace):
                    yield pcm
                return

        model = self._initial_model(user_text)
        memory = self._memory.read_cached_representation(self._memory_user)
        messages = [
            {"role": "system", "content": self._system_prompt(memory)},
            *self._history,  # shared context: the conversation so far
            {"role": "user", "content": user_text},
        ]
        tool_schemas = await self._offer_tools(user_text)

        if tool_schemas or self._cfg.gateway.stream:
            # One speaking core for tool turns AND plain streamed turns: the loop
            # streams each completion, segments the final answer into sentences,
            # and synthesises them as they arrive — speech starts on sentence one,
            # not after the full reply. Yields earcon pulses while tools run.
            async for pcm in self._run_tool_loop(
                messages, model, trace, tool_schemas, result, speak=True
            ):
                yield pcm
        else:
            trace.start("llm")
            raw = await self._gateway_for().complete(messages, model=model)
            trace.end("llm", model=model, chars=len(raw or ""), memory=bool(memory))
            result.raw = raw
            async for pcm in self._tts_source(self._clean_reply(raw), trace):
                yield pcm

    def finalize(self, user_text: str, result: TurnResult, trace=None) -> None:  # noqa: ANN001
        """End-detection + remember + cold-path. Safe to call after a barge-in —
        `result.raw` is what was actually said."""
        raw = result.raw or ""
        result.reply = self._clean_reply(raw)  # never store control markers
        voice_mode_before = self._voice_mode
        result.voice_mode = normalize_mode(result.voice_mode or self._voice_mode)
        is_voice_channel = self._ctx.channel == "voice"
        is_open_mic_voice = is_voice_channel and self._cfg.vad.conversation_mode
        transition = None
        if is_open_mic_voice and not result.close_reason:
            explicit_close = (
                bool(_END_RE.search(raw))
                or _is_clear_signoff(user_text)
                or _is_reply_farewell(raw)
            )
            transition = classify_voice_turn(
                active_mode=self._voice_mode,
                raw_reply=raw,
                user_text=user_text,
                tool_messages=result.tool_messages,
                explicit_close=explicit_close,
            )
            result.ended = transition.ended
            result.continue_listening = transition.continue_listening
            result.close_reason = transition.reason
            result.voice_mode = transition.mode
        elif is_open_mic_voice:
            result.voice_mode = normalize_mode(result.voice_mode)
        elif is_voice_channel:
            transition = voice_disabled_transition()
            result.ended = transition.ended
            result.continue_listening = transition.continue_listening
            result.close_reason = result.close_reason or transition.reason
            result.voice_mode = transition.mode
        else:
            result.ended = False
            result.continue_listening = False
            result.voice_mode = DEFAULT_MODE
        self._voice_mode = result.voice_mode
        if trace is not None and is_voice_channel:
            trace.set(
                voice_mode_before=voice_mode_before,
                voice_mode_after=result.voice_mode,
                close_reason=result.close_reason,
                continue_listening=result.continue_listening,
                ended=result.ended,
            )
            if transition is not None:
                trace.set(
                    policy_decision=transition.policy_decision,
                    marker_seen=transition.marker_seen,
                    assistant_asked_followup=transition.assistant_asked_followup,
                )
        self._remember(user_text, result)
        if result.ended:
            self._sticky_servers.clear()  # next conversation re-narrows the offer
        if result.reply:
            self._fire_cold_path(user_text, result.reply)

    async def respond_text(self, user_text: str, trace, result: TurnResult) -> str:  # noqa: ANN001
        """Text-only turn: the SAME think core as respond() but it returns the reply
        text and plays NO audio (a text client wants ReplyText only). Reuses the tool
        loop, so tools work in text mode — the harness can drive the browser, etc.
        Call finalize() afterwards for end-detection + memory, exactly like respond()."""
        model = self._initial_model(user_text)
        memory = self._memory.read_cached_representation(self._memory_user) if self._memory else ""
        messages = [
            {"role": "system", "content": self._system_prompt(memory)},
            *self._history,
            {"role": "user", "content": user_text},
        ]
        tool_schemas = await self._offer_tools(user_text)
        if tool_schemas:
            # Drain the tool loop's PCM (earcons / vision) — a text client never plays it.
            async for _pcm in self._run_tool_loop(messages, model, trace, tool_schemas, result):
                pass
        else:
            if trace is not None:
                trace.start("llm")
            raw = await self._gateway_for().complete(messages, model=model)
            if trace is not None:
                trace.end("llm", model=model, chars=len(raw or ""), memory=bool(memory))
            result.raw = raw
        return self._clean_reply(result.raw)

    async def run_task(self, task: str, *, max_rounds: int) -> str:
        """Headless agentic execution for the background lane (fire-and-forget): run
        the gated tool loop to completion and return a short spoken-style summary of
        the outcome. No TTS, no audio, no trace, and it does NOT touch the live
        conversation history — its own ephemeral message list. Same `ctx` as the
        asker, so it runs with their capabilities and never more. Uses the strong
        model (off the hot path, quality over latency)."""
        memory = self._memory.read_cached_representation(self._memory_user) if self._memory else ""
        system_prompt = self._system_prompt(memory, include_voice_controls=False)
        messages = [
            {"role": "system", "content": f"{system_prompt}\n\n{_BACKGROUND_FRAMING}"},
            {"role": "user", "content": task},
        ]
        model = self._cfg.gateway.strong_model
        # One-shot background task — no conversation to keep a sticky offer for.
        tool_schemas = await self._offer_tools(task, sticky=False)
        gateway = self._gateway_for(kind="background")
        for _ in range(max(1, max_rounds)):
            msg = await gateway.complete_with_tools(
                messages, model=model, tools=tool_schemas or None
            )
            if not msg.tool_calls:
                return self._clean_reply(msg.content or "")
            messages.append({
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
            })
            for tc in msg.tool_calls:
                tool = self._registry.get(tc.function.name)
                tool_result = await self._execute_call(tc)
                self._log_tool_call(tool, tc, tool_result)
                if tool is not None and tool.produces_image and not tool_result.startswith("error"):
                    messages.append({"role": "tool", "tool_call_id": tc.id, "content": "(image captured — image below)"})
                    messages.append({
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "This is the captured image:"},
                            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{tool_result}"}},
                        ],
                    })
                    model = self._cfg.gateway.vision_model or model
                else:
                    messages.append({"role": "tool", "tool_call_id": tc.id, "content": tool_result})
        # Out of rounds — force a final summary with no further tool calls.
        final = await gateway.complete_with_tools(messages, model=model, tools=None)
        return self._clean_reply(final.content or "")

    def _system_prompt(self, memory: str, *, include_voice_controls: bool = True) -> str:
        """Soul (who Jarvis is) + format + memory (what he knows about you)."""
        parts = []
        if self._soul:
            parts.append(self._soul)
        # Format depends on the surface: voice is heard (spoken rules + TTS cues),
        # messaging surfaces (WhatsApp, the text console) are read (written prose).
        if self._ctx.channel == "voice":
            parts.append(
                _VOICE_FORMAT_EXPRESSIVE
                if self._cfg.persona.expressive
                else _VOICE_FORMAT_BASE
            )
        else:
            parts.append(_MESSAGING_FORMAT)
        # End-detection only matters for an open-mic voice conversation; on a
        # messaging surface every inbound message is already a discrete turn.
        if include_voice_controls and self._cfg.vad.conversation_mode and self._ctx.channel == "voice":
            parts.append(_END_INSTRUCTION)
            parts.append(voice_mode_instruction(self._voice_mode))
        parts.append(_AGENCY)  # act-by-default + persistence (stable, cacheable)
        if self._ctx.can("background.run"):
            parts.append(_BACKGROUND_GUIDANCE)
        if self._ctx.can("profile.write"):
            parts.append(_PROFILE_GUIDANCE)
        if self._ctx.can("worker.browser"):
            parts.append(_BROWSER_GUIDANCE)
        if self._ctx.can("worker.gui"):
            parts.append(_GUI_GUIDANCE)
        if self._ctx.can("intercom.camera"):
            parts.append(_INTERCOM_CAMERA_GUIDANCE)
        if self._ctx.can("intercom.display"):
            parts.append(_INTERCOM_DISPLAY_GUIDANCE)
        if self._ctx.can("self.inspect"):
            parts.append(_SELF_GUIDANCE)
        if self._ctx.can("self.diagnostics"):
            parts.append(_SELF_DIAGNOSTICS_GUIDANCE)
        if self._ctx.can("worker.shell"):
            parts.append(_ENGINEERING_GUIDANCE)
        if self._ctx.can("worker.shell") and self._cfg.worker.shell_secrets:
            names = ", ".join(
                n.strip() for n in self._cfg.worker.shell_secrets.split(",") if n.strip()
            )
            parts.append(
                "Secrets in shell commands: these are set as environment variables on the "
                f"worker — {names}. Reference one by name in a command (e.g. "
                'curl -H "Authorization: Bearer $OPENAI_API_KEY" …) rather than asking the '
                "user for it or writing a placeholder. Never print, echo, or read back a "
                "secret's value."
            )
        # Who you're talking to (§5 know-or-ask). Known speaker → name them; unknown
        # on a shared device → tell the model to ASK before anything personal.
        if self._ctx.identity and self._ctx.identity != "house" and self._ctx.scope == "personal":
            parts.append(f"You're speaking with {self._ctx.identity}.")
        elif self._ctx.confidence == "unknown":
            parts.append(
                "You don't yet know who's speaking (a shared device). If a request "
                "needs personal data or someone's accounts, first ask who you're "
                "talking to; general questions don't need it."
            )
        # Saved facts = the authoritative rail (verbatim, user-curated). Inject them
        # for a known personal speaker, ahead of Honcho's fuzzy summary below.
        facts = self._saved_facts()
        if facts:
            parts.append(
                "Facts the user has asked you to remember (authoritative — trust these "
                f"over anything fuzzier):\n{facts}"
            )
        if memory:
            parts.append(
                "What you already know about the user (use it naturally only if "
                f"relevant; do not recite it):\n{memory}"
            )
        # Most volatile (changes each minute) → last, so the stable prefix above
        # stays cacheable. Lets Jarvis answer time/date instantly, no tool needed.
        parts.append(_now_line(self._cfg.persona.timezone))
        return "\n\n".join(parts)

    def _saved_facts(self) -> str:
        """The speaker's curated facts (local file read, like the memory cache — never a
        network call on the hot path). Only for a known personal-scope principal."""
        if self._ctx.scope != "personal" or not self._ctx.identity or self._ctx.identity == "house":
            return ""
        path = pathlib.Path(self._cfg.capabilities.users_dir) / f"{self._ctx.identity}.md"
        return format_facts(read_facts(path))

    @staticmethod
    def _clean_reply(text: str) -> str:
        return strip_voice_controls(_END_RE.sub(" ", text or "")).strip()

    def _initial_model(self, user_text: str) -> str:
        """Pick the starting model for a turn. Voice is latency-bound by TTS, so short
        voice turns use the voice route (falling back to fast) and escalate on tool
        use in the loop. Messaging channels (WhatsApp, the text console) aren't
        TTS-bound, so they use the strong model from the start for better
        quality/accuracy. Long prompts always go strong."""
        g = self._cfg.gateway
        if self._ctx.channel != "voice" or len(user_text) > 120:
            return g.strong_model
        return g.voice_model or g.fast_model

    async def _offer_tools(self, user_text: str, *, sticky: bool = True) -> list[dict]:
        """The turn's tool offer: capability-filtered, relevance-narrowed (§9), and
        conversation-sticky — once an MCP server is offered it stays offered until
        the conversation ends, so the tool list (part of the provider's cached
        prompt prefix) doesn't flicker turn-to-turn and break the cache. Returns
        name-sorted OpenAI schemas (a byte-identical prefix when unchanged)."""
        available = self._registry.available_for(self._ctx)
        if self._relevance is not None:
            offered = await self._relevance.select(available, user_text)
        else:
            offered = select_tools(
                available,
                user_text,
                enabled=self._cfg.tools.relevance_filter,
                extra_keywords=self._server_keywords,
            )
        if sticky:
            names = {t.name for t in offered}
            offered = list(offered) + [
                t
                for t in available
                if t.required_capability.startswith("mcp.")
                and t.required_capability[len("mcp.") :] in self._sticky_servers
                and t.name not in names
            ]
            self._sticky_servers.update(
                t.required_capability[len("mcp.") :]
                for t in offered
                if t.required_capability.startswith("mcp.")
            )
        self._log_offered(available, offered)
        return [t.openai_schema() for t in sorted(offered, key=lambda t: t.name)]

    def _prewarm_tts(self) -> None:
        """Fire-and-forget: open the pooled TTS connection while STT/LLM run, so
        the turn's first synthesis call skips the TLS handshake."""
        if self._tts is None or not hasattr(self._tts, "prewarm"):
            return
        task = asyncio.create_task(self._tts.prewarm())
        self._cold_tasks.add(task)
        task.add_done_callback(self._cold_tasks.discard)

    def _start_tts_pump(self, text: str) -> tuple[asyncio.Task, asyncio.Queue]:
        """Kick off synthesis of one sentence NOW; chunks land in a queue the
        caller drains in order. Lets sentence N+1 synthesise while N streams."""
        q: asyncio.Queue = asyncio.Queue()

        async def run() -> None:
            try:
                async for chunk in self._tts.synthesize_stream(text):
                    await q.put(chunk)
            except Exception as exc:  # noqa: BLE001 - surfaced on the consumer side
                q.put_nowait(exc)
            finally:
                q.put_nowait(None)

        return asyncio.create_task(run()), q

    async def _drain_pumps(
        self, pumps: deque, meta: dict, *, block: bool, only_head: bool = False
    ) -> AsyncIterator[bytes]:
        """Yield PCM from the pump queue heads, strictly in sentence order.
        Non-blocking mode stops at the first empty queue (used between LLM
        deltas); blocking mode drains to the end (or just the head sentence)."""
        while pumps:
            _task, q = pumps[0]
            while True:
                if block:
                    item = await q.get()
                else:
                    try:
                        item = q.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                if item is None:
                    break
                if isinstance(item, Exception):
                    pumps.popleft()
                    raise item
                if meta.get("first") is None:
                    meta["first"] = time.perf_counter()
                meta["bytes"] = meta.get("bytes", 0) + len(item)
                yield item
            pumps.popleft()
            if only_head:
                return

    async def _run_tool_loop(  # noqa: ANN001
        self, messages, model, trace, tool_schemas, result, *, speak: bool = False
    ):
        """Tool-aware completion: let the model call gated tools, feed results
        back, repeat until it answers. Sets `result.raw` to the text spoken so far
        (safe to read after a barge-in). While a slow/announced tool runs it yields
        a soft heartbeat pulse into the audio stream (instant local tools stay
        silent). When the gateway supports streaming (GATEWAY_STREAM), each round
        streams and — with `speak` on — the answer is segmented into sentences and
        synthesised as it arrives, so speech starts on sentence one instead of
        after the full completion. Each tool is capability-checked and
        hard-timeout-bounded; a tool error is fed back rather than breaking the
        turn."""
        t0 = time.perf_counter()
        n_tools = 0
        usage: dict = {}
        gateway = self._gateway_for()
        use_stream = self._cfg.gateway.stream and hasattr(gateway, "stream_with_tools")
        first_tok: float | None = None
        spoken: list[str] = []  # sentences streamed for speech, across rounds
        steering: str | None = None
        pumps: deque = deque()  # in-flight sentence TTS: (task, queue)
        tts_meta: dict = {"first": None, "bytes": 0, "chars": 0, "t0": None}

        def _stage_text(sent: str) -> str:
            nonlocal steering
            if steering is None:  # capture the leading directive once
                steering, sent = _extract_steering(sent)
            text = self._clean_reply(sent)  # never speak control markers
            if not text:
                return ""
            return f"{steering} {text}" if steering else text

        async def emit(sent: str):  # yields PCM for one completed sentence
            spoken.append(sent)
            result.raw = " ".join(spoken)
            if not (speak and self._tts is not None):
                return
            text = _stage_text(sent)
            if not text:
                return
            if tts_meta["t0"] is None:
                tts_meta["t0"] = time.perf_counter()
            tts_meta["chars"] += len(text)
            while len(pumps) >= 3:  # cap in-flight TTS requests
                async for pcm in self._drain_pumps(pumps, tts_meta, block=True, only_head=True):
                    yield pcm
            pumps.append(self._start_tts_pump(text))
            async for pcm in self._drain_pumps(pumps, tts_meta, block=False):
                yield pcm

        async def stream_round(tools, calls: list, parts: list):  # yields PCM
            nonlocal first_tok
            buf = ""
            async for delta in gateway.stream_with_tools(
                messages, model=model, tools=tools, usage_out=usage, tool_calls_out=calls
            ):
                if first_tok is None:
                    first_tok = time.perf_counter()
                parts.append(delta)
                if calls:
                    continue  # a tool call started — this text is loop-internal now
                buf += delta
                while True:
                    split = _next_sentence(buf)
                    if split is None:
                        break
                    sent, buf = split
                    if sent.strip():
                        async for pcm in emit(sent):
                            yield pcm
                async for pcm in self._drain_pumps(pumps, tts_meta, block=False):
                    yield pcm
            if not calls and buf.strip():
                async for pcm in emit(buf.strip()):
                    yield pcm
            # Speech settles before tools run / before the turn ends.
            async for pcm in self._drain_pumps(pumps, tts_meta, block=True):
                yield pcm

        try:
            for round_no in range(max(1, self._cfg.tools.max_rounds) + 1):
                final_round = round_no >= max(1, self._cfg.tools.max_rounds)
                # Out of tool rounds — force a final answer, no further tool calls.
                tools = None if final_round else tool_schemas or None
                calls: list[dict] = []
                parts: list[str] = []
                if use_stream:
                    async for pcm in stream_round(tools, calls, parts):
                        yield pcm
                    content = "".join(parts)
                else:
                    msg = await gateway.complete_with_tools(
                        messages, model=model, tools=tools, usage_out=usage
                    )
                    content = msg.content or ""
                    calls = [
                        {"id": tc.id, "name": tc.function.name, "arguments": tc.function.arguments}
                        for tc in (msg.tool_calls or [])
                    ]
                if not calls or final_round:
                    if not use_stream:
                        result.raw = content
                        if speak and self._tts is not None:
                            text = self._clean_reply(content)
                            if text:
                                async for pcm in self._tts_source(text, trace):
                                    yield pcm
                    elif not spoken:
                        result.raw = content  # e.g. an all-whitespace stream
                    self._record_llm(trace, t0, model, result.raw, n_tools, usage, first_tok)
                    self._record_tts(trace, tts_meta, t0, first_tok)
                    return
                assistant_msg = {
                    "role": "assistant",
                    "content": content,
                    "tool_calls": [
                        {
                            "id": c["id"],
                            "type": "function",
                            "function": {"name": c["name"], "arguments": c["arguments"]},
                        }
                        for c in calls
                    ],
                }
                messages.append(assistant_msg)
                result.tool_messages.append(assistant_msg)  # carry into history
                # Escalate to the strong model once real work (tools) is underway: it
                # reasons over tool results — a browsed departure board, search hits —
                # and writes the final answer far more reliably than the fast model,
                # which tends to fumble multi-step browsing and answer from stale
                # snippets. Plain no-tool chat stays on fast (this code only runs when
                # the model chose to call a tool).
                fast_tool_routes = {
                    self._cfg.gateway.fast_model,
                    self._cfg.gateway.voice_model or self._cfg.gateway.fast_model,
                }
                if (
                    model in fast_tool_routes
                    and model != self._cfg.gateway.strong_model
                    and model != self._cfg.gateway.vision_model
                ):
                    model = self._cfg.gateway.strong_model
                for c in calls:
                    tc = SimpleNamespace(
                        id=c["id"],
                        function=SimpleNamespace(name=c["name"], arguments=c["arguments"]),
                    )
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
                    self._log_tool_call(tool, tc, tool_result)
                    if trace is not None:
                        trace.event("tool", tool=tc.function.name)
                    if tool is not None and tool.produces_image and not tool_result.startswith("error"):
                        # Native vision: the tool returned a base64 image. Acknowledge the
                        # tool call as text, then hand the image to the model as a user
                        # message so it can SEE it, and switch to the vision route. The
                        # image is NOT carried into long-term history (it's large).
                        ack = {"role": "tool", "tool_call_id": tc.id, "content": "(image captured — image below)"}
                        messages.append(ack)
                        result.tool_messages.append(ack)
                        messages.append({
                            "role": "user",
                            "content": [
                                {"type": "text", "text": "This is the captured image:"},
                                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{tool_result}"}},
                            ],
                        })
                        model = self._cfg.gateway.vision_model or model
                    else:
                        tool_msg = {"role": "tool", "tool_call_id": tc.id, "content": tool_result}
                        messages.append(tool_msg)
                        result.tool_messages.append(tool_msg)  # carry into history
        finally:
            for task, _q in pumps:  # barge-in / error: kill in-flight synthesis
                task.cancel()

    @staticmethod
    def _record_llm(  # noqa: ANN001
        trace, t0: float, model: str, content: str, n_tools: int,
        usage: dict | None = None, first_tok: float | None = None,
    ) -> None:
        if trace is not None:
            extra = {}
            if usage:  # prompt-cache visibility (§9)
                extra = {k: usage[k] for k in ("prompt_tokens", "cached_tokens") if k in usage}
            if first_tok is not None:
                extra["ttft_ms"] = round((first_tok - t0) * 1000, 1)
            trace.stage(
                "llm",
                (time.perf_counter() - t0) * 1000,
                model=model,
                chars=len(content),
                tools=n_tools,
                **extra,
            )

    def _record_tts(self, trace, meta: dict, t0: float, first_tok: float | None) -> None:  # noqa: ANN001
        """One aggregate TTS stage for the sentence-streamed reply (mirrors what
        _tts_source records for a single synthesis call)."""
        if trace is None or not meta.get("bytes"):
            return
        first = meta.get("first")
        trace.stage(
            "tts",
            (time.perf_counter() - (first_tok or meta.get("t0") or t0)) * 1000,
            ttfa_ms=round((first - t0) * 1000, 1) if first else None,
            bytes=meta.get("bytes", 0),
            chars=meta.get("chars", 0),
            voice=self._cfg.tts.voice,
            provider=self._cfg.tts.provider,
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

    def _log_offered(self, available: list, offered: list) -> None:
        """When the relevance prefilter narrowed the tool list, note what was offered
        this turn (which MCP servers made the cut) — visible debugging of §9."""
        if not (self._cfg.tools.log_calls and self._cfg.tools.relevance_filter):
            return
        if len(offered) == len(available):
            return  # nothing trimmed
        servers = ", ".join(offered_servers(offered)) or "none"
        print(f"  ⚙ tools: offered {len(offered)}/{len(available)} (mcp: {servers})")

    def _log_tool_call(self, tool, tc, result: str) -> None:  # noqa: ANN001
        """One console line per tool call — name, the capability that gated it
        (`mcp.<server>` for bridged MCP tools), the args, and a short result — so a
        turn's tool/MCP activity is visible when debugging. Gated by tools.log_calls.
        (Skills, when added in §7, compose these tools and will show the same way.)"""
        if not self._cfg.tools.log_calls:
            return
        cap = tool.required_capability if tool is not None else "ungated?"
        args = " ".join((tc.function.arguments or "").split())
        if len(args) > 120:
            args = args[:119] + "…"
        if tool is not None and tool.produces_image and not (result or "").startswith("error"):
            out = f"(image, {len(result)} b64 chars → sent to vision)"
        else:
            out = " ".join((result or "").split())
        errored = out[:6].lower().startswith("error")
        if len(out) > 160:
            out = out[:159] + "…"
        print(f"  ⚙ tool: {tc.function.name}  [{cap}]  {args}".rstrip())
        print(f"    {'✗' if errored else '↳'} {out}")

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
        spoken reply. Big tool results are truncated on the way in: the model saw
        the full output THIS turn; later turns only need the gist, not a browsed
        page re-billed into every prompt."""
        if not (result.reply or result.tool_messages):
            return
        self._history.append({"role": "user", "content": user_text})
        self._history.extend(self._compact_tool_message(m) for m in result.tool_messages)
        if result.reply:
            self._history.append({"role": "assistant", "content": result.reply})
        self._trim_history()

    def _compact_tool_message(self, msg: dict) -> dict:
        limit = max(0, self._cfg.persona.history_tool_result_chars)
        if not limit or msg.get("role") != "tool":
            return msg
        content = str(msg.get("content") or "")
        if len(content) <= limit:
            return msg
        return {**msg, "content": content[:limit] + f"… [truncated {len(content) - limit} chars]"}

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
            await self._memory.write_turn(user_text, assistant_text, user=self._memory_user)
            refreshed = await self._memory.refresh_cache(
                min_interval_s=self._cfg.memory.refresh_interval_s, user=self._memory_user
            )
            if refreshed:
                ms = (time.perf_counter() - t0) * 1000
                mt = self._tracer.turn(
                    room=self._cfg.gateway.room,
                    speaker=self._ctx.identity,
                    channel=self._ctx.channel,
                    device_id=self._ctx.device_id,
                    kind="memory",
                )
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

