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
import contextlib
import queue
import shlex
import socket
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import websockets
from jarvis.brain.voice_modes import DEFAULT_MODE, STAY_MODE, normalize_mode
from jarvis.config import Config
from jarvis.device_diagnostics import (
    check_tcp_port,
    get_ip_address,
    host_arg,
    int_arg,
    ping_host,
    resolve_dns,
    run_self_diagnostics,
)
from jarvis.intercom.audio import AudioIO, MicStream
from jarvis.intercom.hardware import IntercomHardware
from jarvis.intercom.metrics import IntercomReplyMetrics
from jarvis.intercom.pi_panel import CompositePanel, PiPanel, WebPiPanel
from jarvis.intercom.vad import Endpointer, SileroVAD
from jarvis.intercom.wake import WakeWord
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
    Proactive,
    REPLY_AUDIO_BINARY_V1,
    ReplyEnd,
    ReplyText,
    Transcript,
    UPLINK_AUDIO_BINARY_V1,
    Welcome,
    decode_binary_audio,
    decode,
    encode_uplink_audio_binary,
    encode,
)

FRAME_SAMPLES = 512


@dataclass
class _CapturedUtterance:
    turn_id: str
    pcm: bytes
    metrics: IntercomReplyMetrics
    capture_ms: float
    audio_ms: float
    uplink_bytes: int
    uplink_chunks: int


def _is_passive_proactive_state(state: dict | None) -> bool:
    """Default proactive ReplyEnd metadata should not close an active conversation."""
    return bool(
        state
        and state.get("ended") is False
        and state.get("continue_listening") is False
        and state.get("voice_mode") in {DEFAULT_MODE, STAY_MODE}
        and not state.get("close_reason")
    )


def _is_passive_proactive_end(turn_id: str, msg: ReplyEnd) -> bool:
    """Passive proactive ReplyEnd defaults should not overwrite selected voice mode."""
    return (
        turn_id.startswith("pa-")
        and msg.ended is False
        and msg.continue_listening is False
        and msg.voice_mode == DEFAULT_MODE
        and not msg.close_reason
    )


class IntercomClient:
    def __init__(
        self,
        cfg: Config,
        *,
        audio: AudioIO,
        vad: SileroVAD,
        wake: WakeWord,
        hardware: IntercomHardware | None = None,
        panel: PiPanel | None = None,
    ) -> None:
        self._cfg = cfg
        self._audio = audio
        self._vad = vad
        self._wake = wake
        self._hardware = hardware or IntercomHardware(cfg.intercom_device)
        self._panel = panel or CompositePanel(
            PiPanel(cfg.intercom_device, hardware=self._hardware),
            WebPiPanel(cfg.intercom_device),
        )
        self._sr = cfg.audio.sample_rate
        self._device_id = cfg.capabilities.device_id
        self._active_voice_mode = DEFAULT_MODE

    async def run(self) -> None:
        print("Loading models…")
        self._vad.load()
        self._wake.load()
        self._panel.start()
        self._publish_voice_mode()
        url = self._cfg.intercom.brain_url
        hardware = self._hardware.capabilities()
        if hardware:
            print(f"Local intercom hardware: {', '.join(hardware)}")
        print(f"Connecting to brain at {url}…")
        # Models + mic are loaded once and kept open across brain reconnects: a brain
        # restart must not drop the voice device or re-load Whisper/wake every time.
        try:
            with MicStream(
                self._cfg.audio, sample_rate=self._sr, frame_samples=FRAME_SAMPLES
            ) as mic:
                phrase = self._cfg.wake.keyword.replace("_", " ").title()
                while True:  # reconnect loop — survive brain restarts/outages
                    try:
                        await self._prepare_brain_connect_attempt()
                        async with websockets.connect(
                            url,
                            max_size=self._cfg.intercom.websocket_max_size,
                            ping_interval=self._cfg.intercom.websocket_ping_interval_s,
                            ping_timeout=self._cfg.intercom.websocket_ping_timeout_s,
                            open_timeout=self._cfg.intercom.websocket_open_timeout_s,
                            close_timeout=self._cfg.intercom.websocket_close_timeout_s,
                        ) as ws:
                            await ws.send(
                                encode(
                                    Hello(
                                        device_id=self._device_id,
                                        token=self._cfg.intercom.token.get_secret_value(),
                                        hardware=hardware,
                                    )
                                )
                            )
                            welcome = decode(await ws.recv())
                            if not isinstance(welcome, Welcome):
                                print(f"pairing rejected: {welcome}; retrying in 5s…")
                                self._panel.set("disconnected")
                                await asyncio.sleep(5)
                                continue
                            print(f"Paired with brain. Capabilities: {welcome.capabilities}")
                            print(f"Audio downlink: {REPLY_AUDIO_BINARY_V1}.")
                            print(f'\nJarvis is listening. Say "{phrase}".')
                            # One task reads the socket for the whole connection and queues
                            # every message; the turn flow and the idle wait both consume from
                            # it. This is what lets a proactive push (alarm/notification)
                            # arrive while idle. Race it against the turn loop so a dropped
                            # socket is noticed promptly, not only on the next send.
                            inbound: asyncio.Queue = asyncio.Queue()
                            router = asyncio.create_task(self._router(ws, inbound))
                            turns = asyncio.create_task(self._turn_forever(ws, mic, inbound))
                            try:
                                done, _ = await asyncio.wait(
                                    {router, turns}, return_when=asyncio.FIRST_COMPLETED
                                )
                                for t in done:  # surface a real (non-link) error
                                    exc = t.exception()
                                    if exc and not isinstance(
                                        exc, (OSError, websockets.exceptions.WebSocketException)
                                    ):
                                        raise exc
                                    if exc:
                                        print(f"  [intercom] brain link ended: {exc!r}")
                            finally:
                                for t in (router, turns):
                                    t.cancel()
                                    with contextlib.suppress(asyncio.CancelledError, Exception):
                                        await t
                        await self._set_link_lost_state()
                        print("  [intercom] brain link closed; reconnecting in 3s…")
                        await self._recover_network_if_needed()
                        await asyncio.sleep(3)
                    except (OSError, TimeoutError, websockets.exceptions.WebSocketException) as exc:
                        await self._set_link_lost_state()
                        print(f"  [intercom] brain link lost ({type(exc).__name__}); reconnecting in 3s…")
                        await self._recover_network_if_needed()
                        await asyncio.sleep(3)
        finally:
            self._panel.stop()
            self._wake.delete()

    async def _recover_network_if_needed(self) -> None:
        """Best-effort local nudge for Pi installs whose WiFi stack stopped retrying.

        The installer adds /usr/local/bin/jarvis-network-recover when NetworkManager
        is present. Calling it here is harmless on non-Pi hosts and keeps recovery
        close to the symptom: the intercom just lost the brain.
        """
        if self._cfg.intercom.network_recover_cmd.lower() in {"", "none", "false", "0"}:
            return
        if await asyncio.to_thread(self._brain_port_open):
            return
        argv = shlex.split(self._cfg.intercom.network_recover_cmd)
        if not argv:
            return
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(proc.wait(), timeout=self._cfg.intercom.network_recover_timeout_s)

    def _brain_port_open(self) -> bool:
        try:
            with socket.create_connection(
                (self._cfg.intercom.brain_host, self._cfg.intercom.brain_port), timeout=2.0
            ):
                return True
        except OSError:
            return False

    async def _prepare_brain_connect_attempt(self) -> None:
        if await self._ensure_network_ready():
            self._panel.set("connecting")

    async def _ensure_network_ready(self) -> bool:
        if await asyncio.to_thread(self._network_online):
            return True
        self._panel.set("network")
        print("  [intercom] network probe failed; attempting brain reconnect anyway…")
        await self._recover_network_if_needed()
        return False

    async def _set_link_lost_state(self) -> None:
        if await asyncio.to_thread(self._network_online):
            self._panel.set("disconnected")
        else:
            self._panel.set("network")

    def _network_online(self) -> bool:
        host = self._cfg.intercom.network_probe_host.strip()
        if host.lower() in {"", "none", "false", "0"}:
            return True
        try:
            with socket.create_connection(
                (
                    host,
                    self._cfg.intercom.network_probe_port,
                ),
                timeout=max(0.1, self._cfg.intercom.network_probe_timeout_s),
            ):
                return True
        except OSError:
            return False

    async def _turn_forever(self, ws, mic, inbound: asyncio.Queue) -> None:  # noqa: ANN001
        """Run turns for the life of one brain connection (idle → wake → turn, repeat)."""
        while True:
            await self._idle_then_turn(ws, mic, inbound)

    async def _router(self, ws, inbound: asyncio.Queue) -> None:  # noqa: ANN001
        """Read the socket for the connection's life; queue every decoded message."""
        try:
            async for raw in ws:
                with contextlib.suppress(Exception):
                    if isinstance(raw, bytes):
                        binary = decode_binary_audio(raw)
                        if binary is not None:
                            if binary.kind == "reply_audio":
                                inbound.put_nowait(binary)
                            continue
                        continue
                    msg = decode(raw)
                    if isinstance(msg, DeviceRequest):
                        asyncio.create_task(self._handle_device_request(ws, msg))
                    else:
                        inbound.put_nowait(msg)
        except Exception as exc:  # noqa: BLE001 - socket closed
            print(f"  [intercom] router stopped: {exc!r}")
            raise

    async def _handle_device_request(self, ws, msg: DeviceRequest) -> None:  # noqa: ANN001
        try:
            if msg.action == "control_display":
                result = await asyncio.to_thread(
                    self._panel.control, str(msg.args.get("action") or "status")
                )
            elif msg.action in {
                "run_self_diagnostics",
                "get_ip_address",
                "ping_host",
                "resolve_dns",
                "check_tcp_port",
            }:
                result = await self._handle_diagnostic_request(msg.action, msg.args)
            else:
                result = await self._hardware.handle(msg.action, msg.args)
            resp = DeviceResponse(request_id=msg.request_id, ok=True, result=result)
        except Exception as exc:  # noqa: BLE001 - return failure over protocol
            resp = DeviceResponse(request_id=msg.request_id, ok=False, error=str(exc))
        with contextlib.suppress(Exception):
            await ws.send(encode(resp))

    async def _handle_diagnostic_request(self, action: str, args: dict) -> dict[str, str]:
        tools = self._cfg.tools
        timeout = tools.self_diagnostic_timeout_s
        if action == "run_self_diagnostics":
            text = await run_self_diagnostics(
                request_device_id=self._device_id,
                configured_device_id=self._device_id,
                timeout_s=timeout,
                max_bytes=tools.self_max_bytes,
            )
        elif action == "get_ip_address":
            text = await get_ip_address(
                include_public=bool(args.get("include_public", True)),
                timeout_s=min(max(timeout, 0.5), 3.0),
            )
        elif action == "ping_host":
            host = host_arg(args.get("host"))
            count = int_arg(args.get("count"), default=4, min_value=1, max_value=10)
            text = await ping_host(
                host=host,
                count=count,
                timeout_s=max(timeout * count, timeout + 2.0),
                max_bytes=tools.self_max_bytes,
            )
        elif action == "resolve_dns":
            text = await resolve_dns(host=host_arg(args.get("host")))
        elif action == "check_tcp_port":
            host = host_arg(args.get("host"))
            port = int_arg(args.get("port"), default=443, min_value=1, max_value=65535)
            text = await check_tcp_port(host=host, port=port, timeout_s=min(max(timeout, 0.5), 5.0))
        else:
            raise ValueError(f"unsupported diagnostic action {action!r}")
        return {"text": text}

    async def _idle_then_turn(self, ws, mic: MicStream, inbound: asyncio.Queue) -> None:  # noqa: ANN001
        """Wait for the wake word OR a proactive push (alarm/notification); whichever
        comes first. A proactive plays (and may open the mic for a reply); a wake starts
        a normal turn."""
        mic.drain()
        self._wake.reset()
        print('● idle — say "Hey Jarvis"')
        self._panel.set("idle")
        while True:
            self._sync_panel_voice_mode()
            pro = self._take_proactive(inbound)
            if pro is not None:
                await self._play_proactive(ws, mic, inbound, pro)
                mic.drain()
                self._wake.reset()
                print('● idle — say "Hey Jarvis"')
                continue
            if await asyncio.to_thread(self._wake_batch, mic):
                break
        print("● wake")
        self._panel.set("awake")
        await self._acknowledge()
        mic.drain()
        print("  listening…")
        self._panel.set("listening")
        captured = await self._capture_streaming_utterance(ws, mic)
        self._panel.set("thinking")
        await self._converse(ws, mic, inbound, captured)

    async def _converse(  # noqa: ANN001
        self,
        ws,
        mic: MicStream,
        inbound: asyncio.Queue,
        captured: _CapturedUtterance | None,
    ) -> dict | None:
        """Run turns until the conversation closes — shared by a wake-started turn and a
        proactive that opened the mic."""
        conversation_started = False
        active_voice_mode = self._sync_panel_voice_mode()
        while True:
            active_voice_mode = self._sync_panel_voice_mode()
            if captured is None:
                print("  (nothing said)")
                if conversation_started and active_voice_mode == STAY_MODE:
                    self._panel.set("listening")
                    captured = await self._capture_streaming_utterance(ws, mic)
                    self._panel.set("thinking")
                    continue
                if conversation_started:
                    with contextlib.suppress(Exception):
                        await ws.send(encode(ConversationIdle(reason="timeout")))
                return None
            turn_id = captured.turn_id
            metrics = captured.metrics
            conversation_started = True
            state = {
                "ended": False,
                "text": "",
                "continue_listening": active_voice_mode == STAY_MODE,
                "voice_mode": active_voice_mode,
                "close_reason": "",
            }
            interrupted = await self._play_reply(
                ws, mic, inbound, turn_id, state, metrics
            )
            active_voice_mode = self._set_active_voice_mode(state["voice_mode"])
            print(f"  jarvis: {state['text']}{'  ⏹' if state['ended'] else ''}")

            if interrupted:
                print("⊘ interrupted — listening…")
                self._panel.set("listening")
                captured = await self._capture_streaming_utterance(ws, mic)
                self._panel.set("thinking")
                continue
            if state["ended"]:
                print('  …(conversation closed — say "Hey Jarvis")')
                return state
            if not self._cfg.vad.conversation_mode or not state["continue_listening"]:
                return state
            mic.drain()
            while True:
                proactive_state = await self._play_queued_proactive(ws, mic, inbound, state)
                if proactive_state is not None:
                    state.update(proactive_state)
                    active_voice_mode = self._set_active_voice_mode(state["voice_mode"])
                    if state["ended"] or not state["continue_listening"]:
                        return state
                    continue
                active_voice_mode = self._sync_panel_voice_mode()
                state["voice_mode"] = active_voice_mode
                state["continue_listening"] = state["continue_listening"] or active_voice_mode == STAY_MODE
                if state["voice_mode"] == STAY_MODE:
                    print("  …(stay mode — listening)")
                else:
                    print("  …(listening — keep talking, or stay quiet to sleep)")
                self._panel.set("listening")
                captured = await self._capture_streaming_utterance(
                    ws, mic, initial_wait_ms=self._cfg.vad.conversation_timeout_ms
                )
                self._panel.set("thinking")
                if captured is not None:
                    break
                if state["voice_mode"] == STAY_MODE:
                    continue
                with contextlib.suppress(Exception):
                    await ws.send(encode(ConversationIdle(reason="timeout")))
                return state

    async def _play_queued_proactive(  # noqa: ANN001
        self,
        ws,
        mic: MicStream,
        inbound: asyncio.Queue,
        active_state: dict | None = None,
    ) -> dict | None:
        pro = self._take_proactive(inbound)
        if pro is None:
            return None
        state = await self._play_proactive(ws, mic, inbound, pro)
        mic.drain()
        if active_state and _is_passive_proactive_state(state):
            return dict(active_state)
        return state

    def _take_proactive(self, inbound: asyncio.Queue):  # noqa: ANN202
        """Non-blocking: a Proactive at the head of the queue, else None. Stray
        non-proactive frames while idle (rare) are dropped."""
        try:
            msg = inbound.get_nowait()
        except asyncio.QueueEmpty:
            return None
        return msg if isinstance(msg, Proactive) else None

    def _wake_batch(self, mic: MicStream, max_ms: float = 300.0) -> bool:
        """Process up to ~max_ms of mic frames for the wake word; return True if heard.
        Short batches so the idle loop can also check for proactive pushes."""
        frame_ms = FRAME_SAMPLES / self._sr * 1000.0
        backlog_frames = int(1.5 * self._sr / FRAME_SAMPLES)
        elapsed = 0.0
        while elapsed < max_ms:
            if mic.qsize() > backlog_frames:
                mic.drain()
                self._wake.reset()
                return False
            try:
                frame = mic.read(timeout=0.1)
            except queue.Empty:
                return False
            elapsed += frame_ms
            if self._wake.process(frame):
                return True
        return False

    async def _play_proactive(self, ws, mic: MicStream, inbound: asyncio.Queue, pro: Proactive) -> dict:  # noqa: ANN001
        """Play a proactive's audio (tone + spoken text under its 'pa-' turn id); if it
        asked to open the mic, listen for a reply and carry it into a chat."""
        print(f"  🔔 {pro.text}")
        self._panel.set("awake")
        state = {
            "ended": False,
            "text": "",
            "continue_listening": self._active_voice_mode == STAY_MODE,
            "voice_mode": self._active_voice_mode,
            "close_reason": "",
        }
        metrics = IntercomReplyMetrics(
            turn_id=pro.turn_id, device_id=self._device_id, kind="proactive"
        )
        metrics.mark_proactive_received(text_chars=len(pro.text))
        with contextlib.suppress(Exception):
            self._panel.set("speaking")
            playback = await self._audio.play_stream(
                self._reply_audio(inbound, pro.turn_id, state, metrics),
                sample_rate=self._cfg.tts.sample_rate,
            )
            metrics.attach_playback(playback)
            metrics.emit(self._cfg.trace)
        if pro.open_mic:
            mic.drain()
            print("  …(listening for your reply)")
            self._panel.set("listening")
            captured = await self._capture_streaming_utterance(
                ws, mic, initial_wait_ms=self._cfg.vad.conversation_timeout_ms
            )
            if captured is not None:
                self._panel.set("thinking")
                nested_state = await self._converse(ws, mic, inbound, captured)
                if nested_state is not None:
                    self._set_active_voice_mode(nested_state.get("voice_mode", self._active_voice_mode))
                    return nested_state
        self._set_active_voice_mode(state.get("voice_mode", self._active_voice_mode))
        return state

    # --- reply playback + barge-in -----------------------------------------
    async def _reply_audio(self, inbound, turn_id, state, metrics=None):  # noqa: ANN001
        """Yield reply PCM (from the router queue) until ReplyEnd; record text/ended.
        Used for both a normal turn and a proactive's audio (its 'pa-' turn id)."""
        while True:
            msg = await inbound.get()
            if isinstance(msg, Transcript) and msg.turn_id == turn_id:
                print(f"  you: {msg.text!r}")
                if metrics is not None:
                    metrics.mark_transcript()
            elif isinstance(msg, BinaryAudio) and msg.turn_id == turn_id:
                if metrics is not None:
                    metrics.record_audio_frame(
                        protocol=REPLY_AUDIO_BINARY_V1,
                        encoded_bytes=len(msg.pcm),
                        pcm_bytes=len(msg.pcm),
                    )
                yield msg.pcm
            elif isinstance(msg, ReplyText) and msg.turn_id == turn_id:
                state["text"] = msg.text
            elif isinstance(msg, ReplyEnd) and msg.turn_id == turn_id:
                voice_mode = (
                    state.get("voice_mode", self._active_voice_mode)
                    if _is_passive_proactive_end(turn_id, msg)
                    else msg.voice_mode
                )
                state["ended"] = msg.ended
                state["continue_listening"] = msg.continue_listening
                state["voice_mode"] = voice_mode
                state["close_reason"] = msg.close_reason
                if voice_mode == msg.voice_mode:
                    self._set_active_voice_mode(msg.voice_mode)
                if (
                    metrics is not None
                    and state.get("text")
                    and metrics.reply_audio_chunks == 0
                ):
                    metrics.mark_missing_reply_audio(text_chars=len(state["text"]))
                    print(
                        "  [intercom] reply ended without any audio frames "
                        f"(turn={turn_id})"
                    )
                return
            elif isinstance(msg, Cancel) and msg.turn_id == turn_id:
                return
            # Frames for another turn id (e.g. a proactive arriving mid-turn) are
            # dropped for now; idle-aware queuing is the next layer (#3).

    async def _play_reply(
        self,
        ws,
        mic: MicStream,
        inbound,
        turn_id: str,
        state: dict,
        metrics: IntercomReplyMetrics | None = None,
    ) -> bool:  # noqa: ANN001
        if not self._cfg.vad.bargein_enabled:
            self._panel.set("speaking")
            playback = await self._audio.play_stream(
                self._reply_audio(inbound, turn_id, state, metrics),
                sample_rate=self._cfg.tts.sample_rate,
            )
            if metrics is not None:
                metrics.attach_playback(playback)
                metrics.emit(self._cfg.trace)
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
        self._panel.set("speaking")
        play_task = asyncio.create_task(
            self._audio.play_stream(
                self._reply_audio(inbound, turn_id, state, metrics),
                sample_rate=self._cfg.tts.sample_rate,
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
                if not isinstance(r, BaseException):
                    if metrics is not None and hasattr(r, "as_dict"):
                        metrics.attach_playback(r)
                    continue
                if not isinstance(r, asyncio.CancelledError):
                    print(f"  [reply error] {r!r}")
            if metrics is not None:
                metrics.emit(self._cfg.trace)
        return interrupted.is_set()

    # --- local capture (parallel to TurnLoop) ------------------------------
    async def _capture_streaming_utterance(
        self, ws, mic: MicStream, *, initial_wait_ms: float = 8000  # noqa: ANN001
    ) -> _CapturedUtterance | None:
        turn_id = uuid.uuid4().hex
        metrics = IntercomReplyMetrics(
            turn_id=turn_id, device_id=self._device_id, kind="turn"
        )
        loop = asyncio.get_running_loop()
        out: asyncio.Queue[bytes | None] = asyncio.Queue()
        result: dict = {}
        stop_capture = threading.Event()

        def capture() -> None:
            try:
                result.update(
                    self._capture_utterance_to_queue(
                        mic,
                        lambda chunk: loop.call_soon_threadsafe(out.put_nowait, chunk),
                        stop_capture=stop_capture,
                        initial_wait_ms=initial_wait_ms,
                    )
                )
            finally:
                loop.call_soon_threadsafe(out.put_nowait, None)

        worker = asyncio.create_task(asyncio.to_thread(capture))
        started = False
        uplink_bytes = 0
        uplink_chunks = 0
        try:
            while True:
                item = await out.get()
                if item is None:
                    break
                if not started:
                    await ws.send(
                        encode(
                            AudioStart(
                                turn_id=turn_id,
                                sample_rate=self._sr,
                                voice_mode=self._active_voice_mode,
                            )
                        )
                    )
                    started = True
                frame = encode_uplink_audio_binary(turn_id, self._sr, item)
                uplink_bytes += len(frame)
                uplink_chunks += 1
                await ws.send(frame)
            await worker
        except BaseException:
            stop_capture.set()
            with contextlib.suppress(BaseException):
                await asyncio.wait_for(worker, timeout=1.0)
            raise
        pcm = result.get("pcm", b"")
        if not pcm:
            return None
        if started:
            await ws.send(encode(AudioEnd(turn_id=turn_id)))
        capture_ms = float(result.get("capture_ms", 0.0))
        audio_ms = len(pcm) / 2 / self._sr * 1000
        metrics.mark_capture(
            capture_ms=capture_ms,
            audio_ms=audio_ms,
            pcm_bytes=len(pcm),
            streamed=True,
        )
        metrics.mark_utterance_sent(
            pcm_bytes=len(pcm),
            frame_bytes=uplink_bytes,
            protocol=UPLINK_AUDIO_BINARY_V1,
            chunks=uplink_chunks,
        )
        return _CapturedUtterance(
            turn_id=turn_id,
            pcm=pcm,
            metrics=metrics,
            capture_ms=capture_ms,
            audio_ms=audio_ms,
            uplink_bytes=uplink_bytes,
            uplink_chunks=uplink_chunks,
        )

    def _capture_utterance_to_queue(
        self,
        mic: MicStream,
        send_chunk: Callable[[bytes], None],
        *,
        stop_capture: threading.Event | None = None,
        initial_wait_ms: float = 8000,
    ) -> dict:
        t0 = time.perf_counter()
        frame_ms = FRAME_SAMPLES / self._sr * 1000.0
        self._vad.reset()
        ep = Endpointer(
            frame_ms=frame_ms,
            endpoint_silence_ms=self._cfg.vad.endpoint_silence_ms,
            speech_threshold=self._cfg.vad.speech_threshold,
            min_speech_ms=self._cfg.vad.min_speech_ms,
        )
        waited_ms = 0.0
        sent_len = 0
        stop_capture = stop_capture or threading.Event()
        while True:
            if stop_capture.is_set():
                return {
                    "pcm": ep.audio if ep.started else b"",
                    "capture_ms": (time.perf_counter() - t0) * 1000,
                }
            try:
                frame = mic.read(timeout=0.1)
            except queue.Empty:
                continue
            done = ep.feed(frame, self._vad.prob(frame))
            if ep.started:
                audio = ep.audio
                chunk = audio[sent_len:]
                sent_len = len(audio)
                if chunk:
                    send_chunk(chunk)
            else:
                waited_ms += frame_ms
                if waited_ms >= initial_wait_ms:
                    return {"pcm": b"", "capture_ms": (time.perf_counter() - t0) * 1000}
            if done or len(ep.audio) / 2 / self._sr >= 30.0:
                return {"pcm": ep.audio, "capture_ms": (time.perf_counter() - t0) * 1000}

    async def _acknowledge(self) -> None:
        # Local wake confirmation only — the intercom has no TTS, so "speak" mode
        # degrades to a beep here.
        if self._cfg.audio.ack_mode == "none":
            return
        await asyncio.to_thread(self._audio.play_tone)

    def _sync_panel_voice_mode(self) -> str:
        while True:
            voice_mode = self._panel.take_voice_mode()
            if not voice_mode:
                return self._active_voice_mode
            self._set_active_voice_mode(voice_mode)

    def _set_active_voice_mode(self, voice_mode: str) -> str:
        self._active_voice_mode = normalize_mode(voice_mode)
        self._publish_voice_mode()
        return self._active_voice_mode

    def _publish_voice_mode(self) -> None:
        self._panel.set_voice_mode(self._active_voice_mode)
