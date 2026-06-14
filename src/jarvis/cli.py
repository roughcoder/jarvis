"""Jarvis CLI entrypoint.

Phase 1 / Step 0 provides `jarvis config` — a dry-run that loads everything
from env and prints the resolved configuration (secret-masked), proving the
Step 0 gate: config is env-driven and defaults to localhost.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from jarvis import __version__
from jarvis.config import load_config


def _cmd_config(_args: argparse.Namespace) -> int:
    cfg = load_config()
    resolved = cfg.resolved()
    width = max(len(k) for k in resolved)
    print(f"Jarvis {__version__} — resolved configuration (Phase 1, all-local)\n")
    for key, value in resolved.items():
        print(f"  {key.ljust(width)}  {value}")
    print(
        "\nAll service URLs are env-driven and default to localhost. "
        "Phase 2 migration = change *_HOST env vars only (spec §3.1)."
    )
    return 0


def _cmd_ping_gateway(args: argparse.Namespace) -> int:
    """Step 1 gate: prove fast + strong routes return completions, and that
    switching model is a parameter (same code path, different route name)."""
    from jarvis.brain.gateway_client import GatewayClient

    cfg = load_config()
    prompt = args.prompt
    routes = [
        ("fast", cfg.gateway.fast_model),
        ("strong", cfg.gateway.strong_model),
    ]
    if args.route:  # allow testing an arbitrary route, e.g. strong-openrouter
        routes = [(args.route, args.route)]

    async def run() -> int:
        client = GatewayClient(cfg.gateway)
        try:
            print(f"Gateway: {cfg.gateway.base_url}\n")
            for label, model in routes:
                msgs = [{"role": "user", "content": prompt}]
                try:
                    text = await client.complete(msgs, model=model)
                except Exception as exc:  # noqa: BLE001 - surface provider/proxy errors
                    print(f"  [{label}] route={model!r}  ERROR: {exc}")
                    return 1
                preview = text.strip().replace("\n", " ")
                if len(preview) > 100:
                    preview = preview[:100] + "…"
                print(f"  [{label}] route={model!r}  -> {preview}")
        finally:
            await client.aclose()
        return 0

    return asyncio.run(run())


def _cmd_say(args: argparse.Namespace) -> int:
    """Step 2 gate: speak a string via streaming TTS; playback starts before
    synthesis finishes; stop() cuts within ~100ms."""
    import time

    from jarvis.intercom.audio import AudioIO
    from jarvis.services.tts import InworldTTS

    cfg = load_config()
    tts = InworldTTS(cfg.tts)
    audio = AudioIO(cfg.audio)

    async def run() -> int:
        t0 = time.perf_counter()
        first_audio: float | None = None
        total_bytes = 0

        async def instrumented() -> "any":  # type: ignore[valid-type]
            nonlocal first_audio, total_bytes
            async for chunk in tts.synthesize_stream(args.text, voice=args.voice):
                if first_audio is None:
                    first_audio = time.perf_counter() - t0
                    print(f"  time-to-first-audio: {first_audio * 1000:.0f} ms")
                total_bytes += len(chunk)
                yield chunk

        play_task = asyncio.create_task(
            audio.play_stream(instrumented(), sample_rate=cfg.tts.sample_rate)
        )

        if args.stop_after is not None:
            await asyncio.sleep(args.stop_after)
            t_stop = time.perf_counter()
            audio.stop_playback()
            await play_task
            teardown = (time.perf_counter() - t_stop) * 1000
            cut = audio.last_cut_latency_ms
            if cut is not None:
                print(f"  audible cut: {cut:.0f} ms (target < 100ms)")
            print(f"  full teardown: {teardown:.0f} ms")
        else:
            await play_task

        secs = total_bytes / 2 / cfg.tts.sample_rate  # 16-bit mono
        print(f"  synthesized ~{secs:.1f}s of audio ({total_bytes} bytes)")
        return 0

    try:
        return asyncio.run(run())
    except Exception as exc:  # noqa: BLE001
        print(f"TTS error: {exc}")
        return 1


def _capture_push_to_talk(audio, sample_rate: int, start_prompt: str) -> bytes:  # noqa: ANN001
    """Push-to-talk capture with a single stdin reader (no thread races).

    All ENTER reads happen on THIS thread, sequentially: wait for the start
    ENTER, then wait for the stop ENTER. The recorder — which never touches
    stdin — runs in a background thread in between. Returns 16-bit PCM bytes.
    """
    import threading

    try:
        input(start_prompt)
    except EOFError:
        return b""

    stop = threading.Event()
    holder: dict[str, bytes] = {}

    def _record() -> None:
        holder["pcm"] = audio.record(stop, sample_rate=sample_rate)

    rec = threading.Thread(target=_record, daemon=True)
    rec.start()
    print("🎙  listening… (press ENTER to stop)")
    try:
        input()
    except EOFError:
        pass
    stop.set()
    rec.join()
    return holder.get("pcm", b"")


def _cmd_listen(args: argparse.Namespace) -> int:
    """Step 3 gate: push-to-talk transcription. ENTER to start, ENTER to stop."""
    import time

    from jarvis.intercom.audio import AudioIO
    from jarvis.services.stt import Transcriber

    cfg = load_config()
    audio = AudioIO(cfg.audio)
    stt = Transcriber(cfg.stt)

    print(f"Loading STT model '{cfg.stt.model}' (first run downloads it)…")
    t0 = time.perf_counter()
    stt.load()
    print(f"  model ready in {time.perf_counter() - t0:.1f}s")

    rounds = args.rounds
    for i in range(rounds):
        label = "" if rounds == 1 else f" [{i + 1}/{rounds}]"
        pcm = _capture_push_to_talk(
            audio, cfg.audio.sample_rate, f"\nPress ENTER to start recording{label}…"
        )

        secs = len(pcm) / 2 / cfg.audio.sample_rate
        t1 = time.perf_counter()
        text = stt.transcribe(pcm, sample_rate=cfg.audio.sample_rate)
        dt = time.perf_counter() - t1
        print(f"\n  heard ({secs:.1f}s audio, {dt:.1f}s transcribe):")
        print(f"  → {text!r}")
    return 0


def _cmd_listen_safe(args: argparse.Namespace) -> int:
    try:
        return _cmd_listen(args)
    except KeyboardInterrupt:
        print("\n(interrupted)")
        return 130


_VOICE_SYSTEM_PROMPT = (
    "You are Jarvis, a concise spoken voice assistant. Answer in one or two "
    "short sentences meant to be read aloud. Use plain text only — no markdown, "
    "lists, code blocks, or emoji."
)


def _cmd_chat(args: argparse.Namespace) -> int:
    """Step 4 gate: push-to-talk voice round-trip — speak → STT → gateway LLM
    → streaming TTS. The first 'it talks' milestone."""
    from jarvis.intercom.audio import AudioIO
    from jarvis.brain.gateway_client import GatewayClient
    from jarvis.services.stt import Transcriber
    from jarvis.services.tts import InworldTTS
    from jarvis.intercom.vad import SileroVAD

    cfg = load_config()
    audio = AudioIO(cfg.audio)
    stt = Transcriber(cfg.stt)
    gateway = GatewayClient(cfg.gateway)
    tts = InworldTTS(cfg.tts)
    vad = None if args.manual else SileroVAD(cfg.vad)

    print(f"Loading STT model '{cfg.stt.model}'…")
    stt.load()
    if vad is not None:
        print("Loading Silero VAD…")
        vad.load()

    def capture(prompt: str) -> bytes:
        if vad is None:  # manual push-to-talk fallback
            return _capture_push_to_talk(audio, cfg.audio.sample_rate, prompt)
        try:
            input(prompt)
        except EOFError:
            return b""
        print("🎙  listening… (just stop talking when done)")
        return audio.record_until_silence(
            vad,
            sample_rate=cfg.audio.sample_rate,
            endpoint_silence_ms=cfg.vad.endpoint_silence_ms,
            speech_threshold=cfg.vad.speech_threshold,
            min_speech_ms=cfg.vad.min_speech_ms,
        )

    async def speak_turn(user_text: str) -> None:
        # Per-turn model routing (spec Step 4): strong for longer asks, else fast.
        model = (
            cfg.gateway.strong_model
            if len(user_text) > 120
            else cfg.gateway.fast_model
        )
        messages = [
            {"role": "system", "content": _VOICE_SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ]
        reply = await gateway.complete(messages, model=model)
        print(f"  jarvis [{model}]: {reply}")
        if reply:
            await audio.play_stream(
                tts.synthesize_stream(reply), sample_rate=cfg.tts.sample_rate
            )

    async def run() -> int:
        try:
            for i in range(args.rounds):
                pcm = await asyncio.to_thread(
                    capture, f"\nPress ENTER to speak [{i + 1}/{args.rounds}]…"
                )
                secs = len(pcm) / 2 / cfg.audio.sample_rate
                text = await asyncio.to_thread(
                    stt.transcribe, pcm, sample_rate=cfg.audio.sample_rate
                )
                print(f"  you ({secs:.1f}s): {text!r}")
                if not text:
                    print("  (no speech detected — nothing to answer)")
                    continue
                try:
                    await speak_turn(text)
                except Exception as exc:  # noqa: BLE001 - keep the loop alive
                    import traceback

                    print(f"  turn error: {exc}")
                    traceback.print_exc()
        finally:
            await gateway.aclose()
        return 0

    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        print("\n(interrupted)")
        return 130


def _run_local(cfg) -> None:  # noqa: ANN001
    """In-process single-machine loop (brain + edge in one process)."""
    from jarvis.brain.gateway_client import GatewayClient
    from jarvis.brain.memory_client import MemoryClient
    from jarvis.brain.tracing import Tracer
    from jarvis.brain.turnloop import TurnLoop
    from jarvis.intercom.audio import AudioIO
    from jarvis.intercom.vad import SileroVAD
    from jarvis.intercom.wake import WakeWord
    from jarvis.services.stt import Transcriber
    from jarvis.services.tts import InworldTTS

    loop = TurnLoop(
        cfg,
        audio=AudioIO(cfg.audio),
        stt=Transcriber(cfg.stt),
        vad=SileroVAD(cfg.vad),
        wake=WakeWord(cfg.wake),
        gateway=GatewayClient(cfg.gateway),
        tts=InworldTTS(cfg.tts),
        memory=MemoryClient(cfg.memory),
        tracer=Tracer(cfg.trace),
    )
    asyncio.run(loop.run())


def _run_intercom(cfg) -> None:  # noqa: ANN001
    """Thin intercom client: capture/playback locally, talk to the brain (W4)."""
    from jarvis.intercom.audio import AudioIO
    from jarvis.intercom.client import IntercomClient
    from jarvis.intercom.vad import SileroVAD
    from jarvis.intercom.wake import WakeWord

    client = IntercomClient(
        cfg, audio=AudioIO(cfg.audio), vad=SileroVAD(cfg.vad), wake=WakeWord(cfg.wake)
    )
    asyncio.run(client.run())


def _cmd_run(args: argparse.Namespace) -> int:
    """Hands-free wake-word loop. Default: thin intercom -> brain server (W4).
    `--local` runs the whole thing in one process (no server needed)."""
    cfg = load_config()
    if args.no_bargein:
        cfg.vad.bargein_enabled = False
    if args.brain:
        host, _, port = args.brain.partition(":")
        cfg.intercom.brain_host = host or cfg.intercom.brain_host
        if port:
            cfg.intercom.brain_port = int(port)
    try:
        if args.local:
            _run_local(cfg)
        else:
            _run_intercom(cfg)
    except KeyboardInterrupt:
        print("\n(stopped)")
        # Blocking mic-read worker threads can't be joined on Ctrl-C; exit hard
        # so the interpreter's atexit thread-join doesn't hang.
        os._exit(0)
    return 0


def _cmd_brain(_args: argparse.Namespace) -> int:
    """Run the brain WebSocket server that intercoms connect to (Phase 3 W4)."""
    from jarvis.brain.server import serve

    cfg = load_config()
    try:
        asyncio.run(serve(cfg))
    except KeyboardInterrupt:
        print("\n(stopped)")
        os._exit(0)
    return 0


def _cmd_worker(_args: argparse.Namespace) -> int:
    """Run the worker daemon — deep work + machine control on this host (W3c).
    A standalone service the brain dispatches to over HTTP."""
    from jarvis.worker.server import serve

    cfg = load_config()
    try:
        asyncio.run(serve(cfg.worker))
    except KeyboardInterrupt:
        print("\n(stopped)")
        os._exit(0)
    return 0


def _cmd_jobs(args: argparse.Namespace) -> int:
    """List the worker daemon's recent jobs (deep-work jobs + their results)."""
    import datetime

    import httpx

    cfg = load_config()
    base = cfg.worker.base_url
    headers = {}
    tok = cfg.worker.token.get_secret_value()
    if tok:
        headers["Authorization"] = f"Bearer {tok}"

    if args.prune:  # clean up all finished jobs (worktrees + branches)
        try:
            r = httpx.post(f"{base}/run", json={"action": "cleanup", "args": {"job": ""}}, headers=headers, timeout=30)
            cleaned = r.json().get("cleaned", [])
        except Exception as exc:  # noqa: BLE001
            print(f"Worker not reachable at {base} ({exc}).\nStart it with: jarvis worker")
            return 1
        print(f"Cleaned up {len(cleaned)} finished job(s)." + (f" ({', '.join(cleaned)})" if cleaned else ""))
        return 0

    try:
        r = httpx.get(f"{base}/jobs", headers=headers, timeout=5)
        r.raise_for_status()
        jobs = r.json().get("jobs", [])
    except Exception as exc:  # noqa: BLE001
        print(f"Worker not reachable at {base} ({exc}).")
        print("Start it with: jarvis worker")
        return 1
    if not jobs:
        print("No jobs yet.")
        return 0
    print(f"Worker jobs at {cfg.worker.base_url}:\n")
    for j in jobs[-args.n :]:
        clock = datetime.datetime.fromtimestamp(j.get("started", 0)).strftime("%H:%M:%S")
        out = (j.get("output") or "").replace("\n", " ")
        if len(out) > 64:
            out = out[:64] + "…"
        print(
            f"  {clock}  {(j.get('name') or j.get('id'))[:30]:<30} "
            f"{j.get('status'):<11} {out}"
        )
        if j.get("branch"):
            print(f"            └─ branch: {j['branch']}  (review: git -C {j.get('cwd')} diff)")
        elif j.get("cwd"):
            print(f"            └─ ran in: {j['cwd']}  (git -C {j['cwd']} diff)")
        if j.get("session_id"):
            print(f"            └─ full transcript: codex resume {j['session_id']}")
    return 0


def _upsert_env(key: str, value: str, path: str = ".env") -> None:
    """Set key=value in .env, replacing any existing line."""
    import pathlib
    import re

    p = pathlib.Path(path)
    text = p.read_text() if p.exists() else ""
    line = f"{key}={value}"
    if re.search(rf"^{re.escape(key)}=.*$", text, re.MULTILINE):
        text = re.sub(rf"^{re.escape(key)}=.*$", line, text, flags=re.MULTILINE)
    else:
        text = (text.rstrip("\n") + "\n" if text else "") + line + "\n"
    p.write_text(text)


def _cmd_remote_setup(_args: argparse.Namespace) -> int:
    """One-time: create the cloud agent + environment for remote coding jobs."""
    from jarvis.remote.client import RemoteClient

    cfg = load_config()
    if not cfg.remote.api_key.get_secret_value():
        print("Set ANTHROPIC_API_KEY in .env first.")
        return 1
    client = RemoteClient(cfg.remote)
    system = (
        "You are Jarvis's autonomous coding agent. Work carefully and incrementally, "
        "explain what you change, and avoid anything destructive without good reason."
    )

    async def run() -> tuple[str, str]:
        print("Creating cloud agent…")
        agent = await client.create_agent("Jarvis coding agent", system)
        print(f"  agent: {agent['id']}")
        print("Creating cloud environment…")
        env = await client.create_environment("jarvis-cloud")
        print(f"  environment: {env['id']}")
        return agent["id"], env["id"]

    try:
        agent_id, env_id = asyncio.run(run())
    except Exception as exc:  # noqa: BLE001
        print(f"Setup failed: {exc}")
        return 1
    _upsert_env("ANTHROPIC_AGENT_ID", agent_id)
    _upsert_env("ANTHROPIC_ENVIRONMENT_ID", env_id)
    print("\nWritten ANTHROPIC_AGENT_ID + ANTHROPIC_ENVIRONMENT_ID to .env.")
    print("Add remote.code to CAPS_DEFAULT_CAPABILITIES to use it by voice.")
    return 0


def _cmd_traces(args: argparse.Namespace) -> int:
    """View recent per-turn pipeline traces (STT/LLM/TTS/memory timings)."""
    import json
    import pathlib

    cfg = load_config()
    path = pathlib.Path(cfg.trace.path)
    if not path.exists():
        print(f"No traces yet at {path} (run `jarvis run` first).")
        return 0
    lines = [ln for ln in path.read_text().splitlines() if ln.strip()]
    rows = []
    for ln in lines[-args.n :]:
        try:
            rows.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    import datetime

    def clock(d: dict) -> str:
        ts = d.get("ts")
        if not ts:
            return "--:--:--"
        return datetime.datetime.fromtimestamp(ts).strftime("%H:%M:%S")

    print(f"Last {len(rows)} traces from {path} (time column lines up the timeline):\n")
    for d in rows:
        s = d.get("stages", {})
        kind = d.get("kind", "turn")
        if kind == "memory":
            # Indented so a cold-path burst is easy to see against the turn it
            # overlaps — useful for telling contention from ambient noise.
            print(f"  {clock(d)}  memory      mem={s.get('memory', {}).get('ms', 0):.0f}ms (cold path)")
            continue
        stt = s.get("stt", {})
        llm = s.get("llm", {})
        tts = s.get("tts", {})
        ev = ",".join(e["name"] for e in d.get("events", []))
        print(
            f"  {clock(d)}  turn  {d.get('speaker', '?'):<7} {d.get('room', '?'):<8} "
            f"stt={stt.get('ms', 0):.0f}ms "
            f"llm[{llm.get('model', '?')}]={llm.get('ms', 0):.0f}ms "
            f"tts={tts.get('ms', 0):.0f}ms(ttfa {tts.get('ttfa_ms') or 0:.0f}) "
            f"total={d.get('total_ms', 0):.0f}ms" + (f"  [{ev}]" if ev else "")
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="jarvis", description="Jarvis voice assistant")
    parser.add_argument("--version", action="version", version=f"jarvis {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p_config = sub.add_parser("config", help="Print resolved configuration (dry-run)")
    p_config.set_defaults(func=_cmd_config)

    p_ping = sub.add_parser("ping-gateway", help="Step 1 gate: test fast + strong routes")
    p_ping.add_argument(
        "--prompt",
        default="Reply with exactly one short sentence confirming you are online.",
    )
    p_ping.add_argument("--route", help="Test a single named route instead of fast+strong")
    p_ping.set_defaults(func=_cmd_ping_gateway)

    p_say = sub.add_parser("say", help="Step 2 gate: speak text via streaming TTS")
    p_say.add_argument("text", help="Text for Jarvis to speak")
    p_say.add_argument("--voice", help="Override the configured voice")
    p_say.add_argument(
        "--stop-after",
        type=float,
        metavar="SECONDS",
        help="Hard-stop playback after N seconds (demonstrates barge-in cut)",
    )
    p_say.set_defaults(func=_cmd_say)

    p_listen = sub.add_parser("listen", help="Step 3 gate: push-to-talk STT")
    p_listen.add_argument(
        "--rounds", type=int, default=1, help="How many utterances to capture"
    )
    p_listen.set_defaults(func=_cmd_listen_safe)

    p_chat = sub.add_parser("chat", help="Voice round-trip (Step 4/5): VAD endpointing")
    p_chat.add_argument("--rounds", type=int, default=3, help="Number of turns")
    p_chat.add_argument(
        "--manual",
        action="store_true",
        help="Use ENTER-to-stop push-to-talk instead of VAD endpointing",
    )
    p_chat.set_defaults(func=_cmd_chat)

    p_run = sub.add_parser("run", help="Hands-free wake-word loop (intercom -> brain)")
    p_run.add_argument(
        "--no-bargein",
        action="store_true",
        help="Disable barge-in (use on bare speakers without an AEC mic)",
    )
    p_run.add_argument(
        "--local",
        action="store_true",
        help="Run brain + edge in one process (no separate brain server)",
    )
    p_run.add_argument(
        "--brain",
        metavar="HOST[:PORT]",
        help="Brain server to connect to (overrides INTERCOM_BRAIN_HOST/PORT)",
    )
    p_run.set_defaults(func=_cmd_run)

    p_brain = sub.add_parser("brain", help="Run the brain server intercoms connect to (W4)")
    p_brain.set_defaults(func=_cmd_brain)

    p_worker = sub.add_parser("worker", help="Run the worker daemon (deep work + machine control, W3c)")
    p_worker.set_defaults(func=_cmd_worker)

    p_remote = sub.add_parser("remote-setup", help="One-time: create the cloud agent + environment (remote coding)")
    p_remote.set_defaults(func=_cmd_remote_setup)

    p_jobs = sub.add_parser("jobs", help="List the worker's recent jobs + results")
    p_jobs.add_argument("-n", type=int, default=20, help="How many recent jobs")
    p_jobs.add_argument(
        "--prune", action="store_true", help="Clean up all finished jobs (worktrees + branches)"
    )
    p_jobs.set_defaults(func=_cmd_jobs)

    p_traces = sub.add_parser("traces", help="View recent per-turn pipeline traces")
    p_traces.add_argument("-n", type=int, default=20, help="How many recent traces")
    p_traces.set_defaults(func=_cmd_traces)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
