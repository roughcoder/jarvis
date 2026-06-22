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
            cfg.gateway.strong_model if len(user_text) > 120 else cfg.gateway.fast_model
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


def _cmd_whatsapp(_args: argparse.Namespace) -> int:
    """Run the WhatsApp connector (Phase 3b): bridge `wacli` ↔ the brain."""
    from jarvis.connectors.whatsapp import WhatsAppConnector

    cfg = load_config()
    try:
        asyncio.run(WhatsAppConnector(cfg).run())
    except KeyboardInterrupt:
        print("\nstopped.")
    return 0


def _cmd_whatsapp_log(args: argparse.Namespace) -> int:
    """Print the WhatsApp transcript from wacli's local store (both directions)."""
    import json
    import subprocess

    cfg = load_config().whatsapp
    argv = [cfg.wacli_bin]
    if cfg.account.strip():
        argv += ["--account", cfg.account.strip()]
    argv += ["messages", "list", "--json", "--limit", str(args.n)]
    if args.chat:
        argv += ["--chat", args.chat]
    try:
        out = subprocess.run(argv, capture_output=True, text=True, timeout=30).stdout
        msgs = json.loads(out).get("data", {}).get("messages", [])
    except Exception as exc:  # noqa: BLE001 - wacli missing/not linked
        print(
            f"couldn't read WhatsApp log ({exc}). Is wacli linked? `wacli auth status`"
        )
        return 1
    if not msgs:
        print("(no messages)")
        return 0
    for m in reversed(msgs):  # wacli returns newest-first; show oldest-first
        text = (m.get("Text") or m.get("DisplayText") or "").replace("\n", " ")
        if args.search and args.search.lower() not in text.lower():
            continue
        who = (
            "jarvis"
            if m.get("FromMe")
            else (m.get("SenderName") or m.get("SenderJID", "")[:16])
        )
        ts = (m.get("Timestamp") or "")[:19].replace("T", " ")
        print(f"[{ts}] {who}: {text}")
    return 0


def _cmd_text(args: argparse.Namespace) -> int:
    """Text console: drive the brain from the terminal — no mic/STT/TTS. The dev +
    test harness. `--once` sends one message and prints the reply (scriptable)."""
    from jarvis.connectors.text import TextConsole

    cfg = load_config()
    console = TextConsole(cfg)
    try:
        if args.once is not None:
            return asyncio.run(console.once(args.once))
        return asyncio.run(console.repl())
    except KeyboardInterrupt:
        print("\nstopped.")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"✗ {exc} — is `jarvis brain` running?")
        return 1


def _cmd_worker(args: argparse.Namespace) -> int:
    """Run the worker daemon — deep work + machine control on this host (W3c).
    A standalone service the brain dispatches to over HTTP. `--doctor` reports what
    mac GUI control (peekaboo) needs instead of starting the daemon."""
    cfg = load_config()
    if getattr(args, "doctor", False):
        import json

        from jarvis.worker.actions import gui_doctor

        d = gui_doctor(cfg.worker.peekaboo_bin)
        print(json.dumps(d, indent=2))
        return 0 if d["peekaboo_installed"] else 1
    from jarvis.worker.server import serve

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
            r = httpx.post(
                f"{base}/run",
                json={"action": "cleanup", "args": {"job": ""}},
                headers=headers,
                timeout=30,
            )
            cleaned = r.json().get("cleaned", [])
        except Exception as exc:  # noqa: BLE001
            print(
                f"Worker not reachable at {base} ({exc}).\nStart it with: jarvis worker"
            )
            return 1
        print(
            f"Cleaned up {len(cleaned)} finished job(s)."
            + (f" ({', '.join(cleaned)})" if cleaned else "")
        )
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
        clock = datetime.datetime.fromtimestamp(j.get("started", 0)).strftime(
            "%H:%M:%S"
        )
        out = (j.get("output") or "").replace("\n", " ")
        if len(out) > 64:
            out = out[:64] + "…"
        print(
            f"  {clock}  {(j.get('name') or j.get('id'))[:30]:<30} "
            f"{j.get('status'):<11} {out}"
        )
        if j.get("branch"):
            print(
                f"            └─ branch: {j['branch']}  (review: git -C {j.get('cwd')} diff)"
            )
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


def _cmd_google_setup(_args: argparse.Namespace) -> int:
    """One-time OAuth for the `google` tool (Jarvis's own Gmail/Calendar via gogcli)."""
    import shutil
    import subprocess

    cfg = load_config()
    if not shutil.which(cfg.google.gogcli_bin):
        print(f"{cfg.google.gogcli_bin!r} not found — install gogcli, then re-run.")
        return 1
    print("Launching gogcli auth (a browser window will open)…")
    try:
        return subprocess.run([cfg.google.gogcli_bin, "auth", "login"]).returncode
    except KeyboardInterrupt:
        return 1


def _cmd_mcp(args: argparse.Namespace) -> int:
    if getattr(args, "mcp_action", "probe") == "login":
        return _cmd_mcp_login(args)
    return _cmd_mcp_probe(args)


def _wait_key(prompt: str) -> str:
    """Print a prompt and return a single keypress — SPACE/ENTER to proceed, any
    of s/q/Esc to skip. Falls back to line input when stdin isn't a TTY (CI/pipe)."""
    import sys

    print(prompt, end="", flush=True)
    if not sys.stdin.isatty():
        line = sys.stdin.readline()
        print()
        return line.strip()[:1] or " "
    import termios
    import tty

    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    print()
    return ch


def _cmd_mcp_login(args: argparse.Namespace) -> int:
    """Interactive OAuth onboarding for HTTP MCP servers (Notion, Granola, …). Walks
    the configured OAuth servers one at a time: already authorized => skip; otherwise
    open the browser, catch the redirect, save the token. Tokens persist so the brain
    uses them silently afterwards. Optionally limit to one with --server."""
    from jarvis.mcp.auth import build_oauth_provider, needs_oauth
    from jarvis.mcp.bridge import _root_cause
    from jarvis.mcp.client import MCPClient

    cfg = load_config()
    servers = cfg.mcp.servers
    if args.server:
        servers = [s for s in servers if s.name == args.server]
        if not servers:
            print(f"No MCP server named {args.server!r} in MCP_SERVERS.")
            return 1
    oauth = [s for s in servers if needs_oauth(s)]
    if not oauth:
        print(
            "No OAuth MCP servers to log in to (stdio servers and http servers with "
            "static headers need no browser auth)."
        )
        return 0

    who = args.user or "house"
    print(f"Authorizing as user {who!r} (tokens → {cfg.mcp.auth_dir}/{who}/).")
    total = len(oauth)

    async def run() -> tuple[int, int]:  # noqa: ANN202
        ok = skipped = 0
        for i, spec in enumerate(oauth, 1):
            print(f"\n{'─' * 60}")
            print(f"  [{i}/{total}]  {spec.name}")
            print(f"           {spec.url}")
            key = _wait_key("  ▶ press SPACE to authorize (s = skip, q = quit)… ")
            if key in ("q", "Q", "\x03"):  # q / Ctrl-C
                print("  quitting.")
                break
            if key in ("s", "S", "\x1b"):  # s / Esc
                print("  skipped.")
                skipped += 1
                continue
            provider, _storage, flow = build_oauth_provider(
                spec, cfg.mcp, interactive=True, user=who
            )
            client = MCPClient(
                spec, call_timeout_s=cfg.mcp.call_timeout_s, auth=provider
            )
            try:
                tools = await asyncio.wait_for(
                    client.connect(), 300
                )  # human-in-the-loop
                state = "authorized" if (flow and flow.opened) else "already authorized"
                print(f"  ✓ {state} — {len(tools)} tool(s) available")
                ok += 1
            except Exception as exc:  # noqa: BLE001
                print(f"  ✗ failed: {_root_cause(exc)}")
            finally:
                await client.aclose()
        return ok, skipped

    print(
        f"Authorizing {total} OAuth MCP server(s). I'll announce each — "
        "press SPACE when ready and finish the login in your browser."
    )
    ok, skipped = asyncio.run(run())
    print(f"\n{'─' * 60}")
    print(
        f"Done — {ok}/{total} authorized"
        + (f", {skipped} skipped" if skipped else "")
        + "."
    )
    print(
        f"Tokens saved to {cfg.mcp.auth_dir}/; the brain refreshes them silently on next start."
    )
    return 0 if ok else 1


def _cmd_mcp_probe(args: argparse.Namespace) -> int:
    """Probe the configured MCP servers: connect, discover tools, print what each
    contributes and the capability a profile must grant. The same connect path the
    brain runs at startup — a quick check that a server is reachable + gated."""
    from jarvis.mcp import MCPBridge

    cfg = load_config()
    if not cfg.mcp.enabled:
        print("MCP is disabled. Set MCP_ENABLED=true and MCP_SERVERS in .env.")
        return 0
    if not cfg.mcp.servers:
        print("MCP is enabled but no servers are configured (MCP_SERVERS=[]).")
        return 0

    async def run() -> list:  # noqa: ANN202
        bridge = MCPBridge(cfg.mcp, principals=[args.user] if args.user else ["house"])
        try:
            tools = await bridge.start()
            # snapshot before aclose resets bridge state
            return [
                (t.offered_name, t.server, t.required_capability, t.description)
                for t in tools
            ]
        finally:
            await bridge.aclose()

    print(f"Probing {len(cfg.mcp.servers)} MCP server(s)…\n")
    try:
        rows = asyncio.run(run())
    except Exception as exc:  # noqa: BLE001
        print(f"MCP probe failed: {exc}")
        return 1
    if not rows:
        print("No tools discovered (servers may have failed to connect — see above).")
        return 1
    # What the named user is actually granted (their own user-file caps). This is the
    # per-user dimension — it's what tells discovery apart from access.
    granted: set | None = None
    if args.user and args.user != "house":
        from jarvis.brain.identity import load_users

        u = load_users(cfg.capabilities.users_dir).get(args.user)
        granted = set(u.capabilities) if u else set()

    by_cap: dict[str, list] = {}
    for name, server, cap, desc in rows:
        by_cap.setdefault(cap, []).append((name, desc))
    for cap in sorted(by_cap):
        if granted is None:
            mark = "(needs this capability)"
        elif cap in granted:
            mark = f"✓ granted to {args.user}"
        else:
            mark = f"✗ NOT granted to {args.user} — discovery only"
        print(f"capability {cap!r}  {mark}:")
        for name, desc in by_cap[cap]:
            short = (desc[:70] + "…") if len(desc) > 71 else desc
            print(f"    {name.ljust(34)}  {short}")
        print()
    print(f"{len(rows)} tool(s) discovered across {len(by_cap)} capabilit(ies).")
    print(
        "Note: this is a DISCOVERY view (what connects + the capability each tool "
        "needs), not a grant. Actual access = the device profile + the speaker's "
        "user-file caps; OAuth servers also need that user's own token."
    )
    return 0


def _cmd_status(args: argparse.Namespace) -> int:
    """Device lifecycle (§3): is the brain reachable, and what is THIS device allowed
    to do? Pairs like an intercom (device id + token) and prints the Welcome."""
    import json

    from jarvis.fleet import probe_brain

    cfg = load_config()
    if args.brain_host:
        cfg.intercom.brain_host = args.brain_host
    if args.brain_port:
        cfg.intercom.brain_port = int(args.brain_port)
    url = cfg.intercom.brain_url

    res = asyncio.run(probe_brain(cfg))
    if getattr(args, "json", False):
        print(
            json.dumps(
                {"brain_url": url, "device_id": cfg.capabilities.device_id, **res},
                indent=2,
            )
        )
        return 0 if res.get("paired") else 1
    print(f"Brain: {url}  (device: {cfg.capabilities.device_id})")
    if not res.get("reachable"):
        print(f"  ✗ not reachable ({res.get('error')}) — is `jarvis brain` running?")
        return 1
    if res.get("paired"):
        print("  ✓ reachable + paired")
        print(f"    identity: {res.get('identity')}   scope: {res.get('scope')}")
        print(
            f"    capabilities: {', '.join(res.get('capabilities') or []) or '(none)'}"
        )
        return 0
    print(
        f"  ✗ pairing rejected: {res.get('error')} (check INTERCOM_TOKEN / BRAIN_DEVICES)"
    )
    return 1


def _cmd_fleet_status(args: argparse.Namespace) -> int:
    """Operator/status surface for the Swift menu bar app."""
    import json

    from jarvis.fleet import collect_fleet_status

    cfg = load_config()
    data = asyncio.run(collect_fleet_status(cfg, include_docker=not args.no_docker))
    if args.json:
        print(json.dumps(data, indent=2, sort_keys=True))
        return 0

    pairing = data["intercom"]["pairing"]
    worker = data["worker"]["probe"]
    docker = data["docker"]
    git = data["git"]
    print(f"Jarvis fleet status for {data['device_id']}")
    print(
        f"  brain bind:       {data['brain']['bind']}  auth={'yes' if data['brain']['auth_configured'] else 'no'}"
    )
    print(
        "  intercom pairing: "
        + (
            "paired"
            if pairing.get("paired")
            else f"not paired ({pairing.get('error', 'unknown')})"
        )
    )
    if pairing.get("paired"):
        print(f"    identity/scope: {pairing.get('identity')} / {pairing.get('scope')}")
    print(
        "  worker:          "
        + (
            f"reachable ({worker.get('health', {}).get('agent', data['worker']['agent'])})"
            if worker.get("reachable")
            else f"unreachable ({worker.get('error', 'unknown')})"
        )
    )
    if worker.get("reachable"):
        jobs = worker.get("jobs", {})
        print(
            f"    jobs:           {jobs.get('running', 0)} running / {jobs.get('total', 0)} total"
        )
    if docker.get("available"):
        running = sum(
            1
            for s in docker.get("services", [])
            if s.get("state", "").lower() == "running"
        )
        print(
            f"  docker compose:   {running}/{len(docker.get('services', []))} running"
        )
    else:
        print(f"  docker compose:   unavailable ({docker.get('error', 'not checked')})")
    if git.get("available"):
        dirty = " dirty" if git.get("dirty") else ""
        print(f"  git:              {git.get('branch')} {git.get('commit')}{dirty}")
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
            print(
                f"  {clock(d)}  memory      mem={s.get('memory', {}).get('ms', 0):.0f}ms (cold path)"
            )
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


def _cmd_service(args: argparse.Namespace) -> int:
    """Install and control Jarvis services without exposing launchd/systemd internals."""
    from jarvis.deploy import (
        control_service,
        install_service,
        render_service,
        role_extras,
    )

    if args.service_action == "extras":
        print(" ".join(role_extras(set(args.roles))))
        return 0

    if args.service_action in {"print", "install"}:
        rc = 0
        for role in args.roles:
            if args.service_action == "print":
                print(
                    render_service(
                        role,
                        platform_name=args.platform,
                        jarvis_bin=args.jarvis_bin,
                        workdir=args.workdir,
                        log_dir=args.log_dir,
                    )
                )
                continue
            dest, text = install_service(
                role,
                platform_name=args.platform,
                jarvis_bin=args.jarvis_bin,
                workdir=args.workdir,
                log_dir=args.log_dir,
                destination=args.destination,
                dry_run=args.dry_run,
            )
            if args.dry_run:
                print(text)
                print(f"# would write {dest}")
            else:
                print(f"installed {role} service: {dest}")
        return rc

    result = control_service(
        args.roles[0], args.service_action, platform_name=args.platform
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return result.returncode


def _cmd_pair(args: argparse.Namespace) -> int:
    """Pairing helpers for fleet onboarding."""
    from jarvis.deploy import (
        issue_pairing_entry,
        render_mac_config_command,
        render_pi_installer_command,
    )

    token, fragment = issue_pairing_entry(args.device_id, identity=args.identity or "")
    if args.json:
        import json

        payload = {"token": token, "brain_devices_entry": fragment}
        if args.pi_installer or args.mac_config:
            if not args.brain_host:
                print(
                    "--brain-host is required with --pi-installer or --mac-config",
                    file=sys.stderr,
                )
                return 2
        if args.mac_config:
            payload["mac_config_command"] = render_mac_config_command(
                device_id=args.device_id,
                token=token,
                brain_host=args.brain_host,
                brain_port=args.brain_port,
                identity=args.identity or "",
                workdir=args.mac_workdir,
            )
        if args.pi_installer:
            payload["pi_installer_command"] = render_pi_installer_command(
                device_id=args.device_id,
                token=token,
                brain_host=args.brain_host,
                brain_port=args.brain_port,
                repo=args.repo,
                ref=args.ref,
            )
        print(json.dumps(payload, indent=2))
    elif args.pi_installer or args.mac_config:
        if not args.brain_host:
            print(
                "--brain-host is required with --pi-installer or --mac-config",
                file=sys.stderr,
            )
            return 2
        print("Add this object to BRAIN_DEVICES on the brain:")
        print(fragment)
        if args.mac_config:
            print("\nRun this on the Mac intercom/worker:")
            print(
                render_mac_config_command(
                    device_id=args.device_id,
                    token=token,
                    brain_host=args.brain_host,
                    brain_port=args.brain_port,
                    identity=args.identity or "",
                    workdir=args.mac_workdir,
                )
            )
        if args.pi_installer:
            print("\nRun this on the Raspberry Pi:")
            print(
                render_pi_installer_command(
                    device_id=args.device_id,
                    token=token,
                    brain_host=args.brain_host,
                    brain_port=args.brain_port,
                    repo=args.repo,
                    ref=args.ref,
                )
            )
    else:
        print(f"Device: {args.device_id}")
        print(f"Token:  {token}")
        print("\nAdd this object to BRAIN_DEVICES on the brain:")
        print(fragment)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="jarvis", description="Jarvis voice assistant"
    )
    parser.add_argument("--version", action="version", version=f"jarvis {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p_config = sub.add_parser("config", help="Print resolved configuration (dry-run)")
    p_config.set_defaults(func=_cmd_config)

    p_ping = sub.add_parser(
        "ping-gateway", help="Step 1 gate: test fast + strong routes"
    )
    p_ping.add_argument(
        "--prompt",
        default="Reply with exactly one short sentence confirming you are online.",
    )
    p_ping.add_argument(
        "--route", help="Test a single named route instead of fast+strong"
    )
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

    p_brain = sub.add_parser(
        "brain", help="Run the brain server intercoms connect to (W4)"
    )
    p_brain.set_defaults(func=_cmd_brain)

    p_worker = sub.add_parser(
        "worker", help="Run the worker daemon (deep work + machine control, W3c)"
    )
    p_worker.add_argument(
        "--doctor",
        action="store_true",
        help="Report mac GUI control (peekaboo) readiness and exit",
    )
    p_worker.set_defaults(func=_cmd_worker)

    p_whatsapp = sub.add_parser(
        "whatsapp", help="Run the WhatsApp connector (bridge wacli <-> brain, 3b)"
    )
    p_whatsapp.set_defaults(func=_cmd_whatsapp)

    p_walog = sub.add_parser(
        "whatsapp-log", help="Print the WhatsApp transcript from wacli's store"
    )
    p_walog.add_argument(
        "-n",
        type=int,
        default=30,
        help="how many recent messages to fetch (default 30)",
    )
    p_walog.add_argument(
        "--chat", default="", help="limit to one chat JID (DM or group)"
    )
    p_walog.add_argument(
        "--search", default="", help="only show messages containing this text"
    )
    p_walog.set_defaults(func=_cmd_whatsapp_log)

    p_text = sub.add_parser(
        "text", help="Text console: drive the brain from the terminal (no mic/STT/TTS)"
    )
    p_text.add_argument(
        "--once",
        metavar="MESSAGE",
        help="Send one message, print the reply, and exit (scriptable)",
    )
    p_text.set_defaults(func=_cmd_text)

    p_remote = sub.add_parser(
        "remote-setup",
        help="One-time: create the cloud agent + environment (remote coding)",
    )
    p_remote.set_defaults(func=_cmd_remote_setup)

    p_gsetup = sub.add_parser(
        "google-setup", help="One-time: OAuth for the google tool (gogcli)"
    )
    p_gsetup.set_defaults(func=_cmd_google_setup)

    p_mcp = sub.add_parser(
        "mcp", help="MCP servers: probe tools, or `mcp login` for OAuth onboarding"
    )
    p_mcp.add_argument(
        "mcp_action",
        nargs="?",
        choices=["probe", "login"],
        default="probe",
        help="probe = discover tools (default); login = interactive OAuth for http servers",
    )
    p_mcp.add_argument(
        "--server", default="", help="login: limit to one server by name"
    )
    p_mcp.add_argument(
        "--user", default="", help="principal whose credentials to use (default: house)"
    )
    p_mcp.set_defaults(func=_cmd_mcp)

    p_jobs = sub.add_parser("jobs", help="List the worker's recent jobs + results")
    p_jobs.add_argument("-n", type=int, default=20, help="How many recent jobs")
    p_jobs.add_argument(
        "--prune",
        action="store_true",
        help="Clean up all finished jobs (worktrees + branches)",
    )
    p_jobs.set_defaults(func=_cmd_jobs)

    p_status = sub.add_parser(
        "status", help="Is the brain reachable + what is this device allowed to do?"
    )
    p_status.add_argument(
        "--json", action="store_true", help="Print machine-readable status"
    )
    p_status.add_argument(
        "--brain-host",
        default="",
        help="Override INTERCOM_BRAIN_HOST for this reachability check",
    )
    p_status.add_argument(
        "--brain-port",
        default="",
        help="Override INTERCOM_BRAIN_PORT for this reachability check",
    )
    p_status.set_defaults(func=_cmd_status)

    p_fleet = sub.add_parser(
        "fleet-status", help="Operator status for local roles + remote peers"
    )
    p_fleet.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable status for the toolbar app",
    )
    p_fleet.add_argument(
        "--no-docker", action="store_true", help="Skip docker compose status"
    )
    p_fleet.set_defaults(func=_cmd_fleet_status)

    p_traces = sub.add_parser("traces", help="View recent per-turn pipeline traces")
    p_traces.add_argument("-n", type=int, default=20, help="How many recent traces")
    p_traces.set_defaults(func=_cmd_traces)

    p_service = sub.add_parser(
        "service", help="Install/control Jarvis launchd/systemd services"
    )
    p_service.add_argument(
        "service_action",
        choices=["install", "print", "start", "stop", "restart", "status", "extras"],
        help="install/print render service files; start/stop/restart/status controls one role; extras prints uv extras for roles",
    )
    p_service.add_argument(
        "roles",
        nargs="+",
        choices=["brain", "intercom", "worker"],
        help="Role(s). Control actions accept exactly one role.",
    )
    p_service.add_argument(
        "--platform", choices=["launchd", "systemd"], help="Override platform detection"
    )
    p_service.add_argument(
        "--jarvis-bin", help="Jarvis executable path for generated service files"
    )
    p_service.add_argument(
        "--workdir", help="Working directory for generated service files"
    )
    p_service.add_argument(
        "--log-dir", help="Log directory for generated service files"
    )
    p_service.add_argument(
        "--destination", help="Write service file here instead of the platform default"
    )
    p_service.add_argument(
        "--dry-run",
        action="store_true",
        help="Print install output without writing files",
    )
    p_service.set_defaults(func=_cmd_service)

    p_pair = sub.add_parser("pair", help="Issue a per-device pairing token entry")
    p_pair.add_argument(
        "device_id", help="Device id, e.g. imac-brain, kitchen-pi, neil-laptop"
    )
    p_pair.add_argument(
        "--identity", default="", help="Optional pinned identity for a personal device"
    )
    p_pair.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable token + BRAIN_DEVICES entry",
    )
    p_pair.add_argument(
        "--pi-installer",
        action="store_true",
        help="Print copy/paste Raspberry Pi installer commands",
    )
    p_pair.add_argument(
        "--mac-config",
        action="store_true",
        help="Print copy/paste Mac intercom/worker config commands",
    )
    p_pair.add_argument(
        "--brain-host",
        default="",
        help="Brain hostname for --pi-installer or --mac-config",
    )
    p_pair.add_argument(
        "--brain-port", default="8700", help="Brain WebSocket port for --pi-installer"
    )
    p_pair.add_argument(
        "--repo",
        default="roughcoder/jarvis",
        help="Runtime repository for --pi-installer",
    )
    p_pair.add_argument(
        "--ref",
        default=None,
        help="Runtime branch/tag/ref for --pi-installer; defaults to this jarvis release tag",
    )
    p_pair.add_argument(
        "--mac-workdir",
        default="$HOME/.jarvis",
        help="Mac service workdir for --mac-config",
    )
    p_pair.set_defaults(func=_cmd_pair)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
