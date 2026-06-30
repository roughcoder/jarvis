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
from pathlib import Path

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


def _orch_store(cfg=None):  # noqa: ANN001, ANN202
    from jarvis.orchestration.store import OrchestrationStore

    cfg = cfg or load_config()
    return OrchestrationStore(cfg.orchestration.workspace)


def _cmd_runs(args: argparse.Namespace) -> int:
    import json

    cfg = load_config()
    store = _orch_store(cfg)
    if args.create:
        run = store.create_run(args.create)
        print(f"Created {run.run_id}: {run.objective}")
        return 0
    if args.events:
        events = store.events(args.events)
        if args.json:
            print(json.dumps([e.to_dict() for e in events], indent=2))
        else:
            for e in events:
                print(f"{e.time} {e.type:<28} {e.message}")
        return 0
    if args.sync:
        from jarvis.orchestration.supervisor import sync_run_jobs

        summary = sync_run_jobs(
            store,
            worker_cfg=cfg.worker,
            workers_path=cfg.orchestration.workers_path,
            run_id=args.run_id or "",
        )
        if args.json and not args.run_id:
            print(json.dumps(summary.to_dict(), indent=2))
            return 0
        if not args.json:
            print(
                f"Synced {summary.runs_seen} run(s), {summary.jobs_seen} job(s); "
                f"{summary.jobs_updated} updated, {summary.runs_completed} completed, {summary.runs_failed} failed."
            )
        if not args.run_id:
            return 0
    if args.run_id:
        run = store.get(args.run_id)
        if run is None:
            print(f"No run found for {args.run_id!r}.")
            return 1
        print(json.dumps(run.to_dict(), indent=2) if args.json else _format_run(run))
        return 0
    runs = store.list_runs()
    if args.json:
        print(json.dumps([r.to_dict() for r in runs], indent=2))
        return 0
    if not runs:
        print("No orchestration runs yet.")
        return 0
    for run in runs[-args.n :]:
        print(f"{run.run_id:<26} {run.phase:<13} {run.objective}")
    return 0


def _format_run(run) -> str:  # noqa: ANN001
    parts = [
        f"Run: {run.run_id}",
        f"Objective: {run.objective}",
        f"Phase: {run.phase} ({run.status})",
    ]
    if run.parent_run_id:
        parts.append(f"Parent: {run.parent_run_id}")
    if run.child_run_ids:
        parts.append(f"Children: {', '.join(run.child_run_ids)}")
    if run.work_items:
        parts.append("Work items:")
        parts.extend(
            f"  - {x.role}: {x.item.source}:{x.item.id} {x.item.title}" for x in run.work_items
        )
    if run.jobs:
        parts.append("Jobs:")
        parts.extend(
            f"  - {x.worker_id}:{x.job_id} {x.status} {x.branch}".rstrip() for x in run.jobs
        )
    if run.artifacts:
        parts.append("Artifacts:")
        parts.extend(f"  - {x.type}: {x.url or x.name or x.id}" for x in run.artifacts)
    if run.terminal_reason:
        parts.append(f"Terminal reason: {run.terminal_reason}")
    return "\n".join(parts)


def _cmd_workers(args: argparse.Namespace) -> int:
    import json

    from jarvis.orchestration.workers import WorkerRegistry

    cfg = load_config()
    registry = WorkerRegistry(cfg.worker, profiles_path=cfg.orchestration.workers_path)
    profiles = registry.profiles(probe=args.probe)
    if args.json:
        print(json.dumps([p.public() for p in profiles], indent=2))
        return 0
    if not profiles:
        print("No workers configured.")
        return 0
    for p in profiles:
        pub = p.public()
        cap = ", ".join(pub["capabilities"]) or "<none>"
        capacity = pub["capacity"]
        print(
            f"{pub['worker_id']:<20} {pub['status']:<8} "
            f"{capacity['current_jobs']}/{capacity['max_concurrent_jobs']} jobs  "
            f"{pub['agent']}  {cap}"
        )
    return 0


def _work_source(name: str, cfg=None):  # noqa: ANN001, ANN202
    from jarvis.orchestration.sources import GitHubWorkSource, LinearWorkSource

    if name == "linear":
        api_key = cfg.linear.api_key.get_secret_value() if cfg is not None else None
        return LinearWorkSource(api_key)
    return GitHubWorkSource()


def _cmd_work(args: argparse.Namespace) -> int:
    import json

    from jarvis.brain.capabilities import resolve_capabilities
    from jarvis.orchestration.authority import required_for_command
    from jarvis.orchestration.executor import create_run_and_envelope, start_worker_job
    from jarvis.orchestration.intent import parse_work_command
    from jarvis.orchestration.store import ActiveWorkItemError
    from jarvis.orchestration.workers import WorkerRegistry

    cfg = load_config()
    if args.work_action == "intent":
        command = parse_work_command(" ".join(args.phrase))
        print(json.dumps(command.to_dict(), indent=2))
        return 0

    source_name = args.source or ""
    command = parse_work_command(" ".join(getattr(args, "phrase", []) or [args.work_action]))
    if source_name:
        command.source = source_name
    elif command.source == "direct" and args.work_action in {"check", "next"}:
        command.source = "github"
        command.kind = "issue"
    elif args.work_action == "pr-comments":
        command.source = "github"
        command.operation = "inspect_pr_comments"
        command.kind = "pull_request"
    if getattr(args, "worker", ""):
        command.target_worker_id = args.worker
    if getattr(args, "repo", ""):
        command.filters["repo"] = args.repo
    source = _work_source(command.source, cfg)
    capabilities = resolve_capabilities(cfg.capabilities)
    public_write_mode = cfg.orchestration.landing_mode

    if args.work_action == "check":
        if not _has_orchestration_authority(
            required_for_command(command.operation, command.source),
            capabilities,
            cfg=cfg,
            public_write_mode=public_write_mode,
        ):
            return 1
        try:
            items = source.list(
                repo=args.repo or cfg.orchestration.default_repo,
                filters=command.filters,
                limit=args.limit,
            )
        except RuntimeError as exc:
            print(_work_source_error(command.source, exc))
            return 1
        if args.json:
            print(json.dumps([x.to_dict() for x in items], indent=2))
        else:
            _print_items(items)
        return 0

    if args.work_action == "pr-comments":
        if command.source != "github":
            print("PR comments are currently a GitHub work source operation.")
            return 1
        if not _has_orchestration_authority(
            required_for_command(command.operation, command.source),
            capabilities,
            cfg=cfg,
            public_write_mode=public_write_mode,
        ):
            return 1
        comments = source.pr_comments(args.repo or cfg.orchestration.default_repo, args.number)
        print(
            json.dumps(comments, indent=2)
            if args.json
            else _format_pr_comments_summary(comments, repo=args.repo or cfg.orchestration.default_repo, number=args.number)
        )
        return 0

    if args.work_action == "next":
        if not _has_orchestration_authority(
            required_for_command(command.operation, command.source),
            capabilities,
            cfg=cfg,
            public_write_mode=public_write_mode,
        ):
            return 1
        try:
            item = source.next(repo=args.repo or cfg.orchestration.default_repo, filters=command.filters)
        except RuntimeError as exc:
            print(_work_source_error(command.source, exc))
            return 1
        if item is None:
            print("No eligible work item found.")
            return 0
        store = _orch_store(cfg)
        existing = store.active_primary_owner(item)
        if existing:
            print(f"{item.source}:{item.id} is already owned by {existing.run_id} ({existing.phase}).")
            return 0
        if not args.start:
            print(json.dumps(item.to_dict(), indent=2) if args.json else _format_item(item))
            return 0
        if not _has_orchestration_authority(
            ["worker.job.start", *_required_for_landing_mode(cfg.orchestration.landing_mode)],
            capabilities,
            cfg=cfg,
            public_write_mode=public_write_mode,
        ):
            return 1
        registry = WorkerRegistry(cfg.worker, profiles_path=cfg.orchestration.workers_path)
        worker = registry.get(command.target_worker_id, probe=True) if command.target_worker_id else registry.choose(item.capability_requirements)
        if worker is None or not _worker_is_eligible(worker, item.capability_requirements):
            print("No eligible worker found.")
            return 1
        try:
            envelope = create_run_and_envelope(
                store=store,
                command=command,
                items=[item],
                worker=worker,
                landing_mode=cfg.orchestration.landing_mode,
            )
        except ActiveWorkItemError as exc:
            print(f"{item.source}:{item.id} is already owned by {exc.owner.run_id} ({exc.owner.phase}).")
            return 0
        try:
            job = start_worker_job(envelope, worker_cfg=cfg.worker, worker=worker, store=store)
        except Exception as exc:  # noqa: BLE001 - dispatch failure must release the local claim
            store.set_phase(envelope.run_id, "failed", f"Worker dispatch failed: {exc}")
            print(f"Worker dispatch failed for {envelope.run_id}: {exc}")
            return 1
        print(f"Started {envelope.run_id} on {worker.worker_id}: worker job {job.job_id}")
        if job.branch:
            print(f"Branch: {job.branch}")
        return 0

    if args.work_action == "resume":
        store = _orch_store(cfg)
        run = store.get(args.run_id)
        if run is None:
            print(f"No run found for {args.run_id!r}.")
            return 1
        print(_format_run(run))
        return 0

    print(f"Unknown work action {args.work_action!r}.")
    return 1


def _has_orchestration_authority(
    actions: list[str],
    capabilities: set[str],
    *,
    cfg=None,  # noqa: ANN001
    public_write_mode: str,
) -> bool:
    from jarvis.orchestration.authority import allowed

    denied = [
        action
        for action in actions
        if not allowed(action, capabilities, public_write_mode=public_write_mode)
    ]
    if denied:
        print(f"Missing orchestration capability: {', '.join(denied)}")
        if cfg is not None:
            print(_capability_hint(denied, cfg, capabilities))
        return False
    return True


def _work_source_error(source: str, exc: RuntimeError) -> str:
    message = str(exc)
    if source == "linear" and "LINEAR_API_KEY" in message:
        return (
            "Linear work source is not configured: set LINEAR_API_KEY in the Jarvis env file "
            "used by this service."
        )
    return f"{source} work source failed: {message}"


def _capability_hint(actions: list[str], cfg, capabilities: set[str]) -> str:  # noqa: ANN001
    profile = Path(cfg.capabilities.profiles_dir).expanduser() / f"{cfg.capabilities.device_id}.md"
    worker_profiles = Path(cfg.orchestration.workers_path).expanduser()
    profile_display = profile.resolve(strict=False)
    worker_profiles_display = worker_profiles.resolve(strict=False)
    action_list = ", ".join(actions)
    fallback_caps = ",".join(sorted(capabilities | set(actions)))
    if profile.exists():
        authority = (
            f"Authority source: add {action_list} to {profile_display} front matter. "
            "That profile exists, so CAPS_DEFAULT_CAPABILITIES is ignored for this device."
        )
    else:
        authority = (
            f"Authority source: create {profile_display} with {action_list} in front matter, "
            f"or append the missing capability and set CAPS_DEFAULT_CAPABILITIES={fallback_caps} "
            "for local smoke testing."
        )
    return (
        f"{authority} Named worker capacity lives separately at {worker_profiles_display}."
    )


def _required_for_landing_mode(mode: str) -> list[str]:
    if mode in {"draft_pr", "ready_pr"}:
        return ["forge.github.branch.push", "forge.github.pr.create"]
    if mode == "branch_only":
        return ["forge.github.branch.push"]
    return []


def _worker_is_eligible(worker, required: list[str] | None = None) -> bool:  # noqa: ANN001
    if worker.status == "offline":
        return False
    if worker.current_jobs >= worker.max_concurrent_jobs:
        return False
    return set(required or []).issubset(set(worker.capabilities))


def _format_item(item) -> str:  # noqa: ANN001
    return f"{item.source}:{item.id} {item.title}\n  {item.url or '<no url>'}\n  status={item.status or '<unknown>'} repo={item.repo or '<unset>'}"


def _print_items(items) -> None:  # noqa: ANN001
    if not items:
        print("No work items found.")
        return
    first = items[0]
    kind = first.kind or "item"
    plural = kind if len(items) == 1 else f"{kind}s"
    source = first.source or "work"
    repo = first.repo or "<unset>"
    print(f"Found {len(items)} {source} {plural} for {repo}.")
    for item in items:
        meta = " ".join(
            x
            for x in [
                item.status or "-",
                f"priority={item.priority}" if item.priority else "",
                f"assignee={item.assignee}" if item.assignee else "",
                f"labels={','.join(item.labels[:3])}" if item.labels else "",
            ]
            if x
        )
        print(f"{item.source}:{item.id:<8} {meta:<36} {item.title}")
        if item.url:
            print(f"            {item.url}")
    print(f"Next: use `{_work_next_hint(source, repo)}` to select one, or add `--start --worker <worker_id>` to dispatch.")


def _work_next_hint(source: str, repo: str) -> str:
    import shlex

    parts = ["jarvis", "work", "next"]
    if source in {"github", "linear"}:
        parts.extend(["--source", source])
    if repo and repo != "<unset>":
        parts.extend(["--repo", repo])
    return " ".join(shlex.quote(part) for part in parts)


def _format_pr_comments_summary(comments: list[dict], *, repo: str, number: int) -> str:
    if not comments:
        return f"No PR comment/review objects found for {repo or '<current repo>'}#{number}."
    inline = [c for c in comments if c.get("path")]
    reviewish = [c for c in comments if not c.get("path") and (c.get("state") or c.get("submittedAt"))]
    authors: dict[str, int] = {}
    for comment in comments:
        author = _comment_author(comment)
        authors[author] = authors.get(author, 0) + 1
    author_summary = ", ".join(f"{name}={count}" for name, count in sorted(authors.items()))
    lines = [
        f"PR {repo or '<current repo>'}#{number}: {len(comments)} comment/review object(s)",
        f"  inline={len(inline)} review={len(reviewish)} top-level={len(comments) - len(inline) - len(reviewish)}",
        f"  authors: {author_summary or '<unknown>'}",
        "Highlights:",
    ]
    highlights = _highlight_pr_comments(comments, limit=8)
    for comment in highlights:
        lines.append(f"  - {_format_pr_comment_line(comment)}")
    if len(comments) > len(highlights):
        lines.append(f"  ... {len(comments) - len(highlights)} more; use --json for raw GitHub objects.")
    else:
        lines.append("Use --json for raw GitHub objects.")
    return "\n".join(lines)


def _highlight_pr_comments(comments: list[dict], *, limit: int) -> list[dict]:
    inline = [comment for comment in comments if comment.get("path")]
    other = [comment for comment in comments if not comment.get("path")]
    return [*inline, *other][:limit]


def _comment_author(comment: dict) -> str:
    author = comment.get("author") or comment.get("user") or {}
    if isinstance(author, dict):
        return str(author.get("login") or author.get("name") or "<unknown>")
    return str(author or "<unknown>")


def _format_pr_comment_line(comment: dict) -> str:
    body = str(comment.get("body") or "").strip().splitlines()
    preview = _terminal_safe(body[0].strip()) if body else "<empty>"
    if len(preview) > 120:
        preview = preview[:117] + "..."
    location = "top-level"
    if comment.get("path"):
        path = _terminal_safe(str(comment["path"]))
        line = _terminal_safe(str(comment.get("line") or comment.get("original_line") or "?"))
        location = f"{path}:{line}"
    elif comment.get("state"):
        location = f"review:{_terminal_safe(str(comment['state']))}"
    url = comment.get("url") or comment.get("html_url") or ""
    suffix = f" ({_terminal_safe(str(url))})" if url else ""
    return f"{_terminal_safe(_comment_author(comment))} at {location}: {preview}{suffix}"


def _terminal_safe(text: str) -> str:
    return "".join(
        ch if (ch == "\t" or (ord(ch) >= 32 and ord(ch) != 127 and not 0x80 <= ord(ch) <= 0x9F)) else "?"
        for ch in text
    )


def _parse_weekdays(text: str) -> list[int]:
    if not text:
        return [0, 1, 2, 3, 4, 5, 6]
    names = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
    if text == "weekdays":
        return [0, 1, 2, 3, 4]
    if text == "weekends":
        return [5, 6]
    weekdays = []
    invalid = []
    for part in text.split(","):
        token = part.strip().lower()[:3]
        if not token:
            continue
        if token not in names:
            invalid.append(part.strip())
            continue
        weekdays.append(names[token])
    if invalid or not weekdays:
        raise ValueError(f"invalid weekdays: {', '.join(invalid or [text])}")
    return weekdays


def _cmd_schedules(args: argparse.Namespace) -> int:
    import json
    from datetime import datetime

    from jarvis.brain.capabilities import resolve_capabilities
    from jarvis.orchestration.intent import parse_work_command
    from jarvis.orchestration.schedules import Schedule, ScheduleStore

    cfg = load_config()
    capabilities = resolve_capabilities(cfg.capabilities)
    if args.schedule_action == "add":
        try:
            hh, mm = [int(x) for x in args.at.split(":", 1)]
        except ValueError:
            print("Schedule time must be HH:MM.")
            return 1
        command = parse_work_command(" ".join(args.phrase))
        try:
            weekdays = _parse_weekdays(args.weekdays)
            timezone = args.timezone or cfg.orchestration.default_timezone
            Schedule(
                schedule_id="validate",
                name=args.name or " ".join(args.phrase),
                command=command,
                hour=hh,
                minute=mm,
                weekdays=weekdays,
                timezone=timezone,
                mode=args.mode,
            )
        except ValueError as exc:
            print(f"Invalid schedule: {exc}")
            return 1
        if not _has_orchestration_authority(
            ["orchestration.schedules.write"],
            capabilities,
            cfg=cfg,
            public_write_mode=cfg.orchestration.landing_mode,
        ):
            return 1
        store = ScheduleStore(cfg.orchestration.schedules_path)
        try:
            schedule = store.add(
                args.name or " ".join(args.phrase),
                command,
                hour=hh,
                minute=mm,
                weekdays=weekdays,
                timezone=timezone,
                mode=args.mode,
            )
        except ValueError as exc:
            print(f"Invalid schedule: {exc}")
            return 1
        print(f"Added {schedule.schedule_id}: {schedule.name}")
        return 0
    if args.schedule_action == "tick":
        now = datetime.fromisoformat(args.now) if args.now else datetime.now().astimezone()
        if args.ack:
            if not _has_orchestration_authority(
                ["orchestration.schedules.write"],
                capabilities,
                cfg=cfg,
                public_write_mode=cfg.orchestration.landing_mode,
            ):
                return 1
        store = ScheduleStore(cfg.orchestration.schedules_path)
        due = store.due(now)
        if args.ack:
            for schedule in due:
                store.ack(schedule.schedule_id, now)
        print(json.dumps([x.to_dict() for x in due], indent=2) if args.json else f"{len(due)} schedule(s) due")
        return 0
    store = ScheduleStore(cfg.orchestration.schedules_path)
    schedules = store.list()
    if args.json:
        print(json.dumps([x.to_dict() for x in schedules], indent=2))
    elif not schedules:
        print("No schedules.")
    else:
        for s in schedules:
            days = ",".join(str(x) for x in s.weekdays)
            print(f"{s.schedule_id:<26} {s.hour:02d}:{s.minute:02d} {days:<13} {s.name}")
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
    """One-time OAuth for the current house email/calendar adapter via gogcli."""
    import json
    import shutil
    import subprocess

    cfg = load_config()
    if not shutil.which(cfg.google.gogcli_bin):
        print(f"{cfg.google.gogcli_bin!r} not found — install gogcli, then re-run.")
        return 1
    account = (getattr(_args, "account", "") or os.environ.get("GOG_ACCOUNT", "")).strip()
    if not account:
        print("Set GOG_ACCOUNT or pass `jarvis google-setup --account <email-or-alias>`.")
        return 1
    auth_cmd = [cfg.google.gogcli_bin, "auth", "add", account, "--services", "gmail,calendar"]
    print("Launching gogcli auth (a browser window will open)…")
    try:
        code = subprocess.run(auth_cmd).returncode
    except KeyboardInterrupt:
        return 1
    if code != 0:
        return code

    root = Path(cfg.accounts.bindings_dir) / "house"
    root.mkdir(parents=True, exist_ok=True)
    common = {"provider": "gogcli"}
    if account:
        common["account"] = account
    bindings = {
        cfg.accounts.house_email_binding: {
            **common,
            "kind": "email",
            "grants": ["email.read", "email.draft", "email.send"],
        },
        cfg.accounts.house_calendar_binding: {
            **common,
            "kind": "calendar",
            "grants": ["calendar.freebusy", "calendar.read"],
            "calendar_id": "primary",
        },
    }
    for name, data in bindings.items():
        path = root / f"{name}.json"
        if not path.exists():
            path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            print(f"Wrote {path}")
    return 0


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


def _cmd_bringup(args: argparse.Namespace) -> int:
    """Collect redacted deployment evidence for physical fleet bring-up."""
    import json

    from jarvis.deploy import collect_bringup_evidence
    from jarvis.fleet import probe_brain

    cfg = load_config()
    if args.brain_host:
        cfg.intercom.brain_host = args.brain_host
    if args.brain_port:
        cfg.intercom.brain_port = int(args.brain_port)

    roles = args.roles or ["brain", "intercom", "worker"]
    data = collect_bringup_evidence(
        roles,
        include_hardware=args.hardware,
        platform_name=args.platform,
    )
    if args.check_brain or args.brain_host:
        data["brain_status"] = {
            "brain_url": cfg.intercom.brain_url,
            "device_id": cfg.capabilities.device_id,
            **asyncio.run(probe_brain(cfg)),
        }

    output_path = _write_bringup_evidence(data, args.output) if args.output else None

    if args.json:
        print(json.dumps(data, indent=2, sort_keys=True))
        return 0

    print(f"Jarvis bring-up evidence ({data['platform']})")
    print(f"  version:      {data['jarvis_version']} ({data['release_ref']})")
    print(f"  roles:        {', '.join(data['roles']) or '(none)'}")
    print(f"  extras:       {', '.join(data['role_extras']) or '(none)'}")
    packages = data.get("packages", {})
    if isinstance(packages, dict):
        for name, report in packages.items():
            if isinstance(report, dict):
                status = "ok" if report.get("ok") else "check"
                detail = (report.get("stdout") or report.get("stderr") or "").strip()
                print(f"  package {name}: {status}" + (f" ({detail})" if detail else ""))
    services = data.get("services", {})
    if isinstance(services, dict):
        for role, report in services.items():
            if isinstance(report, dict):
                print(f"  service {role}: {'ok' if report.get('ok') else 'check'}")
    brain = data.get("brain_status")
    if isinstance(brain, dict):
        if brain.get("paired"):
            print(f"  brain:        paired at {brain.get('brain_url')}")
        elif brain.get("reachable"):
            print(f"  brain:        reachable but unpaired at {brain.get('brain_url')}")
        else:
            print(f"  brain:        unreachable at {brain.get('brain_url')}")
    if output_path:
        print(f"  evidence:     {output_path}")
    return 0


def _write_bringup_evidence(data: dict[str, object], output: str) -> str:
    """Write evidence JSON to a chosen file or generated file inside a directory."""
    import json
    import re
    import socket
    from datetime import UTC, datetime
    from pathlib import Path

    target = Path(output).expanduser()
    if target.suffix.lower() == ".json":
        path = target
    else:
        timestamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%SZ")
        hostname = re.sub(r"[^A-Za-z0-9_.-]+", "-", socket.gethostname()).strip("-")
        roles = "-".join(str(role) for role in data.get("roles", []) or ["none"])
        path = target / f"jarvis-bringup-{hostname or 'machine'}-{roles}-{timestamp}.json"

    path.parent.mkdir(parents=True, exist_ok=True)
    data["evidence_path"] = str(path)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return str(path)


def _cmd_bringup_summary(args: argparse.Namespace) -> int:
    """Summarize a folder of redacted physical bring-up evidence files."""
    import json

    from jarvis import __version__
    from jarvis.deploy import summarize_bringup_evidence
    from jarvis.deploy import current_release_ref

    expected_version = args.expect_version or ""
    expected_release_ref = args.expect_release_ref or ""
    if args.expect_current_release:
        expected_version = __version__
        expected_release_ref = current_release_ref()

    data = summarize_bringup_evidence(
        args.path,
        expected_roles=args.expected_roles or (),
        expected_version=expected_version,
        expected_release_ref=expected_release_ref,
        min_files=args.min_files,
    )
    output_path: str | None = None
    if args.output:
        output_path = _resolve_bringup_summary_path(args.output)
        data["summary_path"] = output_path
        _write_bringup_summary(data, output_path)
    if args.json:
        print(json.dumps(data, indent=2, sort_keys=True))
        return 0 if data["ok"] else 1

    print(f"Jarvis bring-up summary: {data['path']}")
    print(f"  files:        {data['file_count']}")
    print(f"  versions:     {', '.join(data['versions_seen']) or '(none)'}")
    print(f"  release refs: {', '.join(data.get('release_refs_seen', [])) or '(none)'}")
    print(f"  platforms:    {', '.join(data['platforms_seen']) or '(none)'}")
    print(f"  roles:        {', '.join(data['roles_seen']) or '(none)'}")
    for entry in data.get("entries", []):
        if not isinstance(entry, dict):
            continue
        checks = [
            "packages" if entry.get("packages_ok") else "packages!",
            "services" if entry.get("services_ok") else "services!",
        ]
        if entry.get("hardware_checked"):
            checks.append("hardware" if entry.get("hardware_ok") else "hardware!")
        if entry.get("brain_checked"):
            checks.append("brain" if entry.get("brain_paired") else "brain!")
        print(
            f"  - {entry.get('platform', 'unknown')} "
            f"{','.join(entry.get('roles', []) or ['none'])}: {', '.join(checks)}"
        )
    issues = data.get("issues", [])
    if issues:
        print("\nIssues:")
        for issue in issues:
            print(f"  - {issue}")
        if output_path:
            print(f"\nSummary: {output_path}")
        return 1
    if output_path:
        print(f"\nSummary: {output_path}")
    print("\nAll summarized evidence checks passed.")
    return 0


def _resolve_bringup_summary_path(output: str) -> str:
    """Return a summary file path from a chosen file or directory."""
    from pathlib import Path

    target = Path(output).expanduser()
    path = target if target.suffix.lower() == ".json" else target / "jarvis-fleet-summary.json"
    return str(path)


def _write_bringup_summary(data: dict[str, object], output_path: str) -> str:
    """Write summary JSON to the resolved output path."""
    import json
    from pathlib import Path

    path = Path(output_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return str(path)


def _cmd_traces(args: argparse.Namespace) -> int:
    """View recent per-turn pipeline traces."""
    import json
    import pathlib

    from jarvis.intercom.metrics import summary as intercom_summary

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
        if kind == "intercom":
            print(f"  {clock(d)}  {intercom_summary(d)}")
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
        sync_role_dependencies,
    )

    if args.service_action == "extras":
        print(" ".join(role_extras(set(args.roles))))
        return 0

    if args.service_action == "sync":
        print("syncing role dependencies: " + " ".join(role_extras(set(args.roles))))
        result = sync_role_dependencies(args.roles)
        return result.returncode

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


def _cmd_mic(args: argparse.Namespace) -> int:
    """Operator shortcut for this machine's intercom listener."""
    from jarvis.deploy import control_service

    def run(action: str, *, emit: bool = True):
        result = control_service("intercom", action, platform_name=args.platform)
        if emit and result.stdout:
            print(result.stdout, end="")
        if emit and result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        return result

    if args.mic_action == "off":
        disable = run("disable")
        stop = run("stop", emit=False)
        if disable.returncode == 0 and stop.returncode == 0:
            print("Jarvis mic/listener off: intercom disabled and stopped.")
            return 0
        if stop.stdout:
            print(stop.stdout, end="")
        if stop.stderr:
            print(stop.stderr, end="", file=sys.stderr)
        return stop.returncode or disable.returncode

    if args.mic_action == "on":
        enable = run("enable")
        start = run("start")
        if enable.returncode == 0 and start.returncode == 0:
            print("Jarvis mic/listener on: intercom enabled and started.")
            return 0
        return start.returncode or enable.returncode

    return run("status").returncode


def _cmd_pair(args: argparse.Namespace) -> int:
    """Pairing helpers for fleet onboarding."""
    from jarvis.deploy import (
        issue_pairing_entry,
        render_mac_config_command,
        render_pi_installer_command,
        upsert_brain_device_entry,
    )

    if (args.pi_installer or args.mac_config) and not args.brain_host:
        print(
            "--brain-host is required with --pi-installer or --mac-config",
            file=sys.stderr,
        )
        return 2

    token, fragment = issue_pairing_entry(args.device_id, identity=args.identity or "")
    brain_config_path = ""
    brain_devices_count: int | None = None
    if args.apply_brain_config:
        try:
            devices = upsert_brain_device_entry(
                args.env_file,
                fragment,
                brain_bind_host=args.brain_bind_host,
            )
        except ValueError as exc:
            print(f"Could not update brain config: {exc}", file=sys.stderr)
            return 2
        brain_config_path = str(Path(args.env_file).expanduser())
        brain_devices_count = len(devices)
    if args.json:
        import json

        payload = {"token": token, "brain_devices_entry": fragment}
        if brain_config_path:
            payload["brain_config_path"] = brain_config_path
            payload["brain_devices_count"] = brain_devices_count
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
        if brain_config_path:
            print(
                f"Updated BRAIN_DEVICES in {brain_config_path} "
                f"({brain_devices_count} configured device(s))."
            )
        if brain_config_path:
            print("Applied this object to BRAIN_DEVICES on the brain:")
        else:
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
        if brain_config_path:
            print(
                f"\nUpdated BRAIN_DEVICES in {brain_config_path} "
                f"({brain_devices_count} configured device(s))."
            )
        if brain_config_path:
            print("\nApplied this object to BRAIN_DEVICES on the brain:")
        else:
            print("\nAdd this object to BRAIN_DEVICES on the brain:")
        print(fragment)
    return 0


def _cmd_setup(args: argparse.Namespace) -> int:
    """Read, apply, and validate packaged setup state for the macOS app."""
    import json

    from jarvis.setup import apply_setup, read_setup, validate_setup

    if args.setup_action == "read":
        print(json.dumps(read_setup(args.env_file), indent=2, sort_keys=True))
        return 0
    if args.setup_action == "apply":
        try:
            payload = json.loads(sys.stdin.read() or "{}")
            result = apply_setup(args.env_file, payload)
        except (json.JSONDecodeError, ValueError) as exc:
            print(f"Could not apply setup: {exc}", file=sys.stderr)
            return 2
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    result = validate_setup(args.env_file, args.roles or [])
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


def _cmd_whatsapp_auth(args: argparse.Namespace) -> int:
    """Run WhatsApp QR auth through wacli and return redacted JSON output."""
    import json

    from jarvis.setup import whatsapp_auth

    result = whatsapp_auth(wacli_bin=args.wacli_bin, account=args.account)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else int(result.get("returncode") or 1)


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

    p_whatsapp_auth = sub.add_parser(
        "whatsapp-auth", help="Authenticate wacli and return QR/progress output as JSON"
    )
    p_whatsapp_auth.add_argument("--json", action="store_true", help="Print JSON output")
    p_whatsapp_auth.add_argument("--wacli-bin", default="wacli", help="wacli executable")
    p_whatsapp_auth.add_argument("--account", default="", help="Optional wacli account")
    p_whatsapp_auth.set_defaults(func=_cmd_whatsapp_auth)

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
        "google-setup", help="One-time: OAuth for the house email/calendar adapter (gogcli)"
    )
    p_gsetup.add_argument(
        "--account",
        default="",
        help="gogcli account email or existing alias to authenticate and store in the house bindings.",
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

    p_runs = sub.add_parser("runs", help="List/show Jarvis orchestration runs")
    p_runs.add_argument("run_id", nargs="?", help="Run id or unique prefix to show")
    p_runs.add_argument("--create", metavar="OBJECTIVE", help="Create a local fake run for inspection")
    p_runs.add_argument("--events", metavar="RUN_ID", help="Show append-only events for a run")
    p_runs.add_argument("--sync", action="store_true", help="Refresh linked worker job status before listing/showing")
    p_runs.add_argument("-n", type=int, default=20, help="How many recent runs to list")
    p_runs.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    p_runs.set_defaults(func=_cmd_runs)

    p_workers = sub.add_parser("workers", help="List named orchestration workers")
    p_workers.add_argument("--probe", action="store_true", help="Probe worker health and current job capacity")
    p_workers.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    p_workers.set_defaults(func=_cmd_workers)

    p_work = sub.add_parser("work", help="Work-source orchestration commands")
    work_sub = p_work.add_subparsers(dest="work_action", required=True)
    p_work_intent = work_sub.add_parser("intent", help="Parse text into a structured WorkCommand")
    p_work_intent.add_argument("phrase", nargs="+")
    p_work_intent.set_defaults(func=_cmd_work)
    p_work_check = work_sub.add_parser("check", help="List work items from a source")
    p_work_check.add_argument("phrase", nargs="*", help="Optional natural-language filter phrase")
    p_work_check.add_argument("--source", choices=["github", "linear"], default="")
    p_work_check.add_argument("--repo", default="", help="Repository, e.g. roughcoder/jarvis")
    p_work_check.add_argument("--limit", type=int, default=10)
    p_work_check.add_argument("--json", action="store_true")
    p_work_check.set_defaults(func=_cmd_work)
    p_work_next = work_sub.add_parser("next", help="Select the next eligible work item")
    p_work_next.add_argument("phrase", nargs="*", help="Optional natural-language filter phrase")
    p_work_next.add_argument("--source", choices=["github", "linear"], default="")
    p_work_next.add_argument("--repo", default="", help="Repository, e.g. roughcoder/jarvis")
    p_work_next.add_argument("--worker", default="", help="Explicit worker_id target")
    p_work_next.add_argument("--start", action="store_true", help="Start a worker job for the selected item")
    p_work_next.add_argument("--json", action="store_true")
    p_work_next.set_defaults(func=_cmd_work)
    p_pr_comments = work_sub.add_parser("pr-comments", help="Inspect GitHub PR review/comment objects")
    p_pr_comments.add_argument("number", type=int)
    p_pr_comments.add_argument("--repo", default="", help="Repository, e.g. roughcoder/jarvis")
    p_pr_comments.add_argument("--source", choices=["github"], default="")
    p_pr_comments.add_argument("--json", action="store_true")
    p_pr_comments.set_defaults(func=_cmd_work)
    p_work_resume = work_sub.add_parser("resume", help="Show a run to resume or inspect")
    p_work_resume.add_argument("run_id")
    p_work_resume.add_argument("--source", default="")
    p_work_resume.set_defaults(func=_cmd_work)

    p_schedules = sub.add_parser("schedules", help="List/add/tick scheduled WorkCommands")
    sched_sub = p_schedules.add_subparsers(dest="schedule_action", required=False)
    p_sched_list = sched_sub.add_parser("list", help="List schedules")
    p_sched_list.add_argument("--json", action="store_true")
    p_sched_list.set_defaults(func=_cmd_schedules)
    p_sched_add = sched_sub.add_parser("add", help="Add a daily/weekly scheduled WorkCommand")
    p_sched_add.add_argument("phrase", nargs="+", help="Natural-language command to store structurally")
    p_sched_add.add_argument("--at", required=True, help="HH:MM local time")
    p_sched_add.add_argument("--weekdays", default="", help="weekdays, weekends, or comma list like mon,wed,sat")
    p_sched_add.add_argument("--timezone", default="")
    p_sched_add.add_argument("--mode", choices=["one_shot", "campaign"], default="one_shot")
    p_sched_add.add_argument("--name", default="")
    p_sched_add.set_defaults(func=_cmd_schedules)
    p_sched_tick = sched_sub.add_parser("tick", help="Return schedules due at the current/simulated minute")
    p_sched_tick.add_argument("--now", default="", help="ISO datetime for deterministic checks")
    p_sched_tick.add_argument("--ack", action="store_true", help="Mark returned schedules fired after the caller handled them")
    p_sched_tick.add_argument("--json", action="store_true")
    p_sched_tick.set_defaults(func=_cmd_schedules)
    p_schedules.set_defaults(func=_cmd_schedules, schedule_action="list", json=False)

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

    p_bringup = sub.add_parser(
        "bringup", help="Collect redacted deployment bring-up evidence"
    )
    p_bringup.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable bring-up evidence",
    )
    p_bringup.add_argument(
        "--role",
        dest="roles",
        action="append",
        default=None,
        choices=["brain", "intercom", "worker", "whatsapp"],
        help="Role to check; repeat for multiple roles (default: all roles)",
    )
    p_bringup.add_argument(
        "--platform",
        choices=["launchd", "systemd"],
        help="Override platform detection",
    )
    p_bringup.add_argument(
        "--hardware",
        action="store_true",
        help="Include local microphone, speaker, and camera listings where available",
    )
    p_bringup.add_argument(
        "--check-brain",
        action="store_true",
        help="Probe configured brain reachability and pairing",
    )
    p_bringup.add_argument(
        "--brain-host",
        default="",
        help="Override INTERCOM_BRAIN_HOST and probe brain reachability",
    )
    p_bringup.add_argument(
        "--brain-port",
        default="",
        help="Override INTERCOM_BRAIN_PORT for brain reachability",
    )
    p_bringup.add_argument(
        "--output",
        default="",
        help=(
            "Write redacted JSON evidence to this .json file, or create a "
            "timestamped file when a directory is provided"
        ),
    )
    p_bringup.set_defaults(func=_cmd_bringup)

    p_bringup_summary = sub.add_parser(
        "bringup-summary", help="Summarize redacted physical bring-up evidence files"
    )
    p_bringup_summary.add_argument(
        "path",
        help="Evidence JSON file or directory created by `jarvis bringup --output`",
    )
    p_bringup_summary.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable summary",
    )
    p_bringup_summary.add_argument(
        "--expect-role",
        dest="expected_roles",
        action="append",
        default=None,
        choices=["brain", "intercom", "worker", "whatsapp"],
        help="Require at least one evidence file containing this role; repeatable",
    )
    p_bringup_summary.add_argument(
        "--expect-version",
        default="",
        help="Require every evidence file to report this Jarvis runtime version",
    )
    p_bringup_summary.add_argument(
        "--expect-release-ref",
        default="",
        help="Require every evidence file to report this Jarvis release ref",
    )
    p_bringup_summary.add_argument(
        "--expect-current-release",
        action="store_true",
        help="Require evidence to match this installed Jarvis version and release tag",
    )
    p_bringup_summary.add_argument(
        "--min-files",
        type=int,
        default=0,
        help="Require at least this many valid evidence files",
    )
    p_bringup_summary.add_argument(
        "--output",
        default="",
        help=(
            "Write the summary JSON to this .json file, or to "
            "jarvis-fleet-summary.json when a directory is provided"
        ),
    )
    p_bringup_summary.set_defaults(func=_cmd_bringup_summary)

    p_traces = sub.add_parser("traces", help="View recent per-turn pipeline traces")
    p_traces.add_argument("-n", type=int, default=20, help="How many recent traces")
    p_traces.set_defaults(func=_cmd_traces)

    p_mic = sub.add_parser(
        "mic", help="Turn this machine's intercom microphone listener on/off"
    )
    p_mic.add_argument(
        "mic_action",
        choices=["off", "on", "status"],
        help="off disables/stops the intercom; on enables/starts it",
    )
    p_mic.add_argument(
        "--platform", choices=["launchd", "systemd"], help="Override platform detection"
    )
    p_mic.set_defaults(func=_cmd_mic)

    p_service = sub.add_parser(
        "service", help="Install/control Jarvis launchd/systemd services"
    )
    p_service.add_argument(
        "service_action",
        choices=[
            "install",
            "print",
            "start",
            "stop",
            "restart",
            "status",
            "enable",
            "disable",
            "extras",
            "sync",
        ],
        help="install/print render service files; start/stop/restart/status/enable/disable controls one role; extras prints uv extras; sync installs role dependencies",
    )
    p_service.add_argument(
        "roles",
        nargs="+",
        choices=["brain", "intercom", "worker", "whatsapp"],
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
    p_pair.add_argument(
        "--apply-brain-config",
        action="store_true",
        help="Upsert the issued device entry into BRAIN_DEVICES in --env-file",
    )
    p_pair.add_argument(
        "--env-file",
        default="~/.jarvis/.env",
        help="Brain dotenv file to update with --apply-brain-config",
    )
    p_pair.add_argument(
        "--brain-bind-host",
        default="",
        help="Also set BRAIN_HOST in --env-file, for example 0.0.0.0 on a brain Mac",
    )
    p_pair.set_defaults(func=_cmd_pair)

    p_setup = sub.add_parser(
        "setup", help="Read/apply/validate packaged setup state for Jarvis.app"
    )
    p_setup.add_argument(
        "setup_action",
        choices=["read", "apply", "validate"],
        help="read current state, apply JSON from stdin, or validate selected roles",
    )
    p_setup.add_argument(
        "--env-file",
        default="~/.jarvis/.env",
        help="Dotenv file to read or update",
    )
    p_setup.add_argument(
        "--role",
        dest="roles",
        action="append",
        default=[],
        choices=["brain", "intercom", "worker", "whatsapp"],
        help="Role to validate; repeatable",
    )
    p_setup.add_argument("--json", action="store_true", help="Accepted for app symmetry")
    p_setup.set_defaults(func=_cmd_setup)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
