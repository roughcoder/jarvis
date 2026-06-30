"""Worker daemon — an aiohttp service the brain dispatches to (Phase 3c).

Standalone and self-contained: imports nothing from the brain. Auth is a bearer
token. Fast actions (shell, screenshot, applescript) return synchronously; `code`
(a headless coding-agent run) starts a background job and returns its id at once.

Endpoints:
  POST /run        {action, args}            -> result, or {job_id} for `code`
  GET  /jobs/{id}                            -> job status + output
  GET  /jobs                                 -> recent jobs
  POST /sessions                             -> create a live provider session record
  GET  /sessions[/id][/events]               -> inspect session state/events
  POST /sessions/{id}/{turns,input,approval,interrupt,stop}
  GET  /health                               -> liveness
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import pathlib
import subprocess
import uuid

from aiohttp import web

from jarvis.capabilities import (
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_STOP,
    WORKER_SESSION_TURN,
)
from jarvis.config import WorkerConfig
from jarvis.engines import engine_ids, normalize_engine_id, worker_supports_engine
from jarvis.worker.authority import WorkerSessionAuthority
from jarvis.worker.actions import (
    capture_screen_jpeg_b64,
    cleanup_job,
    clone_repo,
    code_argv,
    gui_doctor,
    list_repos,
    prepare_worktree,
    resolve_repo,
    run_applescript,
    run_exec,
    run_peekaboo,
    run_shell,
    take_screenshot,
)
from jarvis.worker.jobs import JobManager, slugify
from jarvis.worker.providers import ProviderTurn, provider_for
from jarvis.worker.sessions import SessionManager, WorkerSession
from jarvis.worker_session_contract import (
    CHECKPOINT_ID_KEY,
    EVENT_APPROVAL_RESOLVED,
    EVENT_CHECKPOINT_RESTORED,
    EVENT_INPUT_RECEIVED,
    EVENT_SESSION_INTERRUPTED,
    EVENT_SESSION_STOPPED,
    EVENT_TURN_FAILED,
    SESSION_FAILED,
    SESSION_INTERRUPTED,
    SESSION_STOPPED,
)


def _peekaboo_env(cfg: WorkerConfig) -> dict:
    """peekaboo agent's AI-provider env (for `control_mac`). Empty base URL => direct
    OpenAI; the LiteLLM gateway URL => route the agent through the proxy."""
    env: dict[str, str] = {}
    if cfg.peekaboo_ai_providers:
        env["PEEKABOO_AI_PROVIDERS"] = cfg.peekaboo_ai_providers
    if cfg.peekaboo_openai_base_url:
        env["OPENAI_BASE_URL"] = cfg.peekaboo_openai_base_url
    key = cfg.peekaboo_openai_api_key.get_secret_value()
    if key:
        env["OPENAI_API_KEY"] = key
    or_key = cfg.peekaboo_openrouter_api_key.get_secret_value()
    if or_key:
        env["OPENROUTER_API_KEY"] = or_key
    return env


def _summarize(args: dict) -> str:
    """Compact one-line view of a request's args for the worker log — long/binary
    fields (commands, base64 images) truncated so the log stays readable."""
    parts = []
    for k, v in args.items():
        if k == "cwd":
            continue
        s = str(v)
        parts.append(f"{k}={s[:57] + '…' if len(s) > 60 else s}")
    return " ".join(parts) or "(no args)"


def _shell_env(cfg: WorkerConfig) -> dict:
    """Resolve the allowlisted secrets (WORKER_SHELL_SECRETS) into a name→value env,
    read from the worker's environment or its local .env. These are injected into
    shell commands so Jarvis can USE them by name without ever seeing the value."""
    names = [n.strip() for n in cfg.shell_secrets.split(",") if n.strip()]
    if not names:
        return {}
    from dotenv import dotenv_values

    dotenv = dotenv_values(".env")  # reads .env WITHOUT mutating os.environ
    env: dict[str, str] = {}
    for n in names:
        v = os.environ.get(n) or dotenv.get(n)
        if v:
            env[n] = v
    return env


def _workspace_error(path: pathlib.Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "--is-inside-work-tree"],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode == 0 and result.stdout.strip() == "true":
        return f"worker workspace {path} is inside a git checkout; set WORKER_WORKSPACE to an external absolute path"
    return ""


def _worker_workspace(cfg: WorkerConfig) -> pathlib.Path:
    workspace = pathlib.Path(cfg.workspace).expanduser().resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    if err := _workspace_error(workspace):
        raise ValueError(err)
    return workspace


def make_app(cfg: WorkerConfig) -> web.Application:
    workspace = _worker_workspace(cfg)
    # Persist jobs to disk under the workspace so they survive a daemon restart.
    jobs = JobManager(store_dir=str(workspace / "jobs"))
    sessions = SessionManager(store_dir=str(workspace / "sessions"))

    # Browser lane: one lazily-created BrowserHost per process (own config slice, read
    # from env like the worker's). nodriver is imported only on first use.
    from jarvis.config import BrowserConfig

    browser_cfg = BrowserConfig()
    browser_holder: dict = {}

    async def browser_dispatch(action: str, args: dict) -> web.Response:
        if not browser_cfg.enabled:
            return web.json_response({"ok": False, "error": "browser lane disabled (BROWSER_ENABLED=false)"})
        if action == "browser_doctor":
            from jarvis.browser import browser_doctor

            return web.json_response({"ok": True, **browser_doctor(browser_cfg)})
        host = browser_holder.get("h")
        if host is None:
            from jarvis.browser import BrowserHost

            host = BrowserHost(browser_cfg)
            browser_holder["h"] = host
        ctx = (args.get("context") or browser_cfg.default_context).strip()
        if action == "browser_open":
            data = await host.open(args.get("url", ""), ctx)
        elif action == "browser_snapshot":
            data = await host.snapshot(ctx)
        elif action == "browser_click":
            data = await host.click(int(args.get("ref", 0)), ctx)
        elif action == "browser_type":
            data = await host.type(
                int(args.get("ref", 0)), args.get("text", ""), ctx, submit=bool(args.get("submit"))
            )
        elif action == "browser_press":
            data = await host.press(args.get("keys"), ctx, ref=args.get("ref"))
        elif action == "browser_read":
            data = await host.read(ctx)
        else:
            return web.json_response({"error": f"unknown action {action!r}"}, status=400)
        return web.json_response(data)

    def authorised(request: web.Request) -> bool:
        token = cfg.token.get_secret_value()
        if not token:
            return True  # no token configured => open (dev/local)
        return request.headers.get("Authorization", "") == f"Bearer {token}"

    async def run(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "bad json"}, status=400)
        action = body.get("action")
        args = body.get("args") or {}
        cwd = args.get("cwd") or str(workspace)
        if cfg.verbose:
            print(f"[worker] → {action}  {_summarize(args)}")

        if action == "shell":
            out = await run_shell(
                args.get("command", ""), cwd, cfg.shell_timeout_s, env=_shell_env(cfg) or None
            )
            return web.json_response({"ok": True, "output": out})
        if action == "applescript":
            out = await run_applescript(args.get("script", ""), cfg.shell_timeout_s)
            return web.json_response({"ok": True, "output": out})
        if action == "screenshot":
            shots = str(workspace / "screenshots")
            out = await take_screenshot(shots, args.get("name"), cfg.shell_timeout_s)
            return web.json_response({"ok": True, "output": out})
        if action == "code":
            policy_error = _code_policy_error(args)
            if policy_error:
                return web.json_response({"ok": False, "error": policy_error}, status=403)
            agent = normalize_engine_id(args.get("agent") or cfg.agent)
            supported_engines = engine_ids(cfg.supported_engines, default_engine=cfg.agent)
            if not worker_supports_engine(supported_engines, agent):
                return web.json_response({"ok": False, "error": f"worker does not support engine {agent!r}"}, status=400)
            session_id = str(args.get("session_id") or "").strip()
            session_name = str(args.get("session_name") or args.get("name") or "").strip()
            resume_session = bool(args.get("resume_session"))
            try:
                argv = code_argv(
                    agent,
                    cfg.codex_bin,
                    cfg.claude_bin,
                    args.get("prompt", ""),
                    session_id=session_id,
                    session_name=session_name,
                    resume_session=resume_session,
                )
            except ValueError as exc:
                return web.json_response({"ok": False, "error": str(exc)}, status=400)
            label = args.get("prompt", "")[:80] or agent
            slug = slugify(args.get("name") or args.get("prompt") or "job")
            branch = None
            resolved = ""
            cleanup_owned = True
            if args.get("cwd") and resume_session:
                if not session_id:
                    return web.json_response({"ok": False, "error": "resume cwd requires session_id"}, status=400)
                job_cwd, err = _resume_cwd(str(args["cwd"]), workspace)
                if err:
                    return web.json_response({"ok": False, "error": err}, status=400)
                branch = str(args.get("branch") or "") or None
                resolved = str(args.get("repo") or "")
                cleanup_owned = False
            elif args.get("repo"):
                # Resolve the repo name to a real path (clone it if missing) before
                # isolating it on a fresh worktree branch — never the user's checkout.
                resolved = resolve_repo(args["repo"], cfg.repo_root)
                if resolved is None and cfg.clone_missing and cfg.repo_root:
                    resolved, clone_err = await clone_repo(
                        args["repo"], cfg.repo_root, cfg.clone_timeout_s
                    )
                    if resolved is None:
                        return web.json_response({"ok": False, "error": clone_err}, status=400)
                if resolved is None:
                    avail = list_repos(cfg.repo_root)
                    hint = f" I can see: {', '.join(avail)}." if avail else ""
                    return web.json_response(
                        {"ok": False, "error": f"couldn't find a repo called {args['repo']!r}.{hint}"},
                        status=404,
                    )
                job_cwd, branch, err = await prepare_worktree(
                    resolved, str(workspace / "worktrees"), slug,
                    cfg.worktree_branch_prefix, cfg.shell_timeout_s,
                )
                if err:
                    return web.json_response({"ok": False, "error": err}, status=400)
            else:
                # No repo: an isolated per-job scratch dir.
                job_cwd = str(workspace / "runs" / f"{slug}-{uuid.uuid4().hex[:6]}")
                pathlib.Path(job_cwd).mkdir(parents=True, exist_ok=True)
            job = jobs.start(
                "code",
                label,
                run_exec(argv, job_cwd, cfg.job_timeout_s),
                name=args.get("name", ""),
                engine=agent,
                cwd=job_cwd,
                branch=branch,
                repo=resolved or "",
                session_id=session_id or None,
                session_name=session_name,
                cleanup_owned=cleanup_owned,
            )
            return web.json_response(
                {
                    "ok": True,
                    "job_id": job.id,
                    "name": job.name,
                    "branch": branch,
                    "cwd": job.cwd,
                    "status": "running",
                    "engine": job.engine,
                    "session_id": job.session_id or "",
                    "session_name": job.session_name,
                }
            )
        if action == "peekaboo":
            argv = args.get("argv") or []
            if cfg.verbose:
                print(f"[worker]   peekaboo {' '.join(map(str, argv))}")
            # the agent is multi-step → its own (longer) budget; atomic calls stay fast.
            timeout = cfg.peekaboo_agent_timeout_s if argv[:1] == ["agent"] else cfg.shell_timeout_s
            out = await run_peekaboo(
                cfg.peekaboo_bin, argv, timeout, env=_peekaboo_env(cfg) or None
            )
            if cfg.verbose:
                print(f"[worker]   ← {out}")  # FULL output (not truncated like the brain log)
            return web.json_response({"ok": True, "output": out})
        if action == "gui_doctor":
            return web.json_response({"ok": True, **gui_doctor(cfg.peekaboo_bin)})
        if action == "capture":
            b64, err = await capture_screen_jpeg_b64(cfg.shell_timeout_s)
            if err:
                return web.json_response({"ok": False, "error": err})
            return web.json_response({"ok": True, "image_b64": b64})
        if action and action.startswith("browser"):
            return await browser_dispatch(action, args)
        if action == "list_repos":
            return web.json_response({"ok": True, "repos": list_repos(cfg.repo_root)})
        if action == "cleanup":
            ref = (args.get("job") or "").strip()
            finished = {"done", "error", "interrupted"}
            if ref in ("", "finished", "all", "done"):
                targets = [j for j in jobs.recent(1000) if j.status in finished]
            else:
                j = jobs.get(ref) or jobs.find(ref)
                if j is None:
                    return web.json_response({"ok": False, "error": f"no job {ref!r}"}, status=404)
                if j.status not in finished:
                    return web.json_response({"ok": False, "error": f"job {j.name!r} is still running"})
                targets = [j]
            in_use = _running_workspace_refs(jobs.recent(1000))
            cleaned = []
            for j in targets:
                if j.cleanup_owned and _workspace_in_use(j, in_use):
                    continue
                if j.cleanup_owned:
                    await cleanup_job(
                        j.repo,
                        j.cwd,
                        j.branch,
                        cfg.shell_timeout_s,
                        owned_roots=[str(workspace / "runs"), str(workspace / "worktrees")],
                    )
                jobs.remove(j.id)
                cleaned.append(j.name)
            return web.json_response({"ok": True, "cleaned": cleaned})
        return web.json_response({"error": f"unknown action {action!r}"}, status=400)

    def _code_policy_error(args: dict) -> str:
        envelope = args.get("execution_envelope")
        if envelope is None:
            return ""
        if not isinstance(envelope, dict):
            return "execution_envelope must be an object"
        allowed_actions = set(envelope.get("allowed_actions") or [])
        if "worker.job.start" not in allowed_actions:
            return "execution envelope does not allow worker.job.start"
        landing = envelope.get("landing") or {}
        if not isinstance(landing, dict):
            return "execution envelope landing policy must be an object"
        if landing.get("allow_merge") is True:
            return "execution envelope cannot allow merge at worker dispatch"
        if landing.get("mode") in {"merge", "release"}:
            return "execution envelope landing mode is not allowed at worker dispatch"
        return ""

    async def get_job(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        job_id = request.match_info["id"]
        if job_id == "latest":
            job = jobs.latest()
        else:  # exact id, else fuzzy match by name/label
            job = jobs.get(job_id) or jobs.find(job_id)
        if job is None:
            return web.json_response({"error": "no such job"}, status=404)
        return web.json_response(job.public())

    async def list_jobs(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return web.json_response({"jobs": [j.public() for j in jobs.recent()]})

    async def create_session(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            body = await request.json()
            WorkerSessionAuthority.for_session_create(body or {})
            body = await _prepare_session_body(body or {}, cfg, workspace)
            session, event = sessions.create(body or {})
        except (RuntimeError, ValueError) as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        except Exception:
            return web.json_response({"ok": False, "error": "bad json"}, status=400)
        return web.json_response({"ok": True, "session": session.to_dict(), "event": event.to_dict()})

    async def list_sessions(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return web.json_response({"sessions": [session.to_dict() for session in sessions.list()]})

    async def get_session(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        session = sessions.get(request.match_info["id"])
        if session is None:
            return web.json_response({"error": "no such session"}, status=404)
        return web.json_response(session.to_dict())

    async def get_session_events(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        session_id = request.match_info["id"]
        if sessions.get(session_id) is None:
            return web.json_response({"error": "no such session"}, status=404)
        limit = _query_limit(request.query.get("limit"))
        return web.json_response(
            {
                "events": [
                    event.to_dict()
                    for event in sessions.events(
                        session_id,
                        after=str(request.query.get("after") or ""),
                        limit=limit,
                    )
                ]
            }
        )

    async def get_session_requests(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        session_id = request.match_info["id"]
        if sessions.get(session_id) is None:
            return web.json_response({"error": "no such session"}, status=404)
        return web.json_response({"requests": sessions.pending_requests(session_id)})

    async def list_session_requests(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return web.json_response({"requests": sessions.pending_requests()})

    async def get_session_checkpoints(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        session_id = request.match_info["id"]
        if sessions.get(session_id) is None:
            return web.json_response({"error": "no such session"}, status=404)
        return web.json_response({"checkpoints": sessions.checkpoints(session_id)})

    async def restore_session_checkpoint(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        session_id = request.match_info["id"]
        session = sessions.get(session_id)
        if session is None:
            return web.json_response({"error": "no such session"}, status=404)
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "bad json"}, status=400)
        checkpoint_id = str(body.get(CHECKPOINT_ID_KEY) or "").strip()
        if not checkpoint_id:
            return web.json_response({"error": "checkpoint_id is required"}, status=400)
        known = {x[CHECKPOINT_ID_KEY] for x in sessions.checkpoints(session.session_id)}
        if checkpoint_id not in known:
            return web.json_response({"error": "no such checkpoint"}, status=404)
        try:
            adapter = provider_for(session.provider)
        except ValueError as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        handler = getattr(adapter, "restore_checkpoint", None)
        request_data = {CHECKPOINT_ID_KEY: checkpoint_id, "metadata": dict(body.get("metadata") or {})}
        if handler is None:
            event = sessions.append_event(session.session_id, EVENT_CHECKPOINT_RESTORED, request_data)
        else:
            event = handler(session=session, request=request_data, sessions=sessions)
        return web.json_response({"ok": True, "event": event.to_dict()})

    async def start_session_turn(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        session_id = request.match_info["id"]
        session = sessions.get(session_id)
        if session is None:
            return web.json_response({"error": "no such session"}, status=404)
        authority_error = _require_session_authority(session, WORKER_SESSION_TURN)
        if authority_error is not None:
            return authority_error
        cwd_error = _session_cwd_error(session, workspace)
        if cwd_error:
            return web.json_response({"ok": False, "error": cwd_error}, status=400)
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "bad json"}, status=400)
        turn_id = _clean_request_id(body.get("turn_id") or uuid.uuid4().hex, "turn")
        idempotency_key = str(body.get("idempotency_key") or "").strip()
        existing_events = _events_for_idempotency(sessions, session.session_id, idempotency_key)
        if existing_events:
            existing_turn_id = str(existing_events[0].data.get("turn_id") or turn_id)
            return web.json_response(
                {
                    "ok": True,
                    "session": session.to_dict(),
                    "turn_id": existing_turn_id,
                    "events": [event.to_dict() for event in existing_events],
                    "idempotent": True,
                }
            )
        try:
            adapter = provider_for(session.provider)
        except ValueError as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        turn_data = {
            "turn_id": turn_id,
            "prompt": str(body.get("prompt") or ""),
            "metadata": dict(body.get("metadata") or {}),
            "idempotency_key": idempotency_key,
        }
        try:
            session, started, reserved = sessions.reserve_turn(session.session_id, turn_data)
        except RuntimeError as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=409)
        if not reserved:
            return web.json_response(
                {
                    "ok": True,
                    "session": session.to_dict(),
                    "turn_id": str(started.data.get("turn_id") or turn_id),
                    "events": [started.to_dict()],
                    "idempotent": True,
                }
            )
        try:
            provider_events = adapter.start_turn(
                session=session,
                turn=ProviderTurn(
                    turn_id=turn_id,
                    prompt=str(body.get("prompt") or ""),
                    metadata=dict(body.get("metadata") or {}),
                    idempotency_key=idempotency_key,
                ),
                sessions=sessions,
                worker_cfg=cfg,
            )
        except RuntimeError as exc:
            sessions.update_status(session.session_id, SESSION_FAILED)
            failed = sessions.append_event(
                session.session_id,
                EVENT_TURN_FAILED,
                {"turn_id": turn_id, "idempotency_key": idempotency_key, "error": str(exc)},
            )
            return web.json_response(
                {
                    "ok": False,
                    "error": str(exc),
                    "session": sessions.get(session.session_id).to_dict(),  # type: ignore[union-attr]
                    "turn_id": turn_id,
                    "events": [started.to_dict(), failed.to_dict()],
                },
                status=400,
            )
        return web.json_response(
            {
                "ok": True,
                "session": sessions.get(session.session_id).to_dict(),  # type: ignore[union-attr]
                "turn_id": turn_id,
                "events": [started.to_dict(), *[event.to_dict() for event in provider_events]],
            }
        )

    async def session_input(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return await _provider_control_event(request, sessions, "input")

    async def session_approval(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return await _provider_control_event(request, sessions, "approval")

    async def session_interrupt(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return await _provider_terminal_event(request, sessions, "interrupt")

    async def session_stop(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return await _provider_terminal_event(request, sessions, "stop")

    async def health(_request: web.Request) -> web.Response:
        return web.json_response(
            {
                "ok": True,
                "agent": cfg.agent,
                "default_engine": normalize_engine_id(cfg.agent),
                "supported_engines": engine_ids(cfg.supported_engines, default_engine=cfg.agent),
                "workspace": str(workspace),
                "repo_root_configured": bool(cfg.repo_root),
                "browser_enabled": browser_cfg.enabled,
                "gui_provider_configured": bool(cfg.peekaboo_ai_providers),
            }
        )

    app = web.Application()
    app["browser_holder"] = browser_holder  # for clean shutdown in serve()
    app["browser_cfg"] = browser_cfg
    app.add_routes([
        web.post("/run", run),
        web.get("/jobs/{id}", get_job),
        web.get("/jobs", list_jobs),
        web.post("/sessions", create_session),
        web.get("/sessions", list_sessions),
        web.get("/sessions/requests", list_session_requests),
        web.get("/sessions/{id}/events", get_session_events),
        web.get("/sessions/{id}/requests", get_session_requests),
        web.get("/sessions/{id}/checkpoints", get_session_checkpoints),
        web.post("/sessions/{id}/checkpoints/restore", restore_session_checkpoint),
        web.post("/sessions/{id}/turns", start_session_turn),
        web.post("/sessions/{id}/input", session_input),
        web.post("/sessions/{id}/approval", session_approval),
        web.post("/sessions/{id}/interrupt", session_interrupt),
        web.post("/sessions/{id}/stop", session_stop),
        web.get("/sessions/{id}", get_session),
        web.get("/health", health),
    ])
    return app


async def _append_session_control_event(
    request: web.Request,
    sessions: SessionManager,
    event_type: str,
) -> web.Response:
    session_id = request.match_info["id"]
    session = sessions.get(session_id)
    if session is None:
        return web.json_response({"error": "no such session"}, status=404)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "bad json"}, status=400)
    event = sessions.append_event(session.session_id, event_type, dict(body or {}))
    return web.json_response({"ok": True, "event": event.to_dict()})


async def _provider_control_event(
    request: web.Request,
    sessions: SessionManager,
    action: str,
) -> web.Response:
    session_id = request.match_info["id"]
    session = sessions.get(session_id)
    if session is None:
        return web.json_response({"error": "no such session"}, status=404)
    required_action = WORKER_SESSION_APPROVE if action == "approval" else WORKER_SESSION_INPUT
    authority_error = _require_session_authority(session, required_action)
    if authority_error is not None:
        return authority_error
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "bad json"}, status=400)
    try:
        adapter = provider_for(session.provider)
    except ValueError as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=400)
    if action == "approval":
        handler = getattr(adapter, "resolve_approval", None)
    else:
        handler = getattr(adapter, "receive_input", None)
    if handler is None:
        event_type = EVENT_APPROVAL_RESOLVED if action == "approval" else EVENT_INPUT_RECEIVED
        event = sessions.append_event(session.session_id, event_type, dict(body or {}))
    else:
        try:
            event = handler(session=session, request=dict(body or {}), sessions=sessions)
        except RuntimeError as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
    return web.json_response({"ok": True, "event": event.to_dict()})


async def _provider_terminal_event(
    request: web.Request,
    sessions: SessionManager,
    action: str,
) -> web.Response:
    session_id = request.match_info["id"]
    session = sessions.get(session_id)
    if session is None:
        return web.json_response({"error": "no such session"}, status=404)
    required_action = WORKER_SESSION_STOP if action == "stop" else WORKER_SESSION_INTERRUPT
    authority_error = _require_session_authority(session, required_action)
    if authority_error is not None:
        return authority_error
    try:
        adapter = provider_for(session.provider)
    except ValueError as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=400)
    handler = getattr(adapter, action, None)
    if handler is None:
        status = SESSION_STOPPED if action == "stop" else SESSION_INTERRUPTED
        event_type = EVENT_SESSION_STOPPED if action == "stop" else EVENT_SESSION_INTERRUPTED
        updated = sessions.update_status(session.session_id, status)
        event = sessions.append_event(session.session_id, event_type, {"status": status})
    else:
        updated, event = handler(session=session, sessions=sessions)
    return web.json_response({"ok": True, "session": updated.to_dict(), "event": event.to_dict()})


def _require_session_authority(session, action: str) -> web.Response | None:  # noqa: ANN001
    try:
        authority = WorkerSessionAuthority.from_session(session, provider=session.provider)
        authority.require(action)
    except RuntimeError as exc:
        return web.json_response({"ok": False, "error": str(exc)}, status=400)
    return None


async def _terminal_session_event(
    request: web.Request,
    sessions: SessionManager,
    status: str,
    event_type: str,
) -> web.Response:
    session_id = request.match_info["id"]
    session = sessions.get(session_id)
    if session is None:
        return web.json_response({"error": "no such session"}, status=404)
    session = sessions.update_status(session.session_id, status)
    event = sessions.append_event(session.session_id, event_type, {"status": status})
    return web.json_response({"ok": True, "session": session.to_dict(), "event": event.to_dict()})


async def _prepare_session_body(body: dict, cfg: WorkerConfig, workspace: pathlib.Path) -> dict:
    data = dict(body)
    metadata = dict(data.get("metadata") or {})
    envelope = metadata.get("execution_envelope")
    if data.get("cwd"):
        cwd, err = _worker_owned_cwd(str(data.get("cwd") or ""), workspace)
        if err:
            raise ValueError(err)
        data["cwd"] = cwd
    if data.get("cwd") or not data.get("repo") or not isinstance(envelope, dict):
        return data
    if metadata.get("provision_workspace") is not True:
        return data
    repo_ref = str(data.get("repo") or "")
    resolved = resolve_repo(repo_ref, cfg.repo_root)
    if resolved is None and cfg.clone_missing and cfg.repo_root:
        resolved, clone_err = await clone_repo(repo_ref, cfg.repo_root, cfg.clone_timeout_s)
        if resolved is None:
            raise ValueError(clone_err or f"couldn't clone {repo_ref!r}")
    if resolved is None:
        avail = list_repos(cfg.repo_root)
        hint = f" I can see: {', '.join(avail)}." if avail else ""
        raise ValueError(f"couldn't find a repo called {repo_ref!r}.{hint}")
    slug = slugify(str(data.get("title") or data.get("branch") or data.get("run_id") or repo_ref or "session"))
    cwd, branch, err = await prepare_worktree(
        resolved,
        str(workspace / "worktrees"),
        slug,
        cfg.worktree_branch_prefix,
        cfg.shell_timeout_s,
    )
    if err:
        raise ValueError(err)
    data["cwd"] = cwd or ""
    data["branch"] = branch or str(data.get("branch") or "")
    metadata["source_repo"] = resolved
    data["metadata"] = metadata
    return data


def _resume_cwd(cwd: str, workspace: pathlib.Path) -> tuple[str, str]:
    return _worker_owned_cwd(cwd, workspace, action="resume")


def _worker_owned_cwd(cwd: str, workspace: pathlib.Path, *, action: str = "session") -> tuple[str, str]:
    path = pathlib.Path(cwd).expanduser().resolve(strict=False)
    allowed_roots = [(workspace / "runs").resolve(), (workspace / "worktrees").resolve()]
    if not any(path.is_relative_to(root) for root in allowed_roots):
        return "", f"refusing to {action} outside worker-owned workspace: {cwd}"
    if not path.is_dir():
        return "", f"{action} cwd does not exist: {cwd}"
    return str(path), ""


def _session_cwd_error(session: WorkerSession, workspace: pathlib.Path) -> str:
    if session.provider not in {"codex", "claude"}:
        return ""
    if not session.cwd:
        return f"worker session cwd is required for {session.provider} provider turns"
    _cwd, err = _worker_owned_cwd(session.cwd, workspace, action="start provider turn")
    return err


def _query_limit(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        limit = int(value)
    except ValueError:
        return None
    return max(0, min(limit, 1000))


def _clean_request_id(value: object, prefix: str) -> str:
    text = str(value or "").strip()
    if text and all(ch.isalnum() or ch in {"_", "-"} for ch in text):
        return text
    return f"{prefix}_{uuid.uuid4().hex}"


def _events_for_idempotency(
    sessions: SessionManager,
    session_id: str,
    idempotency_key: str,
) -> list:
    key = idempotency_key.strip()
    if not key:
        return []
    return [event for event in sessions.events(session_id) if str(event.data.get("idempotency_key") or "") == key]


def _running_workspace_refs(items: list) -> set[str]:
    return {job.cwd for job in items if job.status == "running" and job.cwd}


def _workspace_in_use(job, running_refs: set[str]) -> bool:  # noqa: ANN001
    if not job.cwd:
        return False
    return job.cwd in running_refs


async def serve(cfg: WorkerConfig) -> None:
    from jarvis.config import insecure_bind

    bind = cfg.bind_host or cfg.host
    if insecure_bind(bind, bool(cfg.token.get_secret_value()), cfg.allow_insecure):
        print(
            f"\n✗ Refusing to start: worker is bound to {bind!r} (non-loopback) with no "
            "WORKER_TOKEN — that's unauthenticated access to shell/GUI/browser on this Mac.\n"
            "  Set WORKER_TOKEN, or WORKER_ALLOW_INSECURE=true to override.\n"
        )
        return
    app = make_app(cfg)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, bind, cfg.port)
    await site.start()
    print(f"Worker daemon listening on http://{bind}:{cfg.port} (agent={cfg.agent})")
    penv = _peekaboo_env(cfg)
    if penv.get("PEEKABOO_AI_PROVIDERS"):
        via = penv.get("OPENAI_BASE_URL") or "OpenAI direct"
        print(f"  peekaboo agent (control_mac): {penv['PEEKABOO_AI_PROVIDERS']} via {via}")
    else:
        print("  peekaboo agent (control_mac): no AI provider set — it will fail until "
              "WORKER_PEEKABOO_AI_PROVIDERS + key are configured")
    bcfg = app["browser_cfg"]
    if bcfg.enabled:
        print(f"  browser lane: default context {bcfg.default_context!r}, headless={bcfg.headless}")
    # Run until a stop signal — and handle SIGTERM/SIGINT so a `kill` shuts Chrome down
    # gracefully (a hard kill would orphan it and lock the profile).
    import signal

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        host = app["browser_holder"].get("h")
        if host is not None:
            await host.aclose()
        await runner.cleanup()
