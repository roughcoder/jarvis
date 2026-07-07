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
from typing import Any

from aiohttp import web

from jarvis.capabilities import (
    WORKER_SESSION_APPROVE,
    WORKER_SESSION_INPUT,
    WORKER_SESSION_INTERRUPT,
    WORKER_SESSION_RESTORE,
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
    fetch_repo,
    gui_doctor,
    diagnostics,
    list_repos,
    prepare_worktree,
    probe_repo_access,
    prune_worktrees,
    resolve_repo,
    run_applescript,
    run_exec,
    run_peekaboo,
    run_shell,
    take_screenshot,
    worktree_inventory,
)
from jarvis.worker.jobs import JobManager, slugify
from jarvis.worker.providers import ProviderTurn, provider_for
from jarvis.worker.sessions import SessionManager, WorkerSession
from jarvis.worker.workspaces import (
    conversation_workspace_root,
    ensure_workspace,
    get_workspace,
    materialize_worktree,
    remove_worktree,
    worker_owned_cwd,
)
from jarvis.system_info import system_info_cached
from jarvis.worker_session_contract import (
    CHECKPOINT_ID_KEY,
    ACTIVE_SESSION_STATUSES,
    EVENT_APPROVAL_RESOLVED,
    EVENT_INPUT_RECEIVED,
    EVENT_SESSION_INTERRUPTED,
    EVENT_SESSION_STOPPED,
    EVENT_TURN_STARTED,
    EVENT_TURN_FAILED,
    EVENT_PROVISIONING_PROGRESS,
    FAILED_SESSION_STATUSES,
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


def _engine_supports(engines: list[str]) -> dict[str, dict[str, bool]]:
    supports: dict[str, dict[str, bool]] = {}
    for engine in engines:
        try:
            capabilities = provider_for(engine).capabilities()
        except ValueError:
            capabilities = {}
        supports[engine] = {
            "streaming": bool(capabilities.get("streaming")),
            "resume": bool(capabilities.get("resume")),
            "interrupt": bool(capabilities.get("interrupt")),
            "approval_requests": bool(capabilities.get("approval_requests", capabilities.get("approvals"))),
            "input_requests": bool(capabilities.get("input_requests", capabilities.get("questions"))),
            "checkpoints": bool(capabilities.get("checkpoints")),
            "attachments": bool(capabilities.get("attachments")),
        }
    return supports


def _turn_attachments(body: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Turn attachments from a request body; [] when absent, None when malformed."""
    raw = body.get("attachments")
    if raw in (None, []):
        return []
    if not isinstance(raw, list) or not all(isinstance(item, dict) for item in raw):
        return None
    return [dict(item) for item in raw]


def _attachment_summaries(attachments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for item in attachments:
        data_url = str(item.get("data_url") or "")
        payload = data_url.split(",", 1)[1] if "," in data_url else ""
        summaries.append(
            {
                "kind": str(item.get("kind") or ""),
                "mime_type": str(item.get("mime_type") or ""),
                "name": str(item.get("name") or ""),
                "bytes": (len(payload) * 3) // 4,
            }
        )
    return summaries


def make_app(cfg: WorkerConfig) -> web.Application:
    workspace = _worker_workspace(cfg)
    conversation_root = conversation_workspace_root(cfg, workspace)
    conversation_root.mkdir(parents=True, exist_ok=True)
    # Persist jobs to disk under the workspace so they survive a daemon restart.
    jobs = JobManager(store_dir=str(workspace / "jobs"))
    sessions = SessionManager(store_dir=str(workspace / "sessions"))

    # Browser lane: one lazily-created BrowserHost per process (own config slice, read
    # from env like the worker's). nodriver is imported only on first use.
    from jarvis.config import BrowserConfig

    browser_cfg = BrowserConfig()
    browser_holder: dict = {}
    diagnostics_state: dict[str, Any] = {"value": None, "expires_at": 0.0, "task": None}
    worktree_inventory_state: dict[str, Any] = {"value": None, "expires_at": 0.0, "task": None}

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
            provisioning: list[dict[str, Any]] = []
            if args.get("cwd") and resume_session:
                if not session_id:
                    return web.json_response({"ok": False, "error": "resume cwd requires session_id"}, status=400)
                job_cwd, err = _resume_cwd(str(args["cwd"]), workspace, conversation_root)
                if err:
                    return web.json_response({"ok": False, "error": err}, status=400)
                branch = str(args.get("branch") or "") or None
                resolved = str(args.get("repo") or "")
                cleanup_owned = False
            elif args.get("repo"):
                # Resolve the repo name to a real path (clone it if missing) before
                # isolating it on a fresh worktree branch — never the user's checkout.
                def collect(phase: str, status: str = "started", **data: Any) -> None:
                    provisioning.append({"phase": phase, "status": status, **data})

                resolved, job_cwd, branch, err = await _provision_worktree(
                    str(args["repo"]),
                    cfg,
                    workspace,
                    slug,
                    progress=collect,
                )
                if err:
                    status = 404 if err.startswith("couldn't find") else 400
                    return web.json_response(
                        {"ok": False, "error": err, "provisioning": provisioning},
                        status=status,
                    )
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
                    "provisioning": provisioning if args.get("repo") else [],
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
        if action == "repo_access":
            repo = str(args.get("repo") or "").strip()
            if not repo:
                return web.json_response({"ok": False, "error": "repo is required"}, status=400)
            access = await asyncio.to_thread(
                probe_repo_access,
                repo,
                timeout_s=cfg.repo_access_probe_timeout_s,
                ttl_s=cfg.repo_access_ttl_s,
            )
            return web.json_response({"ok": True, "access": access})
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
            session_id = str((body or {}).get("session_id") or "").strip()
            if session_id and sessions.session_path(session_id).exists():
                existing = sessions.get(session_id)
                if existing is not None and _failed_unstarted_provisioning_session(existing, sessions):
                    sessions.delete(session_id)
                else:
                    raise ValueError(f"worker session already exists: {session_id}")
            if (body or {}).get("cwd"):
                cwd, err = worker_owned_cwd(str((body or {}).get("cwd") or ""), workspace, conversation_root=conversation_root)
                if err:
                    raise ValueError(err)
                body["cwd"] = cwd
            # Provisioned sessions are created before workspace materialization
            # so provisioning events have a durable session to attach to. No
            # provider starts in this window; /turns runs later and requires cwd.
            session, event = sessions.create(body or {})
        except (RuntimeError, ValueError) as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        except Exception:
            return web.json_response({"ok": False, "error": "bad json"}, status=400)
        events = [event.to_dict()]
        try:
            session, provisioning_events = await _provision_session_if_requested(session, body or {}, cfg, workspace, sessions, conversation_root=conversation_root)
            events.extend(event.to_dict() for event in provisioning_events)
        except Exception as exc:  # noqa: BLE001 - any provisioning failure (OSError disk-full,
            # KeyError from a malformed workspace state, etc.), not just ValueError, must land
            # the session in FAILED-with-provisioning-failed-event so it's reclaimable on retry.
            failed = sessions.update_status(session.session_id, SESSION_FAILED)
            sessions.append_event(
                session.session_id,
                EVENT_PROVISIONING_PROGRESS,
                {"phase": "provisioning", "status": "failed", "error": str(exc)},
            )
            events = [event.to_dict() for event in sessions.events(session.session_id)]
            return web.json_response(
                {"ok": False, "error": str(exc), "session": failed.to_dict(), "events": events},
                status=400,
            )
        return web.json_response({"ok": True, "session": session.to_dict(), "event": events[-1], "events": events})

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

    async def create_conversation_workspace(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            body = await request.json()
            conversation_id = str(body.get("conversation_id") or "").strip()
            if not conversation_id:
                return web.json_response({"ok": False, "error": "conversation_id is required"}, status=400)
            workspace_state = await ensure_workspace(
                root=conversation_root,
                conversation_id=conversation_id,
                metadata=dict(body.get("metadata") or {}),
            )
        except (OSError, ValueError) as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        except Exception:
            return web.json_response({"ok": False, "error": "bad json"}, status=400)
        return web.json_response({"ok": True, "workspace": workspace_state})

    async def get_conversation_workspace(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            workspace_state = get_workspace(conversation_root, request.match_info["conversation_id"])
        except FileNotFoundError:
            return web.json_response({"error": "no such conversation workspace"}, status=404)
        except ValueError as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        return web.json_response({"ok": True, "workspace": workspace_state})

    async def create_conversation_worktree(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            body = await request.json()
            repo = str(body.get("repo") or "").strip()
            if not repo:
                return web.json_response({"ok": False, "error": "repo is required"}, status=400)
            workspace_state = await materialize_worktree(
                cfg=cfg,
                root=conversation_root,
                conversation_id=request.match_info["conversation_id"],
                repo_ref=repo,
                repo_name=str(body.get("name") or ""),
                base_ref=str(body.get("base_ref") or ""),
            )
        except ValueError as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        except Exception:
            return web.json_response({"ok": False, "error": "bad json"}, status=400)
        return web.json_response({"ok": True, "workspace": workspace_state})

    async def delete_conversation_worktree(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            workspace_state = await remove_worktree(
                cfg=cfg,
                root=conversation_root,
                conversation_id=request.match_info["conversation_id"],
                repo_name=request.match_info["repo_name"],
            )
        except ValueError as exc:
            return web.json_response({"ok": False, "error": str(exc)}, status=400)
        return web.json_response({"ok": True, "workspace": workspace_state})

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
        authority_error = _require_session_control_authority(session, WORKER_SESSION_RESTORE, body)
        if authority_error is not None:
            return authority_error
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
            return web.json_response(
                {"ok": False, "error": f"provider {session.provider!r} does not support checkpoint restore"},
                status=501,
            )
        event = handler(session=session, request=request_data, sessions=sessions)
        return web.json_response({"ok": True, "event": event.to_dict()})

    async def start_session_turn(request: web.Request) -> web.Response:
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
        authority_error = _require_session_control_authority(session, WORKER_SESSION_TURN, body)
        if authority_error is not None:
            return authority_error
        cwd_error = _session_cwd_error(session, workspace, conversation_root)
        if cwd_error:
            return web.json_response({"ok": False, "error": cwd_error}, status=400)
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
        attachments = _turn_attachments(body)
        if attachments is None:
            return web.json_response({"ok": False, "error": "attachments must be an array of objects"}, status=400)
        if attachments and not adapter.capabilities().get("attachments"):
            return web.json_response(
                {"ok": False, "error": f"provider {session.provider!r} does not support turn attachments"},
                status=400,
            )
        running_event = None
        if session.metadata.get("provision_workspace") is True:
            running_event = sessions.append_event(
                session.session_id,
                EVENT_PROVISIONING_PROGRESS,
                {"phase": "running", "status": "started", "turn_id": turn_id},
            )
        turn_data = {
            "turn_id": turn_id,
            "prompt": str(body.get("prompt") or ""),
            "metadata": dict(body.get("metadata") or {}),
            "idempotency_key": idempotency_key,
        }
        if attachments:
            # Events are durable and replayed to cockpits; keep them to
            # summaries and hand the base64 payloads only to the provider.
            turn_data["attachments"] = _attachment_summaries(attachments)
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
                    "events": [*([running_event.to_dict()] if running_event else []), started.to_dict()],
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
                    attachments=attachments,
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
                    "events": [
                        *([running_event.to_dict()] if running_event else []),
                        started.to_dict(),
                        failed.to_dict(),
                    ],
                },
                status=400,
            )
        return web.json_response(
            {
                "ok": True,
                "session": sessions.get(session.session_id).to_dict(),  # type: ignore[union-attr]
                "turn_id": turn_id,
                "events": [
                    *([running_event.to_dict()] if running_event else []),
                    started.to_dict(),
                    *[event.to_dict() for event in provider_events],
                ],
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

    async def delete_session(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        session_id = request.match_info["id"]
        session = sessions.get(session_id)
        if session is None or session.session_id != session_id:
            return web.json_response({"error": "no such session"}, status=404)
        if session.status in ACTIVE_SESSION_STATUSES:
            return web.json_response({"ok": False, "error": "worker session is live"}, status=409)
        events_count = len(sessions.events(session.session_id))
        prune = {"ok": True, "worktrees": 0, "bytes": 0, "pruned": [], "refused": []}
        if session.cwd:
            prune = await prune_worktrees(
                str(workspace / "worktrees"),
                str(workspace / "sessions"),
                target=session.cwd,
                stale_ttl_s=0.0,
                timeout_s=cfg.shell_timeout_s,
            )
            # "outside worktree root" means the session's cwd lives under another
            # worker-owned root (conversations/ or runs/, not worktrees/) — prune
            # correctly refuses to touch it, but that's not a reason to block the
            # session RECORD deletion itself. Only a genuinely live/refused worktree
            # (still in worktrees/, still referenced) should 409 here.
            blocking = [
                item
                for item in prune.get("refused", [])
                if item.get("reason") not in {"worktree not found", "outside worktree root"}
            ]
            if blocking:
                return web.json_response({"ok": False, "error": blocking[0].get("reason") or "worktree prune refused"}, status=409)
        removed = sessions.remove(session.session_id)
        return web.json_response(
            {
                "ok": True,
                "deleted": removed,
                "session_id": session.session_id,
                "reclamation": {
                    "records": 1 if removed else 0,
                    "events": events_count,
                    "worktrees": int(prune.get("worktrees") or 0),
                    "bytes": int(prune.get("bytes") or 0),
                },
                "worktree_prune": prune,
            }
        )

    async def list_worktrees(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        inventory = await asyncio.to_thread(
            worktree_inventory,
            str(workspace / "worktrees"),
            str(workspace / "sessions"),
            stale_ttl_s=cfg.worktree_stale_ttl_s,
        )
        return web.json_response({"ok": True, "worktree_inventory": inventory})

    async def prune_worktree_request(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            body = await request.json() if request.can_read_body else {}
        except Exception:
            return web.json_response({"error": "bad json"}, status=400)
        target = str(body.get("target") or body.get("name") or body.get("worktree") or "").strip()
        try:
            ttl_s = float(body.get("stale_ttl_s") if body.get("stale_ttl_s") is not None else cfg.worktree_stale_ttl_s)
        except (TypeError, ValueError):
            return web.json_response({"error": "stale_ttl_s must be numeric"}, status=400)
        result = await prune_worktrees(
            str(workspace / "worktrees"),
            str(workspace / "sessions"),
            target=target,
            stale_ttl_s=ttl_s,
            timeout_s=cfg.shell_timeout_s,
        )
        status = 200 if result.get("ok") else 409
        return web.json_response(result, status=status)

    def _refreshing_worktree_inventory() -> dict[str, Any]:
        return {"count": 0, "disk_bytes": 0, "stale_count": 0, "status": "refreshing"}

    async def _refresh_worktree_inventory() -> None:
        try:
            value = await asyncio.to_thread(
                worktree_inventory,
                str(workspace / "worktrees"),
                str(workspace / "sessions"),
                stale_ttl_s=cfg.worktree_stale_ttl_s,
            )
            if not isinstance(value, dict):
                value = {"count": 0, "disk_bytes": 0, "stale_count": 0, "error": "invalid inventory payload"}
        except Exception as exc:  # noqa: BLE001 - health must remain a liveness endpoint
            value = {"count": 0, "disk_bytes": 0, "stale_count": 0, "error": str(exc)[:200] or exc.__class__.__name__}
        worktree_inventory_state["value"] = value
        worktree_inventory_state["expires_at"] = asyncio.get_running_loop().time() + max(0.0, cfg.diagnostics_ttl_s)
        worktree_inventory_state["task"] = None

    async def _cached_worktree_inventory() -> dict[str, Any]:
        # /health is a probe endpoint the cockpit hits continuously (SSE ~1s, ~3s
        # timeout). worktree_inventory() is a full os.walk lstat of every worktree —
        # cheap on a small workspace but easily probe-timeout-exceeding on a large
        # one, which flaps the worker offline. Cache it on the same TTL as diagnostics
        # and refresh in the background so a cache miss never blocks liveness.
        now = asyncio.get_running_loop().time()
        cached = worktree_inventory_state.get("value")
        if cached is not None and float(worktree_inventory_state.get("expires_at") or 0.0) > now:
            return dict(cached)
        task = worktree_inventory_state.get("task")
        if task is None or task.done():
            worktree_inventory_state["task"] = asyncio.create_task(_refresh_worktree_inventory())
        if isinstance(cached, dict):
            payload = dict(cached)
            payload["status"] = "refreshing"
            return payload
        return _refreshing_worktree_inventory()

    async def health(request: web.Request) -> web.Response:
        supported_engines = engine_ids(cfg.supported_engines, default_engine=cfg.agent)
        inventory = await _cached_worktree_inventory()
        body = {
            "ok": True,
            "agent": cfg.agent,
            "default_engine": normalize_engine_id(cfg.agent),
            "supported_engines": supported_engines,
            "engine_supports": _engine_supports(supported_engines),
            "workspace": str(workspace),
            "conversation_workspace_root": str(conversation_root),
            "repo_root_configured": bool(cfg.repo_root),
            "browser_enabled": browser_cfg.enabled,
            "gui_provider_configured": bool(cfg.peekaboo_ai_providers),
            "worktree_inventory": inventory,
        }
        if authorised(request):
            body["system"] = system_info_cached()
            readiness = _diagnostics_payload(supported_engines)
            body["diagnostics"] = readiness
            if isinstance(readiness, dict) and isinstance(readiness.get("git_identity"), dict):
                body["git_identity"] = readiness["git_identity"]
            body["repositories"] = readiness.get("repositories") if isinstance(readiness, dict) else []
            if not isinstance(body["repositories"], list):
                body["repositories"] = []
        return web.json_response(body)

    def _diagnostics_payload(supported_engines: list[str]) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        now = loop.time()
        value = diagnostics_state.get("value")
        expired = value is None or float(diagnostics_state.get("expires_at") or 0.0) <= now
        task = diagnostics_state.get("task")
        if expired and (task is None or task.done()):
            diagnostics_state["task"] = loop.create_task(_refresh_diagnostics(supported_engines))
        if isinstance(value, dict):
            payload = dict(value)
            if expired:
                payload["status"] = "refreshing"
            return payload
        return {"status": "refreshing", "repositories": []}

    async def _refresh_diagnostics(supported_engines: list[str]) -> None:
        try:
            value = await asyncio.to_thread(
                diagnostics,
                repo_root=cfg.repo_root,
                engines=supported_engines,
                codex_bin=cfg.codex_bin,
                claude_bin=cfg.claude_bin,
                browser_cfg=browser_cfg,
                ttl_s=cfg.diagnostics_ttl_s,
                probe_timeout_s=cfg.repo_access_probe_timeout_s,
            )
            if not isinstance(value, dict):
                value = {"error": "invalid diagnostics payload", "repositories": []}
        except Exception as exc:  # noqa: BLE001 - health must stay a liveness endpoint
            value = {"error": str(exc)[:200] or exc.__class__.__name__, "repositories": []}
        diagnostics_state["value"] = value
        diagnostics_state["expires_at"] = asyncio.get_running_loop().time() + max(0.0, cfg.diagnostics_ttl_s)
        diagnostics_state["task"] = None

    async def _cleanup_diagnostics(_app: web.Application) -> None:
        task = diagnostics_state.get("task")
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _cleanup_worktree_inventory(_app: web.Application) -> None:
        task = worktree_inventory_state.get("task")
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    app = web.Application(client_max_size=max(1024 * 1024, int(cfg.max_request_bytes)))
    app["browser_holder"] = browser_holder  # for clean shutdown in serve()
    app["browser_cfg"] = browser_cfg
    app.on_cleanup.append(_cleanup_diagnostics)
    app.on_cleanup.append(_cleanup_worktree_inventory)
    app.add_routes([
        web.post("/run", run),
        web.get("/jobs/{id}", get_job),
        web.get("/jobs", list_jobs),
        web.post("/sessions", create_session),
        web.get("/sessions", list_sessions),
        web.get("/sessions/requests", list_session_requests),
        web.post("/conversation-workspaces", create_conversation_workspace),
        web.get("/conversation-workspaces/{conversation_id}", get_conversation_workspace),
        web.post("/conversation-workspaces/{conversation_id}/worktrees", create_conversation_worktree),
        web.delete("/conversation-workspaces/{conversation_id}/worktrees/{repo_name}", delete_conversation_worktree),
        web.get("/sessions/{id}/events", get_session_events),
        web.get("/sessions/{id}/requests", get_session_requests),
        web.get("/sessions/{id}/checkpoints", get_session_checkpoints),
        web.post("/sessions/{id}/checkpoints/restore", restore_session_checkpoint),
        web.post("/sessions/{id}/turns", start_session_turn),
        web.post("/sessions/{id}/input", session_input),
        web.post("/sessions/{id}/approval", session_approval),
        web.post("/sessions/{id}/interrupt", session_interrupt),
        web.post("/sessions/{id}/stop", session_stop),
        web.delete("/sessions/{id}", delete_session),
        web.get("/sessions/{id}", get_session),
        web.get("/worktrees", list_worktrees),
        web.post("/worktrees/prune", prune_worktree_request),
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
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "bad json"}, status=400)
    authority_error = _require_session_control_authority(session, required_action, body)
    if authority_error is not None:
        return authority_error
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
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "bad json"}, status=400)
    authority_error = _require_session_control_authority(session, required_action, body)
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


def _require_session_control_authority(
    session: WorkerSession,
    action: str,
    body: dict[str, Any],
) -> web.Response | None:
    ceiling_error = _require_session_authority(session, action)
    if ceiling_error is not None:
        return ceiling_error
    try:
        caller = WorkerSessionAuthority.from_metadata(_control_authority_metadata(body))
        caller.require(action)
    except RuntimeError as exc:
        return web.json_response({"ok": False, "error": f"caller control authority denied: {exc}"}, status=400)
    return None


def _control_authority_metadata(body: dict[str, Any]) -> dict[str, Any]:
    metadata = dict(body.get("metadata") or {}) if isinstance(body.get("metadata"), dict) else {}
    control_envelope = metadata.get("control_envelope")
    if isinstance(control_envelope, dict):
        metadata["execution_envelope"] = control_envelope
    envelope = body.get("execution_envelope")
    if isinstance(envelope, dict):
        metadata["execution_envelope"] = envelope
    allowed_actions = body.get("allowed_actions")
    if isinstance(allowed_actions, list):
        metadata["allowed_actions"] = allowed_actions
    return metadata


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


async def _provision_session_if_requested(
    session: WorkerSession,
    body: dict,
    cfg: WorkerConfig,
    workspace: pathlib.Path,
    sessions: SessionManager,
    conversation_root: pathlib.Path | None = None,
) -> tuple[WorkerSession, list[Any]]:
    data = dict(body)
    metadata = dict(data.get("metadata") or {})
    envelope = metadata.get("execution_envelope")
    if data.get("cwd"):
        cwd, err = worker_owned_cwd(str(data.get("cwd") or ""), workspace, conversation_root=conversation_root)
        if err:
            raise ValueError(err)
        return sessions.update_workspace(session.session_id, cwd=cwd), []
    if not data.get("repo") or not isinstance(envelope, dict) or metadata.get("provision_workspace") is not True:
        return session, []
    repo_ref = str(data.get("repo") or "")
    slug = slugify(str(data.get("title") or data.get("branch") or data.get("run_id") or repo_ref or "session"))
    events = []

    def progress(phase: str, status: str = "started", **payload: Any) -> None:
        events.append(
            sessions.append_event(
                session.session_id,
                EVENT_PROVISIONING_PROGRESS,
                {"phase": phase, "status": status, **payload},
            )
        )

    resolved, cwd, branch, err = await _provision_worktree(repo_ref, cfg, workspace, slug, progress=progress)
    if err:
        raise ValueError(err)
    metadata["source_repo"] = resolved
    updated = sessions.update_workspace(
        session.session_id,
        cwd=cwd or "",
        branch=branch or str(data.get("branch") or ""),
        repo=resolved or repo_ref,
        metadata=metadata,
    )
    return updated, events


def _failed_unstarted_provisioning_session(session: WorkerSession, sessions: SessionManager) -> bool:
    """A session record is reclaimable (delete-then-recreate on retry with the same
    deterministic session_id) when it's terminal AND never started a turn — not just
    a provisioning failure (the original, narrower check): a session stopped or
    interrupted before its first turn is just as safely reclaimable.

    SESSION_CREATED is deliberately NOT reclaimable here: it's ambiguous between
    "ready for its first turn" (provisioning succeeded — update_workspace doesn't
    change status) and "orphaned by a mid-provisioning crash", with no reliable
    signal in persisted state to tell them apart. Treating CREATED as reclaimable
    would let a duplicate create request destroy a live, ready session. The crash
    case this might otherwise cover already lands in SESSION_FAILED instead, now that
    create_session's except clause (above) catches any provisioning exception, not
    just ValueError.

    Any turn_started event means real work may be in flight — never reclaim that.
    """
    if session.status not in FAILED_SESSION_STATUSES:
        return False
    events = sessions.events(session.session_id)
    return not any(event.type == EVENT_TURN_STARTED for event in events)


async def _provision_worktree(
    repo_ref: str,
    cfg: WorkerConfig,
    workspace: pathlib.Path,
    slug: str,
    *,
    progress: Any,
) -> tuple[str, str, str | None, str]:
    progress("resolving-access")
    if _should_probe_repo_access(repo_ref):
        access = await asyncio.to_thread(
            probe_repo_access,
            repo_ref,
            timeout_s=cfg.repo_access_probe_timeout_s,
            ttl_s=cfg.repo_access_ttl_s,
        )
        if not access.get("accessible"):
            progress(
                "resolving-access",
                "failed",
                reason_code=str(access.get("reason_code") or "identity-lacks-repo-access"),
                message=str(access.get("reason") or "worker identity cannot access repo"),
            )
            return "", "", None, str(access.get("reason") or f"worker identity cannot access {repo_ref!r}")
        progress("resolving-access", "completed", access=access)
    else:
        progress("resolving-access", "completed", access={"repo": repo_ref, "accessible": True, "source": "local"})
    resolved = resolve_repo(repo_ref, cfg.repo_root)
    progress("cloning")
    if resolved is None and cfg.clone_missing and cfg.repo_root:
        resolved, clone_err = await clone_repo(repo_ref, cfg.repo_root, cfg.clone_timeout_s)
        if resolved is None:
            progress("cloning", "failed", message=clone_err or f"couldn't clone {repo_ref!r}")
            return "", "", None, clone_err or f"couldn't clone {repo_ref!r}"
    if resolved is None:
        avail = list_repos(cfg.repo_root)
        hint = f" I can see: {', '.join(avail)}." if avail else ""
        message = f"couldn't find a repo called {repo_ref!r}.{hint}"
        progress("cloning", "failed", message=message)
        return "", "", None, message
    fetch_err = await fetch_repo(resolved, cfg.clone_timeout_s)
    if fetch_err:
        progress("cloning", "warning", message=fetch_err)
    progress("cloning", "completed", repo_path=resolved)
    progress("creating-worktree")
    cwd, branch, err = await prepare_worktree(
        resolved,
        str(workspace / "worktrees"),
        slug,
        cfg.worktree_branch_prefix,
        cfg.shell_timeout_s,
    )
    if err:
        progress("creating-worktree", "failed", message=err)
        return resolved, "", None, err
    progress("creating-worktree", "completed", cwd=cwd or "", branch=branch or "")
    return resolved, cwd or "", branch, ""


def _should_probe_repo_access(repo_ref: str) -> bool:
    text = str(repo_ref or "").strip()
    if pathlib.Path(text).expanduser().is_absolute():
        return False
    if text.startswith(("http://", "https://", "git@")):
        return "github.com" in text
    return text.count("/") == 1


def _resume_cwd(cwd: str, workspace: pathlib.Path, conversation_root: pathlib.Path) -> tuple[str, str]:
    return worker_owned_cwd(cwd, workspace, conversation_root=conversation_root, action="resume")


def _session_cwd_error(session: WorkerSession, workspace: pathlib.Path, conversation_root: pathlib.Path) -> str:
    if session.provider not in {"codex", "claude"}:
        return ""
    if not session.cwd:
        return f"worker session cwd is required for {session.provider} provider turns"
    _cwd, err = worker_owned_cwd(
        session.cwd,
        workspace,
        conversation_root=conversation_root,
        action="start provider turn",
    )
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
