"""Worker daemon — an aiohttp service the brain dispatches to (Phase 3c).

Standalone and self-contained: imports nothing from the brain. Auth is a bearer
token. Fast actions (shell, screenshot, applescript) return synchronously; `code`
(a headless coding-agent run) starts a background job and returns its id at once.

Endpoints:
  POST /run        {action, args}            -> result, or {job_id} for `code`
  GET  /jobs/{id}                            -> job status + output
  GET  /jobs                                 -> recent jobs
  GET  /health                               -> liveness
"""

from __future__ import annotations

import asyncio

from aiohttp import web

from jarvis.config import WorkerConfig
from jarvis.worker.actions import (
    code_argv,
    run_applescript,
    run_exec,
    run_shell,
    take_screenshot,
)
from jarvis.worker.jobs import JobManager


def make_app(cfg: WorkerConfig) -> web.Application:
    import pathlib

    pathlib.Path(cfg.workspace).mkdir(parents=True, exist_ok=True)  # default cwd
    jobs = JobManager()

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
        cwd = args.get("cwd") or cfg.workspace

        if action == "shell":
            out = await run_shell(args.get("command", ""), cwd, cfg.shell_timeout_s)
            return web.json_response({"ok": True, "output": out})
        if action == "applescript":
            out = await run_applescript(args.get("script", ""), cfg.shell_timeout_s)
            return web.json_response({"ok": True, "output": out})
        if action == "screenshot":
            out = await take_screenshot(cfg.workspace, args.get("name"), cfg.shell_timeout_s)
            return web.json_response({"ok": True, "output": out})
        if action == "code":
            agent = args.get("agent") or cfg.agent
            argv = code_argv(agent, cfg.codex_bin, cfg.claude_bin, args.get("prompt", ""))
            label = args.get("prompt", "")[:60] or agent
            job = jobs.start(
                "code",
                label,
                run_exec(argv, args.get("repo") or cfg.workspace, cfg.job_timeout_s),
            )
            return web.json_response({"ok": True, "job_id": job.id, "status": "running"})
        return web.json_response({"error": f"unknown action {action!r}"}, status=400)

    async def get_job(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        job_id = request.match_info["id"]
        job = jobs.latest() if job_id == "latest" else jobs.get(job_id)
        if job is None:
            return web.json_response({"error": "no such job"}, status=404)
        return web.json_response(job.public())

    async def list_jobs(request: web.Request) -> web.Response:
        if not authorised(request):
            return web.json_response({"error": "unauthorized"}, status=401)
        return web.json_response({"jobs": [j.public() for j in jobs.recent()]})

    async def health(_request: web.Request) -> web.Response:
        return web.json_response({"ok": True, "agent": cfg.agent})

    app = web.Application()
    app.add_routes([
        web.post("/run", run),
        web.get("/jobs/{id}", get_job),
        web.get("/jobs", list_jobs),
        web.get("/health", health),
    ])
    return app


async def serve(cfg: WorkerConfig) -> None:
    app = make_app(cfg)
    runner = web.AppRunner(app)
    await runner.setup()
    bind = cfg.bind_host or cfg.host
    site = web.TCPSite(runner, bind, cfg.port)
    await site.start()
    print(f"Worker daemon listening on http://{bind}:{cfg.port} (agent={cfg.agent})")
    try:
        await asyncio.Future()  # run forever
    finally:
        await runner.cleanup()
