from __future__ import annotations

import asyncio
import json
import pathlib
import shutil
from typing import Any

from jarvis.config import WorkerConfig
from jarvis.ids import utc_now
from jarvis.text import slugify
from jarvis.worker.actions import clone_repo, fetch_repo, list_repos, repo_mutation_lock, resolve_available_worktree_branch, resolve_repo, run_exec


WORKSPACE_STATE_FILENAME = "workspace.json"
_WORKSPACE_LOCKS: dict[str, asyncio.Lock] = {}


def conversation_workspace_root(cfg: WorkerConfig, worker_workspace: pathlib.Path) -> pathlib.Path:
    configured = str(cfg.conversation_workspace_root or "").strip()
    return pathlib.Path(configured).expanduser().resolve() if configured else (worker_workspace / "conversations").resolve()


def worker_owned_roots(worker_workspace: pathlib.Path, conversation_root: pathlib.Path | None = None) -> list[pathlib.Path]:
    workspace = worker_workspace.expanduser().resolve(strict=False)
    return [
        (workspace / "runs").resolve(strict=False),
        (workspace / "worktrees").resolve(strict=False),
        (conversation_root or workspace / "conversations").expanduser().resolve(strict=False),
    ]


def is_worker_owned_path(
    path: pathlib.Path,
    worker_workspace: pathlib.Path,
    *,
    conversation_root: pathlib.Path | None = None,
) -> bool:
    resolved = path.expanduser().resolve(strict=False)
    return any(resolved.is_relative_to(root) for root in worker_owned_roots(worker_workspace, conversation_root))


def is_worker_owned_path_for_config(path: pathlib.Path, cfg: WorkerConfig) -> bool:
    workspace = pathlib.Path(cfg.workspace).expanduser().resolve(strict=False)
    return is_worker_owned_path(
        path,
        workspace,
        conversation_root=conversation_workspace_root(cfg, workspace),
    )


def worker_owned_cwd(
    cwd: str,
    worker_workspace: pathlib.Path,
    *,
    conversation_root: pathlib.Path | None = None,
    action: str = "session",
) -> tuple[str, str]:
    path = pathlib.Path(cwd).expanduser().resolve(strict=False)
    if not is_worker_owned_path(path, worker_workspace, conversation_root=conversation_root):
        return "", f"refusing to {action} outside worker-owned workspace: {cwd}"
    if not path.is_dir():
        return "", f"{action} cwd does not exist: {cwd}"
    return str(path), ""


def workspace_id(value: str) -> str:
    return slugify(value or "conversation")


def workspace_path(root: pathlib.Path, conversation_id: str) -> pathlib.Path:
    wid = workspace_id(conversation_id)
    path = (root / wid).resolve(strict=False)
    if not path.is_relative_to(root.resolve(strict=False)):
        raise ValueError(f"conversation id escapes workspace root: {conversation_id!r}")
    return path


def workspace_label(path: str) -> str:
    return pathlib.Path(path).name if path else ""


async def ensure_workspace(
    *,
    root: pathlib.Path,
    conversation_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    async with _workspace_lock(root, conversation_id):
        return _ensure_workspace_unlocked(root=root, conversation_id=conversation_id, metadata=metadata)


def _ensure_workspace_unlocked(
    *,
    root: pathlib.Path,
    conversation_id: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    path = workspace_path(root, conversation_id)
    path.mkdir(parents=True, exist_ok=True)
    state = _read_state(path)
    now = utc_now()
    merged_metadata = {**dict(state.get("metadata") or {}), **dict(metadata or {})}
    state.update(
        {
            "workspace_id": workspace_id(conversation_id),
            "conversation_id": conversation_id,
            "root": str(path),
            "root_label": workspace_label(str(path)),
            "status": "ready",
            "provision_phase": "running",
            "metadata": merged_metadata,
            "created_at": str(state.get("created_at") or now),
            "updated_at": now,
        }
    )
    state.setdefault("worktrees", [])
    _write_state(path, state)
    return public_workspace_state(state)


async def _resolve_and_prepare_repo(
    cfg: WorkerConfig,
    path: pathlib.Path,
    state: dict[str, Any],
    repos_dir: pathlib.Path,
    repo_ref: str,
    base_ref: str,
) -> tuple[str, str]:
    """Resolve the source repo (cloning it if missing and configured to), then
    fetch it and pick a base ref for the new worktree branch. Persists each
    provision-phase transition on `state` as it goes."""
    state["provision_phase"] = "resolving-access"
    state["status"] = "provisioning"
    _write_state(path, state)
    resolved = resolve_repo(repo_ref, cfg.repo_root)
    if resolved is None and cfg.clone_missing and cfg.repo_root:
        state["provision_phase"] = "cloning"
        _write_state(path, state)
        resolved, clone_err = await clone_repo(repo_ref, cfg.repo_root, cfg.clone_timeout_s)
        if resolved is None:
            raise ValueError(clone_err or f"couldn't clone {repo_ref!r}")
    if resolved is None:
        avail = list_repos(cfg.repo_root)
        hint = f" I can see: {', '.join(avail)}." if avail else ""
        raise ValueError(f"couldn't find a repo called {repo_ref!r}.{hint}")

    state["provision_phase"] = "creating-worktree"
    _write_state(path, state)
    repos_dir.mkdir(parents=True, exist_ok=True)
    # Always fetch before branching, even when the caller passed an explicit
    # base_ref — without this, an explicit base_ref skipped _default_base_ref's
    # (conditional) fetch entirely and the worktree branched from a stale local
    # base. Best-effort: a failed fetch is a warning elsewhere in the codebase
    # (see actions.fetch_repo's docstring), not a reason to refuse provisioning.
    await fetch_repo(resolved, cfg.shell_timeout_s)
    base = _safe_base_ref(base_ref.strip() or await _default_base_ref(resolved, cfg.shell_timeout_s))
    return resolved, base


async def _reconcile_orphaned_worktree(
    cfg: WorkerConfig,
    resolved: str,
    worktree: pathlib.Path,
    requested_branch: str,
) -> None:
    """`worktree` exists on disk but no state row references it — the orphan left
    by a daemon crash between `git worktree add` and _write_state (no state row
    references this path, but the dir + git worktree registration + branch
    survived). Reconcile instead of getting stuck forever: drop the orphaned
    registration so the caller can fall through and recreate it cleanly."""
    orphan_branch = await run_exec(
        ["git", "-C", str(worktree), "branch", "--show-current"],
        None,
        cfg.shell_timeout_s,
    )
    await run_exec(["git", "-C", resolved, "worktree", "remove", "--force", str(worktree)], None, cfg.shell_timeout_s)
    if worktree.exists():
        shutil.rmtree(worktree, ignore_errors=True)
    flattened_branch = requested_branch.replace("/", "-")
    if orphan_branch == requested_branch or orphan_branch == flattened_branch or orphan_branch.startswith(
        f"{flattened_branch}-"
    ):
        await run_exec(["git", "-C", resolved, "branch", "-D", orphan_branch], None, cfg.shell_timeout_s)


async def materialize_worktree(
    *,
    cfg: WorkerConfig,
    root: pathlib.Path,
    conversation_id: str,
    repo_ref: str,
    repo_name: str = "",
    base_ref: str = "",
) -> dict[str, Any]:
    path = workspace_path(root, conversation_id)
    async with _workspace_lock(root, conversation_id):
        try:
            if not path.exists():
                _ensure_workspace_unlocked(root=root, conversation_id=conversation_id)
            state = _read_state(path)
            repo_key = slugify(repo_name or pathlib.Path(repo_ref).name or repo_ref)
            if not repo_key:
                raise ValueError("repo is required")
            repos_dir = path / "repos"
            worktree = (repos_dir / repo_key).resolve(strict=False)
            existing = next((row for row in state.get("worktrees", []) if row.get("name") == repo_key), None)
            if existing and worktree.is_dir():
                existing["provision_phase"] = "running"
                existing["status"] = "ready"
                state["provision_phase"] = "running"
                state["status"] = "ready"
                state["updated_at"] = utc_now()
                _write_state(path, state)
                return public_workspace_state(state)

            requested_branch = f"{cfg.worktree_branch_prefix}/{workspace_id(conversation_id)}-{repo_key}"
            resolved, base = await _resolve_and_prepare_repo(cfg, path, state, repos_dir, repo_ref, base_ref)
            if worktree.exists():
                # `existing` (looked up above) was None or not a live dir, so this is
                # not a real name conflict.
                await _reconcile_orphaned_worktree(cfg, resolved, worktree, requested_branch)
            async with repo_mutation_lock(resolved):
                branch = await resolve_available_worktree_branch(resolved, requested_branch, cfg.shell_timeout_s)
                out = await run_exec(
                    ["git", "-C", resolved, "worktree", "add", "-b", branch, "--", str(worktree), base],
                    None,
                    cfg.shell_timeout_s,
                )
            if not worktree.exists():
                raise ValueError(f"could not create worktree for {repo_ref!r}: {out[:200]}")

            now = utc_now()
            row = {
                "name": repo_key,
                "repo": repo_ref,
                "source_repo": resolved,
                "path": str(worktree),
                "path_label": workspace_label(str(worktree)),
                "branch": branch,
                "base_ref": base,
                "status": "ready",
                "provision_phase": "running",
                "created_at": now,
                "updated_at": now,
            }
            state["worktrees"] = [item for item in state.get("worktrees", []) if item.get("name") != repo_key]
            state["worktrees"].append(row)
            state["provision_phase"] = "running"
            state["status"] = "ready"
            state["updated_at"] = utc_now()
            _write_state(path, state)
            return public_workspace_state(state)
        except Exception:
            state = _read_state(path)
            state["status"] = "failed"
            state["provision_phase"] = "failed"
            state["updated_at"] = utc_now()
            _write_state(path, state)
            raise


async def remove_worktree(
    *,
    cfg: WorkerConfig,
    root: pathlib.Path,
    conversation_id: str,
    repo_name: str,
) -> dict[str, Any]:
    path = workspace_path(root, conversation_id)
    async with _workspace_lock(root, conversation_id):
        state = _read_state(path)
        repo_key = slugify(repo_name)
        row = next((item for item in state.get("worktrees", []) if item.get("name") == repo_key), None)
        if row is None:
            raise ValueError(f"workspace repo {repo_name!r} is not materialized")
        wt = pathlib.Path(str(row.get("path") or "")).expanduser().resolve(strict=False)
        if not wt.is_relative_to(path.resolve(strict=False)):
            raise ValueError(f"refusing to remove non-conversation path for repo {repo_name!r}")
        source = str(row.get("source_repo") or "")
        branch = str(row.get("branch") or "")
        if source and branch:
            await run_exec(["git", "-C", source, "worktree", "remove", "--force", str(wt)], None, cfg.shell_timeout_s)
            await run_exec(["git", "-C", source, "branch", "-D", branch], None, cfg.shell_timeout_s)
        else:
            shutil.rmtree(wt, ignore_errors=True)
        state["worktrees"] = [item for item in state.get("worktrees", []) if item.get("name") != repo_key]
        state["updated_at"] = utc_now()
        _write_state(path, state)
        return public_workspace_state(state)


def get_workspace(root: pathlib.Path, conversation_id: str) -> dict[str, Any]:
    path = workspace_path(root, conversation_id)
    if not path.exists():
        raise FileNotFoundError(conversation_id)
    return public_workspace_state(_read_state(path))


def public_workspace_state(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "workspace_id": str(state.get("workspace_id") or ""),
        "conversation_id": str(state.get("conversation_id") or ""),
        "root": str(state.get("root") or ""),
        "root_label": str(state.get("root_label") or workspace_label(str(state.get("root") or ""))),
        "cwd_label": workspace_label(str(state.get("root") or "")),
        "status": str(state.get("status") or ""),
        "provision_phase": str(state.get("provision_phase") or ""),
        "worktrees": [
            {
                "name": str(item.get("name") or ""),
                "repo": str(item.get("repo") or ""),
                "path_label": str(item.get("path_label") or workspace_label(str(item.get("path") or ""))),
                "path": str(item.get("path") or ""),
                "source_repo": str(item.get("source_repo") or ""),
                "branch": str(item.get("branch") or ""),
                "base_ref": str(item.get("base_ref") or ""),
                "status": str(item.get("status") or ""),
                "provision_phase": str(item.get("provision_phase") or ""),
                "created_at": str(item.get("created_at") or ""),
                "updated_at": str(item.get("updated_at") or ""),
            }
            for item in state.get("worktrees", [])
            if isinstance(item, dict)
        ],
        "created_at": str(state.get("created_at") or ""),
        "updated_at": str(state.get("updated_at") or ""),
    }


async def _default_base_ref(repo: str, timeout_s: float) -> str:
    await run_exec(["git", "-C", repo, "fetch", "--quiet", "origin"], None, timeout_s)
    ref = await run_exec(["git", "-C", repo, "symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD"], None, timeout_s)
    if ref and not ref.startswith("error:") and ref != "(no output)":
        return ref.strip()
    branch = await run_exec(["git", "-C", repo, "rev-parse", "--abbrev-ref", "HEAD"], None, timeout_s)
    if branch and not branch.startswith("error:") and branch != "(no output)" and branch != "HEAD":
        return branch.strip()
    return "HEAD"


def _safe_base_ref(value: str) -> str:
    if not value or value.startswith("-"):
        raise ValueError(f"invalid base ref {value!r}")
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/._-")
    if any(char not in allowed for char in value):
        raise ValueError(f"invalid base ref {value!r}")
    if ".." in value or value.endswith(".") or value.endswith("/") or "@{" in value:
        raise ValueError(f"invalid base ref {value!r}")
    return value


def _workspace_lock(root: pathlib.Path, conversation_id: str) -> asyncio.Lock:
    path = str(workspace_path(root, conversation_id))
    lock = _WORKSPACE_LOCKS.get(path)
    if lock is None:
        lock = asyncio.Lock()
        _WORKSPACE_LOCKS[path] = lock
    return lock


def _read_state(path: pathlib.Path) -> dict[str, Any]:
    state_path = path / WORKSPACE_STATE_FILENAME
    if not state_path.exists():
        return {}
    try:
        data = json.loads(state_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_state(path: pathlib.Path, state: dict[str, Any]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    tmp = path / f".{WORKSPACE_STATE_FILENAME}.tmp"
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True))
    tmp.replace(path / WORKSPACE_STATE_FILENAME)
