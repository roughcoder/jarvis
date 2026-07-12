"""Unreleased review-ring deployment with atomic activation and rollback."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.error import HTTPError


ROLES = ("brain", "api", "intercom", "worker", "whatsapp")
ROLE_EXTRAS = {
    "brain": ("gateway", "tts", "stt", "vad", "wake", "memory", "mcp"),
    "api": ("gateway", "cockpit"),
    "intercom": ("stt", "vad", "wake"),
    "worker": ("worker", "browser"),
    "whatsapp": (),
}
KNOWN_EXTRAS = frozenset(
    extra for values in ROLE_EXTRAS.values() for extra in values
) | {"worker-claude"}
SHA_RE = re.compile(r"[0-9a-f]{40}")
REVIEW_RING_ROLES = frozenset({"api", "worker"})
PROBE_TOKEN_ENVS = frozenset({"ORCHESTRATION_API_TOKEN", "WORKER_TOKEN"})
REMOTE_TEMP_RE = re.compile(r"/tmp/jarvis-dogfood\.[A-Za-z0-9]+")


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, ANN201
        return None


@dataclass(frozen=True)
class DogfoodHost:
    name: str
    roles: tuple[str, ...]
    extras: tuple[str, ...]
    workdir: str
    runtime_root: str
    production_bin: str
    platform: str
    uv_bin: str
    python: str
    local: bool = False
    ssh: str = ""
    probes: tuple[dict[str, str], ...] = ()


def load_inventory(path: str | Path, *, repo_root: str | Path | None = None) -> list[DogfoodHost]:
    target = Path(path).expanduser().resolve()
    if repo_root is not None:
        root = Path(repo_root).resolve()
        try:
            relative = target.relative_to(root)
        except ValueError:
            relative = None
        if relative is not None:
            tracked = subprocess.run(
                ["git", "-C", str(root), "ls-files", "--error-unmatch", str(relative)],
                capture_output=True,
                text=True,
                check=False,
            )
            if tracked.returncode == 0:
                raise ValueError("dogfood inventory must remain private and untracked")
    raw = json.loads(target.read_text(encoding="utf-8"))
    values = raw.get("hosts") if isinstance(raw, dict) else None
    if not isinstance(values, list) or not values:
        raise ValueError("dogfood inventory requires a non-empty hosts list")
    hosts = tuple(_parse_host(value) for value in values)
    names = [host.name for host in hosts]
    if len(names) != len(set(names)):
        raise ValueError("dogfood host names must be unique")
    return list(hosts)


def _parse_host(value: Any) -> DogfoodHost:
    if not isinstance(value, dict):
        raise ValueError("each dogfood host must be an object")
    name = str(value.get("name") or "").strip()
    local = value.get("local") is True
    ssh = str(value.get("ssh") or "").strip()
    if not name or local == bool(ssh):
        raise ValueError("each dogfood host needs a name and exactly one of local=true or ssh")
    roles = tuple(dict.fromkeys(str(role) for role in value.get("roles") or ()))
    unknown_roles = sorted(set(roles) - set(ROLES))
    if not roles or unknown_roles:
        raise ValueError(f"dogfood host {name!r} has invalid roles: {unknown_roles or 'none'}")
    unsupported_roles = sorted(set(roles) - REVIEW_RING_ROLES)
    if unsupported_roles:
        raise ValueError(f"dogfood review ring does not support roles: {unsupported_roles}")
    extras = tuple(dict.fromkeys(str(extra) for extra in value.get("extras") or ()))
    unknown_extras = sorted(set(extras) - KNOWN_EXTRAS)
    if unknown_extras:
        raise ValueError(f"dogfood host {name!r} has invalid extras: {unknown_extras}")
    platform = str(value.get("platform") or "launchd")
    if platform != "launchd":
        raise ValueError("the dogfood review ring currently supports launchd hosts only")
    probes_raw = value.get("probes") or []
    if not isinstance(probes_raw, list):
        raise ValueError(f"dogfood host {name!r} probes must be a list")
    probes: list[dict[str, str]] = []
    for probe in probes_raw:
        if not isinstance(probe, dict):
            raise ValueError(f"dogfood host {name!r} has an invalid local HTTP probe")
        role = str(probe.get("role") or "")
        url = str(probe.get("url") or "")
        token_env = str(probe.get("token_env") or "")
        parsed = urlparse(url)
        loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        if (
            role not in roles
            or parsed.scheme != "http"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or token_env not in PROBE_TOKEN_ENVS | {""}
            or (bool(token_env) and not loopback)
        ):
            raise ValueError(f"dogfood host {name!r} has an invalid local HTTP probe")
        probes.append(
            {
                "role": role,
                "url": url,
                "token_env": token_env,
            }
        )
    probe_roles = [probe["role"] for probe in probes]
    if sorted(probe_roles) != sorted(roles) or len(probe_roles) != len(set(probe_roles)):
        raise ValueError(f"dogfood host {name!r} requires exactly one health probe per role")
    return DogfoodHost(
        name=name,
        local=local,
        ssh=ssh,
        roles=roles,
        extras=extras,
        workdir=str(value.get("workdir") or "~/.jarvis"),
        runtime_root=str(value.get("runtime_root") or "~/.jarvis/dogfood"),
        production_bin=str(value.get("production_bin") or "/opt/homebrew/bin/jarvis"),
        platform=platform,
        uv_bin=str(value.get("uv_bin") or "/opt/homebrew/bin/uv"),
        python=str(value.get("python") or "3.12"),
        probes=tuple(probes),
    )


def run_controller(
    action: str,
    *,
    inventory_path: str,
    commit: str = "HEAD",
    repo_root: str | Path | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    if action == "deploy":
        root = _git_root(repo_root)
        hosts = load_inventory(inventory_path, repo_root=root)
        return _deploy(root, hosts, commit=commit, dry_run=dry_run)
    try:
        root = _git_root(repo_root)
    except subprocess.CalledProcessError:
        root = None
    hosts = load_inventory(inventory_path, repo_root=root)
    results = [_invoke_host(host, f"_host-{action}", dry_run=dry_run) for host in hosts]
    return {"ok": all(result.get("ok") for result in results), "action": action, "hosts": results}


def _git_root(repo_root: str | Path | None) -> Path:
    cwd = Path(repo_root).resolve() if repo_root else Path.cwd()
    result = subprocess.run(
        ["git", "-C", str(cwd), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(result.stdout.strip()).resolve()


def _deploy(root: Path, hosts: list[DogfoodHost], *, commit: str, dry_run: bool) -> dict[str, Any]:
    dirty = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    if dirty:
        raise RuntimeError("dogfood deploy requires all tracked changes to be committed")
    sha = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--verify", f"{commit}^{{commit}}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip().lower()
    if SHA_RE.fullmatch(sha) is None:
        raise RuntimeError(f"invalid resolved commit: {sha}")
    with tempfile.TemporaryDirectory(prefix="jarvis-dogfood-") as temp:
        archive = Path(temp) / f"jarvis-{sha}.tar.gz"
        subprocess.run(
            ["git", "-C", str(root), "archive", "--format=tar.gz", "-o", str(archive), sha],
            check=True,
        )
        prepared: list[dict[str, Any]] = []
        for host in hosts:
            prepared.append(
                _invoke_host(host, "_host-prepare", sha=sha, archive=archive, dry_run=dry_run)
            )
        activated: list[DogfoodHost] = []
        attempted: DogfoodHost | None = None
        results: list[dict[str, Any]] = []
        try:
            for host in hosts:
                attempted = host
                result = _invoke_host(host, "_host-activate", sha=sha, dry_run=dry_run)
                results.append(result)
                activated.append(host)
                attempted = None
        except Exception:
            candidates = ([attempted] if attempted is not None else []) + list(reversed(activated))
            seen_hosts: set[str] = set()
            for host in (candidate for candidate in candidates if candidate is not None):
                if host.name in seen_hosts:
                    continue
                seen_hosts.add(host.name)
                status = _invoke_host(host, "_host-status", dry_run=dry_run, check=False)
                if status.get("git_sha") == sha:
                    _invoke_host(host, "_host-rollback", dry_run=dry_run, check=False)
            raise
    return {
        "ok": all(result.get("ok") for result in prepared + results),
        "action": "deploy",
        "git_sha": sha,
        "prepared": prepared,
        "hosts": results,
    }


def _encoded_host(host: DogfoodHost) -> str:
    payload = json.dumps(host.__dict__, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(payload).decode()


def _invoke_host(
    host: DogfoodHost,
    action: str,
    *,
    sha: str = "",
    archive: Path | None = None,
    dry_run: bool = False,
    check: bool = True,
) -> dict[str, Any]:
    helper = Path(__file__).resolve()
    if dry_run:
        return {"ok": True, "host": host.name, "action": action.removeprefix("_host-"), "dry_run": True}
    if host.ssh:
        allocated = subprocess.run(
            ["ssh", host.ssh, "umask 077 && mktemp -d /tmp/jarvis-dogfood.XXXXXXXXXX"],
            capture_output=True,
            text=True,
            check=False,
        )
        remote_root = allocated.stdout.strip()
        if allocated.returncode != 0 or REMOTE_TEMP_RE.fullmatch(remote_root) is None:
            raise RuntimeError(
                f"dogfood could not allocate a private temporary directory on {host.name}: "
                f"{(allocated.stderr or allocated.stdout).strip()}"
            )
        remote_helper = f"{remote_root}/helper.py"
        remote_archive = f"{remote_root}/archive.tar.gz" if archive else ""
        try:
            subprocess.run(["scp", "-q", str(helper), f"{host.ssh}:{remote_helper}"], check=True)
            if archive is not None:
                subprocess.run(["scp", "-q", str(archive), f"{host.ssh}:{remote_archive}"], check=True)
            argv = [
                host.uv_bin,
                "run",
                "--no-project",
                "--python",
                host.python,
                "python",
                remote_helper,
                action,
                "--host-json",
                _encoded_host(host),
            ]
            if sha:
                argv.extend(["--sha", sha])
            if remote_archive:
                argv.extend(["--archive", remote_archive])
            result = subprocess.run(
                ["ssh", host.ssh, shlex.join(argv)],
                capture_output=True,
                text=True,
                check=False,
            )
        finally:
            subprocess.run(
                ["ssh", host.ssh, f"rm -rf -- {shlex.quote(remote_root)}"],
                capture_output=True,
                text=True,
                check=False,
            )
    else:
        argv = [sys.executable, str(helper), action, "--host-json", _encoded_host(host)]
        if sha:
            argv.extend(["--sha", sha])
        if archive is not None:
            argv.extend(["--archive", str(archive)])
        result = subprocess.run(argv, capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        raise RuntimeError(
            f"dogfood {action.removeprefix('_host-')} failed on {host.name}: "
            f"{(result.stderr or result.stdout).strip()}"
        )
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        if check:
            raise RuntimeError(f"dogfood host {host.name} returned invalid JSON") from exc
        payload = {"ok": False, "error": "invalid host response"}
    return {"host": host.name, **payload}


def _host_from_encoded(value: str) -> DogfoodHost:
    raw = json.loads(base64.urlsafe_b64decode(value.encode()))
    raw["probes"] = list(raw.get("probes") or ())
    return _parse_host(raw)


def _host_prepare(host: DogfoodHost, *, sha: str, archive: str) -> dict[str, Any]:
    if SHA_RE.fullmatch(sha) is None:
        raise RuntimeError("host prepare requires a full git SHA")
    root = Path(host.runtime_root).expanduser()
    build = root / "builds" / sha
    roles = list(host.roles)
    extras = _extras_for_host(host)
    expected = {"git_sha": sha, "roles": roles, "extras": extras}
    root.mkdir(parents=True, exist_ok=True)
    manifest_path = build / "manifest.json"
    reused = manifest_path.exists()
    if reused:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if any(manifest.get(key) != value for key, value in expected.items()):
            raise RuntimeError(f"immutable dogfood build {sha} has different roles or extras")
        if not (build / "bin" / "jarvis").is_file() or not (build / ".venv" / "bin" / "jarvis").is_file():
            raise RuntimeError(f"immutable dogfood build {sha} is incomplete")
    else:
        build.parent.mkdir(parents=True, exist_ok=True)
        created = False
        try:
            build.mkdir()
            created = True
            source = build / "source"
            source.mkdir()
            with tarfile.open(archive, "r:gz") as tar:
                tar.extractall(source, filter="data")
            env = os.environ.copy()
            env["UV_PROJECT_ENVIRONMENT"] = str(build / ".venv")
            command = [host.uv_bin, "sync", "--frozen", "--no-dev", "--python", host.python]
            for extra in extras:
                command.extend(["--extra", extra])
            subprocess.run(command, cwd=source, env=env, check=True)
            version = str(
                tomllib.loads((source / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]
            )
            bin_dir = build / "bin"
            bin_dir.mkdir()
            wrapper = bin_dir / "jarvis"
            wrapper.write_text(
                "#!/bin/sh\n"
                "export JARVIS_RUNTIME_CHANNEL=dogfood\n"
                f"export JARVIS_RUNTIME_GIT_SHA={sha}\n"
                f'exec "{build / ".venv" / "bin" / "jarvis"}" "$@"\n',
                encoding="utf-8",
            )
            wrapper.chmod(0o755)
            _write_json_atomic(
                build / "manifest.json",
                {**expected, "version": version, "channel": "dogfood"},
            )
        except Exception:
            if created:
                shutil.rmtree(build, ignore_errors=True)
            raise
    _ensure_stable_launcher(root)
    current = _symlink_target(root / "current") or host.production_bin
    _write_json_atomic(
        root / "transaction.json",
        {"git_sha": sha, "previous_target": current, "prepared_at": time.time()},
    )
    return {"ok": True, "action": "prepare", "git_sha": sha, "reused": reused}


def _host_activate(host: DogfoodHost, *, sha: str) -> dict[str, Any]:
    root = Path(host.runtime_root).expanduser()
    previous_link = root / "previous"
    original_previous_target = _symlink_target(previous_link)
    transaction = _read_json(root / "transaction.json")
    if transaction.get("git_sha") != sha:
        raise RuntimeError("dogfood activation has no matching prepared transaction")
    build = root / "builds" / sha
    target = str(build / "bin" / "jarvis")
    if not Path(target).is_file():
        raise RuntimeError(f"prepared dogfood build is missing: {sha}")
    previous_target = str(transaction.get("previous_target") or host.production_bin)
    state_path = root / "state.json"
    old_state = _read_json(state_path)
    _atomic_symlink(previous_target, previous_link)
    _atomic_symlink(target, root / "current")
    try:
        _configure_services(host, root)
    except Exception:
        _atomic_symlink(previous_target, root / "current")
        _restore_optional_symlink(original_previous_target, previous_link)
        _restart_services(host, root, tolerate_failure=True)
        raise
    state = {
        "channel": "dogfood",
        "git_sha": sha,
        "current_target": target,
        "previous_target": previous_target,
        "previous_sha": str(old_state.get("git_sha") or ""),
        "services_bound": list(host.roles),
        "activated_at": time.time(),
    }
    _write_json_atomic(state_path, state)
    (root / "transaction.json").unlink(missing_ok=True)
    status = _wait_for_status(host)
    if not status["ok"]:
        _atomic_symlink(previous_target, root / "current")
        _restore_optional_symlink(original_previous_target, previous_link)
        if old_state:
            _write_json_atomic(state_path, old_state)
        else:
            state_path.unlink(missing_ok=True)
        _restart_services(host, root, tolerate_failure=True)
        raise RuntimeError(f"dogfood activation health failed: {status['issues']}")
    return {"ok": True, "action": "activate", "git_sha": sha, "status": status}


def _host_rollback(host: DogfoodHost) -> dict[str, Any]:
    root = Path(host.runtime_root).expanduser()
    state_path = root / "state.json"
    state = _read_json(state_path)
    old_state = dict(state)
    current = _symlink_target(root / "current")
    previous = _symlink_target(root / "previous") or str(state.get("previous_target") or "")
    if not current or not previous:
        raise RuntimeError("dogfood rollback has no previous runtime target")
    _atomic_symlink(previous, root / "current")
    _atomic_symlink(current, root / "previous")
    try:
        _restart_services(host, root)
    except Exception:
        _atomic_symlink(current, root / "current")
        _atomic_symlink(previous, root / "previous")
        _restart_services(host, root, tolerate_failure=True)
        raise
    current_sha = str(state.get("git_sha") or "")
    previous_sha = str(state.get("previous_sha") or "")
    state.update(
        {
            "channel": "production" if Path(previous).resolve() == Path(host.production_bin).resolve() else "dogfood",
            "git_sha": previous_sha,
            "previous_sha": current_sha,
            "current_target": previous,
            "previous_target": current,
            "activated_at": time.time(),
        }
    )
    _write_json_atomic(state_path, state)
    status = _wait_for_status(host)
    if not status["ok"]:
        _atomic_symlink(current, root / "current")
        _atomic_symlink(previous, root / "previous")
        _write_json_atomic(state_path, old_state)
        _restart_services(host, root, tolerate_failure=True)
        raise RuntimeError(f"dogfood rollback health failed: {status['issues']}")
    return {"ok": True, "action": "rollback", "git_sha": previous_sha, "status": status}


def _host_status(host: DogfoodHost) -> dict[str, Any]:
    root = Path(host.runtime_root).expanduser()
    state = _read_json(root / "state.json")
    current = _symlink_target(root / "current")
    previous = _symlink_target(root / "previous")
    issues: list[str] = []
    selected_production = bool(current) and Path(current).resolve() == Path(host.production_bin).resolve()
    if not current or not Path(current).is_file():
        issues.append("active runtime target is missing")
    services: dict[str, Any] = {}
    launcher = root / "bin" / "jarvis"
    for role in host.roles:
        result = subprocess.run(
            [str(launcher), "service", "status", role, "--platform", host.platform],
            capture_output=True,
            text=True,
            check=False,
        ) if launcher.is_file() and current else subprocess.CompletedProcess([], 1, "", "launcher missing")
        services[role] = {"ok": result.returncode == 0, "returncode": result.returncode}
        if result.returncode != 0:
            issues.append(f"{role} service is not healthy")
    probes = [_probe(host, probe) for probe in host.probes]
    expected_sha = str(state.get("git_sha") or "")
    expected_channel = str(state.get("channel") or ("production" if selected_production else ""))
    for probe in probes:
        runtime = probe.get("runtime") if isinstance(probe, dict) else None
        legacy_production_probe = selected_production and (
            (probe.get("ok") and runtime is None) or probe.get("status") == 404
        )
        if legacy_production_probe:
            continue
        if not probe.get("ok"):
            issues.append(f"{probe.get('role') or 'runtime'} probe failed")
        elif not isinstance(runtime, dict) or runtime.get("channel") != expected_channel or runtime.get("git_sha", "") != expected_sha:
            issues.append(f"{probe.get('role') or 'runtime'} process identity does not match selected runtime")
    return {
        "ok": not issues,
        "action": "status",
        "channel": expected_channel,
        "git_sha": expected_sha,
        "current_target": current,
        "previous_target": previous,
        "services": services,
        "probes": probes,
        "issues": issues,
    }


def _wait_for_status(host: DogfoodHost, *, timeout_s: float = 30.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    status = _host_status(host)
    while not status["ok"] and time.monotonic() < deadline:
        time.sleep(1)
        status = _host_status(host)
    return status


def _extras_for_host(host: DogfoodHost) -> list[str]:
    result: list[str] = []
    for role in ROLES:
        if role in host.roles:
            for extra in ROLE_EXTRAS[role]:
                if extra not in result:
                    result.append(extra)
    for extra in host.extras:
        if extra not in result:
            result.append(extra)
    return result


def _ensure_stable_launcher(root: Path) -> None:
    launcher = root / "bin" / "jarvis"
    launcher.parent.mkdir(parents=True, exist_ok=True)
    launcher.write_text(
        "#!/bin/sh\n"
        'exec "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/current" "$@"\n',
        encoding="utf-8",
    )
    launcher.chmod(0o755)


def _configure_services(host: DogfoodHost, root: Path) -> None:
    launcher = str(root / "bin" / "jarvis")
    active = str(root / "current")
    subprocess.run(
        [active, "service", "install", *host.roles, "--platform", host.platform, "--jarvis-bin", launcher, "--workdir", str(Path(host.workdir).expanduser())],
        check=True,
        capture_output=True,
        text=True,
    )
    for role in host.roles:
        subprocess.run(
            [active, "service", "stop", role, "--platform", host.platform],
            check=False,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            [active, "service", "start", role, "--platform", host.platform],
            check=True,
            capture_output=True,
            text=True,
        )


def _restart_services(host: DogfoodHost, root: Path, *, tolerate_failure: bool = False) -> None:
    launcher = str(root / "bin" / "jarvis")
    for role in host.roles:
        result = subprocess.run(
            [launcher, "service", "restart", role, "--platform", host.platform],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            result = subprocess.run(
                [launcher, "service", "start", role, "--platform", host.platform],
                check=False,
                capture_output=True,
                text=True,
            )
        if result.returncode != 0 and not tolerate_failure:
            raise RuntimeError(f"failed to restart {role}: {(result.stderr or result.stdout).strip()}")


def _probe(host: DogfoodHost, probe: dict[str, str]) -> dict[str, Any]:
    headers: dict[str, str] = {}
    token_env = probe.get("token_env") or ""
    if token_env:
        token = _dotenv_value(Path(host.workdir).expanduser() / ".env", token_env)
        if not token:
            return {"ok": False, "role": probe.get("role"), "error": f"missing {token_env}"}
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(probe["url"], headers=headers)
    try:
        opener = urllib.request.build_opener(_NoRedirectHandler())
        with opener.open(request, timeout=8) as response:
            payload = json.load(response)
    except HTTPError as exc:
        return {
            "ok": False,
            "role": probe.get("role"),
            "status": exc.code,
            "error": f"HTTP {exc.code}",
        }
    except Exception as exc:  # noqa: BLE001 - status reports bounded probe failures
        return {"ok": False, "role": probe.get("role"), "error": str(exc)[:200]}
    return {"ok": bool(payload.get("ok")), "role": probe.get("role"), "runtime": payload.get("runtime")}


def _dotenv_value(path: Path, key: str) -> str:
    if not path.is_file():
        return ""
    prefix = f"{key}="
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[7:].lstrip()
        if line.startswith(prefix):
            return line[len(prefix) :].strip().strip("'\"")
    return ""


def _atomic_symlink(target: str, link: Path) -> None:
    link.parent.mkdir(parents=True, exist_ok=True)
    temporary = link.parent / f".{link.name}.{uuid.uuid4().hex}.tmp"
    os.symlink(target, temporary)
    os.replace(temporary, link)


def _restore_optional_symlink(target: str, link: Path) -> None:
    if target:
        _atomic_symlink(target, link)
    else:
        link.unlink(missing_ok=True)


def _symlink_target(path: Path) -> str:
    try:
        target = os.readlink(path)
    except OSError:
        return ""
    if not os.path.isabs(target):
        target = str((path.parent / target).resolve())
    return target


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {}


def _write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _internal_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["_host-prepare", "_host-activate", "_host-status", "_host-rollback"])
    parser.add_argument("--host-json", required=True)
    parser.add_argument("--sha", default="")
    parser.add_argument("--archive", default="")
    args = parser.parse_args(argv)
    host = _host_from_encoded(args.host_json)
    try:
        if args.action == "_host-prepare":
            result = _host_prepare(host, sha=args.sha, archive=args.archive)
        elif args.action == "_host-activate":
            result = _host_activate(host, sha=args.sha)
        elif args.action == "_host-rollback":
            result = _host_rollback(host)
        else:
            result = _host_status(host)
    except Exception as exc:  # noqa: BLE001 - remote helper must return concise JSON
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(_internal_main(sys.argv[1:]))
