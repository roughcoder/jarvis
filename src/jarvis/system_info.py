from __future__ import annotations

import os
import platform
import plistlib
import re
import shutil
import socket
import subprocess
import threading
import time
from datetime import UTC, datetime
from typing import Any

SYSTEM_INFO_CACHE_TTL_S = 10.0

_CACHE_LOCK = threading.Lock()
_CACHE: tuple[float, dict[str, Any]] | None = None
_CACHE_REFRESHING = False


def system_info(*, now: float | None = None, cache_ttl_s: float = SYSTEM_INFO_CACHE_TTL_S) -> dict[str, Any]:
    global _CACHE
    current = time.time() if now is None else now
    with _CACHE_LOCK:
        if _CACHE is not None and _CACHE[0] > current:
            return _with_checked_at(dict(_CACHE[1]))
    data = _collect_system_info()
    with _CACHE_LOCK:
        _CACHE = (current + cache_ttl_s, dict(data))
    return _with_checked_at(data)


def system_info_cached(*, now: float | None = None, cache_ttl_s: float = SYSTEM_INFO_CACHE_TTL_S) -> dict[str, Any]:
    current = time.time() if now is None else now
    stale: dict[str, Any] | None = None
    with _CACHE_LOCK:
        if _CACHE is not None:
            stale = dict(_CACHE[1])
            if _CACHE[0] > current:
                return _with_checked_at(stale)
    _start_refresh(current, cache_ttl_s)
    if stale is not None:
        return _with_checked_at(stale)
    return _with_checked_at(_empty_system_info())


def _collect_system_info() -> dict[str, Any]:
    platform_id = platform.system().lower() or None
    arch = platform.machine() or None
    total, available = _memory()
    used = _diff(total, available)
    return {
        "hostname": _safe_hostname(),
        "platform": platform_id,
        "arch": arch,
        "os_name": _os_name(platform_id),
        "os_version": _os_version(platform_id),
        "kernel_version": platform.release() or None,
        "cpu_model": _cpu_model(platform_id),
        "cpu_cores_physical": _cpu_cores_physical(platform_id),
        "cpu_cores_logical": os.cpu_count(),
        "memory_total_bytes": total,
        "memory_available_bytes": available,
        "memory_used_bytes": used,
        "memory_used_percent": _percent(used, total),
        "load_average": _load_average(),
        "uptime_seconds": _uptime_seconds(platform_id),
        "disk": [_disk_root()],
        "gpu": _gpu(platform_id),
        "checked_at": None,
    }


def _start_refresh(current: float, cache_ttl_s: float) -> None:
    global _CACHE_REFRESHING
    with _CACHE_LOCK:
        if _CACHE_REFRESHING:
            return
        _CACHE_REFRESHING = True

    def refresh() -> None:
        global _CACHE, _CACHE_REFRESHING
        try:
            data = _collect_system_info()
            with _CACHE_LOCK:
                _CACHE = (current + cache_ttl_s, dict(data))
        finally:
            with _CACHE_LOCK:
                _CACHE_REFRESHING = False

    threading.Thread(target=refresh, name="jarvis-system-info-refresh", daemon=True).start()


def _empty_system_info() -> dict[str, Any]:
    return {
        "hostname": None,
        "platform": None,
        "arch": None,
        "os_name": None,
        "os_version": None,
        "kernel_version": None,
        "cpu_model": None,
        "cpu_cores_physical": None,
        "cpu_cores_logical": None,
        "memory_total_bytes": None,
        "memory_available_bytes": None,
        "memory_used_bytes": None,
        "memory_used_percent": None,
        "load_average": [None, None, None],
        "uptime_seconds": None,
        "disk": [],
        "gpu": [],
        "checked_at": None,
    }


def _with_checked_at(data: dict[str, Any]) -> dict[str, Any]:
    data["checked_at"] = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return data


def _safe_hostname() -> str | None:
    value = socket.gethostname().split(".", 1)[0].strip()
    return value or None


def _os_name(platform_id: str | None) -> str | None:
    if platform_id == "darwin":
        return "macOS"
    if platform_id == "linux":
        try:
            return platform.freedesktop_os_release().get("NAME") or "Linux"
        except OSError:
            return "Linux"
    return platform.system() or None


def _os_version(platform_id: str | None) -> str | None:
    if platform_id == "darwin":
        return platform.mac_ver()[0] or None
    if platform_id == "linux":
        try:
            release = platform.freedesktop_os_release()
            return release.get("VERSION_ID") or release.get("VERSION")
        except OSError:
            return platform.release() or None
    return platform.version() or None


def _cpu_model(platform_id: str | None) -> str | None:
    if platform_id == "darwin":
        return _run_text(["sysctl", "-n", "machdep.cpu.brand_string"]) or platform.processor() or None
    if platform_id == "linux":
        try:
            for line in _read_text("/proc/cpuinfo").splitlines():
                if line.lower().startswith("model name"):
                    return line.split(":", 1)[1].strip() or None
        except OSError:
            pass
    return platform.processor() or None


def _cpu_cores_physical(platform_id: str | None) -> int | None:
    if platform_id == "darwin":
        return _int(_run_text(["sysctl", "-n", "hw.physicalcpu"]))
    if platform_id == "linux":
        output = _run_text(["sh", "-c", "lscpu -p=CORE,SOCKET 2>/dev/null | grep -v '^#' | sort -u | wc -l"])
        return _int(output)
    return None


def _memory() -> tuple[int | None, int | None]:
    platform_id = platform.system().lower()
    if platform_id == "darwin":
        return _darwin_memory()
    if platform_id == "linux":
        return _linux_memory()
    return None, None


def _darwin_memory() -> tuple[int | None, int | None]:
    total = _int(_run_text(["sysctl", "-n", "hw.memsize"]))
    vm_stat = _run_text(["vm_stat"])
    if not vm_stat:
        return total, None
    page_size_match = re.search(r"page size of (\d+) bytes", vm_stat)
    page_size = _int(page_size_match.group(1) if page_size_match else "") or 4096
    pages: dict[str, int] = {}
    for line in vm_stat.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        pages[key.strip()] = _int(value.strip().rstrip(".")) or 0
    available_pages = (
        pages.get("Pages free", 0)
        + pages.get("Pages inactive", 0)
        + pages.get("Pages speculative", 0)
    )
    return total, available_pages * page_size if available_pages else None


def _linux_memory() -> tuple[int | None, int | None]:
    try:
        rows = {}
        for line in _read_text("/proc/meminfo").splitlines():
            key, value = line.split(":", 1)
            rows[key] = (_int(value.strip().split()[0]) or 0) * 1024
        return rows.get("MemTotal"), rows.get("MemAvailable")
    except (OSError, ValueError):
        return None, None


def _load_average() -> list[float | None]:
    try:
        return [round(value, 2) for value in os.getloadavg()]
    except OSError:
        return [None, None, None]


def _uptime_seconds(platform_id: str | None) -> int | None:
    if platform_id == "darwin":
        boottime = _run_text(["sysctl", "-n", "kern.boottime"])
        match = re.search(r"sec = (\d+)", boottime)
        boot_seconds = _int(match.group(1) if match else "")
        return int(time.time() - boot_seconds) if boot_seconds else None
    if platform_id == "linux":
        try:
            return int(float(_read_text("/proc/uptime").split()[0]))
        except (OSError, ValueError, IndexError):
            return None
    return None


def _disk_root() -> dict[str, Any]:
    mount = "/"
    try:
        usage = shutil.disk_usage(mount)
        total = int(usage.total)
        available = int(usage.free)
        used = int(usage.used)
    except OSError:
        total = available = used = None
    return {
        "mount": mount,
        "filesystem": _filesystem(mount),
        "total_bytes": total,
        "available_bytes": available,
        "used_bytes": used,
        "used_percent": _percent(used, total),
    }


def _filesystem(mount: str) -> str | None:
    if platform.system().lower() == "darwin":
        return _darwin_filesystem(mount)
    return _run_text(["stat", "-f", "-c", "%T", mount]) or None


def _darwin_filesystem(mount: str) -> str | None:
    data = _run_bytes(["diskutil", "info", "-plist", mount])
    if data:
        try:
            plist = plistlib.loads(data)
            filesystem = str(plist.get("FilesystemType") or plist.get("FilesystemName") or "").strip()
            if filesystem:
                return filesystem.lower()
        except (plistlib.InvalidFileException, ValueError, TypeError):
            pass
    return None


def _gpu(platform_id: str | None) -> list[dict[str, Any]]:
    if platform_id == "darwin":
        model = _cpu_model(platform_id)
        if model and model.startswith("Apple "):
            return [{"name": model, "memory_total_bytes": None}]
    return []


def _percent(used: int | None, total: int | None) -> float | None:
    if used is None or total in {None, 0}:
        return None
    return round((used / total) * 100, 1)


def _diff(total: int | None, available: int | None) -> int | None:
    if total is None or available is None:
        return None
    return max(0, total - available)


def _int(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _read_text(path: str) -> str:
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _run_text(argv: list[str]) -> str:
    try:
        result = subprocess.run(argv, text=True, capture_output=True, timeout=1.5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _run_bytes(argv: list[str]) -> bytes:
    try:
        result = subprocess.run(argv, capture_output=True, timeout=1.5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        return b""
    if result.returncode != 0:
        return b""
    return result.stdout
