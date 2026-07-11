"""Safe runtime build identity for fleet and health surfaces."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

from jarvis import __version__


_CHANNEL_RE = re.compile(r"[a-z0-9][a-z0-9._-]{0,31}")
_GIT_SHA_RE = re.compile(r"[0-9a-f]{40}")


def runtime_info(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Return non-secret identity for the code backing the current process."""
    env = os.environ if environ is None else environ
    channel = str(env.get("JARVIS_RUNTIME_CHANNEL") or "production").strip().lower()
    git_sha = str(env.get("JARVIS_RUNTIME_GIT_SHA") or "").strip().lower()
    if _CHANNEL_RE.fullmatch(channel) is None:
        channel = "unknown"
    if _GIT_SHA_RE.fullmatch(git_sha) is None:
        git_sha = ""
    return {"version": __version__, "channel": channel, "git_sha": git_sha}
