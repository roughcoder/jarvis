#!/usr/bin/env python3
"""Exercise the dev-only Honcho v3 stack through its HTTP API.

This script assumes the compose stack is already running. It intentionally uses
only network calls to Honcho, matching Jarvis's service-boundary rule.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


BASE_URL = os.environ.get("HONCHO_V3_API_URL", "http://localhost:8003").rstrip("/")
WORKSPACE = os.environ.get("HONCHO_V3_WORKSPACE", "jarvis-dev")
SESSION = os.environ.get("HONCHO_V3_SESSION", f"validation-{int(time.time())}")
PROJECT_PEER = os.environ.get("HONCHO_V3_PROJECT_PEER", "project-jarvis")
TIMEOUT_S = int(os.environ.get("HONCHO_V3_VALIDATE_TIMEOUT_S", "240"))
POLL_S = float(os.environ.get("HONCHO_V3_VALIDATE_POLL_S", "2"))


def _json_request(
    method: str,
    path: str,
    payload: dict[str, Any] | list[Any] | None = None,
    *,
    expected: tuple[int, ...] = (200,),
) -> Any:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            if response.status not in expected:
                raise RuntimeError(f"{method} {path}: {response.status} {body}")
            return json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path}: {exc.code} {body}") from exc


def _get(path: str, *, expected: tuple[int, ...] = (200,)) -> Any:
    return _json_request("GET", path, expected=expected)


def _post(
    path: str,
    payload: dict[str, Any] | list[Any],
    *,
    expected: tuple[int, ...] = (200,),
) -> Any:
    return _json_request("POST", path, payload, expected=expected)


def _delete(path: str, *, expected: tuple[int, ...] = (204,)) -> Any:
    return _json_request("DELETE", path, expected=expected)


def wait_for_health() -> None:
    deadline = time.monotonic() + 90
    last_error = ""
    while time.monotonic() < deadline:
        try:
            health = _get("/health")
            if health.get("status") == "ok":
                print("health=ok")
                return
        except Exception as exc:  # noqa: BLE001 - diagnostic output
            last_error = str(exc)
        time.sleep(2)
    raise RuntimeError(f"Honcho health did not become ready: {last_error}")


def queue_status() -> dict[str, Any]:
    return _get(f"/v3/workspaces/{urllib.parse.quote(WORKSPACE)}/queue/status")


def wait_for_queue_idle(label: str) -> dict[str, Any]:
    deadline = time.monotonic() + TIMEOUT_S
    last = {}
    while time.monotonic() < deadline:
        last = queue_status()
        pending = int(last.get("pending_work_units", 0))
        in_progress = int(last.get("in_progress_work_units", 0))
        if pending == 0 and in_progress == 0:
            print(f"queue_idle[{label}]={json.dumps(last, sort_keys=True)}")
            return last
        time.sleep(POLL_S)
    raise RuntimeError(f"queue did not drain after {label}: {last}")


def list_conclusions() -> list[dict[str, Any]]:
    page = _post(f"/v3/workspaces/{WORKSPACE}/conclusions/list", {}, expected=(200,))
    return page.get("items", [])


def wait_for_new_conclusions(before_count: int) -> list[dict[str, Any]]:
    deadline = time.monotonic() + TIMEOUT_S
    latest: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        latest = list_conclusions()
        if len(latest) > before_count:
            print(f"new_conclusions={len(latest) - before_count}")
            return latest
        time.sleep(POLL_S)
    raise RuntimeError(
        f"deriver did not create conclusions: before={before_count}, after={len(latest)}"
    )


def assert_contains(label: str, value: str | None, pattern: str) -> None:
    if not value or re.search(pattern, value, re.IGNORECASE) is None:
        raise AssertionError(f"{label} did not contain /{pattern}/: {value!r}")


def docker_log_counts() -> dict[str, int] | None:
    try:
        logs = subprocess.check_output(
            ["docker", "logs", "jarvis-litellm-v3"],
            stderr=subprocess.STDOUT,
            text=True,
            timeout=20,
        )
    except Exception:
        return None
    return {
        "chat_completions": len(re.findall(r"/v1/(?:chat/)?completions", logs)),
        "embeddings": len(re.findall(r"/v1/embeddings", logs)),
    }


def main() -> int:
    print(f"base_url={BASE_URL}")
    print(f"workspace={WORKSPACE}")
    print(f"session={SESSION}")
    wait_for_health()

    _post(
        "/v3/workspaces",
        {
            "id": WORKSPACE,
            "metadata": {"purpose": "jarvis honcho v3 LiteLLM validation"},
        },
        expected=(200, 201),
    )

    _post(
        f"/v3/workspaces/{WORKSPACE}/sessions",
        {
            "id": SESSION,
            "peers": {
                "neil": {"observe_me": True, "observe_others": False},
                "jarvis": {"observe_me": False, "observe_others": False},
            },
            "configuration": {
                "summary": {
                    "enabled": True,
                    "messages_per_short_summary": 10,
                    "messages_per_long_summary": 20,
                },
                "reasoning": {"enabled": True},
                "dream": {"enabled": True},
            },
        },
        expected=(200, 201),
    )
    _post(
        f"/v3/workspaces/{WORKSPACE}/peers",
        {
            "id": PROJECT_PEER,
            "metadata": {"kind": "project", "source": "honcho-v3-validation"},
        },
        expected=(200, 201),
    )

    message_pairs: list[dict[str, Any]] = []
    facts = [
        "I am Neil, and my validation keyword for this Honcho v3 test is copper-lantern.",
        "My sister Sarah lives in Berlin and likes quiet Sunday calls.",
        "For Jarvis validation, the tea is kept in the blue tin.",
        "The memory validation project should record explicit conclusions for Lane 2.",
        "If asked about the validation keyword, answer copper-lantern.",
        "The Honcho v3 validation session is testing LiteLLM routing.",
    ]
    for index, fact in enumerate(facts, start=1):
        message_pairs.append(
            {
                "peer_id": "neil",
                "content": f"Fact {index}: {fact}",
                "metadata": {"source": "honcho-v3-validation", "index": index},
            }
        )
        message_pairs.append(
            {
                "peer_id": "jarvis",
                "content": f"Recorded validation fact {index}.",
                "metadata": {"source": "honcho-v3-validation", "index": index},
            }
        )

    conclusions_before_messages = len(list_conclusions())
    messages = _post(
        f"/v3/workspaces/{WORKSPACE}/sessions/{urllib.parse.quote(SESSION, safe='')}/messages",
        {"messages": message_pairs},
        expected=(201,),
    )
    print(f"messages_created={len(messages)}")
    conclusions = wait_for_new_conclusions(conclusions_before_messages)
    wait_for_queue_idle("messages")

    print(f"conclusions_after_deriver={len(conclusions)}")

    representation = _post(
        f"/v3/workspaces/{WORKSPACE}/peers/neil/representation",
        {
            "session_id": SESSION,
            "search_query": "validation keyword copper lantern",
            "search_top_k": 5,
            "max_conclusions": 10,
        },
    )
    print("representation_excerpt=" + representation["representation"][:500].replace("\n", " "))
    assert_contains("representation", representation["representation"], "copper|lantern|validation")

    summaries = _get(
        f"/v3/workspaces/{WORKSPACE}/sessions/{urllib.parse.quote(SESSION, safe='')}/summaries"
    )
    short_summary = summaries.get("short_summary")
    print(f"short_summary_present={short_summary is not None}")
    if short_summary is None:
        raise AssertionError("short summary was not generated after 12 messages")

    chat = _post(
        f"/v3/workspaces/{WORKSPACE}/peers/neil/chat",
        {
            "session_id": SESSION,
            "query": "What validation keyword did Neil give? Answer with just the keyword if possible.",
            "reasoning_level": "low",
            "stream": False,
        },
    )
    print(f"dialectic_chat={chat.get('content')!r}")
    assert_contains("dialectic_chat", chat.get("content"), "copper|lantern")

    explicit = _post(
        f"/v3/workspaces/{WORKSPACE}/conclusions",
        {
            "conclusions": [
                {
                    "observer_id": "neil",
                    "observed_id": PROJECT_PEER,
                    "session_id": SESSION,
                    "content": "Validation explicit conclusion: Honcho v3 conclusion CRUD works for Jarvis Lane 2.",
                }
            ]
        },
        expected=(201,),
    )[0]
    explicit_id = explicit["id"]
    print(f"explicit_conclusion_created={explicit_id}")

    listed = list_conclusions()
    if not any(item["id"] == explicit_id for item in listed):
        raise AssertionError("created explicit conclusion was not listed")
    _delete(f"/v3/workspaces/{WORKSPACE}/conclusions/{explicit_id}")
    listed_after_delete = list_conclusions()
    if any(item["id"] == explicit_id for item in listed_after_delete):
        raise AssertionError("explicit conclusion still listed after delete")
    print("explicit_conclusion_crud=ok")

    _post(
        f"/v3/workspaces/{WORKSPACE}/schedule_dream",
        {
            "observer": "neil",
            "observed": "neil",
            "dream_type": "omni",
            "session_id": SESSION,
        },
        expected=(204,),
    )
    wait_for_queue_idle("dream")

    final_status = queue_status()
    print(f"final_queue_status={json.dumps(final_status, sort_keys=True)}")

    counts = docker_log_counts()
    if counts is not None:
        print(f"litellm_log_counts={json.dumps(counts, sort_keys=True)}")
        if counts["chat_completions"] == 0 or counts["embeddings"] == 0:
            raise AssertionError(f"LiteLLM logs missing expected calls: {counts}")
    else:
        print("litellm_log_counts=skipped (jarvis-litellm-v3 container not present)")

    print("validation=ok")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 - top-level diagnostic
        print(f"validation=failed: {exc}", file=sys.stderr)
        raise
