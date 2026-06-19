#!/usr/bin/env python
"""List the OpenAI models the project key in .env can use, and flag which ones
peekaboo's agent accepts.

The OpenAI `/v1/models` endpoint returns exactly the models the key's PROJECT is
allowed (the "Allowed models" list in Project Settings → Limits). Run after editing
that list to confirm what `control_mac` / `describe_screen` can use.

    uv run python scripts/check_openai_models.py
"""

from __future__ import annotations

import pathlib
import re
import sys

import httpx

# The OpenAI provider models peekaboo's `agent --model` accepts (from its validator).
PEEKABOO_OPENAI = [
    "gpt-5", "gpt-5-mini", "gpt-5-nano", "gpt-5-pro",
    "gpt-5.4", "gpt-5.4-mini", "gpt-5.4-nano", "gpt-5.5",
]


def _key() -> str:
    env = pathlib.Path(".env").read_text(encoding="utf-8")
    m = re.search(r"^OPENAI_API_KEY=(.+)$", env, re.MULTILINE)
    if not m or not m.group(1).strip():
        sys.exit("OPENAI_API_KEY not found in .env")
    return m.group(1).strip()


def main() -> int:
    key = _key()
    r = httpx.get(
        "https://api.openai.com/v1/models",
        headers={"Authorization": f"Bearer {key}"},
        timeout=20.0,
    )
    if r.status_code != 200:
        sys.exit(f"OpenAI /v1/models failed ({r.status_code}): {r.text[:300]}")
    available = {m["id"] for m in r.json().get("data", [])}
    proj = key.split("-")[2][:8] if key.startswith("sk-proj-") else "(non-project key)"

    print(f"{len(available)} models available to this key (project ~{proj}…).\n")
    print("peekaboo agent (`--model`) options — ✅ = this project allows it:")
    for m in PEEKABOO_OPENAI:
        print(f"  {'✅' if m in available else '❌'} {m}")

    extra = sorted(m for m in available if m.startswith("gpt-5") and m not in PEEKABOO_OPENAI)
    if extra:
        print("\nOther gpt-5* the project allows (NOT peekaboo-valid, e.g. dated/codex):")
        print("  " + ", ".join(extra))

    usable = [m for m in PEEKABOO_OPENAI if m in available]
    print(
        "\n→ Set WORKER_PEEKABOO_AGENT_MODEL to one of: "
        + (", ".join(usable) if usable else "(none — enable a gpt-5.x model for the project)")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
