"""Personal facts — the explicit, authoritative memory rail (distinct from Honcho).

When the user tells Jarvis a durable structured fact ("my email is …", "remember my
address is …"), it's written verbatim to a Jarvis-managed section of the speaker's own
`users/<name>.md`, separate from the front-matter the identity resolver parses:

    ## What Jarvis knows
    <!-- managed by Jarvis: facts you've asked me to remember -->
    - email: neil@eat.sleep.dev
    - address: 12 High St, Bookham

This is the AUTHORITATIVE rail (verbatim, editable, injected deterministically). Honcho
remains the fuzzy/conversational rail — see brain/memory_client. These functions are
pure file ops (no network), unit-tested, and only ever touch the body section; the
front-matter (identity/caps/scope) is preserved untouched. Edits are picked up live by
the brain's users/ hot-reload (server.py `_maybe_reload_users`).
"""

from __future__ import annotations

import pathlib
import re

_HEADING = "## What Jarvis knows"
_MARKER = "<!-- managed by Jarvis: facts you've asked me to remember -->"
# The managed block: the heading through to the next "## " heading (or end of file).
_SECTION_RE = re.compile(
    r"(?ms)^## What Jarvis knows[ \t]*\n(.*?)(?=^\#\# |\Z)",
)
_FACT_RE = re.compile(r"^- ([^:]+):[ \t]*(.*)$")


def _norm_key(key: str) -> str:
    """Normalise a fact key: lowercased, single-spaced, no leading/trailing junk."""
    return re.sub(r"\s+", " ", (key or "").strip().lower()).strip(" :-")


def parse_facts(text: str) -> dict[str, str]:
    """Return the managed facts as an ordered {key: value} dict (empty if no section)."""
    m = _SECTION_RE.search(text or "")
    if not m:
        return {}
    facts: dict[str, str] = {}
    for line in m.group(1).splitlines():
        fm = _FACT_RE.match(line.strip())
        if fm:
            key = _norm_key(fm.group(1))
            if key:
                facts[key] = fm.group(2).strip()
    return facts


def _render(facts: dict[str, str]) -> str:
    body = "\n".join(f"- {k}: {v}" for k, v in facts.items())
    return f"{_HEADING}\n{_MARKER}\n{body}\n"


def _write_section(text: str, facts: dict[str, str]) -> str:
    """Replace (or append) the managed section in `text` with `facts`."""
    section = _render(facts)
    if _SECTION_RE.search(text):
        return _SECTION_RE.sub(lambda _m: section, text, count=1)
    # No section yet — append, keeping a blank line of separation.
    sep = "" if text.endswith("\n\n") else ("\n" if text.endswith("\n") else "\n\n")
    return f"{text}{sep}\n{section}"


def read_facts(path: str | pathlib.Path) -> dict[str, str]:
    p = pathlib.Path(path)
    if not p.is_file():
        return {}
    return parse_facts(p.read_text(encoding="utf-8", errors="replace"))


def format_facts(facts: dict[str, str]) -> str:
    """One-line-per-fact rendering for prompt injection (empty string if none)."""
    return "\n".join(f"- {k}: {v}" for k, v in facts.items())


def remember_fact(path: str | pathlib.Path, key: str, value: str) -> str:
    """Upsert a fact into the file's managed section. Creates the file/section if needed,
    preserves the front-matter and all other content. Returns 'saved' | 'updated'."""
    p = pathlib.Path(path)
    k = _norm_key(key)
    v = (value or "").strip()
    if not k or not v:
        raise ValueError("both a key and a value are required")
    text = p.read_text(encoding="utf-8", errors="replace") if p.is_file() else f"# {p.stem}\n"
    facts = parse_facts(text)
    status = "updated" if k in facts else "saved"
    facts[k] = v
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(_write_section(text, facts), encoding="utf-8")
    return status


def forget_fact(path: str | pathlib.Path, key: str) -> bool:
    """Remove a fact by key. Returns True if it was present (and removed)."""
    p = pathlib.Path(path)
    if not p.is_file():
        return False
    text = p.read_text(encoding="utf-8", errors="replace")
    facts = parse_facts(text)
    k = _norm_key(key)
    if k not in facts:
        return False
    del facts[k]
    p.write_text(_write_section(text, facts), encoding="utf-8")
    return True
