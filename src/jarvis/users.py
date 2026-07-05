"""User profile file store.

All code that reads or updates `users/*.md` goes through this module. The files
hold public metadata in front matter plus a Jarvis-managed facts section; secrets
stay outside the repo and outside this store.
"""

from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass

from jarvis.frontmatter import parse_front_matter

HOUSE = "house"

_FACT_HEADING = "## What Jarvis knows"
_FACT_MARKER = "<!-- managed by Jarvis: facts you've asked me to remember -->"
_FACT_SECTION_RE = re.compile(
    r"(?ms)^## What Jarvis knows[ \t]*\n(.*?)(?=^\#\# |\Z)",
)
_FACT_RE = re.compile(r"^- ([^:]+):[ \t]*(.*)$")
_NAME_OK = re.compile(r"[^a-z0-9_-]")


@dataclass(frozen=True)
class User:
    name: str
    devices: frozenset[str] = frozenset()
    whatsapp: frozenset[str] = frozenset()
    claims: tuple[str, ...] = ()
    capabilities: frozenset[str] = frozenset()
    calendar_accounts: tuple[str, ...] = ()
    email_accounts: tuple[str, ...] = ()
    household_visibility: str = ""
    scope: str = "personal"
    honcho_peer: str = ""
    trust_tier: str = ""
    guardians: tuple[str, ...] = ()

    @property
    def peer(self) -> str:
        return self.honcho_peer or self.name


def as_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(x) for x in value]
    if value:
        return [str(value)]
    return []


def parse_user(name: str, text: str) -> User:
    fm = parse_front_matter(text)
    return User(
        name=name,
        devices=frozenset(as_list(fm.get("devices"))),
        whatsapp=frozenset(as_list(fm.get("whatsapp"))),
        claims=tuple(c.lower() for c in as_list(fm.get("claims"))),
        capabilities=frozenset(as_list(fm.get("capabilities"))),
        calendar_accounts=tuple(as_list(fm.get("calendar_accounts"))),
        email_accounts=tuple(as_list(fm.get("email_accounts"))),
        household_visibility=str(fm.get("household_visibility") or ""),
        scope=str(fm.get("scope") or "personal"),
        honcho_peer=str(fm.get("honcho_peer") or ""),
        trust_tier=str(fm.get("trust_tier") or ""),
        guardians=tuple(as_list(fm.get("guardians"))),
    )


def load_users(users_dir: str) -> dict[str, User]:
    """Load every `users/<name>.md`. Missing dir means house-only."""
    path = pathlib.Path(users_dir)
    if not path.is_dir():
        return {}
    users: dict[str, User] = {}
    for f in sorted(path.glob("*.md")):
        users[f.stem] = parse_user(f.stem, f.read_text(encoding="utf-8"))
    return users


def normalize_whatsapp(value: str) -> str:
    """Phone/JID to digits only."""
    return re.sub(r"\D", "", (value or "").split("@", 1)[0])


def user_whatsapp_numbers(users_dir: str) -> set[str]:
    """Digits of every WhatsApp number across `users/*.md`."""
    return {
        normalize_whatsapp(number)
        for user in load_users(users_dir).values()
        for number in user.whatsapp
        if normalize_whatsapp(number)
    }


def slug_user_name(name: str) -> str:
    value = _NAME_OK.sub("_", (name or "").strip().lower().replace(" ", "_")).strip("_")
    return value[:48] or "user"


def add_whatsapp_number(users_dir: str, name: str, number: str) -> str:
    """Add a WhatsApp number to a user profile, preserving other file content."""
    directory = pathlib.Path(users_dir)
    directory.mkdir(parents=True, exist_ok=True)
    slug = slug_user_name(name)
    path = directory / f"{slug}.md"
    normalized = normalize_whatsapp(number) or number.strip()
    if not path.exists():
        path.write_text(
            f'---\n# {name} — paired via WhatsApp\nwhatsapp: ["{normalized}"]\n'
            f"scope: personal\nhoncho_peer: {slug}\n"
            f"capabilities: [profile.write]\n---\n\n# {name}\n",
            encoding="utf-8",
        )
        return "created"
    text = path.read_text(encoding="utf-8", errors="replace")
    m = re.search(r"^(whatsapp:\s*)\[(.*)\]\s*$", text, re.MULTILINE)
    if m:
        existing = [e.strip().strip("'\"") for e in m.group(2).split(",") if e.strip()]
        if any(normalize_whatsapp(e) == normalize_whatsapp(number) for e in existing):
            return "exists"
        existing.append(normalized)
        new_line = m.group(1) + "[" + ", ".join(f'"{e}"' for e in existing) + "]"
        path.write_text(text[: m.start()] + new_line + text[m.end():], encoding="utf-8")
        return "merged"
    path.write_text(re.sub(r"(?m)^---\s*$", f'---\nwhatsapp: ["{normalized}"]', text, count=1), encoding="utf-8")
    return "merged"


def _norm_key(key: str) -> str:
    return re.sub(r"\s+", " ", (key or "").strip().lower()).strip(" :-")


def parse_facts(text: str) -> dict[str, str]:
    """Return the managed facts as an ordered mapping."""
    m = _FACT_SECTION_RE.search(text or "")
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


def format_facts(facts: dict[str, str]) -> str:
    return "\n".join(f"- {key}: {value}" for key, value in facts.items())


def _render_facts(facts: dict[str, str]) -> str:
    body = "\n".join(f"- {key}: {value}" for key, value in facts.items())
    return f"{_FACT_HEADING}\n{_FACT_MARKER}\n{body}\n"


def _write_facts_section(text: str, facts: dict[str, str]) -> str:
    section = _render_facts(facts)
    if _FACT_SECTION_RE.search(text):
        return _FACT_SECTION_RE.sub(lambda _m: section, text, count=1)
    sep = "" if text.endswith("\n\n") else ("\n" if text.endswith("\n") else "\n\n")
    return f"{text}{sep}\n{section}"


def read_facts(path: str | pathlib.Path) -> dict[str, str]:
    user_path = pathlib.Path(path)
    if not user_path.is_file():
        return {}
    return parse_facts(user_path.read_text(encoding="utf-8", errors="replace"))


def remember_fact(path: str | pathlib.Path, key: str, value: str) -> str:
    user_path = pathlib.Path(path)
    normalized_key = _norm_key(key)
    normalized_value = (value or "").strip()
    if not normalized_key or not normalized_value:
        raise ValueError("both a key and a value are required")
    text = (
        user_path.read_text(encoding="utf-8", errors="replace")
        if user_path.is_file()
        else f"# {user_path.stem}\n"
    )
    facts = parse_facts(text)
    status = "updated" if normalized_key in facts else "saved"
    facts[normalized_key] = normalized_value
    user_path.parent.mkdir(parents=True, exist_ok=True)
    user_path.write_text(_write_facts_section(text, facts), encoding="utf-8")
    return status


def forget_fact(path: str | pathlib.Path, key: str) -> bool:
    user_path = pathlib.Path(path)
    if not user_path.is_file():
        return False
    text = user_path.read_text(encoding="utf-8", errors="replace")
    facts = parse_facts(text)
    normalized_key = _norm_key(key)
    if normalized_key not in facts:
        return False
    del facts[normalized_key]
    user_path.write_text(_write_facts_section(text, facts), encoding="utf-8")
    return True
