"""Identity resolution — WHO is speaking, and how sure are we (Phase 3 §5).

The first layers of the resolution stack (§4): a request arrives stamped with a
device + channel (+ maybe an asserted identity); this resolves it to a *principal*
and a *scope*, gated by confidence. The rule is "know, or ask — never guess":

- **strong** — a personal device (your own Mac) or a bound WhatsApp number → that
  user, personal scope.
- **claimed** — a shared device where the speaker says "it's Jules" → that user,
  personal scope but family-grade confidence.
- **unknown** — a shared device, nobody confirmed → the **house** principal, house
  scope. No personal data. (The model is told to *ask* when a request actually
  needs personal scope — that instruction lives in the system prompt.)

Per-user config lives in `users/<name>.md` (front-matter: channel bindings +
credential *references*, never secrets — §10). House is the default principal.
This module imports nothing heavy and is pure logic, so it's fully unit-testable.
"""

from __future__ import annotations

import pathlib
import re
from dataclasses import dataclass, field

HOUSE = "house"

_FRONT_MATTER = re.compile(r"^\s*---\s*\n(.*?)\n---\s*(?:\n|$)", re.DOTALL)
_INLINE_LIST = re.compile(r"^\[(.*)\]$")
# "it's Jules" / "this is Neil" / "I'm Jules" / "speaking is Neil"
_CLAIM_RE = re.compile(
    r"\b(?:it'?s|this is|i'?m|i am|speaking|you'?re speaking (?:to|with))\s+([a-z][a-z'’.\- ]{0,30})",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class User:
    name: str
    devices: frozenset[str] = frozenset()
    whatsapp: frozenset[str] = frozenset()
    claims: tuple[str, ...] = ()  # extra claim phrases, lowercased
    capabilities: frozenset[str] = frozenset()
    calendar_accounts: tuple[str, ...] = ()
    email_accounts: tuple[str, ...] = ()
    household_visibility: str = ""
    scope: str = "personal"
    honcho_peer: str = ""

    @property
    def peer(self) -> str:
        return self.honcho_peer or self.name


@dataclass(frozen=True)
class Resolution:
    identity: str  # principal name ("house" when unknown)
    scope: str  # "personal" | "house"
    confidence: str  # "strong" | "claimed" | "unknown"
    user: User | None = field(default=None, compare=False)

    @property
    def known(self) -> bool:
        return self.confidence != "unknown"


def _parse_front_matter(text: str) -> dict[str, object]:
    """Minimal front-matter parser: `key: scalar`, `key: [a, b]`, and block lists
    (`key:` then `  - item`). No nesting — the users schema is deliberately flat."""
    m = _FRONT_MATTER.match(text)
    if not m:
        return {}
    out: dict[str, object] = {}
    lines = m.group(1).splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        i += 1
        if not line.strip() or line.lstrip().startswith("#") or ":" not in line:
            continue
        if line[0].isspace():  # stray indented line outside a block — skip
            continue
        key, _, rest = line.partition(":")
        key = key.strip()
        rest = rest.strip()
        if not rest:  # block list: consume following "  - item" lines
            items: list[str] = []
            while i < len(lines) and (lines[i].lstrip().startswith("-") or not lines[i].strip()):
                item = lines[i].lstrip()
                i += 1
                if item.startswith("-"):
                    items.append(item[1:].strip().strip("'\""))
            out[key] = items
        elif _INLINE_LIST.match(rest):
            inner = _INLINE_LIST.match(rest).group(1)
            out[key] = [x.strip().strip("'\"") for x in inner.split(",") if x.strip()]
        else:
            out[key] = rest.strip("'\"")
    return out


def _as_list(v: object) -> list[str]:
    if isinstance(v, list):
        return [str(x) for x in v]
    if v:
        return [str(v)]
    return []


def _norm_wa(num: str) -> str:
    """Normalise a WhatsApp address to digits only — drop any '@…' jid suffix, '+',
    spaces, dashes — so a stored number matches whatever format the connector reports
    (e.g. '447921815819', '+44 7921 815819', '447921815819@s.whatsapp.net')."""
    return re.sub(r"\D", "", (num or "").split("@", 1)[0])


def parse_user(name: str, text: str) -> User:
    fm = _parse_front_matter(text)
    return User(
        name=name,
        devices=frozenset(_as_list(fm.get("devices"))),
        whatsapp=frozenset(_as_list(fm.get("whatsapp"))),
        claims=tuple(c.lower() for c in _as_list(fm.get("claims"))),
        capabilities=frozenset(_as_list(fm.get("capabilities"))),
        calendar_accounts=tuple(_as_list(fm.get("calendar_accounts"))),
        email_accounts=tuple(_as_list(fm.get("email_accounts"))),
        household_visibility=str(fm.get("household_visibility") or ""),
        scope=str(fm.get("scope") or "personal"),
        honcho_peer=str(fm.get("honcho_peer") or ""),
    )


def load_users(users_dir: str) -> dict[str, User]:
    """Load every `users/<name>.md`. Missing dir => no users (house-only)."""
    path = pathlib.Path(users_dir)
    if not path.is_dir():
        return {}
    users: dict[str, User] = {}
    for f in sorted(path.glob("*.md")):
        users[f.stem] = parse_user(f.stem, f.read_text(encoding="utf-8"))
    return users


class IdentityResolver:
    """Resolves (device, channel, asserted, utterance) → Resolution. Built once
    from the loaded users; indexes device/whatsapp/claim bindings for fast lookup."""

    def __init__(self, users: dict[str, User]) -> None:
        self._users = users
        self._by_device: dict[str, User] = {}
        self._by_whatsapp: dict[str, User] = {}
        self._claim_index: dict[str, User] = {}
        for u in users.values():
            for d in u.devices:
                self._by_device[d] = u
            for w in u.whatsapp:
                self._by_whatsapp[_norm_wa(w)] = u
            self._claim_index[u.name.lower()] = u
            for c in u.claims:
                self._claim_index[c.lower()] = u

    def _user(self, name: str) -> User | None:
        return self._users.get(name) or self._claim_index.get((name or "").lower())

    def detect_claim(self, utterance: str) -> User | None:
        """Find a known user claimed in an utterance ("it's Jules" / "this is Neil").
        Matches the spoken name (or a configured claim phrase) to a known user."""
        if not utterance:
            return None
        text = utterance.lower()
        # Direct claim phrase match first (most specific).
        for phrase, user in self._claim_index.items():
            if len(phrase) > 2 and phrase in text:
                if re.search(rf"\b{re.escape(phrase)}\b", text):
                    return user
        m = _CLAIM_RE.search(utterance)
        if m:
            spoken = m.group(1).strip().lower().split()  # take the first word(s)
            for n in (spoken[0] if spoken else "", " ".join(spoken[:2])):
                u = self._claim_index.get(n)
                if u:
                    return u
        return None

    def resolve(
        self,
        *,
        device_id: str,
        channel: str = "voice",
        asserted: str = "",
        utterance: str = "",
        device_default: str = HOUSE,
    ) -> Resolution:
        """Resolve who's speaking. Order: bound personal device / WhatsApp number
        (strong) → a claim in the utterance or an asserted name (claimed) → the
        device's default, else house (unknown)."""
        # 1. WhatsApp: the connector asserts the sender's number (any format).
        if channel == "whatsapp" and asserted:
            u = self._by_whatsapp.get(_norm_wa(asserted))
            if u:
                return Resolution(u.name, u.scope, "strong", u)
        # 2. A personal device bound to exactly one user → strong.
        u = self._by_device.get(device_id)
        if u:
            return Resolution(u.name, u.scope, "strong", u)
        # 3. A claim in this utterance ("it's Jules") → claimed (family-grade).
        u = self.detect_claim(utterance)
        if u:
            return Resolution(u.name, u.scope, "claimed", u)
        # 4. An asserted name from the device (paired with a token) → claimed.
        if asserted:
            u = self._user(asserted)
            if u:
                return Resolution(u.name, u.scope, "claimed", u)
        # 5. Nobody confirmed → the device default (usually house) → unknown.
        u = self._user(device_default)
        if device_default != HOUSE and u:
            return Resolution(u.name, u.scope, "strong", u)
        return Resolution(HOUSE, HOUSE, "unknown", None)
