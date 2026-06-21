"""Personal facts — the authoritative memory rail (brain/profile + tools/profile).

Proves the managed "## What Jarvis knows" section round-trips, upserts, and never
disturbs the front-matter the identity resolver depends on; and that the tools are
self-scoped (a speaker only edits their OWN file, personal scope only).
"""

from __future__ import annotations

import asyncio

from jarvis.brain.context import RequestContext
from jarvis.brain.profile import forget_fact, parse_facts, read_facts, remember_fact
from jarvis.config import CapabilityConfig
from jarvis.tools.profile import make_profile_tools

_FILE = """---
whatsapp: ["447921815819"]
scope: personal
honcho_peer: neil
---

# Neil

Some hand-written notes about Neil.
"""


def test_remember_creates_section_preserving_frontmatter(tmp_path) -> None:  # noqa: ANN001
    p = tmp_path / "neil.md"
    p.write_text(_FILE)
    assert remember_fact(p, "email", "neil@eat.sleep.dev") == "saved"
    out = p.read_text()
    # front-matter + body preserved
    assert "honcho_peer: neil" in out and "hand-written notes" in out
    # managed section added
    assert "## What Jarvis knows" in out
    assert read_facts(p) == {"email": "neil@eat.sleep.dev"}


def test_remember_upserts_and_normalises_keys(tmp_path) -> None:  # noqa: ANN001
    p = tmp_path / "neil.md"
    p.write_text(_FILE)
    remember_fact(p, "Email", "old@x.com")
    assert remember_fact(p, "  EMAIL ", "new@x.com") == "updated"  # same key, normalised
    remember_fact(p, "address", "12 High St")
    facts = read_facts(p)
    assert facts == {"email": "new@x.com", "address": "12 High St"}
    # no duplicate email line left behind
    assert p.read_text().count("- email:") == 1


def test_forget_removes_only_that_fact(tmp_path) -> None:  # noqa: ANN001
    p = tmp_path / "neil.md"
    p.write_text(_FILE)
    remember_fact(p, "email", "neil@x.com")
    remember_fact(p, "birthday", "14 March")
    assert forget_fact(p, "email") is True
    assert read_facts(p) == {"birthday": "14 March"}
    assert forget_fact(p, "email") is False  # already gone


def test_parse_facts_ignores_other_sections(tmp_path) -> None:  # noqa: ANN001
    text = (
        "# Neil\n\n## What Jarvis knows\n<!-- managed -->\n- email: a@b.com\n\n"
        "## Other notes\n- not: a fact\n"
    )
    assert parse_facts(text) == {"email": "a@b.com"}  # stops at the next heading


def test_read_facts_missing_file(tmp_path) -> None:  # noqa: ANN001
    assert read_facts(tmp_path / "nope.md") == {}


# --- tools: self-scoping ----------------------------------------------------


def _ctx(identity="neil", scope="personal") -> RequestContext:  # noqa: ANN001
    return RequestContext("dev", identity, scope, frozenset({"profile.write"}))


def _tools(tmp_path):  # noqa: ANN001, ANN202
    cfg = CapabilityConfig(users_dir=str(tmp_path))
    return {t.name: t for t in make_profile_tools(cfg)}


def test_tool_remember_writes_own_file(tmp_path) -> None:  # noqa: ANN001
    (tmp_path / "neil.md").write_text(_FILE)
    tools = _tools(tmp_path)
    out = asyncio.run(tools["remember"].handler(_ctx(), {"key": "email", "value": "n@x.com"}))
    assert "saved" in out.lower()
    assert read_facts(tmp_path / "neil.md") == {"email": "n@x.com"}


def test_tool_is_self_scoped_by_identity(tmp_path) -> None:  # noqa: ANN001
    # Alice's turn writes alice.md, never neil.md — the file comes from ctx.identity.
    (tmp_path / "neil.md").write_text(_FILE)
    tools = _tools(tmp_path)
    asyncio.run(tools["remember"].handler(_ctx(identity="alice"), {"key": "pet", "value": "Rex"}))
    assert read_facts(tmp_path / "alice.md") == {"pet": "Rex"}
    assert read_facts(tmp_path / "neil.md") == {}  # Neil's file untouched


def test_tool_refuses_house_and_unknown(tmp_path) -> None:  # noqa: ANN001
    tools = _tools(tmp_path)
    for ctx in (_ctx(identity="house", scope="house"), _ctx(identity="", scope="house")):
        out = asyncio.run(tools["remember"].handler(ctx, {"key": "email", "value": "x@y.com"}))
        assert out.startswith("error:")
    # nothing written
    assert not list(tmp_path.glob("*.md"))


# --- seeding Honcho (the authoritative rail mirrors into the fuzzy one) ------


class _FakeMemory:
    def __init__(self, *, boom: bool = False) -> None:
        self.writes: list = []
        self.refreshed: list = []
        self._boom = boom

    async def write_turn(self, user_text, assistant_text, *, user=None) -> None:  # noqa: ANN001
        if self._boom:
            raise RuntimeError("honcho down")
        self.writes.append((user_text, assistant_text, user))

    async def refresh_cache(self, *a, user=None, **k) -> bool:  # noqa: ANN001, ANN002, ANN003
        self.refreshed.append(user)
        return True


async def _drain() -> None:
    """Let fire-and-forget seed tasks run to completion."""
    for _ in range(5):
        await asyncio.sleep(0)


def test_remember_seeds_honcho_for_the_speakers_peer(tmp_path) -> None:  # noqa: ANN001
    (tmp_path / "neil.md").write_text(_FILE)
    mem = _FakeMemory()
    cfg = CapabilityConfig(users_dir=str(tmp_path))

    async def go() -> None:
        tools = {t.name: t for t in make_profile_tools(cfg, memory=mem)}
        await tools["remember"].handler(_ctx(), {"key": "email", "value": "n@x.com"})
        await _drain()

    asyncio.run(go())
    assert mem.writes and "email" in mem.writes[0][0] and "n@x.com" in mem.writes[0][0]
    assert mem.writes[0][2] == "neil"  # scoped to the speaker's peer
    assert mem.refreshed == ["neil"]


def test_seed_failure_never_breaks_the_tool(tmp_path) -> None:  # noqa: ANN001
    (tmp_path / "neil.md").write_text(_FILE)
    mem = _FakeMemory(boom=True)
    cfg = CapabilityConfig(users_dir=str(tmp_path))

    async def go() -> str:
        tools = {t.name: t for t in make_profile_tools(cfg, memory=mem)}
        out = await tools["remember"].handler(_ctx(), {"key": "email", "value": "n@x.com"})
        await _drain()
        return out

    out = asyncio.run(go())
    assert "saved" in out.lower()  # fact still saved; Honcho failure swallowed
    assert read_facts(tmp_path / "neil.md") == {"email": "n@x.com"}
