"""Profile tools — let the user curate the authoritative facts Jarvis keeps about them.

`remember` / `forget` write to the SPEAKER'S OWN `users/<identity>.md` (the managed
"## What Jarvis knows" section, see brain/profile). Self-scoped by construction: the
file is chosen from `ctx.identity`, never an argument, and only personal-scope speakers
may write — so one user can never edit another's facts (the privacy wall, §5). Gated
`profile.write`. These are instant local file ops — no network.
"""

from __future__ import annotations

import pathlib

from jarvis.brain.context import RequestContext
from jarvis.brain.profile import forget_fact, read_facts, remember_fact
from jarvis.config import CapabilityConfig
from jarvis.tools.base import Tool

_CAP = "profile.write"


def make_profile_tools(capabilities: CapabilityConfig) -> list[Tool]:
    users_dir = capabilities.users_dir

    def _own_file(ctx: RequestContext) -> pathlib.Path | None:
        """The speaker's own user file — only for a known, personal-scope principal."""
        if ctx.scope != "personal" or not ctx.identity or ctx.identity == "house":
            return None
        return pathlib.Path(users_dir) / f"{ctx.identity}.md"

    async def remember(ctx: RequestContext, args: dict) -> str:
        path = _own_file(ctx)
        if path is None:
            return ("error: I can only save personal facts once I know who I'm talking to. "
                    "Ask them to confirm who they are first.")
        key = (args.get("key") or "").strip()
        value = (args.get("value") or "").strip()
        if not key or not value:
            return "error: I need both what to remember (a label) and the value."
        try:
            status = remember_fact(path, key, value)
        except ValueError as exc:
            return f"error: {exc}"
        verb = "Saved" if status == "saved" else "Updated"
        return f"{verb} — {key.strip().lower()}: {value}."

    async def forget(ctx: RequestContext, args: dict) -> str:
        path = _own_file(ctx)
        if path is None:
            return "error: I don't have a personal profile to edit for this speaker."
        key = (args.get("key") or "").strip()
        if not key:
            return "error: tell me which fact to forget."
        return (f"Forgotten — {key.lower()}." if forget_fact(path, key)
                else f"I don't have anything saved under {key.lower()!r}.")

    async def list_facts(ctx: RequestContext, args: dict) -> str:
        path = _own_file(ctx)
        if path is None:
            return "error: I don't have a personal profile for this speaker."
        facts = read_facts(path)
        if not facts:
            return "I haven't saved any facts about you yet."
        return "; ".join(f"{k}: {v}" for k, v in facts.items()) + "."

    return [
        Tool(
            name="remember",
            description=(
                "Save a durable personal fact about the current user to their profile so "
                "you reliably know it in future conversations. Use for stable structured "
                "facts they state or ask you to remember — email, postal address, phone "
                "number, birthday, names of family/pets, preferences, important IDs. Do "
                "NOT use for fleeting/conversational remarks. Overwrites an existing fact "
                "with the same label. Confirm briefly after saving."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "Short stable label, e.g. 'email', 'address', 'birthday'.",
                    },
                    "value": {"type": "string", "description": "The fact, verbatim."},
                },
                "required": ["key", "value"],
            },
            required_capability=_CAP,
            handler=remember,
        ),
        Tool(
            name="forget",
            description="Delete a previously saved personal fact by its label.",
            parameters={
                "type": "object",
                "properties": {
                    "key": {"type": "string", "description": "The label of the fact to remove."},
                },
                "required": ["key"],
            },
            required_capability=_CAP,
            handler=forget,
        ),
        Tool(
            name="list_facts",
            description="List the personal facts you currently have saved about the user.",
            parameters={"type": "object", "properties": {}},
            required_capability=_CAP,
            handler=list_facts,
        ),
    ]
