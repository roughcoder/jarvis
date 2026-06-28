"""Skills — composed, mostly self-authored recipes (Phase 3 §7).

Tools stay few and atomic; **skills are many, composed, and often written by Jarvis
itself**. A skill is a markdown recipe (name / when-to-use / recipe / allowed tools
/ params), stored as `SKILLS.md` index + `skills/*.md` bodies. The model selects a
skill the same cheap way it selects a tool (by description), so latency is untouched.

Each skill is surfaced as a gated `Tool`: it's offered only when the context grants
**every** tool the skill composes (`extra_capabilities`), so a skill can never
exceed its profile's powers (§7 safety invariant) — true by construction. Running a
skill is a small, bounded tool loop over just its allowed tools, each call still
going through the gated registry. Self-authoring (`save_skill`) writes a new recipe
off the hot path and registers it live.
"""

from __future__ import annotations

import json
import pathlib
import re
from dataclasses import dataclass

from jarvis.brain.context import RequestContext
from jarvis.brain.dialog import _now_line
from jarvis.brain.gateway_client import LLMAttribution
from jarvis.brain.identity import _parse_front_matter
from jarvis.config import Config
from jarvis.tools.base import Tool, ToolRegistry

_NAME_OK = re.compile(r"[^a-zA-Z0-9_-]")
_SKILL_FORMAT = (
    "Follow the recipe to fulfil the user's request. Use the provided tools as the "
    "recipe directs. Reply with the final result only — concise and spoken-friendly."
)


def _sanitize(name: str) -> str:
    return _NAME_OK.sub("_", name.strip().lower().replace(" ", "_"))[:64]


@dataclass(frozen=True)
class Skill:
    name: str
    description: str  # when-to-use (drives selection)
    recipe: str  # the instructions body
    allowed_tools: tuple[str, ...] = ()
    params: dict | None = None  # JSON-Schema properties; default {request: string}
    required: tuple[str, ...] = ("request",)

    def parameters(self) -> dict:
        props = self.params or {"request": {"type": "string", "description": "The user's request."}}
        return {"type": "object", "properties": props, "required": list(self.required)}


def parse_skill(name: str, text: str) -> Skill:
    fm = _parse_front_matter(text)
    body = re.sub(r"^\s*---\s*\n.*?\n---\s*\n?", "", text, count=1, flags=re.DOTALL).strip()

    def _list(key: str) -> list[str]:
        v = fm.get(key)
        return [str(x) for x in v] if isinstance(v, list) else ([str(v)] if v else [])

    return Skill(
        name=_sanitize(str(fm.get("name") or name)),
        description=str(fm.get("when_to_use") or fm.get("description") or ""),
        recipe=body,
        allowed_tools=tuple(_list("allowed_tools") or _list("tools")),
        required=tuple(_list("required") or ["request"]),
    )


def load_skills(skills_dir: str) -> dict[str, Skill]:
    path = pathlib.Path(skills_dir)
    if not path.is_dir():
        return {}
    out: dict[str, Skill] = {}
    for f in sorted(path.glob("*.md")):
        if f.name.upper() == "SKILLS.MD":
            continue
        skill = parse_skill(f.stem, f.read_text(encoding="utf-8"))
        out[skill.name] = skill
    return out


async def _run_skill(
    skill: Skill, ctx: RequestContext, args: dict, *, gateway, registry: ToolRegistry, cfg: Config
) -> str:
    """Execute a skill: a bounded tool loop over ONLY its allowed tools, each call
    gated by the registry (defense in depth). Returns the final text."""
    tools = [registry.get(n) for n in skill.allowed_tools]
    schemas = [t.openai_schema() for t in tools if t is not None]
    request = args.get("request") or json.dumps(args)
    # Skills run in their own sub-loop, so they don't inherit the turn's system
    # prompt — give them the current date/time too, or "today"/"tomorrow" in a
    # recipe (e.g. train times) is ambiguous.
    now = _now_line(cfg.persona.timezone)
    # Skills are real multi-step tool work (browsing, reading, deciding) — run them on
    # the strong model so they follow the recipe and reason over results reliably; the
    # fast model fumbles the steps and ignores recipe specifics (e.g. which site to use).
    model = cfg.gateway.strong_model
    messages = [
        {"role": "system", "content": f"{skill.recipe}\n\n{_SKILL_FORMAT}\n\n{now}"},
        {"role": "user", "content": request},
    ]
    for _ in range(max(1, cfg.tools.max_rounds)):
        msg = await gateway.complete_with_tools(
            messages, model=model, tools=schemas or None
        )
        if not msg.tool_calls:
            return msg.content or ""
        messages.append(
            {
                "role": "assistant",
                "content": msg.content or "",
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                    }
                    for tc in msg.tool_calls
                ],
            }
        )
        for tc in msg.tool_calls:
            try:
                call_args = json.loads(tc.function.arguments or "{}")
                result = await registry.execute(
                    ctx, tc.function.name, call_args, timeout_s=cfg.tools.timeout_s
                )
            except Exception as exc:  # noqa: BLE001 - a tool error must not break the skill
                result = f"error: {exc}"
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})
    final = await gateway.complete_with_tools(messages, model=model, tools=None)
    return final.content or ""


def _skill_tool(skill: Skill, *, gateway, registry: ToolRegistry, cfg: Config) -> Tool:
    # Offered only when every composed tool's capability is granted (§7 invariant).
    extra = frozenset(
        t.required_capability for t in (registry.get(n) for n in skill.allowed_tools) if t is not None
    )

    async def handler(ctx: RequestContext, args: dict) -> str:
        g = gateway
        if hasattr(gateway, "with_attribution"):
            g = gateway.with_attribution(
                LLMAttribution(
                    kind="skill",
                    channel=ctx.channel,
                    speaker=ctx.identity,
                    device_id=ctx.device_id,
                )
            )
        return await _run_skill(skill, ctx, args, gateway=g, registry=registry, cfg=cfg)

    return Tool(
        skill.name,
        f"[skill] {skill.description}" if skill.description else f"[skill] {skill.name}",
        skill.parameters(),
        "skills.run",
        handler,
        announce=True,
        extra_capabilities=extra,
    )


def make_skill_tools(skills: dict[str, Skill], *, gateway, registry: ToolRegistry, cfg: Config) -> list[Tool]:
    return [_skill_tool(s, gateway=gateway, registry=registry, cfg=cfg) for s in skills.values()]


def register_skills(registry: ToolRegistry, *, gateway, cfg: Config) -> int:
    """Load skills, register each as a gated tool, and register `save_skill` (which
    registers new skills live). Call AFTER MCP tools so skills can compose them.
    Returns the number of skills loaded."""
    skills_dir = cfg.persona.skills_dir
    skills = load_skills(skills_dir)
    for tool in make_skill_tools(skills, gateway=gateway, registry=registry, cfg=cfg):
        registry.register(tool)

    def on_saved(skill: Skill) -> None:
        registry.register(_skill_tool(skill, gateway=gateway, registry=registry, cfg=cfg))

    registry.register(make_save_skill_tool(skills_dir, on_saved=on_saved))
    if skills:
        print(f"  [skills] {len(skills)} loaded: {', '.join(skills)}")
    return len(skills)


# --- self-authoring --------------------------------------------------------


def write_skill(skills_dir: str, skill: Skill) -> pathlib.Path:
    """Persist a skill to `skills/<name>.md` and keep `SKILLS.md` index in step."""
    d = pathlib.Path(skills_dir)
    d.mkdir(parents=True, exist_ok=True)
    tools_line = ", ".join(skill.allowed_tools)
    body = (
        f"---\nname: {skill.name}\nwhen_to_use: {skill.description}\n"
        f"allowed_tools: [{tools_line}]\n---\n\n{skill.recipe.strip()}\n"
    )
    path = d / f"{skill.name}.md"
    path.write_text(body, encoding="utf-8")
    _reindex(d)
    return path


def _reindex(d: pathlib.Path) -> None:
    lines = ["# Skills\n"]
    for f in sorted(d.glob("*.md")):
        if f.name.upper() == "SKILLS.MD":
            continue
        s = parse_skill(f.stem, f.read_text(encoding="utf-8"))
        lines.append(f"- [{s.name}]({f.name}) — {s.description}")
    (d / "SKILLS.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_save_skill_tool(skills_dir: str, *, on_saved) -> Tool:
    """A gated tool (skills.author) that lets Jarvis save a new skill from the
    conversation ("save that as a skill"). Off the hot path — it writes a file and
    registers the skill live via `on_saved(skill)`."""

    async def handler(ctx: RequestContext, args: dict) -> str:
        name = _sanitize(str(args.get("name") or ""))
        if not name:
            return "error: a skill needs a name"
        skill = Skill(
            name=name,
            description=str(args.get("when_to_use") or ""),
            recipe=str(args.get("recipe") or ""),
            allowed_tools=tuple(args.get("allowed_tools") or ()),
        )
        write_skill(skills_dir, skill)
        on_saved(skill)
        return f"Saved the skill {name!r}. You can use it from now on."

    return Tool(
        "save_skill",
        "Save a reusable skill (a named recipe composing existing tools) when the "
        "user says something like 'save that as a skill'.",
        {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "when_to_use": {"type": "string", "description": "When this skill applies."},
                "recipe": {"type": "string", "description": "Step-by-step instructions."},
                "allowed_tools": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["name", "recipe"],
        },
        "skills.author",
        handler,
    )
