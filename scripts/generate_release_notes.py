#!/usr/bin/env python3
"""Generate Jarvis runtime release notes from commits.

Commit messages are the source of truth. Conventional Commit type/scope gives
the broad category, while trailers carry user-facing detail:

  Release-note: Added wake-word barge-in tuning to the room intercom.
  Env: JARVIS_FOO_TIMEOUT added; set to seconds before enabling foo.
  Upgrade-note: No operator action required for existing installs.
  Docs: docs/PI.md explains the new room display controls.

If OPENAI_API_KEY and JARVIS_RELEASE_NOTES_MODEL are present, the script asks an
AI model to rewrite the gathered facts into polished notes. Otherwise it emits a
deterministic markdown summary from the same facts.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import textwrap
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path


CONVENTIONAL_RE = re.compile(
    r"^(?P<type>[a-z]+)(?:\((?P<scope>[^)]+)\))?(?P<breaking>!)?:\s+(?P<title>.+)$"
)
TRAILER_RE = re.compile(r"^(?P<key>[A-Za-z][A-Za-z0-9-]*(?: [A-Za-z0-9-]+)?):\s*(?P<value>.*)$")
ENV_KEY_RE = re.compile(r"^\s*(?:export\s+)?([A-Z][A-Z0-9_]+)\s*=")
RELEASABLE_TYPES = {
    "feat",
    "fix",
    "perf",
    "refactor",
    "docs",
    "build",
    "ci",
    "chore",
    "test",
    "style",
    "revert",
}
DEFAULT_BASE_URL = "https://api.openai.com/v1"
STRICT_TRAILER_TYPES = {"feat", "fix", "perf"}
QUALITY_SECTION_LIMIT = 8
RAW_SCOPE_BULLET_RE = re.compile(r"^- [a-z][a-z0-9_-]+: ")
SECTION_RE = re.compile(r"^## (?P<title>.+)$")


@dataclass
class CommitInfo:
    sha: str
    subject: str
    body: str
    type: str = "other"
    scope: str = ""
    title: str = ""
    breaking: bool = False
    trailers: dict[str, list[str]] = field(default_factory=dict)

    @property
    def release_notes(self) -> list[str]:
        return [
            value
            for value in self.trailers.get("release-note", [])
            if value.strip().lower() not in {"skip", "none", "n/a", "na"}
        ]

    @property
    def has_release_note_decision(self) -> bool:
        return "release-note" in self.trailers

    @property
    def env_notes(self) -> list[str]:
        notes: list[str] = []
        for key in ("env", "env-change", "env-note"):
            notes.extend(self.trailers.get(key, []))
        return notes

    @property
    def breaking_notes(self) -> list[str]:
        notes: list[str] = []
        for key in ("breaking change", "breaking-change"):
            notes.extend(self.trailers.get(key, []))
        return notes

    @property
    def upgrade_notes(self) -> list[str]:
        notes: list[str] = []
        for key in ("upgrade-note", "operator-action"):
            notes.extend(self.trailers.get(key, []))
        return notes

    @property
    def docs_notes(self) -> list[str]:
        notes: list[str] = []
        for key in ("docs", "documentation"):
            notes.extend(self.trailers.get(key, []))
        return notes


def run_git(args: list[str], *, check: bool = True) -> str:
    result = subprocess.run(
        ["git", *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise SystemExit(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout


def latest_semver_tag() -> str:
    tags = run_git(["tag", "--list", "v[0-9]*.[0-9]*.[0-9]*", "--sort=-v:refname"]).splitlines()
    return tags[0] if tags else ""


def parse_trailers(message: str) -> dict[str, list[str]]:
    trailers: dict[str, list[str]] = {}
    for line in message.splitlines()[1:]:
        match = TRAILER_RE.match(line.strip())
        if not match:
            continue
        key = match.group("key").strip().lower()
        trailers.setdefault(key, []).append(match.group("value").strip())
    return trailers


def parse_commit(sha: str, message: str) -> CommitInfo:
    subject = message.splitlines()[0].strip() if message.splitlines() else ""
    match = CONVENTIONAL_RE.match(subject)
    info = CommitInfo(sha=sha, subject=subject, body=message, trailers=parse_trailers(message))
    if match:
        info.type = match.group("type")
        info.scope = match.group("scope") or ""
        info.title = match.group("title").strip()
        info.breaking = bool(match.group("breaking"))
    if info.breaking_notes:
        info.breaking = True
    return info


def load_commits(base_tag: str, head_ref: str) -> list[CommitInfo]:
    commits: list[CommitInfo] = []
    commit_range = f"{base_tag}..{head_ref}" if base_tag else head_ref
    for sha in run_git(["log", "--no-merges", "--format=%H", commit_range]).splitlines():
        message = run_git(["log", "--format=%B", "-n", "1", sha])
        commit = parse_commit(sha, message)
        if commit.subject.startswith("chore(version): sync runtime metadata"):
            continue
        commits.append(commit)
    return commits


def env_keys_at(ref: str) -> set[str]:
    data = run_git(["show", f"{ref}:.env.example"], check=False)
    return {match.group(1) for line in data.splitlines() if (match := ENV_KEY_RE.match(line))}


def env_diff(base_tag: str, head_ref: str) -> dict[str, list[str]]:
    before = env_keys_at(base_tag) if base_tag else set()
    after = env_keys_at(head_ref)
    return {
        "added": sorted(after - before),
        "removed": sorted(before - after),
    }


def note_for(commit: CommitInfo) -> str:
    if commit.release_notes:
        return "; ".join(commit.release_notes)
    return ""


def grouped_notes(commits: list[CommitInfo]) -> dict[str, list[str]]:
    groups = {
        "features": [],
        "fixes": [],
        "performance": [],
        "other": [],
        "breaking": [],
        "env": [],
        "operator_action": [],
        "docs": [],
    }
    for commit in commits:
        if commit.type not in RELEASABLE_TYPES and not commit.release_notes:
            continue
        note = note_for(commit)
        if commit.breaking:
            detail = "; ".join(commit.breaking_notes) or note
            groups["breaking"].append(detail)
        if commit.env_notes:
            groups["env"].extend(commit.env_notes)
        if commit.upgrade_notes:
            groups["operator_action"].extend(commit.upgrade_notes)
        if commit.docs_notes:
            groups["docs"].extend(commit.docs_notes)
        if note:
            if commit.type == "feat":
                groups["features"].append(note)
            elif commit.type == "fix":
                groups["fixes"].append(note)
            elif commit.type == "perf":
                groups["performance"].append(note)
            elif commit.release_notes:
                groups["other"].append(note)
    return groups


def facts_payload(version: str, tag: str, base_tag: str, commits: list[CommitInfo], env_changes: dict[str, list[str]]) -> dict[str, object]:
    return {
        "version": version,
        "tag": tag,
        "base_tag": base_tag,
        "summary": grouped_notes(commits),
        "env_diff": env_changes,
        "commits": [
            {
                "sha": commit.sha[:12],
                "subject": commit.subject,
                "type": commit.type,
                "scope": commit.scope,
                "release_notes": commit.release_notes,
                "env_notes": commit.env_notes,
                "upgrade_notes": commit.upgrade_notes,
                "docs_notes": commit.docs_notes,
                "breaking": commit.breaking,
                "breaking_notes": commit.breaking_notes,
            }
            for commit in commits
        ],
    }


def bullet_lines(items: list[str]) -> list[str]:
    return [f"- {item}" for item in dict.fromkeys(item for item in items if item.strip())]


def deterministic_notes(payload: dict[str, object]) -> str:
    tag = str(payload["tag"])
    summary = payload["summary"]
    assert isinstance(summary, dict)
    env_changes = payload["env_diff"]
    assert isinstance(env_changes, dict)

    lines = [
        f"# Jarvis Runtime {tag}",
        "",
        "Local-first Jarvis runtime and service manager.",
        "",
    ]

    sections = [
        ("Breaking Changes", summary.get("breaking", [])),
        ("Changed", summary.get("features", [])),
        ("Fixed", summary.get("fixes", [])),
        ("Performance", summary.get("performance", [])),
        ("Other Changes", summary.get("other", [])),
    ]
    for title, values in sections:
        bullets = bullet_lines(list(values))
        if bullets:
            lines.extend([f"## {title}", "", *bullets, ""])

    action_lines = list(summary.get("operator_action", []))
    if not action_lines:
        if summary.get("breaking", []):
            action_lines.append("Review the breaking changes before upgrading.")
        elif summary.get("env", []) or env_changes.get("added", []) or env_changes.get("removed", []):
            action_lines.append("Review the environment notes before upgrading.")
        else:
            action_lines.append("No operator action required.")
    lines.extend(["## Operator Action", ""])
    lines.extend(bullet_lines(action_lines))
    lines.append("")

    env_lines: list[str] = []
    added = list(env_changes.get("added", []))
    removed = list(env_changes.get("removed", []))
    if added:
        env_lines.append("Env vars added to `.env.example`: " + ", ".join(f"`{key}`" for key in added) + ".")
    if removed:
        env_lines.append("Env vars removed from `.env.example`: " + ", ".join(f"`{key}`" for key in removed) + ".")
    env_lines.extend(list(summary.get("env", [])))
    lines.extend(["## Environment", ""])
    lines.extend(bullet_lines(env_lines) or ["- No env changes detected."])
    lines.append("")

    docs_lines = bullet_lines(list(summary.get("docs", [])))
    if docs_lines:
        lines.extend(["## Documentation", "", *docs_lines, ""])

    lines.extend(
        [
            "## Install",
            "",
            "```bash",
            "brew tap roughcoder/infinite-stack",
            "brew install jarvis",
            "```",
            "",
            "## Update",
            "",
            "```bash",
            "brew update",
            "brew upgrade jarvis",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def validate_release_trailers(commits: list[CommitInfo]) -> list[str]:
    errors: list[str] = []
    for commit in commits:
        if commit.type not in STRICT_TRAILER_TYPES:
            continue
        if commit.has_release_note_decision:
            continue
        errors.append(
            f"{commit.sha[:12]} {commit.subject!r} needs Release-note: <text> or Release-note: skip"
        )
    return errors


def validate_release_markdown(notes: str, *, section_limit: int = QUALITY_SECTION_LIMIT) -> list[str]:
    errors: list[str] = []
    current_section = ""
    bullet_counts: dict[str, int] = {}
    limited_sections = {"Changed", "Fixed", "Performance", "Other Changes"}
    required_sections = {"Operator Action", "Environment", "Install", "Update"}
    seen_sections: set[str] = set()

    for line in notes.splitlines():
        if match := SECTION_RE.match(line):
            current_section = match.group("title").strip()
            seen_sections.add(current_section)
            continue
        if not line.startswith("- "):
            continue
        if RAW_SCOPE_BULLET_RE.match(line):
            errors.append(f"raw commit-style bullet leaked into release notes: {line}")
        if current_section in limited_sections:
            bullet_counts[current_section] = bullet_counts.get(current_section, 0) + 1

    for section, count in sorted(bullet_counts.items()):
        if count > section_limit:
            errors.append(f"{section} has {count} bullets; group related changes down to {section_limit} or fewer")
    missing = sorted(required_sections - seen_sections)
    if missing:
        errors.append("missing required sections: " + ", ".join(missing))
    return errors


def ai_enabled(mode: str) -> bool:
    api_key = os.environ.get("OPENAI_API_KEY")
    model = os.environ.get("JARVIS_RELEASE_NOTES_MODEL")
    if mode == "never":
        return False
    if mode == "always" and (not api_key or not model):
        raise SystemExit("AI release notes require OPENAI_API_KEY and JARVIS_RELEASE_NOTES_MODEL.")
    return bool(api_key and model)


def ai_notes(payload: dict[str, object]) -> str:
    api_key = os.environ["OPENAI_API_KEY"]
    model = os.environ["JARVIS_RELEASE_NOTES_MODEL"]
    base_url = os.environ.get("OPENAI_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    fallback = deterministic_notes(payload)
    prompt = textwrap.dedent(
        """
        Write concise GitHub release notes for Jarvis Runtime from the JSON facts.
        Use only the facts provided; do not invent features, fixes, dates, or env vars.
        Required sections, in this order:
        - H1 title: Jarvis Runtime <tag>
        - Changed
        - Fixed
        - Operator Action
        - Environment
        - Documentation when documentation links are present
        - Install
        - Update
        Include Breaking Changes before Changed only when present.
        Group related commits by user-visible outcome. Prefer 3-6 bullets per
        section and never exceed 8 bullets in Changed, Fixed, Performance, or
        Other Changes.
        Never emit raw commit-subject bullets or scope prefixes such as
        "voice:", "comms:", "ops:", or "intercom:".
        Environment must explicitly say whether env vars are new, removed, need changing,
        or that no env changes were detected.
        Operator Action must say whether existing installs need action.
        Keep Homebrew install/update commands exactly as provided by the fallback notes.
        """
    ).strip()
    body = json.dumps(
        {
            "model": model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": prompt},
                {
                    "role": "user",
                    "content": json.dumps(
                        {"facts": payload, "fallback_notes": fallback},
                        indent=2,
                        sort_keys=True,
                    ),
                },
            ],
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url}/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    timeout = float(os.environ.get("JARVIS_RELEASE_NOTES_TIMEOUT", "45"))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
        content = data["choices"][0]["message"]["content"].strip()
        if not content:
            raise ValueError("empty AI release notes response")
        quality_errors = validate_release_markdown(content)
        if quality_errors:
            raise ValueError("; ".join(quality_errors))
        return content + "\n"
    except (urllib.error.URLError, KeyError, IndexError, ValueError, json.JSONDecodeError) as exc:
        if os.environ.get("JARVIS_RELEASE_NOTES_AI", "auto").lower() == "always":
            raise SystemExit(f"AI release notes failed: {exc}") from exc
        print(f"AI release notes unavailable, using deterministic fallback: {exc}", file=sys.stderr)
        return fallback


def build_notes(version: str, base_tag: str, head_ref: str, ai_mode: str, *, strict: bool = False) -> str:
    tag = f"v{version.lstrip('v')}"
    commits = load_commits(base_tag, head_ref)
    if strict:
        trailer_errors = validate_release_trailers(commits)
        if trailer_errors:
            raise SystemExit("Release note trailer check failed:\n- " + "\n- ".join(trailer_errors))
    payload = facts_payload(version.lstrip("v"), tag, base_tag, commits, env_diff(base_tag, head_ref))
    if ai_enabled(ai_mode):
        notes = ai_notes(payload)
    else:
        notes = deterministic_notes(payload)
    if strict:
        quality_errors = validate_release_markdown(notes)
        if quality_errors:
            raise SystemExit("Release note quality check failed:\n- " + "\n- ".join(quality_errors))
    return notes


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Jarvis runtime release notes.")
    parser.add_argument("--version", required=True, help="Release version, with or without v prefix.")
    parser.add_argument("--base-tag", default="", help="Previous vX.Y.Z tag. Defaults to latest SemVer tag.")
    parser.add_argument("--head", default="HEAD", help="Git ref to release. Defaults to HEAD.")
    parser.add_argument("--output", required=True, type=Path, help="Markdown output path.")
    parser.add_argument(
        "--ai",
        choices=("auto", "always", "never"),
        default=os.environ.get("JARVIS_RELEASE_NOTES_AI", "auto").lower(),
        help="Use AI when configured, require AI, or never use AI.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        default=os.environ.get("JARVIS_RELEASE_NOTES_STRICT", "").lower() in {"1", "true", "yes"},
        help="Fail when release-visible commits lack release-note trailers or generated notes are noisy.",
    )
    args = parser.parse_args()
    base_tag = args.base_tag or latest_semver_tag()
    notes = build_notes(args.version, base_tag, args.head, args.ai, strict=args.strict)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(notes, encoding="utf-8")
    print(f"Wrote release notes to {args.output} from {base_tag}..{args.head}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
