from __future__ import annotations

import json
import re
import shlex
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


Runner = Callable[..., subprocess.CompletedProcess[str]]
_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
_SEVERITIES = {"P1", "P2", "P3"}


@dataclass(frozen=True)
class GitHubReviewResult:
    review_id: int
    url: str
    comments: int
    skipped_comments: int = 0


def publish_github_pr_review(
    *,
    repo: str,
    pull_number: int,
    summary: str,
    comments: list[dict[str, Any]],
    commit_id: str,
    runner: Runner | None = None,
) -> GitHubReviewResult:
    """Preflight and publish one atomic review through the authenticated `gh` boundary."""

    if not _REPO_RE.fullmatch(repo):
        raise ValueError("repo must be in owner/name form")
    if pull_number < 1:
        raise ValueError("pull_number must be positive")
    if not summary.strip():
        raise ValueError("summary is required")
    if not commit_id.strip():
        raise ValueError("commit_id is required")

    run = runner or subprocess.run
    pull = _gh_json(run, ["gh", "api", f"repos/{repo}/pulls/{pull_number}"])
    head_sha = str(((pull.get("head") or {}).get("sha") if isinstance(pull, dict) else "") or "")
    if not head_sha or head_sha != commit_id.strip():
        raise ValueError("reviewed commit is not the pull request's current head")
    files = _gh_json(run, ["gh", "api", f"repos/{repo}/pulls/{pull_number}/files", "--paginate", "--slurp"])
    anchors = _review_anchors(files)
    existing = _gh_json(run, ["gh", "api", f"repos/{repo}/pulls/{pull_number}/comments", "--paginate", "--slurp"])
    existing_keys = _existing_comment_keys(existing)
    rendered_comments: list[dict[str, Any]] = []
    skipped: list[str] = []
    invalid_count = 0
    duplicate_count = 0
    global_diff_lines = (
        _global_diff_line_map(_gh_text(run, ["gh", "pr", "diff", str(pull_number), "--repo", repo]))
        if comments
        else {}
    )
    for comment in comments:
        try:
            candidate = _resolve_review_comment_line(
                comment,
                anchors=anchors,
                global_diff_lines=global_diff_lines,
            )
            rendered = _review_comment(candidate, anchors=anchors)
        except ValueError as exc:
            invalid_count += 1
            skipped.append(
                f"[{str(comment.get('severity') or 'P3').upper()}] "
                f"{str(comment.get('title') or 'Finding')}: {exc}"
            )
            continue
        key = (rendered["path"], rendered["line"], rendered["side"], rendered["body"])
        if key in existing_keys:
            duplicate_count += 1
            skipped.append(f"{rendered['body'].splitlines()[0]}: equivalent inline comment already exists")
            continue
        rendered_comments.append(rendered)
    if skipped:
        summary = summary.strip() + "\n\nFindings not published inline:\n" + "\n".join(f"- {item}" for item in skipped)
    # Skip only when every comment we were asked to post already exists. An
    # empty `comments` list is not that case: it is a summary-only review, and
    # dropping it published nothing while still reporting success.
    if comments and not rendered_comments and duplicate_count == len(comments) and invalid_count == 0:
        return GitHubReviewResult(review_id=0, url="", comments=0, skipped_comments=len(skipped))
    payload = {
        "event": "COMMENT",
        "body": summary.strip(),
        "commit_id": head_sha,
        "comments": rendered_comments,
    }
    result = run(
        ["gh", "api", "--method", "POST", f"repos/{repo}/pulls/{pull_number}/reviews", "--input", "-"],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "GitHub review publication failed")
    try:
        response = json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError("GitHub review publication returned invalid JSON") from exc
    return GitHubReviewResult(
        review_id=int(response.get("id") or 0),
        url=str(response.get("html_url") or ""),
        comments=len(rendered_comments),
        skipped_comments=len(skipped),
    )


def _gh_json(run: Runner, argv: list[str]) -> Any:
    result = run(argv, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "GitHub preflight failed")
    try:
        return json.loads(result.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError("GitHub preflight returned invalid JSON") from exc


def _gh_text(run: Runner, argv: list[str]) -> str:
    result = run(argv, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "GitHub diff preflight failed")
    return result.stdout


def _review_anchors(raw: Any) -> dict[str, dict[str, set[int]]]:
    pages = raw if isinstance(raw, list) else []
    files = [item for page in pages for item in page] if pages and all(isinstance(page, list) for page in pages) else pages
    anchors: dict[str, dict[str, set[int]]] = {}
    for item in files:
        if not isinstance(item, dict):
            continue
        path = str(item.get("filename") or "")
        patch = str(item.get("patch") or "")
        if path and patch:
            anchors[path] = _patch_anchors(patch)
    return anchors


def _existing_comment_keys(raw: Any) -> set[tuple[str, int, str, str]]:
    pages = raw if isinstance(raw, list) else []
    comments = [item for page in pages for item in page] if pages and all(isinstance(page, list) for page in pages) else pages
    return {
        (
            str(item.get("path") or ""),
            int(item.get("line") or item.get("original_line") or 0),
            str(item.get("side") or "RIGHT").upper(),
            str(item.get("body") or ""),
        )
        for item in comments
        if isinstance(item, dict) and (item.get("line") or item.get("original_line"))
    }


def _patch_anchors(patch: str) -> dict[str, set[int]]:
    result = {"LEFT": set(), "RIGHT": set()}
    old_line = new_line = 0
    for row in patch.splitlines():
        match = _HUNK_RE.match(row)
        if match:
            old_line, new_line = int(match.group(1)), int(match.group(3))
            continue
        if row.startswith("+") and not row.startswith("+++"):
            result["RIGHT"].add(new_line)
            new_line += 1
        elif row.startswith("-") and not row.startswith("---"):
            result["LEFT"].add(old_line)
            old_line += 1
        elif old_line and new_line:
            result["LEFT"].add(old_line)
            result["RIGHT"].add(new_line)
            old_line += 1
            new_line += 1
    return result


def _global_diff_line_map(diff: str) -> dict[tuple[str, int, str], int]:
    result: dict[tuple[str, int, str], int] = {}
    old_path = new_path = ""
    old_line = new_line = 0
    for position, row in enumerate(diff.splitlines(), start=1):
        if row.startswith("diff --git "):
            old_path = new_path = ""
            old_line = new_line = 0
            continue
        if row.startswith("--- "):
            old_path = _diff_path(row[4:], "a/")
            continue
        if row.startswith("+++ "):
            new_path = _diff_path(row[4:], "b/")
            continue
        match = _HUNK_RE.match(row)
        if match:
            old_line, new_line = int(match.group(1)), int(match.group(3))
            continue
        if not old_line and not new_line:
            continue
        if row.startswith("\\"):
            continue
        if row.startswith("+") and not row.startswith("+++"):
            if new_path:
                result[(new_path, position, "RIGHT")] = new_line
            new_line += 1
        elif row.startswith("-") and not row.startswith("---"):
            if old_path:
                result[(old_path, position, "LEFT")] = old_line
            old_line += 1
        else:
            if old_path:
                result[(old_path, position, "LEFT")] = old_line
            if new_path:
                result[(new_path, position, "RIGHT")] = new_line
            old_line += 1
            new_line += 1
    return result


def _diff_path(value: str, prefix: str) -> str:
    if value == "/dev/null":
        return ""
    try:
        parsed = shlex.split(value)[0]
    except (IndexError, ValueError):
        parsed = value.strip()
    return parsed.removeprefix(prefix)


def _normalize_global_diff_line(
    comment: dict[str, Any],
    global_diff_lines: dict[tuple[str, int, str], int],
) -> dict[str, Any] | None:
    path = str(comment.get("path") or "").strip()
    side = str(comment.get("side") or "RIGHT").upper()
    try:
        position = int(comment.get("line") or 0)
    except (TypeError, ValueError):
        return None
    line = global_diff_lines.get((path, position, side))
    return {**comment, "line": line} if line is not None else None


def _resolve_review_comment_line(
    comment: dict[str, Any],
    *,
    anchors: dict[str, dict[str, set[int]]],
    global_diff_lines: dict[tuple[str, int, str], int],
) -> dict[str, Any]:
    line_kind = str(comment.get("line_kind") or "").upper()
    if line_kind == "FILE":
        return comment
    normalized = _normalize_global_diff_line(comment, global_diff_lines)
    if line_kind == "GLOBAL_DIFF_POSITION":
        if normalized is None:
            raise ValueError("global gh diff output position does not map to the declared path and side")
        return normalized
    if line_kind:
        raise ValueError("comment line_kind must be FILE or GLOBAL_DIFF_POSITION")
    if normalized is None or normalized.get("line") == comment.get("line"):
        return comment
    if _comment_anchor_exists(comment, anchors):
        raise ValueError(
            "comment line is ambiguous between a valid file line and a different gh diff output position"
        )
    return normalized


def _comment_anchor_exists(
    comment: dict[str, Any],
    anchors: dict[str, dict[str, set[int]]],
) -> bool:
    path = str(comment.get("path") or "").strip()
    side = str(comment.get("side") or "RIGHT").upper()
    try:
        line = int(comment.get("line") or 0)
    except (TypeError, ValueError):
        return False
    return side in {"LEFT", "RIGHT"} and path in anchors and line in anchors[path][side]


def _review_comment(comment: dict[str, Any], *, anchors: dict[str, dict[str, set[int]]]) -> dict[str, Any]:
    path = str(comment.get("path") or "").strip()
    if not path or path.startswith("/") or ".." in path.split("/"):
        raise ValueError("comment path must be a repository-relative path")
    try:
        line = int(comment.get("line") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("comment line must be a positive integer") from exc
    if line < 1:
        raise ValueError("comment line must be a positive integer")
    side = str(comment.get("side") or "RIGHT").upper()
    if side not in {"LEFT", "RIGHT"}:
        raise ValueError("comment side must be LEFT or RIGHT")
    if path not in anchors or line not in anchors[path][side]:
        raise ValueError(f"comment anchor {path}:{line}:{side} is not in the current pull request diff")
    severity = str(comment.get("severity") or "").upper()
    if severity not in _SEVERITIES:
        raise ValueError("comment severity must be P1, P2, or P3")
    title = " ".join(str(comment.get("title") or "").split())
    body = str(comment.get("body") or "").strip()
    if not title or not body:
        raise ValueError("comment title and body are required")

    rendered = f"[{severity}] {title}\n\n{body}"
    suggestion = str(comment.get("suggestion") or "")
    if suggestion:
        if side != "RIGHT":
            raise ValueError("suggestions can only target RIGHT-side lines")
        rendered += f"\n\n```suggestion\n{suggestion.rstrip()}\n```"
    return {"path": path, "line": line, "side": side, "body": rendered}
