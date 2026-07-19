from __future__ import annotations

import json
import subprocess

import pytest

from jarvis.github_reviews import publish_github_pr_review


def test_publish_github_pr_review_preflights_head_and_formats_inline_suggestion() -> None:
    calls: list[tuple[list[str], str]] = []

    def run(argv, **kwargs):  # noqa: ANN001
        calls.append((list(argv), str(kwargs.get("input") or "")))
        if argv[-1] == "repos/acme/widget/pulls/7":
            return subprocess.CompletedProcess(argv, 0, json.dumps({"head": {"sha": "abc123"}}), "")
        if "--paginate" in argv:
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps([[{"filename": "src/app.py", "patch": "@@ -4,2 +4,3 @@\n old\n+new\n tail"}]]),
                "",
            )
        return subprocess.CompletedProcess(argv, 0, json.dumps({"id": 99, "html_url": "https://github.com/acme/widget/pull/7#review-99"}), "")

    result = publish_github_pr_review(
        repo="acme/widget",
        pull_number=7,
        commit_id="abc123",
        summary="Two independent reviewers agreed on this finding.",
        comments=[
            {
                "path": "src/app.py",
                "line": 5,
                "side": "RIGHT",
                "severity": "P1",
                "title": "Preserve the state transition",
                "body": "The replacement skips the required transition.",
                "suggestion": "fixed()",
            }
        ],
        runner=run,
    )

    payload = json.loads(calls[-1][1])
    assert result.review_id == 99
    assert payload["commit_id"] == "abc123"
    assert payload["comments"] == [
        {
            "path": "src/app.py",
            "line": 5,
            "side": "RIGHT",
            "body": "[P1] Preserve the state transition\n\nThe replacement skips the required transition.\n\n```suggestion\nfixed()\n```",
        }
    ]


def test_publish_github_pr_review_rejects_stale_head_before_write() -> None:
    def run(argv, **_kwargs):  # noqa: ANN001
        return subprocess.CompletedProcess(argv, 0, json.dumps({"head": {"sha": "new-head"}}), "")

    with pytest.raises(ValueError, match="current head"):
        publish_github_pr_review(
            repo="acme/widget",
            pull_number=7,
            commit_id="old-head",
            summary="Summary",
            comments=[],
            runner=run,
        )


def test_publish_github_pr_review_moves_invalid_diff_anchor_out_of_inline_review() -> None:
    def run(argv, **_kwargs):  # noqa: ANN001
        if argv[-1] == "repos/acme/widget/pulls/7":
            return subprocess.CompletedProcess(argv, 0, json.dumps({"head": {"sha": "abc123"}}), "")
        if "--method" in argv:
            return subprocess.CompletedProcess(argv, 0, json.dumps({"id": 77, "html_url": "https://example.test/review/77"}), "")
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps([[{"filename": "src/app.py", "patch": "@@ -1 +1 @@\n-old\n+new"}]]),
            "",
        )

    result = publish_github_pr_review(
        repo="acme/widget",
        pull_number=7,
        commit_id="abc123",
        summary="Summary",
        comments=[{"path": "src/app.py", "line": 99, "severity": "P2", "title": "Bad", "body": "Bad"}],
        runner=run,
    )

    assert result.comments == 0
    assert result.skipped_comments == 1


def test_publish_github_pr_review_normalizes_global_diff_output_line_to_file_line() -> None:
    calls: list[tuple[list[str], str]] = []

    def run(argv, **kwargs):  # noqa: ANN001
        calls.append((list(argv), str(kwargs.get("input") or "")))
        endpoint = next((str(item) for item in argv if str(item).startswith("repos/")), "")
        if endpoint.endswith("/pulls/7"):
            return subprocess.CompletedProcess(argv, 0, json.dumps({"head": {"sha": "abc123"}}), "")
        if endpoint.endswith("/files"):
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps([[{"filename": "src/app.py", "patch": "@@ -100,2 +100,3 @@\n old\n+new\n tail"}]]),
                "",
            )
        if endpoint.endswith("/comments"):
            return subprocess.CompletedProcess(argv, 0, "[]", "")
        if argv[:3] == ["gh", "pr", "diff"]:
            return subprocess.CompletedProcess(
                argv,
                0,
                "diff --git a/src/app.py b/src/app.py\n--- a/src/app.py\n+++ b/src/app.py\n@@ -100,2 +100,3 @@\n old\n+new\n tail\n",
                "",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"id": 101, "html_url": "https://example.test/review/101"}),
            "",
        )

    result = publish_github_pr_review(
        repo="acme/widget",
        pull_number=7,
        commit_id="abc123",
        summary="Summary",
        comments=[
            {
                "path": "src/app.py",
                "line": 6,
                "side": "RIGHT",
                "severity": "P2",
                "title": "Use the file line",
                "body": "The reviewer supplied the global gh diff output line.",
            }
        ],
        runner=run,
    )

    payload = json.loads(calls[-1][1])
    assert result.comments == 1
    assert result.skipped_comments == 0
    assert payload["comments"][0]["line"] == 101


def test_publish_github_pr_review_rejects_ambiguous_global_and_file_line() -> None:
    calls: list[tuple[list[str], str]] = []

    def run(argv, **kwargs):  # noqa: ANN001
        calls.append((list(argv), str(kwargs.get("input") or "")))
        endpoint = next((str(item) for item in argv if str(item).startswith("repos/")), "")
        if endpoint.endswith("/pulls/7"):
            return subprocess.CompletedProcess(argv, 0, json.dumps({"head": {"sha": "abc123"}}), "")
        if endpoint.endswith("/files"):
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps(
                    [[{"filename": "src/app.py", "patch": "@@ -6 +6 @@\n-old-six\n+new-six\n@@ -100,2 +100,3 @@\n old\n+new\n tail"}]]
                ),
                "",
            )
        if endpoint.endswith("/comments"):
            return subprocess.CompletedProcess(argv, 0, "[]", "")
        if argv[:3] == ["gh", "pr", "diff"]:
            return subprocess.CompletedProcess(
                argv,
                0,
                "diff --git a/src/app.py b/src/app.py\n--- a/src/app.py\n+++ b/src/app.py\n@@ -100,2 +100,3 @@\n old\n+new\n tail\n",
                "",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"id": 102, "html_url": "https://example.test/review/102"}),
            "",
        )

    result = publish_github_pr_review(
        repo="acme/widget",
        pull_number=7,
        commit_id="abc123",
        summary="Summary",
        comments=[
            {
                "path": "src/app.py",
                "line": 6,
                "side": "RIGHT",
                "severity": "P2",
                "title": "Ambiguous line",
                "body": "Six is both a valid file line and a global diff position.",
            }
        ],
        runner=run,
    )

    payload = json.loads(calls[-1][1])
    assert result.comments == 0
    assert result.skipped_comments == 1
    assert payload["comments"] == []


def test_publish_github_pr_review_trusts_explicit_verified_file_line() -> None:
    calls: list[tuple[list[str], str]] = []

    def run(argv, **kwargs):  # noqa: ANN001
        calls.append((list(argv), str(kwargs.get("input") or "")))
        endpoint = next((str(item) for item in argv if str(item).startswith("repos/")), "")
        if endpoint.endswith("/pulls/7"):
            return subprocess.CompletedProcess(argv, 0, json.dumps({"head": {"sha": "abc123"}}), "")
        if endpoint.endswith("/files"):
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps([[{"filename": "src/app.py", "patch": "@@ -6 +6 @@\n-old-six\n+new-six"}]]),
                "",
            )
        if endpoint.endswith("/comments"):
            return subprocess.CompletedProcess(argv, 0, "[]", "")
        if argv[:3] == ["gh", "pr", "diff"]:
            return subprocess.CompletedProcess(
                argv,
                0,
                "diff --git a/src/app.py b/src/app.py\n--- a/src/app.py\n+++ b/src/app.py\n@@ -100,2 +100,3 @@\n old\n+new\n tail\n",
                "",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"id": 103, "html_url": "https://example.test/review/103"}),
            "",
        )

    result = publish_github_pr_review(
        repo="acme/widget",
        pull_number=7,
        commit_id="abc123",
        summary="Summary",
        comments=[
            {
                "path": "src/app.py",
                "line": 6,
                "line_kind": "FILE",
                "side": "RIGHT",
                "severity": "P2",
                "title": "Verified file line",
                "body": "The orchestrator verified this as a file line.",
            }
        ],
        runner=run,
    )

    payload = json.loads(calls[-1][1])
    assert result.comments == 1
    assert result.skipped_comments == 0
    assert payload["comments"][0]["line"] == 6


def test_publish_github_pr_review_suppresses_equivalent_existing_inline_comment() -> None:
    posted = False
    body = "[P2] Keep the guard\n\nRemoving this guard reopens the race."

    def run(argv, **_kwargs):  # noqa: ANN001
        nonlocal posted
        endpoint = next((str(item) for item in argv if str(item).startswith("repos/")), "")
        if endpoint.endswith("/pulls/7"):
            return subprocess.CompletedProcess(argv, 0, json.dumps({"head": {"sha": "abc123"}}), "")
        if endpoint.endswith("/files"):
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps([[{"filename": "src/app.py", "patch": "@@ -1 +1 @@\n-old\n+new"}]]),
                "",
            )
        if endpoint.endswith("/comments"):
            return subprocess.CompletedProcess(
                argv,
                0,
                json.dumps([[{"path": "src/app.py", "line": 1, "side": "RIGHT", "body": body}]]),
                "",
            )
        if argv[:3] == ["gh", "pr", "diff"]:
            return subprocess.CompletedProcess(argv, 0, "", "")
        posted = True
        return subprocess.CompletedProcess(argv, 0, "{}", "")

    result = publish_github_pr_review(
        repo="acme/widget",
        pull_number=7,
        commit_id="abc123",
        summary="Summary",
        comments=[
            {
                "path": "src/app.py",
                "line": 1,
                "severity": "P2",
                "title": "Keep the guard",
                "body": "Removing this guard reopens the race.",
            }
        ],
        runner=run,
    )

    assert result.comments == 0
    assert result.skipped_comments == 1
    assert posted is False


def test_publish_github_pr_review_still_posts_a_summary_only_review() -> None:
    calls: list[tuple[list[str], str]] = []

    def run(argv, **kwargs):  # noqa: ANN001
        calls.append((list(argv), str(kwargs.get("input") or "")))
        if argv[-1] == "repos/acme/widget/pulls/7":
            return subprocess.CompletedProcess(argv, 0, json.dumps({"head": {"sha": "abc123"}}), "")
        if "--paginate" in argv:
            return subprocess.CompletedProcess(argv, 0, json.dumps([[]]), "")
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps({"id": 77, "html_url": "https://github.com/acme/widget/pull/7#review-77"}),
            "",
        )

    # A reviewer that cannot anchor its findings to diff lines puts them in the
    # summary instead. That review must still reach GitHub: dropping it posted
    # nothing while the caller was told the review had been published.
    result = publish_github_pr_review(
        repo="acme/widget",
        pull_number=7,
        commit_id="abc123",
        summary="## Joined review\n\n[P2] The bench script skips the generate step.",
        comments=[],
        runner=run,
    )

    assert result.review_id == 77
    posted = [call for call in calls if "reviews" in call[0][-2:] or "reviews" in " ".join(call[0])]
    assert posted, "no review was posted to GitHub"
    payload = json.loads(posted[-1][1])
    assert payload["comments"] == []
    assert "[P2] The bench script skips the generate step." in payload["body"]
