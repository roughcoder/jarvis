from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Callable
from typing import Any, Protocol

import httpx

from jarvis.orchestration.models import Artifact, WorkItem


class WorkSource(Protocol):
    def list(self, *, repo: str = "", filters: dict | None = None, limit: int = 10) -> list[WorkItem]: ...
    def next(self, *, repo: str = "", filters: dict | None = None) -> WorkItem | None: ...
    def claim(self, item: WorkItem) -> bool: ...
    def link_pr(self, item: WorkItem, artifact: Artifact) -> bool: ...
    def comment(self, item: WorkItem, body: str) -> bool: ...


Runner = Callable[[list[str]], subprocess.CompletedProcess[str]]


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=False)


class GitHubWorkSource:
    def __init__(self, runner: Runner = _run) -> None:
        self._run = runner

    def list(self, *, repo: str = "", filters: dict | None = None, limit: int = 10) -> list[WorkItem]:
        filters = filters or {}
        args = ["gh", "issue", "list", "--json", "number,title,url,body,labels,assignees,state,updatedAt", "--limit", str(limit)]
        if repo:
            args.extend(["--repo", repo])
        if label := filters.get("label"):
            args.extend(["--label", str(label)])
        if assignee := filters.get("assignee"):
            args.extend(["--assignee", "@me" if assignee == "me" else str(assignee)])
        result = self._run(args)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "gh issue list failed")
        data = json.loads(result.stdout or "[]")
        return [_issue_to_item(x, repo) for x in data]

    def next(self, *, repo: str = "", filters: dict | None = None) -> WorkItem | None:
        items = self.list(repo=repo, filters=filters, limit=10)
        return items[0] if items else None

    def pr_comments(self, repo: str, number: int) -> list[dict[str, Any]]:
        args = ["gh", "pr", "view", str(number), "--json", "comments,reviews"]
        if repo:
            args.extend(["--repo", repo])
        result = self._run(args)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "gh pr view failed")
        data = json.loads(result.stdout or "{}")
        comments = list(data.get("comments", [])) + list(data.get("reviews", [])) + list(data.get("reviewThreads", []))
        if repo:
            inline = self._run(["gh", "api", f"repos/{repo}/pulls/{number}/comments", "--paginate", "--slurp"])
            if inline.returncode != 0:
                raise RuntimeError(inline.stderr.strip() or inline.stdout.strip() or "gh pr comments failed")
            inline_data = json.loads(inline.stdout or "[]")
            if inline_data and all(isinstance(page, list) for page in inline_data):
                for page in inline_data:
                    comments.extend(page)
            elif isinstance(inline_data, list):
                comments.extend(inline_data)
            elif isinstance(inline_data, dict):
                comments.append(inline_data)
        return comments

    def claim(self, item: WorkItem) -> bool:
        return False

    def link_pr(self, item: WorkItem, artifact: Artifact) -> bool:
        if not artifact.url:
            return False
        return self.comment(item, f"Jarvis linked {artifact.url}")

    def comment(self, item: WorkItem, body: str) -> bool:
        if not item.repo:
            return False
        number = item.id.split("#")[-1]
        result = self._run(["gh", "issue", "comment", number, "--repo", item.repo, "--body", body])
        return result.returncode == 0


def _issue_to_item(raw: dict[str, Any], repo: str) -> WorkItem:
    labels = [x.get("name", "") for x in raw.get("labels", []) if isinstance(x, dict)]
    assignees = [x.get("login", "") for x in raw.get("assignees", []) if isinstance(x, dict)]
    number = str(raw.get("number", ""))
    return WorkItem(
        source="github",
        id=f"#{number}" if number else "",
        title=raw.get("title", ""),
        url=raw.get("url", ""),
        body=raw.get("body", "") or "",
        repo=repo,
        kind="issue",
        status=raw.get("state", ""),
        labels=labels,
        assignee=", ".join(x for x in assignees if x),
        updated_at=raw.get("updatedAt", ""),
    )


class LinearWorkSource:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        endpoint: str = "https://api.linear.app/graphql",
        post: Callable[..., Any] | None = None,
    ) -> None:
        self.api_key = api_key or os.environ.get("LINEAR_API_KEY", "")
        self.endpoint = endpoint
        self._post = post or httpx.post

    def list(self, *, repo: str = "", filters: dict | None = None, limit: int = 10) -> list[WorkItem]:
        query = """
        query JarvisIssues($first: Int!) {
          issues(first: $first, orderBy: updatedAt) {
            nodes {
              identifier
              id
              title
              description
              url
              priorityLabel
              updatedAt
              state { name }
              assignee { name }
              labels { nodes { name } }
            }
          }
        }
        """
        data = self._graphql(query, {"first": limit})
        nodes = data.get("issues", {}).get("nodes", [])
        items = [_linear_to_item(x, repo) for x in nodes]
        filters = filters or {}
        if label := filters.get("label"):
            items = [x for x in items if label in x.labels]
        if filters.get("status") == "ready":
            items = [x for x in items if x.status.lower() not in {"blocked", "done", "canceled", "cancelled"}]
        return items

    def next(self, *, repo: str = "", filters: dict | None = None) -> WorkItem | None:
        items = self.list(repo=repo, filters=filters, limit=10)
        return items[0] if items else None

    def claim(self, item: WorkItem) -> bool:
        return self.comment(item, "Jarvis claimed this work item.")

    def link_pr(self, item: WorkItem, artifact: Artifact) -> bool:
        return bool(artifact.url) and self.comment(item, f"Jarvis linked {artifact.url}")

    def comment(self, item: WorkItem, body: str) -> bool:
        mutation = """
        mutation JarvisComment($issueId: String!, $body: String!) {
          commentCreate(input: { issueId: $issueId, body: $body }) { success }
        }
        """
        try:
            data = self._graphql(mutation, {"issueId": item.source_internal_id or item.id, "body": body})
        except RuntimeError:
            return False
        return bool(data.get("commentCreate", {}).get("success"))

    def _graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        if not self.api_key:
            raise RuntimeError("LINEAR_API_KEY is not set")
        response = self._post(
            self.endpoint,
            headers={"Authorization": self.api_key, "Content-Type": "application/json"},
            json={"query": query, "variables": variables},
            timeout=15,
        )
        response.raise_for_status()
        body = response.json()
        if body.get("errors"):
            raise RuntimeError(str(body["errors"]))
        return body.get("data", {})


def _linear_to_item(raw: dict[str, Any], repo: str) -> WorkItem:
    labels = raw.get("labels", {}).get("nodes", [])
    return WorkItem(
        source="linear",
        id=raw.get("identifier", ""),
        source_internal_id=raw.get("id", ""),
        title=raw.get("title", ""),
        url=raw.get("url", ""),
        body=raw.get("description", "") or "",
        repo=repo,
        kind="ticket",
        status=raw.get("state", {}).get("name", ""),
        priority=raw.get("priorityLabel", ""),
        labels=[x.get("name", "") for x in labels if isinstance(x, dict)],
        assignee=raw.get("assignee", {}).get("name", "") if raw.get("assignee") else "",
        updated_at=raw.get("updatedAt", ""),
    )
