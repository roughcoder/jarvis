from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit


def public_url(value: str) -> str:
    text = str(value or "")
    if text.startswith(("https://github.com/", "https://linear.app/")):
        parts = urlsplit(text)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
    return ""


def redact(value: str) -> str:
    text = str(value or "")
    text = re.sub(r"https?://[^\s)]+", lambda match: public_url(match.group(0)) or "<redacted-url>", text)
    text = re.sub(r"~/[^\s)]+", "<local-path>", text)
    text = re.sub(r"/Users/[^\s)]+", "<local-path>", text)
    text = re.sub(r"/(?:Applications|home|workspace|workspaces|tmp|mnt|opt)/[^\s)]+", "<local-path>", text)
    text = re.sub(r"/(?:private/tmp|var/folders)/[^\s)]+", "<local-path>", text)
    text = re.sub(r"\b(?:lin_api|ghp|github_pat|sk-[A-Za-z0-9])[A-Za-z0-9_\-]{12,}\b", "<redacted-token>", text)
    text = re.sub(r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b", "<redacted-email>", text)
    return text


def public_error_message(value: str) -> str:
    text = redact(value)
    text = re.sub(r"https?://[^\s)]+", lambda match: public_url(match.group(0)) or "<redacted-url>", text)
    return text[:300]
