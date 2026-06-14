"""Shared pytest configuration.

Two tiers (see docs — Phase 3 testing strategy):
  - UNIT   (tests/unit): pure logic, no network/hardware/keys. Runs by default,
           in milliseconds. The safety net for the repo restructure.
  - INTEGRATION (tests/integration): real services / hardware / keys. Opt-in via
           `--run-integration`; each also skips cleanly when its dependency is
           absent (gateway down, no TTS key, no audio device).

    uv run pytest                      # unit only (integration shown as skipped)
    uv run pytest --run-integration    # also run integration where supported
"""

from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests (real LLM gateway / memory / TTS / STT / audio)",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--run-integration"):
        return
    skip = pytest.mark.skip(reason="needs --run-integration (real services/hardware/keys)")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
