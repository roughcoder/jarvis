"""Architecture boundary regression tests."""

from __future__ import annotations

import ast
from pathlib import Path


SRC = Path(__file__).parents[2] / "src" / "jarvis"
MOVED_ACCOUNT_MODULES = frozenset(
    {
        "jarvis.brain.accounts",
        "jarvis.brain.account_router",
        "jarvis.brain.account_adapters",
    }
)


def _absolute_from_import(file: Path, node: ast.ImportFrom) -> str:
    if node.level == 0:
        return node.module or ""

    package = ("jarvis", *file.relative_to(SRC).with_suffix("").parts[:-1])
    keep = len(package) - node.level + 1
    base = package[:keep] if keep > 0 else ("jarvis",)
    if node.module:
        base = (*base, *node.module.split("."))
    return ".".join(base)


def _imports_under(path: Path) -> list[tuple[str, str]]:
    imports: list[tuple[str, str]] = []
    files = [path] if path.is_file() else path.rglob("*.py")
    for file in files:
        tree = ast.parse(file.read_text(encoding="utf-8"))
        rel = str(file.relative_to(SRC))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.module:
                modules.append(_absolute_from_import(file, node))
            elif isinstance(node, ast.ImportFrom) and node.level:
                modules.append(_absolute_from_import(file, node))
            elif isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            for module in modules:
                imports.append((rel, module))
    return imports


def test_tools_do_not_import_brain_package() -> None:
    offenders = [
        f"{rel}: {module}"
        for rel, module in _imports_under(SRC / "tools")
        if module == "jarvis.brain" or module.startswith("jarvis.brain.")
    ]
    assert offenders == []


def test_setup_uses_user_store_not_brain_internals() -> None:
    setup = SRC / "setup.py"
    offenders = [
        module
        for _rel, module in _imports_under(setup.parent)
        if module.startswith("jarvis.brain.") and _rel == setup.name
    ]
    assert offenders == []


def test_skills_do_not_import_user_profile_store() -> None:
    skills = SRC / "brain" / "skills.py"
    offenders = [
        module
        for _rel, module in _imports_under(skills.parent)
        if module == "jarvis.users" and _rel == "brain/skills.py"
    ]
    assert offenders == []


def test_orchestration_and_connectors_reach_brain_only_via_facade() -> None:
    """The Cockpit tiers host brain machinery in-process, but their whole brain
    surface is the curated contract in jarvis/brain/facade.py. A deep import is
    a contract widening by side effect — route the symbol through the facade
    (deliberately) instead."""
    offenders = [
        f"{rel}: {module}"
        for package in ("orchestration", "connectors")
        for rel, module in _imports_under(SRC / package)
        if (module == "jarvis.brain" or module.startswith("jarvis.brain."))
        and module != "jarvis.brain.facade"
    ]
    assert offenders == []


def test_brain_never_imports_its_hosting_tiers() -> None:
    offenders = [
        f"{rel}: {module}"
        for rel, module in _imports_under(SRC / "brain")
        if module.startswith(("jarvis.orchestration", "jarvis.connectors"))
    ]
    assert offenders == []


def test_boundary_peers_import_nothing_from_brain() -> None:
    """worker/ and remote/ talk to the brain over HTTP; they import none of it."""
    offenders = [
        f"{rel}: {module}"
        for package in ("worker", "remote")
        for rel, module in _imports_under(SRC / package)
        if module == "jarvis.brain" or module.startswith("jarvis.brain.")
    ]
    assert offenders == []


def test_moved_account_modules_are_updated_everywhere() -> None:
    stale_files = [
        path
        for path in (
            SRC / "brain" / "accounts.py",
            SRC / "brain" / "account_router.py",
            SRC / "brain" / "account_adapters.py",
        )
        if path.exists()
    ]
    assert stale_files == []

    offenders = [
        f"{rel}: {module}"
        for rel, module in _imports_under(SRC)
        if module in MOVED_ACCOUNT_MODULES
    ]
    assert offenders == []
