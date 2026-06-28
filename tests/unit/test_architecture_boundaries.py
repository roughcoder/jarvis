"""Architecture boundary regression tests."""

from __future__ import annotations

import ast
from pathlib import Path


SRC = Path(__file__).parents[2] / "src" / "jarvis"


def _imports_under(path: Path) -> list[tuple[str, str]]:
    imports: list[tuple[str, str]] = []
    for file in path.rglob("*.py"):
        tree = ast.parse(file.read_text(encoding="utf-8"))
        rel = str(file.relative_to(SRC))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.ImportFrom) and node.module:
                modules.append(node.module)
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
