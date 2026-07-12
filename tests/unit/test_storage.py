from __future__ import annotations

import json

from jarvis.storage import atomic_write_json


def test_atomic_write_json_writes_document_with_trailing_newline(tmp_path) -> None:  # noqa: ANN001
    target = tmp_path / "doc.json"
    atomic_write_json(target, {"b": 2, "a": 1})
    raw = target.read_text(encoding="utf-8")
    assert raw.endswith("\n") and not raw.endswith("\n\n")
    assert json.loads(raw) == {"a": 1, "b": 2}


def test_atomic_write_json_sorts_keys_and_indents_by_default(tmp_path) -> None:  # noqa: ANN001
    target = tmp_path / "doc.json"
    atomic_write_json(target, {"b": 2, "a": 1})
    assert target.read_text(encoding="utf-8") == '{\n  "a": 1,\n  "b": 2\n}\n'


def test_atomic_write_json_honours_indent_and_sort_overrides(tmp_path) -> None:  # noqa: ANN001
    target = tmp_path / "doc.json"
    atomic_write_json(target, {"b": 2, "a": 1}, indent=None, sort_keys=False)
    assert target.read_text(encoding="utf-8") == '{"b": 2, "a": 1}\n'


def test_atomic_write_json_replaces_existing_and_leaves_no_tempfile(tmp_path) -> None:  # noqa: ANN001
    target = tmp_path / "doc.json"
    atomic_write_json(target, {"v": 1})
    atomic_write_json(target, {"v": 2})
    assert json.loads(target.read_text(encoding="utf-8")) == {"v": 2}
    assert [p.name for p in tmp_path.iterdir()] == ["doc.json"]


def test_atomic_write_json_creates_missing_parent_dirs(tmp_path) -> None:  # noqa: ANN001
    target = tmp_path / "nested" / "deeper" / "doc.json"
    atomic_write_json(target, [])
    assert json.loads(target.read_text(encoding="utf-8")) == []
