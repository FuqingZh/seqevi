from __future__ import annotations

from pathlib import Path

import pytest

from seqevi.errors import AdapterError
from seqevi.runtime_identity import RuntimeComponent, calculate_runtime_digest


def _runtime(root: Path) -> tuple[RuntimeComponent, ...]:
    root.mkdir()
    (root / "launcher").write_bytes(b"launcher-v1")
    package = root / "package"
    package.mkdir()
    (package / "core.py").write_bytes(b"core-v1")
    return (
        RuntimeComponent("launcher", root / "launcher"),
        RuntimeComponent("package/core.py", package / "core.py"),
    )


def _digest(components: tuple[RuntimeComponent, ...]) -> str:
    return calculate_runtime_digest(
        runtime_name="fixture",
        versions={"tool": "1.0", "engine": "2.0"},
        components=components,
    )


def test_runtime_digest_is_path_and_component_order_independent(
    tmp_path: Path,
) -> None:
    first = _runtime(tmp_path / "first")
    second = _runtime(tmp_path / "second")

    assert _digest(first) == _digest(tuple(reversed(second)))


def test_runtime_digest_changes_with_content_or_normalized_version(
    tmp_path: Path,
) -> None:
    components = _runtime(tmp_path / "runtime")
    original = _digest(components)

    components[1].path.write_bytes(b"core-v2")
    assert _digest(components) != original
    assert calculate_runtime_digest(
        runtime_name="fixture",
        versions={"tool": "1.1", "engine": "2.0"},
        components=components,
    ) != _digest(components)


def test_runtime_identity_rejects_ambiguous_or_missing_components(
    tmp_path: Path,
) -> None:
    components = _runtime(tmp_path / "runtime")
    with pytest.raises(AdapterError, match="duplicate runtime component"):
        _digest((components[0], components[0]))
    with pytest.raises(AdapterError, match="not a file"):
        _digest((RuntimeComponent("missing", tmp_path / "missing"),))
    with pytest.raises(AdapterError, match="invalid runtime component"):
        _digest((RuntimeComponent("../escape", components[0].path),))
