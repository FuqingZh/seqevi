"""Format SCM-derived SeqEvi development versions."""

from __future__ import annotations

from typing import Protocol


class _SCMVersion(Protocol):
    @property
    def version(self) -> object: ...

    @property
    def distance(self) -> int | None: ...

    @property
    def dirty(self) -> bool: ...

    @property
    def node(self) -> str | None: ...


def format_version(scm_version: _SCMVersion) -> str:
    """Return a final exact-tag version or the next minor development identity."""
    tagged = str(scm_version.version)
    if scm_version.distance is None:
        return f"{tagged}+dirty" if scm_version.dirty else tagged

    release = getattr(scm_version.version, "release", ())
    if len(release) != 3 or scm_version.node is None:
        raise ValueError("SeqEvi SCM versions require a three-part tag and source node")
    major, minor, _patch = release
    local = f"+{scm_version.node}"
    if scm_version.dirty:
        local += ".dirty"
    return f"{major}.{minor + 1}.0.dev{scm_version.distance}{local}"
