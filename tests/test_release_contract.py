from __future__ import annotations

import io
import re
import tarfile
import tomllib
import zipfile
from dataclasses import dataclass
from pathlib import Path

import pytest
from packaging.version import Version

from scripts.release_contract import parse_canonical_tag, validate_distributions
from scripts.version import format_version


ROOT = Path(__file__).parents[1]


@dataclass(frozen=True)
class _FakeSCMVersion:
    version: Version
    distance: int | None
    dirty: bool
    node: str | None


def _write_distributions(
    directory: Path,
    version: str,
    *,
    sdist_metadata_version: str | None = None,
) -> None:
    wheel = directory / f"seqevi-{version}-py3-none-any.whl"
    with zipfile.ZipFile(wheel, mode="w") as archive:
        archive.writestr(
            f"seqevi-{version}.dist-info/METADATA",
            f"Metadata-Version: 2.4\nName: seqevi\nVersion: {version}\n",
        )

    sdist = directory / f"seqevi-{version}.tar.gz"
    metadata = (
        "Metadata-Version: 2.4\n"
        "Name: seqevi\n"
        f"Version: {sdist_metadata_version or version}\n"
    ).encode()
    with tarfile.open(sdist, mode="w:gz") as archive:
        member = tarfile.TarInfo(f"seqevi-{version}/PKG-INFO")
        member.size = len(metadata)
        archive.addfile(member, io.BytesIO(metadata))


@pytest.mark.parametrize("tag", ("v0.4.0", "v1.0.0", "v12.34.56"))
def test_parse_canonical_tag(tag: str) -> None:
    assert parse_canonical_tag(tag) == Version(tag.removeprefix("v"))


@pytest.mark.parametrize(
    "tag",
    ("0.4.0", "v0.4", "v01.2.3", "v1.02.3", "v1.2.03", "v1.2.3rc1", "v1.2.3.4"),
)
def test_parse_canonical_tag_rejects_noncanonical_tags(tag: str) -> None:
    with pytest.raises(ValueError, match="not a canonical"):
        parse_canonical_tag(tag)


def test_distribution_set_accepts_one_exact_final_identity(tmp_path: Path) -> None:
    _write_distributions(tmp_path, "0.4.0")

    result = validate_distributions(tmp_path, expected_version=Version("0.4.0"))

    assert result.version == Version("0.4.0")
    assert result.wheel.name == "seqevi-0.4.0-py3-none-any.whl"
    assert result.sdist.name == "seqevi-0.4.0.tar.gz"


def test_distribution_set_requires_source_tied_development_identity(
    tmp_path: Path,
) -> None:
    source = "4a731dcf346a604df24c9c935dff246708db0782"
    _write_distributions(tmp_path, "0.3.6.dev13+g4a731dc")

    result = validate_distributions(tmp_path, development_source=source)

    assert result.version == Version("0.3.6.dev13+g4a731dc")


def test_distribution_set_rejects_incomplete_or_mismatched_artifacts(
    tmp_path: Path,
) -> None:
    _write_distributions(tmp_path, "0.4.0", sdist_metadata_version="0.4.1")
    with pytest.raises(ValueError, match="identities disagree"):
        validate_distributions(tmp_path)

    next(tmp_path.glob("*.tar.gz")).unlink()
    with pytest.raises(ValueError, match="one wheel and one sdist"):
        validate_distributions(tmp_path)


def test_project_version_is_scm_dynamic_and_runtime_uses_metadata() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    init = (ROOT / "src/seqevi/__init__.py").read_text(encoding="utf-8")

    assert project["project"]["dynamic"] == ["version"]
    assert "version" not in project["project"]
    assert project["tool"]["pdm"]["version"] == {
        "source": "scm",
        "tag_filter": "v[0-9]*.[0-9]*.[0-9]*",
        "tag_regex": (
            "^v(?P<version>(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*))$"
        ),
        "version_format": "scripts.version:format_version",
    }
    assert '__version__ = version("seqevi")' in init
    assert re.search(r'__version__\s*=\s*["\']', init) is None


def test_scm_formatter_preserves_final_and_ties_next_minor_to_source() -> None:
    final = _FakeSCMVersion(
        version=Version("0.4.0"), distance=None, dirty=False, node=None
    )
    development = _FakeSCMVersion(
        version=Version("0.3.5"), distance=13, dirty=False, node="g4a731dc"
    )

    assert format_version(final) == "0.4.0"
    assert format_version(development) == "0.4.0.dev13+g4a731dc"


def test_publication_workflows_pin_every_third_party_action() -> None:
    for relative_path in (
        ".github/workflows/publish.yml",
        ".github/workflows/nightly.yml",
        ".github/workflows/publish-dbcan-image.yml",
    ):
        workflow = (ROOT / relative_path).read_text(encoding="utf-8")
        uses = re.findall(r"(?m)^\s*- uses: ([^\s#]+)", workflow)
        assert uses
        assert all(re.fullmatch(r"[^@]+@[0-9a-f]{40}", use) for use in uses)


def test_tag_validation_and_release_publishing_are_distinct() -> None:
    workflow = (ROOT / ".github/workflows/publish.yml").read_text(encoding="utf-8")
    publish_job = workflow.split("  publish-pypi:\n", 1)[1]

    assert 'tags:\n      - "v*.*.*"' in workflow
    assert "release:\n    types: [published]" in workflow
    assert "workflow_dispatch:" not in workflow
    assert "testpypi" not in workflow.casefold()
    assert "environment: pypi" in publish_job
    assert publish_job.count("id-token: write") == 1
    assert "github.event_name == 'release'" in publish_job
    assert "github.event.release.prerelease == false" in publish_job
    assert publish_job.count("- uses:") == 2
    assert "actions/checkout" not in publish_job
    assert "run:" not in publish_job
    assert "id-token: write" not in workflow.split("  publish-pypi:\n", 1)[0]


def test_nightly_is_finite_validation_without_publication_authority() -> None:
    workflow = (ROOT / ".github/workflows/nightly.yml").read_text(encoding="utf-8")

    assert 'cron: "17 3 * * *"' in workflow
    assert "workflow_dispatch:" in workflow
    assert "ref: main" in workflow
    assert "fetch-depth: 0" in workflow
    assert "pdm run check" in workflow
    assert "python -m twine check dist/*" in workflow
    assert "name: nightly-${{ steps.source.outputs.sha }}" in workflow
    assert "retention-days: 14" in workflow
    assert "id-token: write" not in workflow
    assert "packages: write" not in workflow
    assert "gh-action-pypi-publish" not in workflow
    assert "docker/login-action" not in workflow


def test_dbcan_publication_remains_manual_and_python_release_independent() -> None:
    workflow = (ROOT / ".github/workflows/publish-dbcan-image.yml").read_text(
        encoding="utf-8"
    )
    trigger_block = workflow.split("permissions:", 1)[0]

    assert re.search(r"(?m)^on:\n  workflow_dispatch:\s*$", trigger_block)
    assert "release:" not in trigger_block
    assert "push:" not in trigger_block
    assert "steps.project.outputs.version" in workflow
    assert "rev-${{ github.sha }}" in workflow
    assert "org.opencontainers.image.revision" in workflow
