"""Validate SeqEvi release tags and built distribution identity."""

from __future__ import annotations

import argparse
import tarfile
import zipfile
from dataclasses import dataclass
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from re import fullmatch

from packaging.utils import parse_sdist_filename, parse_wheel_filename
from packaging.version import Version


_CANONICAL_TAG = r"v(?P<version>(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*))"


@dataclass(frozen=True)
class DistributionSet:
    """One wheel and one sdist carrying one exact SeqEvi version."""

    version: Version
    wheel: Path
    sdist: Path


def parse_canonical_tag(tag: str) -> Version:
    """Return the final version encoded by an exact canonical ``vX.Y.Z`` tag."""
    match = fullmatch(_CANONICAL_TAG, tag)
    if match is None:
        raise ValueError(f"not a canonical SeqEvi release tag: {tag!r}")
    return Version(match.group("version"))


def _metadata_version_from_wheel(path: Path) -> Version:
    with zipfile.ZipFile(path) as archive:
        members = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(members) != 1:
            raise ValueError(f"wheel must contain exactly one METADATA file: {path}")
        metadata = BytesParser(policy=default).parsebytes(archive.read(members[0]))
    return Version(str(metadata["Version"]))


def _metadata_version_from_sdist(path: Path) -> Version:
    with tarfile.open(path, mode="r:gz") as archive:
        members = [
            member
            for member in archive.getmembers()
            if member.name.count("/") == 1 and member.name.endswith("/PKG-INFO")
        ]
        if len(members) != 1:
            raise ValueError(f"sdist must contain exactly one root PKG-INFO: {path}")
        stream = archive.extractfile(members[0])
        if stream is None:
            raise ValueError(f"sdist PKG-INFO is not readable: {path}")
        metadata = BytesParser(policy=default).parse(stream)
    return Version(str(metadata["Version"]))


def validate_distributions(
    directory: Path,
    *,
    expected_version: Version | None = None,
    development_source: str | None = None,
) -> DistributionSet:
    """Validate an exact wheel/sdist pair and return their shared identity."""
    artifacts = sorted(path for path in directory.iterdir() if path.is_file())
    wheels = [path for path in artifacts if path.suffix == ".whl"]
    sdists = [path for path in artifacts if path.name.endswith(".tar.gz")]
    if len(artifacts) != 2 or len(wheels) != 1 or len(sdists) != 1:
        raise ValueError("distribution directory must contain one wheel and one sdist")

    wheel = wheels[0]
    sdist = sdists[0]
    wheel_name, wheel_filename_version, _, _ = parse_wheel_filename(wheel.name)
    sdist_name, sdist_filename_version = parse_sdist_filename(sdist.name)
    if wheel_name != "seqevi" or sdist_name != "seqevi":
        raise ValueError("distribution filenames must name the seqevi project")

    versions = {
        wheel_filename_version,
        sdist_filename_version,
        _metadata_version_from_wheel(wheel),
        _metadata_version_from_sdist(sdist),
    }
    if len(versions) != 1:
        raise ValueError(f"wheel and sdist version identities disagree: {versions}")
    version = versions.pop()
    if expected_version is not None and version != expected_version:
        raise ValueError(
            f"distribution version {version} != expected {expected_version}"
        )

    if development_source is not None:
        source = development_source.casefold()
        if fullmatch(r"[0-9a-f]{40}", source) is None:
            raise ValueError("development source must be a full Git commit SHA")
        if version.is_prerelease is False or version.dev is None:
            raise ValueError(
                f"untagged source must build a development version: {version}"
            )
        local = version.local or ""
        if source[:7] not in local and source not in local:
            raise ValueError(f"development version {version} is not tied to {source}")

    return DistributionSet(version=version, wheel=wheel, sdist=sdist)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    tag_parser = subparsers.add_parser("tag")
    tag_parser.add_argument("tag")

    artifacts_parser = subparsers.add_parser("artifacts")
    artifacts_parser.add_argument("--dist-dir", type=Path, required=True)
    artifacts_parser.add_argument("--expected-version")
    artifacts_parser.add_argument("--development-source")
    return parser.parse_args()


def main() -> None:
    """Run a fail-closed release-contract validation command."""
    args = _parse_args()
    if args.command == "tag":
        print(parse_canonical_tag(args.tag))
        return

    expected = Version(args.expected_version) if args.expected_version else None
    distributions = validate_distributions(
        args.dist_dir,
        expected_version=expected,
        development_source=args.development_source,
    )
    print(distributions.version)


if __name__ == "__main__":
    main()
