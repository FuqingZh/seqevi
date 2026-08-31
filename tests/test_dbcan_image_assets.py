from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path


ROOT = Path(__file__).parents[1]
IMAGE_ROOT = ROOT / "containers/dbcan"
DATABASE_FILES = (
    "CAZy.dmnd",
    "dbCAN.hmm",
    "dbCAN-sub.hmm",
    "fam-substrate-mapping.tsv",
)


def test_image_version_declarations_match_project_version() -> None:
    project_version = tomllib.loads(
        (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]["version"]
    dockerfile = (IMAGE_ROOT / "Dockerfile").read_text(encoding="utf-8")
    notice = (IMAGE_ROOT / "NOTICE").read_text(encoding="utf-8")
    correspondence = (IMAGE_ROOT / "SOURCE-CORRESPONDENCE.md").read_text(
        encoding="utf-8"
    )

    version_declaration = re.search(r"(?m)^ARG VERSION=([^\s]+)$", dockerfile)
    assert version_declaration is not None
    assert version_declaration.group(1) == project_version
    assert f"SeqEvi {project_version} (MIT)" in notice
    assert f"| SeqEvi {project_version} |" in correspondence


def test_runtime_inputs_are_exact_and_hash_locked() -> None:
    dbcan_lock = (IMAGE_ROOT / "requirements-dbcan.lock").read_text(encoding="utf-8")
    inputs = json.loads((IMAGE_ROOT / "inputs.json").read_text(encoding="utf-8"))
    artifacts = {artifact["name"]: artifact for artifact in inputs["artifacts"]}

    assert set(artifacts) == {
        "dbcan-source",
        "dbcan-license",
        "diamond-binary",
        "diamond-source",
        "diamond-license",
        "oras-binary",
    }
    assert "dbcan==5.2.9" in dbcan_lock
    assert (
        "daf39033e9921d116f46a374714f6095b71394eb6438035f1754354d7e20d8d2" in dbcan_lock
    )
    assert artifacts["dbcan-source"]["url"].endswith(
        "614c93f896939042ae5bd574b9c6b971e80803f6"
    )
    assert artifacts["diamond-source"]["url"].endswith(
        "5c6b545d2d6eb1b31a5d553f39b3cc65e0aec6ce"
    )
    assert artifacts["diamond-binary"]["sha256"] == (
        "2c1507fbb32164e861857d606fddf4b92d481174e4015cc50682f51c7b2f978a"
    )
    assert artifacts["oras-binary"] == {
        "name": "oras-binary",
        "url": (
            "https://github.com/oras-project/oras/releases/download/v1.3.4/"
            "oras_1.3.4_linux_amd64.tar.gz"
        ),
        "sha256": "f27adb935022d94df8dc77719c322dda592c78a0d57a6f7dcdd8d900b248c454",
    }
    for lock_path in IMAGE_ROOT.glob("requirements-*.lock"):
        lock = lock_path.read_text(encoding="utf-8")
        requirements = re.findall(r"(?m)^[-a-zA-Z0-9_.]+==[^ \\]+", lock)
        assert requirements
        assert lock.count("--hash=sha256:") >= len(requirements)


def test_image_build_has_public_labels_and_no_database_acquisition() -> None:
    dockerfile = (IMAGE_ROOT / "Dockerfile").read_text(encoding="utf-8")
    inputs = (IMAGE_ROOT / "inputs.json").read_text(encoding="utf-8")

    assert "linux" not in dockerfile.splitlines()[1].lower()
    assert (
        "python:3.13.11-slim-bookworm@sha256:"
        "ac76900038d8606cc99b413d4ede77bc7152f1e42b94cf5d50d4b80a999652fe" in dockerfile
    )
    assert (
        'org.opencontainers.image.base.name="docker.io/library/python:3.13.11-slim-bookworm"'
        in dockerfile
    )
    assert (
        'org.opencontainers.image.base.digest="sha256:ac76900038d8606cc99b413d4ede77bc7152f1e42b94cf5d50d4b80a999652fe"'
        in dockerfile
    )
    assert "/usr/local/lib/python3.13/LICENSE.txt" in dockerfile
    assert (
        'org.opencontainers.image.source="https://github.com/FuqingZh/seqevi"'
        in dockerfile
    )
    assert 'org.opencontainers.image.revision="${REVISION}"' in dockerfile
    assert "PATH=/opt/venv/bin:/opt/dbcan-venv/bin:" in dockerfile
    assert "/opt/dbcan-venv/bin/run_dbcan version | grep -F 5.2.9" in dockerfile
    assert "cp /opt/dbcan-venv/bin/run_dbcan" not in dockerfile
    assert "importlib.metadata.version('dbcan')" not in dockerfile
    assert "/usr/local/bin/diamond" in dockerfile
    assert "/usr/local/bin/oras" in dockerfile
    assert "tar -xzf /tmp/downloads/oras-binary -C /opt/oras oras LICENSE" in dockerfile
    assert "oras version | grep -F 'Version:        1.3.4'" in dockerfile
    for database_file in DATABASE_FILES:
        assert database_file not in inputs


def test_compliance_bundle_maps_gpl_artifacts_to_sources() -> None:
    notice = (IMAGE_ROOT / "NOTICE").read_text(encoding="utf-8")
    correspondence = (IMAGE_ROOT / "SOURCE-CORRESPONDENCE.md").read_text(
        encoding="utf-8"
    )

    assert "does not contain a dbCAN annotation database" in notice
    assert (
        "daf39033e9921d116f46a374714f6095b71394eb6438035f1754354d7e20d8d2"
        in correspondence
    )
    assert (
        "2c1507fbb32164e861857d606fddf4b92d481174e4015cc50682f51c7b2f978a"
        in correspondence
    )
    assert "sources/dbcan-source" in correspondence
    assert "sources/diamond-source" in correspondence
    assert "CPython 3.13.11" in correspondence
    assert "ORAS 1.3.4" in correspondence
    assert "ORAS-Apache-2.0.txt" in correspondence
    assert "BuildKit's attached" in correspondence
    assert "final-image SBOM" in correspondence


def test_publish_workflow_uses_ghcr_attestations_without_latest() -> None:
    workflow = (ROOT / ".github/workflows/publish-dbcan-image.yml").read_text(
        encoding="utf-8"
    )

    assert "contents: read" in workflow
    assert "packages: write" in workflow
    assert "ghcr.io/fuqingzh/seqevi-dbcan" in workflow
    assert "sbom: true" in workflow
    assert "provenance: mode=max" in workflow
    assert "GITHUB_TOKEN" not in workflow
    assert "${{ github.token }}" in workflow
    assert ":latest" not in workflow
    assert 'docker pull "${reference}"' in workflow
    assert (
        'docker run --rm --entrypoint /opt/dbcan-venv/bin/run_dbcan "${reference}" version'
        " | grep -F '5.2.9'" in workflow
    )
    assert (
        'docker run --rm --entrypoint /opt/venv/bin/seqevi "${reference}" --version'
        in workflow
    )
    assert 'docker run --rm --entrypoint diamond "${reference}" version' in workflow
    assert 'docker run --rm --entrypoint sh "${reference}" -c' in workflow
    assert "database_url" not in workflow.casefold()
    assert "resource_url" not in workflow.casefold()
    assert "steps.project.outputs.version" in workflow
    assert "seqevi-0.2.0" not in workflow
