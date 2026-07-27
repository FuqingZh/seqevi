from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[1]


def test_publish_workflow_preserves_trusted_publisher_boundaries() -> None:
    workflow = (ROOT / ".github/workflows/publish.yml").read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert "environment: testpypi" in workflow
    assert "environment: pypi" in workflow
    assert "id-token: write" in workflow
    assert "repository-url: https://test.pypi.org/legacy/" in workflow
    assert "if: github.event_name == 'workflow_dispatch'" in workflow
    assert 'test "${GITHUB_REF_NAME}" = "v${project_version}"' in workflow
    assert "password:" not in workflow
    assert "api-token" not in workflow


def test_systemd_unit_is_loopback_only_and_mount_guarded() -> None:
    unit = (ROOT / "deploy/systemd/seqevi-store.service").read_text(encoding="utf-8")

    assert "--network host" in unit
    assert "serve --host 127.0.0.1 --port 18081" in unit
    assert "Environment=DOCKER_HOST=unix:///var/run/docker.sock" in unit
    assert "/usr/bin/test -S /var/run/docker.sock" in unit
    assert "/usr/bin/docker info" in unit
    assert "verify-artifacts-mount" in unit
    assert "--user ${SEQEVI_UID}:${SEQEVI_GID}" in unit
    assert "SEQEVI_HEALTH_URL=http://127.0.0.1:18081/health" in unit
    assert "--read-only" in unit
    assert "--cap-drop ALL" in unit
    assert "--security-opt no-new-privileges" in unit
    assert "--env SEQEVI_DATABASE_URL" in unit
    assert "EnvironmentFile=%h/.config/seqevi/seqevi-store.env" in unit


def test_service_image_excludes_annotation_runtime_inputs() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8").lower()

    assert "python:3.13-slim" in dockerfile
    assert "pdm install --frozen-lockfile --prod -g server" in dockerfile
    assert "user seqevi" in dockerfile
    assert "eggnog" not in dockerfile
    assert "interpro" not in dockerfile


def _write_fake_findmnt(path: Path) -> Path:
    executable = path / "findmnt"
    executable.write_text(
        "\n".join(
            (
                "#!/bin/sh",
                'case "$*" in',
                '  *"--output TARGET"*) printf \'%s\\n\' "$FAKE_TARGET" ;;',
                "  *) printf '%s\\n' \"$FAKE_FSTYPE\" ;;",
                "esac",
                "",
            )
        ),
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return executable


def _verify_mount(
    artifacts_dir: Path,
    artifacts_mount: Path,
    fake_findmnt: Path,
    *,
    target: Path,
    fstype: str,
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(
        {
            "SEQEVI_FINDMNT_BIN": str(fake_findmnt),
            "FAKE_TARGET": str(target),
            "FAKE_FSTYPE": fstype,
        }
    )
    return subprocess.run(
        (
            str(ROOT / "deploy/systemd/verify-artifacts-mount"),
            str(artifacts_dir),
            str(artifacts_mount),
        ),
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )


def test_artifact_mount_guard_requires_exact_ceph_target(tmp_path: Path) -> None:
    artifacts_mount = tmp_path / "ceph"
    artifacts_dir = artifacts_mount / "seqevi" / "artifacts"
    artifacts_dir.mkdir(parents=True)
    fake_findmnt = _write_fake_findmnt(tmp_path)

    accepted = _verify_mount(
        artifacts_dir,
        artifacts_mount,
        fake_findmnt,
        target=artifacts_mount,
        fstype="ceph",
    )
    wrong_target = _verify_mount(
        artifacts_dir,
        artifacts_mount,
        fake_findmnt,
        target=tmp_path,
        fstype="ceph",
    )
    wrong_fstype = _verify_mount(
        artifacts_dir,
        artifacts_mount,
        fake_findmnt,
        target=artifacts_mount,
        fstype="ext4",
    )

    assert accepted.returncode == 0, accepted.stderr
    assert wrong_target.returncode == 1
    assert "resolves to" in wrong_target.stderr
    assert wrong_fstype.returncode == 1
    assert "must both use ceph" in wrong_fstype.stderr


def test_artifact_mount_guard_rejects_missing_path(tmp_path: Path) -> None:
    fake_findmnt = _write_fake_findmnt(tmp_path)

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.run(
            (
                str(ROOT / "deploy/systemd/verify-artifacts-mount"),
                str(tmp_path / "missing"),
                str(tmp_path),
            ),
            check=True,
            env={**os.environ, "SEQEVI_FINDMNT_BIN": str(fake_findmnt)},
        )
