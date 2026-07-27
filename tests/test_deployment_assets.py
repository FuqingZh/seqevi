from __future__ import annotations

from pathlib import Path


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
    assert "/usr/bin/mountpoint --quiet" in unit
    assert "/usr/bin/findmnt --mountpoint" in unit
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
