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
    assert "verify-image-reference" in unit
    assert unit.index("verify-image-reference") < unit.index("docker image inspect")
    assert "--user ${SEQEVI_UID}:${SEQEVI_GID}" in unit
    assert "SEQEVI_HEALTH_URL=http://127.0.0.1:18081/health" in unit
    assert "--read-only" in unit
    assert "--cap-drop ALL" in unit
    assert "--security-opt no-new-privileges" in unit
    assert "--env SEQEVI_DATABASE_URL" in unit
    assert "EnvironmentFile=%h/.config/seqevi/seqevi-store.env" in unit


def test_httpd_ingress_exposes_only_loopback_http_store() -> None:
    config = (ROOT / "deploy/httpd/seqevi-store.conf").read_text(encoding="utf-8")

    assert "Listen 192.168.30.205:18443 https" in config
    assert "SSLEngine on" in config
    assert "AuthType Basic" in config
    assert "AuthUserFile /etc/seqevi/ingress.htpasswd" in config
    assert "Require ip 172.17.0.0/16 192.168.30.0/24" in config
    assert "Require valid-user" in config
    assert "ProxyPass / http://127.0.0.1:18081/ nocanon" in config
    assert "ProxyPassReverse / http://127.0.0.1:18081/" in config
    assert "15432" not in config
    assert "artifacts" not in config.lower()
    assert "0.0.0.0" not in config


def test_httpd_ingress_drops_untrusted_forwarding_headers() -> None:
    config = (ROOT / "deploy/httpd/seqevi-store.conf").read_text(encoding="utf-8")

    for header in (
        "Forwarded",
        "X-Forwarded-For",
        "X-Forwarded-Host",
        "X-Forwarded-Proto",
    ):
        assert f"RequestHeader unset {header}" in config


def _verify_image_reference(reference: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (str(ROOT / "deploy/systemd/verify-image-reference"), reference),
        check=False,
        capture_output=True,
        text=True,
    )


def test_image_reference_guard_accepts_exact_repository_digest() -> None:
    references = (
        "192.168.30.202:23099/bioinfo/seqevi@sha256:"
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "registry.example/team-name/seqevi@sha256:"
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    )

    assert all(
        _verify_image_reference(reference).returncode == 0 for reference in references
    )


@pytest.mark.parametrize(
    "reference",
    (
        "repo:latest",
        "repo@sha256:0123456789abcdef",
        "repo@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeF",
        " repo@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "repo@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef ",
        "repo @sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "repo@sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "repo@@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "repo@sha256:g123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "repo;name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "repo<name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "repo=name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "repo>name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        "repo?name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    ),
)
def test_image_reference_guard_rejects_mutable_or_malformed_references(
    reference: str,
) -> None:
    assert _verify_image_reference(reference).returncode == 1


def test_image_reference_guard_reports_unsupported_characters() -> None:
    rejected = _verify_image_reference(
        "repo?name@sha256:"
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    )

    assert "unsupported, whitespace, or uppercase" in rejected.stderr


def test_user_asset_install_preserves_secret_and_updates_example(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    xdg_config_home = tmp_path / "xdg"
    environment_file = home / ".config/seqevi/seqevi-store.env"
    environment_file.parent.mkdir(parents=True)
    environment_file.write_bytes(b"existing-secret-bytes\n")
    environment_file.chmod(0o600)

    subprocess.run(
        (str(ROOT / "deploy/systemd/install-user-assets"),),
        check=True,
        env={
            **os.environ,
            "HOME": str(home),
            "XDG_CONFIG_HOME": str(xdg_config_home),
        },
    )
    subprocess.run(
        (str(ROOT / "deploy/systemd/install-user-assets"),),
        check=True,
        env={
            **os.environ,
            "HOME": str(home),
            "XDG_CONFIG_HOME": str(xdg_config_home),
        },
    )

    assert environment_file.read_bytes() == b"existing-secret-bytes\n"
    assert environment_file.stat().st_mode & 0o777 == 0o600
    assert (home / ".config/seqevi/seqevi-store.env.example").read_bytes() == (
        ROOT / "deploy/systemd/seqevi-store.env.example"
    ).read_bytes()
    assert (home / ".config/systemd/user/seqevi-store.service").read_bytes() == (
        ROOT / "deploy/systemd/seqevi-store.service"
    ).read_bytes()
    for helper in ("verify-artifacts-mount", "verify-image-reference"):
        assert (home / f".local/libexec/seqevi/{helper}").read_bytes() == (
            ROOT / f"deploy/systemd/{helper}"
        ).read_bytes()
    assert not xdg_config_home.exists()


def test_node4_runbook_selects_one_explicit_rootful_docker_context() -> None:
    runbook = (
        ROOT / "docs/operations/20260727-v0.1.0-loopback-service-runbook.md"
    ).read_text(encoding="utf-8")

    assert runbook.count("export DOCKER_HOST=unix:///var/run/docker.sock") == 1
    assert "DOCKER_HOST=unix:///var/run/docker.sock docker" not in runbook


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
