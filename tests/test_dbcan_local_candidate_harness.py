from __future__ import annotations

import runpy
import subprocess
from typing import Any, cast

import pytest

from seqevi.distribution import oci
from seqevi.distribution.manifest import load_kit_manifest


def _harness() -> dict[str, Any]:
    return cast(dict[str, Any], runpy.run_path("benchmarks/dbcan_local_candidate.py"))


def test_local_candidate_replaces_only_the_oci_launch_image(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AO_SESSION_ID", "seqevi-29-test")
    manifest = load_kit_manifest("dbcan-cazyme")
    local_id = cast(str, _harness()["ACCEPTED_DBCAN_LOCAL_CANDIDATE_ID"])
    calls: list[tuple[str, ...]] = []

    def fake_docker(
        _docker: str,
        arguments: tuple[str, ...],
        *,
        timeout_seconds: float | None,
        action: str,
    ) -> subprocess.CompletedProcess[str]:
        del timeout_seconds, action
        calls.append(arguments)
        stdout = f"{local_id}\n" if arguments[:2] == ("image", "inspect") else ""
        return subprocess.CompletedProcess(arguments, 0, stdout, "")

    monkeypatch.setattr(oci, "_docker_call", fake_docker)
    original_ensure = oci._ensure_image
    boundary = _harness()["local_candidate_boundary"]

    with boundary(local_id):
        oci._ensure_image("/usr/bin/docker", manifest, manifest.image)
        oci._docker_call(
            "/usr/bin/docker",
            ("create", "--name", "candidate", manifest.image, "--version"),
            timeout_seconds=30.0,
            action="create test container",
        )

    assert calls == [
        ("image", "inspect", "--format", "{{.Id}}", local_id),
        (
            "create",
            "--label",
            "ao.session=seqevi-29-test",
            "--name",
            "candidate",
            local_id,
            "--version",
        ),
    ]
    assert oci._ensure_image is original_ensure
    assert oci._docker_call is fake_docker


def test_local_candidate_rejects_mutable_or_unavailable_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AO_SESSION_ID", "seqevi-29-test")
    boundary = _harness()["local_candidate_boundary"]
    with pytest.raises(ValueError, match="immutable sha256 image ID"):
        with boundary("seqevi-dbcan:local"):
            pass
    with pytest.raises(ValueError, match="accepted dbCAN image ID"):
        with boundary("sha256:" + "c" * 64):
            pass

    def missing_image(
        _docker: str,
        arguments: tuple[str, ...],
        *,
        timeout_seconds: float | None,
        action: str,
    ) -> subprocess.CompletedProcess[str]:
        del timeout_seconds, action
        return subprocess.CompletedProcess(arguments, 1, "", "missing")

    monkeypatch.setattr(oci, "_docker_call", missing_image)
    manifest = load_kit_manifest("dbcan-cazyme")
    accepted_id = cast(str, _harness()["ACCEPTED_DBCAN_LOCAL_CANDIDATE_ID"])
    with boundary(accepted_id):
        with pytest.raises(ValueError, match="not inspectable"):
            oci._ensure_image("/usr/bin/docker", manifest, manifest.image)


def test_local_candidate_rejects_public_identity_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AO_SESSION_ID", "seqevi-29-test")
    monkeypatch.setattr(
        oci,
        "_docker_call",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )
    manifest = load_kit_manifest("dbcan-cazyme")
    boundary = _harness()["local_candidate_boundary"]
    accepted_id = cast(str, _harness()["ACCEPTED_DBCAN_LOCAL_CANDIDATE_ID"])
    with boundary(accepted_id):
        with pytest.raises(ValueError, match="differs from bundled kit"):
            oci._ensure_image("/usr/bin/docker", manifest, "sha256:" + "e" * 64)


def test_local_candidate_requires_authoritative_ao_session_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AO_SESSION_ID", raising=False)
    boundary = _harness()["local_candidate_boundary"]
    accepted_id = cast(str, _harness()["ACCEPTED_DBCAN_LOCAL_CANDIDATE_ID"])

    with pytest.raises(ValueError, match="AO_SESSION_ID is required"):
        with boundary(accepted_id):
            pass


def test_local_candidate_rejects_an_existing_ao_session_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AO_SESSION_ID", "seqevi-29-test")
    manifest = load_kit_manifest("dbcan-cazyme")
    local_id = cast(str, _harness()["ACCEPTED_DBCAN_LOCAL_CANDIDATE_ID"])

    def fake_docker(
        _docker: str,
        arguments: tuple[str, ...],
        *,
        timeout_seconds: float | None,
        action: str,
    ) -> subprocess.CompletedProcess[str]:
        del timeout_seconds, action
        stdout = f"{local_id}\n" if arguments[:2] == ("image", "inspect") else ""
        return subprocess.CompletedProcess(arguments, 0, stdout, "")

    monkeypatch.setattr(oci, "_docker_call", fake_docker)
    boundary = _harness()["local_candidate_boundary"]
    with boundary(local_id):
        oci._ensure_image("/usr/bin/docker", manifest, manifest.image)
        with pytest.raises(ValueError, match="already contains an AO session label"):
            oci._docker_call(
                "/usr/bin/docker",
                (
                    "create",
                    "--label",
                    "ao.session=untrusted",
                    manifest.image,
                ),
                timeout_seconds=30.0,
                action="create test container",
            )
