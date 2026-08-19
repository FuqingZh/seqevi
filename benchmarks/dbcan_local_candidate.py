"""In-process launch boundary for a release-equivalent local dbCAN image.

This benchmark-only harness keeps the public managed profile and bundled kit
identity unchanged. It substitutes an already-inspected immutable local image
ID only at the Docker launch boundary and restores the production functions
before returning. It is intentionally single-process and is not a production
CLI or profile override mechanism.
"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from seqevi.distribution import oci
from seqevi.distribution.manifest import KitManifest
from seqevi.execution_profile import ExecutionProfile


_IMMUTABLE_LOCAL_IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}")
ACCEPTED_DBCAN_LOCAL_CANDIDATE_ID = (
    "sha256:75b74528663f7b3bc06a48292c13a488447c5f32581fc461abdc242bf9321e13"
)
_AO_SESSION_LABEL = "ao.session"


def _authoritative_ao_session_id() -> str:
    session_id = os.environ.get("AO_SESSION_ID")
    if (
        session_id is None
        or not session_id
        or session_id != session_id.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in session_id)
    ):
        raise ValueError("AO_SESSION_ID is required for benchmark container ownership")
    return session_id


def _has_ao_session_label(arguments: tuple[str, ...]) -> bool:
    for index, argument in enumerate(arguments):
        if argument.startswith(f"--label={_AO_SESSION_LABEL}="):
            return True
        if (
            argument == "--label"
            and index + 1 < len(arguments)
            and arguments[index + 1].startswith(f"{_AO_SESSION_LABEL}=")
        ):
            return True
    return False


@contextmanager
def local_candidate_boundary(local_image_id: str) -> Iterator[None]:
    """Use one inspected local image ID for a benchmark OCI invocation."""

    if _IMMUTABLE_LOCAL_IMAGE_ID.fullmatch(local_image_id) is None:
        raise ValueError("local candidate must be an immutable sha256 image ID")
    if local_image_id != ACCEPTED_DBCAN_LOCAL_CANDIDATE_ID:
        raise ValueError("local candidate does not match the accepted dbCAN image ID")
    ao_session_id = _authoritative_ao_session_id()

    original_ensure_image = oci._ensure_image
    original_docker_call = oci._docker_call
    published_image: str | None = None

    def ensure_local_image(
        docker: str,
        manifest: KitManifest,
        profile_image: str,
    ) -> None:
        nonlocal published_image
        if profile_image != manifest.image:
            raise ValueError("managed profile image differs from bundled kit")
        inspected = original_docker_call(
            docker,
            ("image", "inspect", "--format", "{{.Id}}", local_image_id),
            timeout_seconds=30.0,
            action="inspect the immutable local dbCAN candidate",
        )
        if inspected.returncode != 0 or inspected.stdout.strip() != local_image_id:
            raise ValueError("immutable local dbCAN candidate is not inspectable")
        published_image = profile_image

    def candidate_docker_call(
        docker: str,
        arguments: tuple[str, ...],
        *,
        timeout_seconds: float | None,
        action: str,
    ) -> subprocess.CompletedProcess[str]:
        if arguments and arguments[0] == "create":
            if published_image is None:
                raise ValueError("local candidate was not inspected before launch")
            if arguments.count(published_image) != 1:
                raise ValueError("Docker create must contain the public image once")
            if _has_ao_session_label(arguments):
                raise ValueError("Docker create already contains an AO session label")
            arguments = tuple(
                local_image_id if value == published_image else value
                for value in arguments
            )
            arguments = (
                arguments[0],
                "--label",
                f"{_AO_SESSION_LABEL}={ao_session_id}",
                *arguments[1:],
            )
        return original_docker_call(
            docker,
            arguments,
            timeout_seconds=timeout_seconds,
            action=action,
        )

    oci._ensure_image = ensure_local_image
    oci._docker_call = candidate_docker_call
    try:
        yield
    finally:
        oci._ensure_image = original_ensure_image
        oci._docker_call = original_docker_call


def run_local_candidate_annotation(
    *,
    local_image_id: str,
    fasta: Path,
    output: Path,
    profile: ExecutionProfile,
    store: str | Path,
    threads: int,
    timeout_seconds: float,
) -> oci.OciAnnotationResult:
    """Run one normal managed annotation with a local candidate at launch."""

    with local_candidate_boundary(local_image_id):
        return oci.run_oci_annotation(
            fasta=fasta,
            output=output,
            profile=profile,
            store=store,
            threads=threads,
            timeout_seconds=timeout_seconds,
        )
