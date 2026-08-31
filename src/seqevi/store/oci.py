"""Thin, deterministic ORAS boundary for shared OCI artifact storage.

This module deliberately delegates transfer/retry mechanics to ORAS.  It owns
only SeqEvi's immutable manifest construction and the proof needed before a
storage reference is offered for evidence finalization.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from seqevi.errors import StoreConfigurationError, StoreError, StoreIntegrityError
from seqevi.evidence import ArtifactFile, StoredArtifact
from seqevi.runner import (
    ToolCancelledError,
    ToolCommand,
    ToolOutputLimitError,
    ToolRunner,
    ToolTimeoutError,
)

if TYPE_CHECKING:
    from seqevi.store.transport import (
        ArtifactReferenceModel,
        OciStorageReference,
        RegistryModel,
    )

_ORAS_VERSION = "1.3.4"
_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
_ARTIFACT_TYPE = "application/vnd.seqevi.evidence-artifact.v1"
_EMPTY_CONFIG = b"{}"
_EMPTY_CONFIG_DIGEST = hashlib.sha256(_EMPTY_CONFIG).hexdigest()
_MAX_MANIFEST_BYTES = 64 * 1024
_MAX_NATIVE_OUTPUT_BYTES = 64 * 1024
_VERSION_LINE = re.compile(r"^Version:\s*1\.3\.4\s*$", re.MULTILINE)


class OciRegistryError(StoreError):
    """A safe OCI transport failure; command output is intentionally withheld."""

    def __init__(self, category: str, digest: str | None = None) -> None:
        self.category = category
        self.digest = digest
        suffix = f": {digest}" if digest is not None else ""
        super().__init__(f"OCI Registry {category}{suffix}")


@dataclass(frozen=True, slots=True)
class OciClientFiles:
    """Invocation-only overrides for ORAS's standard credential and CA lookup."""

    registry_config: Path | None = None
    ca_file: Path | None = None

    def validated(self, *, headless: bool = False) -> OciClientFiles:
        def regular(path: Path, label: str) -> Path:
            try:
                resolved = path.expanduser().resolve(strict=True)
            except OSError as error:
                raise StoreConfigurationError(
                    f"OCI {label} must be a readable regular file"
                ) from error
            if not resolved.is_file() or not os.access(resolved, os.R_OK):
                raise StoreConfigurationError(
                    f"OCI {label} must be a readable regular file"
                )
            return resolved

        config = (
            regular(self.registry_config, "registry configuration")
            if self.registry_config is not None
            else None
        )
        ca_file = regular(self.ca_file, "CA file") if self.ca_file is not None else None
        if headless and config is not None:
            try:
                contents = json.loads(config.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
                raise StoreConfigurationError(
                    "OCI registry configuration is invalid"
                ) from error
            if not isinstance(contents, dict) or any(
                key in contents for key in ("credsStore", "credHelpers")
            ):
                raise StoreConfigurationError(
                    "headless OCI registry configuration cannot use credential helpers"
                )
        return OciClientFiles(config, ca_file)


def canonical_manifest(reference: ArtifactReferenceModel) -> bytes:
    """Return the sole permitted deterministic OCI manifest for raw identity."""

    body = {
        "schemaVersion": 2,
        "mediaType": _MANIFEST_MEDIA_TYPE,
        "artifactType": _ARTIFACT_TYPE,
        "config": {
            "mediaType": "application/vnd.oci.empty.v1+json",
            "digest": f"sha256:{_EMPTY_CONFIG_DIGEST}",
            "size": len(_EMPTY_CONFIG),
        },
        "layers": [
            {
                "mediaType": reference.media_type,
                "digest": f"sha256:{reference.digest}",
                "size": reference.byte_size,
                "annotations": {"org.opencontainers.image.title": "artifact.bin"},
            }
        ],
    }
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")


def manifest_digest(reference: ArtifactReferenceModel) -> str:
    return hashlib.sha256(canonical_manifest(reference)).hexdigest()


class OciRegistry:
    """ORAS v1.3.4 adapter for one configured Registry repository."""

    def __init__(
        self,
        registry: RegistryModel,
        *,
        executable: Path | None = None,
        files: OciClientFiles | None = None,
        runner: ToolRunner | None = None,
    ) -> None:
        self.registry = registry
        self.executable = executable
        self.files = (files or OciClientFiles()).validated()
        self.runner = runner or ToolRunner()
        self._resolved_executable: Path | None = None

    def preflight(
        self, *, deadline: float, cancellation_signal: Event | None = None
    ) -> None:
        executable = self._resolve_executable()
        version = self._run(
            (str(executable), "version"), deadline, cancellation_signal
        ).decode("utf-8", errors="replace")
        if _VERSION_LINE.search(version) is None:
            raise StoreConfigurationError("OCI requires ORAS v1.3.4")
        try:
            probe = f"{self._repository()}@sha256:{'0' * 64}"
            self._run(
                self._oras_args(
                    "blob",
                    "fetch",
                    "--descriptor",
                    probe,
                ),
                deadline,
                cancellation_signal,
                missing_reference=probe,
            )
        except OciRegistryError as error:
            if error.category not in {"blob-unknown", "name-unknown"}:
                raise

    def stage(
        self,
        payload: ArtifactFile,
        *,
        deadline: float,
        cancellation_signal: Event | None = None,
    ) -> StoredArtifact:
        """Retain bytes using the caller's path, independently of child cwd.

        Examples:
            ``registry.stage(payload, deadline=deadline)`` accepts a payload
            whose path is relative to the caller's current directory.
        """
        payload_path = payload.path.resolve(strict=True)
        reference = self._reference(payload)
        manifest = canonical_manifest(reference)
        digest = manifest_digest(reference)
        tag = f"sha256-{digest}"
        with tempfile.TemporaryDirectory(prefix="seqevi-oci-") as directory:
            root = Path(directory)
            config = root / "config.json"
            manifest_path = root / "manifest.json"
            config.write_bytes(_EMPTY_CONFIG)
            manifest_path.write_bytes(manifest)
            target = self._repository()
            self._run_blob_push(
                config, _EMPTY_CONFIG_DIGEST, deadline, cancellation_signal
            )
            self._run_blob_push(
                payload_path, payload.digest, deadline, cancellation_signal
            )
            try:
                self._run(
                    self._oras_args(
                        "manifest", "push", f"{target}:{tag}", str(manifest_path)
                    ),
                    deadline,
                    cancellation_signal,
                )
            except OciRegistryError as error:
                # A lost manifest response is the sole ambiguous outcome we reconcile.
                if error.category != "command":
                    raise
            self._assert_manifest(
                f"{target}:{tag}", manifest, digest, deadline, cancellation_signal
            )
            self._assert_manifest(
                f"{target}@sha256:{digest}",
                manifest,
                digest,
                deadline,
                cancellation_signal,
            )
        return StoredArtifact(
            digest=payload.digest,
            media_type=payload.media_type,
            byte_size=payload.byte_size,
            relative_path=None,
            storage_kind="oci",
            registry_id=self.registry.id,
            repository=self.registry.repository,
            manifest_digest=digest,
        )

    def verify(
        self,
        reference: ArtifactReferenceModel,
        storage: OciStorageReference,
        *,
        deadline: float,
        cancellation_signal: Event | None = None,
    ) -> StoredArtifact:
        if (
            storage.registry_id != self.registry.id
            or storage.repository != self.registry.repository
        ):
            raise StoreIntegrityError(
                f"OCI storage target mismatch: {reference.digest}"
            )
        expected = canonical_manifest(reference)
        expected_digest = manifest_digest(reference)
        if storage.manifest_digest != expected_digest:
            raise StoreIntegrityError(
                f"OCI manifest digest mismatch: {reference.digest}"
            )
        target = self._repository()
        tag = f"sha256-{expected_digest}"
        self._assert_manifest(
            f"{target}:{tag}", expected, expected_digest, deadline, cancellation_signal
        )
        self._assert_manifest(
            f"{target}@sha256:{expected_digest}",
            expected,
            expected_digest,
            deadline,
            cancellation_signal,
        )
        descriptor = self._run(
            self._oras_args(
                "blob",
                "fetch",
                "--descriptor",
                f"{target}@sha256:{reference.digest}",
            ),
            deadline,
            cancellation_signal,
        )
        self._assert_blob_descriptor(reference, descriptor)
        return StoredArtifact(
            digest=reference.digest,
            media_type=reference.media_type,
            byte_size=reference.byte_size,
            relative_path=None,
            storage_kind="oci",
            registry_id=self.registry.id,
            repository=self.registry.repository,
            manifest_digest=expected_digest,
        )

    def download(
        self,
        reference: ArtifactReferenceModel,
        storage: OciStorageReference,
        target: Path,
        *,
        deadline: float,
        cancellation_signal: Event | None = None,
    ) -> None:
        self.verify(
            reference,
            storage,
            deadline=deadline,
            cancellation_signal=cancellation_signal,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary_root = Path(
            tempfile.mkdtemp(prefix="seqevi-oci-download-", dir=target.parent)
        )
        try:
            self._run(
                self._oras_args(
                    "pull",
                    "--concurrency",
                    "1",
                    "--output",
                    str(temporary_root),
                    f"{self._repository()}@sha256:{storage.manifest_digest}",
                ),
                deadline,
                cancellation_signal,
            )
            if cancellation_signal is not None and cancellation_signal.is_set():
                raise OciRegistryError("cancelled")
            if time.monotonic() >= deadline:
                raise OciRegistryError("deadline-exhausted")
            downloaded = temporary_root / "artifact.bin"
            if (
                not downloaded.is_file()
                or downloaded.stat().st_size != reference.byte_size
            ):
                raise StoreIntegrityError(
                    f"OCI artifact byte size mismatch: {reference.digest}"
                )
            self._hash_download(
                downloaded, reference.digest, deadline, cancellation_signal
            )
            os.replace(downloaded, target)
        finally:
            shutil.rmtree(temporary_root)

    def _run_blob_push(
        self,
        path: Path,
        digest: str,
        deadline: float,
        cancellation_signal: Event | None,
    ) -> None:
        self._run(
            self._oras_args(
                "blob", "push", f"{self._repository()}@sha256:{digest}", str(path)
            ),
            deadline,
            cancellation_signal,
        )

    def _assert_manifest(
        self,
        source: str,
        expected: bytes,
        digest: str,
        deadline: float,
        cancellation_signal: Event | None,
    ) -> None:
        actual = self._run(
            self._oras_args("manifest", "fetch", source),
            deadline,
            cancellation_signal,
            output_limit_bytes=_MAX_MANIFEST_BYTES,
        )
        if actual != expected or hashlib.sha256(actual).hexdigest() != digest:
            raise StoreIntegrityError(f"OCI manifest verification failed: {digest}")

    @staticmethod
    def _assert_blob_descriptor(
        reference: ArtifactReferenceModel, payload: bytes
    ) -> None:
        try:
            descriptor = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise StoreIntegrityError(
                f"OCI blob descriptor is invalid: {reference.digest}"
            ) from error
        if not isinstance(descriptor, dict) or (
            descriptor.get("digest") != f"sha256:{reference.digest}"
            or descriptor.get("size") != reference.byte_size
        ):
            raise StoreIntegrityError(
                f"OCI blob descriptor mismatch: {reference.digest}"
            )

    def _hash_download(
        self,
        path: Path,
        expected_digest: str,
        deadline: float,
        cancellation_signal: Event | None,
    ) -> None:
        """Hash one downloaded regular file in a cancellable contained child."""

        code = (
            "import hashlib,pathlib,sys;"
            "path=pathlib.Path(sys.argv[1]);"
            "stream=path.open('rb');"
            "print(hashlib.file_digest(stream,'sha256').hexdigest())"
        )
        actual = (
            self._run(
                (sys.executable, "-I", "-c", code, str(path)),
                deadline,
                cancellation_signal,
            )
            .decode("ascii", errors="strict")
            .strip()
        )
        if actual != expected_digest:
            raise StoreIntegrityError(
                f"OCI artifact digest mismatch: {expected_digest}"
            )

    def _run(
        self,
        args: tuple[str, ...],
        deadline: float,
        cancellation_signal: Event | None,
        *,
        output_limit_bytes: int | None = None,
        missing_reference: str | None = None,
    ) -> bytes:
        if cancellation_signal is not None and cancellation_signal.is_set():
            raise OciRegistryError("cancelled")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise OciRegistryError("deadline-exhausted")
        root = Path(tempfile.mkdtemp(prefix="seqevi-oras-log-"))
        command = ToolCommand(args, root, root / "stdout", root / "stderr")
        try:
            try:
                result = self.runner.run(
                    command,
                    timeout_seconds=remaining,
                    cancellation_signal=cancellation_signal,
                    output_limit_bytes=(
                        _MAX_NATIVE_OUTPUT_BYTES
                        if output_limit_bytes is None
                        else output_limit_bytes
                    ),
                )
            except ToolCancelledError as error:
                raise OciRegistryError("cancelled") from error
            except ToolTimeoutError as error:
                raise OciRegistryError("deadline-exhausted") from error
            except ToolOutputLimitError as error:
                raise OciRegistryError("native-output-too-large") from error
            if result.return_code != 0:
                # ORAS 1.3.4 collapses zot's absent blob response into this exact
                # text. It permits an empty repository, not a proof of future
                # read/write authorization (a registry can conceal denial as 404).
                if (
                    missing_reference is not None
                    and result.stderr_path.read_text(
                        encoding="utf-8", errors="replace"
                    ).strip()
                    == f"Error response from registry: {missing_reference}: not found"
                ):
                    return b""
                # stderr is used only to avoid treating known authentication or
                # integrity failures as an ambiguous lost response; it never
                # crosses this boundary because it may carry registry details.
                category = self._command_failure_category(result.stderr_path)
                raise OciRegistryError(category)
            if time.monotonic() >= deadline:
                raise OciRegistryError("deadline-exhausted")
            return result.stdout_path.read_bytes()
        finally:
            shutil.rmtree(root)

    def _resolve_executable(self) -> Path:
        if self._resolved_executable is not None:
            return self._resolved_executable
        candidate = self.executable or (
            Path(found) if (found := shutil.which("oras")) else None
        )
        if candidate is None:
            raise StoreConfigurationError("OCI requires an ORAS executable")
        try:
            resolved = candidate.expanduser().resolve(strict=True)
        except OSError as error:
            raise StoreConfigurationError(
                "OCI ORAS executable is not executable"
            ) from error
        if not resolved.is_file() or not os.access(resolved, os.X_OK):
            raise StoreConfigurationError("OCI ORAS executable is not executable")
        self._resolved_executable = resolved
        return resolved

    def _oras_args(self, *args: str) -> tuple[str, ...]:
        executable = str(self._resolve_executable())
        file_args: list[str] = []
        if self.files.registry_config is not None:
            file_args.extend(("--registry-config", str(self.files.registry_config)))
        if self.files.ca_file is not None:
            file_args.extend(("--ca-file", str(self.files.ca_file)))
        if urlsplit(self.registry.endpoint).scheme == "http":
            file_args.append("--plain-http")
        # Cobra accepts flags after positional arguments. Append intact option
        # groups: single-level `pull` and two-level `blob fetch` differ in depth.
        return (executable, *args, *file_args)

    def _repository(self) -> str:
        parsed = urlsplit(self.registry.endpoint)
        assert parsed.netloc
        return f"{parsed.netloc}/{self.registry.repository}"

    @staticmethod
    def _command_failure_category(stderr_path: Path) -> str:
        detail = stderr_path.read_text(encoding="utf-8", errors="replace").lower()
        if any(
            token in detail
            for token in (
                "unauthorized",
                "authentication",
                "denied",
                "forbidden",
                "x509",
                "certificate",
                "tls",
            )
        ):
            return "authentication"
        if "blob unknown" in detail:
            return "blob-unknown"
        if "name unknown" in detail:
            return "name-unknown"
        if any(
            token in detail
            for token in (
                "checksum mismatch",
                "content verification failed",
                "invalid manifest",
            )
        ):
            return "integrity"
        return "command"

    @staticmethod
    def _reference(payload: ArtifactFile) -> ArtifactReferenceModel:
        from seqevi.store.transport import ArtifactReferenceModel

        return ArtifactReferenceModel(
            digest=payload.digest,
            media_type=payload.media_type,
            byte_size=payload.byte_size,
        )
