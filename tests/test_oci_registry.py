from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import pytest

from seqevi.errors import StoreConfigurationError, StoreIntegrityError
from seqevi.evidence import ArtifactFile, ArtifactLifetime
from seqevi.runner import ToolRunResult, ToolRunner
from seqevi.store.oci import (
    OciClientFiles,
    OciRegistry,
    canonical_manifest,
    manifest_digest,
)
from seqevi.store.transport import (
    ArtifactReferenceModel,
    OciStorageReference,
    RegistryModel,
)


class RecordingRunner(ToolRunner):
    def __init__(
        self, manifests: dict[str, bytes] | None = None, *, version: str = "1.3.4"
    ) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.manifests = manifests or {}
        self.version = version
        self.return_code = 0
        self.stderr = b""
        self.roots: list[Path] = []

    def run(
        self,
        command,
        *,
        timeout_seconds=None,
        cancellation_signal=None,
        output_limit_bytes=None,
    ):
        self.commands.append(command.arguments)
        self.roots.append(command.working_dir)
        command.stdout_path.parent.mkdir(parents=True, exist_ok=True)
        if "--descriptor" in command.arguments:
            command.stdout_path.write_text(
                json.dumps(
                    {
                        "mediaType": "application/octet-stream",
                        "digest": "sha256:" + "a" * 64,
                        "size": 7,
                    }
                )
            )
        elif "manifest" in command.arguments and "fetch" in command.arguments:
            command.stdout_path.write_bytes(next(iter(self.manifests.values())))
        elif "pull" in command.arguments:
            output = Path(command.arguments[command.arguments.index("--output") + 1])
            output.mkdir(parents=True, exist_ok=True)
            (output / "artifact.bin").write_bytes(b"corrupt")
            command.stdout_path.write_bytes(b"")
        else:
            command.stdout_path.write_text(f"Version: {self.version}\n")
        command.stderr_path.write_bytes(self.stderr)
        return ToolRunResult(
            command.arguments,
            self.return_code,
            __import__("datetime").datetime.now(__import__("datetime").UTC),
            __import__("datetime").datetime.now(__import__("datetime").UTC),
            0,
            command.stdout_path,
            command.stderr_path,
        )


def reference() -> ArtifactReferenceModel:
    return ArtifactReferenceModel(
        digest="a" * 64, media_type="application/x-seqevi", byte_size=7
    )


def test_stage_resolves_relative_payload_before_changing_child_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    path = Path("relative artifact.bin")
    path.write_bytes(b"payload")
    payload = ArtifactFile(
        path=path,
        digest=hashlib.sha256(b"payload").hexdigest(),
        media_type="application/octet-stream",
        byte_size=7,
        lifetime=ArtifactLifetime.CALLER,
    )
    expected = canonical_manifest(
        ArtifactReferenceModel(
            digest=payload.digest, media_type=payload.media_type, byte_size=7
        )
    )
    runner = RecordingRunner({"expected": expected})
    subject = registry(tmp_path, runner)
    subject.stage(payload, deadline=time.monotonic() + 5)
    pushes = [args for args in runner.commands if args[1:3] == ("blob", "push")]
    assert pushes[1][-1] == str(path.resolve())
    assert payload.path == path


def test_stage_missing_file_fails_before_any_registry_write(tmp_path: Path) -> None:
    path = tmp_path / "gone.bin"
    path.write_bytes(b"payload")
    payload = ArtifactFile.from_path(path, "application/octet-stream")
    path.unlink()
    runner = RecordingRunner()
    subject = registry(tmp_path, runner)
    with pytest.raises(FileNotFoundError):
        subject.stage(payload, deadline=time.monotonic() + 5)
    assert runner.commands == []


def registry(tmp_path: Path, runner: RecordingRunner) -> OciRegistry:
    oras = tmp_path / "oras"
    oras.write_text("fixture")
    oras.chmod(0o700)
    return OciRegistry(
        RegistryModel(
            id="primary",
            endpoint="https://registry.example",
            repository="seqevi/artifacts",
        ),
        executable=oras,
        runner=runner,
    )


def test_canonical_manifest_is_stable_and_uses_manifest_retention_tag() -> None:
    body = canonical_manifest(reference())
    decoded = json.loads(body)
    digest = manifest_digest(reference())
    assert body == canonical_manifest(reference())
    assert hashlib.sha256(body).hexdigest() == digest
    assert decoded["layers"] == [
        {
            "annotations": {"org.opencontainers.image.title": "artifact.bin"},
            "digest": "sha256:" + "a" * 64,
            "mediaType": "application/x-seqevi",
            "size": 7,
        }
    ]
    assert f"sha256-{digest}" != f"sha256-{'a' * 64}"


def test_verify_requires_exact_registry_reference_and_canonical_readback(
    tmp_path: Path,
) -> None:
    expected = canonical_manifest(reference())
    runner = RecordingRunner({"expected": expected})
    subject = registry(tmp_path, runner)
    storage = OciStorageReference(
        kind="oci",
        registry_id="primary",
        repository="seqevi/artifacts",
        manifest_digest=manifest_digest(reference()),
    )
    stored = subject.verify(reference(), storage, deadline=time.monotonic() + 5)
    assert stored.storage_kind == "oci"
    assert [command[1:3] for command in runner.commands] == [
        ("manifest", "fetch"),
        ("manifest", "fetch"),
        ("blob", "fetch"),
    ]
    assert any(
        any(f"sha256-{storage.manifest_digest}" in item for item in command)
        for command in runner.commands
    )


def test_verify_rejects_wrong_manifest_or_storage_target(tmp_path: Path) -> None:
    runner = RecordingRunner({"bad": b"{}"})
    subject = registry(tmp_path, runner)
    storage = OciStorageReference(
        kind="oci",
        registry_id="other",
        repository="seqevi/artifacts",
        manifest_digest=manifest_digest(reference()),
    )
    with pytest.raises(StoreIntegrityError):
        subject.verify(reference(), storage, deadline=time.monotonic() + 5)


def test_headless_config_rejects_credential_helper(tmp_path: Path) -> None:
    config = tmp_path / "config.json"
    config.write_text('{"credsStore":"secretservice"}')
    with pytest.raises(StoreConfigurationError):
        OciClientFiles(registry_config=config).validated(headless=True)


def test_preflight_requires_exact_oras_version(tmp_path: Path) -> None:
    runner = RecordingRunner(version="1.3.40")
    subject = registry(tmp_path, runner)
    with pytest.raises(StoreConfigurationError):
        subject.preflight(deadline=time.monotonic() + 5)


def test_preflight_accepts_the_exact_real_version_line(tmp_path: Path) -> None:
    runner = RecordingRunner(version="1.3.4")
    subject = registry(tmp_path, runner)
    subject.preflight(deadline=time.monotonic() + 5)


def test_blob_descriptor_does_not_invent_artifact_media_type(tmp_path: Path) -> None:
    expected = canonical_manifest(reference())
    runner = RecordingRunner({"expected": expected})
    subject = registry(tmp_path, runner)
    storage = OciStorageReference(
        kind="oci",
        registry_id="primary",
        repository="seqevi/artifacts",
        manifest_digest=manifest_digest(reference()),
    )
    assert (
        subject.verify(reference(), storage, deadline=time.monotonic() + 5).media_type
        == "application/x-seqevi"
    )


def test_download_rejects_same_size_corrupt_bytes_before_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = canonical_manifest(reference())
    runner = RecordingRunner({"expected": expected})
    subject = registry(tmp_path, runner)
    storage = OciStorageReference(
        kind="oci",
        registry_id="primary",
        repository="seqevi/artifacts",
        manifest_digest=manifest_digest(reference()),
    )
    target = tmp_path / "result.bin"

    def reject_corrupt(path, expected_digest, deadline, cancellation_signal):
        assert path.read_bytes() == b"corrupt"
        raise StoreIntegrityError(f"OCI artifact digest mismatch: {expected_digest}")

    monkeypatch.setattr(subject, "_hash_download", reject_corrupt)
    with pytest.raises(StoreIntegrityError, match="digest mismatch"):
        subject.download(reference(), storage, target, deadline=time.monotonic() + 5)
    assert not target.exists()


def test_registry_reference_strips_https_and_only_loopback_http_enables_flag(
    tmp_path: Path,
) -> None:
    runner = RecordingRunner()
    secure = registry(tmp_path, runner)
    assert secure._repository() == "registry.example/seqevi/artifacts"
    assert "--plain-http" not in secure._oras_args("blob", "fetch", "repo")
    loopback = OciRegistry(
        RegistryModel(
            id="primary",
            endpoint="http://127.0.0.1:5000",
            repository="seqevi/artifacts",
        ),
        executable=tmp_path / "oras",
        runner=runner,
    )
    assert loopback._repository() == "127.0.0.1:5000/seqevi/artifacts"
    assert "--plain-http" in loopback._oras_args("blob", "fetch", "repo")


def test_pre_cancelled_preflight_never_spawns(tmp_path: Path) -> None:
    runner = RecordingRunner()
    subject = registry(tmp_path, runner)
    cancellation = __import__("threading").Event()
    cancellation.set()
    with pytest.raises(Exception, match="cancelled"):
        subject.preflight(
            deadline=time.monotonic() + 5, cancellation_signal=cancellation
        )
    assert runner.commands == []


def test_unknown_command_cleanup_and_unknown_stderr_remain_ambiguous(
    tmp_path: Path,
) -> None:
    runner = RecordingRunner()
    runner.return_code = 1
    runner.stderr = b"upload endpoint contained sha256 digest parameter"
    subject = registry(tmp_path, runner)
    with pytest.raises(Exception, match="command"):
        subject._run(
            (str(tmp_path / "oras"), "manifest", "push"), time.monotonic() + 5, None
        )
    assert all(not root.exists() for root in runner.roots)
