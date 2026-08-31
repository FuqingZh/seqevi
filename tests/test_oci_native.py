"""Opt-in native ORAS/zot validation for the R2 OCI boundary.

Select verified binaries with SEQEVI_TEST_ORAS and SEQEVI_TEST_ZOT.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import subprocess
import time
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import cast
from urllib.request import urlopen

import pytest
import httpx
from fastapi.testclient import TestClient

from seqevi.errors import StoreIntegrityError
from seqevi.evidence import (
    ArtifactFile,
    ArtifactLifetime,
    ClaimDisposition,
    CommitOutcome,
    EvidenceQuery,
)
from seqevi.service import ServiceSettings, create_service_app
from seqevi.service.persistence import PostgresEvidencePersistence
from seqevi.sequence import identify_protein_sequence
from seqevi.store import HttpEvidenceStore
from seqevi.store import client as client_module
from seqevi.store.oci import OciRegistryError, OciRegistry, manifest_digest
from seqevi.store.transport import (
    ArtifactReferenceModel,
    OciStorageReference,
    RegistryModel,
)
from tests.test_shared_store import _hit_commit, _isolated_postgres_url

ORAS = os.environ.get("SEQEVI_TEST_ORAS")
ZOT = os.environ.get("SEQEVI_TEST_ZOT")
if not ORAS or not ZOT:
    pytest.skip(
        "set SEQEVI_TEST_ORAS and SEQEVI_TEST_ZOT for native OCI validation",
        allow_module_level=True,
    )
ORAS_PATH = Path(ORAS).resolve()
ZOT_PATH = Path(ZOT).resolve()
if not ORAS_PATH.is_file() or not ZOT_PATH.is_file():
    pytest.skip("native OCI test binaries are unavailable", allow_module_level=True)


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _wait_ready(port: int, process: subprocess.Popen[bytes]) -> None:
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError("task-owned zot exited before readiness")
        try:
            with urlopen(f"http://127.0.0.1:{port}/v2/", timeout=0.2) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.05)
    raise RuntimeError("task-owned zot did not become ready")


@pytest.fixture(scope="module")
def native_registry(tmp_path_factory: pytest.TempPathFactory):
    root = tmp_path_factory.mktemp("seqevi-native-registry")
    port = _free_port()
    config = root / "zot.json"
    storage = root / "registry-root"
    config.write_text(
        json.dumps(
            {
                "distSpecVersion": "1.1.1",
                "storage": {
                    "rootDirectory": str(storage),
                    "commit": True,
                    "dedupe": True,
                    "gc": True,
                    "gcDelay": "1h",
                    "gcInterval": "1h",
                },
                "http": {"address": "127.0.0.1", "port": str(port)},
                "log": {"level": "warn"},
            }
        )
    )
    log = (root / "zot.log").open("wb")
    process = subprocess.Popen(
        (str(ZOT_PATH), "serve", str(config)),
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        _wait_ready(port, process)
        yield (
            root,
            storage,
            RegistryModel(
                id="native",
                endpoint=f"http://127.0.0.1:{port}",
                repository="seqevi/native",
            ),
        )
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        log.close()
        shutil.rmtree(root)


def _deadline(seconds: int = 120) -> float:
    requested = int(os.environ.get("SEQEVI_TEST_OCI_TIMEOUT_SECONDS", seconds))
    return time.monotonic() + min(max(requested, 1), 600)


def test_native_relative_path_has_same_manifest_as_absolute_path(
    native_registry, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, configured = native_registry
    monkeypatch.chdir(tmp_path)
    path = Path("relative artifact.bin")
    path.write_bytes(b"relative-path-native-regression")
    absolute = ArtifactFile.from_path(
        path.resolve(), "application/octet-stream", lifetime=ArtifactLifetime.CALLER
    )
    relative = replace(absolute, path=path)
    subject = OciRegistry(configured, executable=ORAS_PATH)
    relative_stored = subject.stage(relative, deadline=_deadline())
    absolute_stored = subject.stage(absolute, deadline=_deadline())
    assert relative_stored == absolute_stored
    assert relative.path == path


def _payload(root: Path, *, size: int = 512 * 1024 * 1024) -> ArtifactFile:
    path = root / "representative.bin"
    block = bytes(range(256)) * 4096
    with path.open("wb") as stream:
        for _ in range(size // len(block)):
            stream.write(block)
    return ArtifactFile.from_path(
        path, "application/x-seqevi", lifetime=ArtifactLifetime.CALLER
    )


@pytest.mark.timeout(180)
def test_native_oras_stage_verify_download_and_negative_controls(
    native_registry,
) -> None:
    root, storage_root, configured = native_registry
    subject = OciRegistry(configured, executable=ORAS_PATH)
    probe = subprocess.run(
        (
            str(ORAS_PATH),
            "blob",
            "fetch",
            "--plain-http",
            "--descriptor",
            f"{configured.endpoint.removeprefix('http://')}/{configured.repository}@sha256:{'0' * 64}",
        ),
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert probe.returncode != 0
    # zot/ORAS reduces the OCI BLOB_UNKNOWN response to a generic 404 string;
    # this is evidence that a missing-blob probe cannot distinguish hidden auth.
    assert probe.stderr.lower().endswith(b": not found\n")
    subject.preflight(deadline=_deadline())
    payload = _payload(root)
    stored = subject.stage(payload, deadline=_deadline(600))
    reference = ArtifactReferenceModel(
        digest=payload.digest,
        media_type=payload.media_type,
        byte_size=payload.byte_size,
    )
    storage = OciStorageReference(
        registry_id=stored.registry_id or "",
        repository=stored.repository or "",
        manifest_digest=stored.manifest_digest or "",
    )
    assert subject.verify(reference, storage, deadline=_deadline(600)) == stored
    destination = root / "downloaded.bin"
    subject.download(reference, storage, destination, deadline=_deadline(600))
    with destination.open("rb") as stream:
        assert hashlib.file_digest(stream, "sha256").hexdigest() == payload.digest
    different_media = ArtifactReferenceModel(
        digest=payload.digest,
        media_type="application/x-other",
        byte_size=payload.byte_size,
    )
    assert manifest_digest(reference) != manifest_digest(different_media)
    with pytest.raises(StoreIntegrityError):
        subject.verify(different_media, storage, deadline=_deadline())
    with pytest.raises(StoreIntegrityError):
        subject.verify(
            reference,
            OciStorageReference(
                registry_id="native",
                repository="wrong/repository",
                manifest_digest=storage.manifest_digest,
            ),
            deadline=_deadline(),
        )
    with pytest.raises(StoreIntegrityError):
        subject.verify(
            reference,
            OciStorageReference(
                registry_id="native",
                repository="seqevi/native",
                manifest_digest="f" * 64,
            ),
            deadline=_deadline(),
        )
    blob = next(path for path in storage_root.rglob(payload.digest) if path.is_file())
    with blob.open("r+b") as stream:
        first = stream.read(1)
        stream.seek(0)
        stream.write(b"\x00" if first != b"\x00" else b"\x01")
    refused = root / "refused.bin"
    with pytest.raises((StoreIntegrityError, OciRegistryError)):
        subject.download(reference, storage, refused, deadline=_deadline(600))
    assert not refused.exists()


@pytest.mark.requires_postgres
def test_native_postgres_claim_finalize_and_fetch(
    native_registry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, _storage_root, configured = native_registry
    auth_file = root / "anonymous.json"
    auth_file.write_text('{"auths":{}}')
    monkeypatch.setattr(
        client_module, "OciRegistry", partial(OciRegistry, executable=ORAS_PATH)
    )
    commit = _hit_commit(root / "claim-sources")
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        settings = ServiceSettings(
            database_url=database_url,
            artifacts_dir=root / "legacy-artifacts",
            artifact_backend="oci-registry",
            oci_registry_id=configured.id,
            oci_registry_endpoint=configured.endpoint,
            oci_registry_repository=configured.repository,
            oci_oras_executable=ORAS_PATH,
            oci_registry_config=auth_file,
        )
        app = create_service_app(settings, persistence=persistence)
        with (
            TestClient(app) as service,
            HttpEvidenceStore(
                "http://testserver",
                _client=cast(httpx.Client, service),
                _async_transport=httpx.ASGITransport(app=app),
            ) as store,
        ):
            with store.claim_session() as session:
                decisions = session.acquire_many(
                    (EvidenceQuery(commit.identity, commit.key),)
                )
                assert decisions[0].disposition is ClaimDisposition.ACQUIRED
                assert session.finalize_many((commit,)) == (CommitOutcome.CREATED,)
            fetched = store.fetch(commit.key)
            assert fetched is not None
            assert (
                fetched.normalized_artifact is not None
                and commit.normalized_artifact is not None
            )
            assert (
                fetched.normalized_artifact.path.read_bytes()
                == commit.normalized_artifact.path.read_bytes()
            )
            assert fetched.raw_artifact is not None and commit.raw_artifact is not None
            assert (
                fetched.raw_artifact.path.read_bytes()
                == commit.raw_artifact.path.read_bytes()
            )
            registered = persistence.artifact_metadata(commit.raw_artifact.digest)
            assert registered is not None and registered.storage_kind == "oci"
            assert registered.digest == commit.raw_artifact.digest
            assert registered.manifest_digest != registered.digest


@pytest.mark.requires_postgres
@pytest.mark.timeout(120)
def test_native_postgres_finalizes_representative_shared_artifact_batch(
    native_registry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, _storage_root, configured = native_registry
    auth_file = root / "representative-anonymous.json"
    auth_file.write_text('{"auths":{}}')
    monkeypatch.setattr(
        client_module, "OciRegistry", partial(OciRegistry, executable=ORAS_PATH)
    )
    template = _hit_commit(root / "representative-claim-sources")
    commits = tuple(
        replace(
            template,
            identity=(identity := identify_protein_sequence("M" + "A" * (index + 1))),
            key=replace(template.key, sequence_id=identity.sequence_id),
        )
        for index in range(1000)
    )
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        settings = ServiceSettings(
            database_url=database_url,
            artifacts_dir=root / "representative-legacy-artifacts",
            artifact_backend="oci-registry",
            oci_registry_id=configured.id,
            oci_registry_endpoint=configured.endpoint,
            oci_registry_repository=configured.repository,
            oci_oras_executable=ORAS_PATH,
            oci_registry_config=auth_file,
        )
        app = create_service_app(settings, persistence=persistence)
        with (
            TestClient(app) as service,
            HttpEvidenceStore(
                "http://testserver",
                _client=cast(httpx.Client, service),
                _async_transport=httpx.ASGITransport(app=app),
            ) as store,
        ):
            with store.claim_session() as session:
                decisions = session.acquire_many(
                    EvidenceQuery(commit.identity, commit.key) for commit in commits
                )
                assert all(
                    decision.disposition is ClaimDisposition.ACQUIRED
                    for decision in decisions
                )
                started = time.monotonic()
                outcomes = session.finalize_many(commits)
                elapsed = time.monotonic() - started
            assert outcomes == (CommitOutcome.CREATED,) * 1000
            assert elapsed < 30.0
            with persistence.engine.connect() as connection:
                assert (
                    connection.exec_driver_sql(
                        "SELECT count(*) FROM evidence"
                    ).scalar_one()
                    == 1000
                )
                assert (
                    connection.exec_driver_sql(
                        "SELECT count(*) FROM artifact"
                    ).scalar_one()
                    == 2
                )
