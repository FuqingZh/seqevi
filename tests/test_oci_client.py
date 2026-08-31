"""Contract tests for OCI client wiring.

These intentionally exercise only the public HTTP seams plus a fake OciRegistry:
no ORAS binary, Registry, or service process is required.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from seqevi.errors import StoreConfigurationError, StoreError
from seqevi.evidence import (
    ArtifactFile,
    ArtifactLifetime,
    EvidenceCommit,
    EvidenceKey,
    EvidenceStatus,
    StoredArtifact,
)
from seqevi.sequence import SequenceIdentity, ga4gh_sequence_id
from seqevi.store import client as client_module
from seqevi.store.client import HttpEvidenceStore
from seqevi.store.factory import open_evidence_store
from seqevi.store.oci import OciClientFiles


_HEALTH = {
    "status": "ok",
    "api_version": "v1",
    "maximum_batch_size": 100,
    "maximum_artifact_bytes": 1024 * 1024,
}
_CAPABILITIES = {
    "protocol": "claim-session-v1",
    "maximum_batch_size": 100,
    "retention_seconds": 60,
    "maximum_session_receipt_headers": 1000,
    "maximum_session_receipt_items": 32000,
    "server_time": datetime.now(UTC).isoformat(),
}
_DISCOVERY = {
    "protocol": "storage-discovery-v1",
    "artifact_backend": "oci-registry",
    "minimum_client_capability": "oci-artifacts-v1",
    "registry": {
        "id": "primary",
        "endpoint": "https://registry.example.test",
        "repository": "seqevi/artifacts",
    },
}


class _FakeRegistry:
    instances: list[_FakeRegistry] = []

    def __init__(self, registry, *, files=None, **_kwargs) -> None:
        self.registry = registry
        self.files = files
        self.preflight_calls = 0
        self.stage_calls: list[str] = []
        self.verify_calls: list[str] = []
        self.download_calls: list[str] = []
        self.__class__.instances.append(self)

    def preflight(self, **_kwargs) -> None:
        self.preflight_calls += 1

    def stage(self, artifact: ArtifactFile, **_kwargs) -> StoredArtifact:
        self.stage_calls.append(artifact.digest)
        return StoredArtifact(
            digest=artifact.digest,
            media_type=artifact.media_type,
            byte_size=artifact.byte_size,
            relative_path=None,
            storage_kind="oci",
            registry_id="primary",
            repository="seqevi/artifacts",
            manifest_digest="a" * 64,
        )

    def verify(self, artifact, storage, **_kwargs) -> StoredArtifact:
        self.verify_calls.append(artifact.digest)
        return StoredArtifact(
            digest=artifact.digest,
            media_type=artifact.media_type,
            byte_size=artifact.byte_size,
            relative_path=None,
            storage_kind="oci",
            registry_id=storage.registry_id,
            repository=storage.repository,
            manifest_digest=storage.manifest_digest,
        )

    def download(self, artifact, _storage, target: Path, **_kwargs) -> None:
        self.download_calls.append(artifact.digest)
        target.write_bytes(b"oci-download")


def _artifact(tmp_path: Path, name: str, contents: bytes = b"payload") -> ArtifactFile:
    path = tmp_path / name
    path.write_bytes(contents)
    return ArtifactFile(
        path=path,
        media_type="application/octet-stream",
        byte_size=len(contents),
        digest=hashlib.sha256(contents).hexdigest(),
        lifetime=ArtifactLifetime.CALLER,
    )


def _commit(tmp_path: Path) -> EvidenceCommit:
    raw = _artifact(tmp_path, "raw", b"raw")
    normalized = _artifact(tmp_path, "normalized", b"normalized")
    sequence = "MOCIWIRE"
    identity = SequenceIdentity(
        sequence_id=ga4gh_sequence_id(sequence),
        md5=hashlib.md5(sequence.encode("ascii"), usedforsecurity=False).hexdigest(),
        length=len(sequence),
        sequence=sequence,
    )
    key = EvidenceKey(
        sequence_id=identity.sequence_id,
        adapter_contract_version="adapter-v1",
        tool_runtime_digest="sha256:tool",
        resource_id="resource-v1",
        semantic_parameters_json="{}",
    )
    return EvidenceCommit(
        identity=identity,
        key=key,
        status=EvidenceStatus.HIT,
        payload_digest=hashlib.sha256(b"payload").hexdigest(),
        normalized_artifact=normalized,
        raw_artifact=raw,
    )


def _transport(
    *,
    discovery: dict[str, Any] | None = _DISCOVERY,
    discovery_status: int | None = None,
    discovered_artifacts: dict[str, dict[str, Any]] | None = None,
    seen: list[httpx.Request] | None = None,
) -> httpx.MockTransport:
    known = discovered_artifacts or {}

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        path = request.url.path
        if path == "/health":
            return httpx.Response(200, json=_HEALTH, request=request)
        if path == "/v1/storage/discovery":
            if discovery_status is not None:
                return httpx.Response(
                    discovery_status, json={"detail": "denied"}, request=request
                )
            if discovery is None:
                return httpx.Response(404, request=request)
            return httpx.Response(200, json=discovery, request=request)
        if path == "/v1/internal/claim-sessions/capabilities":
            return httpx.Response(200, json=_CAPABILITIES, request=request)
        if path == "/v1/evidence/lookup":
            return httpx.Response(200, json={"records": []}, request=request)
        if path == "/v1/artifacts/resolve":
            digest = json.loads(request.content)["digest"]
            return httpx.Response(
                200, json={"artifact": known.get(digest)}, request=request
            )
        if path == "/v1/evidence/commit":
            commits = json.loads(request.content)["commits"]
            return httpx.Response(
                200, json={"outcomes": ["created"] * len(commits)}, request=request
            )
        if path.startswith("/v1/artifacts/"):
            raise AssertionError("OCI artifact download must not proxy through Store")
        raise AssertionError(f"unexpected request: {request.method} {path}")

    return httpx.MockTransport(handler)


@pytest.fixture(autouse=True)
def _fake_oci_registry(monkeypatch: pytest.MonkeyPatch):
    _FakeRegistry.instances.clear()
    monkeypatch.setattr(client_module, "OciRegistry", _FakeRegistry)


def _store(transport: httpx.MockTransport) -> HttpEvidenceStore:
    return HttpEvidenceStore(
        "https://store.example.test",
        _client=httpx.Client(
            transport=transport, base_url="https://store.example.test"
        ),
        _async_transport=transport,
    )


def test_oci_discovery_preflights_and_sends_capability_on_sync_and_async_requests():
    seen: list[httpx.Request] = []
    with _store(_transport(seen=seen)) as store:
        assert _FakeRegistry.instances[0].preflight_calls == 1
        store._request("POST", "/v1/evidence/lookup", json={"queries": []})
        # This is asynchronous ClaimSession transport under the client runtime.
        store._resolve_artifact("0" * 64, deadline=10**12)

    capability_paths = {
        request.url.path
        for request in seen
        if request.url.path
        in {
            "/v1/internal/claim-sessions/capabilities",
            "/v1/artifacts/resolve",
            "/v1/evidence/lookup",
        }
    }
    assert capability_paths == {
        "/v1/internal/claim-sessions/capabilities",
        "/v1/artifacts/resolve",
        "/v1/evidence/lookup",
    }
    for request in seen:
        if request.url.path in capability_paths:
            assert request.headers["X-SeqEvi-Client-Capability"] == "oci-artifacts-v1"


def test_exact_discovery_404_after_health_is_legacy_fallback_not_oci():
    with _store(_transport(discovery=None)) as store:
        assert store._registry is None
    assert _FakeRegistry.instances == []


@pytest.mark.parametrize(
    "discovery",
    [
        {"artifact_backend": "oci-registry"},  # malformed/missing required fields
        {
            "protocol": "storage-discovery-v1",
            "artifact_backend": "legacy-posix",
            "registry": _DISCOVERY["registry"],
        },
    ],
)
def test_malformed_discovery_does_not_fall_back_to_legacy(discovery):
    with pytest.raises(Exception):
        _store(_transport(discovery=discovery))
    assert _FakeRegistry.instances == []


def test_discovery_auth_failure_does_not_fall_back_to_legacy() -> None:
    with pytest.raises(StoreError, match="HTTP 401"):
        _store(_transport(discovery_status=401))
    assert _FakeRegistry.instances == []


def test_oci_staging_reuses_registered_posix_and_commits_new_oci_reference(
    tmp_path: Path,
) -> None:
    legacy = _artifact(tmp_path, "legacy")
    new_commit = _commit(tmp_path)
    assert new_commit.raw_artifact is not None
    known = {
        legacy.digest: {
            "digest": legacy.digest,
            "media_type": legacy.media_type,
            "byte_size": legacy.byte_size,
            "storage_reference": {"kind": "posix", "relative_path": "legacy/blob"},
        }
    }
    seen: list[httpx.Request] = []
    with _store(_transport(discovered_artifacts=known, seen=seen)) as store:
        store._stage_oci(
            legacy,
            deadline=10**12,
            cancellation_signal=store._closing,
        )
        assert _FakeRegistry.instances[0].stage_calls == []
        assert store.commit_many((new_commit,)) == ("created",)

    registry = _FakeRegistry.instances[0]
    assert new_commit.raw_artifact is not None
    assert new_commit.normalized_artifact is not None
    assert sorted(registry.stage_calls) == sorted(
        [new_commit.raw_artifact.digest, new_commit.normalized_artifact.digest]
    )
    commit_body = next(
        json.loads(request.content)
        for request in seen
        if request.url.path == "/v1/evidence/commit"
    )
    for name in ("raw_artifact", "normalized_artifact"):
        reference = commit_body["commits"][0][name]
        assert reference["storage_reference"]["kind"] == "oci"
        assert reference["storage_reference"]["manifest_digest"] == "a" * 64


def test_oci_download_uses_registry_not_store_artifact_proxy(tmp_path: Path) -> None:
    digest = hashlib.sha256(b"oci-download").hexdigest()
    known = {
        digest: {
            "digest": digest,
            "media_type": "application/octet-stream",
            "byte_size": len(b"oci-download"),
            "storage_reference": {
                "kind": "oci",
                "registry_id": "primary",
                "repository": "seqevi/artifacts",
                "manifest_digest": "b" * 64,
            },
        }
    }
    with _store(_transport(discovered_artifacts=known)) as store:
        artifact = store._download(digest)
        assert artifact.path.read_bytes() == b"oci-download"
    assert _FakeRegistry.instances[0].download_calls == [digest]


def test_oci_staging_honors_store_cancellation_before_registry_work(
    tmp_path: Path,
) -> None:
    artifact = _artifact(tmp_path, "cancelled")
    with _store(_transport()) as store:
        store._closing.set()
        with pytest.raises(StoreError, match="staging cancelled"):
            store._stage_oci(
                artifact,
                deadline=10**12,
                cancellation_signal=store._closing,
            )
    registry = _FakeRegistry.instances[0]
    assert registry.stage_calls == []
    assert registry.verify_calls == []


def test_discovery_transport_failure_is_safe_store_error_without_fallback() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/health":
            return httpx.Response(200, json=_HEALTH)
        assert request.url.path == "/v1/storage/discovery"
        raise httpx.ConnectError("private endpoint credential=secret", request=request)

    with pytest.raises(StoreError, match="storage discovery request failed") as raised:
        _store(httpx.MockTransport(handler))
    assert "secret" not in str(raised.value)
    assert _FakeRegistry.instances == []


def test_local_store_rejects_oci_file_options_without_constructing_oras(
    tmp_path: Path,
) -> None:
    config = tmp_path / "registry.json"
    config.write_text('{"auths": {}}', encoding="utf-8")
    with pytest.raises(StoreConfigurationError, match="OCI file inputs"):
        with open_evidence_store(
            tmp_path / "local", oci_files=OciClientFiles(registry_config=config)
        ):
            pass
