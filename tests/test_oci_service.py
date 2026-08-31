"""Focused R2 service-contract tests for OCI storage admission.

All Registry operations are mocked; no binary or deployment is exercised here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable, NoReturn

import pytest
from fastapi.testclient import TestClient

from seqevi.errors import StoreError, StoreIntegrityError
from seqevi.evidence import (
    ClaimSessionAuthority,
    CommitOutcome,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    EvidenceStatus,
    SessionEvidenceClaim,
    StoredArtifact,
)
from seqevi.sequence import identify_protein_sequence
from seqevi.service import ServiceSettings, create_service_app
from seqevi.service import app as app_module
from seqevi.store.transport import (
    ArtifactReferenceModel,
    ClaimSessionFinalizeItem,
    ClaimSessionFinalizeRequest,
    CommitModel,
    CommitRequest,
    EvidenceKeyModel,
    OciStorageReference,
    OCI_CAPABILITY_HEADER,
    OCI_CLIENT_CAPABILITY,
    SequenceModel,
)


class _Persistence:
    """Small observation-only ServicePersistence substitute."""

    def __init__(self) -> None:
        self.lookup_calls = 0
        self.commit_calls = 0
        self.finalize_calls = 0
        self.artifacts: dict[str, StoredArtifact] = {}

    @property
    def supports_claim_sessions(self) -> bool:
        return True

    def lookup_many(
        self, queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        del queries
        self.lookup_calls += 1
        return {}

    def commit_many(
        self,
        commits: Iterable[CommitModel],
        stored_artifacts: dict[str, StoredArtifact],
        *,
        deadline: float | None = None,
    ) -> tuple[CommitOutcome, ...]:
        del commits, deadline
        self.commit_calls += 1
        self.artifacts.update(stored_artifacts)
        return (CommitOutcome.CREATED,)

    def artifact_metadata(self, digest: str) -> StoredArtifact | None:
        return self.artifacts.get(digest)

    def fetch_record(self, key: EvidenceKey) -> EvidenceRecord | None:
        del key
        return None

    def fetch_many(
        self, keys: Iterable[EvidenceKey]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        del keys
        return {}

    def database_time(self) -> datetime:
        return datetime.now(UTC)

    # These must never run in the pre-admission tests.
    def open_claim_session(
        self,
        *,
        open_request_id: str,
        server_time: datetime,
        open_not_after: datetime,
    ) -> NoReturn:
        del open_request_id, server_time, open_not_after
        raise AssertionError("claim session opened before OCI admission")

    def renew_claim_session(self, authority: ClaimSessionAuthority) -> NoReturn:
        del authority
        raise AssertionError("claim session renewed before OCI admission")

    def close_claim_session(self, authority: ClaimSessionAuthority) -> None:
        del authority
        return None

    def acquire_claim_session(
        self,
        authority: ClaimSessionAuthority,
        *,
        acquire_request_id: str,
        query_digest: str,
        queries: Iterable[EvidenceQuery],
    ) -> NoReturn:
        del authority, acquire_request_id, query_digest, queries
        raise AssertionError("claim acquired before OCI admission")

    def finalize_claim_session(
        self,
        authority: ClaimSessionAuthority,
        commits: Iterable[ClaimSessionFinalizeItem],
        stored_artifacts: dict[str, StoredArtifact],
        *,
        deadline: float | None = None,
    ) -> tuple[CommitOutcome, ...]:
        del authority, commits, stored_artifacts, deadline
        self.finalize_calls += 1
        return ()

    def claim_session_authority_is_live(
        self,
        authority: ClaimSessionAuthority,
        claims: Iterable[SessionEvidenceClaim],
    ) -> bool:
        del authority, claims
        return False

    def sweep_claim_sessions(self) -> int:
        return 0

    def close(self) -> None:
        return None


class _Files:
    def __init__(self, *_args: Path | None) -> None:
        pass

    def validated(self, *, headless: bool) -> _Files:
        assert headless
        return self


class _Registry:
    verify_error: Exception | None = None
    instances: list["_Registry"] = []

    def __init__(
        self, definition: object, *, executable: Path | None, files: object
    ) -> None:
        self.definition = definition
        self.executable = executable
        self.files = files
        self.preflight_calls = 0
        self.verify_calls = 0
        type(self).instances.append(self)

    def preflight(self, *, deadline: float, cancellation_signal: object) -> None:
        assert deadline > 0
        assert hasattr(cancellation_signal, "is_set")
        self.preflight_calls += 1

    def verify(
        self,
        reference: ArtifactReferenceModel,
        location: OciStorageReference,
        *,
        deadline: float,
        cancellation_signal: object,
    ) -> StoredArtifact:
        assert deadline > 0
        assert hasattr(cancellation_signal, "is_set")
        self.verify_calls += 1
        if self.verify_error is not None:
            raise self.verify_error
        return StoredArtifact(
            digest=reference.digest,
            media_type=reference.media_type,
            byte_size=reference.byte_size,
            relative_path=None,
            storage_kind="oci",
            registry_id=location.registry_id,
            repository=location.repository,
            manifest_digest=location.manifest_digest,
        )


def _oci_settings(tmp_path: Path) -> ServiceSettings:
    return ServiceSettings(
        database_url="postgresql+psycopg://unused/seqevi",
        artifacts_dir=tmp_path / "artifacts",
        artifact_backend="oci-registry",
        oci_registry_id="team-cache",
        oci_registry_endpoint="https://registry.example.test",
        oci_registry_repository="seqevi/evidence",
        oci_oras_executable=tmp_path / "oras",
        oci_registry_config=tmp_path / "registry-auth.json",
    )


def _legacy_settings(tmp_path: Path) -> ServiceSettings:
    return ServiceSettings(
        database_url="postgresql+psycopg://unused/seqevi",
        artifacts_dir=tmp_path / "artifacts",
    )


def _oci_headers() -> dict[str, str]:
    return {OCI_CAPABILITY_HEADER: OCI_CLIENT_CAPABILITY}


def _commit_model() -> CommitModel:
    identity = identify_protein_sequence("MOCIADMISSION")
    key = EvidenceKey.from_parameters(
        sequence_id=identity.sequence_id,
        adapter_contract_version="fixture/1",
        tool_runtime_digest="sha256:" + "a" * 64,
        resource_id="fixture/1",
        semantic_parameters={"threshold": 0.01},
    )
    artifact = ArtifactReferenceModel(
        digest="b" * 64,
        media_type="text/plain",
        byte_size=7,
        storage_reference=OciStorageReference(
            registry_id="team-cache",
            repository="seqevi/evidence",
            manifest_digest="c" * 64,
        ),
    )
    model = CommitModel(
        identity=SequenceModel.from_domain(identity),
        key=EvidenceKeyModel.from_domain(key),
        status=EvidenceStatus.NO_HIT,
        payload_digest="d" * 64,
        normalized_artifact=None,
        raw_artifact=artifact,
    )
    return model


def _commit_payload() -> dict[str, object]:
    return CommitRequest(commits=[_commit_model()]).model_dump(mode="json")


def _finalize_payload() -> dict[str, object]:
    return ClaimSessionFinalizeRequest(
        session_id="session",
        owner_token="owner",
        generation=1,
        finalize_request_id="finalize-oci-test",
        commits=[ClaimSessionFinalizeItem(commit=_commit_model(), claim_generation=1)],
    ).model_dump(mode="json")


@pytest.fixture
def oci_service(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    _Registry.instances.clear()
    _Registry.verify_error = None
    monkeypatch.setattr(app_module, "OciClientFiles", _Files)
    monkeypatch.setattr(app_module, "OciRegistry", _Registry)
    persistence = _Persistence()
    app = create_service_app(_oci_settings(tmp_path), persistence=persistence)
    return app, persistence


def test_oci_discovery_is_credential_free_and_preflights_registry(oci_service) -> None:
    app, _persistence = oci_service
    with TestClient(app) as client:
        health = client.get("/health")
        discovery = client.get("/v1/storage/discovery")

    assert health.status_code == 200
    assert discovery.status_code == 200
    assert discovery.json() == {
        "protocol": "storage-discovery-v1",
        "artifact_backend": "oci-registry",
        "minimum_client_capability": OCI_CLIENT_CAPABILITY,
        "registry": {
            "id": "team-cache",
            "endpoint": "https://registry.example.test",
            "repository": "seqevi/evidence",
        },
    }
    assert len(_Registry.instances) == 1
    assert _Registry.instances[0].preflight_calls == 1
    assert "registry-auth" not in discovery.text
    assert "oras" not in discovery.text


def test_oci_mode_rejects_legacy_data_routes_before_persistence(oci_service) -> None:
    app, persistence = oci_service
    digest = "a" * 64
    with TestClient(app) as client:
        # This is the old-client shape: health is fine, but the first lookup is
        # rejected before a ClaimSession or external annotation could be reached.
        assert client.get("/health").status_code == 200
        responses = (
            client.post("/v1/evidence/lookup", json={"queries": []}),
            client.post("/v1/evidence/fetch-many", json={"keys": []}),
            client.post("/v1/evidence/commit", json={"commits": []}),
            client.post("/v1/artifacts/resolve", json={"digest": digest}),
            client.get(f"/v1/artifacts/{digest}"),
            client.put(
                f"/v1/artifacts/{digest}",
                content=b"x",
                headers={
                    "X-Artifact-Media-Type": "text/plain",
                    "X-Artifact-Byte-Size": "1",
                },
            ),
            client.get("/v1/internal/claim-sessions/capabilities"),
        )

    assert all(response.status_code == 426 for response in responses)
    assert all(
        response.headers["Upgrade"] == OCI_CLIENT_CAPABILITY for response in responses
    )
    assert persistence.lookup_calls == 0
    assert persistence.commit_calls == 0
    assert persistence.finalize_calls == 0


def test_oci_capable_lookup_is_admitted_after_header_check(oci_service) -> None:
    app, persistence = oci_service
    with TestClient(app) as client:
        response = client.post(
            "/v1/evidence/lookup", json={"queries": []}, headers=_oci_headers()
        )

    assert response.status_code == 200
    assert response.json() == {"records": []}
    assert persistence.lookup_calls == 1


def test_oci_resolve_exposes_raw_digest_and_typed_registry_location(
    oci_service,
) -> None:
    app, persistence = oci_service
    digest = "a" * 64
    persistence.artifacts[digest] = StoredArtifact(
        digest=digest,
        media_type="text/plain",
        byte_size=7,
        relative_path=None,
        storage_kind="oci",
        registry_id="team-cache",
        repository="seqevi/evidence",
        manifest_digest="b" * 64,
    )
    with TestClient(app) as client:
        response = client.post(
            "/v1/artifacts/resolve", json={"digest": digest}, headers=_oci_headers()
        )

    assert response.status_code == 200
    assert response.json() == {
        "artifact": {
            "digest": digest,
            "media_type": "text/plain",
            "byte_size": 7,
            "storage_reference": {
                "kind": "oci",
                "registry_id": "team-cache",
                "repository": "seqevi/evidence",
                "manifest_digest": "b" * 64,
            },
        }
    }


@pytest.mark.parametrize(
    ("verify_error", "status_code"),
    [
        (StoreIntegrityError("digest mismatch"), 422),
        (StoreError("Registry authorization failed"), 503),
        (StoreError("OCI metadata verification deadline expired"), 503),
    ],
)
def test_oci_finalize_verifies_before_database_finalization(
    oci_service, verify_error: Exception, status_code: int
) -> None:
    app, persistence = oci_service
    _Registry.verify_error = verify_error
    with TestClient(app) as client:
        response = client.post(
            "/v1/internal/claim-sessions/finalize",
            json=_finalize_payload(),
            headers=_oci_headers(),
        )

    assert response.status_code == status_code
    assert persistence.commit_calls == 0
    assert persistence.finalize_calls == 0
    assert len(_Registry.instances) == 1
    assert _Registry.instances[0].verify_calls == 1


def test_oci_direct_put_is_rejected_even_for_capable_client(oci_service) -> None:
    app, persistence = oci_service
    with TestClient(app) as client:
        response = client.put(
            "/v1/artifacts/" + "a" * 64,
            content=b"payload",
            headers={
                **_oci_headers(),
                "X-Artifact-Media-Type": "text/plain",
                "X-Artifact-Byte-Size": "7",
            },
        )

    assert response.status_code == 409
    assert persistence.commit_calls == 0


def test_legacy_service_keeps_unheadered_lookup_and_posix_discovery(
    tmp_path: Path,
) -> None:
    persistence = _Persistence()
    app = create_service_app(_legacy_settings(tmp_path), persistence=persistence)
    with TestClient(app) as client:
        discovery = client.get("/v1/storage/discovery")
        lookup = client.post("/v1/evidence/lookup", json={"queries": []})

    assert discovery.status_code == 200
    assert discovery.json() == {
        "protocol": "storage-discovery-v1",
        "artifact_backend": "legacy-posix",
        "minimum_client_capability": None,
        "registry": None,
    }
    assert lookup.status_code == 200
    assert persistence.lookup_calls == 1
