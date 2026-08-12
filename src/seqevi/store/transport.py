"""Versioned HTTP transport models for the shared evidence Store."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from seqevi.evidence import (
    ArtifactFile,
    BusyEvidenceClaim,
    ClaimDisposition,
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    EvidenceStatus,
    ClaimSessionAuthority,
    SessionEvidenceClaim,
)
from seqevi.sequence import SequenceIdentity

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
SequenceId = Annotated[str, Field(pattern=r"^SQ\.[A-Za-z0-9_-]{32}$")]


class TransportModel(BaseModel):
    """Strict base for the public Store wire contract."""

    model_config = ConfigDict(extra="forbid")


class SequenceModel(TransportModel):
    sequence_id: SequenceId
    md5: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    length: Annotated[int, Field(gt=0, strict=True)]
    sequence: str

    @model_validator(mode="after")
    def validate_domain_identity(self) -> SequenceModel:
        SequenceIdentity(**self.model_dump())
        return self

    @classmethod
    def from_domain(cls, value: SequenceIdentity) -> SequenceModel:
        return cls(
            sequence_id=value.sequence_id,
            md5=value.md5,
            length=value.length,
            sequence=value.sequence,
        )

    def to_domain(self) -> SequenceIdentity:
        return SequenceIdentity(**self.model_dump())


class EvidenceKeyModel(TransportModel):
    sequence_id: SequenceId
    adapter_contract_version: str
    tool_runtime_digest: str
    resource_id: str
    semantic_parameters_json: str

    @model_validator(mode="after")
    def validate_domain_key(self) -> EvidenceKeyModel:
        EvidenceKey(**self.model_dump())
        return self

    @classmethod
    def from_domain(cls, value: EvidenceKey) -> EvidenceKeyModel:
        return cls(
            sequence_id=value.sequence_id,
            adapter_contract_version=value.adapter_contract_version,
            tool_runtime_digest=value.tool_runtime_digest,
            resource_id=value.resource_id,
            semantic_parameters_json=value.semantic_parameters_json,
        )

    def to_domain(self) -> EvidenceKey:
        return EvidenceKey(**self.model_dump())


class EvidenceQueryModel(TransportModel):
    identity: SequenceModel
    key: EvidenceKeyModel

    @model_validator(mode="after")
    def validate_domain_query(self) -> EvidenceQueryModel:
        EvidenceQuery(self.identity.to_domain(), self.key.to_domain())
        return self

    @classmethod
    def from_domain(cls, value: EvidenceQuery) -> EvidenceQueryModel:
        return cls(
            identity=SequenceModel.from_domain(value.identity),
            key=EvidenceKeyModel.from_domain(value.key),
        )

    def to_domain(self) -> EvidenceQuery:
        return EvidenceQuery(self.identity.to_domain(), self.key.to_domain())


class ArtifactReferenceModel(TransportModel):
    digest: Sha256
    media_type: str
    byte_size: Annotated[int, Field(ge=0, strict=True)]


class EvidenceRecordModel(TransportModel):
    key: EvidenceKeyModel
    status: EvidenceStatus
    payload_digest: Sha256
    normalized_artifact_digest: Sha256 | None
    raw_artifact_digest: Sha256 | None
    created_at: datetime

    @classmethod
    def from_domain(cls, value: EvidenceRecord) -> EvidenceRecordModel:
        return cls(
            key=EvidenceKeyModel.from_domain(value.key),
            status=value.status,
            payload_digest=value.payload_digest,
            normalized_artifact_digest=value.normalized_artifact_digest,
            raw_artifact_digest=value.raw_artifact_digest,
            created_at=value.created_at,
        )

    def to_domain(self) -> EvidenceRecord:
        return EvidenceRecord(
            key=self.key.to_domain(),
            status=self.status,
            payload_digest=self.payload_digest,
            normalized_artifact_digest=self.normalized_artifact_digest,
            raw_artifact_digest=self.raw_artifact_digest,
            created_at=self.created_at,
        )


class LookupRequest(TransportModel):
    queries: list[EvidenceQueryModel]


class LookupResponse(TransportModel):
    records: list[EvidenceRecordModel]


class FetchRequest(TransportModel):
    key: EvidenceKeyModel


class FetchResponse(TransportModel):
    record: EvidenceRecordModel | None


class FetchManyRequest(TransportModel):
    keys: list[EvidenceKeyModel]


class FetchManyResponse(TransportModel):
    records: list[EvidenceRecordModel]


class CommitModel(TransportModel):
    identity: SequenceModel
    key: EvidenceKeyModel
    status: EvidenceStatus
    payload_digest: Sha256
    normalized_artifact: ArtifactReferenceModel | None
    raw_artifact: ArtifactReferenceModel | None

    @model_validator(mode="after")
    def validate_shared_commit(self) -> CommitModel:
        if self.key.sequence_id != self.identity.sequence_id:
            raise ValueError("evidence key and sequence identity do not match")
        if self.status is EvidenceStatus.HIT and self.normalized_artifact is None:
            raise ValueError("hit evidence requires a normalized artifact")
        if (
            self.status is EvidenceStatus.NO_HIT
            and self.normalized_artifact is not None
        ):
            raise ValueError("no-hit evidence cannot contain a normalized artifact")
        if self.raw_artifact is None:
            raise ValueError("shared evidence requires a raw artifact")
        return self

    @classmethod
    def from_domain(cls, value: EvidenceCommit) -> CommitModel:
        def reference(
            payload: ArtifactFile | None,
        ) -> ArtifactReferenceModel | None:
            if payload is None:
                return None
            return ArtifactReferenceModel(
                digest=payload.digest,
                media_type=payload.media_type,
                byte_size=payload.byte_size,
            )

        return cls(
            identity=SequenceModel.from_domain(value.identity),
            key=EvidenceKeyModel.from_domain(value.key),
            status=value.status,
            payload_digest=value.payload_digest,
            normalized_artifact=reference(value.normalized_artifact),
            raw_artifact=reference(value.raw_artifact),
        )


class CommitRequest(TransportModel):
    commits: list[CommitModel]


class CommitResponse(TransportModel):
    outcomes: list[CommitOutcome]


class ArtifactUploadResponse(TransportModel):
    status: Literal["created", "existing"]
    artifact: ArtifactReferenceModel


class HealthResponse(TransportModel):
    status: Literal["ok"] = "ok"
    api_version: Literal["v1"] = "v1"
    maximum_batch_size: Annotated[int, Field(ge=1, strict=True)]
    maximum_artifact_bytes: Annotated[int, Field(ge=1, strict=True)]


OpaqueId = Annotated[str, Field(min_length=1, max_length=64)]


class ClaimSessionCapabilitiesResponse(TransportModel):
    protocol: Literal["claim-session-v1"] = "claim-session-v1"
    maximum_batch_size: Annotated[int, Field(ge=1, le=1000, strict=True)]
    retention_seconds: Literal[60] = 60
    maximum_session_receipt_headers: Literal[1000] = 1000
    maximum_session_receipt_items: Literal[32000] = 32000
    server_time: datetime


class ClaimSessionOpenRequest(TransportModel):
    open_request_id: OpaqueId
    server_time: datetime
    open_not_after: datetime


class ClaimSessionAuthorityModel(TransportModel):
    session_id: OpaqueId
    owner_token: Annotated[str, Field(min_length=1, max_length=255, repr=False)]
    generation: Annotated[int, Field(ge=1, strict=True)]

    @classmethod
    def from_domain(cls, value: ClaimSessionAuthority) -> ClaimSessionAuthorityModel:
        return cls(
            session_id=value.session_id,
            owner_token=value.owner_token,
            generation=value.generation,
        )


class ClaimSessionOpenResponse(ClaimSessionAuthorityModel):
    expires_at: datetime
    remaining_lease_seconds: Annotated[float, Field(gt=0)]
    heartbeat_after_seconds: Annotated[float, Field(gt=0)]
    renew_deadline_seconds: Annotated[float, Field(gt=0)]

    def to_domain(self) -> ClaimSessionAuthority:
        return ClaimSessionAuthority(**self.model_dump())


class ClaimSessionRenewRequest(ClaimSessionAuthorityModel):
    pass


class ClaimSessionRenewResponse(ClaimSessionOpenResponse):
    pass


class ClaimSessionCloseRequest(ClaimSessionAuthorityModel):
    pass


class ClaimSessionCloseResponse(TransportModel):
    session_id: OpaqueId
    generation: Annotated[int, Field(ge=1, strict=True)]
    closed: Literal[True] = True


class SessionEvidenceClaimModel(TransportModel):
    key: EvidenceKeyModel
    generation: Annotated[int, Field(ge=1, strict=True)]

    @classmethod
    def from_domain(cls, value: SessionEvidenceClaim) -> SessionEvidenceClaimModel:
        return cls(
            key=EvidenceKeyModel.from_domain(value.key), generation=value.generation
        )

    def to_domain(self) -> SessionEvidenceClaim:
        return SessionEvidenceClaim(self.key.to_domain(), self.generation)


class BusyEvidenceClaimModel(TransportModel):
    key: EvidenceKeyModel
    expires_at: datetime
    retry_after_seconds: Annotated[float, Field(gt=0)]

    @classmethod
    def from_domain(cls, value: BusyEvidenceClaim) -> BusyEvidenceClaimModel:
        return cls(
            key=EvidenceKeyModel.from_domain(value.key),
            expires_at=value.expires_at,
            retry_after_seconds=value.retry_after_seconds,
        )

    def to_domain(self) -> BusyEvidenceClaim:
        return BusyEvidenceClaim(
            self.key.to_domain(), self.expires_at, self.retry_after_seconds
        )


class ClaimSessionAcquireRequest(ClaimSessionAuthorityModel):
    acquire_request_id: OpaqueId
    query_digest: Sha256
    queries: list[EvidenceQueryModel]

    @model_validator(mode="after")
    def validate_queries_and_digest(self) -> ClaimSessionAcquireRequest:
        keys = [query.key.to_domain() for query in self.queries]
        if len(set(keys)) != len(keys):
            raise ValueError("claim acquisition contains a duplicate evidence key")
        if canonical_query_digest(self.queries) != self.query_digest:
            raise ValueError("query_digest does not describe the submitted queries")
        return self


class ClaimSessionAcquireResultModel(TransportModel):
    disposition: ClaimDisposition
    record: EvidenceRecordModel | None = None
    claim: SessionEvidenceClaimModel | None = None
    busy: BusyEvidenceClaimModel | None = None


class ClaimSessionAcquireResponse(TransportModel):
    results: list[ClaimSessionAcquireResultModel]


class ClaimSessionFinalizeItem(TransportModel):
    commit: CommitModel
    claim_generation: Annotated[int, Field(ge=1, strict=True)]


class ClaimSessionFinalizeRequest(ClaimSessionAuthorityModel):
    finalize_request_id: OpaqueId
    commits: list[ClaimSessionFinalizeItem]

    @model_validator(mode="after")
    def validate_unique_keys(self) -> ClaimSessionFinalizeRequest:
        keys = [item.commit.key.to_domain() for item in self.commits]
        if len(set(keys)) != len(keys):
            raise ValueError("claim finalization contains a duplicate evidence key")
        return self


class ClaimSessionFinalizeResponse(TransportModel):
    outcomes: list[CommitOutcome]


class InternalErrorResponse(TransportModel):
    code: Literal["open_request_expired", "claim_receipt_capacity"]
    detail: str


def canonical_query_digest(queries: list[EvidenceQueryModel]) -> str:
    payload = [query.model_dump(mode="json") for query in queries]
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
