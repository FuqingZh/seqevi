"""Versioned HTTP transport models for the shared evidence Store."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from seqevi.evidence import (
    ArtifactFile,
    BusyEvidenceClaim,
    ClaimAcquireResult,
    ClaimDisposition,
    ClaimedEvidenceCommit,
    CommitOutcome,
    EvidenceClaim,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    EvidenceStatus,
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


class ClaimCapabilitiesResponse(TransportModel):
    maximum_batch_size: Annotated[int, Field(ge=1, le=1000, strict=True)]
    lease_seconds: Annotated[float, Field(gt=0)]
    renewal_after_seconds: Annotated[float, Field(gt=0)]

    @model_validator(mode="after")
    def validate_runway(self) -> ClaimCapabilitiesResponse:
        if self.lease_seconds <= 5.0:
            raise ValueError("claim lease_seconds must exceed the 5-second runway")
        if self.renewal_after_seconds > self.lease_seconds - 5.0:
            raise ValueError("claim renewal cadence must preserve the 5-second runway")
        return self


class EvidenceClaimModel(TransportModel):
    key: EvidenceKeyModel
    owner_token: Annotated[str, Field(min_length=1, max_length=255, repr=False)]
    generation: Annotated[int, Field(ge=1, strict=True)]
    expires_at: datetime
    renewal_after_seconds: Annotated[float, Field(gt=0)]

    @model_validator(mode="after")
    def validate_domain_claim(self) -> EvidenceClaimModel:
        self.to_domain()
        return self

    @classmethod
    def from_domain(cls, value: EvidenceClaim) -> EvidenceClaimModel:
        return cls(
            key=EvidenceKeyModel.from_domain(value.key),
            owner_token=value.owner_token,
            generation=value.generation,
            expires_at=value.expires_at,
            renewal_after_seconds=value.renewal_after_seconds,
        )

    def to_domain(self) -> EvidenceClaim:
        return EvidenceClaim(
            self.key.to_domain(),
            self.owner_token,
            self.generation,
            self.expires_at,
            self.renewal_after_seconds,
        )


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


class ClaimAcquireResultModel(TransportModel):
    disposition: ClaimDisposition
    record: EvidenceRecordModel | None = None
    claim: EvidenceClaimModel | None = None
    busy: BusyEvidenceClaimModel | None = None

    @model_validator(mode="after")
    def validate_domain_result(self) -> ClaimAcquireResultModel:
        self.to_domain()
        return self

    @classmethod
    def from_domain(cls, value: ClaimAcquireResult) -> ClaimAcquireResultModel:
        return cls(
            disposition=value.disposition,
            record=None
            if value.record is None
            else EvidenceRecordModel.from_domain(value.record),
            claim=None
            if value.claim is None
            else EvidenceClaimModel.from_domain(value.claim),
            busy=None
            if value.busy is None
            else BusyEvidenceClaimModel.from_domain(value.busy),
        )

    def to_domain(self) -> ClaimAcquireResult:
        return ClaimAcquireResult(
            self.disposition,
            record=None if self.record is None else self.record.to_domain(),
            claim=None if self.claim is None else self.claim.to_domain(),
            busy=None if self.busy is None else self.busy.to_domain(),
        )


class ClaimAcquireRequest(TransportModel):
    owner_token: Annotated[str, Field(min_length=1, max_length=255, repr=False)]
    queries: list[EvidenceQueryModel]

    @model_validator(mode="after")
    def validate_unique_keys(self) -> ClaimAcquireRequest:
        if len({query.key.to_domain() for query in self.queries}) != len(self.queries):
            raise ValueError("claim acquisition contains a duplicate evidence key")
        return self


class ClaimAcquireResponse(TransportModel):
    results: list[ClaimAcquireResultModel]


class ClaimMutationRequest(TransportModel):
    claims: list[EvidenceClaimModel]

    @model_validator(mode="after")
    def validate_unique_keys(self) -> ClaimMutationRequest:
        if len({claim.key.to_domain() for claim in self.claims}) != len(self.claims):
            raise ValueError("claim mutation contains a duplicate evidence key")
        return self


class ClaimRenewResponse(TransportModel):
    claims: list[EvidenceClaimModel]


class ClaimReleaseAcknowledgement(TransportModel):
    key: EvidenceKeyModel
    generation: Annotated[int, Field(ge=1, strict=True)]


class ClaimReleaseResponse(TransportModel):
    released: list[ClaimReleaseAcknowledgement]


class ClaimedCommitModel(TransportModel):
    commit: CommitModel
    claim: EvidenceClaimModel

    @model_validator(mode="after")
    def validate_matching_key(self) -> ClaimedCommitModel:
        if self.commit.key != self.claim.key:
            raise ValueError("claim and evidence commit keys do not match")
        return self

    @classmethod
    def from_domain(cls, value: ClaimedEvidenceCommit) -> ClaimedCommitModel:
        return cls(
            commit=CommitModel.from_domain(value.commit),
            claim=EvidenceClaimModel.from_domain(value.claim),
        )


class ClaimFinalizeRequest(TransportModel):
    commits: list[ClaimedCommitModel]

    @model_validator(mode="after")
    def validate_unique_keys(self) -> ClaimFinalizeRequest:
        if len({item.claim.key.to_domain() for item in self.commits}) != len(
            self.commits
        ):
            raise ValueError("claim finalization contains a duplicate evidence key")
        return self


class ArtifactUploadResponse(TransportModel):
    status: Literal["created", "existing"]
    artifact: ArtifactReferenceModel


class HealthResponse(TransportModel):
    status: Literal["ok"] = "ok"
    api_version: Literal["v1"] = "v1"
    maximum_batch_size: Annotated[int, Field(ge=1, strict=True)]
    maximum_artifact_bytes: Annotated[int, Field(ge=1, strict=True)]
