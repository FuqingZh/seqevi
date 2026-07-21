"""Immutable evidence identity and Store transfer values."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from .sequence import SequenceIdentity

_SEQUENCE_ID_PATTERN = re.compile(r"SQ\.[A-Za-z0-9_-]{32}\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")


class EvidenceStatus(StrEnum):
    """Reusable terminal state for one exact evidence key."""

    HIT = "hit"
    NO_HIT = "no_hit"


class EvidenceSource(StrEnum):
    """How an invocation obtained a terminal evidence record."""

    CACHE = "cache"
    COMPUTED = "computed"


class CommitOutcome(StrEnum):
    """Result of an immutable evidence commit."""

    CREATED = "created"
    EXISTING = "existing"


def _normalize_json_value(value: Any, *, path: str) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} contains a non-finite float")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} contains a non-string object key")
            normalized[key] = _normalize_json_value(child, path=f"{path}.{key}")
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            _normalize_json_value(child, path=f"{path}[{index}]")
            for index, child in enumerate(value)
        ]
    raise TypeError(f"{path} contains unsupported value type {type(value).__name__}")


def canonical_semantic_parameters(parameters: Mapping[str, Any]) -> str:
    """Serialize semantic parameters to deterministic, strict JSON."""

    normalized = _normalize_json_value(parameters, path="parameters")
    return json.dumps(
        normalized,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def sha256_digest(data: bytes) -> str:
    """Return the lowercase SHA-256 digest of exact bytes."""

    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True, slots=True)
class EvidenceKey:
    """Exact scientific identity required for evidence reuse."""

    sequence_id: str
    adapter_contract_version: str
    tool_runtime_digest: str
    resource_id: str
    semantic_parameters_json: str

    def __post_init__(self) -> None:
        if not _SEQUENCE_ID_PATTERN.fullmatch(self.sequence_id):
            raise ValueError(f"invalid GA4GH SequenceID: {self.sequence_id!r}")
        for name in (
            "adapter_contract_version",
            "tool_runtime_digest",
            "resource_id",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} must not be empty")

        try:
            parsed = json.loads(self.semantic_parameters_json)
        except json.JSONDecodeError as error:
            raise ValueError("semantic_parameters_json is not valid JSON") from error
        if not isinstance(parsed, dict):
            raise ValueError("semantic parameters must be a JSON object")
        canonical = canonical_semantic_parameters(parsed)
        if canonical != self.semantic_parameters_json:
            raise ValueError("semantic_parameters_json is not canonical")

    @classmethod
    def from_parameters(
        cls,
        *,
        sequence_id: str,
        adapter_contract_version: str,
        tool_runtime_digest: str,
        resource_id: str,
        semantic_parameters: Mapping[str, Any],
    ) -> EvidenceKey:
        """Construct a key while canonicalizing semantic parameters."""

        return cls(
            sequence_id=sequence_id,
            adapter_contract_version=adapter_contract_version,
            tool_runtime_digest=tool_runtime_digest,
            resource_id=resource_id,
            semantic_parameters_json=canonical_semantic_parameters(semantic_parameters),
        )

    @property
    def semantic_parameters_hash(self) -> str:
        return sha256_digest(self.semantic_parameters_json.encode("utf-8"))

    @property
    def contract_identity(self) -> tuple[str, str, str, str]:
        """Return key fields shared by sequences in one annotation request."""

        return (
            self.adapter_contract_version,
            self.tool_runtime_digest,
            self.resource_id,
            self.semantic_parameters_hash,
        )


@dataclass(frozen=True, slots=True)
class ArtifactPayload:
    """Exact artifact bytes and their media type before Store insertion."""

    data: bytes
    media_type: str

    def __post_init__(self) -> None:
        if not isinstance(self.data, bytes):
            raise TypeError("artifact data must be bytes")
        if not self.media_type:
            raise ValueError("artifact media_type must not be empty")

    @property
    def digest(self) -> str:
        return sha256_digest(self.data)


@dataclass(frozen=True, slots=True)
class StoredArtifact:
    """Metadata for one immutable content-addressed artifact."""

    digest: str
    media_type: str
    byte_size: int
    relative_path: str


@dataclass(frozen=True, slots=True)
class EvidenceCommit:
    """Validated terminal evidence proposed for atomic Store commit."""

    identity: SequenceIdentity
    key: EvidenceKey
    status: EvidenceStatus
    payload_digest: str
    normalized_artifact: ArtifactPayload | None = None
    raw_artifact: ArtifactPayload | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, EvidenceStatus):
            raise TypeError("status must be an EvidenceStatus")
        if self.key.sequence_id != self.identity.sequence_id:
            raise ValueError("evidence key and sequence identity do not match")
        if not _SHA256_PATTERN.fullmatch(self.payload_digest):
            raise ValueError("payload_digest must be a lowercase SHA-256 digest")
        if self.status is EvidenceStatus.HIT and self.normalized_artifact is None:
            raise ValueError("hit evidence requires a normalized artifact")
        if (
            self.status is EvidenceStatus.NO_HIT
            and self.normalized_artifact is not None
        ):
            raise ValueError("no-hit evidence cannot contain a normalized artifact")


@dataclass(frozen=True, slots=True)
class EvidenceQuery:
    """Exact lookup request carrying content for collision verification."""

    identity: SequenceIdentity
    key: EvidenceKey

    def __post_init__(self) -> None:
        if self.key.sequence_id != self.identity.sequence_id:
            raise ValueError("evidence key and sequence identity do not match")


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    """Persisted metadata for one exact terminal evidence result."""

    key: EvidenceKey
    status: EvidenceStatus
    payload_digest: str
    normalized_artifact_digest: str | None
    raw_artifact_digest: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class FetchedEvidence:
    """Evidence metadata with integrity-checked artifact bytes."""

    record: EvidenceRecord
    normalized_artifact: bytes | None
    raw_artifact: bytes | None


@dataclass(frozen=True, slots=True)
class SequenceMapRow:
    """One public invocation mapping from FASTA label to evidence."""

    input_order: int
    input_id: str
    input_header: str
    sequence_id: str
    md5: str
    length: int
    evidence_status: EvidenceStatus
    evidence_source: EvidenceSource
