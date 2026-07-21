"""Typed boundary shared by SeqEvi's official adapters."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import polars as pl

from seqevi.evidence import (
    ArtifactPayload,
    EvidenceKey,
    EvidenceStatus,
    canonical_semantic_parameters,
)
from seqevi.runner import ToolRunner
from seqevi.sequence import SequenceIdentity


@dataclass(frozen=True, slots=True)
class AdapterContract:
    """Resolved immutable identity shared by one adapter invocation."""

    name: str
    version: str
    tool_runtime_digest: str
    resource_id: str
    semantic_parameters_json: str

    def __post_init__(self) -> None:
        for field_name in ("name", "version", "tool_runtime_digest", "resource_id"):
            if not getattr(self, field_name):
                raise ValueError(f"adapter contract {field_name} must not be empty")
        parsed = json.loads(self.semantic_parameters_json)
        if not isinstance(parsed, dict):
            raise ValueError("adapter semantic parameters must be a JSON object")
        if canonical_semantic_parameters(parsed) != self.semantic_parameters_json:
            raise ValueError("adapter semantic parameters are not canonical")

    @classmethod
    def from_parameters(
        cls,
        *,
        name: str,
        version: str,
        tool_runtime_digest: str,
        resource_id: str,
        semantic_parameters: Mapping[str, object],
    ) -> AdapterContract:
        """Construct a resolved contract with canonical semantic parameters."""

        return cls(
            name=name,
            version=version,
            tool_runtime_digest=tool_runtime_digest,
            resource_id=resource_id,
            semantic_parameters_json=canonical_semantic_parameters(semantic_parameters),
        )

    @property
    def semantic_parameters(self) -> dict[str, object]:
        """Return a fresh JSON-compatible semantic-parameter mapping."""

        parameters = json.loads(self.semantic_parameters_json)
        assert isinstance(parameters, dict)
        return parameters

    def evidence_key(self, identity: SequenceIdentity) -> EvidenceKey:
        """Build the exact evidence key for one canonical sequence."""

        return EvidenceKey(
            sequence_id=identity.sequence_id,
            adapter_contract_version=self.version,
            tool_runtime_digest=self.tool_runtime_digest,
            resource_id=self.resource_id,
            semantic_parameters_json=self.semantic_parameters_json,
        )


@dataclass(frozen=True, slots=True)
class AdapterSequenceResult:
    """Terminal adapter result for one requested sequence."""

    sequence_id: str
    status: EvidenceStatus
    payload_digest: str


@dataclass(frozen=True, slots=True)
class AdapterBatchResult:
    """Validated batch artifacts and per-sequence terminal states."""

    sequences: tuple[AdapterSequenceResult, ...]
    raw_artifact: ArtifactPayload
    normalized_artifact: ArtifactPayload | None = None

    def __post_init__(self) -> None:
        if not self.sequences:
            raise ValueError("adapter batch result must not be empty")
        sequence_ids = [result.sequence_id for result in self.sequences]
        if len(sequence_ids) != len(set(sequence_ids)):
            raise ValueError("adapter batch result contains duplicate SequenceIDs")
        if (
            any(result.status is EvidenceStatus.HIT for result in self.sequences)
            and self.normalized_artifact is None
        ):
            raise ValueError("adapter hits require a normalized artifact")


class AnnotationAdapter(Protocol):
    """Behavior required by the annotation orchestrator."""

    @property
    def contract(self) -> AdapterContract: ...

    @property
    def evidence_schema(self) -> Mapping[str, pl.DataType]: ...

    def run_batch(
        self,
        *,
        identities: tuple[SequenceIdentity, ...],
        input_fasta: Path,
        work_dir: Path,
        runner: ToolRunner,
        timeout_seconds: float | None,
    ) -> AdapterBatchResult: ...
