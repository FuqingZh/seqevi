from __future__ import annotations

from pathlib import Path

import pytest

from seqevi.evidence import (
    EvidenceCommit,
    EvidenceKey,
    EvidenceStatus,
    canonical_semantic_parameters,
    sha256_digest,
)
from seqevi.sequence import identify_protein_sequence

from .support import write_artifact_file


def make_key(**parameter_overrides: object) -> EvidenceKey:
    identity = identify_protein_sequence("MPEPTIDE")
    parameters: dict[str, object] = {
        "evalue": 1e-5,
        "applications": ["Pfam"],
        **parameter_overrides,
    }
    return EvidenceKey.from_parameters(
        sequence_id=identity.sequence_id,
        adapter_contract_version="interpro-pfam/1",
        tool_runtime_digest="sha256:" + "a" * 64,
        resource_id="interproscan/5.77-108.0",
        semantic_parameters=parameters,
    )


def test_semantic_parameters_are_canonical_json() -> None:
    left = canonical_semantic_parameters({"z": (1, True), "a": {"value": None}})
    right = canonical_semantic_parameters({"a": {"value": None}, "z": [1, True]})

    assert left == right == '{"a":{"value":null},"z":[1,true]}'


@pytest.mark.parametrize("value", [float("nan"), float("inf"), Path("local")])
def test_semantic_parameters_reject_non_json_values(value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        canonical_semantic_parameters({"value": value})


def test_evidence_key_changes_for_each_semantic_change() -> None:
    baseline = make_key()
    changed = make_key(evalue=1e-3)

    assert baseline != changed
    assert baseline.semantic_parameters_hash != changed.semantic_parameters_hash


def test_evidence_key_rejects_noncanonical_json() -> None:
    identity = identify_protein_sequence("MPEPTIDE")

    with pytest.raises(ValueError, match="not canonical"):
        EvidenceKey(
            sequence_id=identity.sequence_id,
            adapter_contract_version="adapter/1",
            tool_runtime_digest="runtime",
            resource_id="resource",
            semantic_parameters_json='{"z": 1, "a": 2}',
        )


def test_hit_commit_requires_normalized_artifact() -> None:
    identity = identify_protein_sequence("MPEPTIDE")

    with pytest.raises(ValueError, match="requires a normalized artifact"):
        EvidenceCommit(
            identity=identity,
            key=make_key(),
            status=EvidenceStatus.HIT,
            payload_digest=sha256_digest(b"result"),
        )


def test_no_hit_commit_rejects_normalized_artifact(tmp_path: Path) -> None:
    identity = identify_protein_sequence("MPEPTIDE")

    with pytest.raises(ValueError, match="cannot contain"):
        EvidenceCommit(
            identity=identity,
            key=make_key(),
            status=EvidenceStatus.NO_HIT,
            payload_digest=sha256_digest(b"no-hit"),
            normalized_artifact=write_artifact_file(
                tmp_path / "unexpected.txt", b"unexpected", "text/plain"
            ),
        )
