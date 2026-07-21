from __future__ import annotations

import json
from collections.abc import Iterable
from io import StringIO
from pathlib import Path

from seqevi.evidence import (
    ArtifactPayload,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceStatus,
    sha256_digest,
)
from seqevi.sequence import InputSequence, parse_fasta, unique_identities
from seqevi.store import LocalStore

_AMINO_ACIDS = "ACDEFGHIKLMNPQRSTVWY"


def protein_sequence(index: int) -> str:
    digits: list[str] = []
    value = index
    for _position in range(5):
        value, remainder = divmod(value, len(_AMINO_ACIDS))
        digits.append(_AMINO_ACIDS[remainder])
    return "M" + "".join(reversed(digits))


def fasta_records(indices: Iterable[int], prefix: str) -> tuple[InputSequence, ...]:
    fasta = "".join(
        f">{prefix}-{index} invocation-specific header\n{protein_sequence(index)}\n"
        for index in indices
    )
    return parse_fasta(StringIO(fasta))


def evidence_key(record: InputSequence) -> EvidenceKey:
    return EvidenceKey.from_parameters(
        sequence_id=record.identity.sequence_id,
        adapter_contract_version="fake/1",
        tool_runtime_digest="sha256:" + "f" * 64,
        resource_id="fake-resource/1",
        semantic_parameters={"mode": "deterministic"},
    )


def fake_annotate(store: LocalStore, records: tuple[InputSequence, ...]) -> int:
    identities = unique_identities(records)
    keys = {
        identity.sequence_id: EvidenceKey.from_parameters(
            sequence_id=identity.sequence_id,
            adapter_contract_version="fake/1",
            tool_runtime_digest="sha256:" + "f" * 64,
            resource_id="fake-resource/1",
            semantic_parameters={"mode": "deterministic"},
        )
        for identity in identities
    }
    cached = store.lookup_many(
        EvidenceQuery(identity, keys[identity.sequence_id]) for identity in identities
    )
    missing = [
        identity for identity in identities if keys[identity.sequence_id] not in cached
    ]
    if not missing:
        return 0

    batch_rows = [
        {"SequenceID": identity.sequence_id, "Annotation": identity.sequence[:3]}
        for identity in sorted(missing, key=lambda item: item.sequence_id)
    ]
    artifact = ArtifactPayload(
        (
            "\n".join(json.dumps(row, sort_keys=True) for row in batch_rows) + "\n"
        ).encode(),
        "application/x-ndjson",
    )
    commits = [
        EvidenceCommit(
            identity=identity,
            key=keys[identity.sequence_id],
            status=EvidenceStatus.HIT,
            payload_digest=sha256_digest(
                f"annotation:{identity.sequence_id}:{identity.sequence[:3]}".encode()
            ),
            normalized_artifact=artifact,
        )
        for identity in missing
    ]
    store.commit_many(commits)
    return len(missing)


def test_a_b_c_fastas_reuse_only_exact_sequence_content(tmp_path: Path) -> None:
    fasta_a = fasta_records(range(0, 2000), "a")
    fasta_b = fasta_records(range(500, 1500), "b")
    fasta_c = fasta_records(
        (*range(0, 1000), *range(2000, 2500)),
        "c",
    )

    with LocalStore.open(tmp_path / "store") as store:
        assert fake_annotate(store, fasta_a) == 2000
        assert fake_annotate(store, fasta_b) == 0
        assert fake_annotate(store, fasta_c) == 500

        c_queries = [
            EvidenceQuery(record.identity, evidence_key(record)) for record in fasta_c
        ]
        assert len(store.lookup_many(c_queries)) == 1500
