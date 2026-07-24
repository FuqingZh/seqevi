from __future__ import annotations

from io import StringIO
from pathlib import Path
from string import ascii_letters

import pytest
from hypothesis import given
from hypothesis import strategies as st

from seqevi.errors import FastaValidationError
from seqevi.sequence import (
    SequenceIdentity,
    canonicalize_protein_sequence,
    ga4gh_sequence_id,
    identify_protein_sequence,
    iter_fasta_lines,
    iter_staged_identities,
    iter_staged_records,
    parse_fasta,
    stage_fasta,
    unique_identities,
)
from seqevi.evidence import sha256_digest


def test_ga4gh_identifier_matches_official_refget_vector() -> None:
    identity = identify_protein_sequence("ac gt*")

    assert identity.sequence == "ACGT"
    assert identity.sequence_id == "SQ.aKF498dAxcJAqme6QYQ7EZ07-fiw8Kw2"
    assert identity.md5 == "f1f8f4bf413b16ad135722aa4591043e"
    assert identity.length == 4


@pytest.mark.parametrize(
    "sequence",
    ["", "*", "AC*GT", "AC-GT", "AC.GT", "AC1GT", "ACéGT", "ACGT**"],
)
def test_canonicalization_rejects_non_protein_content(sequence: str) -> None:
    with pytest.raises(ValueError):
        canonicalize_protein_sequence(sequence)


def test_identity_layer_accepts_ambiguous_ascii_residues() -> None:
    assert canonicalize_protein_sequence("bjouxz") == "BJOUXZ"


@given(st.text(alphabet=ascii_letters, min_size=1, max_size=100))
def test_identity_is_stable_across_case_whitespace_and_terminal_stop(
    sequence: str,
) -> None:
    canonical = sequence.upper()
    variant = " \t".join(sequence.lower()) + "*"

    assert canonicalize_protein_sequence(variant) == canonical
    assert identify_protein_sequence(variant) == identify_protein_sequence(canonical)
    assert canonicalize_protein_sequence(canonical) == canonical


def test_parse_fasta_preserves_headers_and_maps_duplicate_content() -> None:
    records = parse_fasta(
        StringIO(">first customer label\nacgt\n>second another label\nAC GT*\n")
    )

    assert [record.input_order for record in records] == [1, 2]
    assert [record.input_id for record in records] == ["first", "second"]
    assert records[0].input_header == "first customer label"
    assert records[0].identity is not records[1].identity
    assert records[0].identity == records[1].identity
    assert unique_identities(records) == (records[0].identity,)


def test_parse_fasta_collects_errors_and_returns_no_partial_result() -> None:
    with pytest.raises(FastaValidationError) as caught:
        parse_fasta(
            StringIO(">valid\nACGT\n>valid duplicate\nACGT\n>invalid\nAC-GT\n>\nACGT\n")
        )

    assert len(caught.value.issues) == 3
    assert "InputID is duplicated" in str(caught.value)
    assert "invalid residue" in str(caught.value)
    assert "header is empty" in str(caught.value)


def test_empty_fasta_is_invalid() -> None:
    with pytest.raises(FastaValidationError, match="contains no records"):
        parse_fasta(StringIO(""))


def test_fasta_stage_preserves_atomic_records_identity_and_raw_digest(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    raw = b">first label\nacgt\n>second alias\nAC GT*\n>third\nMPEPTIDE\n"
    fasta.write_bytes(raw)

    stage = stage_fasta(fasta, tmp_path / "stage")

    records = tuple(iter_staged_records(stage))
    identities = tuple(iter_staged_identities(stage))
    assert stage.input_digest == sha256_digest(raw)
    assert stage.input_records == 3
    assert stage.unique_sequences == 2
    assert [record.input_id for record in records] == ["first", "second", "third"]
    assert identities == (records[0].identity, records[2].identity)


def test_invalid_fasta_stage_is_removed_and_returns_no_partial_records(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(
        ">valid\nACGT\n>middle\nAC-GT\n>valid\nMPEPTIDE\n>last\nAC1GT\n",
        encoding="utf-8",
    )
    stage_dir = tmp_path / "stage"

    with pytest.raises(FastaValidationError) as caught:
        stage_fasta(fasta, stage_dir)

    assert len(caught.value.issues) == 3
    assert not stage_dir.exists()


def test_non_utf8_fasta_stage_is_a_validation_error_and_is_removed(
    tmp_path: Path,
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_bytes(b">valid\nACGT\n>invalid\nMPEP\xffTIDE\n")
    stage_dir = tmp_path / "stage"

    with pytest.raises(FastaValidationError, match="not valid UTF-8"):
        stage_fasta(fasta, stage_dir)

    assert not stage_dir.exists()


def test_sequence_identity_rejects_inconsistent_manual_values() -> None:
    with pytest.raises(ValueError, match="sequence_id"):
        SequenceIdentity(
            sequence_id="SQ." + "a" * 32,
            md5="f1f8f4bf413b16ad135722aa4591043e",
            length=4,
            sequence="ACGT",
        )


def test_adapter_fasta_is_sorted_by_sequence_id() -> None:
    identities = (
        identify_protein_sequence("MPEPTIDE"),
        identify_protein_sequence("ACGT"),
    )

    lines = list(iter_fasta_lines(identities))

    observed_ids = [lines[index][1:].strip() for index in range(0, len(lines), 2)]
    assert observed_ids == sorted(identity.sequence_id for identity in identities)
    assert ga4gh_sequence_id("ACGT") in observed_ids
