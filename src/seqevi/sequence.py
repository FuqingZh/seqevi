"""Protein FASTA parsing and canonical content identity."""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from Bio.SeqIO.FastaIO import SimpleFastaParser

from .errors import FastaIssue, FastaValidationError

_ASCII_WHITESPACE = str.maketrans("", "", " \t\n\r\v\f")


@dataclass(frozen=True, slots=True)
class SequenceIdentity:
    """Canonical protein sequence and its stable content identifiers."""

    sequence_id: str
    md5: str
    length: int
    sequence: str

    def __post_init__(self) -> None:
        canonical = canonicalize_protein_sequence(self.sequence)
        if canonical != self.sequence:
            raise ValueError("SequenceIdentity.sequence must already be canonical")
        sequence_bytes = self.sequence.encode("ascii")
        expected_md5 = hashlib.md5(sequence_bytes, usedforsecurity=False).hexdigest()
        if self.sequence_id != ga4gh_sequence_id(self.sequence):
            raise ValueError("SequenceIdentity.sequence_id does not match sequence")
        if self.md5 != expected_md5:
            raise ValueError("SequenceIdentity.md5 does not match sequence")
        if self.length != len(self.sequence):
            raise ValueError("SequenceIdentity.length does not match sequence")


@dataclass(frozen=True, slots=True)
class InputSequence:
    """One FASTA record linked to its reusable canonical identity."""

    input_order: int
    input_id: str
    input_header: str
    identity: SequenceIdentity


def canonicalize_protein_sequence(sequence: str) -> str:
    """Return the v1 canonical protein sequence or raise ``ValueError``.

    Identity accepts every ASCII letter after normalization. Adapter-specific
    residue restrictions belong to the adapter and must not rewrite identity.
    """

    canonical = sequence.translate(_ASCII_WHITESPACE).upper()
    if canonical.endswith("*"):
        canonical = canonical[:-1]
    if not canonical:
        raise ValueError("sequence is empty after canonicalization")

    invalid = sorted({residue for residue in canonical if not "A" <= residue <= "Z"})
    if invalid:
        rendered = ", ".join(repr(residue) for residue in invalid)
        raise ValueError(f"sequence contains invalid residue characters: {rendered}")
    return canonical


def ga4gh_sequence_id(canonical_sequence: str) -> str:
    """Compute the GA4GH refget ``SQ.`` identifier for canonical content."""

    sequence_bytes = canonical_sequence.encode("ascii")
    digest = hashlib.sha512(sequence_bytes).digest()[:24]
    encoded = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return f"SQ.{encoded}"


def identify_protein_sequence(sequence: str) -> SequenceIdentity:
    """Canonicalize a protein sequence and return all v1 identity fields."""

    canonical = canonicalize_protein_sequence(sequence)
    sequence_bytes = canonical.encode("ascii")
    return SequenceIdentity(
        sequence_id=ga4gh_sequence_id(canonical),
        md5=hashlib.md5(sequence_bytes, usedforsecurity=False).hexdigest(),
        length=len(canonical),
        sequence=canonical,
    )


def parse_fasta(handle: TextIO) -> tuple[InputSequence, ...]:
    """Parse and validate a complete protein FASTA stream atomically.

    All detectable record errors are collected. No records are returned when
    any record is invalid, which lets callers fail before touching a Store.
    Input order is one-based in the public sequence map contract.
    """

    records: list[InputSequence] = []
    issues: list[FastaIssue] = []
    seen_input_ids: set[str] = set()

    for record_number, (header, sequence) in enumerate(
        SimpleFastaParser(handle), start=1
    ):
        input_id = header.split(maxsplit=1)[0] if header else ""
        issue_id = input_id or None
        record_is_valid = True
        identity: SequenceIdentity | None = None
        if not input_id:
            issues.append(FastaIssue(record_number, None, "FASTA header is empty"))
            record_is_valid = False
        elif input_id in seen_input_ids:
            issues.append(FastaIssue(record_number, input_id, "InputID is duplicated"))
            record_is_valid = False
        else:
            seen_input_ids.add(input_id)

        try:
            identity = identify_protein_sequence(sequence)
        except ValueError as error:
            issues.append(FastaIssue(record_number, issue_id, str(error)))
            record_is_valid = False

        if record_is_valid and identity is not None:
            records.append(
                InputSequence(
                    input_order=record_number,
                    input_id=input_id,
                    input_header=header,
                    identity=identity,
                )
            )

    if not records and not issues:
        issues.append(FastaIssue(0, None, "FASTA contains no records"))
    if issues:
        raise FastaValidationError(tuple(issues))
    return tuple(records)


def read_fasta(path: Path) -> tuple[InputSequence, ...]:
    """Read a UTF-8 protein FASTA file using the normative parser."""

    with path.open("r", encoding="utf-8", newline=None) as handle:
        return parse_fasta(handle)


def unique_identities(
    records: tuple[InputSequence, ...],
) -> tuple[SequenceIdentity, ...]:
    """Return first-seen canonical identities from an invocation."""

    by_sequence_id: dict[str, SequenceIdentity] = {}
    for record in records:
        by_sequence_id.setdefault(record.identity.sequence_id, record.identity)
    return tuple(by_sequence_id.values())


def iter_fasta_lines(identities: tuple[SequenceIdentity, ...]) -> Iterator[str]:
    """Yield deterministic adapter FASTA lines ordered by ``SequenceID``."""

    for identity in sorted(identities, key=lambda item: item.sequence_id):
        yield f">{identity.sequence_id}\n"
        yield f"{identity.sequence}\n"
