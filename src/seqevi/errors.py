"""Domain errors exposed by SeqEvi's core contracts."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FastaIssue:
    """One invalid FASTA record discovered during whole-input validation."""

    record_number: int
    input_id: str | None
    message: str

    def describe(self) -> str:
        label = self.input_id if self.input_id is not None else "<missing>"
        return f"record {self.record_number} ({label}): {self.message}"


class FastaValidationError(ValueError):
    """Raised after collecting all detectable FASTA input defects."""

    def __init__(self, issues: tuple[FastaIssue, ...]) -> None:
        if not issues:
            raise ValueError("FastaValidationError requires at least one issue")
        self.issues = issues
        detail = "; ".join(issue.describe() for issue in issues)
        super().__init__(f"FASTA validation failed: {detail}")


class StoreError(RuntimeError):
    """Base class for evidence Store failures."""


class StoreConfigurationError(StoreError):
    """Raised when a Store location is missing or unsupported."""


class StoreIntegrityError(StoreError):
    """Raised when persisted content does not match its immutable identity."""


class EvidenceConflictError(StoreIntegrityError):
    """Raised when one evidence key is associated with different payloads."""


class EvidenceClaimLostError(StoreError):
    """Raised when a claim owner or generation no longer owns its lease."""


class AnnotationError(RuntimeError):
    """Base class for one annotation invocation failure."""


class ProfileConfigurationError(AnnotationError):
    """Raised when an execution profile cannot be resolved safely."""


class SetupError(AnnotationError):
    """Raised when a managed adapter setup plan cannot be built safely."""


class AdapterError(AnnotationError):
    """Raised when an adapter cannot produce valid terminal evidence."""


class ResourceLockError(AdapterError):
    """Raised when a database resource lock is invalid or conflicts with files."""


class AdapterUnavailableError(AdapterError):
    """Raised when a declared adapter is not implemented in this release."""


class OutputPackageError(AnnotationError):
    """Raised when a valid invocation package cannot be materialized."""
