"""Private annotation progress events and failure containment."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

_LOGGER = logging.getLogger(__name__)


class ProgressPhase(StrEnum):
    """Stable internal phases of one annotation application invocation."""

    ANNOTATION = "annotation"
    STAGING = "staging"
    STORE_LOOKUP = "store_lookup"
    CLAIM_WAIT = "claim_wait"
    TOOL = "tool"
    STORE_COMMIT = "store_commit"
    STORE_FETCH = "store_fetch"
    PACKAGE = "package"
    FINALIZATION = "finalization"
    COMPLETED = "completed"


class ProgressState(StrEnum):
    """Lifecycle state of one internal annotation phase."""

    STARTED = "started"
    RUNNING = "running"
    COMPLETED = "completed"


class ProgressUnit(StrEnum):
    """Exact SeqEvi-owned work units supported by the initial renderer."""

    SEQUENCES = "sequences"


@dataclass(frozen=True, slots=True)
class WorkProgress:
    """One exact cumulative measure owned by SeqEvi."""

    completed: int
    total: int
    unit: ProgressUnit

    def __post_init__(self) -> None:
        if self.completed < 0 or self.total < 0:
            raise ValueError("progress counts cannot be negative")
        if self.completed > self.total:
            raise ValueError("completed progress cannot exceed total")


@dataclass(frozen=True, slots=True)
class BatchProgress:
    """Exact metadata for one internally owned adapter batch."""

    number: int
    size: int

    def __post_init__(self) -> None:
        if self.number < 1:
            raise ValueError("batch number must be positive")
        if self.size < 1:
            raise ValueError("batch size must be positive")


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    """Immutable private progress observation for terminal presentation.

    Notes:
        ``evidence_ready`` is cumulative terminal evidence, not phase-local
        activity. Tool batches remain indeterminate even when their size is
        known.
    """

    phase: ProgressPhase
    state: ProgressState
    message: str
    evidence_ready: WorkProgress | None = None
    batch: BatchProgress | None = None


ProgressSink = Callable[[ProgressEvent], None]


def emit_progress(sink: ProgressSink | None, event: ProgressEvent) -> None:
    """Deliver one best-effort event without changing annotation behavior."""

    if sink is None:
        return
    try:
        sink(event)
    except Exception:
        _LOGGER.exception("annotation progress sink failed")
