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
class ProgressEvent:
    """Immutable private progress observation for terminal presentation.

    Notes:
        Counts are accepted only as a complete, typed ratio. This prevents a
        presenter from inferring a denominator or work unit from free text.
    """

    phase: ProgressPhase
    state: ProgressState
    message: str
    completed: int | None = None
    total: int | None = None
    unit: ProgressUnit | None = None
    elapsed_seconds: float | None = None

    def __post_init__(self) -> None:
        has_completed = self.completed is not None
        has_total = self.total is not None
        if has_completed != has_total:
            raise ValueError("progress counts require both completed and total")
        if has_completed != (self.unit is not None):
            raise ValueError("progress counts require exactly one typed unit")
        if has_completed:
            assert self.completed is not None
            assert self.total is not None
            if self.completed < 0 or self.total < 0:
                raise ValueError("progress counts cannot be negative")
            if self.completed > self.total:
                raise ValueError("completed progress cannot exceed total")
        if self.elapsed_seconds is not None and self.elapsed_seconds < 0:
            raise ValueError("elapsed progress time cannot be negative")


ProgressSink = Callable[[ProgressEvent], None]


def emit_progress(sink: ProgressSink | None, event: ProgressEvent) -> None:
    """Deliver one best-effort event without changing annotation behavior."""

    if sink is None:
        return
    try:
        sink(event)
    except Exception:
        _LOGGER.exception("annotation progress sink failed")
