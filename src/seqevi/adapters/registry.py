"""Explicit registry for official SeqEvi adapters."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import NoReturn

from seqevi.errors import AdapterUnavailableError

from .base import AnnotationAdapter


class AdapterName(StrEnum):
    """Official v1 adapter names accepted by the public CLI."""

    EGGNOG = "eggnog"
    INTERPRO_PFAM = "interpro-pfam"


@dataclass(frozen=True, slots=True)
class AdapterConfiguration:
    """Concrete external tool and database locations supplied by a caller."""

    name: AdapterName
    executable: Path
    database: Path


def create_adapter(configuration: AdapterConfiguration) -> AnnotationAdapter:
    """Create an official adapter implemented by the installed release."""

    phase = 3 if configuration.name is AdapterName.INTERPRO_PFAM else 4
    _raise_unavailable(configuration.name, phase)


def _raise_unavailable(name: AdapterName, phase: int) -> NoReturn:
    raise AdapterUnavailableError(
        f"adapter {name.value!r} is declared for v1 but is implemented in Phase "
        f"{phase}; this Phase 2 release provides the runner and package contract"
    )
