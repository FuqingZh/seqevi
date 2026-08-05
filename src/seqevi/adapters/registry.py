"""Explicit registry for official SeqEvi adapters."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import NoReturn

from seqevi.errors import AdapterUnavailableError

from .base import AnnotationAdapter
from .dbcan_cazyme import DBCanCazymeAdapter
from .eggnog import EggnogAdapter
from .interpro_pfam import InterProPfamAdapter


class AdapterName(StrEnum):
    """Official v1 adapter names accepted by the public CLI."""

    EGGNOG = "eggnog"
    INTERPRO_PFAM = "interpro-pfam"
    DBCAN_CAZYME = "dbcan-cazyme"


@dataclass(frozen=True, slots=True)
class AdapterConfiguration:
    """Concrete external tool and database locations supplied by a caller."""

    name: AdapterName
    executable: Path
    database: Path
    verify_resource: bool = False
    environment: tuple[tuple[str, str], ...] = ()


def create_adapter(configuration: AdapterConfiguration) -> AnnotationAdapter:
    """Create an official adapter implemented by the installed release."""

    if configuration.name is AdapterName.INTERPRO_PFAM:
        return InterProPfamAdapter(
            executable=configuration.executable,
            database=configuration.database,
            verify_resource=configuration.verify_resource,
            environment=dict(configuration.environment),
        )
    if configuration.name is AdapterName.EGGNOG:
        return EggnogAdapter(
            executable=configuration.executable,
            database=configuration.database,
            verify_resource=configuration.verify_resource,
            environment=dict(configuration.environment),
        )
    if configuration.name is AdapterName.DBCAN_CAZYME:
        return DBCanCazymeAdapter(
            executable=configuration.executable,
            database=configuration.database,
            verify_resource=configuration.verify_resource,
            environment=dict(configuration.environment),
        )
    _raise_unavailable(configuration.name, 4)


def _raise_unavailable(name: AdapterName, phase: int) -> NoReturn:
    raise AdapterUnavailableError(
        f"adapter {name.value!r} is declared for v1 but is implemented in Phase "
        f"{phase}; it is not implemented in this release"
    )
