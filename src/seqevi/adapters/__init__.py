"""Explicit first-party annotation adapter boundaries."""

from .base import (
    AdapterBatchResult,
    AdapterContract,
    AdapterSequenceResult,
    AnnotationAdapter,
)
from .dbcan_cazyme import DBCAN_EVIDENCE_SCHEMA, DBCanCazymeAdapter, DBCanParameters
from .eggnog import (
    EGGNOG_EVIDENCE_SCHEMA,
    EggnogAdapter,
    EggnogParameters,
)
from .interpro_pfam import (
    ADAPTER_CONTRACT_VERSION,
    INTERPRO_PFAM_EVIDENCE_SCHEMA,
    InterProPfamAdapter,
    InterProPfamParameters,
)
from .registry import AdapterConfiguration, AdapterName, create_adapter

__all__ = [
    "AdapterBatchResult",
    "AdapterConfiguration",
    "AdapterContract",
    "AdapterName",
    "AdapterSequenceResult",
    "AnnotationAdapter",
    "DBCAN_EVIDENCE_SCHEMA",
    "DBCanCazymeAdapter",
    "DBCanParameters",
    "EGGNOG_EVIDENCE_SCHEMA",
    "EggnogAdapter",
    "EggnogParameters",
    "ADAPTER_CONTRACT_VERSION",
    "INTERPRO_PFAM_EVIDENCE_SCHEMA",
    "InterProPfamAdapter",
    "InterProPfamParameters",
    "create_adapter",
]
