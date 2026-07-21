"""Explicit first-party annotation adapter boundaries."""

from .base import (
    AdapterBatchResult,
    AdapterContract,
    AdapterSequenceResult,
    AnnotationAdapter,
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
    "ADAPTER_CONTRACT_VERSION",
    "INTERPRO_PFAM_EVIDENCE_SCHEMA",
    "InterProPfamAdapter",
    "InterProPfamParameters",
    "create_adapter",
]
