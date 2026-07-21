"""Explicit first-party annotation adapter boundaries."""

from .base import (
    AdapterBatchResult,
    AdapterContract,
    AdapterSequenceResult,
    AnnotationAdapter,
)
from .registry import AdapterConfiguration, AdapterName, create_adapter

__all__ = [
    "AdapterBatchResult",
    "AdapterConfiguration",
    "AdapterContract",
    "AdapterName",
    "AdapterSequenceResult",
    "AnnotationAdapter",
    "create_adapter",
]
