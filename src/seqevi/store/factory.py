"""Explicit local-path or shared-URL Store selection."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path

from seqevi.errors import StoreConfigurationError

from .client import HttpEvidenceStore
from .contract import EvidenceStore
from .local import LocalStore
from .oci import OciClientFiles


@contextmanager
def open_evidence_store(
    value: str | Path | None,
    *,
    environ: Mapping[str, str] | None = None,
    oci_files: OciClientFiles | None = None,
) -> Iterator[EvidenceStore]:
    """Open an explicit local Store path or HTTP(S) shared Store URL."""

    environment = os.environ if environ is None else environ
    raw = environment.get("SEQEVI_STORE") if value is None else os.fspath(value)
    if not raw:
        raise StoreConfigurationError(
            "a Store path or URL is required via --store or SEQEVI_STORE"
        )
    if raw.startswith(("http://", "https://")):
        with HttpEvidenceStore(raw, oci_files=oci_files) as store:
            yield store
        return
    if "://" in raw:
        raise StoreConfigurationError(f"unsupported Store URL scheme: {raw}")
    if oci_files is not None and (
        oci_files.registry_config is not None or oci_files.ca_file is not None
    ):
        raise StoreConfigurationError(
            "OCI file inputs require an OCI-enabled shared Store"
        )
    with LocalStore.open(raw, environ={}) as store:
        yield store
