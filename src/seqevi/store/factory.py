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


@contextmanager
def open_evidence_store(
    value: str | Path | None,
    *,
    environ: Mapping[str, str] | None = None,
) -> Iterator[EvidenceStore]:
    """Open an explicit local Store path or HTTP(S) shared Store URL.

    HTTP Store URLs must not contain credentials. Set
    ``SEQEVI_HTTP_BASIC_AUTH_FILE`` to the absolute path of an owner-only file
    containing exactly two non-empty lines (username, then password).

    Example:
        >>> environment = {
        ...     "SEQEVI_STORE": "https://node4.cluster.local:18443",
        ...     "SEQEVI_HTTP_BASIC_AUTH_FILE": "/run/secrets/seqevi-basic-auth",
        ... }
        >>> with open_evidence_store(None, environ=environment) as store:
        ...     store.maximum_batch_size
        1000

    Args:
        value: Explicit Store path or URL, or ``None`` to read
            ``SEQEVI_STORE``.
        environ: Optional environment mapping used for Store selection and the
            Basic-auth file path.

    Yields:
        An opened local or HTTP evidence Store.
    """

    environment = os.environ if environ is None else environ
    raw = environment.get("SEQEVI_STORE") if value is None else os.fspath(value)
    if not raw:
        raise StoreConfigurationError(
            "a Store path or URL is required via --store or SEQEVI_STORE"
        )
    if raw.startswith(("http://", "https://")):
        auth_file = environment.get("SEQEVI_HTTP_BASIC_AUTH_FILE")
        with HttpEvidenceStore(raw, basic_auth_file=auth_file) as store:
            yield store
        return
    if "://" in raw:
        raise StoreConfigurationError(f"unsupported Store URL scheme: {raw}")
    with LocalStore.open(raw, environ={}) as store:
        yield store
