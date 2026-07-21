"""Atomic adapter-specific Data Package materialization."""

from __future__ import annotations

import io
import json
import os
import shutil
import tempfile
import warnings
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import polars as pl

from . import __version__
from .adapters.base import AdapterContract
from .errors import OutputPackageError
from .evidence import (
    EvidenceSource,
    EvidenceStatus,
    FetchedEvidence,
    SequenceMapRow,
    sha256_digest,
)
from .sequence import InputSequence, SequenceIdentity

_SEQUENCE_MAP_COLUMNS = (
    "InputOrder",
    "InputID",
    "InputHeader",
    "SequenceID",
    "MD5",
    "Length",
    "EvidenceStatus",
    "EvidenceSource",
)
_NO_HIT_SCHEMA: Mapping[str, pl.DataType] = {
    "SequenceID": pl.String(),
    "MD5": pl.String(),
    "Length": pl.Int64(),
}


def materialize_output_package(
    *,
    output_dir: Path,
    records: tuple[InputSequence, ...],
    fetched_by_sequence_id: Mapping[str, FetchedEvidence],
    source_by_sequence_id: Mapping[str, EvidenceSource],
    evidence_schema: Mapping[str, pl.DataType],
    adapter_contract: AdapterContract,
    input_digest: str,
    created_at: datetime | None = None,
) -> None:
    """Write one complete output package and atomically expose its directory."""

    if output_dir.exists():
        raise OutputPackageError(f"output path already exists: {output_dir}")
    parent = output_dir.parent
    if not parent.is_dir():
        raise OutputPackageError(f"output parent directory does not exist: {parent}")

    stage_dir = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.", dir=parent))
    try:
        df_evidence = _collect_evidence_frame(
            fetched_by_sequence_id=fetched_by_sequence_id,
            evidence_schema=evidence_schema,
        )
        df_sequence_map, rows_sequence_map = _build_sequence_map(
            records=records,
            fetched_by_sequence_id=fetched_by_sequence_id,
            source_by_sequence_id=source_by_sequence_id,
        )
        df_no_hits = _build_no_hits(
            records=records,
            fetched_by_sequence_id=fetched_by_sequence_id,
        )

        paths = {
            "evidence": stage_dir / "evidence.parquet",
            "sequence-map": stage_dir / "sequence-map.tsv",
            "no-hits": stage_dir / "no-hits.parquet",
        }
        df_evidence.write_parquet(paths["evidence"], compression="zstd")
        df_sequence_map.write_csv(paths["sequence-map"], separator="\t")
        df_no_hits.write_parquet(paths["no-hits"], compression="zstd")

        descriptor = _build_descriptor(
            stage_dir=stage_dir,
            frames={
                "evidence": df_evidence,
                "sequence-map": df_sequence_map,
                "no-hits": df_no_hits,
            },
            paths=paths,
            adapter_contract=adapter_contract,
            input_digest=input_digest,
            created_at=created_at or datetime.now(UTC),
        )
        (stage_dir / "datapackage.json").write_text(
            json.dumps(descriptor, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        if len(rows_sequence_map) != len(records):
            raise OutputPackageError("sequence map does not account for every input")
        os.replace(stage_dir, output_dir)
    except Exception:
        shutil.rmtree(stage_dir, ignore_errors=True)
        raise


def _collect_evidence_frame(
    *,
    fetched_by_sequence_id: Mapping[str, FetchedEvidence],
    evidence_schema: Mapping[str, pl.DataType],
) -> pl.DataFrame:
    sequence_ids_by_digest: dict[str, set[str]] = defaultdict(set)
    artifact_by_digest: dict[str, bytes] = {}

    for sequence_id, fetched in fetched_by_sequence_id.items():
        if fetched.record.status is EvidenceStatus.NO_HIT:
            continue
        digest = fetched.record.normalized_artifact_digest
        data = fetched.normalized_artifact
        if digest is None or data is None:
            raise OutputPackageError(
                f"hit evidence is missing its normalized artifact: {sequence_id}"
            )
        sequence_ids_by_digest[digest].add(sequence_id)
        artifact_by_digest[digest] = data

    frames: list[pl.DataFrame] = []
    seen_hit_ids: set[str] = set()
    expected_schema = dict(evidence_schema)
    if "SequenceID" not in expected_schema:
        raise OutputPackageError("adapter evidence schema requires SequenceID")

    for digest in sorted(artifact_by_digest):
        frame = pl.read_parquet(io.BytesIO(artifact_by_digest[digest]))
        if frame.schema != expected_schema:
            raise OutputPackageError(
                f"normalized artifact schema does not match adapter contract: {digest}"
            )
        owned_ids = sequence_ids_by_digest[digest]
        selected = frame.filter(pl.col("SequenceID").is_in(sorted(owned_ids)))
        selected_ids = set(selected.get_column("SequenceID").to_list())
        missing_ids = owned_ids - selected_ids
        if missing_ids:
            rendered = ", ".join(sorted(missing_ids))
            raise OutputPackageError(
                f"normalized artifact has no rows for hit sequences: {rendered}"
            )
        overlap = seen_hit_ids.intersection(selected_ids)
        if overlap:
            rendered = ", ".join(sorted(overlap))
            raise OutputPackageError(
                f"normalized artifacts duplicate hit sequences: {rendered}"
            )
        seen_hit_ids.update(selected_ids)
        frames.append(selected)

    if not frames:
        return pl.DataFrame(schema=expected_schema)
    return pl.concat(frames, how="vertical").sort("SequenceID", maintain_order=True)


def _build_sequence_map(
    *,
    records: tuple[InputSequence, ...],
    fetched_by_sequence_id: Mapping[str, FetchedEvidence],
    source_by_sequence_id: Mapping[str, EvidenceSource],
) -> tuple[pl.DataFrame, tuple[SequenceMapRow, ...]]:
    rows: list[SequenceMapRow] = []
    for record in records:
        sequence_id = record.identity.sequence_id
        fetched = fetched_by_sequence_id.get(sequence_id)
        source = source_by_sequence_id.get(sequence_id)
        if fetched is None or source is None:
            raise OutputPackageError(
                f"sequence map is missing terminal evidence: {sequence_id}"
            )
        rows.append(
            SequenceMapRow(
                input_order=record.input_order,
                input_id=record.input_id,
                input_header=record.input_header,
                sequence_id=sequence_id,
                md5=record.identity.md5,
                length=record.identity.length,
                evidence_status=fetched.record.status,
                evidence_source=source,
            )
        )

    data = [
        {
            "InputOrder": row.input_order,
            "InputID": row.input_id,
            "InputHeader": row.input_header,
            "SequenceID": row.sequence_id,
            "MD5": row.md5,
            "Length": row.length,
            "EvidenceStatus": row.evidence_status.value,
            "EvidenceSource": row.evidence_source.value,
        }
        for row in rows
    ]
    frame = pl.DataFrame(
        data,
        schema={
            "InputOrder": pl.Int64,
            "InputID": pl.String,
            "InputHeader": pl.String,
            "SequenceID": pl.String,
            "MD5": pl.String,
            "Length": pl.Int64,
            "EvidenceStatus": pl.String,
            "EvidenceSource": pl.String,
        },
    ).select(_SEQUENCE_MAP_COLUMNS)
    return frame, tuple(rows)


def _build_no_hits(
    *,
    records: tuple[InputSequence, ...],
    fetched_by_sequence_id: Mapping[str, FetchedEvidence],
) -> pl.DataFrame:
    identity_by_sequence_id: dict[str, SequenceIdentity] = {
        record.identity.sequence_id: record.identity for record in records
    }
    rows = [
        {
            "SequenceID": sequence_id,
            "MD5": identity.md5,
            "Length": identity.length,
        }
        for sequence_id, identity in sorted(identity_by_sequence_id.items())
        if fetched_by_sequence_id[sequence_id].record.status is EvidenceStatus.NO_HIT
    ]
    return pl.DataFrame(rows, schema=dict(_NO_HIT_SCHEMA))


def _build_descriptor(
    *,
    stage_dir: Path,
    frames: Mapping[str, pl.DataFrame],
    paths: Mapping[str, Path],
    adapter_contract: AdapterContract,
    input_digest: str,
    created_at: datetime,
) -> dict[str, object]:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=(
                'Field name "schema" in "Resource" shadows an attribute in parent '
                '"Model"'
            ),
            category=UserWarning,
        )
        from dplib.actions.package.check import check_package
        from dplib.models import Package
        from dplib.plugins.polars.models.schema import PolarsSchema

    resources = []
    for name in ("evidence", "sequence-map", "no-hits"):
        path = paths[name]
        is_tsv = path.suffix == ".tsv"
        resources.append(
            {
                "name": name,
                "path": path.name,
                "type": "table",
                "format": "tsv" if is_tsv else "parquet",
                "mediatype": (
                    "text/tab-separated-values"
                    if is_tsv
                    else "application/vnd.apache.parquet"
                ),
                "encoding": "utf-8" if is_tsv else None,
                "bytes": path.stat().st_size,
                "hash": f"sha256:{sha256_digest(path.read_bytes())}",
                "rowCount": frames[name].height,
                "schema": PolarsSchema(df=frames[name]).to_dp().to_dict(),
            }
        )

    descriptor: dict[str, object] = {
        "name": f"seqevi-{adapter_contract.name}",
        "title": f"SeqEvi {adapter_contract.name} evidence",
        "version": __version__,
        "created": created_at.astimezone(UTC).isoformat(),
        "resources": resources,
        "seqevi": {
            "extensionVersion": "1.0",
            "seqeviVersion": __version__,
            "adapter": adapter_contract.name,
            "adapterContractVersion": adapter_contract.version,
            "toolRuntimeDigest": adapter_contract.tool_runtime_digest,
            "resourceId": adapter_contract.resource_id,
            "semanticParameters": adapter_contract.semantic_parameters,
            "inputDigest": f"sha256:{input_digest}",
        },
    }
    package = Package.from_dict(descriptor, basepath=str(stage_dir))
    errors = check_package(package)
    if errors:
        detail = "; ".join(str(error) for error in errors)
        raise OutputPackageError(f"invalid Data Package descriptor: {detail}")
    return cast(dict[str, object], package.to_dict())
