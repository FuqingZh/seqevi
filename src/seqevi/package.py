"""Atomic adapter-specific Data Package materialization."""

from __future__ import annotations

import csv
import json
import os
import shutil
import tempfile
import warnings
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
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
)
from .hashing import sha256_file
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
_SEQUENCE_MAP_SCHEMA: Mapping[str, pl.DataType] = {
    "InputOrder": pl.Int64(),
    "InputID": pl.String(),
    "InputHeader": pl.String(),
    "SequenceID": pl.String(),
    "MD5": pl.String(),
    "Length": pl.Int64(),
    "EvidenceStatus": pl.String(),
    "EvidenceSource": pl.String(),
}


@dataclass(frozen=True, slots=True)
class _TableSummary:
    row_count: int
    schema: Mapping[str, pl.DataType]


def materialize_output_package(
    *,
    output_dir: Path,
    records: Iterable[InputSequence],
    identities: Iterable[SequenceIdentity],
    input_record_count: int,
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
        paths = {
            "evidence": stage_dir / "evidence.parquet",
            "sequence-map": stage_dir / "sequence-map.tsv",
            "no-hits": stage_dir / "no-hits.parquet",
        }
        evidence_summary = _write_evidence(
            path=paths["evidence"],
            staging_dir=stage_dir,
            fetched_by_sequence_id=fetched_by_sequence_id,
            evidence_schema=evidence_schema,
        )
        no_hits_summary = _write_no_hits(
            path=paths["no-hits"],
            staging_dir=stage_dir,
            identities=identities,
            fetched_by_sequence_id=fetched_by_sequence_id,
        )

        sequence_map_rows = _write_sequence_map(
            path=paths["sequence-map"],
            records=records,
            fetched_by_sequence_id=fetched_by_sequence_id,
            source_by_sequence_id=source_by_sequence_id,
        )
        descriptor = _build_descriptor(
            stage_dir=stage_dir,
            tables={
                "evidence": evidence_summary,
                "sequence-map": _TableSummary(
                    sequence_map_rows,
                    _SEQUENCE_MAP_SCHEMA,
                ),
                "no-hits": no_hits_summary,
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

        if sequence_map_rows != input_record_count:
            raise OutputPackageError("sequence map does not account for every input")
        os.replace(stage_dir, output_dir)
    except Exception:
        shutil.rmtree(stage_dir, ignore_errors=True)
        raise


def _write_evidence(
    *,
    path: Path,
    staging_dir: Path,
    fetched_by_sequence_id: Mapping[str, FetchedEvidence],
    evidence_schema: Mapping[str, pl.DataType],
) -> _TableSummary:
    sequence_ids_by_digest: dict[str, set[str]] = defaultdict(set)
    artifact_by_digest: dict[str, Path] = {}

    for sequence_id, fetched in fetched_by_sequence_id.items():
        if fetched.record.status is EvidenceStatus.NO_HIT:
            continue
        digest = fetched.record.normalized_artifact_digest
        artifact = fetched.normalized_artifact
        if digest is None or artifact is None:
            raise OutputPackageError(
                f"hit evidence is missing its normalized artifact: {sequence_id}"
            )
        sequence_ids_by_digest[digest].add(sequence_id)
        if artifact.digest != digest:
            raise OutputPackageError(
                f"normalized artifact reference has the wrong digest: {sequence_id}"
            )
        artifact_by_digest[digest] = artifact.path

    lazy_frames: list[pl.LazyFrame] = []
    id_paths: list[Path] = []
    seen_hit_ids: set[str] = set()
    row_count = 0
    expected_schema = dict(evidence_schema)
    if "SequenceID" not in expected_schema:
        raise OutputPackageError("adapter evidence schema requires SequenceID")

    for digest in sorted(artifact_by_digest):
        artifact_frame = pl.scan_parquet(artifact_by_digest[digest])
        if dict(artifact_frame.collect_schema()) != expected_schema:
            raise OutputPackageError(
                f"normalized artifact schema does not match adapter contract: {digest}"
            )
        owned_ids = sequence_ids_by_digest[digest]
        ids_path = staging_dir / f".owned-{digest}.parquet"
        pl.DataFrame(
            {"SequenceID": sorted(owned_ids)},
            schema={"SequenceID": pl.String()},
        ).write_parquet(ids_path)
        id_paths.append(ids_path)
        selected = artifact_frame.join(
            pl.scan_parquet(ids_path), on="SequenceID", how="semi"
        )
        selected_ids = set(
            selected.select("SequenceID")
            .unique()
            .collect(engine="streaming")
            .get_column("SequenceID")
        )
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
        row_count += selected.select(pl.len()).collect(engine="streaming").item()
        lazy_frames.append(selected)

    try:
        if not lazy_frames:
            pl.DataFrame(schema=expected_schema).write_parquet(path, compression="zstd")
        else:
            pl.concat(lazy_frames, how="vertical").sort(
                "SequenceID", maintain_order=True
            ).sink_parquet(path, compression="zstd", maintain_order=True)
    finally:
        for ids_path in id_paths:
            ids_path.unlink(missing_ok=True)
    return _TableSummary(row_count, expected_schema)


def _write_sequence_map(
    *,
    path: Path,
    records: Iterable[InputSequence],
    fetched_by_sequence_id: Mapping[str, FetchedEvidence],
    source_by_sequence_id: Mapping[str, EvidenceSource],
) -> int:
    row_count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(_SEQUENCE_MAP_COLUMNS)
        for record in records:
            sequence_id = record.identity.sequence_id
            fetched = fetched_by_sequence_id.get(sequence_id)
            source = source_by_sequence_id.get(sequence_id)
            if fetched is None or source is None:
                raise OutputPackageError(
                    f"sequence map is missing terminal evidence: {sequence_id}"
                )
            writer.writerow(
                (
                    record.input_order,
                    record.input_id,
                    record.input_header,
                    sequence_id,
                    record.identity.md5,
                    record.identity.length,
                    fetched.record.status.value,
                    source.value,
                )
            )
            row_count += 1
    return row_count


def _write_no_hits(
    *,
    path: Path,
    staging_dir: Path,
    identities: Iterable[SequenceIdentity],
    fetched_by_sequence_id: Mapping[str, FetchedEvidence],
) -> _TableSummary:
    staging_path = staging_dir / ".no-hits.ndjson"
    row_count = 0
    try:
        with staging_path.open("w", encoding="utf-8") as handle:
            for identity in identities:
                if (
                    fetched_by_sequence_id[identity.sequence_id].record.status
                    is not EvidenceStatus.NO_HIT
                ):
                    continue
                handle.write(
                    json.dumps(
                        {
                            "SequenceID": identity.sequence_id,
                            "MD5": identity.md5,
                            "Length": identity.length,
                        },
                        ensure_ascii=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                )
                row_count += 1
        if row_count == 0:
            pl.DataFrame(schema=dict(_NO_HIT_SCHEMA)).write_parquet(
                path, compression="zstd"
            )
        else:
            pl.scan_ndjson(
                staging_path,
                schema=dict(_NO_HIT_SCHEMA),
            ).sort("SequenceID").sink_parquet(
                path, compression="zstd", maintain_order=True
            )
    finally:
        staging_path.unlink(missing_ok=True)
    return _TableSummary(row_count, _NO_HIT_SCHEMA)


def _build_descriptor(
    *,
    stage_dir: Path,
    tables: Mapping[str, _TableSummary],
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
        table = tables[name]
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
                "hash": f"sha256:{sha256_file(path)}",
                "rowCount": table.row_count,
                "schema": PolarsSchema(df=pl.DataFrame(schema=dict(table.schema)))
                .to_dp()
                .to_dict(),
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
