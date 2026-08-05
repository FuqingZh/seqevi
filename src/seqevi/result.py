"""DuckDB result contract, atomic writer, and Slice A prototype helpers."""

from __future__ import annotations

import csv
import json
import os
import shutil
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from .adapters.base import AdapterContract
from .errors import OutputPackageError
from .evidence import EvidenceSource, EvidenceStatus, FetchedEvidence
from .sequence import InputSequence

RESULT_FORMAT_VERSION = "seqevi-duckdb/1"
STORAGE_COMPATIBILITY_VERSION = "v1.0.0"

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
_COMMON_COLUMN_DESCRIPTIONS = {
    "InputOrder": "One-based order in the input FASTA.",
    "InputID": "First whitespace-delimited token from the FASTA header.",
    "InputHeader": "Complete FASTA header text for this invocation.",
    "SequenceID": "GA4GH refget identity of the canonical protein sequence.",
    "MD5": "Lowercase MD5 compatibility alias for the canonical sequence.",
    "Length": "Canonical protein sequence length in residues.",
    "EvidenceStatus": "Terminal status under the selected evidence contract.",
    "EvidenceSource": "Whether this invocation reused or computed the evidence.",
}
_REQUIRED_METADATA_COLUMNS = (
    "ResultFormatVersion",
    "ResultSchemaID",
    "SeqEviVersion",
    "Adapter",
    "AdapterContractVersion",
    "UpstreamTool",
    "UpstreamToolVersion",
    "ToolRuntimeDigest",
    "ResourceID",
    "InputDigest",
    "CreatedAt",
)


def materialize_result_database(
    *,
    output_path: Path,
    records: Iterable[InputSequence],
    input_record_count: int,
    fetched_by_sequence_id: Mapping[str, FetchedEvidence],
    source_by_sequence_id: Mapping[str, EvidenceSource],
    evidence_schema: Mapping[str, pl.DataType],
    adapter_contract: AdapterContract,
    input_digest: str,
    metadata: Mapping[str, str],
    run_metrics: Mapping[str, object],
    created_at: datetime | None = None,
) -> None:
    """Atomically publish one complete, self-describing DuckDB result.

    The Store remains file-backed and incremental. This writer reads immutable
    normalized Parquet artifacts and a temporary sequence map into a new
    database, validates the closed file read-only, and only then exposes the
    requested path.
    """

    if output_path.exists():
        raise OutputPackageError(f"output path already exists: {output_path}")
    if not output_path.parent.is_dir():
        raise OutputPackageError(
            f"output parent directory does not exist: {output_path.parent}"
        )
    if input_record_count < 1:
        raise OutputPackageError("result must contain at least one input record")
    _validate_inputs_for_schema(evidence_schema, metadata)
    for field, expected in (
        ("Adapter", adapter_contract.name),
        ("AdapterContractVersion", adapter_contract.version),
        ("ToolRuntimeDigest", adapter_contract.tool_runtime_digest),
        ("ResourceID", adapter_contract.resource_id),
        ("InputDigest", input_digest),
    ):
        if metadata[field] != expected:
            raise OutputPackageError(
                f"result metadata {field} does not match the resolved invocation"
            )

    stage_fd, stage_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.", suffix=".tmp", dir=output_path.parent
    )
    os.close(stage_fd)
    stage_path = Path(stage_name)
    stage_path.unlink()
    temporary_dir = Path(
        tempfile.mkdtemp(prefix=f".{output_path.name}.", dir=output_path.parent)
    )
    connection: duckdb.DuckDBPyConnection | None = None
    try:
        sequence_map_path = temporary_dir / "sequence-map.tsv"
        _write_sequence_map_tsv(
            path=sequence_map_path,
            records=records,
            input_record_count=input_record_count,
            fetched_by_sequence_id=fetched_by_sequence_id,
            source_by_sequence_id=source_by_sequence_id,
        )
        connection = duckdb.connect(
            str(stage_path),
            config={"storage_compatibility_version": STORAGE_COMPATIBILITY_VERSION},
        )
        _create_sequence_map_table(connection, sequence_map_path)
        evidence_columns = _create_evidence_table(
            connection,
            fetched_by_sequence_id=fetched_by_sequence_id,
            evidence_schema=evidence_schema,
        )
        _create_public_relations(connection, evidence_columns=evidence_columns)
        connection.execute("CREATE SCHEMA _seqevi")
        _create_metadata_tables(
            connection,
            metadata=metadata,
            semantic_parameters=adapter_contract.semantic_parameters,
            run_metrics=run_metrics,
            evidence_columns=evidence_columns,
        )
        _add_catalog_comments(connection)
        _validate_result_file(
            connection,
            expected_input_record_count=input_record_count,
        )
        connection.execute("CHECKPOINT")
        connection.close()
        connection = None

        read_only = duckdb.connect(
            str(stage_path),
            read_only=True,
            config={"storage_compatibility_version": STORAGE_COMPATIBILITY_VERSION},
        )
        try:
            _validate_result_file(
                read_only,
                expected_input_record_count=input_record_count,
            )
        finally:
            read_only.close()
        os.replace(stage_path, output_path)
    except Exception:
        if connection is not None:
            connection.close()
        stage_path.unlink(missing_ok=True)
        raise
    finally:
        shutil.rmtree(temporary_dir, ignore_errors=True)


def scan_annotations(path: Path) -> duckdb.DuckDBPyRelation:
    """Open a published result read-only and return ``main.annotations``."""

    resolved = path.expanduser().resolve()
    if not resolved.is_file():
        raise OutputPackageError(f"result file does not exist: {resolved}")
    connection = duckdb.connect(
        str(resolved),
        read_only=True,
        config={"storage_compatibility_version": STORAGE_COMPATIBILITY_VERSION},
    )
    try:
        _validate_result_file(connection)
        return connection.sql("SELECT * FROM main.annotations")
    except Exception:
        connection.close()
        raise


def build_result_prototype(
    *,
    sequence_map: pl.DataFrame,
    evidence: pl.DataFrame,
    metadata: Mapping[str, str],
    semantic_parameters: Mapping[str, object],
    run_metrics: Mapping[str, object],
    database_path: Path | None = None,
) -> tuple[duckdb.DuckDBPyConnection, duckdb.DuckDBPyRelation]:
    """Build the Slice A result relations and return the annotations relation.

    The returned connection is intentionally returned alongside the relation so
    the prototype tests can close file handles deterministically.  DuckDB
    relations retain their connection when used by a caller, which is the
    behavior the later public API will rely on.
    """

    _validate_inputs(sequence_map, evidence, metadata)
    if database_path is not None:
        if database_path.exists():
            raise OutputPackageError(f"output path already exists: {database_path}")
        if not database_path.parent.is_dir():
            raise OutputPackageError(
                f"output parent directory does not exist: {database_path.parent}"
            )
        connection = duckdb.connect(
            str(database_path),
            config={"storage_compatibility_version": STORAGE_COMPATIBILITY_VERSION},
        )
    else:
        connection = duckdb.connect(
            ":memory:",
            config={"storage_compatibility_version": STORAGE_COMPATIBILITY_VERSION},
        )

    try:
        connection.register("_seqevi_sequence_map_input", sequence_map)
        connection.register("_seqevi_evidence_input", evidence)
        connection.execute(
            "CREATE TABLE main.sequence_map AS "
            "SELECT * FROM _seqevi_sequence_map_input ORDER BY InputOrder"
        )
        connection.execute(
            "CREATE TABLE main.evidence AS "
            "SELECT * FROM _seqevi_evidence_input ORDER BY SequenceID"
        )
        connection.unregister("_seqevi_sequence_map_input")
        connection.unregister("_seqevi_evidence_input")

        evidence_columns = list(evidence.columns)
        _create_public_relations(connection, evidence_columns=evidence_columns)

        connection.execute("CREATE SCHEMA _seqevi")
        _create_metadata_tables(
            connection,
            metadata=metadata,
            semantic_parameters=semantic_parameters,
            run_metrics=run_metrics,
            evidence_columns=evidence_columns,
        )
        _add_catalog_comments(connection)
        if database_path is not None:
            connection.execute("CHECKPOINT")
        relation = connection.sql("SELECT * FROM main.annotations")
        _validate_relation_contract(connection, relation)
        return connection, relation
    except Exception:
        connection.close()
        raise


def _validate_inputs_for_schema(
    evidence_schema: Mapping[str, pl.DataType],
    metadata: Mapping[str, str],
) -> None:
    if not evidence_schema or next(iter(evidence_schema)) != "SequenceID":
        raise OutputPackageError("evidence schema must start with SequenceID")
    if set(_SEQUENCE_MAP_COLUMNS).intersection(evidence_schema.keys() - {"SequenceID"}):
        raise OutputPackageError(
            "adapter evidence cannot reuse the common result column names"
        )
    missing_metadata = set(_REQUIRED_METADATA_COLUMNS) - metadata.keys()
    if missing_metadata:
        rendered = ", ".join(sorted(missing_metadata))
        raise OutputPackageError(f"result metadata is missing: {rendered}")
    if any(not isinstance(value, str) for value in metadata.values()):
        raise OutputPackageError("result metadata values must be strings")


def _write_sequence_map_tsv(
    *,
    path: Path,
    records: Iterable[InputSequence],
    input_record_count: int,
    fetched_by_sequence_id: Mapping[str, FetchedEvidence],
    source_by_sequence_id: Mapping[str, EvidenceSource],
) -> None:
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
    if row_count != input_record_count:
        raise OutputPackageError("sequence map does not account for every input")


def _create_sequence_map_table(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
) -> None:
    columns = {
        "InputOrder": "BIGINT",
        "InputID": "VARCHAR",
        "InputHeader": "VARCHAR",
        "SequenceID": "VARCHAR",
        "MD5": "VARCHAR",
        "Length": "BIGINT",
        "EvidenceStatus": "VARCHAR",
        "EvidenceSource": "VARCHAR",
    }
    connection.execute(
        "CREATE TABLE main.sequence_map AS "
        "SELECT * FROM read_csv(?, delim='\\t', header=true, columns=?) "
        "ORDER BY InputOrder",
        [str(path), columns],
    )


def _create_evidence_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    fetched_by_sequence_id: Mapping[str, FetchedEvidence],
    evidence_schema: Mapping[str, pl.DataType],
) -> list[str]:
    expected_schema = dict(evidence_schema)
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
        if artifact.digest != digest:
            raise OutputPackageError(
                f"normalized artifact reference has the wrong digest: {sequence_id}"
            )
        observed_schema = dict(pl.scan_parquet(artifact.path).collect_schema())
        if observed_schema != expected_schema:
            raise OutputPackageError(
                f"normalized artifact schema does not match adapter contract: {digest}"
            )
        sequence_ids_by_digest[digest].add(sequence_id)
        artifact_by_digest[digest] = artifact.path

    evidence_columns = list(expected_schema)
    seen_hit_ids: set[str] = set()
    first = True
    for digest in sorted(artifact_by_digest):
        owned_ids = sequence_ids_by_digest[digest]
        owned_frame = pl.DataFrame(
            {"SequenceID": sorted(owned_ids)},
            schema={"SequenceID": pl.String()},
        )
        connection.register("_seqevi_owned_ids", owned_frame)
        path_literal = _quote_literal(str(artifact_by_digest[digest]))
        selected_rows = connection.execute(
            "SELECT DISTINCT SequenceID FROM read_parquet("
            f"{path_literal}) WHERE SequenceID IN "
            "(SELECT SequenceID FROM _seqevi_owned_ids)"
        ).fetchall()
        selected_ids = {str(row[0]) for row in selected_rows}
        if selected_ids != owned_ids:
            missing = ", ".join(sorted(owned_ids - selected_ids)) or "<none>"
            raise OutputPackageError(
                f"normalized artifact has incomplete hit sequences: {missing}"
            )
        overlap = seen_hit_ids.intersection(selected_ids)
        if overlap:
            rendered = ", ".join(sorted(overlap))
            raise OutputPackageError(
                f"normalized artifacts duplicate hit sequences: {rendered}"
            )
        seen_hit_ids.update(selected_ids)
        select_sql = (
            "SELECT "
            + ", ".join(_quote_identifier(column) for column in evidence_columns)
            + " FROM read_parquet("
            + path_literal
            + ") WHERE SequenceID IN (SELECT SequenceID FROM _seqevi_owned_ids)"
        )
        if first:
            connection.execute("CREATE TABLE main.evidence AS " + select_sql)
            first = False
        else:
            connection.execute("INSERT INTO main.evidence " + select_sql)
        connection.unregister("_seqevi_owned_ids")

    if first:
        empty_frame = pl.DataFrame(schema=expected_schema)
        connection.register("_seqevi_empty_evidence", empty_frame)
        connection.execute(
            "CREATE TABLE main.evidence AS SELECT * FROM _seqevi_empty_evidence"
        )
        connection.unregister("_seqevi_empty_evidence")
    return evidence_columns


def _create_public_relations(
    connection: duckdb.DuckDBPyConnection,
    *,
    evidence_columns: list[str],
) -> None:
    select_columns = [
        f"s.{_quote_identifier(column)} AS {_quote_identifier(column)}"
        for column in _SEQUENCE_MAP_COLUMNS
    ]
    select_columns.extend(
        f"e.{_quote_identifier(column)} AS {_quote_identifier(column)}"
        for column in evidence_columns
        if column != "SequenceID"
    )
    connection.execute(
        "CREATE VIEW main.annotations AS "
        "SELECT " + ", ".join(select_columns) + " FROM main.sequence_map AS s "
        "LEFT JOIN main.evidence AS e ON s.SequenceID = e.SequenceID"
    )
    connection.execute(
        "CREATE VIEW main.no_hits AS "
        "SELECT SequenceID, max(MD5) AS MD5, max(Length) AS Length "
        "FROM main.sequence_map "
        "WHERE EvidenceStatus = 'no_hit' "
        "GROUP BY SequenceID"
    )


def _validate_result_file(
    connection: duckdb.DuckDBPyConnection,
    *,
    expected_input_record_count: int | None = None,
) -> None:
    setting = connection.execute(
        "SELECT current_setting('storage_compatibility_version')"
    ).fetchone()
    if setting is None or setting[0] != STORAGE_COMPATIBILITY_VERSION:
        raise OutputPackageError("result storage compatibility version is not fixed")
    if expected_input_record_count is not None:
        count = connection.execute("SELECT count(*) FROM main.sequence_map").fetchone()
        if count is None or int(count[0]) != expected_input_record_count:
            raise OutputPackageError("sequence map row count does not match input")
    relation = connection.sql("SELECT * FROM main.annotations")
    _validate_relation_contract(connection, relation)


def _validate_inputs(
    sequence_map: pl.DataFrame,
    evidence: pl.DataFrame,
    metadata: Mapping[str, str],
) -> None:
    if sequence_map.columns != list(_SEQUENCE_MAP_COLUMNS):
        raise OutputPackageError(
            "sequence_map must have the fixed columns in contract order"
        )
    if not evidence.columns or evidence.columns[0] != "SequenceID":
        raise OutputPackageError("evidence must start with SequenceID")
    if set(_SEQUENCE_MAP_COLUMNS).intersection(evidence.columns[1:]):
        raise OutputPackageError(
            "adapter evidence cannot reuse the common result column names"
        )
    missing_metadata = set(_REQUIRED_METADATA_COLUMNS) - metadata.keys()
    if missing_metadata:
        rendered = ", ".join(sorted(missing_metadata))
        raise OutputPackageError(f"result metadata is missing: {rendered}")
    if any(not isinstance(value, str) for value in metadata.values()):
        raise OutputPackageError("result metadata values must be strings")


def _create_metadata_tables(
    connection: duckdb.DuckDBPyConnection,
    *,
    metadata: Mapping[str, str],
    semantic_parameters: Mapping[str, object],
    run_metrics: Mapping[str, object],
    evidence_columns: list[str],
) -> None:
    metadata_columns = [*_REQUIRED_METADATA_COLUMNS]
    metadata_columns.extend(
        sorted(set(metadata).difference(_REQUIRED_METADATA_COLUMNS))
    )
    metadata_row = {column: metadata[column] for column in metadata_columns}
    metadata_frame = pl.DataFrame(
        [metadata_row],
        schema={column: pl.String for column in metadata_columns},
    )
    connection.register("_seqevi_metadata_input", metadata_frame)
    connection.execute(
        "CREATE TABLE _seqevi.metadata AS SELECT * FROM _seqevi_metadata_input"
    )
    connection.unregister("_seqevi_metadata_input")

    parameters_frame = pl.DataFrame(
        {
            "Parameter": sorted(semantic_parameters),
            "ValueJSON": [
                _json_value(semantic_parameters[name])
                for name in sorted(semantic_parameters)
            ],
        },
        schema={"Parameter": pl.String, "ValueJSON": pl.String},
    )
    connection.register("_seqevi_parameters_input", parameters_frame)
    connection.execute(
        "CREATE TABLE _seqevi.semantic_parameters AS "
        "SELECT * FROM _seqevi_parameters_input"
    )
    connection.unregister("_seqevi_parameters_input")

    relation_specs = (
        ("annotations", "view", "input record × adapter evidence row", "SequenceID"),
        ("sequence_map", "table", "one input FASTA record", "SequenceID"),
        ("evidence", "table", "adapter-native hit row", "SequenceID"),
        ("no_hits", "view", "one unique no-hit SequenceID", "SequenceID"),
    )
    table_rows = []
    for name, kind, row_grain, join_key in relation_specs:
        count = connection.execute(
            f"SELECT count(*) FROM main.{_quote_identifier(name)}"
        ).fetchone()
        assert count is not None
        table_rows.append(
            {
                "RelationName": name,
                "RelationKind": kind,
                "RowGrain": row_grain,
                "JoinKey": join_key,
                "RowCount": int(count[0]),
                "Description": _relation_description(name),
            }
        )
    table_frame = pl.DataFrame(
        table_rows,
        schema={
            "RelationName": pl.String,
            "RelationKind": pl.String,
            "RowGrain": pl.String,
            "JoinKey": pl.String,
            "RowCount": pl.Int64,
            "Description": pl.String,
        },
    )
    connection.register("_seqevi_table_info_input", table_frame)
    connection.execute(
        "CREATE TABLE _seqevi.table_info AS SELECT * FROM _seqevi_table_info_input"
    )
    connection.unregister("_seqevi_table_info_input")

    column_rows: list[dict[str, Any]] = []
    for relation_name, _kind, _row_grain, _join_key in relation_specs:
        description = connection.execute(
            f"DESCRIBE main.{_quote_identifier(relation_name)}"
        ).fetchall()
        for ordinal, row in enumerate(description, start=1):
            column_name = str(row[0])
            column_rows.append(
                {
                    "RelationName": relation_name,
                    "Ordinal": ordinal,
                    "ColumnName": column_name,
                    "DuckDBType": str(row[1]),
                    "Description": _column_description(column_name, evidence_columns),
                }
            )
    column_frame = pl.DataFrame(
        column_rows,
        schema={
            "RelationName": pl.String,
            "Ordinal": pl.Int64,
            "ColumnName": pl.String,
            "DuckDBType": pl.String,
            "Description": pl.String,
        },
    )
    connection.register("_seqevi_column_info_input", column_frame)
    connection.execute(
        "CREATE TABLE _seqevi.column_info AS SELECT * FROM _seqevi_column_info_input"
    )
    connection.unregister("_seqevi_column_info_input")

    metrics_frame = pl.DataFrame(
        {"MetricsJSON": [_json_value(dict(run_metrics))]},
        schema={"MetricsJSON": pl.String},
    )
    connection.register("_seqevi_metrics_input", metrics_frame)
    connection.execute(
        "CREATE TABLE _seqevi.run_metrics AS SELECT * FROM _seqevi_metrics_input"
    )
    connection.unregister("_seqevi_metrics_input")


def _validate_relation_contract(
    connection: duckdb.DuckDBPyConnection,
    relation: duckdb.DuckDBPyRelation,
) -> None:
    expected: list[str] = list(_SEQUENCE_MAP_COLUMNS)
    evidence_columns = [
        str(row[0]) for row in connection.execute("DESCRIBE main.evidence").fetchall()
    ]
    expected.extend(column for column in evidence_columns if column != "SequenceID")
    if relation.columns != expected:
        raise OutputPackageError(
            "annotations relation schema does not match the result contract"
        )
    for table in ("metadata", "table_info", "column_info"):
        count = connection.execute(
            f"SELECT count(*) FROM _seqevi.{_quote_identifier(table)}"
        ).fetchone()
        if count is None or int(count[0]) < 1:
            raise OutputPackageError(f"_seqevi.{table} is empty")
    metrics = connection.execute("SELECT count(*) FROM _seqevi.run_metrics").fetchone()
    if metrics is None or int(metrics[0]) != 1:
        raise OutputPackageError("_seqevi.run_metrics must contain one row")


def _add_catalog_comments(connection: duckdb.DuckDBPyConnection) -> None:
    for relation, description in (
        ("annotations", "Input-linked adapter annotation view."),
        ("sequence_map", "Input FASTA records and terminal evidence status."),
        ("evidence", "Adapter-native normalized hit evidence."),
        ("no_hits", "Unique canonical sequences with terminal no-hit evidence."),
    ):
        try:
            connection.execute(
                f"COMMENT ON TABLE main.{_quote_identifier(relation)} IS "
                f"{_quote_literal(description)}"
            )
        except duckdb.Error:
            # Comments are a convenience for IDEs; _seqevi is normative.
            pass


def _column_description(column_name: str, evidence_columns: list[str]) -> str:
    if column_name in _COMMON_COLUMN_DESCRIPTIONS:
        return _COMMON_COLUMN_DESCRIPTIONS[column_name]
    if column_name in evidence_columns:
        return "Adapter-native normalized evidence field."
    return "SeqEvi result catalog field."


def _relation_description(name: str) -> str:
    return {
        "annotations": "Input-linked adapter annotation view.",
        "sequence_map": "Input FASTA records and terminal evidence status.",
        "evidence": "Adapter-native normalized hit evidence.",
        "no_hits": "Unique canonical sequences with terminal no-hit evidence.",
    }[name]


def _json_value(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
