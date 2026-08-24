from __future__ import annotations

import inspect
from pathlib import Path

import duckdb
import pytest

import seqevi
import seqevi.api
from seqevi.adapters import AdapterName
from seqevi.annotate import AnnotationMetrics, AnnotationSummary
from seqevi.distribution.oci import OciAnnotationResult
from seqevi.errors import AnnotationError
from seqevi.execution_profile import ExecutionProfile, ManagedRuntime
from seqevi.progress import ProgressEvent, ProgressPhase
from seqevi.store import LocalStore

from .support import FixtureAdapter, write_fixture_database, write_fixture_tool


def test_public_annotate_returns_native_relation_and_scan_is_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein first\nMPEPTIDE\n", encoding="utf-8")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database")

    monkeypatch.setattr(
        seqevi.api,
        "create_adapter",
        lambda configuration: FixtureAdapter(
            executable=configuration.executable,
            database=configuration.database,
        ),
    )

    relation = seqevi.annotate(
        fasta,
        adapter="interpro-pfam",
        executable=executable,
        resource=database,
        store=tmp_path / "store",
        output=tmp_path / "annotations.duckdb",
    )

    assert isinstance(relation, duckdb.DuckDBPyRelation)
    assert "InputID" in relation.columns
    assert relation.filter("EvidenceStatus = 'hit'").count("*").fetchone() == (1,)
    assert relation.select("InputID", "Annotation").fetchall() == [("protein", "MPE")]
    assert relation.pl(lazy=True).collect().height == 1
    assert relation.to_arrow_reader().read_all().num_rows == 1

    scanned = seqevi.scan_annotations(tmp_path / "annotations.duckdb")
    assert scanned.columns == relation.columns
    assert scanned.fetchall() == relation.fetchall()
    relation.close()
    scanned.close()

    with duckdb.connect(
        str(tmp_path / "annotations.duckdb"),
        read_only=True,
        config={"storage_compatibility_version": "v1.0.0"},
    ) as connection:
        assert connection.table("_seqevi.column_info").filter(
            "RelationName = 'annotations'"
        ).count("*").fetchone() == (9,)


def test_public_annotate_does_not_export_progress_callback() -> None:
    assert "progress" not in inspect.signature(seqevi.annotate).parameters
    assert "progress_sink" not in inspect.signature(seqevi.annotate).parameters


def test_public_annotate_rejects_existing_result_without_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database")
    output = tmp_path / "annotations.duckdb"
    output.write_bytes(b"sentinel")
    monkeypatch.setattr(
        seqevi.api,
        "create_adapter",
        lambda configuration: FixtureAdapter(
            executable=configuration.executable,
            database=configuration.database,
        ),
    )

    with pytest.raises(AnnotationError, match="already exists"):
        seqevi.annotate(
            fasta,
            adapter="interpro-pfam",
            executable=executable,
            resource=database,
            store=tmp_path / "store",
            output=output,
        )
    assert output.read_bytes() == b"sentinel"


def test_application_progress_finalizes_across_store_close_and_result_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fasta = tmp_path / "proteins.fasta"
    fasta.write_text(">protein\nMPEPTIDE\n", encoding="utf-8")
    executable = write_fixture_tool(tmp_path / "fixture-tool")
    database = write_fixture_database(tmp_path / "database")
    trace: list[str] = []
    local_store = LocalStore.open(tmp_path / "store")

    class TracedStoreContext:
        def __enter__(self) -> LocalStore:
            trace.append("store_open")
            return local_store.__enter__()

        def __exit__(self, *error: object) -> None:
            trace.append("store_close")
            local_store.__exit__(*error)

    real_scan = seqevi.api._scan_annotations

    def traced_scan(path: Path) -> duckdb.DuckDBPyRelation:
        trace.append("result_scan")
        return real_scan(path)

    def record(event: ProgressEvent) -> None:
        trace.append(event.phase.value)

    monkeypatch.setattr(
        seqevi.api, "open_evidence_store", lambda _value: TracedStoreContext()
    )
    monkeypatch.setattr(seqevi.api, "_scan_annotations", traced_scan)

    invocation = seqevi.api._run_annotation_application(
        fasta=fasta,
        output=tmp_path / "annotations.duckdb",
        profile=None,
        config=None,
        adapter="interpro-pfam",
        executable=executable,
        resource=database,
        store=tmp_path / "store",
        threads=1,
        timeout_seconds=None,
        adapter_factory=lambda configuration: FixtureAdapter(
            executable=configuration.executable,
            database=configuration.database,
        ),
        progress_sink=record,
    )

    assert trace.index(ProgressPhase.FINALIZATION.value) < trace.index("store_close")
    assert trace.index("store_close") < trace.index("result_scan")
    assert trace.index("result_scan") < trace.index(ProgressPhase.COMPLETED.value)
    invocation.relation.close()


def test_managed_application_progress_wraps_unchanged_inner_invocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    profile = ExecutionProfile(
        source=tmp_path / "profile.toml",
        adapter=AdapterName.DBCAN_CAZYME,
        executable=None,
        resource=tmp_path / "resource",
        version=2,
        runtime=ManagedRuntime(
            kind="oci",
            kit_id="fixture-kit",
            engine="docker",
            image="ghcr.io/example/dbcan@sha256:" + "a" * 64,
        ),
    )
    inputs = seqevi.api.ResolvedAnnotationInputs(
        adapter=AdapterName.DBCAN_CAZYME,
        executable=None,
        resource=profile.resource,
        store=tmp_path / "store",
        threads=2,
        timeout_seconds=30.0,
        profile=profile,
    )
    metrics = AnnotationMetrics(
        elapsed_seconds=1.0,
        fasta_staging_seconds=0.0,
        store_lookup_seconds=0.0,
        adapter_seconds=0.0,
        external_tool_seconds=1.0,
        store_commit_seconds=0.0,
        store_fetch_seconds=0.0,
        package_seconds=0.0,
        peak_rss_kib=None,
        store_lookup_batches=0,
        store_commit_batches=0,
        store_fetch_batches=0,
        tool_batches=1,
        unique_artifact_reads=0,
        configured_threads=2,
    )
    summary = AnnotationSummary(1, 1, 0, 1, 1, 0, tmp_path / "result.duckdb", metrics)
    managed = OciAnnotationResult(
        output=summary.output_dir,
        summary=summary,
        adapter="dbcan-cazyme",
        result_schema_id="dbcan-cazyme/5",
    )
    trace: list[str] = []
    relation = duckdb.sql("SELECT 1 AS value")

    monkeypatch.setattr(seqevi.api, "resolve_annotation_inputs", lambda **_kw: inputs)

    def run_managed(**_kwargs: object) -> OciAnnotationResult:
        trace.append("managed_inner")
        return managed

    def scan_managed(_path: Path) -> duckdb.DuckDBPyRelation:
        trace.append("result_scan")
        return relation

    monkeypatch.setattr(seqevi.api, "run_oci_annotation", run_managed)
    monkeypatch.setattr(seqevi.api, "_scan_annotations", scan_managed)

    invocation = seqevi.api._run_annotation_application(
        fasta=tmp_path / "input.fasta",
        output=summary.output_dir,
        profile="managed",
        config=None,
        adapter=None,
        executable=None,
        resource=None,
        store=None,
        threads=None,
        timeout_seconds=None,
        progress_sink=lambda event: trace.append(event.phase.value),
    )

    assert trace == [
        ProgressPhase.ANNOTATION.value,
        ProgressPhase.ANNOTATION.value,
        "managed_inner",
        ProgressPhase.FINALIZATION.value,
        "result_scan",
        ProgressPhase.COMPLETED.value,
    ]
    invocation.relation.close()
