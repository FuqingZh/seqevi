from __future__ import annotations

import gzip
import stat
import sys
from pathlib import Path

import pytest
from polars.testing import assert_frame_equal

from seqevi.adapters import DBCAN_EVIDENCE_SCHEMA, DBCanCazymeAdapter, DBCanParameters
from seqevi.annotate import run_annotation
from seqevi.errors import AdapterError, AnnotationError, ResourceLockError
from seqevi.sequence import read_fasta, unique_identities
from seqevi.store import LocalStore

from .support import read_result_table


_OVERVIEW_HEADER = "\t".join(
    (
        "Gene ID",
        "EC#",
        "dbCAN_hmm",
        "dbCAN_sub",
        "DIAMOND",
        "#ofTools",
        "Recommend Results",
        "Substrate",
    )
)


def _write_runtime(root: Path) -> tuple[Path, Path]:
    database = root / "dbcan-data"
    database.mkdir(parents=True)
    for name, content in (
        ("CAZy.dmnd", b"diamond-db-v1"),
        ("dbCAN.hmm", b"hmm-db-v1"),
        ("dbCAN-sub.hmm", b"sub-hmm-db-v1"),
        ("fam-substrate-mapping.tsv", b"GH1\tstarch\n"),
    ):
        (database / name).write_bytes(content)
    (database / "mode.txt").write_text("success", encoding="utf-8")

    runtime = root / "runtime"
    runtime_bin = runtime / "bin"
    package = runtime / "lib" / "python3.12" / "site-packages" / "dbcan"
    runtime_bin.mkdir(parents=True)
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VERSION = '5.2.9'\n", encoding="utf-8")
    (package / "cli.py").write_text("RUNTIME = 'fixture-v1'\n", encoding="utf-8")
    distribution = (
        runtime / "lib" / "python3.12" / "site-packages" / "dbcan-5.2.9.dist-info"
    )
    distribution.mkdir()
    (distribution / "RECORD").write_text(
        "dbcan/cli.py,,\n",
        encoding="utf-8",
    )

    executable = runtime_bin / "run_dbcan"
    executable.write_text(_fixture_script(), encoding="utf-8")
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    diamond = runtime_bin / "diamond"
    diamond.write_text("#!/bin/sh\necho 'diamond version 2.1.15'\n", encoding="utf-8")
    diamond.chmod(diamond.stat().st_mode | stat.S_IXUSR)
    return executable, database


def _fixture_script() -> str:
    return f"""#!{sys.executable}
import argparse
import sys
from pathlib import Path

if len(sys.argv) > 1 and sys.argv[1] == "version":
    print("dbCAN version: 5.2.9")
    raise SystemExit(0)

parser = argparse.ArgumentParser()
parser.add_argument("command")
parser.add_argument("--input_raw_data", required=True)
parser.add_argument("--mode", required=True)
parser.add_argument("--output_dir", required=True)
parser.add_argument("--db_dir", required=True)
parser.add_argument("--methods", required=True)
parser.add_argument("--threads", required=True)
parser.add_argument("--e_value_threshold", required=True)
parser.add_argument("--coverage_threshold_dbcan", required=True)
parser.add_argument("--e_value_threshold_dbcan", required=True)
parser.add_argument("--coverage_threshold_dbsub", required=True)
parser.add_argument("--e_value_threshold_dbsub", required=True)
args = parser.parse_args()
if args.command != "CAZyme_annotation" or args.mode != "protein":
    raise SystemExit(9)
if args.methods != "diamond,hmm,dbCANsub":
    raise SystemExit(10)
if (args.e_value_threshold, args.coverage_threshold_dbcan,
        args.e_value_threshold_dbcan, args.coverage_threshold_dbsub,
        args.e_value_threshold_dbsub) != (
        "1e-102", "0.35", "1e-15", "0.35", "1e-15"):
    raise SystemExit(11)
database = Path(args.db_dir)
expected_threads = database / "expected-threads.txt"
if expected_threads.is_file() and args.threads != expected_threads.read_text().strip():
    raise SystemExit(12)
mode = (database / "mode.txt").read_text().strip()
counter = database / "run-count.txt"
count = int(counter.read_text().strip()) if counter.is_file() else 0
counter.write_text(str(count + 1))
ids = [line[1:] for line in Path(args.input_raw_data).read_text().splitlines()
       if line.startswith(">")]
rows = []
for index, sequence_id in enumerate(ids):
    if index == 1:
        continue
    gene_id = "UNKNOWN" if mode == "unknown-id" else sequence_id
    rows.append("\\t".join((
        gene_id, "3.2.1.4", "GH1|4.2e-20|0.80", "GH1_sub|0.91",
        "CAZy00001|1e-40", "3", "GH1|starch degradation", "starch",
    )))
if mode == "duplicate" and rows:
    rows.append(rows[0])
if mode == "bad-tools" and rows:
    fields = rows[0].split("\\t")
    fields[5] = "4"
    rows[0] = "\\t".join(fields)
if mode == "bad-number" and rows:
    fields = rows[0].split("\\t")
    fields[5] = "many"
    rows[0] = "\\t".join(fields)
header = {_OVERVIEW_HEADER!r}
if mode == "schema":
    header = header.replace("Recommend Results", "ChangedColumn")
output = Path(args.output_dir)
output.mkdir(parents=True, exist_ok=True)
(output / "overview.tsv").write_text(
    header + "\\n" + "\\n".join(rows) + "\\n", encoding="utf-8"
)
"""


def _write_fasta(path: Path) -> Path:
    path.write_text(
        ">hit\nMPEPTIDE\n>no-hit\nMNOHITA\n",
        encoding="utf-8",
    )
    return path


def test_dbcan_contract_probes_runtime_and_resource(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    first = DBCanCazymeAdapter(executable=executable, database=database)

    assert first.contract.name == "dbcan-cazyme"
    assert first.contract.version == "dbcan-cazyme/1"
    assert first.contract.resource_id.startswith("dbcan/db_v5-2-9_5-5-2026/sha256:")
    assert first.contract.semantic_parameters == {
        "dbcan_coverage": 0.35,
        "dbcan_evalue": 1e-15,
        "dbcan_sub_coverage": 0.35,
        "dbcan_sub_evalue": 1e-15,
        "diamond_evalue": 1e-102,
        "methods": "diamond,hmm,dbCANsub",
        "mode": "protein",
    }

    (database / "dbCAN.hmm").write_bytes(b"hmm-db-v2")
    cached = DBCanCazymeAdapter(executable=executable, database=database)
    assert cached.contract.resource_id == first.contract.resource_id
    with pytest.raises(ResourceLockError, match=r"SHA-256.*dbCAN\.hmm"):
        DBCanCazymeAdapter(
            executable=executable,
            database=database,
            verify_resource=True,
        )


def test_dbcan_parameters_reject_alternate_scientific_contract() -> None:
    with pytest.raises(ValueError, match="one fixed protein"):
        DBCanParameters(methods="diamond")


def test_dbcan_runtime_digest_covers_package_diamond_and_relocation(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path / "first")
    first = DBCanCazymeAdapter(executable=executable, database=database)

    package_file = (
        executable.parent.parent
        / "lib"
        / "python3.12"
        / "site-packages"
        / "dbcan"
        / "cli.py"
    )
    package_file.write_text("RUNTIME = 'fixture-v2'\n", encoding="utf-8")
    package_changed = DBCanCazymeAdapter(executable=executable, database=database)
    assert (
        package_changed.contract.tool_runtime_digest
        != first.contract.tool_runtime_digest
    )

    diamond = executable.parent / "diamond"
    diamond.write_text(
        diamond.read_text(encoding="utf-8") + "# rebuilt\n",
        encoding="utf-8",
    )
    diamond_changed = DBCanCazymeAdapter(executable=executable, database=database)
    assert (
        diamond_changed.contract.tool_runtime_digest
        != package_changed.contract.tool_runtime_digest
    )

    moved_executable, moved_database = _write_runtime(tmp_path / "moved")
    assert (
        DBCanCazymeAdapter(
            executable=moved_executable,
            database=moved_database,
        ).contract.tool_runtime_digest
        == first.contract.tool_runtime_digest
    )


def test_dbcan_annotation_preserves_overview_and_no_hits(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    adapter = DBCanCazymeAdapter(executable=executable, database=database)
    fasta = _write_fasta(tmp_path / "proteins.fasta")

    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
            threads=3,
        )
        fetched = store.fetch(
            adapter.contract.evidence_key(unique_identities(read_fasta(fasta))[0])
        )

    assert (summary.hits, summary.no_hits, summary.computed) == (1, 1, 2)
    frame = read_result_table(summary.output_dir, "main.evidence")
    assert frame.schema == DBCAN_EVIDENCE_SCHEMA
    assert frame.row(0, named=True)["dbCAN_hmm"] == "GH1|4.2e-20|0.80"
    assert frame.row(0, named=True)["#ofTools"] == 3
    assert read_result_table(summary.output_dir, "main.no_hits").height == 1
    metadata = read_result_table(summary.output_dir, "_seqevi.metadata").row(
        0, named=True
    )
    assert metadata["ResultSchemaID"] == "dbcan-cazyme/5"
    assert metadata["UpstreamTool"] == "dbCAN"
    assert fetched is not None and fetched.raw_artifact is not None
    with gzip.open(fetched.raw_artifact.path, "rt", encoding="utf-8") as handle:
        assert handle.readline().rstrip("\n") == _OVERVIEW_HEADER


def test_dbcan_threads_do_not_change_scientific_payload(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    adapter = DBCanCazymeAdapter(executable=executable, database=database)
    fasta = _write_fasta(tmp_path / "proteins.fasta")
    (database / "expected-threads.txt").write_text("1", encoding="utf-8")
    with LocalStore.open(tmp_path / "one-store") as store:
        one = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "one",
            adapter=adapter,
            store=store,
            threads=1,
        )
    (database / "expected-threads.txt").write_text("4", encoding="utf-8")
    with LocalStore.open(tmp_path / "four-store") as store:
        four = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "four",
            adapter=adapter,
            store=store,
            threads=4,
        )

    assert_frame_equal(
        read_result_table(one.output_dir, "main.evidence"),
        read_result_table(four.output_dir, "main.evidence"),
    )


def test_dbcan_store_reuses_exact_and_partial_inputs(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    adapter = DBCanCazymeAdapter(executable=executable, database=database)
    first_fasta = _write_fasta(tmp_path / "first.fasta")
    second_fasta = tmp_path / "second.fasta"
    second_fasta.write_text(
        ">hit\nMPEPTIDE\n>no-hit\nMNOHITA\n>new\nMNEWSEQ\n",
        encoding="utf-8",
    )

    with LocalStore.open(tmp_path / "store") as store:
        first = run_annotation(
            fasta_path=first_fasta,
            output_dir=tmp_path / "first-output",
            adapter=adapter,
            store=store,
        )
        exact = run_annotation(
            fasta_path=first_fasta,
            output_dir=tmp_path / "exact-output",
            adapter=adapter,
            store=store,
        )
        partial = run_annotation(
            fasta_path=second_fasta,
            output_dir=tmp_path / "partial-output",
            adapter=adapter,
            store=store,
        )

    assert first.computed == 2
    assert exact.cache_hits == 2 and exact.computed == 0
    assert partial.cache_hits == 2 and partial.computed == 1
    assert (database / "run-count.txt").read_text(encoding="utf-8") == "2"


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        ("unknown-id", "unknown SequenceID"),
        ("duplicate", "duplicate Gene ID"),
        ("bad-tools", "invalid #ofTools"),
        ("bad-number", "invalid #ofTools"),
        ("schema", "unexpected header"),
    ],
)
def test_dbcan_rejects_invalid_overview(
    tmp_path: Path,
    mode: str,
    message: str,
) -> None:
    executable, database = _write_runtime(tmp_path)
    (database / "mode.txt").write_text(mode, encoding="utf-8")
    with LocalStore.open(tmp_path / "store") as store:
        with pytest.raises(AnnotationError, match=message):
            run_annotation(
                fasta_path=_write_fasta(tmp_path / "proteins.fasta"),
                output_dir=tmp_path / "output",
                adapter=DBCanCazymeAdapter(executable=executable, database=database),
                store=store,
            )


def test_dbcan_rejects_unprovable_runtime(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    executable.write_text(
        executable.read_text(encoding="utf-8").replace(
            "dbCAN version: 5.2.9", "dbCAN version: 5.3.0"
        ),
        encoding="utf-8",
    )
    with pytest.raises(AdapterError, match="unsupported dbCAN release"):
        DBCanCazymeAdapter(executable=executable, database=database)
