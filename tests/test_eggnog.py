from __future__ import annotations

import gzip
import stat
import sys
from pathlib import Path

import pytest
from polars.testing import assert_frame_equal
from typer.testing import CliRunner

from seqevi.adapters import EGGNOG_EVIDENCE_SCHEMA, EggnogAdapter, EggnogParameters
from seqevi.annotate import run_annotation
from seqevi.cli import app
from seqevi.errors import AdapterError, AnnotationError, ResourceLockError
from seqevi.resource_lock import LOCK_FILENAME
from seqevi.sequence import read_fasta, unique_identities
from seqevi.store import LocalStore

from .support import read_result_table

runner = CliRunner()

_HEADER = "\t".join(
    (
        "#query",
        "seed_ortholog",
        "evalue",
        "score",
        "eggNOG_OGs",
        "max_annot_lvl",
        "COG_category",
        "Description",
        "Preferred_name",
        "GOs",
        "EC",
        "KEGG_ko",
        "KEGG_Pathway",
        "KEGG_Module",
        "KEGG_Reaction",
        "KEGG_rclass",
        "BRITE",
        "KEGG_TC",
        "CAZy",
        "BiGG_Reaction",
        "PFAMs",
    )
)


def _write_runtime(root: Path) -> tuple[Path, Path]:
    database = root / "eggnog-data"
    database.mkdir(parents=True)
    (database / "eggnog.db").write_bytes(b"annotations-v1")
    (database / "eggnog.taxa.db").write_bytes(b"taxonomy-v1")
    (database / "eggnog_proteins.dmnd").write_bytes(b"diamond-v1")
    (database / "mode.txt").write_text("success", encoding="utf-8")

    runtime = root / "runtime"
    runtime_bin = runtime / "bin"
    package = runtime / "lib" / "python3.10" / "site-packages" / "eggnogmapper"
    runtime_bin.mkdir(parents=True)
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "core.py").write_text("RUNTIME = 'fixture-v1'\n", encoding="utf-8")
    distribution = (
        runtime
        / "lib"
        / "python3.10"
        / "site-packages"
        / "eggnog_mapper-2.1.12.dist-info"
    )
    distribution.mkdir()
    (distribution / "RECORD").write_text(
        "eggnogmapper/core.py,,\n",
        encoding="utf-8",
    )

    executable = runtime_bin / "emapper.py"
    executable.write_text(_fixture_script(), encoding="utf-8")
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    (runtime_bin / "python").symlink_to(sys.executable)
    diamond = runtime_bin / "diamond"
    diamond.write_text("#!/bin/sh\necho 'diamond version 2.1.8'\n", encoding="utf-8")
    diamond.chmod(diamond.stat().st_mode | stat.S_IXUSR)
    return executable, database


def _fixture_script() -> str:
    return f"""#!/usr/bin/env python
import argparse
import sys
from pathlib import Path

if Path(sys.executable).resolve() != (Path(__file__).parent / "python").resolve():
    raise SystemExit(8)

if "--version" in sys.argv:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="store_true")
    parser.add_argument("--data_dir", required=True)
    args = parser.parse_args()
    installed = "5.0.2" if (Path(args.data_dir) / "eggnog.db").is_file() else "unknown"
    print("emapper-2.1.12 / Expected eggNOG DB version: 5.0.2 / "
          f"Installed eggNOG DB version: {{installed}} / "
          "Diamond version found: diamond version 2.1.8")
    raise SystemExit(0)

parser = argparse.ArgumentParser()
parser.add_argument("-i", dest="input", required=True)
parser.add_argument("--itype", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--output_dir", required=True)
parser.add_argument("--data_dir", required=True)
parser.add_argument("--cpu", required=True)
parser.add_argument("--override", action="store_true")
parser.add_argument("-m", dest="mode", required=True)
parser.add_argument("--seed_ortholog_evalue", required=True)
parser.add_argument("--tax_scope", required=True)
parser.add_argument("--target_orthologs", required=True)
parser.add_argument("--go_evidence", required=True)
parser.add_argument("--pfam_realign", required=True)
args = parser.parse_args()
if (args.itype, args.mode, args.tax_scope, args.target_orthologs,
        args.go_evidence, args.pfam_realign) != (
        "proteins", "diamond", "auto", "all", "non-electronic", "none"):
    raise SystemExit(9)

mode = (Path(args.data_dir) / "mode.txt").read_text().strip()
expected_threads = Path(args.data_dir) / "expected-threads.txt"
if expected_threads.is_file() and args.cpu != expected_threads.read_text().strip():
    raise SystemExit(10)
if mode == "fail":
    print("fixture eggNOG failure", file=sys.stderr)
    raise SystemExit(7)

ids = [line[1:] for line in Path(args.input).read_text().splitlines()
       if line.startswith(">")]
header = {_HEADER!r}
rows = []
for index, sequence_id in enumerate(ids):
    if index == 1:
        continue
    query = "UNKNOWN" if mode == "unknown-id" else sequence_id
    values = [
        query, "9606.ENSP000001", "1e-20", "100.0", "KOG0001@1|root",
        "2759|Eukaryota", "J", "Fixture protein", "fixture", "GO:0000001",
        "1.1.1.1", "ko:K00001", "map00010", "M00001", "R00001", "RC00001",
        "ko00000", "-", "-", "-", "PF00001,PF00002",
    ]
    if mode == "bad-score":
        values[3] = "nan"
    rows.append("\\t".join(values))
if mode == "duplicate" and rows:
    rows.append(rows[0])
if mode == "schema":
    header = header.replace("PFAMs", "ChangedColumn")
output = Path(args.output_dir) / f"{{args.output}}.emapper.annotations"
output.write_text("## fixture\\n" + header + "\\n" + "\\n".join(rows) + "\\n")
"""


def _write_fasta(path: Path) -> Path:
    path.write_text(">hit\nMPEPTIDE\n>no-hit\nMNOHITA\n", encoding="utf-8")
    return path


def test_eggnog_contract_hashes_runtime_resource_and_semantics(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    first = EggnogAdapter(executable=executable, database=database)

    assert first.contract.version == "eggnog/1"
    assert first.contract.resource_id.startswith("eggnog/5.0.2/sha256:")
    assert first.contract.semantic_parameters["search_mode"] == "diamond"
    assert first.contract.semantic_parameters["pfam_realign"] == "none"

    (database / "eggnog.db").write_bytes(b"annotations-v2")
    cached = EggnogAdapter(executable=executable, database=database)
    assert cached.contract.resource_id == first.contract.resource_id
    with pytest.raises(ResourceLockError, match=r"SHA-256.*eggnog\.db"):
        EggnogAdapter(
            executable=executable,
            database=database,
            verify_resource=True,
        )

    changed_executable, changed_database = _write_runtime(tmp_path / "changed")
    (changed_database / "eggnog.db").write_bytes(b"annotations-v2")
    changed = EggnogAdapter(
        executable=changed_executable,
        database=changed_database,
    )
    assert changed.contract.resource_id != first.contract.resource_id
    assert changed.contract.tool_runtime_digest == first.contract.tool_runtime_digest


def test_eggnog_parameters_reject_alternate_contract() -> None:
    with pytest.raises(ValueError, match="one fixed protein"):
        EggnogParameters(tax_scope="Metazoa")


def test_eggnog_runtime_digest_covers_mapper_package_and_diamond(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    first = EggnogAdapter(executable=executable, database=database)

    package_file = (
        executable.parent.parent
        / "lib"
        / "python3.10"
        / "site-packages"
        / "eggnogmapper"
        / "core.py"
    )
    package_file.write_text("RUNTIME = 'fixture-v2'\n", encoding="utf-8")
    package_changed = EggnogAdapter(executable=executable, database=database)
    assert (
        package_changed.contract.tool_runtime_digest
        != first.contract.tool_runtime_digest
    )

    diamond = executable.parent / "diamond"
    diamond.write_text(
        diamond.read_text(encoding="utf-8") + "# rebuilt\n",
        encoding="utf-8",
    )
    diamond_changed = EggnogAdapter(executable=executable, database=database)
    assert (
        diamond_changed.contract.tool_runtime_digest
        != package_changed.contract.tool_runtime_digest
    )

    record = (
        executable.parent.parent
        / "lib"
        / "python3.10"
        / "site-packages"
        / "eggnog_mapper-2.1.12.dist-info"
        / "RECORD"
    )
    record.write_text("eggnogmapper/core.py,sha256=changed,\n", encoding="utf-8")
    dependency_changed = EggnogAdapter(executable=executable, database=database)
    assert (
        dependency_changed.contract.tool_runtime_digest
        != diamond_changed.contract.tool_runtime_digest
    )


def test_eggnog_resource_identity_includes_optional_taxonomy_component(
    tmp_path: Path,
) -> None:
    first_executable, first_database = _write_runtime(tmp_path / "first")
    (first_database / "eggnog.taxa.db.traverse.pkl").write_bytes(b"traverse-v1")
    first = EggnogAdapter(executable=first_executable, database=first_database)

    second_executable, second_database = _write_runtime(tmp_path / "second")
    (second_database / "eggnog.taxa.db.traverse.pkl").write_bytes(b"traverse-v2")
    second = EggnogAdapter(executable=second_executable, database=second_database)

    assert second.contract.resource_id != first.contract.resource_id


def test_eggnog_annotation_preserves_native_columns_and_no_hits(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    adapter = EggnogAdapter(executable=executable, database=database)
    fasta = _write_fasta(tmp_path / "proteins.fasta")
    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
        )
        hit_identity = unique_identities(read_fasta(fasta))[0]
        fetched = store.fetch(adapter.contract.evidence_key(hit_identity))

    assert (summary.hits, summary.no_hits) == (1, 1)
    frame = read_result_table(summary.output_dir, "main.evidence")
    assert frame.schema == EGGNOG_EVIDENCE_SCHEMA
    assert frame.row(0, named=True)["seed_ortholog"] == "9606.ENSP000001"
    assert frame.row(0, named=True)["PFAMs"] == "PF00001,PF00002"
    assert frame.row(0, named=True)["KEGG_TC"] is None
    assert read_result_table(summary.output_dir, "main.no_hits").height == 1
    metadata = read_result_table(summary.output_dir, "_seqevi.metadata").row(
        0, named=True
    )
    assert metadata["Adapter"] == "eggnog"
    assert fetched is not None
    assert fetched.raw_artifact is not None
    with gzip.open(fetched.raw_artifact.path, "rt", encoding="utf-8") as handle:
        retained_raw = handle.read()
    assert "## fixture" not in retained_raw
    assert retained_raw.startswith("#query\t")


def test_eggnog_threads_change_execution_but_not_scientific_payload(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    adapter = EggnogAdapter(executable=executable, database=database)
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
    assert one.metrics.configured_threads == 1
    assert four.metrics.configured_threads == 4


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        ("unknown-id", "unknown SequenceID"),
        ("bad-score", "non-finite score"),
        ("duplicate", "duplicate query"),
        ("schema", "canonical eggNOG-mapper 2.x"),
    ],
)
def test_eggnog_rejects_invalid_annotations(
    tmp_path: Path, mode: str, message: str
) -> None:
    executable, database = _write_runtime(tmp_path)
    (database / "mode.txt").write_text(mode, encoding="utf-8")
    with LocalStore.open(tmp_path / "store") as store:
        with pytest.raises(AnnotationError, match=message):
            run_annotation(
                fasta_path=_write_fasta(tmp_path / "proteins.fasta"),
                output_dir=tmp_path / "output",
                adapter=EggnogAdapter(executable=executable, database=database),
                store=store,
            )


def test_eggnog_rejects_unprovable_version_or_database(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    executable.write_text(
        executable.read_text().replace("emapper-2.1.12", "emapper-3.0.0"),
        encoding="utf-8",
    )
    with pytest.raises(AdapterError, match="2.x release"):
        EggnogAdapter(executable=executable, database=database)

    executable, database = _write_runtime(tmp_path / "missing")
    (database / "eggnog.taxa.db").unlink()
    with pytest.raises(AdapterError, match="does not exist"):
        EggnogAdapter(executable=executable, database=database)


def test_eggnog_runs_through_public_cli(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    result = runner.invoke(
        app,
        [
            "annotate",
            "--adapter",
            "eggnog",
            "--fasta",
            str(_write_fasta(tmp_path / "proteins.fasta")),
            "--store",
            str(tmp_path / "store"),
            "--output",
            str(tmp_path / "output"),
            "--executable",
            str(executable),
            "--resource",
            str(database),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Annotated 2 unique sequences" in result.stdout
    assert "(0 cached, 2 computed)" in result.stdout


def test_eggnog_resource_verify_cli_creates_and_audits_lock(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    arguments = [
        "resource",
        "verify",
        "--adapter",
        "eggnog",
        "--executable",
        str(executable),
        "--resource",
        str(database),
    ]

    created = runner.invoke(app, arguments)

    assert created.exit_code == 0, created.output
    assert "Verified resource eggnog/5.0.2/sha256:" in created.stdout
    assert (database / LOCK_FILENAME).is_file()

    (database / "eggnog.db").write_bytes(b"annotations-v2")
    corrupted = runner.invoke(app, arguments)
    assert corrupted.exit_code == 1
    assert "SHA-256 does not match" in corrupted.stderr
