from __future__ import annotations

import json
import stat
import sys
from pathlib import Path

import polars as pl
import pytest
from typer.testing import CliRunner

from seqevi.adapters import EGGNOG_EVIDENCE_SCHEMA, EggnogAdapter, EggnogParameters
from seqevi.annotate import run_annotation
from seqevi.cli import app
from seqevi.errors import AdapterError, AnnotationError
from seqevi.store import LocalStore

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

    executable = root / "emapper.py"
    executable.write_text(_fixture_script(), encoding="utf-8")
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    return executable, database


def _fixture_script() -> str:
    return f"""#!{sys.executable}
import argparse
import sys
from pathlib import Path

if "--version" in sys.argv:
    print("emapper-2.1.12 / Expected eggNOG DB version: 5.0.2 / "
          "Installed eggNOG DB version: 5.0.2 / "
          "Diamond version found: diamond version 2.1.8")
    raise SystemExit(0)

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--itype", required=True)
parser.add_argument("--output", required=True)
parser.add_argument("--output_dir", required=True)
parser.add_argument("--data_dir", required=True)
parser.add_argument("--cpu", required=True)
parser.add_argument("--override", action="store_true")
parser.add_argument("--mode", required=True)
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
    changed = EggnogAdapter(executable=executable, database=database)
    assert changed.contract.resource_id != first.contract.resource_id
    assert changed.contract.tool_runtime_digest == first.contract.tool_runtime_digest


def test_eggnog_parameters_reject_alternate_contract() -> None:
    with pytest.raises(ValueError, match="one fixed protein"):
        EggnogParameters(tax_scope="Metazoa")


def test_eggnog_annotation_preserves_native_columns_and_no_hits(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    adapter = EggnogAdapter(executable=executable, database=database)
    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=_write_fasta(tmp_path / "proteins.fasta"),
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
        )

    assert (summary.hits, summary.no_hits) == (1, 1)
    frame = pl.read_parquet(summary.output_dir / "evidence.parquet")
    assert frame.schema == EGGNOG_EVIDENCE_SCHEMA
    assert frame.row(0, named=True)["seed_ortholog"] == "9606.ENSP000001"
    assert frame.row(0, named=True)["PFAMs"] == "PF00001,PF00002"
    assert frame.row(0, named=True)["KEGG_TC"] is None
    assert pl.read_parquet(summary.output_dir / "no-hits.parquet").height == 1
    package = json.loads((summary.output_dir / "datapackage.json").read_text())
    assert package["seqevi"]["adapter"] == "eggnog"


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
            "--database",
            str(database),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "2 unique sequences (0 cached, 2 computed)" in result.stdout
