from __future__ import annotations

import os
import stat
import sys
from collections.abc import Mapping
from pathlib import Path

import pytest
from polars.testing import assert_frame_equal
from typer.testing import CliRunner

import seqevi.adapters.interpro_pfam as interpro_pfam_module
from seqevi.adapters import (
    ADAPTER_CONTRACT_VERSION,
    INTERPRO_PFAM_EVIDENCE_SCHEMA,
    InterProPfamAdapter,
    InterProPfamParameters,
)
from seqevi.annotate import run_annotation
from seqevi.cli import app
from seqevi.errors import AdapterError, AnnotationError, ResourceLockError
from seqevi.evidence import EvidenceQuery
from seqevi.runtime_identity import RuntimeComponent
from seqevi.sequence import read_fasta, unique_identities
from seqevi.store import LocalStore

from .support import read_result_table

runner = CliRunner()
JAVA_OPTION_VARIABLES = ("JDK_JAVA_OPTIONS", "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS")


def _write_jdk(root: Path, *, marker: bytes = b"v1") -> Path:
    java_home = root / "jdk"
    for relative in ("bin", "conf", "lib"):
        (java_home / relative).mkdir(parents=True)
    java = java_home / "bin" / "java"
    java.write_bytes(b"fixture-java-" + marker)
    java.chmod(java.stat().st_mode | stat.S_IXUSR)
    (java_home / "conf" / "security.properties").write_bytes(b"security-" + marker)
    (java_home / "lib" / "modules").write_bytes(b"modules-" + marker)
    (java_home / "release").write_bytes(b"JAVA_VERSION=17-" + marker)
    return java


@pytest.fixture(autouse=True)
def _fixture_java(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    java = _write_jdk(tmp_path / "fixture-java")
    monkeypatch.setenv("PATH", str(java.parent))


def _write_runtime(
    root: Path,
    *,
    version: str = "5.77-108.0",
) -> tuple[Path, Path]:
    install_dir = root / "interproscan"
    database = root / "interpro-data"
    hmmer_dir = install_dir / "bin" / "hmmer" / "hmmer3" / "3.3"
    model_dir = database / "pfam" / "38.1"
    hmmer_dir.mkdir(parents=True)
    model_dir.mkdir(parents=True)

    (install_dir / "interproscan-5.jar").write_bytes(b"fixture-jar-v1")
    (hmmer_dir / "hmmscan").write_bytes(b"fixture-hmmscan-v1")
    (model_dir / "pfam_a.hmm").write_bytes(b"fixture-pfam-model-v1")
    (model_dir / "pfam_a.dat").write_bytes(b"fixture-pfam-metadata-v1")
    (database / "mode.txt").write_text("success", encoding="utf-8")
    (database / "run-date.txt").write_text("21-07-2026", encoding="utf-8")
    (install_dir / "interproscan.properties").write_text(
        "data.directory=data\n"
        "bin.directory=bin\n"
        "binary.hmmer3.path=${bin.directory}/hmmer/hmmer3/3.3\n"
        "pfam-a.hmm.path=${data.directory}/pfam/38.1/pfam_a.hmm\n",
        encoding="utf-8",
    )

    executable = install_dir / "interproscan.sh"
    executable.write_text(
        _fixture_script(version),
        encoding="utf-8",
    )
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    return executable, database


def _fixture_script(version: str) -> str:
    return f"""#!{sys.executable}
import argparse
import hashlib
import os
import shutil
import sys
from pathlib import Path

expected_environment = Path(__file__).parent / "expected-environment.txt"
if expected_environment.is_file():
    expected = expected_environment.read_text().strip()
    if os.environ.get("SEQEVI_TEST_RUNTIME_ENV") != expected:
        raise SystemExit(13)

expected_path = Path(__file__).parent / "expected-path.txt"
if expected_path.is_file() and os.environ.get("PATH") != expected_path.read_text():
    raise SystemExit(14)

expected_java = Path(__file__).parent / "expected-java.txt"
if expected_java.is_file():
    selected_java = shutil.which("java")
    if selected_java is None or Path(selected_java).resolve() != Path(expected_java.read_text()):
        raise SystemExit(15)

expected_java_options = Path(__file__).parent / "expected-java-options.txt"
if expected_java_options.is_file():
    option_names = ("JDK_JAVA_OPTIONS", "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS")
    if any(os.environ.get(name) != "" for name in option_names):
        raise SystemExit(16)

if "--version" in sys.argv:
    print("InterProScan version {version}")
    raise SystemExit(0)

parser = argparse.ArgumentParser()
parser.add_argument("--input", required=True)
parser.add_argument("--applications", required=True)
parser.add_argument("--formats", required=True)
parser.add_argument("--disable-precalc", action="store_true")
parser.add_argument("--cpu", required=True)
parser.add_argument("--outfile", required=True)
parser.add_argument("--tempdir", required=True)
args = parser.parse_args()
if args.applications != "Pfam" or args.formats != "TSV":
    raise SystemExit(9)
if not args.disable_precalc:
    raise SystemExit(10)

properties = Path(os.environ["INTERPROSCAN_CONF"])
data_dir = None
for line in properties.read_text().splitlines():
    if line.startswith("data.directory="):
        data_dir = Path(line.split("=", 1)[1])
if data_dir is None:
    raise SystemExit(11)

mode = (data_dir / "mode.txt").read_text().strip()
expected_threads = data_dir / "expected-threads.txt"
if expected_threads.is_file() and args.cpu != expected_threads.read_text().strip():
    raise SystemExit(12)
run_date = (data_dir / "run-date.txt").read_text().strip()
if mode == "fail":
    print("fixture InterProScan failure", file=sys.stderr)
    raise SystemExit(7)
if mode == "missing-output":
    raise SystemExit(0)

records = []
header = None
sequence = []
for line in Path(args.input).read_text().splitlines():
    if line.startswith(">"):
        if header is not None:
            records.append((header, "".join(sequence)))
        header = line[1:]
        sequence = []
    else:
        sequence.append(line.strip())
if header is not None:
    records.append((header, "".join(sequence)))

lines = []
for sequence_id, sequence in records:
    if sequence.endswith("X"):
        continue
    md5 = hashlib.md5(sequence.encode(), usedforsecurity=False).hexdigest()
    accession = "UNKNOWN" if mode == "unknown-id" else sequence_id
    if mode == "bad-md5":
        md5 = "0" * 32
    analysis = "SMART" if mode == "wrong-analysis" else "Pfam"
    date = "2026-07-21" if mode == "bad-date" else run_date
    first = [
        accession, md5, str(len(sequence)), analysis, "PF00001", "Fixture domain",
        "1", str(min(3, len(sequence))), "1.0E-5", "T", date,
        "IPR000001", "Fixture InterPro entry", "-", "-",
    ]
    if mode == "unexpected-go":
        first[-2] = "GO:0000001"
    lines.append("\\t".join(first))
    if len(sequence) >= 5:
        second = [
            accession, md5, str(len(sequence)), analysis, "PF00002", "-",
            "2", "5", "2.5E-4", "T", date, "-", "-", "-", "-",
        ]
        lines.append("\\t".join(second))

if mode == "duplicate" and lines:
    lines.append(lines[0])
if mode == "malformed":
    lines = ["not\\ta\\tvalid\\tInterProScan\\trow"]
Path(args.tempdir).mkdir(parents=True, exist_ok=True)
Path(args.outfile).write_text("\\n".join(lines) + ("\\n" if lines else ""))
"""


def _write_input(path: Path) -> Path:
    path.write_text(
        ">hit protein\nMPEPTIDE\n>no-hit protein\nMNOHITX\n",
        encoding="utf-8",
    )
    return path


def test_interpro_pfam_contract_probes_exact_runtime_and_resource(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    first = InterProPfamAdapter(executable=executable, database=database)

    assert first.contract.version == ADAPTER_CONTRACT_VERSION
    assert first.contract.name == "interpro-pfam"
    assert first.contract.tool_runtime_digest.startswith("sha256:")
    assert first.contract.resource_id.startswith("interpro/108.0/pfam/38.1/sha256:")
    assert first.contract.semantic_parameters == {
        "application": "Pfam",
        "disable_precalculated_lookup": True,
        "include_go_terms": False,
        "include_pathways": False,
        "output_format": "TSV",
        "sequence_type": "protein",
    }

    (database / "pfam" / "38.1" / "pfam_a.hmm").write_bytes(b"fixture-pfam-model-v2")
    cached = InterProPfamAdapter(
        executable=executable,
        database=database,
    )
    assert cached.contract.resource_id == first.contract.resource_id
    with pytest.raises(ResourceLockError, match=r"SHA-256.*pfam_a\.hmm"):
        InterProPfamAdapter(
            executable=executable,
            database=database,
            verify_resource=True,
        )

    changed_executable, changed_database = _write_runtime(tmp_path / "changed")
    (changed_database / "pfam" / "38.1" / "pfam_a.hmm").write_bytes(
        b"fixture-pfam-model-v2"
    )
    resource_changed = InterProPfamAdapter(
        executable=changed_executable,
        database=changed_database,
    )
    assert resource_changed.contract.resource_id != first.contract.resource_id
    assert (
        resource_changed.contract.tool_runtime_digest
        == first.contract.tool_runtime_digest
    )

    (changed_executable.parent / "interproscan-5.jar").write_bytes(b"fixture-jar-v2")
    runtime_changed = InterProPfamAdapter(
        executable=changed_executable,
        database=changed_database,
    )
    assert (
        runtime_changed.contract.tool_runtime_digest
        != resource_changed.contract.tool_runtime_digest
    )
    assert runtime_changed.contract.resource_id == resource_changed.contract.resource_id

    (changed_executable.parent / "interproscan.properties").write_text(
        (changed_executable.parent / "interproscan.properties").read_text(
            encoding="utf-8"
        )
        + "exclude.sites.from.output=false\n",
        encoding="utf-8",
    )
    properties_changed = InterProPfamAdapter(
        executable=changed_executable,
        database=changed_database,
    )
    assert (
        properties_changed.contract.tool_runtime_digest
        != runtime_changed.contract.tool_runtime_digest
    )

    (changed_database / "pfam" / "38.1" / "pfam_a.dat").write_bytes(
        b"fixture-pfam-metadata-v2"
    )
    metadata_changed_executable, metadata_changed_database = _write_runtime(
        tmp_path / "metadata-changed"
    )
    (metadata_changed_database / "pfam" / "38.1" / "pfam_a.dat").write_bytes(
        b"fixture-pfam-metadata-v2"
    )
    metadata_changed = InterProPfamAdapter(
        executable=metadata_changed_executable,
        database=metadata_changed_database,
    )
    assert metadata_changed.contract.resource_id != first.contract.resource_id


def test_interpro_pfam_runtime_digest_includes_selected_java(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    java = _write_jdk(tmp_path / "selected-java")
    environment = {"PATH": str(java.parent)}

    first = InterProPfamAdapter(
        executable=executable,
        database=database,
        environment=environment,
    )
    java.write_bytes(b"fixture-java-v2")
    java.chmod(java.stat().st_mode | stat.S_IXUSR)
    second = InterProPfamAdapter(
        executable=executable,
        database=database,
        environment=environment,
    )

    assert second.contract.tool_runtime_digest != first.contract.tool_runtime_digest


@pytest.mark.parametrize(
    ("relative", "replacement"),
    (("lib/modules", b"changed-modules"), ("conf/security.properties", b"changed")),
)
def test_interpro_pfam_runtime_digest_covers_jdk_runtime_tree(
    tmp_path: Path, relative: str, replacement: bytes
) -> None:
    executable, database = _write_runtime(tmp_path)
    java = _write_jdk(tmp_path / "selected-java")
    environment = {"PATH": str(java.parent)}
    first = InterProPfamAdapter(
        executable=executable, database=database, environment=environment
    )

    (java.parent.parent / relative).write_bytes(replacement)
    second = InterProPfamAdapter(
        executable=executable, database=database, environment=environment
    )

    assert second.contract.tool_runtime_digest != first.contract.tool_runtime_digest


def test_interpro_pfam_runtime_digest_is_independent_of_jdk_path(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    first_java = _write_jdk(tmp_path / "first", marker=b"same")
    second_java = _write_jdk(tmp_path / "second", marker=b"same")

    first = InterProPfamAdapter(
        executable=executable,
        database=database,
        environment={"PATH": str(first_java.parent)},
    )
    second = InterProPfamAdapter(
        executable=executable,
        database=database,
        environment={"PATH": str(second_java.parent)},
    )

    assert second.contract.tool_runtime_digest == first.contract.tool_runtime_digest


def test_interpro_pfam_runtime_digest_tracks_launcher_path_not_java_home(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    path_java = _write_jdk(tmp_path / "path-jdk", marker=b"path")
    home_java = _write_jdk(tmp_path / "home-jdk", marker=b"home")
    environment = {
        "JAVA_HOME": str(home_java.parent.parent),
        "PATH": str(path_java.parent),
    }
    first = InterProPfamAdapter(
        executable=executable, database=database, environment=environment
    )

    (home_java.parent.parent / "lib" / "modules").write_bytes(b"unused-change")
    unused_changed = InterProPfamAdapter(
        executable=executable, database=database, environment=environment
    )
    (path_java.parent.parent / "lib" / "modules").write_bytes(b"executed-change")
    executed_changed = InterProPfamAdapter(
        executable=executable, database=database, environment=environment
    )

    assert (
        unused_changed.contract.tool_runtime_digest
        == first.contract.tool_runtime_digest
    )
    assert (
        executed_changed.contract.tool_runtime_digest
        != unused_changed.contract.tool_runtime_digest
    )


def test_interpro_pfam_runtime_digest_fails_on_incomplete_jdk(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    java = _write_jdk(tmp_path / "selected-java")
    (java.parent.parent / "lib" / "modules").unlink()
    (java.parent.parent / "lib").rmdir()

    with pytest.raises(AdapterError, match="runtime directory is missing"):
        InterProPfamAdapter(
            executable=executable,
            database=database,
            environment={"PATH": str(java.parent)},
        )


def test_interpro_pfam_runtime_digest_requires_java_11_modules(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    java = _write_jdk(tmp_path / "selected-java")
    (java.parent.parent / "lib" / "native.so").write_bytes(b"native")
    (java.parent.parent / "lib" / "modules").unlink()

    with pytest.raises(AdapterError, match=r"Java 11\+.*lib/modules"):
        InterProPfamAdapter(
            executable=executable,
            database=database,
            environment={"PATH": str(java.parent)},
        )


def test_interpro_pfam_runtime_digest_rejects_internal_jdk_symlink(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    java = _write_jdk(tmp_path / "selected-java")
    (java.parent.parent / "conf" / "linked.conf").symlink_to("security.properties")

    with pytest.raises(AdapterError, match="contains a symbolic link"):
        InterProPfamAdapter(
            executable=executable,
            database=database,
            environment={"PATH": str(java.parent)},
        )


def test_interpro_pfam_runtime_digest_rejects_symlinked_jdk_root(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    java = _write_jdk(tmp_path / "selected-java")
    java_home = java.parent.parent
    (java_home / "conf").rename(java_home / "conf-real")
    (java_home / "conf").symlink_to("conf-real", target_is_directory=True)

    with pytest.raises(AdapterError, match="directory is missing or a symbolic link"):
        InterProPfamAdapter(
            executable=executable,
            database=database,
            environment={"PATH": str(java.parent)},
        )


def test_interpro_pfam_runtime_digest_fails_on_jdk_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable, database = _write_runtime(tmp_path)
    java = _write_jdk(tmp_path / "selected-java")
    modules = java.parent.parent / "lib" / "modules"
    original = interpro_pfam_module.calculate_runtime_digest

    def calculate_then_drift(
        *,
        runtime_name: str,
        versions: Mapping[str, str],
        components: tuple[RuntimeComponent, ...],
    ) -> str:
        digest = original(
            runtime_name=runtime_name,
            versions=versions,
            components=components,
        )
        modules.write_bytes(b"drifted-after-hash")
        return digest

    monkeypatch.setattr(
        interpro_pfam_module, "calculate_runtime_digest", calculate_then_drift
    )

    with pytest.raises(AdapterError, match="JAVA_HOME changed while hashing"):
        InterProPfamAdapter(
            executable=executable,
            database=database,
            environment={"PATH": str(java.parent)},
        )


def test_interpro_pfam_applies_environment_to_probe_and_execution(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    (executable.parent / "expected-environment.txt").write_text(
        "profile-runtime",
        encoding="utf-8",
    )
    adapter = InterProPfamAdapter(
        executable=executable,
        database=database,
        environment={"SEQEVI_TEST_RUNTIME_ENV": "profile-runtime"},
    )

    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=_write_input(tmp_path / "proteins.fasta"),
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
            threads=1,
        )

    assert summary.computed == 2


def test_interpro_pfam_freezes_inherited_path_for_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable, database = _write_runtime(tmp_path)
    selected_java = _write_jdk(tmp_path / "selected-java", marker=b"selected")
    later_java = _write_jdk(tmp_path / "later-java", marker=b"later")
    selected_path = str(selected_java.parent)
    monkeypatch.setenv("PATH", selected_path)
    adapter = InterProPfamAdapter(executable=executable, database=database)
    frozen_path = os.pathsep.join((str(selected_java.parent.resolve()), selected_path))

    (executable.parent / "expected-path.txt").write_text(frozen_path, encoding="utf-8")
    monkeypatch.setenv("PATH", str(later_java.parent))
    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=_write_input(tmp_path / "proteins.fasta"),
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
            threads=1,
        )

    assert summary.computed == 2


def test_interpro_pfam_freezes_resolved_java_alias_for_execution(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    selected_java = _write_jdk(tmp_path / "selected-java", marker=b"selected")
    later_java = _write_jdk(tmp_path / "later-java", marker=b"later")
    alias_bin = tmp_path / "alias-bin"
    alias_bin.mkdir()
    alias = alias_bin / "java"
    alias.symlink_to(selected_java)
    adapter = InterProPfamAdapter(
        executable=executable,
        database=database,
        environment={"PATH": str(alias_bin)},
    )

    alias.unlink()
    alias.symlink_to(later_java)
    (executable.parent / "expected-java.txt").write_text(
        str(selected_java.resolve()), encoding="utf-8"
    )
    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=_write_input(tmp_path / "proteins.fasta"),
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
            threads=1,
        )

    assert summary.computed == 2


@pytest.mark.parametrize("name", JAVA_OPTION_VARIABLES)
@pytest.mark.parametrize("source", ("inherited", "profile"))
def test_interpro_pfam_rejects_java_option_injection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    source: str,
) -> None:
    executable, database = _write_runtime(tmp_path)
    environment: dict[str, str] = {}
    if source == "inherited":
        monkeypatch.setenv(name, "-javaagent:untracked.jar")
    else:
        environment[name] = "-javaagent:untracked.jar"

    with pytest.raises(AdapterError, match=f"non-empty {name}"):
        InterProPfamAdapter(
            executable=executable,
            database=database,
            environment=environment,
        )


def test_interpro_pfam_masks_java_options_after_construction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    executable, database = _write_runtime(tmp_path)
    adapter = InterProPfamAdapter(executable=executable, database=database)
    (executable.parent / "expected-java-options.txt").write_text(
        "empty", encoding="utf-8"
    )
    for name in JAVA_OPTION_VARIABLES:
        monkeypatch.setenv(name, "-javaagent:late.jar")

    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=_write_input(tmp_path / "proteins.fasta"),
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
            threads=1,
        )

    assert summary.computed == 2
    assert all(adapter.environment[name] == "" for name in JAVA_OPTION_VARIABLES)


def test_interpro_pfam_rejects_relative_runtime_path(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)

    with pytest.raises(AdapterError, match="non-empty absolute paths"):
        InterProPfamAdapter(
            executable=executable,
            database=database,
            environment={"PATH": f"{tmp_path}:relative-bin"},
        )


def test_interpro_pfam_parameters_reject_alternate_scientific_contract() -> None:
    with pytest.raises(ValueError, match="one fixed direct-scan"):
        InterProPfamParameters(disable_precalculated_lookup=False)


def test_interpro_pfam_annotation_preserves_matches_and_accounts_for_no_hits(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    adapter = InterProPfamAdapter(executable=executable, database=database)
    fasta = _write_input(tmp_path / "proteins.fasta")

    with LocalStore.open(tmp_path / "store") as store:
        summary = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "output",
            adapter=adapter,
            store=store,
        )

    assert summary.unique_sequences == 2
    assert summary.hits == 1
    assert summary.no_hits == 1
    frame = read_result_table(summary.output_dir, "main.evidence")
    assert frame.schema == INTERPRO_PFAM_EVIDENCE_SCHEMA
    assert frame.height == 2
    assert frame.get_column("SignatureAccession").to_list() == [
        "PF00001",
        "PF00002",
    ]
    assert "RunDate" not in frame.columns
    assert frame.row(1, named=True)["SignatureDescription"] is None
    assert read_result_table(summary.output_dir, "main.no_hits").height == 1

    metadata = read_result_table(summary.output_dir, "_seqevi.metadata").row(
        0, named=True
    )
    assert metadata["Adapter"] == "interpro-pfam"
    assert metadata["ResourceID"] == adapter.contract.resource_id


def test_interpro_threads_change_execution_but_not_scientific_payload(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    adapter = InterProPfamAdapter(executable=executable, database=database)
    fasta = _write_input(tmp_path / "proteins.fasta")
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


def test_interpro_pfam_cache_hit_does_not_rerun_tool(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    fasta = _write_input(tmp_path / "proteins.fasta")
    adapter = InterProPfamAdapter(executable=executable, database=database)

    with LocalStore.open(tmp_path / "store") as store:
        first = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "first",
            adapter=adapter,
            store=store,
        )
        (database / "mode.txt").write_text("fail", encoding="utf-8")
        second = run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "second",
            adapter=InterProPfamAdapter(
                executable=executable,
                database=database,
            ),
            store=store,
        )

    assert first.computed == 2
    assert second.computed == 0
    assert second.cache_hits == 2


def test_interpro_pfam_run_date_is_excluded_from_scientific_payload(
    tmp_path: Path,
) -> None:
    executable, database = _write_runtime(tmp_path)
    fasta = _write_input(tmp_path / "proteins.fasta")
    adapter = InterProPfamAdapter(executable=executable, database=database)

    with LocalStore.open(tmp_path / "first-store") as store:
        run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "first",
            adapter=adapter,
            store=store,
        )
    (database / "run-date.txt").write_text("22-07-2026", encoding="utf-8")
    with LocalStore.open(tmp_path / "second-store") as store:
        run_annotation(
            fasta_path=fasta,
            output_dir=tmp_path / "second",
            adapter=InterProPfamAdapter(
                executable=executable,
                database=database,
            ),
            store=store,
        )

    assert_frame_equal(
        read_result_table(tmp_path / "first", "main.evidence"),
        read_result_table(tmp_path / "second", "main.evidence"),
    )


@pytest.mark.parametrize(
    ("mode", "message"),
    [
        ("malformed", "expected 15"),
        ("unknown-id", "unknown SequenceID"),
        ("bad-md5", "MD5 does not match"),
        ("wrong-analysis", "not a Pfam match"),
        ("bad-date", "invalid run date"),
        ("unexpected-go", "outside the interpro-pfam/2 contract"),
        ("duplicate", "duplicate match"),
    ],
)
def test_interpro_pfam_rejects_invalid_native_tsv_without_caching(
    tmp_path: Path,
    mode: str,
    message: str,
) -> None:
    executable, database = _write_runtime(tmp_path)
    (database / "mode.txt").write_text(mode, encoding="utf-8")
    adapter = InterProPfamAdapter(executable=executable, database=database)
    fasta = _write_input(tmp_path / "proteins.fasta")
    identities = unique_identities(read_fasta(fasta))
    queries = tuple(
        EvidenceQuery(identity, adapter.contract.evidence_key(identity))
        for identity in identities
    )

    with LocalStore.open(tmp_path / "store") as store:
        with pytest.raises(AnnotationError, match=message):
            run_annotation(
                fasta_path=fasta,
                output_dir=tmp_path / "output",
                adapter=adapter,
                store=store,
            )
        assert store.lookup_many(queries) == {}

    assert not (tmp_path / "output").exists()


def test_interpro_pfam_rejects_unprovable_runtime_or_resource(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path, version="unknown")
    with pytest.raises(AdapterError, match="exactly one release"):
        InterProPfamAdapter(executable=executable, database=database)

    executable, database = _write_runtime(tmp_path / "missing-model")
    (database / "pfam" / "38.1" / "pfam_a.hmm").unlink()
    with pytest.raises(AdapterError, match="model file does not exist"):
        InterProPfamAdapter(executable=executable, database=database)


def test_interpro_pfam_runs_through_public_cli(tmp_path: Path) -> None:
    executable, database = _write_runtime(tmp_path)
    result = runner.invoke(
        app,
        [
            "annotate",
            "--adapter",
            "interpro-pfam",
            "--fasta",
            str(_write_input(tmp_path / "proteins.fasta")),
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
    assert (tmp_path / "output").is_file()
