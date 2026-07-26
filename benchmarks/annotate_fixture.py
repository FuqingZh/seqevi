"""Measure SeqEvi orchestration without a third-party annotation database."""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import time
from dataclasses import asdict
from pathlib import Path

import polars as pl

from seqevi.annotate import AnnotationSummary, run_annotation
from seqevi.store import open_evidence_store
from seqevi.store.contract import EvidenceStore
from tests.support import FixtureAdapter, write_fixture_database, write_fixture_tool

_RESIDUES = "ACDEFGHIKLMNPQRSTVWY"


def _sequence(index: int, *, no_hit: bool) -> str:
    value = index
    encoded = []
    for _position in range(12):
        encoded.append(_RESIDUES[value % len(_RESIDUES)])
        value //= len(_RESIDUES)
    sequence = "M" + "".join(reversed(encoded))
    return sequence + "X" if no_hit else sequence


def _write_fasta(path: Path, indices: list[int]) -> None:
    with path.open("w", encoding="ascii", newline="\n") as handle:
        for index in indices:
            handle.write(f">protein-{index:09d}\n")
            handle.write(_sequence(index, no_hit=index % 5 == 0) + "\n")


def _run(
    *,
    name: str,
    indices: list[int],
    root: Path,
    adapter: FixtureAdapter,
    store: EvidenceStore,
    threads: int,
) -> AnnotationSummary:
    fasta = root / f"{name}.fasta"
    _write_fasta(fasta, indices)
    return run_annotation(
        fasta_path=fasta,
        output_dir=root / name,
        adapter=adapter,
        store=store,
        threads=threads,
    )


def _summary(summary: AnnotationSummary) -> dict[str, object]:
    return {
        "input_records": summary.input_records,
        "unique_sequences": summary.unique_sequences,
        "cache_hits": summary.cache_hits,
        "computed": summary.computed,
        "hits": summary.hits,
        "no_hits": summary.no_hits,
        "metrics": asdict(summary.metrics),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sequences", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--store",
        help="Local Store path or HTTP(S) shared Store URL; defaults under output.",
    )
    parser.add_argument(
        "--fresh-store-per-run",
        action="store_true",
        help="Open an independent Store client for each A/B/C run.",
    )
    parser.add_argument(
        "--sequence-offset",
        type=int,
        default=0,
        help="First deterministic sequence index; use a new offset for immutable reruns.",
    )
    parser.add_argument("--threads", type=int, default=1)
    args = parser.parse_args()
    if args.sequences < 2:
        parser.error("--sequences must be at least 2")
    if args.sequence_offset < 0:
        parser.error("--sequence-offset must not be negative")
    if args.threads < 1:
        parser.error("--threads must be positive")

    root = args.output.resolve()
    if root.exists():
        raise SystemExit(f"benchmark output already exists: {root}")
    root.parent.mkdir(parents=True, exist_ok=True)
    root.mkdir()
    started = time.perf_counter()
    try:
        executable = write_fixture_tool(root / "fixture-tool")
        database = write_fixture_database(root / "database")
        adapter = FixtureAdapter(executable=executable, database=database)
        count = args.sequences
        half = count // 2
        first = args.sequence_offset
        store_value = args.store or str(root / "store")
        run_specs = (
            ("a-new", list(range(first, first + count))),
            ("b-reused-subset", list(range(first, first + half))),
            (
                "c-half-new",
                [
                    *range(first, first + half),
                    *range(first + count, first + count + half),
                ],
            ),
        )
        if args.fresh_store_per_run:
            summaries = []
            for name, indices in run_specs:
                with open_evidence_store(store_value) as store:
                    summaries.append(
                        _run(
                            name=name,
                            indices=indices,
                            root=root,
                            adapter=adapter,
                            store=store,
                            threads=args.threads,
                        )
                    )
            run_a, run_b, run_c = summaries
        else:
            with open_evidence_store(store_value) as store:
                run_a, run_b, run_c = (
                    _run(
                        name=name,
                        indices=indices,
                        root=root,
                        adapter=adapter,
                        store=store,
                        threads=args.threads,
                    )
                    for name, indices in run_specs
                )

        if (
            run_a.cache_hits != 0
            or run_a.computed != count
            or run_b.cache_hits != half
            or run_b.computed != 0
            or run_c.cache_hits != half
            or run_c.computed != half
        ):
            raise RuntimeError("A/B/C reuse counts do not match the benchmark contract")
        if not pl.read_parquet(root / "a-new" / "evidence.parquet").schema:
            raise RuntimeError("benchmark evidence schema is empty")

        report = {
            "schema_version": 1,
            "sequences": count,
            "sequence_offset": args.sequence_offset,
            "threads": args.threads,
            "python": platform.python_version(),
            "platform": platform.platform(),
            "store": store_value,
            "fresh_store_per_run": args.fresh_store_per_run,
            "wall_seconds": time.perf_counter() - started,
            "runs": {
                "a_new": _summary(run_a),
                "b_reused_subset": _summary(run_b),
                "c_half_new": _summary(run_c),
            },
        }
        (root / "benchmark.json").write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(json.dumps(report, sort_keys=True))
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise


if __name__ == "__main__":
    main()
