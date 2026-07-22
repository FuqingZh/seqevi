"""Run one official adapter benchmark and persist operational measurements."""

from __future__ import annotations

import argparse
import json
import platform
import time
from dataclasses import asdict
from pathlib import Path

from seqevi.adapters import AdapterConfiguration, AdapterName, create_adapter
from seqevi.annotate import run_annotation
from seqevi.store import open_evidence_store


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--adapter", choices=tuple(AdapterName), required=True)
    parser.add_argument("--fasta", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--store", required=True)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.threads < 1:
        parser.error("--threads must be positive")
    if args.report.exists():
        parser.error(f"--report already exists: {args.report}")
    if not args.report.parent.is_dir():
        parser.error(f"--report parent does not exist: {args.report.parent}")

    started = time.perf_counter()
    adapter = create_adapter(
        AdapterConfiguration(
            name=AdapterName(args.adapter),
            executable=args.executable.resolve(),
            database=args.database.resolve(),
        )
    )
    initialization_seconds = time.perf_counter() - started
    with open_evidence_store(args.store) as store:
        summary = run_annotation(
            fasta_path=args.fasta.resolve(),
            output_dir=args.output.resolve(),
            adapter=adapter,
            store=store,
            threads=args.threads,
        )

    report = {
        "schema_version": 1,
        "adapter": adapter.contract.name,
        "adapter_contract_version": adapter.contract.version,
        "resource_id": adapter.contract.resource_id,
        "tool_runtime_digest": adapter.contract.tool_runtime_digest,
        "semantic_parameters": dict(adapter.contract.semantic_parameters),
        "fasta": str(args.fasta.resolve()),
        "output": str(summary.output_dir),
        "store": args.store,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "initialization_seconds": initialization_seconds,
        "input_records": summary.input_records,
        "unique_sequences": summary.unique_sequences,
        "cache_hits": summary.cache_hits,
        "computed": summary.computed,
        "hits": summary.hits,
        "no_hits": summary.no_hits,
        "metrics": asdict(summary.metrics),
    }
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
