"""Run the frozen two-process C2 annotation and independent replay gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import psycopg

from seqevi.sequence import read_fasta


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _identity_set(path: Path) -> set[str]:
    return {record.identity.sequence_id for record in read_fasta(path)}


def _identity_digest(identities: set[str]) -> str:
    return hashlib.sha256("\n".join(sorted(identities)).encode()).hexdigest()


def _candidate_head() -> str:
    dirty = subprocess.check_output(
        [
            "git",
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            "src",
            "benchmarks",
        ],
        text=True,
    ).strip()
    if dirty:
        raise RuntimeError("C2 requires committed source and benchmark harnesses")
    return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()


def _external_tool_processes(output_root: Path) -> list[int]:
    marker = os.fsencode(str(output_root.resolve()))
    matches = []
    for entry in os.scandir("/proc"):
        if not entry.name.isdecimal() or int(entry.name) == os.getpid():
            continue
        try:
            command = Path(entry.path, "cmdline").read_bytes()
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        if marker in command and (b"emapper.py" in command or b"diamond" in command):
            matches.append(int(entry.name))
    return sorted(matches)


def _run_command(
    *,
    fasta: Path,
    output: Path,
    profile: str,
    store: str,
    threads: int,
    stdout_path: Path,
    stderr_path: Path,
) -> subprocess.Popen[bytes]:
    command = (
        sys.executable,
        "-m",
        "seqevi",
        "annotate",
        "--fasta",
        str(fasta),
        "--output",
        str(output),
        "--profile",
        profile,
        "--store",
        store,
        "--threads",
        str(threads),
        "--json",
    )
    stdout = stdout_path.open("wb")
    stderr = stderr_path.open("wb")
    try:
        process = subprocess.Popen(  # noqa: S603
            command,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
            env=os.environ.copy(),
        )
    finally:
        stdout.close()
        stderr.close()
    return process


def _result(path: Path, return_code: int) -> dict[str, object]:
    raw = path.read_text(encoding="utf-8").strip()
    if return_code != 0:
        raise RuntimeError(f"annotation exited {return_code}: {raw}")
    result = json.loads(raw)
    if not isinstance(result, dict):
        raise RuntimeError("annotation JSON result is not an object")
    return result


def _wait_initial(processes: dict[str, subprocess.Popen[bytes]]) -> dict[str, int]:
    return_codes: dict[str, int] = {}
    pending = dict(processes)
    while pending:
        for name, process in tuple(pending.items()):
            return_code = process.poll()
            if return_code is None:
                continue
            return_codes[name] = return_code
            del pending[name]
            if return_code != 0:
                for peer in pending.values():
                    try:
                        peer.send_signal(signal.SIGINT)
                    except ProcessLookupError:
                        pass
        if pending:
            time.sleep(0.1)
    return return_codes


def _reuse_counts(result: dict[str, object]) -> tuple[int, int]:
    counts = result.get("counts")
    if not isinstance(counts, dict):
        raise RuntimeError("annotation JSON result has no counts object")
    cache_hits = counts.get("cache_hits")
    computed = counts.get("computed")
    if not isinstance(cache_hits, int) or not isinstance(computed, int):
        raise RuntimeError("annotation JSON result has invalid reuse counts")
    return cache_hits, computed


def _database_readback(database_url: str) -> dict[str, int | str]:
    def scalar(cursor: psycopg.Cursor[tuple[object, ...]]) -> int:
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError("database count query returned no row")
        return cast(int, row[0])

    direct_url = database_url.replace("postgresql+psycopg://", "postgresql://")
    with psycopg.connect(direct_url) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM evidence")
        evidence_count = scalar(cursor)
        cursor.execute("SELECT sequence_id FROM evidence ORDER BY sequence_id")
        evidence_identities = {cast(str, row[0]) for row in cursor.fetchall()}
        cursor.execute("SELECT count(*) FROM claim_sessions")
        session_count = scalar(cursor)
        cursor.execute("SELECT count(*) FROM session_claims")
        claim_count = scalar(cursor)
        cursor.execute(
            """
            SELECT count(*)
            FROM evidence AS e
            LEFT JOIN artifact AS raw ON raw.digest = e.raw_artifact_digest
            LEFT JOIN artifact AS normalized
              ON normalized.digest = e.normalized_artifact_digest
            WHERE (e.raw_artifact_digest IS NOT NULL AND raw.digest IS NULL)
               OR (e.normalized_artifact_digest IS NOT NULL
                   AND normalized.digest IS NULL)
            """
        )
        missing_artifacts = scalar(cursor)
    return {
        "evidence": evidence_count,
        "evidence_identity_sha256": _identity_digest(evidence_identities),
        "claim_sessions": session_count,
        "session_claims": claim_count,
        "missing_artifact_references": missing_artifacts,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--blf", type=Path, required=True)
    parser.add_argument("--uniprot", type=Path, required=True)
    parser.add_argument("--profile", default="eggnog-5.0.2")
    parser.add_argument("--store", required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--threads", type=int, default=64)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--cleanup-wait-seconds", type=float, default=61.0)
    args = parser.parse_args()
    if args.output_root.exists():
        parser.error("--output-root must not already exist")
    source_head = _candidate_head()

    blf_ids = _identity_set(args.blf)
    uniprot_ids = _identity_set(args.uniprot)
    if len(blf_ids) != 9116 or len(uniprot_ids) != 9115:
        raise RuntimeError("frozen C2 input cardinalities do not match 9,116/9,115")
    if not uniprot_ids < blf_ids or len(blf_ids | uniprot_ids) != 9116:
        raise RuntimeError("frozen C2 overlap/subset contract is not satisfied")

    args.output_root.mkdir(parents=True)
    initial_started = time.perf_counter()
    processes = {}
    for name, fasta in (("blf", args.blf), ("uniprot", args.uniprot)):
        root = args.output_root / f"initial-{name}"
        root.mkdir()
        processes[name] = _run_command(
            fasta=fasta,
            output=root / "result.duckdb",
            profile=args.profile,
            store=args.store,
            threads=args.threads,
            stdout_path=root / "stdout.json",
            stderr_path=root / "stderr.log",
        )
    return_codes = _wait_initial(processes)
    initial_results = {
        name: _result(
            args.output_root / f"initial-{name}" / "stdout.json",
            return_codes[name],
        )
        for name in processes
    }
    initial_elapsed = time.perf_counter() - initial_started
    after_initial = _database_readback(args.database_url)

    replay_results = {}
    replay_started = time.perf_counter()
    for name, fasta in (("blf", args.blf), ("uniprot", args.uniprot)):
        root = args.output_root / f"replay-{name}"
        root.mkdir()
        process = _run_command(
            fasta=fasta,
            output=root / "result.duckdb",
            profile=args.profile,
            store=args.store,
            threads=args.threads,
            stdout_path=root / "stdout.json",
            stderr_path=root / "stderr.log",
        )
        replay_results[name] = _result(root / "stdout.json", process.wait())
    replay_elapsed = time.perf_counter() - replay_started

    if args.cleanup_wait_seconds > 0:
        time.sleep(args.cleanup_wait_seconds)
    final_readback = _database_readback(args.database_url)
    expected_replay = {"blf": 9116, "uniprot": 9115}
    initial_reuse = [_reuse_counts(result) for result in initial_results.values()]
    if (
        sum(item[1] for item in initial_reuse) != 9116
        or sum(item[0] for item in initial_reuse) != 9115
    ):
        raise RuntimeError("initial C2 results did not prove duplicate suppression")
    for name, expected in expected_replay.items():
        cache_hits, computed = _reuse_counts(replay_results[name])
        if cache_hits != expected or computed != 0:
            raise RuntimeError(f"{name} replay did not report exact cache reuse")
    expected_identity_digest = _identity_digest(blf_ids)
    if (
        after_initial["evidence"] != 9116
        or after_initial["evidence_identity_sha256"] != expected_identity_digest
        or final_readback
        != {
            "evidence": 9116,
            "evidence_identity_sha256": expected_identity_digest,
            "claim_sessions": 0,
            "session_claims": 0,
            "missing_artifact_references": 0,
        }
    ):
        raise RuntimeError("C2 database readback did not satisfy acceptance")
    external_tool_processes = _external_tool_processes(args.output_root)
    if external_tool_processes:
        raise RuntimeError(
            f"external tool processes remain after C2: {external_tool_processes}"
        )

    report = {
        "schema_version": 1,
        "status": "accepted",
        "recorded_at": datetime.now(UTC).isoformat(),
        "source_head": source_head,
        "python": platform.python_version(),
        "profile": args.profile,
        "threads_per_process": args.threads,
        "inputs": {
            "blf": {
                "path": str(args.blf.resolve()),
                "sha256": _sha256(args.blf),
                "unique_evidence_identities": len(blf_ids),
            },
            "uniprot": {
                "path": str(args.uniprot.resolve()),
                "sha256": _sha256(args.uniprot),
                "unique_evidence_identities": len(uniprot_ids),
            },
            "overlap": len(blf_ids & uniprot_ids),
            "union": len(blf_ids | uniprot_ids),
        },
        "initial": initial_results,
        "initial_elapsed_seconds": initial_elapsed,
        "after_initial_database": after_initial,
        "replay": replay_results,
        "replay_elapsed_seconds": replay_elapsed,
        "cleanup_wait_seconds": args.cleanup_wait_seconds,
        "finalized_existing_outputs": False,
        "remaining_external_tool_processes": external_tool_processes,
        "final_database": final_readback,
    }
    report_path = args.output_root / "acceptance.json"
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
