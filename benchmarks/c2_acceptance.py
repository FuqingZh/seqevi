"""Run the frozen two-process C2 annotation and independent replay gate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit
from uuid import uuid4

import duckdb
import httpx
import psycopg
from psycopg import sql
import seqevi.service.persistence as persistence_module

from seqevi.sequence import read_fasta
from seqevi.service.persistence import PostgresEvidencePersistence

_HARNESS_PATH = Path(__file__).resolve()
_REQUIRED_THREADS = 64
_REQUIRED_PROFILE = "eggnog-5.0.2"
_FROZEN_INPUT_SHA256 = {
    "blf": "9dc23bc3d230e097243110ac8e3a77df3e7f69c181d29290ed1d8d20e3e268d5",
    "uniprot": "a21f7da241177ebc75ab182e6ad77ff974ed4d21c41807953e0535266f0a7509",
}
_FROZEN_STATUS_COUNTS = {
    "blf": {"hits": 8777, "no_hits": 339},
    "uniprot": {"hits": 8776, "no_hits": 339},
}
_FROZEN_RESULT_IDENTITY = {
    "AdapterContractVersion": "eggnog/1",
    "ToolRuntimeDigest": (
        "sha256:c6fcd6164f8cb70724574c10e9c5eaa6df5134ce6c5f1ce9a13fe806b0eff3d3"
    ),
    "ResourceID": (
        "eggnog/5.0.2/"
        "sha256:77a3d83856104f0bdee2d6016fd7bc6e565e46e638b680ddc4679282e92d9ea6"
    ),
}
_OPEN_RECEIPT_RETENTION_SECONDS = 120.0
_ACTIVE_STORE_PROCESS: subprocess.Popen[bytes] | None = None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_frozen_inputs(paths: dict[str, Path]) -> dict[str, str]:
    observed = {name: _sha256(path) for name, path in paths.items()}
    if observed != _FROZEN_INPUT_SHA256:
        raise RuntimeError("C2 input SHA-256 values do not match the frozen inputs")
    return observed


def _validate_threads(threads: int) -> None:
    if threads != _REQUIRED_THREADS:
        raise ValueError(f"accepted C2 requires exactly {_REQUIRED_THREADS} threads")


def _validate_profile(profile: str) -> None:
    if profile != _REQUIRED_PROFILE:
        raise ValueError(f"accepted C2 requires profile {_REQUIRED_PROFILE}")


def _resolve_filesystem_arguments(args: argparse.Namespace) -> None:
    args.blf = args.blf.resolve()
    args.uniprot = args.uniprot.resolve()
    args.output_root = args.output_root.resolve()


def _identity_set(path: Path) -> set[str]:
    return {record.identity.sequence_id for record in read_fasta(path)}


def _identity_digest(identities: set[str]) -> str:
    return hashlib.sha256("\n".join(sorted(identities)).encode()).hexdigest()


def _candidate_head() -> str:
    repository_root = _HARNESS_PATH.parents[1]
    dirty = subprocess.check_output(
        [
            "git",
            "-C",
            str(repository_root),
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
    return subprocess.check_output(
        ["git", "-C", str(repository_root), "rev-parse", "HEAD"], text=True
    ).strip()


def _candidate_source_root() -> Path:
    harness_root = _HARNESS_PATH.parents[1]
    repository_root = Path(
        subprocess.check_output(
            ["git", "-C", str(harness_root), "rev-parse", "--show-toplevel"],
            text=True,
        ).strip()
    ).resolve()
    source_root = repository_root / "src"
    package_marker = source_root / "seqevi" / "__init__.py"
    if harness_root != repository_root or not package_marker.is_file():
        raise RuntimeError("C2 harness is not bound to the candidate source tree")
    persistence_path = Path(cast(str, persistence_module.__file__)).resolve()
    if not persistence_path.is_relative_to(source_root / "seqevi"):
        raise RuntimeError("C2 persistence is not imported from candidate source")
    subprocess.check_output(
        ["git", "ls-files", "--error-unmatch", "src/seqevi/__init__.py"],
        cwd=repository_root,
        stderr=subprocess.STDOUT,
    )
    return source_root


def _candidate_child_environment(source_root: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(source_root.resolve())
    return environment


def _child_environment_record(environment: Mapping[str, str]) -> dict[str, str]:
    return {"PYTHONPATH": environment["PYTHONPATH"]}


def _preflight_candidate_import(
    source_root: Path, environment: Mapping[str, str]
) -> str:
    repository_root = source_root.parent
    command = (
        sys.executable,
        "-P",
        "-c",
        "import pathlib, seqevi; print(pathlib.Path(seqevi.__file__).resolve())",
    )
    imported = Path(
        subprocess.check_output(
            command,
            cwd=repository_root,
            env=dict(environment),
            text=True,
            stderr=subprocess.STDOUT,
        ).strip()
    ).resolve()
    if not imported.is_relative_to(source_root / "seqevi"):
        raise RuntimeError("C2 child preflight imported a non-candidate seqevi")
    return str(imported)


def _controlled_store_bind(store: str) -> tuple[str, int]:
    parsed = urlsplit(store)
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or parsed.port is None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
    ):
        raise RuntimeError(
            "accepted C2 --store must be an explicit http://127.0.0.1:PORT bind"
        )
    return parsed.hostname, parsed.port


def _require_unoccupied_store_bind(host: str, port: int) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        if probe.connect_ex((host, port)) == 0:
            raise RuntimeError("accepted C2 Store bind is already occupied")


def _child_owns_store_listener(pid: int, host: str, port: int) -> bool:
    if host != "127.0.0.1":
        return False
    expected_address = f"0100007F:{port:04X}"
    listener_inodes: set[str] = set()
    try:
        lines = Path("/proc/net/tcp").read_text(encoding="ascii").splitlines()[1:]
        for line in lines:
            fields = line.split()
            if (
                len(fields) >= 10
                and fields[1] == expected_address
                and fields[3] == "0A"
            ):
                listener_inodes.add(fields[9])
        descriptors = tuple(Path(f"/proc/{pid}/fd").iterdir())
    except OSError as error:
        raise RuntimeError("candidate Store listener ownership is ambiguous") from error
    for descriptor in descriptors:
        try:
            target = descriptor.readlink().as_posix()
        except FileNotFoundError:
            continue
        except OSError as error:
            raise RuntimeError(
                "candidate Store listener ownership is ambiguous"
            ) from error
        if target.startswith("socket:[") and target[8:-1] in listener_inodes:
            return True
    return False


def _redacted_store_command(command: tuple[str, ...]) -> list[str]:
    recorded = list(command)
    database_index = recorded.index("--database-url") + 1
    recorded[database_index] = "<redacted>"
    return recorded


def _start_candidate_store(
    *,
    store: str,
    database_url: str,
    output_root: Path,
    source_root: Path,
    environment: Mapping[str, str],
) -> tuple[subprocess.Popen[bytes], dict[str, object]]:
    global _ACTIVE_STORE_PROCESS
    host, port = _controlled_store_bind(store)
    _require_unoccupied_store_bind(host, port)
    artifacts_dir = output_root / "store-artifacts"
    artifacts_dir.mkdir()
    command = (
        sys.executable,
        "-P",
        "-m",
        "seqevi",
        "serve",
        "--database-url",
        database_url,
        "--artifacts-dir",
        str(artifacts_dir),
        "--host",
        host,
        "--port",
        str(port),
    )
    stdout_path = output_root / "store.stdout.log"
    stderr_path = output_root / "store.stderr.log"
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        process = subprocess.Popen(  # noqa: S603
            command,
            cwd=source_root.parent,
            env=dict(environment),
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
    _ACTIVE_STORE_PROCESS = process
    deadline = time.monotonic() + 30.0
    try:
        while True:
            if _store_leader_exited_without_reap(process.pid):
                raise RuntimeError("candidate Store exited during startup")
            try:
                response = httpx.get(
                    f"{store.rstrip('/')}/v1/internal/claim-sessions/capabilities",
                    timeout=1.0,
                )
                response.raise_for_status()
                if not _child_owns_store_listener(process.pid, host, port):
                    raise RuntimeError(
                        "Store readiness response is not owned by candidate child"
                    )
                break
            except httpx.HTTPError:
                if time.monotonic() >= deadline:
                    raise RuntimeError("candidate Store did not become ready") from None
                time.sleep(0.1)
    except BaseException:
        _stop_candidate_store(process)
        raise
    return process, {
        "source": str(source_root / "seqevi"),
        "command": _redacted_store_command(command),
        "pid": process.pid,
        "cwd": str(source_root.parent),
        "artifacts_dir": str(artifacts_dir),
        "stopped_and_reaped": False,
    }


def _stop_candidate_store(process: subprocess.Popen[bytes]) -> None:
    global _ACTIVE_STORE_PROCESS
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    grace_deadline = time.monotonic() + 5.0
    while not _store_leader_exited_without_reap(process.pid):
        if time.monotonic() >= grace_deadline:
            break
        time.sleep(0.05)
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()
    if _ACTIVE_STORE_PROCESS is process:
        _ACTIVE_STORE_PROCESS = None


def _store_leader_exited_without_reap(pid: int) -> bool:
    try:
        observed = os.waitid(
            os.P_PID,
            pid,
            os.WEXITED | os.WNOHANG | os.WNOWAIT,
        )
    except ChildProcessError as error:
        raise RuntimeError(
            "candidate Store leader ownership was lost before group fence"
        ) from error
    return observed is not None


def _external_tool_processes(output_root: Path) -> list[int]:
    marker = os.fsencode(str(output_root.resolve()))
    matches = []
    try:
        entries = tuple(os.scandir("/proc"))
    except OSError as error:
        raise RuntimeError("external-tool process inspection is ambiguous") from error
    for entry in entries:
        if not entry.name.isdecimal() or int(entry.name) == os.getpid():
            continue
        try:
            command = Path(entry.path, "cmdline").read_bytes()
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as error:
            raise RuntimeError(
                f"external-tool process inspection is ambiguous for PID {entry.name}"
            ) from error
        if marker in command and (b"emapper.py" in command or b"diamond" in command):
            matches.append(int(entry.name))
    return sorted(matches)


def _require_no_external_tools(output_root: Path, stage: str) -> list[int]:
    processes = _external_tool_processes(output_root)
    if processes:
        raise RuntimeError(f"external tool processes remain {stage}: {processes}")
    return processes


def _run_command(
    *,
    fasta: Path,
    output: Path,
    profile: str,
    store: str,
    threads: int,
    stdout_path: Path,
    stderr_path: Path,
    environment: Mapping[str, str],
    candidate_source_root: Path,
) -> subprocess.Popen[bytes]:
    command = (
        sys.executable,
        "-P",
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
            env=dict(environment),
            cwd=candidate_source_root.parent,
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


def _stop_and_reap(
    processes: dict[str, subprocess.Popen[bytes]],
) -> BaseException | None:
    deferred: BaseException | None = None
    for process in processes.values():
        signalled = False
        while not signalled:
            try:
                if process.poll() is not None:
                    break
                process.send_signal(signal.SIGINT)
                signalled = True
            except ProcessLookupError:
                signalled = True
            except BaseException as error:
                deferred = deferred or error
    for process in processes.values():
        while True:
            try:
                process.wait()
                break
            except BaseException as error:
                deferred = deferred or error
    return deferred


def _wait_initial(processes: dict[str, subprocess.Popen[bytes]]) -> dict[str, int]:
    return_codes: dict[str, int] = {}
    pending = dict(processes)
    try:
        while pending:
            for name, process in tuple(pending.items()):
                return_code = process.poll()
                if return_code is None:
                    continue
                return_codes[name] = return_code
                del pending[name]
                if return_code != 0:
                    cleanup_error = _stop_and_reap(pending)
                    if cleanup_error is not None:
                        raise cleanup_error
            if pending:
                time.sleep(0.1)
    except BaseException:
        _stop_and_reap(processes)
        raise
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


def _status_counts(result: dict[str, object]) -> tuple[int, int]:
    counts = result.get("counts")
    if not isinstance(counts, dict):
        raise RuntimeError("annotation JSON result has no counts object")
    hits = counts.get("hits")
    no_hits = counts.get("no_hits")
    if not isinstance(hits, int) or not isinstance(no_hits, int):
        raise RuntimeError("annotation JSON result has invalid status counts")
    return hits, no_hits


def _validate_frozen_result(name: str, result: dict[str, object]) -> None:
    expected = _FROZEN_STATUS_COUNTS[name]
    if _status_counts(result) != (expected["hits"], expected["no_hits"]):
        raise RuntimeError(f"{name} result did not match frozen hit/no-hit totals")


def _validate_result_identity(name: str, result_path: Path) -> dict[str, str]:
    fields = (*_FROZEN_RESULT_IDENTITY, "InputDigest")
    with duckdb.connect(str(result_path), read_only=True) as connection:
        rows = connection.execute(
            "SELECT "
            + ", ".join(f'"{field}"' for field in fields)
            + " FROM _seqevi.metadata"
        ).fetchall()
    if len(rows) != 1:
        raise RuntimeError(f"{name} result has invalid identity metadata cardinality")
    observed = dict(zip(fields, rows[0], strict=True))
    expected = {**_FROZEN_RESULT_IDENTITY, "InputDigest": _FROZEN_INPUT_SHA256[name]}
    if observed != expected:
        raise RuntimeError(f"{name} result identity does not match frozen C2 metadata")
    return observed


def _database_counts(database_url: str, tables: tuple[str, ...]) -> dict[str, int]:
    direct_url = database_url.replace("postgresql+psycopg://", "postgresql://")
    with psycopg.connect(direct_url) as connection, connection.cursor() as cursor:
        counts = {}
        for table in tables:
            cursor.execute(
                sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))
            )
            row = cursor.fetchone()
            if row is None:
                raise RuntimeError(f"database count query returned no row for {table}")
            counts[table] = cast(int, row[0])
    return counts


def _database_has_session(database_url: str, session_id: str) -> bool:
    direct_url = database_url.replace("postgresql+psycopg://", "postgresql://")
    with psycopg.connect(direct_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM claim_sessions WHERE session_id = %s",
            (session_id,),
        )
        row = cursor.fetchone()
    return row is not None and row[0] == 1


def _remove_binding_probe(
    database_url: str, session_id: str, open_request_id: str
) -> None:
    direct_url = database_url.replace("postgresql+psycopg://", "postgresql://")
    with psycopg.connect(direct_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            DELETE FROM claim_session_open_receipts
            WHERE open_request_id = %s AND session_id = %s AND closed = 0
            """,
            (open_request_id, session_id),
        )
        receipt_rows = cursor.rowcount
        cursor.execute(
            "DELETE FROM claim_sessions WHERE session_id = %s AND state = 'closing'",
            (session_id,),
        )
        session_rows = cursor.rowcount
    if receipt_rows != 1 or session_rows != 1:
        raise RuntimeError("Store binding probe cleanup did not own exact rows")


def _validate_store_database_binding(store: str, database_url: str) -> str:
    parsed = urlsplit(store)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError("accepted C2 requires a shared HTTP Store endpoint")
    fresh_tables = (
        "sequence",
        "artifact",
        "evidence",
        "evidence_claim_generations",
        "claim_sessions",
        "session_claims",
        "claim_session_open_receipts",
        "claim_session_acquire_receipts",
        "claim_session_acquire_receipt_items",
    )
    fresh = _database_counts(database_url, fresh_tables)
    if any(fresh.values()):
        raise RuntimeError(f"accepted C2 requires a fresh PostgreSQL database: {fresh}")

    open_request_id = uuid4().hex
    authority: dict[str, object] | None = None
    with httpx.Client(base_url=store.rstrip("/"), timeout=30.0) as client:
        capabilities_response = client.get("/v1/internal/claim-sessions/capabilities")
        capabilities_response.raise_for_status()
        capabilities = capabilities_response.json()
        try:
            server_time = datetime.fromisoformat(cast(str, capabilities["server_time"]))
        except (KeyError, TypeError, ValueError) as error:
            raise RuntimeError("Store capabilities have invalid server_time") from error
        open_response = client.post(
            "/v1/internal/claim-sessions/open",
            json={
                "open_request_id": open_request_id,
                "server_time": server_time.isoformat(),
                "open_not_after": (server_time + timedelta(seconds=30)).isoformat(),
            },
        )
        open_response.raise_for_status()
        authority = cast(dict[str, object], open_response.json())
        session_id = authority.get("session_id")
        if not isinstance(session_id, str):
            raise RuntimeError("Store binding probe returned no session identity")
        try:
            if not _database_has_session(database_url, session_id):
                raise RuntimeError(
                    "shared Store endpoint is not bound to the readback database"
                )
        finally:
            close_response = client.post(
                "/v1/internal/claim-sessions/close",
                json={
                    field: authority[field]
                    for field in ("session_id", "owner_token", "generation")
                },
            )
            close_response.raise_for_status()
    _remove_binding_probe(database_url, session_id, open_request_id)
    after_probe = _database_counts(database_url, fresh_tables)
    if any(after_probe.values()):
        raise RuntimeError(
            f"Store binding probe did not restore fresh database: {after_probe}"
        )
    return open_request_id


def _database_readback(database_url: str) -> dict[str, object]:
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
        cursor.execute(
            """
            SELECT DISTINCT adapter_contract_version, tool_runtime_digest, resource_id
            FROM evidence
            ORDER BY adapter_contract_version, tool_runtime_digest, resource_id
            """
        )
        evidence_contract_identities = [
            {
                "AdapterContractVersion": cast(str, row[0]),
                "ToolRuntimeDigest": cast(str, row[1]),
                "ResourceID": cast(str, row[2]),
            }
            for row in cursor.fetchall()
        ]
        cursor.execute("SELECT count(*) FROM claim_sessions")
        session_count = scalar(cursor)
        cursor.execute("SELECT count(*) FROM session_claims")
        claim_count = scalar(cursor)
        cursor.execute("SELECT count(*) FROM claim_session_open_receipts")
        open_receipt_count = scalar(cursor)
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
        "evidence_contract_identities": evidence_contract_identities,
        "claim_sessions": session_count,
        "session_claims": claim_count,
        "claim_session_open_receipts": open_receipt_count,
        "missing_artifact_references": missing_artifacts,
    }


def _retention_cleanup(
    database_url: str, cleanup_wait_seconds: float
) -> dict[str, object]:
    if cleanup_wait_seconds <= _OPEN_RECEIPT_RETENTION_SECONDS:
        raise RuntimeError(
            "accepted C2 cleanup wait must exceed the 120-second receipt retention"
        )
    wait_started = time.monotonic()
    time.sleep(cleanup_wait_seconds)
    persistence = PostgresEvidencePersistence.open(database_url)
    sweep_calls = 0
    try:
        while persistence.sweep_claim_sessions():
            sweep_calls += 1
    finally:
        persistence.close()
    return {
        "required_retention_seconds": _OPEN_RECEIPT_RETENTION_SECONDS,
        "configured_wait_seconds": cleanup_wait_seconds,
        "observed_wait_seconds": time.monotonic() - wait_started,
        "sweep_calls_returning_work": sweep_calls,
        "persistence_source": str(
            Path(
                cast(str, sys.modules[PostgresEvidencePersistence.__module__].__file__)
            ).resolve()
        ),
    }


def _validate_database_acceptance(
    after_initial: dict[str, object],
    final_readback: dict[str, object],
    expected_identity_digest: str,
) -> None:
    if (
        after_initial["evidence"] != 9116
        or after_initial["evidence_identity_sha256"] != expected_identity_digest
        or after_initial["evidence_contract_identities"] != [_FROZEN_RESULT_IDENTITY]
        or final_readback
        != {
            "evidence": 9116,
            "evidence_identity_sha256": expected_identity_digest,
            "evidence_contract_identities": [_FROZEN_RESULT_IDENTITY],
            "claim_sessions": 0,
            "session_claims": 0,
            "claim_session_open_receipts": 0,
            "missing_artifact_references": 0,
        }
    ):
        raise RuntimeError("C2 database readback did not satisfy acceptance")


def _main() -> None:
    global _ACTIVE_STORE_PROCESS
    parser = argparse.ArgumentParser()
    parser.add_argument("--blf", type=Path, required=True)
    parser.add_argument("--uniprot", type=Path, required=True)
    parser.add_argument("--profile", default="eggnog-5.0.2")
    parser.add_argument("--store", required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--threads", type=int, default=64)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--cleanup-wait-seconds", type=float, default=121.0)
    args = parser.parse_args()
    _resolve_filesystem_arguments(args)
    try:
        _validate_threads(args.threads)
        _validate_profile(args.profile)
    except ValueError as error:
        parser.error(str(error))
    if args.output_root.exists():
        parser.error("--output-root must not already exist")
    source_head = _candidate_head()
    candidate_source_root = _candidate_source_root()
    child_environment = _candidate_child_environment(candidate_source_root)
    candidate_import = _preflight_candidate_import(
        candidate_source_root, child_environment
    )

    input_paths = {"blf": args.blf, "uniprot": args.uniprot}
    input_sha256 = _validate_frozen_inputs(input_paths)

    blf_ids = _identity_set(args.blf)
    uniprot_ids = _identity_set(args.uniprot)
    if len(blf_ids) != 9116 or len(uniprot_ids) != 9115:
        raise RuntimeError("frozen C2 input cardinalities do not match 9,116/9,115")
    if not uniprot_ids < blf_ids or len(blf_ids | uniprot_ids) != 9116:
        raise RuntimeError("frozen C2 overlap/subset contract is not satisfied")

    args.output_root.mkdir(parents=True)
    store_process, store_service = _start_candidate_store(
        store=args.store,
        database_url=args.database_url,
        output_root=args.output_root,
        source_root=candidate_source_root,
        environment=child_environment,
    )
    _ACTIVE_STORE_PROCESS = store_process
    store_binding_probe = _validate_store_database_binding(
        args.store, args.database_url
    )
    initial_started = time.perf_counter()
    processes = {}
    try:
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
                environment=child_environment,
                candidate_source_root=candidate_source_root,
            )
    except BaseException:
        _stop_and_reap(processes)
        raise
    return_codes = _wait_initial(processes)
    _require_no_external_tools(args.output_root, "immediately after initial children")
    initial_results = {
        name: _result(
            args.output_root / f"initial-{name}" / "stdout.json",
            return_codes[name],
        )
        for name in processes
    }
    for name, result in initial_results.items():
        _validate_frozen_result(name, result)
    initial_identity = {
        name: _validate_result_identity(
            name, args.output_root / f"initial-{name}" / "result.duckdb"
        )
        for name in initial_results
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
            environment=child_environment,
            candidate_source_root=candidate_source_root,
        )
        try:
            return_code = process.wait()
        except BaseException:
            _stop_and_reap({name: process})
            raise
        replay_results[name] = _result(root / "stdout.json", return_code)
        _validate_frozen_result(name, replay_results[name])
    _require_no_external_tools(args.output_root, "immediately after replay children")
    replay_identity = {
        name: _validate_result_identity(
            name, args.output_root / f"replay-{name}" / "result.duckdb"
        )
        for name in replay_results
    }
    replay_elapsed = time.perf_counter() - replay_started

    _stop_candidate_store(store_process)
    _ACTIVE_STORE_PROCESS = None
    store_service["stopped_and_reaped"] = True
    retention_cleanup = _retention_cleanup(args.database_url, args.cleanup_wait_seconds)
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
    _validate_database_acceptance(
        after_initial, final_readback, expected_identity_digest
    )
    external_tool_processes = _require_no_external_tools(
        args.output_root, "after retention cleanup"
    )

    report = {
        "schema_version": 1,
        "status": "accepted",
        "recorded_at": datetime.now(UTC).isoformat(),
        "source_head": source_head,
        "python": platform.python_version(),
        "profile": args.profile,
        "threads_per_process": args.threads,
        "annotation_child_environment": _child_environment_record(child_environment),
        "annotation_candidate_import": candidate_import,
        "store_service": store_service,
        "store_binding_probe_open_request_id": store_binding_probe,
        "inputs": {
            "blf": {
                "path": str(args.blf.resolve()),
                "sha256": input_sha256["blf"],
                "unique_evidence_identities": len(blf_ids),
            },
            "uniprot": {
                "path": str(args.uniprot.resolve()),
                "sha256": input_sha256["uniprot"],
                "unique_evidence_identities": len(uniprot_ids),
            },
            "overlap": len(blf_ids & uniprot_ids),
            "union": len(blf_ids | uniprot_ids),
        },
        "initial": initial_results,
        "initial_result_identity": initial_identity,
        "initial_elapsed_seconds": initial_elapsed,
        "after_initial_database": after_initial,
        "replay": replay_results,
        "replay_result_identity": replay_identity,
        "replay_elapsed_seconds": replay_elapsed,
        "cleanup_wait_seconds": args.cleanup_wait_seconds,
        "retention_cleanup": retention_cleanup,
        "finalized_existing_outputs": False,
        "remaining_external_tool_processes": external_tool_processes,
        "final_database": final_readback,
    }
    report_path = args.output_root / "acceptance.json"
    report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


def main() -> None:
    try:
        _main()
    finally:
        global _ACTIVE_STORE_PROCESS
        if _ACTIVE_STORE_PROCESS is not None:
            process = _ACTIVE_STORE_PROCESS
            _ACTIVE_STORE_PROCESS = None
            _stop_candidate_store(process)


if __name__ == "__main__":
    main()
