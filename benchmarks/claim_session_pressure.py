"""Measure ClaimSession PostgreSQL scaling with production persistence code."""

from __future__ import annotations

import argparse
import contextvars
import hashlib
import json
import math
import platform
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterator, cast
from uuid import uuid4

import psycopg
from sqlalchemy import event, text

from seqevi.evidence import (
    ClaimDisposition,
    EvidenceKey,
    EvidenceQuery,
    EvidenceStatus,
    StoredArtifact,
)
from seqevi.sequence import identify_protein_sequence
from seqevi.service.persistence import PostgresEvidencePersistence
from seqevi.store.transport import (
    ArtifactReferenceModel,
    ClaimSessionFinalizeItem,
    CommitModel,
    EvidenceKeyModel,
    EvidenceQueryModel,
    SequenceModel,
    canonical_query_digest,
)

_RESIDUES = "ACDEFGHIKLMNPQRSTVWY"
_phase = contextvars.ContextVar("pressure_phase", default="other")
_PHASE_SQL_PER_OPERATION = {
    "acquire": 19,
    "renew": 5,
    "finalize": 15,
    "close": 5,
    "sweep": 10,
}
_OPERATION_DEADLINE_SECONDS = 5.0


def _records_sweep_delete_rows(phase: str) -> bool:
    return phase in {"sweep", "final_cleanup"}


def _candidate_head() -> str:
    dirty = (
        __import__("subprocess")
        .check_output(
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
        )
        .strip()
    )
    if dirty:
        raise RuntimeError("pressure requires committed source and benchmark harnesses")
    return (
        __import__("subprocess")
        .check_output(["git", "rev-parse", "HEAD"], text=True)
        .strip()
    )


def _sequence(index: int) -> str:
    encoded = []
    value = index
    for _ in range(12):
        encoded.append(_RESIDUES[value % len(_RESIDUES)])
        value //= len(_RESIDUES)
    return "M" + "".join(reversed(encoded))


@contextmanager
def _measured(phase: str) -> Iterator[None]:
    token = _phase.set(phase)
    try:
        yield
    finally:
        _phase.reset(token)


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(round((len(ordered) - 1) * percentile), len(ordered) - 1)
    return ordered[index]


def _validate_lane_report(
    report: dict[str, Any],
    *,
    expected_terminal: int,
    expected_sessions: int,
    expected_receipt_headers: int,
) -> None:
    claims = cast(int, report["claims"])
    expected_operations = {
        "acquire": math.ceil(claims / 1000),
        "renew": cast(int, report["heartbeat_calls"]),
        "finalize": math.ceil(claims / 1000),
        "close": 1,
        "sweep": 1,
    }
    if expected_operations["renew"] < 1:
        raise RuntimeError("pressure lane did not overlap heartbeat traffic")
    phases = cast(dict[str, dict[str, int | float]], report["phases"])
    for phase, operations in expected_operations.items():
        metrics = phases[phase]
        if metrics["operations"] != operations:
            raise RuntimeError(f"pressure {phase} operation count is not canonical")
        if metrics["sql_executions"] != operations * _PHASE_SQL_PER_OPERATION[phase]:
            raise RuntimeError(f"pressure {phase} SQL count is not bounded")
        if metrics["transactions"] != operations:
            raise RuntimeError(f"pressure {phase} transaction count is not bounded")
        if metrics["pool_checkouts"] != operations:
            raise RuntimeError(f"pressure {phase} pool checkout count is not bounded")
        latency = [
            cast(float, metrics[name])
            for name in (
                "p50_seconds",
                "p95_seconds",
                "p99_seconds",
                "maximum_seconds",
            )
        ]
        if (
            not all(math.isfinite(value) and value >= 0 for value in latency)
            or latency != sorted(latency)
            or latency[-1] >= _OPERATION_DEADLINE_SECONDS
        ):
            raise RuntimeError(f"pressure {phase} latency exceeded its invariant")
        pool_latency = [
            cast(float, metrics["pool_wait_p95_seconds"]),
            cast(float, metrics["pool_wait_maximum_seconds"]),
        ]
        if (
            not all(math.isfinite(value) and value >= 0 for value in pool_latency)
            or pool_latency != sorted(pool_latency)
            or pool_latency[-1] >= _OPERATION_DEADLINE_SECONDS
        ):
            raise RuntimeError(f"pressure {phase} pool wait exceeded its invariant")
    if report["http_status_counts"] != {"412": 0, "503": 0}:
        raise RuntimeError("pressure observed unexpected authority/backpressure status")
    residual = cast(dict[str, int], report["residual"])
    expected_residual = {
        "claim_sessions": expected_sessions,
        "session_claims": 0,
        "claim_session_acquire_receipts": expected_receipt_headers,
        "claim_session_acquire_receipt_items": expected_terminal,
    }
    if residual != expected_residual:
        raise RuntimeError(f"pressure lane residual is not canonical: {residual}")
    if report["terminal_evidence_total"] != expected_terminal:
        raise RuntimeError(
            f"pressure terminal evidence was {report['terminal_evidence_total']}, "
            f"expected {expected_terminal}"
        )
    delete_rows = cast(dict[str, dict[str, int]], report["sweep_delete_rows"])
    if any(values["maximum_rows"] > 1000 for values in delete_rows.values()):
        raise RuntimeError("pressure sweep exceeded its 1,000-row delete bound")


def _validate_final_cleanup(metrics: dict[str, Any]) -> None:
    operations = cast(int, metrics["calls_returning_work"]) + 1
    if (
        metrics["sql_executions"] != operations * _PHASE_SQL_PER_OPERATION["sweep"]
        or metrics["transactions"] != operations
        or metrics["pool_checkouts"] != operations
    ):
        raise RuntimeError("pressure final cleanup work is not bounded")
    pool_p95 = cast(float, metrics["pool_wait_p95_seconds"])
    pool_maximum = cast(float, metrics["pool_wait_maximum_seconds"])
    if (
        not all(
            math.isfinite(value) and value >= 0 for value in (pool_p95, pool_maximum)
        )
        or pool_p95 > pool_maximum
        or pool_maximum >= _OPERATION_DEADLINE_SECONDS
    ):
        raise RuntimeError("pressure final cleanup pool wait exceeded its invariant")
    delete_rows = cast(dict[str, dict[str, int]], metrics["delete_rows"])
    if any(values["maximum_rows"] > 1000 for values in delete_rows.values()):
        raise RuntimeError("pressure final cleanup exceeded its 1,000-row delete bound")


def _queries(count: int, lane: int) -> tuple[EvidenceQuery, ...]:
    queries = []
    semantic = json.dumps(
        {"benchmark": "claim-session-pressure", "lane": lane},
        sort_keys=True,
        separators=(",", ":"),
    )
    for index in range(count):
        identity = identify_protein_sequence(_sequence(index))
        key = EvidenceKey(
            sequence_id=identity.sequence_id,
            adapter_contract_version="pressure/1",
            tool_runtime_digest="sha256:" + "1" * 64,
            resource_id="pressure-resource",
            semantic_parameters_json=semantic,
        )
        queries.append(EvidenceQuery(identity, key))
    return tuple(queries)


def _finalize_item(query: EvidenceQuery, generation: int) -> ClaimSessionFinalizeItem:
    raw = ArtifactReferenceModel(
        digest="2" * 64,
        media_type="application/octet-stream",
        byte_size=0,
    )
    return ClaimSessionFinalizeItem(
        commit=CommitModel(
            identity=SequenceModel.from_domain(query.identity),
            key=EvidenceKeyModel.from_domain(query.key),
            status=EvidenceStatus.NO_HIT,
            payload_digest=hashlib.sha256(query.key.sequence_id.encode()).hexdigest(),
            normalized_artifact=None,
            raw_artifact=raw,
        ),
        claim_generation=generation,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--counts", type=int, nargs="+", default=[100, 1000, 3000, 9116]
    )
    parser.add_argument("--cleanup-wait-seconds", type=float, default=61.0)
    args = parser.parse_args()
    if any(count < 1 for count in args.counts):
        parser.error("--counts values must be positive")
    if args.report.exists():
        parser.error("--report must not already exist")
    source_head = _candidate_head()

    persistence = PostgresEvidencePersistence.open(
        args.database_url, pool_size=32, max_overflow=32
    )
    statement_counts: dict[str, int] = defaultdict(int)
    statement_started: dict[int, tuple[str, float]] = {}
    statement_latencies: dict[str, list[float]] = defaultdict(list)
    transaction_counts: dict[str, int] = defaultdict(int)
    pool_waits: dict[str, list[float]] = defaultdict(list)
    sweep_delete_rows: dict[str, list[int]] = defaultdict(list)
    metrics_lock = threading.Lock()

    pool = cast(Any, persistence.engine.pool)
    original_do_get = pool._do_get

    def measured_do_get():
        phase = _phase.get()
        started = time.perf_counter()
        try:
            return original_do_get()
        finally:
            with metrics_lock:
                pool_waits[phase].append(time.perf_counter() - started)

    pool._do_get = measured_do_get

    def before_cursor_execute(
        _connection, _cursor, _statement, _parameters, context, _executemany
    ) -> None:
        phase = _phase.get()
        with metrics_lock:
            statement_counts[phase] += 1
            statement_started[id(context)] = (phase, time.perf_counter())

    def after_cursor_execute(
        _connection, cursor, statement, _parameters, context, _executemany
    ) -> None:
        with metrics_lock:
            phase, started = statement_started.pop(id(context))
            statement_latencies[phase].append(time.perf_counter() - started)
            normalized = " ".join(statement.lower().split())
            if _records_sweep_delete_rows(phase) and normalized.startswith(
                "delete from "
            ):
                table = normalized.removeprefix("delete from ").split()[0]
                sweep_delete_rows[table].append(max(cursor.rowcount, 0))

    def after_transaction(_connection) -> None:
        with metrics_lock:
            transaction_counts[_phase.get()] += 1

    event.listen(persistence.engine, "before_cursor_execute", before_cursor_execute)
    event.listen(persistence.engine, "after_cursor_execute", after_cursor_execute)
    event.listen(persistence.engine, "commit", after_transaction)
    event.listen(persistence.engine, "rollback", after_transaction)
    activity_stop = threading.Event()
    activity_summary = {
        "samples": 0,
        "maximum_sessions": 0,
        "maximum_active": 0,
        "maximum_lock_waiters": 0,
    }
    activity_wait_events: dict[str, int] = defaultdict(int)
    activity_errors: list[str] = []
    direct_url = args.database_url.replace("postgresql+psycopg://", "postgresql://")

    def sample_activity() -> None:
        try:
            with psycopg.connect(direct_url, autocommit=True) as connection:
                with connection.cursor() as cursor:
                    while not activity_stop.wait(0.01):
                        cursor.execute(
                            """
                            SELECT state, wait_event_type, wait_event
                            FROM pg_stat_activity
                            WHERE datname = current_database()
                              AND pid <> pg_backend_pid()
                            """
                        )
                        rows = cursor.fetchall()
                        lock_waiters = sum(row[1] == "Lock" for row in rows)
                        with metrics_lock:
                            activity_summary["samples"] += 1
                            activity_summary["maximum_sessions"] = max(
                                activity_summary["maximum_sessions"], len(rows)
                            )
                            activity_summary["maximum_active"] = max(
                                activity_summary["maximum_active"],
                                sum(row[0] == "active" for row in rows),
                            )
                            activity_summary["maximum_lock_waiters"] = max(
                                activity_summary["maximum_lock_waiters"], lock_waiters
                            )
                            for _state, wait_type, wait_event in rows:
                                if wait_type is not None:
                                    activity_wait_events[
                                        f"{wait_type}:{wait_event}"
                                    ] += 1
        except Exception as error:  # pragma: no cover - benchmark evidence
            activity_errors.append(repr(error))

    activity_thread = threading.Thread(target=sample_activity)
    activity_thread.start()
    reports = []
    raw_artifact = StoredArtifact(
        digest="2" * 64,
        media_type="application/octet-stream",
        byte_size=0,
        relative_path="pressure/empty",
    )
    try:
        with persistence.engine.connect() as connection:
            postgres_version = connection.execute(text("SELECT version()")).scalar_one()
            settings = {
                name: connection.execute(text(f"SHOW {name}")).scalar_one()
                for name in (
                    "max_connections",
                    "shared_buffers",
                    "transaction_timeout",
                )
            }
        for lane, count in enumerate(args.counts, start=1):
            queries = _queries(count, lane)
            server_time = persistence.database_time()
            authority = persistence.open_claim_session(
                open_request_id=uuid4().hex,
                server_time=server_time,
                open_not_after=server_time + timedelta(seconds=30),
            )
            acquired = []
            operation_latencies: dict[str, list[float]] = defaultdict(list)
            start_counts = dict(statement_counts)
            start_transactions = dict(transaction_counts)
            start_pool_waits = {
                phase: len(values) for phase, values in pool_waits.items()
            }
            start_sweep_deletes = {
                table: len(values) for table, values in sweep_delete_rows.items()
            }

            heartbeat_stop = threading.Event()
            heartbeat_errors: list[str] = []
            heartbeat_calls = 0

            def heartbeat() -> None:
                nonlocal authority, heartbeat_calls
                while not heartbeat_stop.wait(0.01):
                    try:
                        started = time.perf_counter()
                        with _measured("renew"):
                            authority = persistence.renew_claim_session(authority)
                        operation_latencies["renew"].append(
                            time.perf_counter() - started
                        )
                        heartbeat_calls += 1
                    except Exception as error:  # pragma: no cover - benchmark evidence
                        heartbeat_errors.append(repr(error))
                        return

            heartbeat_thread = threading.Thread(target=heartbeat)
            heartbeat_thread.start()
            try:
                for offset in range(0, count, 1000):
                    batch = queries[offset : offset + 1000]
                    models = [EvidenceQueryModel.from_domain(query) for query in batch]
                    started = time.perf_counter()
                    with _measured("acquire"):
                        outcomes = persistence.acquire_claim_session(
                            authority,
                            acquire_request_id=uuid4().hex,
                            query_digest=canonical_query_digest(models),
                            queries=batch,
                        )
                    operation_latencies["acquire"].append(time.perf_counter() - started)
                    if any(
                        outcome.disposition is not ClaimDisposition.ACQUIRED
                        for outcome in outcomes
                    ):
                        raise RuntimeError(
                            "pressure acquire did not own every cold key"
                        )
                    acquired.extend(
                        (query, outcome.claim.generation)
                        for query, outcome in zip(batch, outcomes, strict=True)
                        if outcome.claim is not None
                    )
                for offset in range(0, count, 1000):
                    items = tuple(
                        _finalize_item(query, generation)
                        for query, generation in acquired[offset : offset + 1000]
                    )
                    started = time.perf_counter()
                    with _measured("finalize"):
                        persistence.finalize_claim_session(
                            authority, items, {raw_artifact.digest: raw_artifact}
                        )
                    operation_latencies["finalize"].append(
                        time.perf_counter() - started
                    )
            finally:
                heartbeat_stop.set()
                heartbeat_thread.join()
            started = time.perf_counter()
            with _measured("close"):
                persistence.close_claim_session(authority)
            operation_latencies["close"].append(time.perf_counter() - started)
            sweep_started = time.perf_counter()
            sweep_calls = 0
            with _measured("sweep"):
                while persistence.sweep_claim_sessions():
                    sweep_calls += 1
            operation_latencies["sweep"].append(time.perf_counter() - sweep_started)
            if heartbeat_errors:
                raise RuntimeError(f"heartbeat failed: {heartbeat_errors}")

            with persistence.engine.connect() as connection:
                residual = {
                    table: connection.execute(
                        text(f"SELECT count(*) FROM {table}")
                    ).scalar_one()
                    for table in (
                        "claim_sessions",
                        "session_claims",
                        "claim_session_acquire_receipts",
                        "claim_session_acquire_receipt_items",
                    )
                }
                terminal = connection.execute(
                    text(
                        "SELECT count(*) FROM evidence "
                        "WHERE adapter_contract_version = 'pressure/1'"
                    )
                ).scalar_one()
            phase_metrics = {}
            for phase in ("acquire", "renew", "finalize", "close", "sweep"):
                values = operation_latencies[phase]
                phase_pool_waits = pool_waits[phase][start_pool_waits.get(phase, 0) :]
                phase_metrics[phase] = {
                    "operations": len(values),
                    "sql_executions": statement_counts[phase]
                    - start_counts.get(phase, 0),
                    "sql_executions_per_operation": (
                        (statement_counts[phase] - start_counts.get(phase, 0))
                        / len(values)
                        if values
                        else 0.0
                    ),
                    "p50_seconds": _percentile(values, 0.50),
                    "p95_seconds": _percentile(values, 0.95),
                    "p99_seconds": _percentile(values, 0.99),
                    "maximum_seconds": max(values, default=0.0),
                    "transactions": transaction_counts[phase]
                    - start_transactions.get(phase, 0),
                    "pool_checkouts": len(phase_pool_waits),
                    "pool_wait_p95_seconds": _percentile(phase_pool_waits, 0.95),
                    "pool_wait_maximum_seconds": max(phase_pool_waits, default=0.0),
                }
            delete_rows = {
                table: {
                    "statements": len(values[start_sweep_deletes.get(table, 0) :]),
                    "maximum_rows": max(
                        values[start_sweep_deletes.get(table, 0) :], default=0
                    ),
                    "total_rows": sum(values[start_sweep_deletes.get(table, 0) :]),
                }
                for table, values in sweep_delete_rows.items()
                if values[start_sweep_deletes.get(table, 0) :]
            }
            expected_terminal = sum(args.counts[:lane])
            lane_report = {
                "claims": count,
                "workload_client_concurrency": 2,
                "heartbeat_calls": heartbeat_calls,
                "http_status_counts": {"412": 0, "503": 0},
                "phases": phase_metrics,
                "sweep_calls_returning_work": sweep_calls,
                "sweep_delete_rows": delete_rows,
                "residual": residual,
                "terminal_evidence_total": terminal,
            }
            _validate_lane_report(
                lane_report,
                expected_terminal=expected_terminal,
                expected_sessions=lane,
                expected_receipt_headers=sum(
                    math.ceil(value / 1000) for value in args.counts[:lane]
                ),
            )
            reports.append(lane_report)
        if args.cleanup_wait_seconds > 0:
            time.sleep(args.cleanup_wait_seconds)
        final_cleanup_started = time.perf_counter()
        final_cleanup_start_statements = statement_counts["final_cleanup"]
        final_cleanup_start_transactions = transaction_counts["final_cleanup"]
        final_cleanup_start_pool_waits = len(pool_waits["final_cleanup"])
        final_cleanup_start_deletes = {
            table: len(values) for table, values in sweep_delete_rows.items()
        }
        final_cleanup_calls = 0
        with _measured("final_cleanup"):
            while persistence.sweep_claim_sessions():
                final_cleanup_calls += 1
        final_cleanup_elapsed = time.perf_counter() - final_cleanup_started
        with persistence.engine.connect() as connection:
            final_residual = {
                table: connection.execute(
                    text(f"SELECT count(*) FROM {table}")
                ).scalar_one()
                for table in (
                    "claim_sessions",
                    "session_claims",
                    "claim_session_acquire_receipts",
                    "claim_session_acquire_receipt_items",
                )
            }
        final_cleanup_pool_waits = pool_waits["final_cleanup"][
            final_cleanup_start_pool_waits:
        ]
        final_cleanup_delete_rows = {
            table: {
                "statements": len(values[final_cleanup_start_deletes.get(table, 0) :]),
                "maximum_rows": max(
                    values[final_cleanup_start_deletes.get(table, 0) :], default=0
                ),
                "total_rows": sum(values[final_cleanup_start_deletes.get(table, 0) :]),
            }
            for table, values in sweep_delete_rows.items()
            if values[final_cleanup_start_deletes.get(table, 0) :]
        }
    finally:
        activity_stop.set()
        activity_thread.join()
        event.remove(persistence.engine, "before_cursor_execute", before_cursor_execute)
        event.remove(persistence.engine, "after_cursor_execute", after_cursor_execute)
        event.remove(persistence.engine, "commit", after_transaction)
        event.remove(persistence.engine, "rollback", after_transaction)
        pool._do_get = original_do_get
        persistence.close()
    if activity_errors:
        raise RuntimeError(f"pg_stat_activity sampling failed: {activity_errors}")
    expected_zero = {
        "claim_sessions": 0,
        "session_claims": 0,
        "claim_session_acquire_receipts": 0,
        "claim_session_acquire_receipt_items": 0,
    }
    if final_residual != expected_zero:
        raise RuntimeError(f"pressure left residual coordination: {final_residual}")

    final_cleanup = {
        "elapsed_seconds": final_cleanup_elapsed,
        "calls_returning_work": final_cleanup_calls,
        "sql_executions": statement_counts["final_cleanup"]
        - final_cleanup_start_statements,
        "transactions": transaction_counts["final_cleanup"]
        - final_cleanup_start_transactions,
        "pool_checkouts": len(final_cleanup_pool_waits),
        "pool_wait_p95_seconds": _percentile(final_cleanup_pool_waits, 0.95),
        "pool_wait_maximum_seconds": max(final_cleanup_pool_waits, default=0.0),
        "delete_rows": final_cleanup_delete_rows,
    }
    _validate_final_cleanup(final_cleanup)

    report = {
        "schema_version": 1,
        "source_head": source_head,
        "python": platform.python_version(),
        "postgres": postgres_version,
        "settings": settings,
        "pg_stat_activity": {
            **activity_summary,
            "wait_event_sample_counts": dict(sorted(activity_wait_events.items())),
        },
        "counts": reports,
        "cleanup_wait_seconds": args.cleanup_wait_seconds,
        "final_cleanup": final_cleanup,
        "final_residual": final_residual,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
