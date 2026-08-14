"""Measure ClaimSession PostgreSQL scaling with production persistence code."""

from __future__ import annotations

import argparse
import contextvars
import hashlib
import json
import platform
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Iterator
from uuid import uuid4

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

    persistence = PostgresEvidencePersistence.open(
        args.database_url, pool_size=32, max_overflow=32
    )
    statement_counts: dict[str, int] = defaultdict(int)
    statement_started: dict[int, tuple[str, float]] = {}
    statement_latencies: dict[str, list[float]] = defaultdict(list)
    metrics_lock = threading.Lock()

    def before_cursor_execute(
        _connection, _cursor, _statement, _parameters, context, _executemany
    ) -> None:
        phase = _phase.get()
        with metrics_lock:
            statement_counts[phase] += 1
            statement_started[id(context)] = (phase, time.perf_counter())

    def after_cursor_execute(
        _connection, _cursor, _statement, _parameters, context, _executemany
    ) -> None:
        with metrics_lock:
            phase, started = statement_started.pop(id(context))
            statement_latencies[phase].append(time.perf_counter() - started)

    event.listen(persistence.engine, "before_cursor_execute", before_cursor_execute)
    event.listen(persistence.engine, "after_cursor_execute", after_cursor_execute)
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
                    raise RuntimeError("pressure acquire did not own every cold key")
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
                operation_latencies["finalize"].append(time.perf_counter() - started)
            heartbeat_stop.set()
            heartbeat_thread.join()
            started = time.perf_counter()
            with _measured("close"):
                persistence.close_claim_session(authority)
            operation_latencies["close"].append(time.perf_counter() - started)
            while persistence.sweep_claim_sessions():
                pass
            if heartbeat_errors:
                raise RuntimeError(f"heartbeat failed: {heartbeat_errors}")

            with persistence.engine.connect() as connection:
                residual = {
                    table: connection.execute(
                        text(f"SELECT count(*) FROM {table}")
                    ).scalar_one()
                    for table in ("claim_sessions", "session_claims")
                }
                terminal = connection.execute(
                    text(
                        "SELECT count(*) FROM evidence "
                        "WHERE adapter_contract_version = 'pressure/1'"
                    )
                ).scalar_one()
            phase_metrics = {}
            for phase in ("acquire", "renew", "finalize", "close"):
                values = operation_latencies[phase]
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
                }
            reports.append(
                {
                    "claims": count,
                    "heartbeat_calls": heartbeat_calls,
                    "phases": phase_metrics,
                    "residual": residual,
                    "terminal_evidence_total": terminal,
                }
            )
        if args.cleanup_wait_seconds > 0:
            time.sleep(args.cleanup_wait_seconds)
        while persistence.sweep_claim_sessions():
            pass
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
    finally:
        event.remove(persistence.engine, "before_cursor_execute", before_cursor_execute)
        event.remove(persistence.engine, "after_cursor_execute", after_cursor_execute)
        persistence.close()

    report = {
        "schema_version": 1,
        "source_head": __import__("subprocess")
        .check_output(["git", "rev-parse", "HEAD"], text=True)
        .strip(),
        "python": platform.python_version(),
        "postgres": postgres_version,
        "settings": settings,
        "counts": reports,
        "cleanup_wait_seconds": args.cleanup_wait_seconds,
        "final_residual": final_residual,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
