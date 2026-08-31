"""Single-host SQLite/POSIX implementation of the evidence Store."""

from __future__ import annotations

import os
import secrets
import threading
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from sqlalchemy import and_, create_engine, delete, event, select, tuple_, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine, RowMapping, URL
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import NullPool

from seqevi.errors import (
    EvidenceClaimLostError,
    EvidenceConflictError,
    StoreConfigurationError,
    StoreIntegrityError,
)
from seqevi.evidence import (
    ArtifactFile,
    BusyEvidenceClaim,
    ClaimDisposition,
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    EvidenceStatus,
    FetchedEvidence,
    StoredArtifact,
    SessionClaimAcquireResult,
    SessionEvidenceClaim,
)
from seqevi.sequence import SequenceIdentity

from .artifact import PosixArtifactStore
from .migration import upgrade_database
from .schema import (
    artifacts,
    claim_sessions,
    claim_session_acquire_receipt_items,
    claim_session_acquire_receipts,
    claim_session_open_receipts,
    evidence,
    evidence_claim_generations,
    sequences,
    session_claims,
)

_LOOKUP_CHUNK_SIZE = 400
_CLAIM_LEASE_SECONDS = 120.0
_CLAIM_RENEWAL_SECONDS = 30.0
_CLAIM_RETRY_SECONDS = 1.0
_SWEEP_BUSY_TIMEOUT_MS = 100
_SWEEP_SHUTDOWN_SECONDS = 1.0
_SWEEP_DRAIN_SECONDS = 1.0


def resolve_store_path(
    value: str | Path | None,
    *,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Resolve an explicit local Store path without a hidden home default."""

    environment = os.environ if environ is None else environ
    raw_value = environment.get("SEQEVI_STORE") if value is None else os.fspath(value)
    if not raw_value:
        raise StoreConfigurationError(
            "a local Store path is required via --store or SEQEVI_STORE"
        )
    if "://" in raw_value:
        raise StoreConfigurationError(
            "LocalStore requires a filesystem path, not a service URL"
        )

    path = Path(raw_value).expanduser().resolve()
    if path.exists() and not path.is_dir():
        raise StoreConfigurationError(f"Store path is not a directory: {path}")
    return path


def _configure_sqlite(dbapi_connection: Any, _connection_record: Any) -> None:
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA busy_timeout=30000")
        cursor.execute("PRAGMA synchronous=FULL")
    finally:
        cursor.close()


def _reset_sqlite_after_sweep(
    dbapi_connection: Any | None, _connection_record: Any
) -> None:
    if dbapi_connection is None:
        return
    dbapi_connection.set_progress_handler(None, 0)
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA busy_timeout=30000")
    finally:
        cursor.close()


class LocalStore:
    """Exact immutable evidence Store for processes on one host."""

    def __init__(
        self,
        *,
        root: Path,
        engine: Engine,
        sweep_engine: Engine,
        artifact_store: PosixArtifactStore,
    ) -> None:
        self.root = root
        self.database_path = root / "store.sqlite3"
        self.engine = engine
        self._sweep_engine = sweep_engine
        self.artifact_store = artifact_store
        self._sweeper_stop = threading.Event()
        self._sweeper_wake = threading.Event()
        self._sweeper = threading.Thread(
            target=self._sweep_loop,
            name="seqevi-local-claim-sweeper",
            daemon=True,
        )

    @classmethod
    def open(
        cls,
        value: str | Path | None,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> LocalStore:
        """Create or open a local Store and upgrade its embedded schema."""

        root = resolve_store_path(value, environ=environ)
        root.mkdir(parents=True, exist_ok=True)
        database_path = root / "store.sqlite3"
        url = URL.create("sqlite+pysqlite", database=str(database_path))
        engine = create_engine(url, connect_args={"timeout": 30.0})
        event.listen(engine, "connect", _configure_sqlite)
        event.listen(engine, "checkin", _reset_sqlite_after_sweep)
        sweep_engine = create_engine(
            url, connect_args={"timeout": 30.0}, poolclass=NullPool
        )
        event.listen(sweep_engine, "connect", _configure_sqlite)
        event.listen(sweep_engine, "checkin", _reset_sqlite_after_sweep)

        try:
            upgrade_database(engine, root)
            with engine.connect() as connection:
                journal_mode = connection.exec_driver_sql(
                    "PRAGMA journal_mode=WAL"
                ).scalar_one()
            if str(journal_mode).lower() != "wal":
                raise StoreConfigurationError(
                    f"SQLite Store did not enter WAL mode: {journal_mode}"
                )
            store = cls(
                root=root,
                engine=engine,
                sweep_engine=sweep_engine,
                artifact_store=PosixArtifactStore(root / "artifacts"),
            )
            store._drain_coordination(time.monotonic() + _SWEEP_DRAIN_SECONDS)
            store._sweeper.start()
            return store
        except Exception:
            sweep_engine.dispose()
            engine.dispose()
            raise

    def close(self) -> None:
        self._sweeper_stop.set()
        self._sweeper_wake.set()
        self._sweeper.join(timeout=_SWEEP_SHUTDOWN_SECONDS)
        if not self._sweeper.is_alive():
            self._drain_coordination(time.monotonic() + _SWEEP_DRAIN_SECONDS)
        self._sweep_engine.dispose()
        self.engine.dispose()

    def _sweep_loop(self) -> None:
        while not self._sweeper_stop.is_set():
            self._sweeper_wake.wait(timeout=1.0)
            self._sweeper_wake.clear()
            try:
                while not self._sweeper_stop.is_set() and self._sweep_once():
                    pass
            except OperationalError as error:
                if not _sqlite_is_busy(error):
                    raise

    def _drain_coordination(self, deadline: float) -> None:
        try:
            while time.monotonic() < deadline and self._sweep_once(
                deadline=deadline,
                busy_timeout_ms=max(
                    min(
                        int((deadline - time.monotonic()) * 1000),
                        _SWEEP_BUSY_TIMEOUT_MS,
                    ),
                    1,
                ),
            ):
                pass
        except TimeoutError:
            pass
        except OperationalError as error:
            if not _sqlite_is_busy(error):
                raise

    def _sweep_once(
        self,
        *,
        busy_timeout_ms: int = _SWEEP_BUSY_TIMEOUT_MS,
        deadline: float | None = None,
    ) -> int:
        operation_deadline = (
            time.monotonic() + _SWEEP_SHUTDOWN_SECONDS if deadline is None else deadline
        )
        now = datetime.now(UTC)
        removed = 0
        with self._sweep_engine.begin() as connection:
            connection.exec_driver_sql(f"PRAGMA busy_timeout={busy_timeout_ms}")
            raw = cast(Any, connection.connection.driver_connection)
            raw.set_progress_handler(
                lambda: 1 if time.monotonic() >= operation_deadline else 0,
                100,
            )
            try:
                if time.monotonic() >= operation_deadline:
                    raise TimeoutError(
                        "SQLite coordination sweep exceeded its deadline"
                    )
                stale_sessions = select(claim_sessions.c.session_id).where(
                    (claim_sessions.c.state == "closing")
                    | (claim_sessions.c.expires_at <= now)
                )
                claim_ids = (
                    select(
                        session_claims.c.sequence_id,
                        session_claims.c.adapter_contract_version,
                        session_claims.c.tool_runtime_digest,
                        session_claims.c.resource_id,
                        session_claims.c.semantic_parameters_hash,
                    )
                    .where(session_claims.c.session_id.in_(stale_sessions))
                    .order_by(*session_claims.primary_key.columns)
                    .limit(1000)
                )
                result = connection.execute(
                    delete(session_claims).where(
                        tuple_(*session_claims.primary_key.columns).in_(claim_ids)
                    )
                )
                removed += result.rowcount
                cutoff = now - timedelta(seconds=60)
                item_ids = (
                    select(
                        claim_session_acquire_receipt_items.c.session_id,
                        claim_session_acquire_receipt_items.c.request_id,
                        claim_session_acquire_receipt_items.c.input_index,
                    )
                    .join(claim_session_acquire_receipts)
                    .where(claim_session_acquire_receipts.c.created_at <= cutoff)
                    .order_by(
                        claim_session_acquire_receipts.c.created_at,
                        claim_session_acquire_receipt_items.c.session_id,
                        claim_session_acquire_receipt_items.c.request_id,
                        claim_session_acquire_receipt_items.c.input_index,
                    )
                    .limit(1000)
                )
                result = connection.execute(
                    delete(claim_session_acquire_receipt_items).where(
                        tuple_(
                            claim_session_acquire_receipt_items.c.session_id,
                            claim_session_acquire_receipt_items.c.request_id,
                            claim_session_acquire_receipt_items.c.input_index,
                        ).in_(item_ids)
                    )
                )
                removed += result.rowcount
                header_ids = (
                    select(
                        claim_session_acquire_receipts.c.session_id,
                        claim_session_acquire_receipts.c.request_id,
                    )
                    .where(
                        claim_session_acquire_receipts.c.created_at <= cutoff,
                        ~select(claim_session_acquire_receipt_items.c.input_index)
                        .where(
                            claim_session_acquire_receipt_items.c.session_id
                            == claim_session_acquire_receipts.c.session_id,
                            claim_session_acquire_receipt_items.c.request_id
                            == claim_session_acquire_receipts.c.request_id,
                        )
                        .exists(),
                    )
                    .limit(1000)
                )
                result = connection.execute(
                    delete(claim_session_acquire_receipts).where(
                        tuple_(
                            claim_session_acquire_receipts.c.session_id,
                            claim_session_acquire_receipts.c.request_id,
                        ).in_(header_ids)
                    )
                )
                removed += result.rowcount
                empty_sessions = (
                    select(claim_sessions.c.session_id)
                    .where(
                        (
                            (claim_sessions.c.state == "closing")
                            | (claim_sessions.c.expires_at <= now)
                        ),
                        ~select(session_claims.c.sequence_id)
                        .where(
                            session_claims.c.session_id == claim_sessions.c.session_id
                        )
                        .exists(),
                        ~select(claim_session_acquire_receipts.c.request_id)
                        .where(
                            claim_session_acquire_receipts.c.session_id
                            == claim_sessions.c.session_id
                        )
                        .exists(),
                    )
                    .limit(1000)
                )
                result = connection.execute(
                    delete(claim_sessions).where(
                        claim_sessions.c.session_id.in_(empty_sessions)
                    )
                )
                removed += result.rowcount
                result = connection.execute(
                    delete(claim_session_open_receipts).where(
                        claim_session_open_receipts.c.open_request_id.in_(
                            select(claim_session_open_receipts.c.open_request_id)
                            .where(
                                claim_session_open_receipts.c.created_at
                                <= now - timedelta(seconds=120)
                            )
                            .limit(1000)
                        )
                    )
                )
                removed += result.rowcount
                return removed
            except BaseException:
                raise

    @property
    def supports_claim_sessions(self) -> bool:
        return True

    def claim_session(self) -> _LocalClaimSession:
        return _LocalClaimSession(self)

    def __enter__(self) -> LocalStore:
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def lookup_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        """Return exact records after verifying requested sequence content."""

        queries = tuple(dict.fromkeys(requested_queries))
        expected_identities: dict[str, SequenceIdentity] = {}
        for query in queries:
            existing = expected_identities.setdefault(
                query.identity.sequence_id, query.identity
            )
            if existing != query.identity:
                raise StoreIntegrityError(
                    f"SequenceID collision in lookup: {query.identity.sequence_id}"
                )
        return self._lookup_keys(
            (query.key for query in queries),
            expected_identities=expected_identities,
        )

    def _lookup_keys(
        self,
        requested_keys: Iterable[EvidenceKey],
        *,
        expected_identities: Mapping[str, SequenceIdentity] | None = None,
    ) -> dict[EvidenceKey, EvidenceRecord]:
        keys = tuple(dict.fromkeys(requested_keys))
        groups: dict[tuple[str, str, str, str], list[EvidenceKey]] = defaultdict(list)
        for key in keys:
            groups[key.contract_identity].append(key)

        found: dict[EvidenceKey, EvidenceRecord] = {}
        with self.engine.connect() as connection:
            for contract, group_keys in groups.items():
                requested_set = set(group_keys)
                for offset in range(0, len(group_keys), _LOOKUP_CHUNK_SIZE):
                    chunk = group_keys[offset : offset + _LOOKUP_CHUNK_SIZE]
                    statement = select(evidence).where(
                        and_(
                            evidence.c.sequence_id.in_(
                                [key.sequence_id for key in chunk]
                            ),
                            evidence.c.adapter_contract_version == contract[0],
                            evidence.c.tool_runtime_digest == contract[1],
                            evidence.c.resource_id == contract[2],
                            evidence.c.semantic_parameters_hash == contract[3],
                        )
                    )
                    for row in connection.execute(statement).mappings():
                        record = self._record_from_row(row)
                        if record.key in requested_set:
                            if expected_identities is not None:
                                expected = expected_identities[record.key.sequence_id]
                                self._verify_persisted_sequence(connection, expected)
                            found[record.key] = record
        return found

    def get_sequence(self, sequence_id: str) -> SequenceIdentity | None:
        """Return canonical sequence content for one primary identifier."""

        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(sequences).where(sequences.c.sequence_id == sequence_id)
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return SequenceIdentity(
            sequence_id=row["sequence_id"],
            md5=row["md5"],
            length=row["length"],
            sequence=row["sequence"],
        )

    def commit_many(
        self, proposed_commits: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        """Atomically commit a validated batch of terminal evidence."""

        commits = tuple(proposed_commits)
        seen_keys: set[EvidenceKey] = set()
        identities: dict[str, SequenceIdentity] = {}
        payloads: dict[str, ArtifactFile] = {}

        for commit in commits:
            if commit.key in seen_keys:
                raise ValueError("commit batch contains a duplicate evidence key")
            seen_keys.add(commit.key)

            existing_identity = identities.setdefault(
                commit.identity.sequence_id, commit.identity
            )
            if existing_identity != commit.identity:
                raise StoreIntegrityError(
                    f"SequenceID collision in commit batch: {commit.identity.sequence_id}"
                )

            for payload in (commit.normalized_artifact, commit.raw_artifact):
                if payload is None:
                    continue
                existing_payload = payloads.setdefault(payload.digest, payload)
                if _artifact_identity(existing_payload) != _artifact_identity(payload):
                    raise StoreIntegrityError(
                        f"artifact digest has conflicting payload metadata: {payload.digest}"
                    )

        stored_artifacts = {
            digest: self.artifact_store.put(payload)
            for digest, payload in payloads.items()
        }

        ordered = sorted(commits, key=lambda item: _key_sort_value(item.key))
        outcomes: dict[EvidenceKey, CommitOutcome] = {}
        with self.engine.begin() as connection:
            for commit in ordered:
                connection.execute(
                    delete(session_claims).where(_session_claim_key_clause(commit.key))
                )
            for commit in ordered:
                self._insert_sequence(connection, commit.identity)
            for digest in sorted(stored_artifacts):
                artifact = stored_artifacts[digest]
                self._insert_artifact(connection, artifact)
            for commit in ordered:
                outcomes[commit.key] = self._insert_evidence(connection, commit)
        return tuple(outcomes[commit.key] for commit in commits)

    def fetch(self, key: EvidenceKey) -> FetchedEvidence | None:
        """Fetch one exact evidence record and verify referenced artifact bytes."""

        return self.fetch_many((key,)).get(key)

    def fetch_many(
        self, keys: Iterable[EvidenceKey]
    ) -> dict[EvidenceKey, FetchedEvidence]:
        """Fetch records while reading each referenced artifact only once."""

        requested = tuple(dict.fromkeys(keys))
        records = self._lookup_keys(requested)
        digests = {
            digest
            for record in records.values()
            for digest in (
                record.normalized_artifact_digest,
                record.raw_artifact_digest,
            )
            if digest is not None
        }
        artifact_by_digest = {
            digest: self._reference_registered_artifact(digest)
            for digest in sorted(digests)
        }
        return {
            key: FetchedEvidence(
                record=record,
                normalized_artifact=(
                    artifact_by_digest[record.normalized_artifact_digest]
                    if record.normalized_artifact_digest is not None
                    else None
                ),
                raw_artifact=(
                    artifact_by_digest[record.raw_artifact_digest]
                    if record.raw_artifact_digest is not None
                    else None
                ),
            )
            for key, record in records.items()
        }

    def _reference_registered_artifact(self, digest: str) -> ArtifactFile:
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(artifacts).where(artifacts.c.digest == digest)
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            raise StoreIntegrityError(f"artifact metadata is missing: {digest}")
        if row["storage_kind"] != "posix":
            raise StoreIntegrityError("local Store cannot read OCI storage references")
        return self.artifact_store.reference(
            StoredArtifact(
                digest=row["digest"],
                media_type=row["media_type"],
                byte_size=row["byte_size"],
                relative_path=row["relative_path"],
            )
        )

    @staticmethod
    def _verify_persisted_sequence(
        connection: Connection, expected: SequenceIdentity
    ) -> None:
        row = (
            connection.execute(
                select(sequences).where(sequences.c.sequence_id == expected.sequence_id)
            )
            .mappings()
            .one()
        )
        persisted = (row["md5"], row["length"], row["sequence"])
        requested = (expected.md5, expected.length, expected.sequence)
        if persisted != requested:
            raise StoreIntegrityError(
                f"SequenceID collision in Store lookup: {expected.sequence_id}"
            )

    @staticmethod
    def _insert_sequence(connection: Connection, identity: SequenceIdentity) -> None:
        statement = (
            sqlite_insert(sequences)
            .values(
                sequence_id=identity.sequence_id,
                md5=identity.md5,
                length=identity.length,
                sequence=identity.sequence,
            )
            .on_conflict_do_nothing(index_elements=[sequences.c.sequence_id])
        )
        result = connection.execute(statement)
        if result.rowcount:
            return

        row = (
            connection.execute(
                select(sequences).where(sequences.c.sequence_id == identity.sequence_id)
            )
            .mappings()
            .one()
        )
        persisted = (row["md5"], row["length"], row["sequence"])
        proposed = (identity.md5, identity.length, identity.sequence)
        if persisted != proposed:
            raise StoreIntegrityError(
                f"SequenceID collision in Store: {identity.sequence_id}"
            )

    @staticmethod
    def _insert_artifact(connection: Connection, artifact: StoredArtifact) -> None:
        statement = (
            sqlite_insert(artifacts)
            .values(
                digest=artifact.digest,
                media_type=artifact.media_type,
                byte_size=artifact.byte_size,
                relative_path=artifact.relative_path,
            )
            .on_conflict_do_nothing(index_elements=[artifacts.c.digest])
        )
        result = connection.execute(statement)
        if result.rowcount:
            return

        row = (
            connection.execute(
                select(artifacts).where(artifacts.c.digest == artifact.digest)
            )
            .mappings()
            .one()
        )
        persisted = (row["media_type"], row["byte_size"], row["relative_path"])
        proposed = (artifact.media_type, artifact.byte_size, artifact.relative_path)
        if persisted != proposed:
            raise StoreIntegrityError(f"artifact metadata conflict: {artifact.digest}")

    @staticmethod
    def _insert_evidence(
        connection: Connection, commit: EvidenceCommit
    ) -> CommitOutcome:
        normalized_digest = (
            commit.normalized_artifact.digest
            if commit.normalized_artifact is not None
            else None
        )
        raw_digest = commit.raw_artifact.digest if commit.raw_artifact else None
        key = commit.key
        statement = (
            sqlite_insert(evidence)
            .values(
                sequence_id=key.sequence_id,
                adapter_contract_version=key.adapter_contract_version,
                tool_runtime_digest=key.tool_runtime_digest,
                resource_id=key.resource_id,
                semantic_parameters_hash=key.semantic_parameters_hash,
                semantic_parameters_json=key.semantic_parameters_json,
                status=commit.status.value,
                payload_digest=commit.payload_digest,
                normalized_artifact_digest=normalized_digest,
                raw_artifact_digest=raw_digest,
            )
            .on_conflict_do_nothing(
                index_elements=[
                    evidence.c.sequence_id,
                    evidence.c.adapter_contract_version,
                    evidence.c.tool_runtime_digest,
                    evidence.c.resource_id,
                    evidence.c.semantic_parameters_hash,
                ]
            )
        )
        result = connection.execute(statement)
        if result.rowcount:
            return CommitOutcome.CREATED

        row = (
            connection.execute(select(evidence).where(_evidence_key_clause(key)))
            .mappings()
            .one()
        )
        if (
            row["semantic_parameters_json"] != key.semantic_parameters_json
            or row["status"] != commit.status.value
            or row["payload_digest"] != commit.payload_digest
        ):
            raise EvidenceConflictError(
                f"evidence key has a different immutable payload: {key.sequence_id}"
            )
        return CommitOutcome.EXISTING

    @staticmethod
    def _record_from_row(row: RowMapping) -> EvidenceRecord:
        key = EvidenceKey(
            sequence_id=row["sequence_id"],
            adapter_contract_version=row["adapter_contract_version"],
            tool_runtime_digest=row["tool_runtime_digest"],
            resource_id=row["resource_id"],
            semantic_parameters_json=row["semantic_parameters_json"],
        )
        return EvidenceRecord(
            key=key,
            status=EvidenceStatus(row["status"]),
            payload_digest=row["payload_digest"],
            normalized_artifact_digest=row["normalized_artifact_digest"],
            raw_artifact_digest=row["raw_artifact_digest"],
            created_at=row["created_at"],
        )


class _LocalClaimSession:
    """SQLite ClaimSession with one invocation-scoped heartbeat."""

    def __init__(self, store: LocalStore) -> None:
        self.store = store
        self.session_id = secrets.token_hex(24)
        self.owner_token = secrets.token_urlsafe(32)
        self.generation = 1
        self.cancellation_signal = threading.Event()
        self._stop = threading.Event()
        self._lost: BaseException | None = None
        self._claims: dict[EvidenceKey, SessionEvidenceClaim] = {}
        now = datetime.now(UTC)
        with store.engine.begin() as connection:
            connection.execute(
                claim_sessions.insert().values(
                    session_id=self.session_id,
                    owner_token=self.owner_token,
                    generation=self.generation,
                    state="open",
                    expires_at=now + timedelta(seconds=_CLAIM_LEASE_SECONDS),
                    created_at=now,
                    updated_at=now,
                )
            )
        self._thread = threading.Thread(
            target=self._heartbeat, name="seqevi-claim-session-heartbeat", daemon=True
        )
        self._thread.start()

    def __enter__(self) -> _LocalClaimSession:
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def raise_if_lost(self) -> None:
        if self._lost is not None:
            raise EvidenceClaimLostError(
                "ClaimSession authority was lost"
            ) from self._lost

    def _heartbeat(self) -> None:
        while not self._stop.wait(_CLAIM_RENEWAL_SECONDS):
            deadline = time.monotonic() + 90.0
            while not self._stop.is_set():
                try:
                    with self.store.engine.connect() as connection:
                        connection.exec_driver_sql("PRAGMA busy_timeout=250")
                        connection.exec_driver_sql("BEGIN IMMEDIATE")
                        now = datetime.now(UTC)
                        result = connection.execute(
                            update(claim_sessions)
                            .where(
                                and_(
                                    claim_sessions.c.session_id == self.session_id,
                                    claim_sessions.c.owner_token == self.owner_token,
                                    claim_sessions.c.generation == self.generation,
                                    claim_sessions.c.state == "open",
                                    claim_sessions.c.expires_at > now,
                                )
                            )
                            .values(
                                expires_at=now
                                + timedelta(seconds=_CLAIM_LEASE_SECONDS),
                                updated_at=now,
                            )
                        )
                        connection.commit()
                    if result.rowcount != 1:
                        raise EvidenceClaimLostError("ClaimSession renewal was fenced")
                    break
                except EvidenceClaimLostError as error:
                    self._lost = error
                    self.cancellation_signal.set()
                    return
                except BaseException as error:
                    if time.monotonic() >= deadline:
                        self._lost = error
                        self.cancellation_signal.set()
                        return
                    self._stop.wait(min(1.0, max(deadline - time.monotonic(), 0.0)))

    def acquire_many(
        self, requested_queries: Iterable[EvidenceQuery]
    ) -> tuple[SessionClaimAcquireResult, ...]:
        self.raise_if_lost()
        queries = tuple(requested_queries)
        if len({query.key for query in queries}) != len(queries):
            raise ValueError("acquire batch contains a duplicate evidence key")
        results: dict[EvidenceKey, SessionClaimAcquireResult] = {}
        with self.store.engine.connect() as connection:
            connection.exec_driver_sql("BEGIN IMMEDIATE")
            try:
                now = datetime.now(UTC)
                _require_local_session(connection, self, now)
                for query in sorted(
                    queries, key=lambda item: _key_sort_value(item.key)
                ):
                    claim: SessionEvidenceClaim | None = None
                    self.store._insert_sequence(connection, query.identity)
                    terminal = (
                        connection.execute(
                            select(evidence).where(_evidence_key_clause(query.key))
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if terminal is not None:
                        results[query.key] = SessionClaimAcquireResult(
                            ClaimDisposition.CACHED,
                            record=self.store._record_from_row(terminal),
                        )
                        continue
                    row = (
                        connection.execute(
                            select(session_claims).where(
                                _session_claim_key_clause(query.key)
                            )
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if row is not None and row["session_id"] == self.session_id:
                        claim = SessionEvidenceClaim(query.key, row["generation"])
                    elif row is not None:
                        owner = (
                            connection.execute(
                                select(
                                    claim_sessions.c.expires_at, claim_sessions.c.state
                                ).where(
                                    claim_sessions.c.session_id == row["session_id"]
                                )
                            )
                            .mappings()
                            .one_or_none()
                        )
                        if (
                            owner is not None
                            and owner["state"] == "open"
                            and _as_utc(owner["expires_at"]) > now
                        ):
                            results[query.key] = SessionClaimAcquireResult(
                                ClaimDisposition.BUSY,
                                busy=BusyEvidenceClaim(
                                    query.key,
                                    _as_utc(owner["expires_at"]),
                                    _CLAIM_RETRY_SECONDS,
                                ),
                            )
                            continue
                        connection.execute(
                            delete(session_claims).where(
                                _session_claim_key_clause(query.key)
                            )
                        )
                        row = None
                    if row is None:
                        generation_row = connection.execute(
                            select(evidence_claim_generations.c.high_water).where(
                                _generation_key_clause(query.key)
                            )
                        ).scalar_one_or_none()
                        generation = 1 if generation_row is None else generation_row + 1
                        statement = (
                            sqlite_insert(evidence_claim_generations)
                            .values(
                                **_claim_key_values(query.key), high_water=generation
                            )
                            .on_conflict_do_update(
                                index_elements=list(
                                    evidence_claim_generations.primary_key.columns
                                ),
                                set_={"high_water": generation},
                            )
                        )
                        connection.execute(statement)
                        connection.execute(
                            session_claims.insert().values(
                                **_claim_key_values(query.key),
                                semantic_parameters_json=query.key.semantic_parameters_json,
                                session_id=self.session_id,
                                generation=generation,
                                created_at=now,
                            )
                        )
                        claim = SessionEvidenceClaim(query.key, generation)
                    assert claim is not None
                    self._claims[query.key] = claim
                    results[query.key] = SessionClaimAcquireResult(
                        ClaimDisposition.ACQUIRED, claim=claim
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return tuple(results[query.key] for query in queries)

    def finalize_many(
        self, proposed: Iterable[EvidenceCommit]
    ) -> tuple[CommitOutcome, ...]:
        self.raise_if_lost()
        commits = tuple(proposed)
        if len({commit.key for commit in commits}) != len(commits):
            raise ValueError("finalize batch contains a duplicate evidence key")
        payloads: dict[str, ArtifactFile] = {}
        for commit in commits:
            for payload in (commit.normalized_artifact, commit.raw_artifact):
                if payload is None:
                    continue
                existing = payloads.setdefault(payload.digest, payload)
                if (
                    existing.media_type != payload.media_type
                    or existing.byte_size != payload.byte_size
                ):
                    raise StoreIntegrityError(
                        f"artifact metadata conflict: {payload.digest}"
                    )
        stored = {
            digest: self.store.artifact_store.put(value)
            for digest, value in payloads.items()
        }
        outcomes: dict[EvidenceKey, CommitOutcome] = {}
        with self.store.engine.connect() as connection:
            connection.exec_driver_sql("BEGIN IMMEDIATE")
            try:
                now = datetime.now(UTC)
                _require_local_session(connection, self, now)
                for commit in sorted(
                    commits, key=lambda item: _key_sort_value(item.key)
                ):
                    claim = self._claims.get(commit.key)
                    if claim is None:
                        raise EvidenceClaimLostError("no exact claim for finalization")
                    consumed = connection.execute(
                        delete(session_claims)
                        .where(
                            and_(
                                _session_claim_key_clause(commit.key),
                                session_claims.c.session_id == self.session_id,
                                session_claims.c.generation == claim.generation,
                            )
                        )
                        .returning(session_claims.c.sequence_id)
                    ).scalar_one_or_none()
                    if consumed is None:
                        terminal = (
                            connection.execute(
                                select(evidence).where(_evidence_key_clause(commit.key))
                            )
                            .mappings()
                            .one_or_none()
                        )
                        if terminal is None:
                            raise EvidenceClaimLostError("exact claim was fenced")
                for commit in commits:
                    self.store._insert_sequence(connection, commit.identity)
                for digest in sorted(stored):
                    self.store._insert_artifact(connection, stored[digest])
                for commit in commits:
                    outcomes[commit.key] = self.store._insert_evidence(
                        connection, commit
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        for commit in commits:
            self._claims.pop(commit.key, None)
        return tuple(outcomes[commit.key] for commit in commits)

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1.0)
        if self._thread.is_alive():
            raise StoreIntegrityError("ClaimSession heartbeat did not stop promptly")
        now = datetime.now(UTC)
        with self.store.engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA busy_timeout=1000")
            connection.exec_driver_sql("BEGIN IMMEDIATE")
            connection.execute(
                update(claim_sessions)
                .where(
                    and_(
                        claim_sessions.c.session_id == self.session_id,
                        claim_sessions.c.owner_token == self.owner_token,
                        claim_sessions.c.generation == self.generation,
                    )
                )
                .values(state="closing", expires_at=now, updated_at=now)
            )
            connection.commit()
        self.store._sweeper_wake.set()  # pyright: ignore[reportPrivateUsage]


def _require_local_session(
    connection: Connection, session: _LocalClaimSession, now: datetime
) -> None:
    row = (
        connection.execute(
            select(claim_sessions).where(
                claim_sessions.c.session_id == session.session_id
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        row is None
        or row["owner_token"] != session.owner_token
        or row["generation"] != session.generation
        or row["state"] != "open"
        or _as_utc(row["expires_at"]) <= now
    ):
        raise EvidenceClaimLostError("ClaimSession authority was lost")


def _local_receipt_item(
    session_id: str,
    request_id: str,
    input_index: int,
    result: SessionClaimAcquireResult,
) -> dict[str, object]:
    return {
        "session_id": session_id,
        "request_id": request_id,
        "input_index": input_index,
        "outcome": result.disposition.value,
        "generation": None if result.claim is None else result.claim.generation,
        "busy_expires_at": None if result.busy is None else result.busy.expires_at,
        "evidence_created_at": (
            None if result.record is None else result.record.created_at
        ),
    }


def _evidence_key_clause(key: EvidenceKey) -> Any:
    return and_(
        evidence.c.sequence_id == key.sequence_id,
        evidence.c.adapter_contract_version == key.adapter_contract_version,
        evidence.c.tool_runtime_digest == key.tool_runtime_digest,
        evidence.c.resource_id == key.resource_id,
        evidence.c.semantic_parameters_hash == key.semantic_parameters_hash,
    )


def _claim_key_values(key: EvidenceKey) -> dict[str, str]:
    return {
        "sequence_id": key.sequence_id,
        "adapter_contract_version": key.adapter_contract_version,
        "tool_runtime_digest": key.tool_runtime_digest,
        "resource_id": key.resource_id,
        "semantic_parameters_hash": key.semantic_parameters_hash,
    }


def _session_claim_key_clause(key: EvidenceKey) -> Any:
    return and_(
        session_claims.c.sequence_id == key.sequence_id,
        session_claims.c.adapter_contract_version == key.adapter_contract_version,
        session_claims.c.tool_runtime_digest == key.tool_runtime_digest,
        session_claims.c.resource_id == key.resource_id,
        session_claims.c.semantic_parameters_hash == key.semantic_parameters_hash,
    )


def _generation_key_clause(key: EvidenceKey) -> Any:
    return and_(
        evidence_claim_generations.c.sequence_id == key.sequence_id,
        evidence_claim_generations.c.adapter_contract_version
        == key.adapter_contract_version,
        evidence_claim_generations.c.tool_runtime_digest == key.tool_runtime_digest,
        evidence_claim_generations.c.resource_id == key.resource_id,
        evidence_claim_generations.c.semantic_parameters_hash
        == key.semantic_parameters_hash,
    )


def _key_sort_value(key: EvidenceKey) -> tuple[str, str, str, str, str]:
    return (
        key.sequence_id,
        key.adapter_contract_version,
        key.tool_runtime_digest,
        key.resource_id,
        key.semantic_parameters_hash,
    )


def _sqlite_is_busy(error: OperationalError) -> bool:
    code = getattr(error.orig, "sqlite_errorcode", None)
    return isinstance(code, int) and code & 0xFF in {5, 6, 9}


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _artifact_identity(artifact: ArtifactFile) -> tuple[str, str, int]:
    return artifact.digest, artifact.media_type, artifact.byte_size
