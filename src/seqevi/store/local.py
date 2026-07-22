"""Single-host SQLite/POSIX implementation of the evidence Store."""

from __future__ import annotations

import os
from collections import defaultdict
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from sqlalchemy import and_, create_engine, event, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine, RowMapping, URL

from seqevi.errors import (
    EvidenceConflictError,
    StoreConfigurationError,
    StoreIntegrityError,
)
from seqevi.evidence import (
    ArtifactFile,
    CommitOutcome,
    EvidenceCommit,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    EvidenceStatus,
    FetchedEvidence,
    StoredArtifact,
)
from seqevi.sequence import SequenceIdentity

from .artifact import PosixArtifactStore
from .migration import upgrade_database
from .schema import artifacts, evidence, sequences

_LOOKUP_CHUNK_SIZE = 400


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


class LocalStore:
    """Exact immutable evidence Store for processes on one host."""

    def __init__(
        self,
        *,
        root: Path,
        engine: Engine,
        artifact_store: PosixArtifactStore,
    ) -> None:
        self.root = root
        self.database_path = root / "store.sqlite3"
        self.engine = engine
        self.artifact_store = artifact_store

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
            return cls(
                root=root,
                engine=engine,
                artifact_store=PosixArtifactStore(root / "artifacts"),
            )
        except Exception:
            engine.dispose()
            raise

    def close(self) -> None:
        self.engine.dispose()

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

        outcomes: list[CommitOutcome] = []
        with self.engine.begin() as connection:
            for identity in identities.values():
                self._insert_sequence(connection, identity)
            for artifact in stored_artifacts.values():
                self._insert_artifact(connection, artifact)
            for commit in commits:
                outcomes.append(self._insert_evidence(connection, commit))
        return tuple(outcomes)

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


def _evidence_key_clause(key: EvidenceKey) -> Any:
    return and_(
        evidence.c.sequence_id == key.sequence_id,
        evidence.c.adapter_contract_version == key.adapter_contract_version,
        evidence.c.tool_runtime_digest == key.tool_runtime_digest,
        evidence.c.resource_id == key.resource_id,
        evidence.c.semantic_parameters_hash == key.semantic_parameters_hash,
    )


def _artifact_identity(artifact: ArtifactFile) -> tuple[str, str, int]:
    return artifact.digest, artifact.media_type, artifact.byte_size
