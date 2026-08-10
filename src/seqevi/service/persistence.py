"""PostgreSQL metadata persistence for the shared evidence Store."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from sqlalchemy import and_, create_engine, delete, select, update
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.engine import Connection, Engine, RowMapping

from seqevi.errors import (
    EvidenceClaimLostError,
    EvidenceConflictError,
    StoreIntegrityError,
)
from seqevi.evidence import (
    BusyEvidenceClaim,
    ClaimAcquireResult,
    ClaimDisposition,
    CommitOutcome,
    EvidenceClaim,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    EvidenceStatus,
    StoredArtifact,
)
from seqevi.sequence import SequenceIdentity
from seqevi.store.migration import upgrade_postgres_database
from seqevi.store.schema import artifacts, evidence, evidence_claims, sequences
from seqevi.store.transport import ClaimedCommitModel, CommitModel

_LOOKUP_CHUNK_SIZE = 1000
CLAIM_LEASE_SECONDS = 60.0
CLAIM_RENEWAL_SECONDS = 20.0
CLAIM_RETRY_SECONDS = 1.0


class ServicePersistence(Protocol):
    """Metadata operations required by the HTTP service."""

    @property
    def supports_claims(self) -> bool: ...

    def lookup_many(
        self, queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]: ...

    def commit_many(
        self,
        commits: Iterable[CommitModel],
        stored_artifacts: dict[str, StoredArtifact],
    ) -> tuple[CommitOutcome, ...]: ...

    def fetch_record(self, key: EvidenceKey) -> EvidenceRecord | None: ...

    def fetch_many(
        self, keys: Iterable[EvidenceKey]
    ) -> dict[EvidenceKey, EvidenceRecord]: ...

    def artifact_metadata(self, digest: str) -> StoredArtifact | None: ...

    def acquire_many(
        self, queries: Iterable[EvidenceQuery], *, owner_token: str
    ) -> tuple[ClaimAcquireResult, ...]: ...

    def renew_many(
        self, claims: Iterable[EvidenceClaim]
    ) -> tuple[EvidenceClaim, ...]: ...

    def release_many(self, claims: Iterable[EvidenceClaim]) -> None: ...

    def finalize_many(
        self,
        commits: Iterable[ClaimedCommitModel],
        stored_artifacts: dict[str, StoredArtifact],
    ) -> tuple[CommitOutcome, ...]: ...

    def close(self) -> None: ...


class PostgresEvidencePersistence:
    """Immutable evidence metadata backed by PostgreSQL."""

    def __init__(self, engine: Engine) -> None:
        if engine.dialect.name != "postgresql":
            raise ValueError("shared Store persistence requires PostgreSQL")
        self.engine = engine

    @classmethod
    def open(cls, database_url: str) -> PostgresEvidencePersistence:
        engine = create_engine(database_url, pool_pre_ping=True)
        try:
            upgrade_postgres_database(engine)
        except Exception:
            engine.dispose()
            raise
        return cls(engine)

    def close(self) -> None:
        self.engine.dispose()

    @property
    def supports_claims(self) -> bool:
        return True

    def acquire_many(
        self, queries: Iterable[EvidenceQuery], *, owner_token: str
    ) -> tuple[ClaimAcquireResult, ...]:
        requested = tuple(queries)
        if len(set(requested)) != len(requested):
            raise ValueError("acquire batch contains a duplicate evidence query")
        _validate_owner_token(owner_token)
        results: dict[EvidenceKey, ClaimAcquireResult] = {}
        with self.engine.begin() as connection:
            for query in sorted(requested, key=lambda item: _key_sort_value(item.key)):
                _insert_sequence(connection, query.identity)
                terminal = (
                    connection.execute(select(evidence).where(_key_clause(query.key)))
                    .mappings()
                    .one_or_none()
                )
                if terminal is not None:
                    connection.execute(
                        delete(evidence_claims).where(_claim_key_clause(query.key))
                    )
                    results[query.key] = ClaimAcquireResult(
                        ClaimDisposition.CACHED, record=_record_from_row(terminal)
                    )
                    continue
                now = datetime.now(UTC)
                expiry = now + timedelta(seconds=CLAIM_LEASE_SECONDS)
                inserted = connection.execute(
                    postgres_insert(evidence_claims)
                    .values(
                        **_claim_key_values(query.key),
                        semantic_parameters_json=query.key.semantic_parameters_json,
                        owner_token=owner_token,
                        generation=1,
                        expires_at=expiry,
                        created_at=now,
                        updated_at=now,
                    )
                    .on_conflict_do_nothing()
                    .returning(evidence_claims.c.sequence_id)
                ).scalar_one_or_none()
                if inserted is not None:
                    now = datetime.now(UTC)
                    expiry = now + timedelta(seconds=CLAIM_LEASE_SECONDS)
                    connection.execute(
                        update(evidence_claims)
                        .where(_claim_key_clause(query.key))
                        .values(expires_at=expiry, updated_at=now)
                    )
                    terminal = (
                        connection.execute(
                            select(evidence).where(_key_clause(query.key))
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if terminal is not None:
                        connection.execute(
                            delete(evidence_claims).where(_claim_key_clause(query.key))
                        )
                        results[query.key] = ClaimAcquireResult(
                            ClaimDisposition.CACHED, record=_record_from_row(terminal)
                        )
                    else:
                        results[query.key] = _acquired_result(
                            query.key, owner_token, 1, expiry
                        )
                    continue
                row = (
                    connection.execute(
                        select(evidence_claims)
                        .where(_claim_key_clause(query.key))
                        .with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                now = datetime.now(UTC)
                expiry = now + timedelta(seconds=CLAIM_LEASE_SECONDS)
                terminal = (
                    connection.execute(select(evidence).where(_key_clause(query.key)))
                    .mappings()
                    .one_or_none()
                )
                if terminal is not None:
                    if row is not None:
                        connection.execute(
                            delete(evidence_claims).where(_claim_key_clause(query.key))
                        )
                    results[query.key] = ClaimAcquireResult(
                        ClaimDisposition.CACHED, record=_record_from_row(terminal)
                    )
                elif row is None:
                    retried = connection.execute(
                        postgres_insert(evidence_claims)
                        .values(
                            **_claim_key_values(query.key),
                            semantic_parameters_json=query.key.semantic_parameters_json,
                            owner_token=owner_token,
                            generation=1,
                            expires_at=expiry,
                            created_at=now,
                            updated_at=now,
                        )
                        .on_conflict_do_nothing()
                        .returning(evidence_claims.c.sequence_id)
                    ).scalar_one_or_none()
                    if retried is None:
                        row = (
                            connection.execute(
                                select(evidence_claims)
                                .where(_claim_key_clause(query.key))
                                .with_for_update()
                            )
                            .mappings()
                            .one_or_none()
                        )
                        terminal = (
                            connection.execute(
                                select(evidence).where(_key_clause(query.key))
                            )
                            .mappings()
                            .one_or_none()
                        )
                        if terminal is not None:
                            if row is not None:
                                connection.execute(
                                    delete(evidence_claims).where(
                                        _claim_key_clause(query.key)
                                    )
                                )
                            results[query.key] = ClaimAcquireResult(
                                ClaimDisposition.CACHED,
                                record=_record_from_row(terminal),
                            )
                            continue
                        if row is None:
                            raise StoreIntegrityError(
                                "claim changed repeatedly during atomic acquire"
                            )
                        now = datetime.now(UTC)
                        expiry = now + timedelta(seconds=CLAIM_LEASE_SECONDS)
                        if _as_utc(row["expires_at"]) <= now:
                            generation = row["generation"] + 1
                            connection.execute(
                                update(evidence_claims)
                                .where(_claim_key_clause(query.key))
                                .values(
                                    owner_token=owner_token,
                                    generation=generation,
                                    expires_at=expiry,
                                    updated_at=now,
                                )
                            )
                            results[query.key] = _acquired_result(
                                query.key, owner_token, generation, expiry
                            )
                        elif row["owner_token"] == owner_token:
                            connection.execute(
                                update(evidence_claims)
                                .where(_claim_key_clause(query.key))
                                .values(expires_at=expiry, updated_at=now)
                            )
                            results[query.key] = _acquired_result(
                                query.key,
                                owner_token,
                                row["generation"],
                                expiry,
                            )
                        else:
                            results[query.key] = ClaimAcquireResult(
                                ClaimDisposition.BUSY,
                                busy=BusyEvidenceClaim(
                                    query.key,
                                    _as_utc(row["expires_at"]),
                                    CLAIM_RETRY_SECONDS,
                                ),
                            )
                        continue
                    terminal = (
                        connection.execute(
                            select(evidence).where(_key_clause(query.key))
                        )
                        .mappings()
                        .one_or_none()
                    )
                    if terminal is not None:
                        connection.execute(
                            delete(evidence_claims).where(_claim_key_clause(query.key))
                        )
                        results[query.key] = ClaimAcquireResult(
                            ClaimDisposition.CACHED, record=_record_from_row(terminal)
                        )
                    else:
                        results[query.key] = _acquired_result(
                            query.key, owner_token, 1, expiry
                        )
                elif (
                    row["owner_token"] == owner_token
                    and _as_utc(row["expires_at"]) > now
                ):
                    connection.execute(
                        update(evidence_claims)
                        .where(_claim_key_clause(query.key))
                        .values(expires_at=expiry, updated_at=now)
                    )
                    results[query.key] = _acquired_result(
                        query.key, owner_token, row["generation"], expiry
                    )
                elif _as_utc(row["expires_at"]) <= now:
                    generation = row["generation"] + 1
                    connection.execute(
                        update(evidence_claims)
                        .where(_claim_key_clause(query.key))
                        .values(
                            owner_token=owner_token,
                            generation=generation,
                            expires_at=expiry,
                            updated_at=now,
                        )
                    )
                    results[query.key] = _acquired_result(
                        query.key, owner_token, generation, expiry
                    )
                else:
                    results[query.key] = ClaimAcquireResult(
                        ClaimDisposition.BUSY,
                        busy=BusyEvidenceClaim(
                            query.key, _as_utc(row["expires_at"]), CLAIM_RETRY_SECONDS
                        ),
                    )
        return tuple(results[query.key] for query in requested)

    def renew_many(self, claims: Iterable[EvidenceClaim]) -> tuple[EvidenceClaim, ...]:
        requested = tuple(claims)
        if len({claim.key for claim in requested}) != len(requested):
            raise ValueError("claim renewal contains a duplicate evidence key")
        renewed: dict[EvidenceKey, EvidenceClaim] = {}
        with self.engine.begin() as connection:
            for claim in sorted(requested, key=lambda item: _key_sort_value(item.key)):
                row = (
                    connection.execute(
                        select(evidence_claims)
                        .where(_claim_key_clause(claim.key))
                        .with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                now = datetime.now(UTC)
                if (
                    row is None
                    or row["owner_token"] != claim.owner_token
                    or row["generation"] != claim.generation
                    or _as_utc(row["expires_at"]) <= now
                ):
                    raise EvidenceClaimLostError(
                        f"claim ownership was lost: {claim.key.sequence_id}"
                    )
                expiry = now + timedelta(seconds=CLAIM_LEASE_SECONDS)
                connection.execute(
                    update(evidence_claims)
                    .where(_claim_key_clause(claim.key))
                    .values(expires_at=expiry, updated_at=now)
                )
                renewed[claim.key] = EvidenceClaim(
                    claim.key,
                    claim.owner_token,
                    claim.generation,
                    expiry,
                    CLAIM_RENEWAL_SECONDS,
                )
        return tuple(renewed[claim.key] for claim in requested)

    def release_many(self, claims: Iterable[EvidenceClaim]) -> None:
        requested = tuple(claims)
        if len({claim.key for claim in requested}) != len(requested):
            raise ValueError("claim release contains a duplicate evidence key")
        with self.engine.begin() as connection:
            for claim in sorted(requested, key=lambda item: _key_sort_value(item.key)):
                row = (
                    connection.execute(
                        select(evidence_claims)
                        .where(_claim_key_clause(claim.key))
                        .with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                now = datetime.now(UTC)
                if (
                    row is None
                    or row["owner_token"] != claim.owner_token
                    or row["generation"] != claim.generation
                    or _as_utc(row["expires_at"]) <= now
                ):
                    raise EvidenceClaimLostError(
                        f"claim ownership was lost: {claim.key.sequence_id}"
                    )
                result = connection.execute(
                    update(evidence_claims)
                    .where(_claim_owner_clause(claim, now))
                    .values(expires_at=now, updated_at=now)
                )
                if result.rowcount != 1:
                    raise EvidenceClaimLostError(
                        f"claim ownership was lost: {claim.key.sequence_id}"
                    )

    def finalize_many(
        self,
        commits: Iterable[ClaimedCommitModel],
        stored_artifacts: dict[str, StoredArtifact],
    ) -> tuple[CommitOutcome, ...]:
        proposed = tuple(commits)
        if len({item.claim.key.to_domain() for item in proposed}) != len(proposed):
            raise ValueError("claim finalization contains a duplicate evidence key")
        outcomes: dict[EvidenceKey, CommitOutcome] = {}
        with self.engine.begin() as connection:
            for item in sorted(
                proposed, key=lambda value: _key_sort_value(value.claim.key.to_domain())
            ):
                claim = item.claim.to_domain()
                row = (
                    connection.execute(
                        select(evidence_claims)
                        .where(_claim_key_clause(claim.key))
                        .with_for_update()
                    )
                    .mappings()
                    .one_or_none()
                )
                now = datetime.now(UTC)
                if (
                    row is None
                    or row["owner_token"] != claim.owner_token
                    or row["generation"] != claim.generation
                    or _as_utc(row["expires_at"]) <= now
                ):
                    raise EvidenceClaimLostError(
                        f"claim ownership was lost: {claim.key.sequence_id}"
                    )
                consumed = connection.execute(
                    delete(evidence_claims)
                    .where(_claim_owner_clause(claim, now))
                    .returning(evidence_claims.c.sequence_id)
                ).scalar_one_or_none()
                if consumed is None:
                    raise EvidenceClaimLostError(
                        f"claim ownership was lost: {claim.key.sequence_id}"
                    )
            for digest in sorted(stored_artifacts):
                artifact = stored_artifacts[digest]
                _insert_artifact(connection, artifact)
            for item in sorted(
                proposed, key=lambda value: _key_sort_value(value.claim.key.to_domain())
            ):
                _insert_sequence(connection, item.commit.identity.to_domain())
                outcomes[item.commit.key.to_domain()] = _insert_evidence(
                    connection, item.commit
                )
        return tuple(outcomes[item.commit.key.to_domain()] for item in proposed)

    def lookup_many(
        self, queries: Iterable[EvidenceQuery]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        requested = tuple(dict.fromkeys(queries))
        identities: dict[str, SequenceIdentity] = {}
        for query in requested:
            existing = identities.setdefault(query.identity.sequence_id, query.identity)
            if existing != query.identity:
                raise StoreIntegrityError(
                    f"SequenceID collision in lookup: {query.identity.sequence_id}"
                )
        groups: dict[tuple[str, str, str, str], list[EvidenceKey]] = defaultdict(list)
        for query in requested:
            groups[query.key.contract_identity].append(query.key)

        found: dict[EvidenceKey, EvidenceRecord] = {}
        with self.engine.connect() as connection:
            for contract, group in groups.items():
                requested_keys = set(group)
                for offset in range(0, len(group), _LOOKUP_CHUNK_SIZE):
                    chunk = group[offset : offset + _LOOKUP_CHUNK_SIZE]
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
                    rows = tuple(connection.execute(statement).mappings())
                    returned_sequence_ids = {
                        row["sequence_id"]
                        for row in rows
                        if _record_from_row(row).key in requested_keys
                    }
                    _verify_sequences(
                        connection,
                        {
                            sequence_id: identities[sequence_id]
                            for sequence_id in returned_sequence_ids
                        },
                    )
                    for row in rows:
                        record = _record_from_row(row)
                        if record.key not in requested_keys:
                            continue
                        found[record.key] = record
        return found

    def commit_many(
        self,
        commits: Iterable[CommitModel],
        stored_artifacts: dict[str, StoredArtifact],
    ) -> tuple[CommitOutcome, ...]:
        proposed = tuple(commits)
        if len({commit.key.to_domain() for commit in proposed}) != len(proposed):
            raise ValueError("commit batch contains a duplicate evidence key")
        ordered = sorted(
            proposed, key=lambda item: _key_sort_value(item.key.to_domain())
        )
        outcomes: dict[EvidenceKey, CommitOutcome] = {}
        with self.engine.begin() as connection:
            for commit in ordered:
                connection.execute(
                    delete(evidence_claims).where(
                        _claim_key_clause(commit.key.to_domain())
                    )
                )
            for commit in ordered:
                _insert_sequence(connection, commit.identity.to_domain())
            for digest in sorted(stored_artifacts):
                artifact = stored_artifacts[digest]
                _insert_artifact(connection, artifact)
            for commit in ordered:
                outcomes[commit.key.to_domain()] = _insert_evidence(connection, commit)
        return tuple(outcomes[commit.key.to_domain()] for commit in proposed)

    def fetch_record(self, key: EvidenceKey) -> EvidenceRecord | None:
        return self.fetch_many((key,)).get(key)

    def fetch_many(
        self, keys: Iterable[EvidenceKey]
    ) -> dict[EvidenceKey, EvidenceRecord]:
        requested = tuple(dict.fromkeys(keys))
        groups: dict[tuple[str, str, str, str], list[EvidenceKey]] = defaultdict(list)
        for key in requested:
            groups[key.contract_identity].append(key)

        found: dict[EvidenceKey, EvidenceRecord] = {}
        with self.engine.connect() as connection:
            for contract, group in groups.items():
                requested_keys = set(group)
                for offset in range(0, len(group), _LOOKUP_CHUNK_SIZE):
                    chunk = group[offset : offset + _LOOKUP_CHUNK_SIZE]
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
                        record = _record_from_row(row)
                        if record.key in requested_keys:
                            found[record.key] = record
        return found

    def artifact_metadata(self, digest: str) -> StoredArtifact | None:
        with self.engine.connect() as connection:
            row = (
                connection.execute(
                    select(artifacts).where(artifacts.c.digest == digest)
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return StoredArtifact(
            digest=row["digest"],
            media_type=row["media_type"],
            byte_size=row["byte_size"],
            relative_path=row["relative_path"],
        )


def _insert_sequence(connection: Connection, identity: SequenceIdentity) -> None:
    statement = (
        postgres_insert(sequences)
        .values(
            sequence_id=identity.sequence_id,
            md5=identity.md5,
            length=identity.length,
            sequence=identity.sequence,
        )
        .on_conflict_do_nothing(index_elements=[sequences.c.sequence_id])
    )
    inserted = connection.execute(
        statement.returning(sequences.c.sequence_id)
    ).scalar_one_or_none()
    if inserted is not None:
        return
    _verify_sequence(connection, identity)


def _verify_sequence(connection: Connection, identity: SequenceIdentity) -> None:
    row = (
        connection.execute(
            select(sequences).where(sequences.c.sequence_id == identity.sequence_id)
        )
        .mappings()
        .one()
    )
    persisted = (row["md5"], row["length"], row["sequence"])
    requested = (identity.md5, identity.length, identity.sequence)
    if persisted != requested:
        raise StoreIntegrityError(
            f"SequenceID collision in shared Store: {identity.sequence_id}"
        )


def _verify_sequences(
    connection: Connection, identities: dict[str, SequenceIdentity]
) -> None:
    if not identities:
        return
    rows = connection.execute(
        select(sequences).where(sequences.c.sequence_id.in_(identities))
    ).mappings()
    observed = {}
    for row in rows:
        observed[row["sequence_id"]] = (row["md5"], row["length"], row["sequence"])
    for sequence_id, identity in identities.items():
        persisted = observed.get(sequence_id)
        requested = (identity.md5, identity.length, identity.sequence)
        if persisted != requested:
            raise StoreIntegrityError(
                f"SequenceID collision in shared Store: {sequence_id}"
            )


def _insert_artifact(connection: Connection, artifact: StoredArtifact) -> None:
    statement = (
        postgres_insert(artifacts)
        .values(
            digest=artifact.digest,
            media_type=artifact.media_type,
            byte_size=artifact.byte_size,
            relative_path=artifact.relative_path,
        )
        .on_conflict_do_nothing(index_elements=[artifacts.c.digest])
    )
    inserted = connection.execute(
        statement.returning(artifacts.c.digest)
    ).scalar_one_or_none()
    if inserted is not None:
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


def _insert_evidence(connection: Connection, commit: CommitModel) -> CommitOutcome:
    key = commit.key.to_domain()
    normalized_digest = (
        commit.normalized_artifact.digest if commit.normalized_artifact else None
    )
    raw_digest = commit.raw_artifact.digest if commit.raw_artifact else None
    statement = (
        postgres_insert(evidence)
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
    inserted = connection.execute(
        statement.returning(evidence.c.sequence_id)
    ).scalar_one_or_none()
    if inserted is not None:
        return CommitOutcome.CREATED
    row = connection.execute(select(evidence).where(_key_clause(key))).mappings().one()
    persisted = (
        row["semantic_parameters_json"],
        row["status"],
        row["payload_digest"],
    )
    proposed = (
        key.semantic_parameters_json,
        commit.status.value,
        commit.payload_digest,
    )
    if persisted != proposed:
        raise EvidenceConflictError(
            f"evidence key has a different immutable payload: {key.sequence_id}"
        )
    return CommitOutcome.EXISTING


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


def _key_clause(key: EvidenceKey) -> Any:
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


def _claim_key_clause(key: EvidenceKey) -> Any:
    return and_(
        evidence_claims.c.sequence_id == key.sequence_id,
        evidence_claims.c.adapter_contract_version == key.adapter_contract_version,
        evidence_claims.c.tool_runtime_digest == key.tool_runtime_digest,
        evidence_claims.c.resource_id == key.resource_id,
        evidence_claims.c.semantic_parameters_hash == key.semantic_parameters_hash,
    )


def _claim_owner_clause(claim: EvidenceClaim, now: datetime) -> Any:
    return and_(
        _claim_key_clause(claim.key),
        evidence_claims.c.owner_token == claim.owner_token,
        evidence_claims.c.generation == claim.generation,
        evidence_claims.c.expires_at > now,
    )


def _validate_owner_token(owner_token: str) -> None:
    if not owner_token or len(owner_token) > 255:
        raise ValueError("owner_token must contain 1 to 255 characters")


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _acquired_result(
    key: EvidenceKey, owner_token: str, generation: int, expiry: datetime
) -> ClaimAcquireResult:
    return ClaimAcquireResult(
        ClaimDisposition.ACQUIRED,
        claim=EvidenceClaim(
            key, owner_token, generation, expiry, CLAIM_RENEWAL_SECONDS
        ),
    )


def _key_sort_value(key: EvidenceKey) -> tuple[str, str, str, str, str]:
    return (
        key.sequence_id,
        key.adapter_contract_version,
        key.tool_runtime_digest,
        key.resource_id,
        key.semantic_parameters_hash,
    )
