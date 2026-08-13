"""PostgreSQL metadata persistence for the shared evidence Store."""

from __future__ import annotations

import hashlib
import json
import secrets
from collections import defaultdict
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Iterator, Protocol

from sqlalchemy import and_, create_engine, delete, func, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.engine import Connection, Engine, RowMapping
from sqlalchemy.exc import DBAPIError, TimeoutError as SQLAlchemyTimeoutError

from seqevi.errors import (
    ClaimReceiptCapacityError,
    EvidenceClaimLostError,
    EvidenceConflictError,
    StoreBackpressureError,
    StoreIntegrityError,
)
from seqevi.evidence import (
    BusyEvidenceClaim,
    ClaimDisposition,
    CommitOutcome,
    EvidenceKey,
    EvidenceQuery,
    EvidenceRecord,
    EvidenceStatus,
    StoredArtifact,
    ClaimSessionAuthority,
    SessionClaimAcquireResult,
    SessionEvidenceClaim,
)
from seqevi.sequence import SequenceIdentity
from seqevi.store.migration import upgrade_postgres_database
from seqevi.store.schema import (
    artifacts,
    claim_session_acquire_receipt_items,
    claim_session_acquire_receipts,
    claim_session_open_receipts,
    claim_sessions,
    evidence,
    evidence_claim_generations,
    sequences,
    session_claims,
)
from seqevi.store.transport import (
    ClaimSessionFinalizeItem,
    CommitModel,
)

from .config import (
    DEFAULT_DATABASE_LOCK_TIMEOUT_SECONDS,
    DEFAULT_DATABASE_MAX_OVERFLOW,
    DEFAULT_DATABASE_POOL_SIZE,
    DEFAULT_DATABASE_POOL_TIMEOUT_SECONDS,
    DEFAULT_DATABASE_STATEMENT_TIMEOUT_SECONDS,
    DEFAULT_DATABASE_TRANSACTION_TIMEOUT_SECONDS,
)

_LOOKUP_CHUNK_SIZE = 1000
CLAIM_LEASE_SECONDS = 120.0
CLAIM_RENEWAL_SECONDS = 30.0
CLAIM_RENEW_DEADLINE_SECONDS = 90.0
CLAIM_RETRY_SECONDS = 1.0
_MINIMUM_POSTGRES_SERVER_VERSION_NUM = 170000
_CLAIM_KEY_NAMES = (
    "sequence_id",
    "adapter_contract_version",
    "tool_runtime_digest",
    "resource_id",
    "semantic_parameters_hash",
)


def _require_supported_postgres_version(server_version_num: int) -> None:
    if server_version_num < _MINIMUM_POSTGRES_SERVER_VERSION_NUM:
        major = server_version_num // 10000
        raise RuntimeError(
            "shared Store requires PostgreSQL 17 or newer for bounded mutation "
            f"transactions; detected PostgreSQL {major}"
        )


class ServicePersistence(Protocol):
    """Metadata operations required by the HTTP service."""

    @property
    def supports_claim_sessions(self) -> bool: ...

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

    def database_time(self) -> datetime: ...

    def open_claim_session(
        self, *, open_request_id: str, server_time: datetime, open_not_after: datetime
    ) -> ClaimSessionAuthority: ...

    def renew_claim_session(
        self, authority: ClaimSessionAuthority
    ) -> ClaimSessionAuthority: ...

    def close_claim_session(self, authority: ClaimSessionAuthority) -> None: ...

    def acquire_claim_session(
        self,
        authority: ClaimSessionAuthority,
        *,
        acquire_request_id: str,
        query_digest: str,
        queries: Iterable[EvidenceQuery],
    ) -> tuple[SessionClaimAcquireResult, ...]: ...

    def finalize_claim_session(
        self,
        authority: ClaimSessionAuthority,
        commits: Iterable[ClaimSessionFinalizeItem],
        stored_artifacts: dict[str, StoredArtifact],
    ) -> tuple[CommitOutcome, ...]: ...

    def claim_session_authority_is_live(
        self,
        authority: ClaimSessionAuthority,
        claims: Iterable[SessionEvidenceClaim],
    ) -> bool: ...

    def sweep_claim_sessions(self) -> int: ...

    def close(self) -> None: ...


class PostgresEvidencePersistence:
    """Immutable evidence metadata backed by PostgreSQL."""

    def __init__(
        self,
        engine: Engine,
        *,
        lock_timeout_seconds: float = DEFAULT_DATABASE_LOCK_TIMEOUT_SECONDS,
        statement_timeout_seconds: float = DEFAULT_DATABASE_STATEMENT_TIMEOUT_SECONDS,
        transaction_timeout_seconds: float = (
            DEFAULT_DATABASE_TRANSACTION_TIMEOUT_SECONDS
        ),
    ) -> None:
        if engine.dialect.name != "postgresql":
            raise ValueError("shared Store persistence requires PostgreSQL")
        self.engine = engine
        self.lock_timeout_seconds = lock_timeout_seconds
        self.statement_timeout_seconds = statement_timeout_seconds
        self.transaction_timeout_seconds = transaction_timeout_seconds

    @classmethod
    def open(
        cls,
        database_url: str,
        *,
        pool_size: int = DEFAULT_DATABASE_POOL_SIZE,
        max_overflow: int = DEFAULT_DATABASE_MAX_OVERFLOW,
        pool_timeout_seconds: float = DEFAULT_DATABASE_POOL_TIMEOUT_SECONDS,
        lock_timeout_seconds: float = DEFAULT_DATABASE_LOCK_TIMEOUT_SECONDS,
        statement_timeout_seconds: float = DEFAULT_DATABASE_STATEMENT_TIMEOUT_SECONDS,
        transaction_timeout_seconds: float = (
            DEFAULT_DATABASE_TRANSACTION_TIMEOUT_SECONDS
        ),
    ) -> PostgresEvidencePersistence:
        engine = create_engine(
            database_url,
            pool_pre_ping=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout_seconds,
        )
        try:
            with engine.connect() as connection:
                server_version_num = int(
                    connection.execute(text("SHOW server_version_num")).scalar_one()
                )
            _require_supported_postgres_version(server_version_num)
            upgrade_postgres_database(engine)
        except Exception:
            engine.dispose()
            raise
        return cls(
            engine,
            lock_timeout_seconds=lock_timeout_seconds,
            statement_timeout_seconds=statement_timeout_seconds,
            transaction_timeout_seconds=transaction_timeout_seconds,
        )

    def close(self) -> None:
        self.engine.dispose()

    @property
    def supports_claim_sessions(self) -> bool:
        return True

    @contextmanager
    def _connection(self) -> Iterator[Connection]:
        """Acquire one read connection and translate bounded-pool saturation."""

        try:
            with self.engine.connect() as connection:
                yield connection
        except SQLAlchemyTimeoutError as error:
            raise StoreBackpressureError(
                "shared Store database pool is saturated; retry the request"
            ) from error

    @contextmanager
    def _transaction(self) -> Iterator[Connection]:
        """Apply bounded PostgreSQL mutation deadlines and translate saturation."""

        try:
            with self.engine.begin() as connection:
                connection.execute(
                    text("SELECT set_config('transaction_timeout', :value, true)"),
                    {"value": f"{self.transaction_timeout_seconds:g}s"},
                )
                connection.execute(
                    text("SELECT set_config('lock_timeout', :value, true)"),
                    {"value": f"{self.lock_timeout_seconds:g}s"},
                )
                connection.execute(
                    text("SELECT set_config('statement_timeout', :value, true)"),
                    {"value": f"{self.statement_timeout_seconds:g}s"},
                )
                yield connection
        except SQLAlchemyTimeoutError as error:
            raise StoreBackpressureError(
                "shared Store database pool is saturated; retry the request"
            ) from error
        except DBAPIError as error:
            sqlstate = getattr(error.orig, "sqlstate", None) or getattr(
                error.orig, "pgcode", None
            )
            if sqlstate in {"25P04", "55P03", "57014"}:
                raise StoreBackpressureError(
                    "shared Store database mutation exceeded its wait budget; "
                    "retry the request"
                ) from error
            raise

    def database_time(self) -> datetime:
        with self._connection() as connection:
            return _as_utc(
                connection.execute(text("SELECT clock_timestamp()")).scalar_one()
            )

    def open_claim_session(
        self, *, open_request_id: str, server_time: datetime, open_not_after: datetime
    ) -> ClaimSessionAuthority:
        server_time = _as_utc(server_time)
        open_not_after = _as_utc(open_not_after)
        request_digest = hashlib.sha256(
            json.dumps(
                {
                    "protocol": "claim-session-v1",
                    "server_time": server_time.isoformat(),
                    "open_not_after": open_not_after.isoformat(),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        with self._transaction() as connection:
            connection.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {
                    "lock_id": int.from_bytes(
                        hashlib.sha256(open_request_id.encode()).digest()[:8],
                        "big",
                        signed=True,
                    )
                },
            )
            existing = (
                connection.execute(
                    select(claim_session_open_receipts)
                    .where(
                        claim_session_open_receipts.c.open_request_id == open_request_id
                    )
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            now = _database_now(connection)
            if existing is not None:
                if existing["request_digest"] != request_digest:
                    raise EvidenceConflictError(
                        "open_request_id was reused with different timing fields"
                    )
                session = (
                    connection.execute(
                        select(
                            claim_sessions.c.state, claim_sessions.c.expires_at
                        ).where(claim_sessions.c.session_id == existing["session_id"])
                    )
                    .mappings()
                    .one_or_none()
                )
                if (
                    session is None
                    or session["state"] != "open"
                    or _as_utc(session["expires_at"]) <= now
                ):
                    raise EvidenceClaimLostError(
                        "ClaimSession open receipt is terminal"
                    )
                return _session_authority_from_receipt(existing, now)
            if (
                server_time > now
                or open_not_after <= now
                or open_not_after - server_time > timedelta(seconds=30)
            ):
                raise TimeoutError("open_request_expired")
            session_id = secrets.token_hex(24)
            owner_token = secrets.token_urlsafe(32)
            expiry = now + timedelta(seconds=CLAIM_LEASE_SECONDS)
            inserted = connection.execute(
                text(
                    """
                INSERT INTO claim_sessions
                  (session_id, owner_token, generation, state, expires_at, created_at, updated_at)
                SELECT :session_id, :owner_token, 1, 'open', clock_timestamp() + interval '120 seconds', clock_timestamp(), clock_timestamp()
                WHERE clock_timestamp() <= :open_not_after
                RETURNING expires_at
                """
                ),
                {
                    "session_id": session_id,
                    "owner_token": owner_token,
                    "open_not_after": open_not_after,
                },
            ).scalar_one_or_none()
            if inserted is None:
                raise TimeoutError("open_request_expired")
            expiry = _as_utc(inserted)
            connection.execute(
                claim_session_open_receipts.insert().values(
                    open_request_id=open_request_id,
                    request_digest=request_digest,
                    session_id=session_id,
                    owner_token=owner_token,
                    generation=1,
                    expires_at=expiry,
                    closed=0,
                    created_at=now,
                )
            )
            return _authority(session_id, owner_token, 1, expiry, now)

    def renew_claim_session(
        self, authority: ClaimSessionAuthority
    ) -> ClaimSessionAuthority:
        with self._transaction() as connection:
            row = connection.execute(
                update(claim_sessions)
                .where(
                    claim_sessions.c.session_id == authority.session_id,
                    claim_sessions.c.owner_token == authority.owner_token,
                    claim_sessions.c.generation == authority.generation,
                    claim_sessions.c.state == "open",
                    claim_sessions.c.expires_at > func.clock_timestamp(),
                )
                .values(
                    expires_at=func.clock_timestamp() + text("interval '120 seconds'"),
                    updated_at=func.clock_timestamp(),
                )
                .returning(claim_sessions.c.expires_at)
            ).scalar_one_or_none()
            if row is None:
                raise EvidenceClaimLostError("ClaimSession authority was lost")
            now = _database_now(connection)
            return _authority(
                authority.session_id,
                authority.owner_token,
                authority.generation,
                _as_utc(row),
                now,
            )

    def close_claim_session(self, authority: ClaimSessionAuthority) -> None:
        with self._transaction() as connection:
            now = _database_now(connection)
            closed = connection.execute(
                update(claim_sessions)
                .where(
                    and_(
                        claim_sessions.c.session_id == authority.session_id,
                        claim_sessions.c.owner_token == authority.owner_token,
                        claim_sessions.c.generation == authority.generation,
                    )
                )
                .values(state="closing", expires_at=now, updated_at=now)
            )
            if closed.rowcount != 1:
                raise EvidenceClaimLostError("ClaimSession authority was lost")

    def claim_session_authority_is_live(
        self,
        authority: ClaimSessionAuthority,
        claims: Iterable[SessionEvidenceClaim],
    ) -> bool:
        requested = tuple(claims)
        with self._transaction() as connection:
            now = _database_now(connection)
            session_live = connection.execute(
                select(func.count())
                .select_from(claim_sessions)
                .where(_session_authority_clause(authority, now))
            ).scalar_one()
            if session_live != 1:
                return False
            if not requested:
                return True
            expected = {
                (*_key_sort_value(claim.key), claim.generation) for claim in requested
            }
            key_values = tuple(
                dict.fromkeys(_key_sort_value(claim.key) for claim in requested)
            )
            rows = connection.execute(
                select(
                    *(session_claims.c[name] for name in _CLAIM_KEY_NAMES),
                    session_claims.c.generation,
                ).where(
                    session_claims.c.session_id == authority.session_id,
                    tuple_(*(session_claims.c[name] for name in _CLAIM_KEY_NAMES)).in_(
                        key_values
                    ),
                )
            ).all()
            actual = {tuple(row) for row in rows}
            return actual == expected

    def sweep_claim_sessions(self) -> int:
        """Reclaim at most one fixed-width chunk of each coordination row type."""

        with self._transaction() as connection:
            now = _database_now(connection)
            stale = tuple(
                connection.execute(
                    select(claim_sessions.c.session_id)
                    .where(
                        (claim_sessions.c.state == "closing")
                        | (claim_sessions.c.expires_at <= now)
                    )
                    .order_by(claim_sessions.c.session_id)
                    .limit(1000)
                    .with_for_update(skip_locked=True)
                ).scalars()
            )
            claim_ids = (
                select(*session_claims.primary_key.columns)
                .where(session_claims.c.session_id.in_(stale))
                .order_by(*session_claims.primary_key.columns)
                .limit(1000)
                .with_for_update(skip_locked=True)
            )
            removed = connection.execute(
                delete(session_claims).where(
                    tuple_(*session_claims.primary_key.columns).in_(claim_ids)
                )
            ).rowcount
            cutoff = now - timedelta(seconds=60)
            item_ids = (
                select(*claim_session_acquire_receipt_items.primary_key.columns)
                .join(claim_session_acquire_receipts)
                .where(claim_session_acquire_receipts.c.created_at <= cutoff)
                .order_by(*claim_session_acquire_receipt_items.primary_key.columns)
                .limit(1000)
                .with_for_update(skip_locked=True)
            )
            removed += connection.execute(
                delete(claim_session_acquire_receipt_items).where(
                    tuple_(
                        *claim_session_acquire_receipt_items.primary_key.columns
                    ).in_(item_ids)
                )
            ).rowcount
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
                .with_for_update(skip_locked=True)
            )
            removed += connection.execute(
                delete(claim_session_acquire_receipts).where(
                    tuple_(
                        claim_session_acquire_receipts.c.session_id,
                        claim_session_acquire_receipts.c.request_id,
                    ).in_(header_ids)
                )
            ).rowcount
            empty_sessions = (
                select(claim_sessions.c.session_id)
                .where(
                    (
                        (claim_sessions.c.state == "closing")
                        | (claim_sessions.c.expires_at <= now)
                    ),
                    ~select(session_claims.c.sequence_id)
                    .where(session_claims.c.session_id == claim_sessions.c.session_id)
                    .exists(),
                    ~select(claim_session_acquire_receipts.c.request_id)
                    .where(
                        claim_session_acquire_receipts.c.session_id
                        == claim_sessions.c.session_id
                    )
                    .exists(),
                )
                .limit(1000)
                .with_for_update(skip_locked=True)
            )
            removed += connection.execute(
                delete(claim_sessions).where(
                    claim_sessions.c.session_id.in_(empty_sessions)
                )
            ).rowcount
            tombstones = (
                select(claim_session_open_receipts.c.open_request_id)
                .where(
                    claim_session_open_receipts.c.created_at
                    <= now - timedelta(seconds=120)
                )
                .limit(1000)
                .with_for_update(skip_locked=True)
            )
            removed += connection.execute(
                delete(claim_session_open_receipts).where(
                    claim_session_open_receipts.c.open_request_id.in_(tombstones)
                )
            ).rowcount
            return removed

    def acquire_claim_session(
        self,
        authority: ClaimSessionAuthority,
        *,
        acquire_request_id: str,
        query_digest: str,
        queries: Iterable[EvidenceQuery],
    ) -> tuple[SessionClaimAcquireResult, ...]:
        requested = tuple(queries)
        if len({query.key for query in requested}) != len(requested):
            raise ValueError("acquire batch contains a duplicate evidence key")
        results: dict[EvidenceKey, SessionClaimAcquireResult] = {}
        with self._transaction() as connection:
            _lock_evidence_keys(connection, (query.key for query in requested))
            owner_ids = set(
                connection.execute(
                    select(session_claims.c.session_id).where(
                        _session_claim_key_tuple().in_(
                            [_key_sort_value(query.key) for query in requested]
                        )
                    )
                ).scalars()
            )
            owner_ids.add(authority.session_id)
            locked = tuple(
                connection.execute(
                    select(claim_sessions)
                    .where(claim_sessions.c.session_id.in_(sorted(owner_ids)))
                    .order_by(claim_sessions.c.session_id)
                    .with_for_update()
                ).mappings()
            )
            now = _database_now(connection)
            requester = next(
                (row for row in locked if row["session_id"] == authority.session_id),
                None,
            )
            if (
                requester is None
                or requester["owner_token"] != authority.owner_token
                or requester["generation"] != authority.generation
                or requester["state"] != "open"
                or _as_utc(requester["expires_at"]) <= now
            ):
                raise EvidenceClaimLostError("ClaimSession authority was lost")
            receipt = (
                connection.execute(
                    select(claim_session_acquire_receipts).where(
                        claim_session_acquire_receipts.c.session_id
                        == authority.session_id,
                        claim_session_acquire_receipts.c.request_id
                        == acquire_request_id,
                    )
                )
                .mappings()
                .one_or_none()
            )
            if receipt is not None:
                if receipt["query_digest"] != query_digest:
                    raise EvidenceConflictError(
                        "acquire request ID was reused with another query digest"
                    )
                return _replay_acquire_receipt(
                    connection, requested, authority.session_id, acquire_request_id
                )
            cutoff = now - timedelta(seconds=60)
            expired_items = (
                select(*claim_session_acquire_receipt_items.primary_key.columns)
                .join(claim_session_acquire_receipts)
                .where(
                    claim_session_acquire_receipts.c.session_id == authority.session_id,
                    claim_session_acquire_receipts.c.created_at <= cutoff,
                )
                .limit(1000)
            )
            connection.execute(
                delete(claim_session_acquire_receipt_items).where(
                    tuple_(
                        *claim_session_acquire_receipt_items.primary_key.columns
                    ).in_(expired_items)
                )
            )
            connection.execute(
                delete(claim_session_acquire_receipts).where(
                    claim_session_acquire_receipts.c.session_id == authority.session_id,
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
            )
            header_count, item_count = connection.execute(
                select(
                    select(func.count())
                    .select_from(claim_session_acquire_receipts)
                    .where(
                        claim_session_acquire_receipts.c.session_id
                        == authority.session_id
                    )
                    .scalar_subquery(),
                    select(func.count())
                    .select_from(claim_session_acquire_receipt_items)
                    .where(
                        claim_session_acquire_receipt_items.c.session_id
                        == authority.session_id
                    )
                    .scalar_subquery(),
                )
            ).one()
            if header_count >= 1000 or item_count + len(requested) > 32000:
                raise ClaimReceiptCapacityError("claim_receipt_capacity")
            owner_by_id = {row["session_id"]: row for row in locked}
            identities = {
                query.identity.sequence_id: query.identity for query in requested
            }
            connection.execute(
                postgres_insert(sequences)
                .values(
                    [
                        {
                            "sequence_id": identity.sequence_id,
                            "md5": identity.md5,
                            "length": identity.length,
                            "sequence": identity.sequence,
                        }
                        for identity in identities.values()
                    ]
                )
                .on_conflict_do_nothing(index_elements=[sequences.c.sequence_id])
            )
            _verify_sequences(connection, identities)
            key_values = [_key_sort_value(query.key) for query in requested]
            terminal_by_key = {
                _key_sort_value(_record_from_row(row).key): row
                for row in connection.execute(
                    select(evidence).where(
                        tuple_(
                            evidence.c.sequence_id,
                            evidence.c.adapter_contract_version,
                            evidence.c.tool_runtime_digest,
                            evidence.c.resource_id,
                            evidence.c.semantic_parameters_hash,
                        ).in_(key_values)
                    )
                ).mappings()
            }
            claim_by_key = {
                tuple(row[column] for column in _CLAIM_KEY_NAMES): row
                for row in connection.execute(
                    select(session_claims).where(
                        _session_claim_key_tuple().in_(key_values)
                    )
                ).mappings()
            }
            stale_keys: list[tuple[str, str, str, str, str]] = []
            allocate: list[EvidenceQuery] = []
            for query in requested:
                key_value = _key_sort_value(query.key)
                terminal = terminal_by_key.get(key_value)
                if terminal is not None:
                    results[query.key] = SessionClaimAcquireResult(
                        ClaimDisposition.CACHED, record=_record_from_row(terminal)
                    )
                    continue
                row = claim_by_key.get(key_value)
                if row is not None and row["session_id"] == authority.session_id:
                    claim = SessionEvidenceClaim(query.key, row["generation"])
                elif (
                    row is not None
                    and (owner := owner_by_id.get(row["session_id"])) is not None
                    and owner["state"] == "open"
                    and _as_utc(owner["expires_at"]) > now
                ):
                    results[query.key] = SessionClaimAcquireResult(
                        ClaimDisposition.BUSY,
                        busy=BusyEvidenceClaim(
                            query.key, _as_utc(owner["expires_at"]), CLAIM_RETRY_SECONDS
                        ),
                    )
                    continue
                else:
                    if row is not None:
                        stale_keys.append(key_value)
                    allocate.append(query)
                    continue
                results[query.key] = SessionClaimAcquireResult(
                    ClaimDisposition.ACQUIRED, claim=claim
                )
            if stale_keys:
                connection.execute(
                    delete(session_claims).where(
                        _session_claim_key_tuple().in_(stale_keys)
                    )
                )
            if allocate:
                generation_rows = connection.execute(
                    postgres_insert(evidence_claim_generations)
                    .values(
                        [
                            {**_claim_key_values(query.key), "high_water": 1}
                            for query in allocate
                        ]
                    )
                    .on_conflict_do_update(
                        index_elements=list(
                            evidence_claim_generations.primary_key.columns
                        ),
                        set_={
                            "high_water": evidence_claim_generations.c.high_water + 1
                        },
                    )
                    .returning(
                        *evidence_claim_generations.primary_key.columns,
                        evidence_claim_generations.c.high_water,
                    )
                ).mappings()
                generations = {
                    tuple(row[column] for column in _CLAIM_KEY_NAMES): row["high_water"]
                    for row in generation_rows
                }
                connection.execute(
                    session_claims.insert(),
                    [
                        {
                            **_claim_key_values(query.key),
                            "semantic_parameters_json": query.key.semantic_parameters_json,
                            "session_id": authority.session_id,
                            "generation": generations[_key_sort_value(query.key)],
                            "created_at": now,
                        }
                        for query in allocate
                    ],
                )
                for query in allocate:
                    claim = SessionEvidenceClaim(
                        query.key, generations[_key_sort_value(query.key)]
                    )
                    results[query.key] = SessionClaimAcquireResult(
                        ClaimDisposition.ACQUIRED, claim=claim
                    )
            connection.execute(
                claim_session_acquire_receipts.insert().values(
                    session_id=authority.session_id,
                    request_id=acquire_request_id,
                    query_digest=query_digest,
                    created_at=now,
                )
            )
            connection.execute(
                claim_session_acquire_receipt_items.insert(),
                [
                    _acquire_receipt_item(
                        authority.session_id,
                        acquire_request_id,
                        index,
                        results[query.key],
                    )
                    for index, query in enumerate(requested)
                ],
            )
        return tuple(results[query.key] for query in requested)

    def finalize_claim_session(
        self,
        authority: ClaimSessionAuthority,
        commits: Iterable[ClaimSessionFinalizeItem],
        stored_artifacts: dict[str, StoredArtifact],
    ) -> tuple[CommitOutcome, ...]:
        proposed = tuple(commits)
        keys = tuple(item.commit.key.to_domain() for item in proposed)
        outcomes: dict[EvidenceKey, CommitOutcome] = {}
        with self._transaction() as connection:
            _lock_evidence_keys(connection, keys)
            locked_session = (
                connection.execute(
                    select(claim_sessions)
                    .where(claim_sessions.c.session_id == authority.session_id)
                    .with_for_update()
                )
                .mappings()
                .one_or_none()
            )
            now = _database_now(connection)
            if (
                locked_session is None
                or locked_session["owner_token"] != authority.owner_token
                or locked_session["generation"] != authority.generation
                or locked_session["state"] != "open"
                or _as_utc(locked_session["expires_at"]) <= now
            ):
                raise EvidenceClaimLostError("ClaimSession authority was lost")
            key_values = [_key_sort_value(key) for key in keys]
            claims = {
                tuple(row[column] for column in _CLAIM_KEY_NAMES): row
                for row in connection.execute(
                    select(session_claims).where(
                        _session_claim_key_tuple().in_(key_values)
                    )
                ).mappings()
            }
            terminal = {
                tuple(row[column] for column in _CLAIM_KEY_NAMES): row
                for row in connection.execute(
                    select(evidence).where(
                        tuple_(
                            evidence.c.sequence_id,
                            evidence.c.adapter_contract_version,
                            evidence.c.tool_runtime_digest,
                            evidence.c.resource_id,
                            evidence.c.semantic_parameters_hash,
                        ).in_(key_values)
                    )
                ).mappings()
            }
            for item, key in zip(proposed, keys, strict=True):
                row = claims.get(_key_sort_value(key))
                if (
                    row is None
                    or row["session_id"] != authority.session_id
                    or row["generation"] != item.claim_generation
                ) and _key_sort_value(key) not in terminal:
                    raise EvidenceClaimLostError("exact claim was fenced")
            connection.execute(
                delete(session_claims).where(
                    session_claims.c.session_id == authority.session_id,
                    _session_claim_key_tuple().in_(key_values),
                )
            )
            identities = {
                item.commit.identity.sequence_id: item.commit.identity.to_domain()
                for item in proposed
            }
            connection.execute(
                postgres_insert(sequences)
                .values(
                    [
                        {
                            "sequence_id": identity.sequence_id,
                            "md5": identity.md5,
                            "length": identity.length,
                            "sequence": identity.sequence,
                        }
                        for identity in identities.values()
                    ]
                )
                .on_conflict_do_nothing(index_elements=[sequences.c.sequence_id])
            )
            _verify_sequences(connection, identities)
            if stored_artifacts:
                connection.execute(
                    postgres_insert(artifacts)
                    .values(
                        [
                            {
                                "digest": artifact.digest,
                                "media_type": artifact.media_type,
                                "byte_size": artifact.byte_size,
                                "relative_path": artifact.relative_path,
                            }
                            for artifact in stored_artifacts.values()
                        ]
                    )
                    .on_conflict_do_nothing(index_elements=[artifacts.c.digest])
                )
                persisted_artifacts = {
                    row["digest"]: row
                    for row in connection.execute(
                        select(artifacts).where(
                            artifacts.c.digest.in_(stored_artifacts)
                        )
                    ).mappings()
                }
                for digest, artifact in stored_artifacts.items():
                    row = persisted_artifacts[digest]
                    if (row["media_type"], row["byte_size"], row["relative_path"]) != (
                        artifact.media_type,
                        artifact.byte_size,
                        artifact.relative_path,
                    ):
                        raise StoreIntegrityError(
                            f"artifact metadata conflict: {digest}"
                        )
            created_keys = {
                tuple(row)
                for row in connection.execute(
                    postgres_insert(evidence)
                    .values([_evidence_values(item.commit) for item in proposed])
                    .on_conflict_do_nothing(
                        index_elements=list(evidence.primary_key.columns)
                    )
                    .returning(*evidence.primary_key.columns)
                )
            }
            persisted = {
                tuple(row[column] for column in _CLAIM_KEY_NAMES): row
                for row in connection.execute(
                    select(evidence).where(
                        tuple_(*evidence.primary_key.columns).in_(key_values)
                    )
                ).mappings()
            }
            for item, key in zip(proposed, keys, strict=True):
                row = persisted[_key_sort_value(key)]
                if (
                    row["semantic_parameters_json"],
                    row["status"],
                    row["payload_digest"],
                ) != (
                    key.semantic_parameters_json,
                    item.commit.status.value,
                    item.commit.payload_digest,
                ):
                    raise EvidenceConflictError(
                        f"evidence key has a different immutable payload: {key.sequence_id}"
                    )
                outcomes[key] = (
                    CommitOutcome.CREATED
                    if _key_sort_value(key) in created_keys
                    else CommitOutcome.EXISTING
                )
        return tuple(outcomes[key] for key in keys)

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
        with self._connection() as connection:
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
        with self._transaction() as connection:
            _lock_evidence_keys(
                connection, (commit.key.to_domain() for commit in proposed)
            )
            for commit in ordered:
                connection.execute(
                    delete(session_claims).where(
                        _session_claim_key_clause(commit.key.to_domain())
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
        with self._connection() as connection:
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
        with self._connection() as connection:
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


def _evidence_values(commit: CommitModel) -> dict[str, object]:
    key = commit.key.to_domain()
    return {
        **_claim_key_values(key),
        "semantic_parameters_json": key.semantic_parameters_json,
        "status": commit.status.value,
        "payload_digest": commit.payload_digest,
        "normalized_artifact_digest": (
            None
            if commit.normalized_artifact is None
            else commit.normalized_artifact.digest
        ),
        "raw_artifact_digest": (
            None if commit.raw_artifact is None else commit.raw_artifact.digest
        ),
    }


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


def _lock_evidence_keys(connection: Connection, keys: Iterable[EvidenceKey]) -> None:
    """Serialize mutations even when neither evidence nor claim rows exist."""

    lock_ids = sorted({_advisory_lock_id(key) for key in keys})
    if lock_ids:
        connection.execute(
            text(
                "SELECT pg_advisory_xact_lock(lock_id) "
                "FROM unnest(CAST(:lock_ids AS bigint[])) AS lock_id "
                "ORDER BY lock_id"
            ),
            {"lock_ids": lock_ids},
        )


def _advisory_lock_id(key: EvidenceKey) -> int:
    digest = hashlib.sha256("\0".join(_key_sort_value(key)).encode()).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


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


def _session_claim_key_tuple() -> Any:
    return tuple_(
        session_claims.c.sequence_id,
        session_claims.c.adapter_contract_version,
        session_claims.c.tool_runtime_digest,
        session_claims.c.resource_id,
        session_claims.c.semantic_parameters_hash,
    )


def _as_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _database_now(connection: Connection) -> datetime:
    return _as_utc(connection.execute(text("SELECT clock_timestamp()")).scalar_one())


def _authority(
    session_id: str,
    owner_token: str,
    generation: int,
    expires_at: datetime,
    now: datetime,
) -> ClaimSessionAuthority:
    remaining = max((expires_at - now).total_seconds(), 0.001)
    return ClaimSessionAuthority(
        session_id=session_id,
        owner_token=owner_token,
        generation=generation,
        expires_at=expires_at,
        remaining_lease_seconds=remaining,
        heartbeat_after_seconds=CLAIM_RENEWAL_SECONDS,
        renew_deadline_seconds=CLAIM_RENEW_DEADLINE_SECONDS,
    )


def _session_authority_from_receipt(
    row: RowMapping, now: datetime
) -> ClaimSessionAuthority:
    expiry = _as_utc(row["expires_at"])
    if expiry <= now:
        raise EvidenceClaimLostError("ClaimSession open receipt is expired")
    return _authority(
        row["session_id"], row["owner_token"], row["generation"], expiry, now
    )


def _session_authority_clause(authority: ClaimSessionAuthority, now: datetime) -> Any:
    return and_(
        claim_sessions.c.session_id == authority.session_id,
        claim_sessions.c.owner_token == authority.owner_token,
        claim_sessions.c.generation == authority.generation,
        claim_sessions.c.state == "open",
        claim_sessions.c.expires_at > now,
    )


def _acquire_receipt_item(
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


def _replay_acquire_receipt(
    connection: Connection,
    queries: tuple[EvidenceQuery, ...],
    session_id: str,
    request_id: str,
) -> tuple[SessionClaimAcquireResult, ...]:
    items = tuple(
        connection.execute(
            select(claim_session_acquire_receipt_items)
            .where(
                claim_session_acquire_receipt_items.c.session_id == session_id,
                claim_session_acquire_receipt_items.c.request_id == request_id,
            )
            .order_by(claim_session_acquire_receipt_items.c.input_index)
        ).mappings()
    )
    if len(items) != len(queries):
        raise StoreIntegrityError("acquire receipt has incomplete item rows")
    replayed: list[SessionClaimAcquireResult] = []
    for index, (query, item) in enumerate(zip(queries, items, strict=True)):
        if item["input_index"] != index:
            raise StoreIntegrityError("acquire receipt item order is corrupt")
        outcome = ClaimDisposition(item["outcome"])
        if outcome is ClaimDisposition.CACHED:
            row = (
                connection.execute(select(evidence).where(_key_clause(query.key)))
                .mappings()
                .one_or_none()
            )
            if row is None or _as_utc(row["created_at"]) != _as_utc(
                item["evidence_created_at"]
            ):
                raise StoreIntegrityError("cached acquire receipt cannot be replayed")
            replayed.append(
                SessionClaimAcquireResult(outcome, record=_record_from_row(row))
            )
        elif outcome is ClaimDisposition.ACQUIRED:
            generation = item["generation"]
            if generation is None:
                raise StoreIntegrityError("acquired receipt has no generation")
            replayed.append(
                SessionClaimAcquireResult(
                    outcome, claim=SessionEvidenceClaim(query.key, generation)
                )
            )
        else:
            expires_at = item["busy_expires_at"]
            if expires_at is None:
                raise StoreIntegrityError("busy receipt has no expiry")
            replayed.append(
                SessionClaimAcquireResult(
                    outcome,
                    busy=BusyEvidenceClaim(
                        query.key, _as_utc(expires_at), CLAIM_RETRY_SECONDS
                    ),
                )
            )
    return tuple(replayed)


def _key_sort_value(key: EvidenceKey) -> tuple[str, str, str, str, str]:
    return (
        key.sequence_id,
        key.adapter_contract_version,
        key.tool_runtime_digest,
        key.resource_id,
        key.semantic_parameters_hash,
    )
