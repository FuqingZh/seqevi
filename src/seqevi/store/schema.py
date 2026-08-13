"""SQLAlchemy Core schema shared by Store implementations."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    Text,
    func,
)

metadata = MetaData()

sequences = Table(
    "sequence",
    metadata,
    Column("sequence_id", String(35), primary_key=True),
    Column("md5", String(32), nullable=False),
    Column("length", Integer, nullable=False),
    Column("sequence", Text, nullable=False),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    CheckConstraint("length > 0", name="ck_sequence_length_positive"),
)
Index("ix_sequence_md5", sequences.c.md5)

artifacts = Table(
    "artifact",
    metadata,
    Column("digest", String(64), primary_key=True),
    Column("media_type", Text, nullable=False),
    Column("byte_size", BigInteger, nullable=False),
    Column("relative_path", Text, nullable=False, unique=True),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    CheckConstraint("byte_size >= 0", name="ck_artifact_byte_size_nonnegative"),
)

evidence = Table(
    "evidence",
    metadata,
    Column(
        "sequence_id",
        String(35),
        ForeignKey("sequence.sequence_id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column("adapter_contract_version", Text, primary_key=True),
    Column("tool_runtime_digest", Text, primary_key=True),
    Column("resource_id", Text, primary_key=True),
    Column("semantic_parameters_hash", String(64), primary_key=True),
    Column("semantic_parameters_json", Text, nullable=False),
    Column("status", String(16), nullable=False),
    Column("payload_digest", String(64), nullable=False),
    Column(
        "normalized_artifact_digest",
        String(64),
        ForeignKey("artifact.digest", ondelete="RESTRICT"),
        nullable=True,
    ),
    Column(
        "raw_artifact_digest",
        String(64),
        ForeignKey("artifact.digest", ondelete="RESTRICT"),
        nullable=True,
    ),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    CheckConstraint("status IN ('hit', 'no_hit')", name="ck_evidence_status"),
    CheckConstraint(
        "(status = 'hit' AND normalized_artifact_digest IS NOT NULL) OR "
        "(status = 'no_hit' AND normalized_artifact_digest IS NULL)",
        name="ck_evidence_normalized_artifact",
    ),
)

claim_sessions = Table(
    "claim_sessions",
    metadata,
    Column("session_id", String(64), primary_key=True),
    Column("owner_token", String(255), nullable=False),
    Column("generation", BigInteger, nullable=False),
    Column("state", String(16), nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    Column(
        "updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    CheckConstraint("generation > 0", name="ck_claim_sessions_generation_positive"),
    CheckConstraint("state IN ('open', 'closing')", name="ck_claim_sessions_state"),
)
Index("ix_claim_sessions_expiry", claim_sessions.c.state, claim_sessions.c.expires_at)

evidence_claim_generations = Table(
    "evidence_claim_generations",
    metadata,
    Column("sequence_id", String(35), primary_key=True),
    Column("adapter_contract_version", Text, primary_key=True),
    Column("tool_runtime_digest", Text, primary_key=True),
    Column("resource_id", Text, primary_key=True),
    Column("semantic_parameters_hash", String(64), primary_key=True),
    Column("high_water", BigInteger, nullable=False),
    CheckConstraint("high_water > 0", name="ck_evidence_claim_generations_positive"),
)

session_claims = Table(
    "session_claims",
    metadata,
    Column("sequence_id", String(35), primary_key=True),
    Column("adapter_contract_version", Text, primary_key=True),
    Column("tool_runtime_digest", Text, primary_key=True),
    Column("resource_id", Text, primary_key=True),
    Column("semantic_parameters_hash", String(64), primary_key=True),
    Column("semantic_parameters_json", Text, nullable=False),
    Column(
        "session_id",
        String(64),
        ForeignKey("claim_sessions.session_id", ondelete="RESTRICT"),
        nullable=False,
    ),
    Column("generation", BigInteger, nullable=False),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    CheckConstraint("generation > 0", name="ck_session_claims_generation_positive"),
)
Index("ix_session_claims_session_id", session_claims.c.session_id)

claim_session_open_receipts = Table(
    "claim_session_open_receipts",
    metadata,
    Column("open_request_id", String(64), primary_key=True),
    Column("request_digest", String(64), nullable=False),
    Column("session_id", String(64), nullable=False),
    Column("owner_token", String(255), nullable=False),
    Column("generation", BigInteger, nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column("closed", Integer, nullable=False, server_default="0"),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
)
Index(
    "ix_claim_session_open_receipts_created", claim_session_open_receipts.c.created_at
)

claim_session_acquire_receipts = Table(
    "claim_session_acquire_receipts",
    metadata,
    Column(
        "session_id",
        String(64),
        ForeignKey("claim_sessions.session_id", ondelete="RESTRICT"),
        primary_key=True,
    ),
    Column("request_id", String(64), primary_key=True),
    Column("query_digest", String(64), nullable=False),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
)
Index(
    "ix_claim_session_acquire_receipts_created",
    claim_session_acquire_receipts.c.created_at,
)

claim_session_acquire_receipt_items = Table(
    "claim_session_acquire_receipt_items",
    metadata,
    Column("session_id", String(64), nullable=False),
    Column("request_id", String(64), nullable=False),
    Column("input_index", Integer, nullable=False),
    Column("outcome", String(16), nullable=False),
    Column("generation", BigInteger),
    Column("busy_expires_at", DateTime(timezone=True)),
    Column("evidence_created_at", DateTime(timezone=True)),
    ForeignKeyConstraint(
        ["session_id", "request_id"],
        [
            "claim_session_acquire_receipts.session_id",
            "claim_session_acquire_receipts.request_id",
        ],
        ondelete="RESTRICT",
    ),
    CheckConstraint(
        "outcome IN ('cached', 'acquired', 'busy')",
        name="ck_claim_session_acquire_items_outcome",
    ),
    CheckConstraint("input_index >= 0", name="ck_claim_session_acquire_items_index"),
    PrimaryKeyConstraint("session_id", "request_id", "input_index"),
)
Index(
    "ix_claim_session_acquire_items_cleanup",
    claim_session_acquire_receipt_items.c.session_id,
    claim_session_acquire_receipt_items.c.request_id,
    claim_session_acquire_receipt_items.c.input_index,
)
