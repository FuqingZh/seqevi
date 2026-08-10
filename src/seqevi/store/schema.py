"""SQLAlchemy Core schema shared by Store implementations."""

from __future__ import annotations

from sqlalchemy import (
    CheckConstraint,
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
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

evidence_claims = Table(
    "evidence_claim",
    metadata,
    Column("sequence_id", String(35), primary_key=True),
    Column("adapter_contract_version", Text, primary_key=True),
    Column("tool_runtime_digest", Text, primary_key=True),
    Column("resource_id", Text, primary_key=True),
    Column("semantic_parameters_hash", String(64), primary_key=True),
    Column("semantic_parameters_json", Text, nullable=False),
    Column("owner_token", String(255), nullable=False),
    Column("generation", BigInteger, nullable=False),
    Column("expires_at", DateTime(timezone=True), nullable=False),
    Column(
        "created_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    Column(
        "updated_at", DateTime(timezone=True), nullable=False, server_default=func.now()
    ),
    CheckConstraint("generation > 0", name="ck_evidence_claim_generation_positive"),
)
Index("ix_evidence_claim_expires_at", evidence_claims.c.expires_at)
