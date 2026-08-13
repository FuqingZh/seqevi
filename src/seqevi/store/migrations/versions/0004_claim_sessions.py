"""Replace ephemeral per-key leases with ClaimSession coordination.

Revision ID: 0004_claim_sessions
Revises: 0003_evidence_claim_leases
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from seqevi.store.schema import (
    claim_session_acquire_receipt_items,
    claim_session_acquire_receipts,
    claim_session_open_receipts,
    claim_sessions,
    evidence_claim_generations,
    session_claims,
)

revision = "0004_claim_sessions"
down_revision = "0003_evidence_claim_leases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 0003 authority is deliberately ephemeral and cannot be translated safely.
    op.drop_index("ix_evidence_claim_expires_at", table_name="evidence_claim")
    op.drop_table("evidence_claim")
    bind = op.get_bind()
    for table in (
        claim_sessions,
        evidence_claim_generations,
        session_claims,
        claim_session_open_receipts,
        claim_session_acquire_receipts,
        claim_session_acquire_receipt_items,
    ):
        table.create(bind, checkfirst=False)


def downgrade() -> None:
    bind = op.get_bind()
    for table in (
        claim_session_acquire_receipt_items,
        claim_session_acquire_receipts,
        session_claims,
        evidence_claim_generations,
        claim_sessions,
        claim_session_open_receipts,
    ):
        table.drop(bind, checkfirst=False)
    op.create_table(
        "evidence_claim",
        sa.Column("sequence_id", sa.String(length=35), nullable=False),
        sa.Column("adapter_contract_version", sa.Text(), nullable=False),
        sa.Column("tool_runtime_digest", sa.Text(), nullable=False),
        sa.Column("resource_id", sa.Text(), nullable=False),
        sa.Column("semantic_parameters_hash", sa.String(length=64), nullable=False),
        sa.Column("semantic_parameters_json", sa.Text(), nullable=False),
        sa.Column("owner_token", sa.String(length=255), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "generation > 0", name="ck_evidence_claim_generation_positive"
        ),
        sa.PrimaryKeyConstraint(
            "sequence_id",
            "adapter_contract_version",
            "tool_runtime_digest",
            "resource_id",
            "semantic_parameters_hash",
            name="pk_evidence_claim",
        ),
    )
    op.create_index("ix_evidence_claim_expires_at", "evidence_claim", ["expires_at"])
