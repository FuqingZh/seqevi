"""Add ephemeral per-EvidenceKey claim leases.

Revision ID: 0003_evidence_claim_leases
Revises: 0002_artifact_byte_size_bigint
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0003_evidence_claim_leases"
down_revision = "0002_artifact_byte_size_bigint"
branch_labels = None
depends_on = None


def upgrade() -> None:
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


def downgrade() -> None:
    op.drop_index("ix_evidence_claim_expires_at", table_name="evidence_claim")
    op.drop_table("evidence_claim")
