"""Create the initial immutable evidence Store.

Revision ID: 0001_initial_store
Revises:
Create Date: 2026-07-21
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001_initial_store"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "sequence",
        sa.Column("sequence_id", sa.String(length=35), nullable=False),
        sa.Column("md5", sa.String(length=32), nullable=False),
        sa.Column("length", sa.Integer(), nullable=False),
        sa.Column("sequence", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint("length > 0", name="ck_sequence_length_positive"),
        sa.PrimaryKeyConstraint("sequence_id"),
    )
    op.create_index("ix_sequence_md5", "sequence", ["md5"], unique=False)

    op.create_table(
        "artifact",
        sa.Column("digest", sa.String(length=64), nullable=False),
        sa.Column("media_type", sa.Text(), nullable=False),
        sa.Column("byte_size", sa.BigInteger(), nullable=False),
        sa.Column("relative_path", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint("byte_size >= 0", name="ck_artifact_byte_size_nonnegative"),
        sa.PrimaryKeyConstraint("digest"),
        sa.UniqueConstraint("relative_path"),
    )

    op.create_table(
        "evidence",
        sa.Column("sequence_id", sa.String(length=35), nullable=False),
        sa.Column("adapter_contract_version", sa.Text(), nullable=False),
        sa.Column("tool_runtime_digest", sa.Text(), nullable=False),
        sa.Column("resource_id", sa.Text(), nullable=False),
        sa.Column("semantic_parameters_hash", sa.String(length=64), nullable=False),
        sa.Column("semantic_parameters_json", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("payload_digest", sa.String(length=64), nullable=False),
        sa.Column("normalized_artifact_digest", sa.String(length=64), nullable=True),
        sa.Column("raw_artifact_digest", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(status = 'hit' AND normalized_artifact_digest IS NOT NULL) OR "
            "(status = 'no_hit' AND normalized_artifact_digest IS NULL)",
            name="ck_evidence_normalized_artifact",
        ),
        sa.CheckConstraint("status IN ('hit', 'no_hit')", name="ck_evidence_status"),
        sa.ForeignKeyConstraint(
            ["normalized_artifact_digest"],
            ["artifact.digest"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["raw_artifact_digest"], ["artifact.digest"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["sequence_id"], ["sequence.sequence_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint(
            "sequence_id",
            "adapter_contract_version",
            "tool_runtime_digest",
            "resource_id",
            "semantic_parameters_hash",
        ),
    )


def downgrade() -> None:
    op.drop_table("evidence")
    op.drop_table("artifact")
    op.drop_index("ix_sequence_md5", table_name="sequence")
    op.drop_table("sequence")
