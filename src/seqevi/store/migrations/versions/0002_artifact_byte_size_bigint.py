"""Widen artifact byte sizes for large shared-Store payloads.

Revision ID: 0002_artifact_byte_size_bigint
Revises: 0001_initial_store
Create Date: 2026-07-24
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002_artifact_byte_size_bigint"
down_revision = "0001_initial_store"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.alter_column(
            "artifact",
            "byte_size",
            existing_type=sa.Integer(),
            type_=sa.BigInteger(),
            existing_nullable=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        op.alter_column(
            "artifact",
            "byte_size",
            existing_type=sa.BigInteger(),
            type_=sa.Integer(),
            existing_nullable=False,
        )
