"""Represent immutable artifact content in POSIX or OCI storage.

Revision ID: 0005_oci_artifact_storage
Revises: 0004_claim_sessions
Create Date: 2026-08-31
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0005_oci_artifact_storage"
down_revision = "0004_claim_sessions"
branch_labels = None
depends_on = None

_STORAGE_REFERENCE_CONSTRAINT = (
    "(storage_kind = 'posix' AND relative_path IS NOT NULL "
    "AND registry_id IS NULL AND repository IS NULL AND manifest_digest IS NULL) "
    "OR (storage_kind = 'oci' AND relative_path IS NULL "
    "AND registry_id IS NOT NULL AND repository IS NOT NULL "
    "AND manifest_digest IS NOT NULL)"
)


def upgrade() -> None:
    # The temporary server default makes the existing POSIX rows explicit.  New
    # code must supply storage_kind itself; leave no database default behind.
    op.add_column(
        "artifact",
        sa.Column(
            "storage_kind",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'posix'"),
        ),
    )
    op.add_column("artifact", sa.Column("registry_id", sa.Text(), nullable=True))
    op.add_column("artifact", sa.Column("repository", sa.Text(), nullable=True))
    op.add_column(
        "artifact", sa.Column("manifest_digest", sa.String(length=64), nullable=True)
    )
    with op.batch_alter_table("artifact") as batch:
        batch.alter_column("relative_path", existing_type=sa.Text(), nullable=True)
        batch.create_check_constraint(
            "ck_artifact_storage_reference", _STORAGE_REFERENCE_CONSTRAINT
        )
    with op.batch_alter_table("artifact") as batch:
        batch.alter_column("storage_kind", server_default=None)


def downgrade() -> None:
    bind = op.get_bind()
    oci_artifact_count = bind.execute(
        sa.text("SELECT count(*) FROM artifact WHERE storage_kind = 'oci'")
    ).scalar_one()
    if oci_artifact_count:
        raise RuntimeError(
            "cannot downgrade OCI artifact storage while OCI artifact rows exist"
        )

    with op.batch_alter_table("artifact") as batch:
        batch.drop_constraint("ck_artifact_storage_reference", type_="check")
        batch.drop_column("manifest_digest")
        batch.drop_column("repository")
        batch.drop_column("registry_id")
        batch.drop_column("storage_kind")
        batch.alter_column("relative_path", existing_type=sa.Text(), nullable=False)
