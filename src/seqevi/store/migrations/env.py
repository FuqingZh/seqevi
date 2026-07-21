"""Alembic environment driven by a connection supplied by SeqEvi."""

from __future__ import annotations

from alembic import context
from sqlalchemy.engine import Connection

from seqevi.store.schema import metadata


def run_migrations_online() -> None:
    connection = context.config.attributes.get("connection")
    if not isinstance(connection, Connection):
        raise RuntimeError(
            "SeqEvi migrations require an existing SQLAlchemy connection"
        )

    context.configure(
        connection=connection,
        target_metadata=metadata,
        compare_type=True,
        render_as_batch=True,
    )
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    raise RuntimeError("SeqEvi does not run Store migrations in offline mode")
run_migrations_online()
