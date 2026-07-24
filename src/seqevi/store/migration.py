"""Alembic migration entrypoint for the embedded local schema."""

from __future__ import annotations

import fcntl
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Engine

_POSTGRES_MIGRATION_LOCK_ID = 0x534551455649


@contextmanager
def _exclusive_migration_lock(store_root: Path) -> Iterator[None]:
    lock_path = store_root / ".migration.lock"
    with lock_path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def upgrade_database(engine: Engine, store_root: Path) -> None:
    """Upgrade one Store schema to the package's current Alembic head."""

    script_location = Path(__file__).with_name("migrations")
    config = Config()
    config.set_main_option("script_location", str(script_location))

    with _exclusive_migration_lock(store_root), engine.connect() as connection:
        config.attributes["connection"] = connection
        command.upgrade(config, "head")


def upgrade_postgres_database(engine: Engine) -> None:
    """Upgrade a shared Store while holding a PostgreSQL advisory lock."""

    script_location = Path(__file__).with_name("migrations")
    config = Config()
    config.set_main_option("script_location", str(script_location))
    with engine.connect() as connection:
        connection.exec_driver_sql(
            "SELECT pg_advisory_lock(%s)", (_POSTGRES_MIGRATION_LOCK_ID,)
        )
        connection.commit()
        try:
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.exec_driver_sql(
                "SELECT pg_advisory_unlock(%s)", (_POSTGRES_MIGRATION_LOCK_ID,)
            )
            connection.commit()
