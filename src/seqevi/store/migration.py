"""Alembic migration entrypoint for the embedded local schema."""

from __future__ import annotations

import fcntl
import random
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, cast

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError

_POSTGRES_MIGRATION_LOCK_ID = 0x534551455649
_MAINTENANCE_TIMEOUT_SECONDS = 60.0
_CURRENT_REVISION = "0004_claim_sessions"
_AUTOMATIC_EXISTING_CEILING = "0003_evidence_claim_leases"


@dataclass(frozen=True, slots=True)
class MaintenanceAcknowledgement:
    """Single-target operator acknowledgement for destructive coordination DDL."""

    database_identity: str
    expected_revision: str


@contextmanager
def _exclusive_migration_lock(store_root: Path) -> Iterator[None]:
    lock_path = store_root / ".migration.lock"
    with lock_path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _configure(connection: Connection) -> Config:
    script_location = Path(__file__).with_name("migrations")
    config = Config()
    config.set_main_option("script_location", str(script_location))
    config.attributes["connection"] = connection
    return config


def _revision(connection: Connection) -> str | None:
    if "alembic_version" not in inspect(connection).get_table_names():
        return None
    return connection.execute(
        text("SELECT version_num FROM alembic_version")
    ).scalar_one()


def _pristine(connection: Connection) -> bool:
    names = set(inspect(connection).get_table_names())
    return not names or names == {"alembic_version"} and _revision(connection) is None


def _database_identity(engine: Engine) -> str:
    return engine.url.render_as_string(hide_password=True)


def upgrade_database(engine: Engine, store_root: Path) -> None:
    """Bootstrap pristine SQLite to 0004; fail closed for existing pre-0004 Stores."""

    with _exclusive_migration_lock(store_root), engine.connect() as connection:
        connection.exec_driver_sql("BEGIN EXCLUSIVE")
        try:
            revision = _revision(connection)
            if revision == _CURRENT_REVISION:
                connection.commit()
                return
            if _pristine(connection):
                command.upgrade(_configure(connection), "head")
                connection.commit()
                return
            if revision is None:
                raise RuntimeError(
                    "ambiguous unversioned Store refuses automatic migration"
                )
            if revision != _AUTOMATIC_EXISTING_CEILING:
                command.upgrade(_configure(connection), _AUTOMATIC_EXISTING_CEILING)
                connection.commit()
            raise RuntimeError(
                "existing Store requires the explicit ClaimSession 0004 maintenance upgrade"
            )
        except Exception:
            connection.rollback()
            raise


def upgrade_postgres_database(engine: Engine) -> None:
    """Bootstrap pristine PostgreSQL to 0004; fail closed for existing Stores."""

    with engine.connect() as connection:
        connection.exec_driver_sql(
            "SELECT pg_advisory_lock(%s)", (_POSTGRES_MIGRATION_LOCK_ID,)
        )
        connection.commit()
        try:
            revision = _revision(connection)
            if revision == _CURRENT_REVISION:
                return
            if _pristine(connection):
                command.upgrade(_configure(connection), "head")
            elif revision is None:
                raise RuntimeError(
                    "ambiguous unversioned Store refuses automatic migration"
                )
            else:
                if revision != _AUTOMATIC_EXISTING_CEILING:
                    command.upgrade(_configure(connection), _AUTOMATIC_EXISTING_CEILING)
                    connection.commit()
                raise RuntimeError(
                    "existing Store requires the explicit ClaimSession 0004 maintenance upgrade"
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.exec_driver_sql(
                "SELECT pg_advisory_unlock(%s)", (_POSTGRES_MIGRATION_LOCK_ID,)
            )
            connection.commit()


def maintenance_upgrade_database(
    engine: Engine,
    store_root: Path | None,
    acknowledgement: MaintenanceAcknowledgement,
) -> None:
    """Apply 0004 under a bounded persistence fence after operator quiescence."""

    if acknowledgement.database_identity != _database_identity(engine):
        raise RuntimeError("maintenance acknowledgement targets another database")
    deadline = time.monotonic() + _MAINTENANCE_TIMEOUT_SECONDS
    if engine.dialect.name == "sqlite":
        if store_root is None:
            raise ValueError("SQLite maintenance requires the Store root")
        with _bounded_file_lock(store_root, deadline), engine.connect() as connection:
            remaining = _remaining(deadline)
            connection.exec_driver_sql(
                f"PRAGMA busy_timeout={max(int(remaining * 1000), 1)}"
            )
            raw = cast(Any, connection.connection.driver_connection)
            raw.set_progress_handler(
                lambda: 1 if time.monotonic() >= deadline else 0, 1000
            )
            try:
                connection.exec_driver_sql("BEGIN EXCLUSIVE")
                _run_maintenance_upgrade(connection, acknowledgement, deadline)
            finally:
                raw.set_progress_handler(None, 0)
        return
    _maintenance_upgrade_postgres(engine, acknowledgement, deadline)


@contextmanager
def _bounded_file_lock(store_root: Path, deadline: float) -> Iterator[None]:
    lock_path = store_root / ".migration.lock"
    with lock_path.open("a+b") as handle:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(min(random.uniform(1.0, 5.0), _remaining(deadline)))
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("ClaimSession maintenance deadline expired")
    return remaining


def _run_maintenance_upgrade(
    connection: Connection,
    acknowledgement: MaintenanceAcknowledgement,
    deadline: float,
) -> None:
    revision = _revision(connection)
    if revision != acknowledgement.expected_revision:
        connection.rollback()
        raise RuntimeError("maintenance acknowledgement has a stale revision")
    if revision != _AUTOMATIC_EXISTING_CEILING:
        connection.rollback()
        raise RuntimeError("maintenance upgrade requires revision 0003")
    clock = (
        "clock_timestamp()"
        if connection.dialect.name == "postgresql"
        else "CURRENT_TIMESTAMP"
    )
    live_claims = connection.execute(
        text(f"SELECT count(*) FROM evidence_claim WHERE expires_at > {clock}")  # noqa: S608
    ).scalar_one()
    if live_claims:
        connection.rollback()
        raise RuntimeError("maintenance upgrade refuses unexpired evidence claims")
    _remaining(deadline)
    command.upgrade(_configure(connection), _CURRENT_REVISION)
    _remaining(deadline)
    connection.commit()


def _maintenance_upgrade_postgres(
    engine: Engine,
    acknowledgement: MaintenanceAcknowledgement,
    deadline: float,
) -> None:
    with engine.connect() as connection:
        acquired = False
        primary: BaseException | None = None
        cleanup: BaseException | None = None
        try:
            while not acquired:
                acquired = bool(
                    connection.exec_driver_sql(
                        "SELECT pg_try_advisory_lock(%s)",
                        (_POSTGRES_MIGRATION_LOCK_ID,),
                    ).scalar_one()
                )
                connection.commit()
                if not acquired:
                    time.sleep(min(random.uniform(1.0, 5.0), _remaining(deadline)))
            while True:
                try:
                    _arm_postgres_transaction_timeout(connection, deadline)
                    connection.exec_driver_sql("BEGIN")
                    lock_ms = max(min(int(_remaining(deadline) * 1000), 5000), 1)
                    connection.exec_driver_sql(
                        f"SET LOCAL lock_timeout = '{lock_ms}ms'"
                    )
                    connection.exec_driver_sql(
                        "LOCK TABLE sequence, evidence_claim, artifact, evidence "
                        "IN ACCESS EXCLUSIVE MODE"
                    )
                    _remaining(deadline)
                    _run_maintenance_upgrade(connection, acknowledgement, deadline)
                    break
                except DBAPIError as error:
                    connection.rollback()
                    sqlstate = getattr(error.orig, "sqlstate", None) or getattr(
                        error.orig, "pgcode", None
                    )
                    if sqlstate not in {"40P01", "55P03"}:
                        raise
                    time.sleep(min(random.uniform(1.0, 5.0), _remaining(deadline)))
        except BaseException as error:
            primary = error
        finally:
            try:
                connection.rollback()
                _reset_postgres_transaction_timeout(connection)
            except BaseException as error:
                cleanup = error
                connection.invalidate()
            if acquired and not connection.invalidated:
                try:
                    connection.exec_driver_sql(
                        "SELECT pg_advisory_unlock(%s)", (_POSTGRES_MIGRATION_LOCK_ID,)
                    )
                    connection.commit()
                except BaseException as error:
                    cleanup = cleanup or error
                    connection.invalidate()
        if primary is not None:
            if cleanup is not None:
                primary.add_note(f"maintenance connection cleanup failed: {cleanup!r}")
            raise primary
        if cleanup is not None:
            raise cleanup


def _arm_postgres_transaction_timeout(connection: Connection, deadline: float) -> None:
    """Arm the remaining whole-transaction budget across an autocommit Sync."""

    remaining_ms = max(int(_remaining(deadline) * 1000), 1)
    autocommit = connection.execution_options(isolation_level="AUTOCOMMIT")
    autocommit.exec_driver_sql(f"SET transaction_timeout = '{remaining_ms}ms'")
    configured = str(
        autocommit.exec_driver_sql("SHOW transaction_timeout").scalar_one()
    )
    if configured not in {f"{remaining_ms}ms", f"{remaining_ms / 1000:g}s"}:
        raise RuntimeError(
            "transaction_timeout setup readback did not match remaining budget"
        )
    _remaining(deadline)


def _reset_postgres_transaction_timeout(connection: Connection) -> None:
    """Reset and read back session state before a pooled connection can return."""

    autocommit = connection.execution_options(isolation_level="AUTOCOMMIT")
    autocommit.exec_driver_sql("RESET transaction_timeout")
    configured = autocommit.exec_driver_sql("SHOW transaction_timeout").scalar_one()
    if str(configured) not in {"0", "0ms"}:
        raise RuntimeError("transaction_timeout reset readback failed")
