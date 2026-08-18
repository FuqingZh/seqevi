"""Alembic migration entrypoint for the embedded local schema."""

from __future__ import annotations

import fcntl
import json
import os
import random
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from ipaddress import ip_address
from pathlib import Path
from typing import Any, Callable, Iterator, cast

from alembic import command
from alembic.config import Config
from sqlalchemy import event, inspect, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError

_POSTGRES_MIGRATION_LOCK_ID = 0x534551455649
_MAINTENANCE_TIMEOUT_SECONDS = 60.0
_MAINTENANCE_READBACK_TIMEOUT_SECONDS = 60.0
_CURRENT_REVISION = "0004_claim_sessions"
_AUTOMATIC_EXISTING_CEILING = "0003_evidence_claim_leases"
_PREPARATION_SOURCE_REVISION = "0002_artifact_byte_size_bigint"
_CLAIM_SESSION_TABLES = {
    "claim_sessions",
    "session_claims",
    "evidence_claim_generations",
    "claim_session_open_receipts",
    "claim_session_acquire_receipts",
    "claim_session_acquire_receipt_items",
}
_POSTGRES_ACQUISITION_LOCK = threading.Lock()
_RESOLVER_STOP_GRACE_SECONDS = 0.2
_ACQUISITION_CANCEL_TIMEOUT_SECONDS = 0.05
_ACQUISITION_CANCEL_INTERVAL_SECONDS = 0.075
_ACQUISITION_WATCHDOG_JOIN_SECONDS = 0.15


class _AmbiguousMaintenanceCommit(RuntimeError):
    pass


class _MaintenanceWatchdog:
    def __init__(self, connection: Connection, deadline: float) -> None:
        self.connection = connection
        self.expired = threading.Event()
        if connection.invalidated:
            raise RuntimeError(
                "PostgreSQL maintenance cannot arm a watchdog for an "
                "invalidated connection"
            )
        dbapi_connection = cast(Any, connection)._dbapi_connection
        if dbapi_connection is None:
            raise RuntimeError(
                "PostgreSQL maintenance cannot arm a watchdog without an "
                "existing DBAPI connection"
            )
        raw = dbapi_connection.driver_connection

        def cancel_stalled_operation() -> None:
            self.expired.set()
            try:
                raw.cancel()
            except BaseException:
                connection.invalidate()

        self.timer = threading.Timer(_remaining(deadline), cancel_stalled_operation)
        self.timer.daemon = True
        self.timer.start()

    def require_precommit_budget(self) -> None:
        if self.expired.is_set():
            self.connection.invalidate()
            raise TimeoutError("ClaimSession maintenance exceeded its deadline")

    def commit(self) -> None:
        self.require_precommit_budget()
        try:
            self.connection.commit()
        except DBAPIError as error:
            raise _AmbiguousMaintenanceCommit from error
        finally:
            self.timer.cancel()
        if self.expired.is_set():
            self.connection.invalidate()
            raise _AmbiguousMaintenanceCommit(
                "maintenance commit exceeded its client deadline"
            )

    def cancel(self) -> None:
        self.timer.cancel()


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


def _database_identity(engine: Engine, store_root: Path | None = None) -> str:
    if engine.dialect.name != "sqlite":
        return engine.url.render_as_string(hide_password=True)
    database_path = _canonical_sqlite_database_path(engine, store_root)
    return engine.url.set(database=str(database_path)).render_as_string(
        hide_password=True
    )


def _canonical_sqlite_database_path(engine: Engine, store_root: Path | None) -> Path:
    database = engine.url.database
    if not database or database == ":memory:" or engine.url.query:
        raise RuntimeError("SQLite maintenance requires one unambiguous file database")
    lexical_database = Path(database).expanduser()
    if lexical_database.is_symlink():
        raise RuntimeError("SQLite maintenance refuses a symlink database target")
    try:
        database_path = lexical_database.resolve(strict=True)
    except FileNotFoundError as error:
        raise RuntimeError("SQLite maintenance database target is missing") from error
    if not database_path.is_file():
        raise RuntimeError("SQLite maintenance target is not a regular database file")
    if store_root is not None:
        try:
            canonical_root = store_root.expanduser().resolve(strict=True)
        except FileNotFoundError as error:
            raise RuntimeError("SQLite maintenance Store root is missing") from error
        if not canonical_root.is_dir():
            raise RuntimeError("SQLite maintenance Store root is not a directory")
        expected_path = canonical_root / "store.sqlite3"
        if database_path != expected_path:
            raise RuntimeError(
                "SQLite maintenance database is not the Store database under store_root"
            )
    return database_path


def _sqlite_file_identity(path_or_fd: Path | int) -> tuple[int, int]:
    stat = os.fstat(path_or_fd) if isinstance(path_or_fd, int) else path_or_fd.stat()
    return stat.st_dev, stat.st_ino


@contextmanager
def _pinned_sqlite_database(
    path: Path, acknowledged_identity: tuple[int, int]
) -> Iterator[int]:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        descriptor_identity = _sqlite_file_identity(descriptor)
        if (
            _sqlite_file_identity(path) != descriptor_identity
            or descriptor_identity != acknowledged_identity
        ):
            raise RuntimeError("SQLite maintenance database changed while opening")
        yield descriptor
    finally:
        os.close(descriptor)


def _validate_opened_sqlite_target(
    connection: Connection, expected_path: Path, pinned_descriptor: int
) -> None:
    databases = connection.exec_driver_sql("PRAGMA database_list").all()
    main_paths = [row[2] for row in databases if row[1] == "main"]
    if len(main_paths) != 1 or not main_paths[0]:
        raise RuntimeError("SQLite maintenance opened an ambiguous database target")
    try:
        opened_path = Path(main_paths[0]).resolve(strict=True)
        opened_identity = _sqlite_file_identity(opened_path)
        pinned_identity = _sqlite_file_identity(pinned_descriptor)
    except (FileNotFoundError, OSError) as error:
        raise RuntimeError(
            "SQLite maintenance opened target cannot be verified"
        ) from error
    if opened_path != expected_path or opened_identity != pinned_identity:
        raise RuntimeError("SQLite maintenance database target changed while fenced")


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


def maintenance_prepare_database(
    engine: Engine,
    store_root: Path | None,
    acknowledgement: MaintenanceAcknowledgement,
    *,
    rollback: bool = False,
) -> None:
    """Move an acknowledged existing Store between 0002 and 0003 only."""

    deadline = time.monotonic() + _MAINTENANCE_TIMEOUT_SECONDS
    source = _AUTOMATIC_EXISTING_CEILING if rollback else _PREPARATION_SOURCE_REVISION
    target = _PREPARATION_SOURCE_REVISION if rollback else _AUTOMATIC_EXISTING_CEILING
    if acknowledgement.expected_revision != source:
        raise RuntimeError("preparation acknowledgement has a stale revision")
    if engine.dialect.name == "sqlite":
        if store_root is None:
            raise ValueError("SQLite maintenance requires the Store root")
        database_path = _canonical_sqlite_database_path(engine, store_root)
        identity = engine.url.set(database=str(database_path)).render_as_string(
            hide_password=True
        )
        if acknowledgement.database_identity != identity:
            raise RuntimeError("maintenance acknowledgement targets another database")
        acknowledged_identity = _sqlite_file_identity(database_path)
        engine.dispose()
        with _pinned_sqlite_database(
            database_path, acknowledged_identity
        ) as pinned_descriptor:
            try:
                with (
                    _bounded_file_lock(store_root, deadline),
                    engine.connect() as connection,
                ):
                    _validate_opened_sqlite_target(
                        connection, database_path, pinned_descriptor
                    )
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
                        _run_preparation_transition(
                            connection, source, target, deadline
                        )
                        _validate_opened_sqlite_target(
                            connection, database_path, pinned_descriptor
                        )
                    finally:
                        raw.set_progress_handler(None, 0)
            except _AmbiguousMaintenanceCommit as error:
                _classify_ambiguous_transition(
                    engine,
                    source,
                    target,
                    _readback_deadline(),
                    error,
                    sqlite_binding=(database_path, pinned_descriptor),
                )
                return
            _verify_maintenance_completion(
                engine,
                target,
                _readback_deadline(),
                sqlite_binding=(database_path, pinned_descriptor),
            )
        return
    if acknowledgement.database_identity != _database_identity(engine, store_root):
        raise RuntimeError("maintenance acknowledgement targets another database")
    try:
        _maintenance_prepare_postgres(engine, source, target, deadline)
    except _AmbiguousMaintenanceCommit as error:
        _classify_ambiguous_transition(
            engine, source, target, _readback_deadline(), error
        )
        return
    _verify_maintenance_completion(engine, target, _readback_deadline())


def _run_preparation_transition(
    connection: Connection,
    source: str,
    target: str,
    deadline: float,
    commit: Callable[[], None] | None = None,
) -> None:
    if _revision(connection) != source:
        connection.rollback()
        raise RuntimeError("preparation acknowledgement has a stale revision")
    try:
        if (
            source == _AUTOMATIC_EXISTING_CEILING
            and target == _PREPARATION_SOURCE_REVISION
        ):
            clock = (
                "clock_timestamp()"
                if connection.dialect.name == "postgresql"
                else "CURRENT_TIMESTAMP"
            )
            live_claims = connection.execute(
                text(  # noqa: S608
                    f"SELECT count(*) FROM evidence_claim WHERE expires_at > {clock}"
                )
            ).scalar_one()
            if live_claims:
                connection.rollback()
                raise RuntimeError(
                    "maintenance preparation rollback refuses unexpired evidence claims"
                )
        _remaining(deadline)
        if target == _AUTOMATIC_EXISTING_CEILING:
            command.upgrade(_configure(connection), target)
        else:
            command.downgrade(_configure(connection), target)
        _remaining(deadline)
        if commit is None:
            try:
                connection.commit()
            except DBAPIError as error:
                raise _AmbiguousMaintenanceCommit from error
        else:
            commit()
    except _AmbiguousMaintenanceCommit:
        raise
    except Exception:
        connection.rollback()
        raise


def _maintenance_prepare_postgres(
    engine: Engine, source: str, target: str, deadline: float
) -> None:
    with _bounded_postgres_connect(engine, deadline) as connection:
        acquired = False
        primary: BaseException | None = None
        cleanup: BaseException | None = None
        try:
            while not acquired:
                lock_watchdog = _MaintenanceWatchdog(connection, deadline)
                try:
                    acquired = bool(
                        connection.exec_driver_sql(
                            "SELECT pg_try_advisory_lock(%s)",
                            (_POSTGRES_MIGRATION_LOCK_ID,),
                        ).scalar_one()
                    )
                    connection.commit()
                except BaseException:
                    connection.invalidate()
                    raise
                finally:
                    lock_watchdog.cancel()
                if lock_watchdog.expired.is_set():
                    connection.invalidate()
                    raise TimeoutError(
                        "ClaimSession maintenance advisory lock exceeded deadline"
                    )
                if not acquired:
                    time.sleep(min(random.uniform(1.0, 5.0), _remaining(deadline)))
            while True:
                watchdog: _MaintenanceWatchdog | None = None
                try:
                    watchdog = _arm_postgres_transaction_timeout(connection, deadline)
                    connection.exec_driver_sql("BEGIN")
                    lock_ms = max(min(int(_remaining(deadline) * 1000), 5000), 1)
                    connection.exec_driver_sql(
                        f"SET LOCAL lock_timeout = '{lock_ms}ms'"
                    )
                    tables = "sequence, artifact, evidence"
                    if source == _AUTOMATIC_EXISTING_CEILING:
                        tables += ", evidence_claim"
                    connection.exec_driver_sql(
                        f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE"
                    )
                    _run_preparation_transition(
                        connection, source, target, deadline, watchdog.commit
                    )
                    break
                except DBAPIError as error:
                    connection.rollback()
                    sqlstate = getattr(error.orig, "sqlstate", None) or getattr(
                        error.orig, "pgcode", None
                    )
                    if sqlstate not in {"40P01", "55P03"}:
                        raise
                    time.sleep(min(random.uniform(1.0, 5.0), _remaining(deadline)))
                finally:
                    if watchdog is not None:
                        watchdog.cancel()
        except BaseException as error:
            primary = error
        finally:
            try:
                _cleanup_postgres_maintenance(connection, acquired, deadline)
            except BaseException as error:
                cleanup = error
                connection.invalidate()
        if primary is not None:
            if cleanup is not None:
                primary.add_note(f"maintenance connection cleanup failed: {cleanup!r}")
            raise primary
        if cleanup is not None:
            raise cleanup


def maintenance_upgrade_database(
    engine: Engine,
    store_root: Path | None,
    acknowledgement: MaintenanceAcknowledgement,
) -> None:
    """Apply 0004 under a bounded persistence fence after operator quiescence."""

    deadline = time.monotonic() + _MAINTENANCE_TIMEOUT_SECONDS
    if engine.dialect.name == "sqlite":
        if store_root is None:
            raise ValueError("SQLite maintenance requires the Store root")
        database_path = _canonical_sqlite_database_path(engine, store_root)
        if acknowledgement.database_identity != engine.url.set(
            database=str(database_path)
        ).render_as_string(hide_password=True):
            raise RuntimeError("maintenance acknowledgement targets another database")
        acknowledged_identity = _sqlite_file_identity(database_path)
        engine.dispose()
        with _pinned_sqlite_database(
            database_path, acknowledged_identity
        ) as pinned_descriptor:
            try:
                with (
                    _bounded_file_lock(store_root, deadline),
                    engine.connect() as connection,
                ):
                    _validate_opened_sqlite_target(
                        connection, database_path, pinned_descriptor
                    )
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
                        _validate_opened_sqlite_target(
                            connection, database_path, pinned_descriptor
                        )
                    finally:
                        raw.set_progress_handler(None, 0)
            except _AmbiguousMaintenanceCommit as error:
                _classify_ambiguous_maintenance(
                    engine,
                    _CURRENT_REVISION,
                    _readback_deadline(),
                    error,
                    sqlite_binding=(database_path, pinned_descriptor),
                )
                return
            _verify_maintenance_completion(
                engine,
                _CURRENT_REVISION,
                _readback_deadline(),
                sqlite_binding=(database_path, pinned_descriptor),
            )
        return
    if acknowledgement.database_identity != _database_identity(engine, store_root):
        raise RuntimeError("maintenance acknowledgement targets another database")
    try:
        _maintenance_upgrade_postgres(engine, acknowledgement, deadline)
    except _AmbiguousMaintenanceCommit as error:
        _classify_ambiguous_maintenance(
            engine, _CURRENT_REVISION, _readback_deadline(), error
        )
        return
    _verify_maintenance_completion(engine, _CURRENT_REVISION, _readback_deadline())


def maintenance_downgrade_database(
    engine: Engine,
    store_root: Path | None,
    acknowledgement: MaintenanceAcknowledgement,
) -> None:
    """Downgrade 0004 to empty 0003 coordination under the same bounded fence."""

    deadline = time.monotonic() + _MAINTENANCE_TIMEOUT_SECONDS
    if engine.dialect.name == "sqlite":
        if store_root is None:
            raise ValueError("SQLite maintenance requires the Store root")
        database_path = _canonical_sqlite_database_path(engine, store_root)
        if acknowledgement.database_identity != engine.url.set(
            database=str(database_path)
        ).render_as_string(hide_password=True):
            raise RuntimeError("maintenance acknowledgement targets another database")
        acknowledged_identity = _sqlite_file_identity(database_path)
        engine.dispose()
        with _pinned_sqlite_database(
            database_path, acknowledged_identity
        ) as pinned_descriptor:
            try:
                with (
                    _bounded_file_lock(store_root, deadline),
                    engine.connect() as connection,
                ):
                    _validate_opened_sqlite_target(
                        connection, database_path, pinned_descriptor
                    )
                    connection.exec_driver_sql(
                        f"PRAGMA busy_timeout={max(int(_remaining(deadline) * 1000), 1)}"
                    )
                    raw = cast(Any, connection.connection.driver_connection)
                    raw.set_progress_handler(
                        lambda: 1 if time.monotonic() >= deadline else 0, 1000
                    )
                    try:
                        connection.exec_driver_sql("BEGIN EXCLUSIVE")
                        _run_maintenance_downgrade(
                            connection, acknowledgement, deadline
                        )
                        _validate_opened_sqlite_target(
                            connection, database_path, pinned_descriptor
                        )
                    finally:
                        raw.set_progress_handler(None, 0)
            except _AmbiguousMaintenanceCommit as error:
                _classify_ambiguous_maintenance(
                    engine,
                    _AUTOMATIC_EXISTING_CEILING,
                    _readback_deadline(),
                    error,
                    sqlite_binding=(database_path, pinned_descriptor),
                )
                return
            _verify_maintenance_completion(
                engine,
                _AUTOMATIC_EXISTING_CEILING,
                _readback_deadline(),
                sqlite_binding=(database_path, pinned_descriptor),
            )
        return
    if acknowledgement.database_identity != _database_identity(engine, store_root):
        raise RuntimeError("maintenance acknowledgement targets another database")
    try:
        _maintenance_upgrade_postgres(engine, acknowledgement, deadline, downgrade=True)
    except _AmbiguousMaintenanceCommit as error:
        _classify_ambiguous_maintenance(
            engine,
            _AUTOMATIC_EXISTING_CEILING,
            _readback_deadline(),
            error,
        )
        return
    _verify_maintenance_completion(
        engine, _AUTOMATIC_EXISTING_CEILING, _readback_deadline()
    )


def _readback_deadline() -> float:
    return time.monotonic() + _MAINTENANCE_READBACK_TIMEOUT_SECONDS


def _maintenance_state(
    engine: Engine,
    deadline: float,
    *,
    sqlite_binding: tuple[Path, int] | None = None,
) -> tuple[str | None, set[str]]:
    if engine.dialect.name == "sqlite":
        engine.dispose()
    connection_context = (
        engine.connect()
        if engine.dialect.name == "sqlite"
        else _bounded_postgres_connect(engine, deadline)
    )
    with connection_context as connection:
        if engine.dialect.name == "sqlite":
            if sqlite_binding is None:
                raise RuntimeError("SQLite maintenance readback requires file binding")
            _validate_opened_sqlite_target(connection, *sqlite_binding)
            remaining = _remaining(deadline)
            connection.exec_driver_sql(
                f"PRAGMA busy_timeout={max(int(remaining * 1000), 1)}"
            )
            raw = cast(Any, connection.connection.driver_connection)
            raw.set_progress_handler(
                lambda: 1 if time.monotonic() >= deadline else 0, 1000
            )
            try:
                state = (
                    _revision(connection),
                    set(inspect(connection).get_table_names()),
                )
                _validate_opened_sqlite_target(connection, *sqlite_binding)
                _remaining(deadline)
                return state
            finally:
                raw.set_progress_handler(None, 0)
        watchdog = _arm_postgres_transaction_timeout(connection, deadline)
        try:
            connection.exec_driver_sql("BEGIN")
            state = _revision(connection), set(inspect(connection).get_table_names())
            watchdog.require_precommit_budget()
            return state
        except BaseException:
            if watchdog.expired.is_set():
                connection.invalidate()
            raise
        finally:
            try:
                if not connection.invalidated:
                    try:
                        connection.rollback()
                        _reset_postgres_transaction_timeout(connection)
                        watchdog.require_precommit_budget()
                    except BaseException:
                        connection.invalidate()
                        raise
            finally:
                watchdog.cancel()


def _state_matches_target(revision: str | None, tables: set[str], target: str) -> bool:
    if revision != target:
        return False
    if target == _CURRENT_REVISION:
        return _CLAIM_SESSION_TABLES <= tables and "evidence_claim" not in tables
    if target == _PREPARATION_SOURCE_REVISION:
        return "evidence_claim" not in tables and not (_CLAIM_SESSION_TABLES & tables)
    return "evidence_claim" in tables and not (_CLAIM_SESSION_TABLES & tables)


def _verify_maintenance_completion(
    engine: Engine,
    target: str,
    deadline: float,
    *,
    sqlite_binding: tuple[Path, int] | None = None,
) -> None:
    revision, tables = _maintenance_state(
        engine, deadline, sqlite_binding=sqlite_binding
    )
    if not _state_matches_target(revision, tables, target):
        raise RuntimeError("maintenance completion readback found mixed schema state")


def _classify_ambiguous_maintenance(
    engine: Engine,
    target: str,
    deadline: float,
    error: BaseException,
    *,
    sqlite_binding: tuple[Path, int] | None = None,
) -> None:
    revision, tables = _maintenance_state(
        engine, deadline, sqlite_binding=sqlite_binding
    )
    if _state_matches_target(revision, tables, target):
        return
    source = (
        _AUTOMATIC_EXISTING_CEILING
        if target == _CURRENT_REVISION
        else _CURRENT_REVISION
    )
    if _state_matches_target(revision, tables, source):
        raise RuntimeError(
            "maintenance commit did not change the acknowledged source schema"
        ) from error
    raise RuntimeError("maintenance commit outcome left mixed schema state") from error


def _classify_ambiguous_transition(
    engine: Engine,
    source: str,
    target: str,
    deadline: float,
    error: BaseException,
    *,
    sqlite_binding: tuple[Path, int] | None = None,
) -> None:
    revision, tables = _maintenance_state(
        engine, deadline, sqlite_binding=sqlite_binding
    )
    if _state_matches_target(revision, tables, target):
        return
    if _state_matches_target(revision, tables, source):
        raise RuntimeError(
            "maintenance commit did not change the acknowledged source schema"
        ) from error
    raise RuntimeError("maintenance commit outcome left mixed schema state") from error


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
    commit: Callable[[], None] | None = None,
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
    if commit is not None:
        commit()
    else:
        try:
            connection.commit()
        except DBAPIError as error:
            raise _AmbiguousMaintenanceCommit from error


def _run_maintenance_downgrade(
    connection: Connection,
    acknowledgement: MaintenanceAcknowledgement,
    deadline: float,
    commit: Callable[[], None] | None = None,
) -> None:
    revision = _revision(connection)
    if revision != acknowledgement.expected_revision or revision != _CURRENT_REVISION:
        connection.rollback()
        raise RuntimeError("maintenance downgrade requires acknowledged revision 0004")
    _remaining(deadline)
    command.downgrade(_configure(connection), _AUTOMATIC_EXISTING_CEILING)
    _remaining(deadline)
    if commit is not None:
        commit()
    else:
        try:
            connection.commit()
        except DBAPIError as error:
            raise _AmbiguousMaintenanceCommit from error


def _maintenance_upgrade_postgres(
    engine: Engine,
    acknowledgement: MaintenanceAcknowledgement,
    deadline: float,
    *,
    downgrade: bool = False,
) -> None:
    with _bounded_postgres_connect(engine, deadline) as connection:
        acquired = False
        primary: BaseException | None = None
        cleanup: BaseException | None = None
        try:
            while not acquired:
                lock_watchdog = _MaintenanceWatchdog(connection, deadline)
                try:
                    acquired = bool(
                        connection.exec_driver_sql(
                            "SELECT pg_try_advisory_lock(%s)",
                            (_POSTGRES_MIGRATION_LOCK_ID,),
                        ).scalar_one()
                    )
                    connection.commit()
                except BaseException:
                    connection.invalidate()
                    raise
                finally:
                    lock_watchdog.cancel()
                if lock_watchdog.expired.is_set():
                    connection.invalidate()
                    raise TimeoutError(
                        "ClaimSession maintenance advisory lock exceeded deadline"
                    )
                if not acquired:
                    time.sleep(min(random.uniform(1.0, 5.0), _remaining(deadline)))
            while True:
                watchdog: _MaintenanceWatchdog | None = None
                try:
                    watchdog = _arm_postgres_transaction_timeout(connection, deadline)
                    connection.exec_driver_sql("BEGIN")
                    lock_ms = max(min(int(_remaining(deadline) * 1000), 5000), 1)
                    connection.exec_driver_sql(
                        f"SET LOCAL lock_timeout = '{lock_ms}ms'"
                    )
                    lock_tables = (
                        "LOCK TABLE claim_session_open_receipts, claim_sessions, "
                        "evidence_claim_generations, session_claims, "
                        "claim_session_acquire_receipt_items, "
                        "claim_session_acquire_receipts, sequence, artifact, evidence "
                        if downgrade
                        else "LOCK TABLE sequence, evidence_claim, artifact, evidence "
                    )
                    connection.exec_driver_sql(lock_tables + "IN ACCESS EXCLUSIVE MODE")
                    _remaining(deadline)
                    if downgrade:
                        _run_maintenance_downgrade(
                            connection,
                            acknowledgement,
                            deadline,
                            watchdog.commit,
                        )
                    else:
                        _run_maintenance_upgrade(
                            connection,
                            acknowledgement,
                            deadline,
                            watchdog.commit,
                        )
                    break
                except DBAPIError as error:
                    connection.rollback()
                    sqlstate = getattr(error.orig, "sqlstate", None) or getattr(
                        error.orig, "pgcode", None
                    )
                    if sqlstate not in {"40P01", "55P03"}:
                        raise
                    time.sleep(min(random.uniform(1.0, 5.0), _remaining(deadline)))
                finally:
                    if watchdog is not None:
                        watchdog.cancel()
        except BaseException as error:
            primary = error
        finally:
            try:
                _cleanup_postgres_maintenance(connection, acquired, deadline)
            except BaseException as error:
                cleanup = error
                connection.invalidate()
        if primary is not None:
            if cleanup is not None:
                primary.add_note(f"maintenance connection cleanup failed: {cleanup!r}")
            raise primary
        if cleanup is not None:
            raise cleanup


@contextmanager
def _bounded_postgres_connect(engine: Engine, deadline: float) -> Iterator[Connection]:
    """Acquire one pooled PostgreSQL connection inside an absolute deadline."""

    from psycopg import capabilities

    if (
        engine.pool.dispatch.checkout.listeners
        or engine.dialect.dispatch.do_connect.listeners
    ):
        raise RuntimeError(
            "PostgreSQL maintenance requires a fresh Engine without instance "
            "checkout or do_connect listeners"
        )
    if not capabilities.has_cancel_safe():
        raise RuntimeError(
            "PostgreSQL maintenance requires libpq 17 bounded cancellation"
        )
    if not _POSTGRES_ACQUISITION_LOCK.acquire(timeout=_remaining(deadline)):
        raise TimeoutError(
            "ClaimSession maintenance connection serialization exceeded deadline"
        )
    try:
        pool = cast(Any, engine.pool)
        if not hasattr(pool, "_timeout"):
            raise RuntimeError(
                "PostgreSQL maintenance requires a timeout-capable connection pool"
            )
        original_pool_timeout = pool._timeout
        pre_ping_was_own_attribute = "_do_ping_w_event" in vars(engine.dialect)
        original_own_pre_ping = vars(engine.dialect).get("_do_ping_w_event")
        original_pre_ping = engine.dialect._do_ping_w_event
        listener_active = True
        acquisition_raw: Any | None = None
        acquisition_expired = threading.Event()
        acquisition_finished = threading.Event()
        acquisition_state_lock = threading.Lock()

        def cancel_raw(raw: Any) -> None:
            cancel_safe = getattr(raw, "cancel_safe", None)
            if not callable(cancel_safe):
                return
            try:
                cancel_safe(timeout=_ACQUISITION_CANCEL_TIMEOUT_SECONDS)
            except BaseException:
                pass

        def cancel_checkout_initialization() -> None:
            with acquisition_state_lock:
                acquisition_expired.set()
            while not acquisition_finished.is_set():
                with acquisition_state_lock:
                    raw = acquisition_raw
                if raw is not None:
                    cancel_raw(raw)
                acquisition_finished.wait(_ACQUISITION_CANCEL_INTERVAL_SECONDS)

        def publish_pooled_raw(raw: Any) -> bool:
            nonlocal acquisition_raw
            if not listener_active:
                return False
            if not callable(getattr(raw, "cancel_safe", None)):
                raise RuntimeError(
                    "PostgreSQL maintenance requires bounded psycopg cancellation"
                )
            with acquisition_state_lock:
                if acquisition_expired.is_set():
                    raise TimeoutError(
                        "ClaimSession maintenance connection initialization "
                        "exceeded deadline"
                    )
                acquisition_raw = raw
            return True

        def bounded_pre_ping(dbapi_connection: Any) -> bool:
            if not publish_pooled_raw(dbapi_connection):
                return original_pre_ping(dbapi_connection)
            return original_pre_ping(dbapi_connection)

        def capture_before_checkout_hooks(
            raw: Any, _connection_record: Any, _connection_proxy: Any
        ) -> None:
            publish_pooled_raw(raw)

        def quiesce_acquisition_timer(timer: threading.Timer) -> None:
            timer.cancel()
            acquisition_finished.set()
            timer.join(_ACQUISITION_WATCHDOG_JOIN_SECONDS)
            if timer.is_alive():
                raise RuntimeError(
                    "PostgreSQL maintenance acquisition watchdog failed to stop"
                )

        def bounded_physical_connect(
            dialect: Any,
            _connection_record: Any,
            cargs: list[Any],
            cparams: dict[str, Any],
        ) -> Any:
            if not listener_active:
                return None
            if any(str(arg) for arg in cargs) or cparams.get("conninfo"):
                raise RuntimeError(
                    "PostgreSQL maintenance rejects embedded conninfo targets"
                )
            bounded_cparams, attempts = _resolve_postgres_connect_targets(
                cparams, deadline
            )
            with acquisition_state_lock:
                if acquisition_expired.is_set():
                    raise TimeoutError(
                        "ClaimSession maintenance connection initialization "
                        "exceeded deadline"
                    )
                connect_timeout = int(_remaining(deadline)) // attempts
                if connect_timeout < 2:
                    raise TimeoutError(
                        "ClaimSession maintenance has insufficient "
                        "physical-connect budget"
                    )
                configured = bounded_cparams.get("connect_timeout")
                configured_timeout = (
                    _parse_postgres_connect_timeout(configured)
                    if configured is not None
                    else 0
                )
                bounded_cparams["connect_timeout"] = (
                    min(configured_timeout, connect_timeout)
                    if configured_timeout > 0
                    else connect_timeout
                )
            if acquisition_expired.is_set():
                raise TimeoutError(
                    "ClaimSession maintenance connection initialization "
                    "exceeded deadline"
                )
            raw = dialect.connect(*cargs, **bounded_cparams)
            if not callable(getattr(raw, "cancel_safe", None)):
                raw.close()
                raise RuntimeError(
                    "PostgreSQL maintenance requires bounded psycopg cancellation"
                )
            nonlocal acquisition_raw
            with acquisition_state_lock:
                if acquisition_expired.is_set():
                    late = True
                else:
                    acquisition_raw = raw
                    late = False
            if late:
                raw.close()
                raise TimeoutError(
                    "ClaimSession maintenance connection initialization "
                    "exceeded deadline"
                )
            return raw

        connection: Connection | None = None
        connect_listener_registered = False
        checkout_listener_registered = False
        acquisition_timer = threading.Timer(
            _remaining(deadline), cancel_checkout_initialization
        )
        acquisition_timer.name = "seqevi-postgres-acquisition-watchdog"
        acquisition_timer.daemon = True
        acquisition_timer.start()
        try:
            try:
                engine.dialect._do_ping_w_event = bounded_pre_ping
                event.listen(
                    pool,
                    "checkout",
                    capture_before_checkout_hooks,
                    insert=True,
                )
                checkout_listener_registered = True
                event.listen(
                    engine,
                    "do_connect",
                    bounded_physical_connect,
                )
                connect_listener_registered = True
                pool._timeout = min(float(original_pool_timeout), _remaining(deadline))
                connection = engine.connect()
            finally:
                try:
                    listener_active = False
                    if checkout_listener_registered:
                        event.remove(pool, "checkout", capture_before_checkout_hooks)
                finally:
                    try:
                        if connect_listener_registered:
                            event.remove(engine, "do_connect", bounded_physical_connect)
                    finally:
                        if pre_ping_was_own_attribute:
                            setattr(
                                engine.dialect,
                                "_do_ping_w_event",
                                original_own_pre_ping,
                            )
                        else:
                            del engine.dialect._do_ping_w_event
                        pool._timeout = original_pool_timeout
        except BaseException as error:
            try:
                quiesce_acquisition_timer(acquisition_timer)
            except BaseException as cleanup:
                error.add_note(f"maintenance watchdog cleanup failed: {cleanup!r}")
            if connection is not None:
                _discard_postgres_connection(connection, error)
            if acquisition_expired.is_set() and not isinstance(error, TimeoutError):
                raise TimeoutError(
                    "ClaimSession maintenance connection initialization "
                    "exceeded deadline"
                ) from error
            raise
        else:
            try:
                quiesce_acquisition_timer(acquisition_timer)
            except BaseException as error:
                assert connection is not None
                _discard_postgres_connection(connection, error)
                raise
        assert connection is not None
        if acquisition_expired.is_set():
            timeout = TimeoutError(
                "ClaimSession maintenance connection initialization exceeded deadline"
            )
            _discard_postgres_connection(connection, timeout)
            raise timeout
        try:
            _remaining(deadline)
        except BaseException as error:
            _discard_postgres_connection(connection, error)
            raise
    finally:
        _POSTGRES_ACQUISITION_LOCK.release()
    try:
        yield connection
    finally:
        connection.close()


def _postgres_resolver_command(host: str, port: int) -> tuple[str, ...]:
    return (
        sys.executable,
        "-I",
        "-m",
        "seqevi.store._resolver",
        host,
        str(port),
        str(socket.AF_UNSPEC),
        str(socket.SOCK_STREAM),
        "0",
        "0",
    )


def _resolve_postgres_connect_targets(
    cparams: dict[str, Any], deadline: float
) -> tuple[dict[str, Any], int]:
    """Resolve copied libpq targets so DNS and all attempts share one deadline."""

    bounded = _effective_postgres_connect_params(cparams)
    raw_host = bounded.get("host")
    raw_hostaddr = bounded.get("hostaddr")
    if raw_host is None and raw_hostaddr is None:
        attempts = 2 if bounded.get("target_session_attrs") == "prefer-standby" else 1
        return bounded, attempts
    hosts = str(raw_host or "").split(",")
    hostaddrs = str(raw_hostaddr or "").split(",")
    ports = str(bounded.get("port", "")).split(",")
    if any(host.startswith("@") for host in hosts):
        raise RuntimeError(
            "PostgreSQL maintenance does not support abstract Unix socket targets"
        )
    if any(hosts) and any(hostaddrs) and len(hosts) != len(hostaddrs):
        raise ValueError(
            "PostgreSQL explicit host and hostaddr lists must have equal lengths"
        )
    target_count = max(len(hosts), len(hostaddrs))
    hosts = _align_postgres_connect_values(hosts, target_count, "host")
    hostaddrs = _align_postgres_connect_values(hostaddrs, target_count, "hostaddr")
    ports = _align_postgres_connect_values(ports, target_count, "port")
    resolved_hosts: list[str] = []
    resolved_hostaddrs: list[str] = []
    resolved_ports: list[str] = []
    for host, hostaddr, raw_port in zip(hosts, hostaddrs, ports, strict=True):
        if hostaddr or not host or host.startswith("/"):
            resolved_hosts.append(host)
            resolved_hostaddrs.append(hostaddr)
            resolved_ports.append(raw_port)
            continue
        try:
            ip_address(host)
        except ValueError:
            try:
                addresses = _resolve_postgres_host(
                    host,
                    int(raw_port) if raw_port else _postgres_default_port(),
                    deadline,
                )
            except TimeoutError:
                raise
            except OSError:
                _remaining(deadline)
                continue
        else:
            addresses = (host,)
        for address in addresses:
            resolved_hosts.append(host)
            resolved_hostaddrs.append(address)
            resolved_ports.append(raw_port)
    if not resolved_hosts:
        raise OSError("PostgreSQL resolver returned no usable connection targets")
    bounded["host"] = ",".join(resolved_hosts)
    bounded["hostaddr"] = ",".join(resolved_hostaddrs)
    bounded["port"] = ",".join(resolved_ports)
    attempts = len(resolved_hosts)
    if bounded.get("target_session_attrs") == "prefer-standby":
        attempts *= 2
    return bounded, attempts


def _effective_postgres_connect_params(cparams: dict[str, Any]) -> dict[str, Any]:
    """Freeze psycopg/libpq environment-derived attempt parameters."""

    bounded = dict(cparams)
    service = bounded.get("service") or os.environ.get("PGSERVICE")
    if service:
        raise RuntimeError(
            "PostgreSQL maintenance cannot bound service-derived connection targets"
        )
    for key, envvar in (
        ("host", "PGHOST"),
        ("hostaddr", "PGHOSTADDR"),
        ("port", "PGPORT"),
        ("target_session_attrs", "PGTARGETSESSIONATTRS"),
        ("connect_timeout", "PGCONNECT_TIMEOUT"),
    ):
        if bounded.get(key) in (None, ""):
            bounded.pop(key, None)
            if (value := os.environ.get(envvar)) not in (None, ""):
                bounded[key] = value
    return bounded


def _postgres_default_port() -> int:
    """Return the default port compiled into the active client libpq."""

    from psycopg import pq

    for option in pq.Conninfo.get_defaults():
        if option.keyword == b"port" and option.compiled is not None:
            return int(option.compiled)
    raise RuntimeError("PostgreSQL client libpq did not report a default port")


def _parse_postgres_connect_timeout(value: Any) -> int:
    """Parse a timeout with the locked Psycopg driver's conversion semantics."""

    return int(float(value))


def _align_postgres_connect_values(
    values: list[str], target_count: int, name: str
) -> list[str]:
    if len(values) == target_count:
        return values
    if len(values) == 1:
        return values * target_count
    raise ValueError(f"PostgreSQL {name} list does not align with connection targets")


def _resolve_postgres_host(host: str, port: int, deadline: float) -> tuple[str, ...]:
    process = subprocess.Popen(
        _postgres_resolver_command(host, port),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        stdout, stderr = process.communicate(timeout=_remaining(deadline))
    except subprocess.TimeoutExpired as error:
        timeout = TimeoutError(
            "ClaimSession maintenance PostgreSQL DNS resolution exceeded deadline"
        )
        try:
            _stop_postgres_resolver(process)
        except BaseException as cleanup:
            timeout.add_note(f"PostgreSQL resolver cleanup failed: {cleanup!r}")
        raise timeout from error
    except BaseException as error:
        try:
            _stop_postgres_resolver(process)
        except BaseException as cleanup:
            error.add_note(f"PostgreSQL resolver cleanup failed: {cleanup!r}")
        raise
    _remaining(deadline)
    if process.returncode != 0:
        raise OSError(stderr[:4096].decode("utf-8", errors="replace"))
    payload = json.loads(stdout)
    resolved_addresses: list[str] = []
    for item in payload:
        address = str(item[4][0])
        if (
            int(item[0]) == socket.AF_INET6
            and len(item[4]) >= 4
            and int(item[4][3]) != 0
            and "%" not in address
        ):
            address = f"{address}%{int(item[4][3])}"
        resolved_addresses.append(address)
    addresses = tuple(dict.fromkeys(resolved_addresses))
    if not addresses:
        raise OSError(f"PostgreSQL resolver returned no addresses for {host!r}")
    _remaining(deadline)
    return addresses


def _stop_postgres_resolver(process: subprocess.Popen[bytes]) -> None:
    try:
        process.terminate()
    except ProcessLookupError:
        pass
    try:
        process.communicate(timeout=_RESOLVER_STOP_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            process.kill()
        except ProcessLookupError:
            pass
        process.communicate()


def _discard_postgres_connection(
    connection: Connection, primary: BaseException
) -> None:
    """Attempt all discard steps while retaining the acquisition failure."""

    try:
        connection.invalidate()
    except BaseException as cleanup:
        primary.add_note(f"maintenance connection invalidation failed: {cleanup!r}")
    try:
        connection.close()
    except BaseException as cleanup:
        primary.add_note(f"maintenance connection close failed: {cleanup!r}")


def _cleanup_postgres_maintenance(
    connection: Connection, acquired: bool, deadline: float
) -> None:
    if connection.invalidated:
        return
    watchdog: _MaintenanceWatchdog | None = None
    try:
        watchdog = _MaintenanceWatchdog(connection, deadline)
        connection.rollback()
        watchdog.require_precommit_budget()
        if acquired:
            connection.exec_driver_sql(
                "SELECT pg_advisory_unlock(%s)", (_POSTGRES_MIGRATION_LOCK_ID,)
            )
            connection.commit()
            watchdog.require_precommit_budget()
        _reset_postgres_transaction_timeout(connection)
        watchdog.require_precommit_budget()
    except BaseException:
        connection.invalidate()
        raise
    finally:
        if watchdog is not None:
            watchdog.cancel()


def _arm_postgres_transaction_timeout(
    connection: Connection, deadline: float
) -> _MaintenanceWatchdog:
    """Arm the remaining whole-transaction budget across an autocommit Sync."""

    remaining_ms = max(int(_remaining(deadline) * 1000), 1)
    autocommit = connection.execution_options(isolation_level="AUTOCOMMIT")
    watchdog = _MaintenanceWatchdog(connection, deadline)
    try:
        autocommit.exec_driver_sql(f"SET transaction_timeout = '{remaining_ms}ms'")
        configured = str(
            autocommit.exec_driver_sql("SHOW transaction_timeout").scalar_one()
        )
    except BaseException:
        watchdog.cancel()
        raise
    if watchdog.expired.is_set():
        connection.invalidate()
        raise TimeoutError("ClaimSession maintenance timeout setup exceeded deadline")
    if configured not in {f"{remaining_ms}ms", f"{remaining_ms / 1000:g}s"}:
        watchdog.cancel()
        connection.invalidate()
        raise RuntimeError(
            "transaction_timeout setup readback did not match remaining budget"
        )
    _remaining(deadline)
    return watchdog


def _reset_postgres_transaction_timeout(connection: Connection) -> None:
    """Reset and read back session state before a pooled connection can return."""

    autocommit = connection.execution_options(isolation_level="AUTOCOMMIT")
    autocommit.exec_driver_sql("RESET transaction_timeout")
    configured = autocommit.exec_driver_sql("SHOW transaction_timeout").scalar_one()
    if str(configured) not in {"0", "0ms"}:
        raise RuntimeError("transaction_timeout reset readback failed")
