"""Alembic migration entrypoint for the embedded local schema."""

from __future__ import annotations

import fcntl
import os
import random
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
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
_CLAIM_SESSION_TABLES = {
    "claim_sessions",
    "session_claims",
    "evidence_claim_generations",
    "claim_session_open_receipts",
    "claim_session_acquire_receipts",
    "claim_session_acquire_receipt_items",
}
_POSTGRES_ACQUISITION_LOCK = threading.Lock()


class _AmbiguousMaintenanceCommit(RuntimeError):
    pass


class _MaintenanceWatchdog:
    def __init__(self, connection: Connection, deadline: float) -> None:
        self.connection = connection
        self.expired = threading.Event()
        raw = cast(Any, connection.connection.driver_connection)

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

    if not _POSTGRES_ACQUISITION_LOCK.acquire(timeout=_remaining(deadline)):
        raise TimeoutError(
            "ClaimSession maintenance connection serialization exceeded deadline"
        )
    try:
        remaining = _remaining(deadline)
        pool = cast(Any, engine.pool)
        if not hasattr(pool, "_timeout"):
            raise RuntimeError(
                "PostgreSQL maintenance requires a timeout-capable connection pool"
            )
        original_pool_timeout = pool._timeout

        def clamp_physical_connect(
            _dialect: Any,
            _connection_record: Any,
            _cargs: list[Any],
            cparams: dict[str, Any],
        ) -> None:
            connect_timeout = int(_remaining(deadline))
            if connect_timeout < 2:
                raise TimeoutError(
                    "ClaimSession maintenance has insufficient physical-connect budget"
                )
            configured = cparams.get("connect_timeout")
            cparams["connect_timeout"] = (
                min(int(configured), connect_timeout)
                if configured is not None and int(configured) > 0
                else connect_timeout
            )
            return None

        connection: Connection | None = None
        listener_registered = False
        try:
            pool._timeout = min(float(original_pool_timeout), remaining)
            try:
                event.listen(engine, "do_connect", clamp_physical_connect)
                listener_registered = True
                connection = engine.connect()
            finally:
                try:
                    if listener_registered:
                        event.remove(engine, "do_connect", clamp_physical_connect)
                finally:
                    pool._timeout = original_pool_timeout
        except BaseException as error:
            if connection is not None:
                _discard_postgres_connection(connection, error)
            raise
        assert connection is not None
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
