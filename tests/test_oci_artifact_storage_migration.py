"""SQLite coverage for acknowledged OCI artifact schema transitions."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from seqevi.store import migration as store_migration


def _run_revision(function_name: str, connection: Connection) -> None:
    revision = importlib.import_module(
        "seqevi.store.migrations.versions.0005_oci_artifact_storage"
    )
    # Alembic's revision module is intentionally context-bound.  The production
    # chain creates this proxy; the narrow test supplies the same operation API.
    revision.op._proxy = Operations(MigrationContext.configure(connection))
    try:
        getattr(revision, function_name)()
    finally:
        del revision.op._proxy


def test_oci_artifact_storage_migration_backfills_and_refuses_live_oci_rows() -> None:
    engine = create_engine("sqlite://")
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA foreign_keys=ON")
        connection.execute(
            text(
                "CREATE TABLE artifact ("
                "digest VARCHAR(64) NOT NULL PRIMARY KEY, "
                "media_type TEXT NOT NULL, byte_size BIGINT NOT NULL, "
                "relative_path TEXT NOT NULL UNIQUE, "
                "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                "CONSTRAINT ck_artifact_byte_size_nonnegative "
                "CHECK (byte_size >= 0))"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE evidence (evidence_id TEXT PRIMARY KEY, "
                "normalized_artifact_digest VARCHAR(64), "
                "FOREIGN KEY(normalized_artifact_digest) REFERENCES artifact(digest) "
                "ON DELETE RESTRICT)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO artifact (digest, media_type, byte_size, relative_path) "
                "VALUES ('posix', 'text/plain', 1, 'one.txt')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO evidence (evidence_id, normalized_artifact_digest) "
                "VALUES ('evidence', 'posix')"
            )
        )
        connection.commit()

        with store_migration._sqlite_foreign_key_rebuild_window(  # pyright: ignore[reportPrivateUsage]
            connection, enabled=True
        ):
            _run_revision("upgrade", connection)
            connection.commit()
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        assert connection.execute(
            text(
                "SELECT storage_kind, relative_path, registry_id, repository, "
                "manifest_digest FROM artifact WHERE digest = 'posix'"
            )
        ).one() == ("posix", "one.txt", None, None, None)

        connection.execute(
            text(
                "INSERT INTO artifact "
                "(digest, media_type, byte_size, relative_path, storage_kind, "
                "registry_id, repository, manifest_digest) "
                "VALUES ('oci', 'text/plain', 1, NULL, 'oci', 'test-registry', "
                "'seqevi/evidence', 'sha256:abc')"
            )
        )
        connection.commit()
        with pytest.raises(RuntimeError, match="OCI artifact rows"):
            _run_revision("downgrade", connection)

        connection.execute(text("DELETE FROM artifact WHERE storage_kind = 'oci'"))
        connection.commit()
        with store_migration._sqlite_foreign_key_rebuild_window(  # pyright: ignore[reportPrivateUsage]
            connection, enabled=True
        ):
            _run_revision("downgrade", connection)
            connection.commit()
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA foreign_key_check").all() == []
        assert (
            connection.execute(
                text(
                    "SELECT normalized_artifact_digest FROM evidence "
                    "WHERE evidence_id = 'evidence'"
                )
            ).scalar_one()
            == "posix"
        )

        names = {
            row[1] for row in connection.exec_driver_sql("PRAGMA table_info(artifact)")
        }
        assert names == {
            "digest",
            "media_type",
            "byte_size",
            "relative_path",
            "created_at",
        }
    engine.dispose()


def test_existing_0004_store_fails_closed_without_attempting_0003_upgrade(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_root = tmp_path / "store"
    store_root.mkdir()
    database_path = store_root / "store.sqlite3"
    engine = create_engine(f"sqlite:///{database_path}")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE alembic_version (version_num VARCHAR(32))")
        )
        connection.execute(
            text(
                "INSERT INTO alembic_version (version_num) "
                "VALUES ('0004_claim_sessions')"
            )
        )

    attempted_targets: list[str] = []

    def fail_if_called(*_args: object) -> None:
        attempted_targets.append("called")
        raise AssertionError("startup must not attempt an obsolete migration target")

    monkeypatch.setattr(store_migration.command, "upgrade", fail_if_called)
    with pytest.raises(RuntimeError, match="OCI artifact storage 0005"):
        store_migration.upgrade_database(engine, store_root)
    assert attempted_targets == []
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one() == ("0004_claim_sessions")
    engine.dispose()
