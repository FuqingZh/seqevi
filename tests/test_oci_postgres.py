"""Focused PostgreSQL proof for the 0005 OCI artifact-storage boundary."""

from __future__ import annotations

from pathlib import Path
from time import monotonic

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from seqevi.errors import StoreBackpressureError, StoreIntegrityError
from seqevi.evidence import CommitOutcome, StoredArtifact
from seqevi.service.persistence import PostgresEvidencePersistence
from seqevi.store import migration as store_migration
from seqevi.store.transport import CommitModel

from .test_shared_store import _hit_commit, _isolated_postgres_url, _seed_0002_evidence

pytestmark = pytest.mark.requires_postgres


def _oci_artifacts(commit: CommitModel) -> dict[str, StoredArtifact]:
    references = (commit.normalized_artifact, commit.raw_artifact)
    return {
        reference.digest: StoredArtifact(
            digest=reference.digest,
            media_type=reference.media_type,
            byte_size=reference.byte_size,
            relative_path=None,
            storage_kind="oci",
            registry_id="unit0",
            repository="seqevi/evidence",
            manifest_digest=("a" if index == 0 else "b") * 64,
        )
        for index, reference in enumerate(references)
        if reference is not None
    }


def test_postgres_oci_commit_roundtrips_metadata_and_rejects_location_conflict(
    tmp_path: Path,
) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        try:
            model = CommitModel.from_domain(_hit_commit(tmp_path / "source", "MOCIPG"))
            stored = _oci_artifacts(model)
            assert persistence.commit_many((model,), stored) == (CommitOutcome.CREATED,)
            for digest, expected in stored.items():
                assert persistence.artifact_metadata(digest) == expected

            conflicting = dict(stored)
            first_digest = next(iter(conflicting))
            first = conflicting[first_digest]
            conflicting[first_digest] = StoredArtifact(
                digest=first.digest,
                media_type=first.media_type,
                byte_size=first.byte_size,
                relative_path=None,
                storage_kind="oci",
                registry_id="unit0",
                repository="seqevi/other",
                manifest_digest=first.manifest_digest,
            )
            with pytest.raises(StoreIntegrityError, match="artifact metadata conflict"):
                persistence.commit_many((model,), conflicting)
        finally:
            persistence.close()


def test_postgres_oci_union_check_and_expired_deadline_leave_no_rows(
    tmp_path: Path,
) -> None:
    with _isolated_postgres_url() as database_url:
        persistence = PostgresEvidencePersistence.open(database_url)
        try:
            with persistence.engine.begin() as connection:
                with pytest.raises(
                    IntegrityError, match="ck_artifact_storage_reference"
                ):
                    connection.execute(
                        text(
                            "INSERT INTO artifact "
                            "(digest, media_type, byte_size, relative_path, storage_kind, "
                            "registry_id, repository, manifest_digest) "
                            "VALUES (:digest, 'text/plain', 0, 'mixed', 'oci', "
                            "'unit0', 'seqevi/evidence', :manifest)"
                        ),
                        {"digest": "1" * 64, "manifest": "2" * 64},
                    )

            model = CommitModel.from_domain(
                _hit_commit(tmp_path / "expired", "MOCITIME")
            )
            with pytest.raises(
                StoreBackpressureError, match="expired before transaction"
            ):
                persistence.commit_many(
                    (model,), _oci_artifacts(model), deadline=monotonic() - 0.001
                )
            with persistence.engine.connect() as connection:
                assert (
                    connection.execute(
                        text("SELECT count(*) FROM artifact")
                    ).scalar_one()
                    == 0
                )
                assert (
                    connection.execute(
                        text("SELECT count(*) FROM evidence")
                    ).scalar_one()
                    == 0
                )
        finally:
            persistence.close()


def test_postgres_0004_to_0005_backfills_posix_and_refuses_oci_downgrade() -> None:
    with _isolated_postgres_url() as database_url:
        engine = create_engine(database_url)
        config = Config()
        config.set_main_option(
            "script_location",
            str(Path(store_migration.__file__).with_name("migrations")),
        )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            command.upgrade(config, "0004_claim_sessions")
            _seed_0002_evidence(connection)
            connection.commit()
        acknowledgement = store_migration.MaintenanceAcknowledgement(
            store_migration._database_identity(engine), "0004_claim_sessions"
        )
        store_migration.maintenance_upgrade_database(engine, None, acknowledgement)
        with engine.connect() as connection:
            assert store_migration._revision(connection) == "0005_oci_artifact_storage"
            rows = connection.execute(
                text("SELECT storage_kind, relative_path FROM artifact ORDER BY digest")
            ).all()
            assert rows and all(
                row.storage_kind == "posix" and row.relative_path for row in rows
            )
            connection.execute(
                text(
                    "INSERT INTO artifact "
                    "(digest, media_type, byte_size, relative_path, storage_kind, "
                    "registry_id, repository, manifest_digest) "
                    "VALUES (:digest, 'text/plain', 0, NULL, 'oci', 'unit0', "
                    "'seqevi/evidence', :manifest)"
                ),
                {"digest": "3" * 64, "manifest": "4" * 64},
            )
            connection.commit()
        with pytest.raises(RuntimeError, match="OCI artifact rows"):
            store_migration.maintenance_downgrade_database(
                engine,
                None,
                store_migration.MaintenanceAcknowledgement(
                    store_migration._database_identity(engine),
                    "0005_oci_artifact_storage",
                ),
            )
        engine.dispose()
