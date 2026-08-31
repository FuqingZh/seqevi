"""Validated shared Store service settings."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from seqevi.store.transport import RegistryModel


DEFAULT_DATABASE_POOL_SIZE = 16
DEFAULT_DATABASE_MAX_OVERFLOW = 8
DEFAULT_DATABASE_POOL_TIMEOUT_SECONDS = 5.0
DEFAULT_DATABASE_LOCK_TIMEOUT_SECONDS = 5.0
DEFAULT_DATABASE_STATEMENT_TIMEOUT_SECONDS = 15.0
DEFAULT_DATABASE_TRANSACTION_TIMEOUT_SECONDS = 25.0
# The first-party lease renews after 20 seconds and preserves a five-second
# runway. Leave a further five seconds for dispatch and the response after the
# combined pool checkout and PostgreSQL transaction wait.
MAXIMUM_DATABASE_REQUEST_WAIT_SECONDS = 30.0


def _normalize_postgres_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    if not database_url.startswith("postgresql+psycopg://"):
        raise ValueError("shared Store database_url must use PostgreSQL")
    return database_url


class ServiceSettings(BaseSettings):
    """Deployment configuration with bounded public request sizes."""

    model_config = SettingsConfigDict(env_prefix="SEQEVI_", extra="forbid")

    database_url: str
    artifacts_dir: Path
    artifact_backend: Literal["legacy-posix", "oci-registry"] = "legacy-posix"
    oci_registry_id: str | None = None
    oci_registry_endpoint: str | None = None
    oci_registry_repository: str | None = None
    oci_oras_executable: Path | None = None
    oci_registry_config: Path | None = None
    oci_registry_ca_file: Path | None = None
    maximum_batch_size: int = Field(default=1000, ge=1, le=10000)
    maximum_artifact_bytes: int = Field(
        default=512 * 1024 * 1024,
        ge=1,
        le=8 * 1024 * 1024 * 1024,
    )
    maximum_concurrent_artifact_uploads: int = Field(default=8, ge=1, le=64)
    database_pool_size: int = Field(default=DEFAULT_DATABASE_POOL_SIZE, ge=1, le=256)
    database_max_overflow: int = Field(
        default=DEFAULT_DATABASE_MAX_OVERFLOW, ge=0, le=256
    )
    database_pool_timeout_seconds: float = Field(
        default=DEFAULT_DATABASE_POOL_TIMEOUT_SECONDS, gt=0, le=120
    )
    database_lock_timeout_seconds: float = Field(
        default=DEFAULT_DATABASE_LOCK_TIMEOUT_SECONDS, gt=0, le=60
    )
    database_statement_timeout_seconds: float = Field(
        default=DEFAULT_DATABASE_STATEMENT_TIMEOUT_SECONDS, gt=0, le=120
    )
    database_transaction_timeout_seconds: float = Field(
        default=DEFAULT_DATABASE_TRANSACTION_TIMEOUT_SECONDS,
        gt=0,
        le=MAXIMUM_DATABASE_REQUEST_WAIT_SECONDS,
    )

    def model_post_init(self, _context: object) -> None:
        oci_values = (
            self.oci_registry_id,
            self.oci_registry_endpoint,
            self.oci_registry_repository,
            self.oci_oras_executable,
            self.oci_registry_config,
        )
        if self.artifact_backend == "oci-registry":
            if not all(oci_values):
                raise ValueError(
                    "OCI mode requires explicit Registry, ORAS and auth-file settings"
                )
            self.registry_definition()
            for path in (
                self.oci_oras_executable,
                self.oci_registry_config,
                self.oci_registry_ca_file,
            ):
                if path is not None and not path.is_absolute():
                    raise ValueError("service OCI file paths must be absolute")
        elif any(
            value is not None for value in (*oci_values, self.oci_registry_ca_file)
        ):
            raise ValueError("OCI settings require artifact_backend=oci-registry")
        total_wait = (
            self.database_pool_timeout_seconds
            + self.database_transaction_timeout_seconds
        )
        if total_wait > MAXIMUM_DATABASE_REQUEST_WAIT_SECONDS:
            raise ValueError(
                "database pool and transaction timeouts must total at most "
                f"{MAXIMUM_DATABASE_REQUEST_WAIT_SECONDS:g} seconds"
            )
        object.__setattr__(
            self,
            "database_url",
            _normalize_postgres_database_url(self.database_url),
        )
        object.__setattr__(
            self,
            "artifacts_dir",
            self.artifacts_dir.expanduser().resolve(),
        )

    def registry_definition(self) -> RegistryModel | None:
        """Return public endpoint metadata, never service credential paths."""
        if self.artifact_backend != "oci-registry":
            return None
        assert self.oci_registry_id is not None
        assert self.oci_registry_endpoint is not None
        assert self.oci_registry_repository is not None
        return RegistryModel(
            id=self.oci_registry_id,
            endpoint=self.oci_registry_endpoint,
            repository=self.oci_registry_repository,
        )
