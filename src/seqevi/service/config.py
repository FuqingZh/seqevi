"""Validated shared Store service settings."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServiceSettings(BaseSettings):
    """Deployment configuration with bounded public request sizes."""

    model_config = SettingsConfigDict(env_prefix="SEQEVI_", extra="forbid")

    database_url: str
    artifacts_dir: Path
    maximum_batch_size: int = Field(default=1000, ge=1, le=10000)
    maximum_artifact_bytes: int = Field(
        default=512 * 1024 * 1024,
        ge=1,
        le=8 * 1024 * 1024 * 1024,
    )

    def model_post_init(self, _context: object) -> None:
        if not self.database_url.startswith(("postgresql://", "postgresql+psycopg://")):
            raise ValueError("shared Store database_url must use PostgreSQL")
        object.__setattr__(
            self,
            "artifacts_dir",
            self.artifacts_dir.expanduser().resolve(),
        )
