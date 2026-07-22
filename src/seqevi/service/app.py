"""FastAPI application for the passive shared evidence Store."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import StreamingResponse

from seqevi.errors import EvidenceConflictError, StoreIntegrityError
from seqevi.evidence import EvidenceKey
from seqevi.store.artifact import PosixArtifactStore
from seqevi.store.transport import (
    ArtifactReferenceModel,
    ArtifactUploadResponse,
    CommitRequest,
    CommitResponse,
    CommitModel,
    EvidenceRecordModel,
    FetchManyRequest,
    FetchManyResponse,
    FetchRequest,
    FetchResponse,
    HealthResponse,
    LookupRequest,
    LookupResponse,
)

from .config import ServiceSettings
from .persistence import PostgresEvidencePersistence, ServicePersistence


def create_service_app(
    settings: ServiceSettings,
    *,
    persistence: ServicePersistence | None = None,
) -> FastAPI:
    """Construct a bounded v1 Store API without annotation scheduling."""

    database = persistence or PostgresEvidencePersistence.open(settings.database_url)
    artifact_store = PosixArtifactStore(settings.artifacts_dir)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            database.close()

    app = FastAPI(
        title="SeqEvi Shared Store",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse()

    @app.post("/v1/evidence/lookup", response_model=LookupResponse)
    def lookup(request: LookupRequest) -> LookupResponse:
        _check_batch_size(len(request.queries), settings.maximum_batch_size)
        try:
            records = database.lookup_many(
                query.to_domain() for query in request.queries
            )
        except StoreIntegrityError as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        return LookupResponse(
            records=[
                EvidenceRecordModel.from_domain(records[key])
                for key in sorted(records, key=_key_sort_value)
            ]
        )

    @app.post("/v1/evidence/fetch", response_model=FetchResponse)
    def fetch(request: FetchRequest) -> FetchResponse:
        record = database.fetch_record(request.key.to_domain())
        return FetchResponse(
            record=None if record is None else EvidenceRecordModel.from_domain(record)
        )

    @app.post("/v1/evidence/fetch-many", response_model=FetchManyResponse)
    def fetch_many(request: FetchManyRequest) -> FetchManyResponse:
        _check_batch_size(len(request.keys), settings.maximum_batch_size)
        keys = [key.to_domain() for key in request.keys]
        records = database.fetch_many(keys)
        return FetchManyResponse(
            records=[
                EvidenceRecordModel.from_domain(records[key])
                for key in keys
                if key in records
            ]
        )

    @app.put(
        "/v1/artifacts/{digest}",
        response_model=ArtifactUploadResponse,
    )
    async def upload_artifact(
        digest: str,
        request: Request,
        media_type: Annotated[str, Header(alias="X-Artifact-Media-Type")],
        byte_size: Annotated[int, Header(alias="X-Artifact-Byte-Size", ge=0)],
    ) -> ArtifactUploadResponse:
        if byte_size > settings.maximum_artifact_bytes:
            raise HTTPException(
                status.HTTP_413_CONTENT_TOO_LARGE,
                "artifact exceeds configured upload limit",
            )
        try:
            artifact, created = await artifact_store.put_async(
                request.stream(),
                expected_digest=digest,
                expected_size=byte_size,
                media_type=media_type,
                maximum_size=settings.maximum_artifact_bytes,
            )
        except (StoreIntegrityError, ValueError) as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        return ArtifactUploadResponse(
            status="created" if created else "existing",
            artifact=ArtifactReferenceModel(
                digest=artifact.digest,
                media_type=artifact.media_type,
                byte_size=artifact.byte_size,
            ),
        )

    @app.get("/v1/artifacts/{digest}")
    def download_artifact(digest: str) -> StreamingResponse:
        artifact = database.artifact_metadata(digest)
        if artifact is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "artifact is not registered")
        return StreamingResponse(
            artifact_store.iter_bytes(digest),
            media_type=artifact.media_type,
            headers={
                "Content-Length": str(artifact.byte_size),
                "X-Artifact-Digest": artifact.digest,
            },
        )

    @app.post("/v1/evidence/commit", response_model=CommitResponse)
    def commit(request: CommitRequest) -> CommitResponse:
        _check_batch_size(len(request.commits), settings.maximum_batch_size)
        stored = {}
        references: dict[str, ArtifactReferenceModel] = {}
        try:
            for commit_model in request.commits:
                _validate_commit_model(commit_model)
                for reference in (
                    commit_model.normalized_artifact,
                    commit_model.raw_artifact,
                ):
                    if reference is None:
                        continue
                    existing_reference = references.setdefault(
                        reference.digest, reference
                    )
                    if existing_reference != reference:
                        raise StoreIntegrityError(
                            f"artifact reference conflict: {reference.digest}"
                        )
                    if reference.digest in stored:
                        continue
                    stored[reference.digest] = artifact_store.describe_existing(
                        digest=reference.digest,
                        media_type=reference.media_type,
                        byte_size=reference.byte_size,
                    )
            outcomes = database.commit_many(request.commits, stored)
        except EvidenceConflictError as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        except (StoreIntegrityError, ValueError) as error:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_CONTENT, str(error)
            ) from error
        return CommitResponse(outcomes=list(outcomes))

    return app


def _check_batch_size(size: int, maximum: int) -> None:
    if size > maximum:
        raise HTTPException(
            status.HTTP_413_CONTENT_TOO_LARGE,
            f"batch contains {size} entries; maximum is {maximum}",
        )


def _validate_commit_model(commit: CommitModel) -> None:
    if commit.key.sequence_id != commit.identity.sequence_id:
        raise ValueError("evidence key and sequence identity do not match")
    if commit.status.value == "hit" and commit.normalized_artifact is None:
        raise ValueError("hit evidence requires a normalized artifact")
    if commit.status.value == "no_hit" and commit.normalized_artifact is not None:
        raise ValueError("no-hit evidence cannot contain a normalized artifact")


def _key_sort_value(key: EvidenceKey) -> tuple[str, str, str, str, str]:
    return (
        key.sequence_id,
        key.adapter_contract_version,
        key.tool_runtime_digest,
        key.resource_id,
        key.semantic_parameters_hash,
    )
