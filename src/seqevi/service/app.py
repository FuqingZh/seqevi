"""FastAPI application for the passive shared evidence Store."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from time import perf_counter
from typing import Annotated, Iterator
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse

from seqevi.errors import (
    EvidenceClaimLostError,
    EvidenceConflictError,
    StoreBackpressureError,
    StoreIntegrityError,
)
from seqevi.evidence import EvidenceKey
from seqevi.store.artifact import PosixArtifactStore
from seqevi.store.transport import (
    ArtifactReferenceModel,
    ArtifactUploadResponse,
    ClaimAcquireRequest,
    ClaimAcquireResponse,
    ClaimAcquireResultModel,
    ClaimCapabilitiesResponse,
    ClaimFinalizeRequest,
    ClaimMutationRequest,
    ClaimReleaseResponse,
    ClaimReleaseAcknowledgement,
    ClaimRenewResponse,
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
    EvidenceClaimModel,
)

from .config import ServiceSettings
from .persistence import (
    CLAIM_LEASE_SECONDS,
    CLAIM_RENEWAL_SECONDS,
    PostgresEvidencePersistence,
    ServicePersistence,
)

_CLAIM_MAXIMUM_BATCH_SIZE = 1000
_CLAIM_LOGGER = logging.getLogger("seqevi.service.claims")
_CLAIM_LOGGER.setLevel(logging.INFO)


def create_service_app(
    settings: ServiceSettings,
    *,
    persistence: ServicePersistence | None = None,
) -> FastAPI:
    """Construct a bounded v1 Store API without annotation scheduling."""

    database = persistence or PostgresEvidencePersistence.open(
        settings.database_url,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout_seconds=settings.database_pool_timeout_seconds,
        lock_timeout_seconds=settings.database_lock_timeout_seconds,
        statement_timeout_seconds=settings.database_statement_timeout_seconds,
    )
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

    @app.middleware("http")
    async def capture_claim_request_boundary(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        if _claim_operation(request.url.path) is not None:
            request.state.seqevi_claim_request_id = uuid4().hex
            request.state.seqevi_claim_started = perf_counter()
        return await call_next(request)

    @app.exception_handler(RequestValidationError)
    async def claim_request_validation_error(
        request: Request, error: RequestValidationError
    ) -> JSONResponse:
        operation = _claim_operation(request.url.path)
        if operation is not None:
            started = getattr(request.state, "seqevi_claim_started", perf_counter())
            _log_claim_request(
                operation,
                batch_size=_claim_batch_size(operation, error.body),
                outcome="http_error",
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                request_id=getattr(request.state, "seqevi_claim_request_id", None),
                duration_ms=round((perf_counter() - started) * 1000, 3),
            )
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": jsonable_encoder(error.errors())},
        )

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(
            maximum_batch_size=settings.maximum_batch_size,
            maximum_artifact_bytes=settings.maximum_artifact_bytes,
        )

    @app.get(
        "/v1/evidence/claims/capabilities",
        response_model=ClaimCapabilitiesResponse,
    )
    def claim_capabilities() -> ClaimCapabilitiesResponse:
        if not getattr(database, "supports_claims", False):
            raise HTTPException(
                status.HTTP_404_NOT_FOUND, "evidence claims unsupported"
            )
        return ClaimCapabilitiesResponse(
            maximum_batch_size=min(
                settings.maximum_batch_size, _CLAIM_MAXIMUM_BATCH_SIZE
            ),
            lease_seconds=CLAIM_LEASE_SECONDS,
            renewal_after_seconds=CLAIM_RENEWAL_SECONDS,
        )

    @app.post("/v1/evidence/claims/acquire", response_model=ClaimAcquireResponse)
    def acquire_claims(
        request: ClaimAcquireRequest, http_request: Request
    ) -> ClaimAcquireResponse:
        with _observe_claim_request(
            "acquire",
            len(request.queries),
            started=_claim_started(http_request),
            request_id=_claim_request_id(http_request),
        ):
            _check_batch_size(
                len(request.queries),
                min(settings.maximum_batch_size, _CLAIM_MAXIMUM_BATCH_SIZE),
            )
            try:
                results = database.acquire_many(
                    (query.to_domain() for query in request.queries),
                    owner_token=request.owner_token,
                )
            except StoreBackpressureError as error:
                raise _backpressure_error(error) from error
            except StoreIntegrityError as error:
                raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
            except ValueError as error:
                raise HTTPException(422, str(error)) from error
            return ClaimAcquireResponse(
                results=[
                    ClaimAcquireResultModel.from_domain(result) for result in results
                ]
            )

    @app.post("/v1/evidence/claims/renew", response_model=ClaimRenewResponse)
    def renew_claims(
        request: ClaimMutationRequest, http_request: Request
    ) -> ClaimRenewResponse:
        with _observe_claim_request(
            "renew",
            len(request.claims),
            started=_claim_started(http_request),
            request_id=_claim_request_id(http_request),
        ):
            _check_batch_size(
                len(request.claims),
                min(settings.maximum_batch_size, _CLAIM_MAXIMUM_BATCH_SIZE),
            )
            try:
                renewed = database.renew_many(
                    claim.to_domain() for claim in request.claims
                )
            except StoreBackpressureError as error:
                raise _backpressure_error(error) from error
            except EvidenceClaimLostError as error:
                raise HTTPException(
                    status.HTTP_412_PRECONDITION_FAILED, str(error)
                ) from error
            return ClaimRenewResponse(
                claims=[EvidenceClaimModel.from_domain(claim) for claim in renewed]
            )

    @app.post("/v1/evidence/claims/release", response_model=ClaimReleaseResponse)
    def release_claims(
        request: ClaimMutationRequest, http_request: Request
    ) -> ClaimReleaseResponse:
        with _observe_claim_request(
            "release",
            len(request.claims),
            started=_claim_started(http_request),
            request_id=_claim_request_id(http_request),
        ):
            _check_batch_size(
                len(request.claims),
                min(settings.maximum_batch_size, _CLAIM_MAXIMUM_BATCH_SIZE),
            )
            try:
                database.release_many(claim.to_domain() for claim in request.claims)
            except StoreBackpressureError as error:
                raise _backpressure_error(error) from error
            except EvidenceClaimLostError as error:
                raise HTTPException(
                    status.HTTP_412_PRECONDITION_FAILED, str(error)
                ) from error
            return ClaimReleaseResponse(
                released=[
                    ClaimReleaseAcknowledgement(
                        key=claim.key, generation=claim.generation
                    )
                    for claim in request.claims
                ]
            )

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
        except StoreBackpressureError as error:
            raise _backpressure_error(error) from error
        except EvidenceConflictError as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        except (StoreIntegrityError, ValueError) as error:
            raise HTTPException(422, str(error)) from error
        return CommitResponse(outcomes=list(outcomes))

    @app.post("/v1/evidence/claims/finalize", response_model=CommitResponse)
    def finalize_claims(
        request: ClaimFinalizeRequest, http_request: Request
    ) -> CommitResponse:
        with _observe_claim_request(
            "finalize",
            len(request.commits),
            started=_claim_started(http_request),
            request_id=_claim_request_id(http_request),
        ):
            _check_batch_size(
                len(request.commits),
                min(settings.maximum_batch_size, _CLAIM_MAXIMUM_BATCH_SIZE),
            )
            stored = {}
            references: dict[str, ArtifactReferenceModel] = {}
            try:
                for item in request.commits:
                    _validate_commit_model(item.commit)
                    for reference in (
                        item.commit.normalized_artifact,
                        item.commit.raw_artifact,
                    ):
                        if reference is None:
                            continue
                        existing = references.setdefault(reference.digest, reference)
                        if existing != reference:
                            raise StoreIntegrityError(
                                f"artifact reference conflict: {reference.digest}"
                            )
                        if reference.digest not in stored:
                            stored[reference.digest] = artifact_store.describe_existing(
                                digest=reference.digest,
                                media_type=reference.media_type,
                                byte_size=reference.byte_size,
                            )
                outcomes = database.finalize_many(request.commits, stored)
            except StoreBackpressureError as error:
                raise _backpressure_error(error) from error
            except EvidenceClaimLostError as error:
                raise HTTPException(
                    status.HTTP_412_PRECONDITION_FAILED, str(error)
                ) from error
            except EvidenceConflictError as error:
                raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
            except (StoreIntegrityError, ValueError) as error:
                raise HTTPException(422, str(error)) from error
            return CommitResponse(outcomes=list(outcomes))

    return app


def configure_claim_logging() -> None:
    """Attach a concrete INFO handler for the standalone ``serve`` process."""

    _CLAIM_LOGGER.setLevel(logging.INFO)
    if _CLAIM_LOGGER.handlers:
        return
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(message)s"))
    _CLAIM_LOGGER.addHandler(handler)
    _CLAIM_LOGGER.propagate = False


def _check_batch_size(size: int, maximum: int) -> None:
    if size > maximum:
        raise HTTPException(
            status.HTTP_413_CONTENT_TOO_LARGE,
            f"batch contains {size} entries; maximum is {maximum}",
        )


def _backpressure_error(error: StoreBackpressureError) -> HTTPException:
    return HTTPException(
        status.HTTP_503_SERVICE_UNAVAILABLE,
        str(error),
        headers={"Retry-After": "1"},
    )


@contextmanager
def _observe_claim_request(
    operation: str,
    batch_size: int,
    *,
    started: float | None = None,
    request_id: str | None = None,
) -> Iterator[None]:
    """Emit one secret-free, machine-readable timing record per claim request."""

    request_id = request_id or uuid4().hex
    started = started if started is not None else perf_counter()
    outcome = "error"
    status_code = 500
    try:
        yield
    except HTTPException as error:
        outcome = "http_error"
        status_code = error.status_code
        raise
    except Exception:
        raise
    else:
        outcome = "ok"
        status_code = 200
    finally:
        _log_claim_request(
            operation,
            batch_size=batch_size,
            outcome=outcome,
            status_code=status_code,
            request_id=request_id,
            duration_ms=round((perf_counter() - started) * 1000, 3),
        )


def _claim_operation(path: str) -> str | None:
    for operation in ("acquire", "renew", "release", "finalize"):
        if path.endswith(f"/claims/{operation}"):
            return operation
    return None


def _log_claim_request(
    operation: str,
    *,
    batch_size: int | None,
    outcome: str,
    status_code: int,
    request_id: str | None = None,
    duration_ms: float | None = None,
) -> None:
    _CLAIM_LOGGER.info(
        json.dumps(
            {
                "event": "seqevi.claim_request",
                "request_id": request_id or uuid4().hex,
                "operation": operation,
                "batch_size": batch_size,
                "outcome": outcome,
                "status_code": status_code,
                "duration_ms": duration_ms if duration_ms is not None else 0.0,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )


def _claim_started(request: Request) -> float:
    return getattr(request.state, "seqevi_claim_started", perf_counter())


def _claim_request_id(request: Request) -> str | None:
    return getattr(request.state, "seqevi_claim_request_id", None)


def _claim_batch_size(operation: str, body: object) -> int | None:
    if not isinstance(body, dict):
        return None
    field = {
        "acquire": "queries",
        "renew": "claims",
        "release": "claims",
        "finalize": "commits",
    }[operation]
    values = body.get(field)
    if isinstance(values, list):
        return len(values)
    return None


def _validate_commit_model(commit: CommitModel) -> None:
    if commit.key.sequence_id != commit.identity.sequence_id:
        raise ValueError("evidence key and sequence identity do not match")
    if commit.status.value == "hit" and commit.normalized_artifact is None:
        raise ValueError("hit evidence requires a normalized artifact")
    if commit.status.value == "no_hit" and commit.normalized_artifact is not None:
        raise ValueError("no-hit evidence cannot contain a normalized artifact")
    if commit.raw_artifact is None:
        raise ValueError("shared evidence requires a raw artifact")


def _key_sort_value(key: EvidenceKey) -> tuple[str, str, str, str, str]:
    return (
        key.sequence_id,
        key.adapter_contract_version,
        key.tool_runtime_digest,
        key.resource_id,
        key.semantic_parameters_hash,
    )
