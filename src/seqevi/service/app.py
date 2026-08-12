"""FastAPI application for the passive shared evidence Store."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from time import perf_counter
from datetime import UTC, datetime, timedelta
from typing import Annotated, Iterator
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response, StreamingResponse

from seqevi.errors import (
    ClaimReceiptCapacityError,
    EvidenceClaimLostError,
    EvidenceConflictError,
    StoreBackpressureError,
    StoreIntegrityError,
)
from seqevi.evidence import ClaimSessionAuthority, EvidenceKey
from seqevi.store.artifact import PosixArtifactStore
from seqevi.store.transport import (
    ArtifactReferenceModel,
    ArtifactUploadResponse,
    BusyEvidenceClaimModel,
    ClaimSessionAcquireRequest,
    ClaimSessionAcquireResponse,
    ClaimSessionAcquireResultModel,
    ClaimSessionCapabilitiesResponse,
    ClaimSessionCloseRequest,
    ClaimSessionCloseResponse,
    ClaimSessionFinalizeRequest,
    ClaimSessionFinalizeResponse,
    ClaimSessionOpenRequest,
    ClaimSessionOpenResponse,
    ClaimSessionRenewRequest,
    ClaimSessionRenewResponse,
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
    SessionEvidenceClaimModel,
)

from .config import ServiceSettings
from .persistence import (
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
        transaction_timeout_seconds=settings.database_transaction_timeout_seconds,
    )
    artifact_store = PosixArtifactStore(settings.artifacts_dir)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        stopped = asyncio.Event()

        async def sweep_coordination() -> None:
            sweep = getattr(database, "sweep_claim_sessions", None)
            if sweep is None:
                return
            while not stopped.is_set():
                try:
                    await asyncio.to_thread(sweep)
                except StoreBackpressureError:
                    pass
                try:
                    await asyncio.wait_for(stopped.wait(), timeout=1.0)
                except TimeoutError:
                    continue

        sweeper = asyncio.create_task(sweep_coordination())
        try:
            yield
        finally:
            stopped.set()
            await sweeper
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
        operation = _claim_operation(request.url.path)
        if operation is not None:
            request.state.seqevi_claim_request_id = uuid4().hex
            request.state.seqevi_claim_started = perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            if operation is not None:
                _log_claim_request(
                    operation,
                    batch_size=0,
                    outcome="error",
                    status_code=500,
                    request_id=request.state.seqevi_claim_request_id,
                    duration_ms=round(
                        (perf_counter() - request.state.seqevi_claim_started) * 1000,
                        3,
                    ),
                )
            raise
        if operation is not None:
            _log_claim_request(
                operation,
                batch_size=0,
                outcome="ok" if response.status_code < 400 else "http_error",
                status_code=response.status_code,
                request_id=request.state.seqevi_claim_request_id,
                duration_ms=round(
                    (perf_counter() - request.state.seqevi_claim_started) * 1000, 3
                ),
            )
        return response

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
        "/v1/internal/claim-sessions/capabilities",
        response_model=ClaimSessionCapabilitiesResponse,
    )
    def claim_session_capabilities() -> ClaimSessionCapabilitiesResponse:
        if not database.supports_claim_sessions:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "ClaimSession unsupported")
        return ClaimSessionCapabilitiesResponse(
            maximum_batch_size=min(
                settings.maximum_batch_size, _CLAIM_MAXIMUM_BATCH_SIZE
            ),
            server_time=database.database_time(),
        )

    @app.post(
        "/v1/internal/claim-sessions/open", response_model=ClaimSessionOpenResponse
    )
    def open_claim_session(
        request: ClaimSessionOpenRequest,
    ) -> ClaimSessionOpenResponse:
        try:
            authority = database.open_claim_session(
                open_request_id=request.open_request_id,
                server_time=request.server_time,
                open_not_after=request.open_not_after,
            )
        except TimeoutError as error:
            raise HTTPException(
                status.HTTP_408_REQUEST_TIMEOUT,
                {"code": "open_request_expired", "detail": str(error)},
            ) from error
        except StoreBackpressureError as error:
            raise _backpressure_error(error) from error
        return ClaimSessionOpenResponse.model_validate(_authority_payload(authority))

    @app.post(
        "/v1/internal/claim-sessions/renew", response_model=ClaimSessionRenewResponse
    )
    def renew_claim_session(
        request: ClaimSessionRenewRequest,
    ) -> ClaimSessionRenewResponse:
        try:
            renewed = database.renew_claim_session(_request_authority(request))
        except EvidenceClaimLostError as error:
            raise HTTPException(
                status.HTTP_412_PRECONDITION_FAILED, str(error)
            ) from error
        except EvidenceConflictError as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        except StoreBackpressureError as error:
            raise _backpressure_error(error) from error
        return ClaimSessionRenewResponse.model_validate(_authority_payload(renewed))

    @app.post(
        "/v1/internal/claim-sessions/close", response_model=ClaimSessionCloseResponse
    )
    def close_claim_session(
        request: ClaimSessionCloseRequest,
    ) -> ClaimSessionCloseResponse:
        try:
            database.close_claim_session(_request_authority(request))
        except EvidenceClaimLostError as error:
            raise HTTPException(
                status.HTTP_412_PRECONDITION_FAILED, str(error)
            ) from error
        except EvidenceConflictError as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        except StoreBackpressureError as error:
            raise _backpressure_error(error) from error
        return ClaimSessionCloseResponse(
            session_id=request.session_id, generation=request.generation
        )

    @app.post(
        "/v1/internal/claim-sessions/acquire",
        response_model=ClaimSessionAcquireResponse,
    )
    def acquire_claim_session(
        request: ClaimSessionAcquireRequest,
    ) -> ClaimSessionAcquireResponse:
        _check_batch_size(len(request.queries), min(settings.maximum_batch_size, 1000))
        try:
            results = database.acquire_claim_session(
                _request_authority(request),
                acquire_request_id=request.acquire_request_id,
                query_digest=request.query_digest,
                queries=(query.to_domain() for query in request.queries),
            )
        except ClaimReceiptCapacityError as error:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE,
                {"code": "claim_receipt_capacity", "detail": str(error)},
                headers={"Retry-After": "1"},
            ) from error
        except EvidenceClaimLostError as error:
            raise HTTPException(
                status.HTTP_412_PRECONDITION_FAILED, str(error)
            ) from error
        except EvidenceConflictError as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        except StoreBackpressureError as error:
            raise _backpressure_error(error) from error
        return ClaimSessionAcquireResponse(
            results=[
                ClaimSessionAcquireResultModel(
                    disposition=result.disposition,
                    record=None
                    if result.record is None
                    else EvidenceRecordModel.from_domain(result.record),
                    claim=None
                    if result.claim is None
                    else SessionEvidenceClaimModel.from_domain(result.claim),
                    busy=None
                    if result.busy is None
                    else BusyEvidenceClaimModel.from_domain(result.busy),
                )
                for result in results
            ]
        )

    @app.post(
        "/v1/internal/claim-sessions/finalize",
        response_model=ClaimSessionFinalizeResponse,
    )
    def finalize_claim_session(
        request: ClaimSessionFinalizeRequest,
    ) -> ClaimSessionFinalizeResponse:
        _check_batch_size(len(request.commits), min(settings.maximum_batch_size, 1000))
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
                    existing_reference = references.setdefault(
                        reference.digest, reference
                    )
                    if existing_reference != reference:
                        raise StoreIntegrityError(
                            f"artifact reference conflict: {reference.digest}"
                        )
                    if reference.digest not in stored:
                        stored[reference.digest] = artifact_store.describe_existing(
                            digest=reference.digest,
                            media_type=reference.media_type,
                            byte_size=reference.byte_size,
                        )
            outcomes = database.finalize_claim_session(
                _request_authority(request), request.commits, stored
            )
        except EvidenceClaimLostError as error:
            raise HTTPException(
                status.HTTP_412_PRECONDITION_FAILED, str(error)
            ) from error
        except EvidenceConflictError as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        except StoreBackpressureError as error:
            raise _backpressure_error(error) from error
        return ClaimSessionFinalizeResponse(outcomes=list(outcomes))

    @app.post("/v1/evidence/lookup", response_model=LookupResponse)
    def lookup(request: LookupRequest) -> LookupResponse:
        _check_batch_size(len(request.queries), settings.maximum_batch_size)
        try:
            records = database.lookup_many(
                query.to_domain() for query in request.queries
            )
        except StoreBackpressureError as error:
            raise _backpressure_error(error) from error
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
        try:
            record = database.fetch_record(request.key.to_domain())
        except StoreBackpressureError as error:
            raise _backpressure_error(error) from error
        return FetchResponse(
            record=None if record is None else EvidenceRecordModel.from_domain(record)
        )

    @app.post("/v1/evidence/fetch-many", response_model=FetchManyResponse)
    def fetch_many(request: FetchManyRequest) -> FetchManyResponse:
        _check_batch_size(len(request.keys), settings.maximum_batch_size)
        keys = [key.to_domain() for key in request.keys]
        try:
            records = database.fetch_many(keys)
        except StoreBackpressureError as error:
            raise _backpressure_error(error) from error
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
        try:
            artifact = database.artifact_metadata(digest)
        except StoreBackpressureError as error:
            raise _backpressure_error(error) from error
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

    return app


def _request_authority(request: object) -> ClaimSessionAuthority:
    session_id = getattr(request, "session_id")
    owner_token = getattr(request, "owner_token")
    generation = getattr(request, "generation")
    return ClaimSessionAuthority(
        session_id=session_id,
        owner_token=owner_token,
        generation=generation,
        expires_at=datetime.now(UTC) + timedelta(seconds=1),
        remaining_lease_seconds=1.0,
        heartbeat_after_seconds=1.0,
        renew_deadline_seconds=1.0,
    )


def _authority_payload(authority: ClaimSessionAuthority) -> dict[str, object]:
    return {
        "session_id": authority.session_id,
        "owner_token": authority.owner_token,
        "generation": authority.generation,
        "expires_at": authority.expires_at,
        "remaining_lease_seconds": authority.remaining_lease_seconds,
        "heartbeat_after_seconds": authority.heartbeat_after_seconds,
        "renew_deadline_seconds": authority.renew_deadline_seconds,
    }


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
    for operation in ("capabilities", "open", "acquire", "renew", "close", "finalize"):
        if path.endswith(f"/claim-sessions/{operation}"):
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
        "finalize": "commits",
    }.get(operation)
    if field is None:
        return None
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
