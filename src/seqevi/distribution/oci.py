"""Ephemeral Docker delegation for managed execution profiles."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import duckdb

from seqevi import __version__
from seqevi.annotate import AnnotationMetrics, AnnotationSummary
from seqevi.errors import AnnotationError
from seqevi.evidence import sha256_digest
from seqevi.execution_profile import ExecutionProfile, ManagedRuntime
from seqevi.resource_lock import read_resource_lock
from seqevi.result import RESULT_FORMAT_VERSION

from .manifest import KitManifest, load_kit_manifest

_CONTAINER_INPUT = "/mnt/seqevi/input.fasta"
_CONTAINER_RESOURCE = "/mnt/seqevi/resource"
_CONTAINER_OUTPUT = "/mnt/seqevi/output"
_CONTAINER_STORE = "/mnt/seqevi/store"
_CONTAINER_ENTRYPOINT = "/opt/venv/bin/seqevi"
_CONTAINER_EXECUTABLE = "/opt/dbcan-venv/bin/run_dbcan"
_DEFAULT_DOCKER_TIMEOUT_SECONDS = 30.0
_PULL_TIMEOUT_SECONDS = 900.0


@dataclass(frozen=True, slots=True)
class OciAnnotationResult:
    """Validated result produced by one managed container invocation.

    This is an application-boundary value, not an adapter result. The adapter
    remains unaware of Docker; callers receive the same summary and published
    DuckDB path as the local execution path.
    """

    output: Path
    summary: AnnotationSummary
    adapter: str
    result_schema_id: str


def run_oci_annotation(
    *,
    fasta: Path,
    output: Path,
    profile: ExecutionProfile,
    store: str | Path | None,
    threads: int,
    timeout_seconds: float | None,
) -> OciAnnotationResult:
    """Delegate one v2 annotation through its digest-pinned Docker image.

    The container is ephemeral and receives only the input FASTA, the
    caller-owned resource, a narrow output staging directory and (for a local
    Store) its Store directory. FASTA and resource binds are read-only; the
    container uses the caller's numeric UID/GID and local Store execution uses
    ``--network none``.

    Notes:
        The managed profile and bundled kit are checked together before Docker
        is launched. The function intentionally rejects credential-bearing
        shared Store URLs until an external secret boundary is available.
        Container cleanup is explicit so failures and cancellations do not
        leave an annotation process behind.
    """

    runtime = profile.runtime
    if profile.version != 2 or runtime is None:
        raise AnnotationError("OCI delegation requires a managed execution profile v2")
    if profile.adapter.value != "dbcan-cazyme":
        raise AnnotationError(
            f"managed OCI adapter is not supported: {profile.adapter.value}"
        )
    if not fasta.is_file():
        raise AnnotationError(f"annotation FASTA is not a readable file: {fasta}")
    if output.exists():
        raise AnnotationError(f"output path already exists: {output}")
    if not output.parent.is_dir():
        raise AnnotationError(
            f"output parent directory does not exist: {output.parent}"
        )
    resource = profile.resource.resolve()
    if not resource.is_dir() or not os.access(resource, os.R_OK):
        raise AnnotationError(f"managed profile resource is not readable: {resource}")

    manifest = load_kit_manifest("dbcan-cazyme")
    _validate_runtime(runtime, manifest)
    expected_resource_id = _resource_id_from_lock(resource, manifest)
    docker = shutil.which("docker")
    if docker is None:
        raise AnnotationError("Docker executable is not available on PATH")
    _ensure_image(docker, manifest, runtime.image)

    store_mount, store_argument, network = _resolve_store(store)
    stage = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    staged_output = stage / output.name
    container_name = f"seqevi-annotate-oci-{uuid.uuid4().hex[:16]}"
    created = False
    failure: AnnotationError | None = None
    try:
        command = _inner_command(
            fasta=_CONTAINER_INPUT,
            output=f"{_CONTAINER_OUTPUT}/{output.name}",
            resource=_CONTAINER_RESOURCE,
            store=store_argument,
            threads=threads,
            timeout_seconds=timeout_seconds,
        )
        mounts = (
            _mount(fasta.resolve(), _CONTAINER_INPUT, readonly=True),
            _mount(resource, _CONTAINER_RESOURCE, readonly=True),
            _mount(stage, _CONTAINER_OUTPUT, readonly=False),
        )
        if store_mount is not None:
            mounts += (_mount(store_mount, _CONTAINER_STORE, readonly=False),)
        create_args = (
            "create",
            "--name",
            container_name,
            "--user",
            f"{_uid()}:{_gid()}",
            "--network",
            network,
            "--workdir",
            "/work",
            "--entrypoint",
            _CONTAINER_ENTRYPOINT,
        )
        for mount in mounts:
            create_args += ("--mount", mount)
        created_result = _docker_call(
            docker,
            (*create_args, runtime.image, *command),
            timeout_seconds=_DEFAULT_DOCKER_TIMEOUT_SECONDS,
            action="create the managed annotation container",
        )
        if created_result.returncode != 0:
            raise AnnotationError(
                "Docker could not create the managed annotation container: "
                + _process_detail(created_result)
            )
        created = True
        started = _docker_call(
            docker,
            ("start", "--attach", container_name),
            timeout_seconds=timeout_seconds,
            action="run the managed annotation container",
        )
        if started.returncode != 0:
            raise AnnotationError(
                "managed annotation container failed: " + _process_detail(started)
            )
        payload = _json_result(started.stdout)
        _validate_result(
            staged_output,
            payload,
            profile=profile,
            manifest=manifest,
            resource_id=expected_resource_id,
        )
        _publish_without_overwrite(staged_output, output)
        return OciAnnotationResult(
            output=output,
            summary=_summary_from_payload(payload, output=output),
            adapter=str(payload["adapter"]),
            result_schema_id=str(payload["result_schema"]),
        )
    except AnnotationError:
        raise
    except (OSError, ValueError, KeyError, TypeError, duckdb.Error) as error:
        raise AnnotationError(f"managed OCI annotation failed: {error}") from error
    finally:
        if created:
            removed = _docker_call(
                docker,
                ("rm", "--force", container_name),
                timeout_seconds=_DEFAULT_DOCKER_TIMEOUT_SECONDS,
                action="remove the managed annotation container",
            )
            if removed.returncode != 0 and failure is None:
                failure = AnnotationError(
                    "Docker could not remove the managed annotation container: "
                    + _process_detail(removed)
                )
        shutil.rmtree(stage, ignore_errors=True)
        if failure is not None:
            raise failure


def _validate_runtime(runtime: ManagedRuntime, manifest: KitManifest) -> None:
    if runtime.kind != "oci" or runtime.engine != "docker":
        raise AnnotationError("managed profile runtime must be OCI Docker")
    if runtime.kit_id != manifest.kit_id:
        raise AnnotationError(
            "managed profile kit_id conflicts with the bundled dbCAN kit"
        )
    if runtime.image != manifest.image:
        raise AnnotationError(
            "managed profile image conflicts with the bundled dbCAN kit"
        )


def _ensure_image(docker: str, manifest: KitManifest, image: str) -> None:
    inspected = _docker_call(
        docker,
        ("image", "inspect", image),
        timeout_seconds=_DEFAULT_DOCKER_TIMEOUT_SECONDS,
        action="inspect the managed runtime image",
    )
    if inspected.returncode != 0:
        pulled = _docker_call(
            docker,
            ("pull", "--platform", manifest.platform, image),
            timeout_seconds=_PULL_TIMEOUT_SECONDS,
            action="pull the managed runtime image",
        )
        if pulled.returncode != 0:
            raise AnnotationError(
                "Docker could not pull the exact managed runtime image: "
                + _process_detail(pulled)
            )
    verified = _docker_call(
        docker,
        ("image", "inspect", image),
        timeout_seconds=_DEFAULT_DOCKER_TIMEOUT_SECONDS,
        action="verify the managed runtime image",
    )
    if verified.returncode != 0:
        raise AnnotationError(
            "Docker did not retain the exact digest-pinned managed runtime image: "
            + _process_detail(verified)
        )


def _resolve_store(store: str | Path | None) -> tuple[Path | None, str, str]:
    if store is None:
        raise AnnotationError(
            "a Store path or URL is required via --store or SEQEVI_STORE"
        )
    raw = os.fspath(store)
    if raw.startswith(("http://", "https://")):
        parsed = urlsplit(raw)
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise AnnotationError(
                "managed OCI shared Store URLs must not embed credentials or "
                "query secrets; provide an external credential boundary"
            )
        return None, raw, "bridge"
    if "://" in raw:
        raise AnnotationError(f"unsupported Store URL scheme: {raw.split(':', 1)[0]}")
    path = Path(raw).expanduser().resolve()
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise AnnotationError(
            f"cannot prepare local Store mount: {path}: {error}"
        ) from error
    return path, _CONTAINER_STORE, "none"


def _inner_command(
    *,
    fasta: str,
    output: str,
    resource: str,
    store: str,
    threads: int,
    timeout_seconds: float | None,
) -> tuple[str, ...]:
    command = (
        "annotate",
        "--json",
        "--fasta",
        fasta,
        "--output",
        output,
        "--adapter",
        "dbcan-cazyme",
        "--executable",
        _CONTAINER_EXECUTABLE,
        "--resource",
        resource,
        "--store",
        store,
        "--threads",
        str(threads),
    )
    if timeout_seconds is not None:
        command += ("--timeout-seconds", str(timeout_seconds))
    return command


def _resource_id_from_lock(resource: Path, manifest: KitManifest) -> str:
    lock = read_resource_lock(resource)
    if lock is None:
        raise AnnotationError(
            "managed dbCAN resource has no seqevi.lock; rerun setup to verify it"
        )
    expected = {
        component.path: (component.name, component.size, component.sha256)
        for component in manifest.components
    }
    observed = {
        component.relative_path: (component.name, component.size, component.sha256)
        for component in lock.components
    }
    if (lock.resource_name, lock.resource_version) != (
        manifest.resource_name,
        manifest.resource_version,
    ) or observed != expected:
        raise AnnotationError("managed dbCAN resource lock conflicts with the kit")
    values = [(component.name, component.sha256) for component in manifest.components]
    digest = sha256_digest(
        json.dumps(values, separators=(",", ":"), ensure_ascii=True).encode()
    )
    return f"dbcan/{manifest.resource_version}/sha256:{digest}"


def _validate_result(
    path: Path,
    payload: Mapping[str, object],
    *,
    profile: ExecutionProfile,
    manifest: KitManifest,
    resource_id: str,
) -> None:
    if not path.is_file():
        raise AnnotationError("managed container did not publish a DuckDB result")
    if payload.get("status") != "ok":
        raise AnnotationError("managed container returned a non-success result")
    expected = {
        "adapter": profile.adapter.value,
        "result_schema": "dbcan-cazyme/5",
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise AnnotationError(f"managed container returned unexpected {key}")
    with duckdb.connect(str(path), read_only=True) as connection:
        row = connection.execute("SELECT * FROM _seqevi.metadata LIMIT 1").fetchone()
        if row is None:
            raise AnnotationError("managed result metadata is empty")
        columns = [item[0] for item in connection.description]
        metadata = dict(zip(columns, row, strict=True))
    checks = {
        "ResultFormatVersion": RESULT_FORMAT_VERSION,
        "SeqEviVersion": __version__,
        "Adapter": profile.adapter.value,
        "AdapterContractVersion": "dbcan-cazyme/1",
        "UpstreamTool": "dbCAN",
        "UpstreamToolVersion": manifest.dbcan_version,
        "ResourceID": resource_id,
    }
    for key, expected_value in checks.items():
        if metadata.get(key) != expected_value:
            raise AnnotationError(
                f"managed result metadata {key} conflicts with the selected kit"
            )
    runtime_digest = metadata.get("ToolRuntimeDigest")
    if not isinstance(runtime_digest, str) or not runtime_digest.startswith("sha256:"):
        raise AnnotationError("managed result is missing ToolRuntimeDigest")


def _summary_from_payload(
    payload: Mapping[str, object], *, output: Path
) -> AnnotationSummary:
    counts = _mapping(payload.get("counts"), "counts")
    metrics = _mapping(payload.get("metrics"), "metrics")
    return AnnotationSummary(
        input_records=_integer(counts, "input_records"),
        unique_sequences=_integer(counts, "unique_sequences"),
        cache_hits=_integer(counts, "cache_hits"),
        computed=_integer(counts, "computed"),
        hits=_integer(counts, "hits"),
        no_hits=_integer(counts, "no_hits"),
        output_dir=output,
        metrics=AnnotationMetrics(
            elapsed_seconds=_number(metrics, "elapsed_seconds"),
            fasta_staging_seconds=0.0,
            store_lookup_seconds=0.0,
            adapter_seconds=0.0,
            external_tool_seconds=0.0,
            store_commit_seconds=0.0,
            store_fetch_seconds=0.0,
            package_seconds=_number(metrics, "package_seconds"),
            peak_rss_kib=None,
            store_lookup_batches=0,
            store_commit_batches=0,
            store_fetch_batches=0,
            tool_batches=0,
            unique_artifact_reads=0,
            configured_threads=_integer(metrics, "configured_threads"),
        ),
    )


def _publish_without_overwrite(staged: Path, output: Path) -> None:
    try:
        os.link(staged, output)
    except FileExistsError as error:
        raise AnnotationError(
            f"output path appeared during managed annotation: {output}"
        ) from error
    except OSError as error:
        raise AnnotationError(
            f"cannot publish managed annotation result: {error}"
        ) from error
    staged.unlink()


def _mount(source: Path, target: str, *, readonly: bool) -> str:
    suffix = ",readonly" if readonly else ""
    return f"type=bind,source={source},target={target}{suffix}"


def _json_result(stdout: str) -> dict[str, object]:
    lines = [line.strip() for line in stdout.splitlines() if line.strip()]
    if not lines:
        raise AnnotationError("managed container returned no JSON result")
    try:
        value = json.loads(lines[-1])
    except json.JSONDecodeError as error:
        raise AnnotationError("managed container returned invalid JSON") from error
    if not isinstance(value, dict):
        raise AnnotationError("managed container JSON result is not an object")
    return value


def _mapping(value: object, name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise AnnotationError(f"managed container JSON is missing {name}")
    return value


def _integer(mapping: Mapping[str, object], name: str) -> int:
    value = mapping.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise AnnotationError(f"managed container JSON has invalid {name}")
    return value


def _number(mapping: Mapping[str, object], name: str) -> float:
    value = mapping.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AnnotationError(f"managed container JSON has invalid {name}")
    return float(value)


def _docker_call(
    docker: str,
    arguments: tuple[str, ...],
    *,
    timeout_seconds: float | None,
    action: str,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            [docker, *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        raise AnnotationError(f"timed out while trying to {action}") from error
    except OSError as error:
        raise AnnotationError(f"could not {action}: {error}") from error


def _process_detail(result: subprocess.CompletedProcess[str]) -> str:
    detail = (result.stderr or result.stdout or "no diagnostic output").strip()
    return detail[-2000:]


def _uid() -> int:
    return getattr(os, "getuid", lambda: 0)()


def _gid() -> int:
    return getattr(os, "getgid", lambda: 0)()
