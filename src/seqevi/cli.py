"""SeqEvi command-line entrypoint."""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Literal, NoReturn, TextIO

import typer
from rich.console import Console, Group, RenderableType
from rich.live import Live
from rich.progress_bar import ProgressBar
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

from . import __version__
from .adapters import (
    AdapterConfiguration,
    AdapterName,
    create_adapter,
)
from .api import _run_annotation_application
from .distribution import SetupPlan, apply_setup, build_setup_plan
from .errors import AnnotationError, FastaValidationError, SetupError, StoreError
from .execution_profile import (
    initialize_named_profile,
    list_named_profiles,
    load_execution_profile,
    load_named_profile,
    profile_example,
    redacted_effective_configuration,
)
from .progress import ProgressEvent
from .resource_lock import resource_lock_path

_LOGGER = logging.getLogger(__name__)
_PROGRESS_REFRESH_SECONDS = 0.25
_FULL_PROGRESS_MIN_WIDTH = 80

app = typer.Typer(
    name="seqevi",
    help="Content-addressed protein sequence annotation evidence.",
    invoke_without_command=True,
    no_args_is_help=False,
)
resource_app = typer.Typer(
    name="resource",
    help="Initialize and verify immutable database resource locks.",
    no_args_is_help=True,
)
profile_app = typer.Typer(
    name="profile",
    help="Inspect and validate reusable external-tool execution profiles.",
    no_args_is_help=True,
)
app.add_typer(resource_app, name="resource")
app.add_typer(profile_app, name="profile")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


def _resolve_executable(value: str) -> Path:
    candidate = str(Path(value).expanduser()) if value.startswith("~") else value
    resolved = shutil.which(candidate)
    if resolved is None:
        raise typer.BadParameter(f"Executable not found or not executable: {value}")
    return Path(resolved).resolve()


class _ProgressRenderer:
    """Render one invocation in a transient Rich live region."""

    def __init__(
        self,
        *,
        stream: TextIO,
        refresh_seconds: float = _PROGRESS_REFRESH_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
        width: int | None = None,
    ) -> None:
        if refresh_seconds <= 0:
            raise ValueError("progress refresh interval must be positive")
        self._monotonic = monotonic
        self._started_at = monotonic()
        self._lock = threading.Lock()
        self._latest: ProgressEvent | None = None
        self._started = False
        self._closed = False
        self._spinner = Spinner("dots", style="cyan")
        self._console = Console(
            file=stream,
            stderr=True,
            force_terminal=True,
            force_interactive=True,
            highlight=False,
            width=width,
        )
        self._live = Live(
            console=self._console,
            get_renderable=self._render,
            refresh_per_second=1.0 / refresh_seconds,
            transient=True,
            redirect_stdout=False,
            redirect_stderr=False,
        )

    def __call__(self, event: ProgressEvent) -> None:
        with self._lock:
            if self._closed:
                return
            self._latest = event
            should_start = not self._started
            self._started = True
        if should_start:
            self._live.start(refresh=True)
        else:
            self._live.refresh()

    def close(self) -> None:
        """Stop refreshing and clear the transient region before final output."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            started = self._started
        if started:
            self._live.stop()

    def _render(self) -> RenderableType:
        with self._lock:
            event = self._latest
        if event is None:
            return Text("")
        return _render_progress_event(
            event,
            elapsed_seconds=max(0.0, self._monotonic() - self._started_at),
            width=self._console.width,
            spinner=self._spinner,
        )


def _render_progress_event(
    event: ProgressEvent,
    *,
    elapsed_seconds: float,
    width: int,
    spinner: Spinner | None = None,
) -> RenderableType:
    """Build a full or compact Rich view from one semantic event."""

    active_spinner = spinner or Spinner("dots", style="cyan")
    elapsed = f"{_format_elapsed(elapsed_seconds)} elapsed"
    ready = event.evidence_ready
    show_ratio = ready is not None and ready.completed < ready.total

    if width < _FULL_PROGRESS_MIN_WIDTH:
        compact = Table.grid(expand=True, padding=(0, 1))
        compact.add_column(width=1)
        compact.add_column(ratio=1, no_wrap=True, overflow="ellipsis")
        if show_ratio:
            assert ready is not None
            compact.add_column(justify="right", no_wrap=True)
            compact.add_column(justify="right", no_wrap=True)
        compact.add_column(justify="right", no_wrap=True)
        row: list[RenderableType] = [
            active_spinner,
            Text(event.message),
        ]
        if show_ratio:
            assert ready is not None
            row.extend(
                (
                    Text(f"{ready.completed:,}/{ready.total:,}"),
                    Text(f"{_percentage(ready.completed, ready.total):.0f}%"),
                )
            )
        row.append(Text(elapsed, style="dim"))
        compact.add_row(*row)
        return compact

    header = Table.grid(expand=True, padding=(0, 1))
    header.add_column(width=1)
    header.add_column(ratio=1, no_wrap=True, overflow="ellipsis")
    header.add_column(justify="right", no_wrap=True)
    message = event.message
    if event.batch is not None:
        message = f"{message} · {event.batch.size:,} sequences"
    header.add_row(
        active_spinner,
        Text(message),
        Text(elapsed, style="dim"),
    )
    if not show_ratio:
        return header

    assert ready is not None
    progress = Table.grid(expand=True, padding=(0, 1))
    progress.add_column(no_wrap=True)
    progress.add_column(ratio=1)
    progress.add_column(justify="right", no_wrap=True)
    progress.add_column(justify="right", no_wrap=True)
    progress.add_row(
        Text("Unique sequences", style="dim"),
        ProgressBar(total=ready.total, completed=ready.completed),
        Text(f"{ready.completed:,}/{ready.total:,}"),
        Text(f"{_percentage(ready.completed, ready.total):.0f}%"),
    )
    return Group(header, progress)


def _percentage(completed: int, total: int) -> float:
    return 100.0 if total == 0 else completed / total * 100


def _format_elapsed(elapsed_seconds: float) -> str:
    seconds = int(max(0.0, elapsed_seconds))
    hours, remainder = divmod(seconds, 3_600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{seconds:02d}s"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def _stderr_is_interactive() -> bool:
    return sys.stderr.isatty() and os.environ.get("TERM", "").lower() != "dumb"


def _close_progress_renderer(renderer: _ProgressRenderer | None) -> None:
    if renderer is None:
        return
    try:
        renderer.close()
    except Exception:
        _LOGGER.exception("annotation progress renderer shutdown failed")


@app.callback()
def root(
    context: typer.Context,
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show the SeqEvi version and exit.",
        ),
    ] = False,
) -> None:
    """Run SeqEvi commands or show top-level help."""

    del version
    if context.invoked_subcommand is None:
        typer.echo(context.get_help())


@app.command("annotate")
def annotate_command(
    fasta: Annotated[
        Path,
        typer.Option(
            "--fasta",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Protein FASTA to annotate.",
        ),
    ],
    output: Annotated[
        Path,
        typer.Option(
            "-o",
            "--output",
            file_okay=True,
            dir_okay=False,
            writable=True,
            resolve_path=True,
            help="New DuckDB result file (normally ending in .duckdb).",
        ),
    ],
    adapter: Annotated[
        AdapterName | None,
        typer.Option("--adapter", help="Official annotation adapter to run."),
    ] = None,
    executable: Annotated[
        Path | None,
        typer.Option(
            "--executable",
            parser=_resolve_executable,
            metavar="EXECUTABLE",
            help="Adapter tool executable or command name.",
        ),
    ] = None,
    resource: Annotated[
        Path | None,
        typer.Option(
            "--resource",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
            help="Native upstream annotation resource directory.",
        ),
    ] = None,
    profile: Annotated[
        str | None,
        typer.Option(
            "--profile",
            help="Named profile under the SeqEvi user configuration directory.",
        ),
    ] = None,
    config: Annotated[
        Path | None,
        typer.Option(
            "--config",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Explicit execution-profile TOML file.",
        ),
    ] = None,
    store: Annotated[
        str | None,
        typer.Option(
            "--store",
            envvar="SEQEVI_STORE",
            help="Local Store path or shared Store HTTP(S) URL.",
        ),
    ] = None,
    oci_registry_config: Annotated[
        Path | None,
        typer.Option(
            "--oci-registry-config",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Invocation-only ORAS registry config for an OCI-backed shared Store.",
        ),
    ] = None,
    oci_registry_ca_file: Annotated[
        Path | None,
        typer.Option(
            "--oci-registry-ca-file",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Invocation-only PEM trust anchor for an OCI registry.",
        ),
    ] = None,
    timeout_seconds: Annotated[
        float | None,
        typer.Option(
            "--timeout-seconds",
            min=0.001,
            help="Optional external tool timeout in seconds.",
        ),
    ] = None,
    threads: Annotated[
        int | None,
        typer.Option(
            "--threads",
            min=1,
            help="Worker threads; overrides the profile default.",
        ),
    ] = None,
    progress: Annotated[
        bool,
        typer.Option(
            "--progress/--no-progress",
            help="Show dynamic annotation progress on capable interactive stderr.",
        ),
    ] = True,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit one machine-readable JSON result or error document.",
        ),
    ] = False,
) -> None:
    """Reuse exact evidence and publish one immutable DuckDB result."""

    invocation_started = time.monotonic()
    renderer = (
        _ProgressRenderer(stream=sys.stderr)
        if progress and not json_output and _stderr_is_interactive()
        else None
    )
    try:
        invocation = _run_annotation_application(
            fasta=fasta,
            output=output,
            profile=profile,
            config=config,
            adapter=adapter,
            executable=executable,
            resource=resource,
            store=store,
            threads=threads,
            timeout_seconds=timeout_seconds,
            oci_registry_config=oci_registry_config,
            oci_registry_ca_file=oci_registry_ca_file,
            adapter_factory=create_adapter,
            progress_sink=renderer,
        )
    except (AnnotationError, FastaValidationError, StoreError) as error:
        _close_progress_renderer(renderer)
        renderer = None
        if json_output:
            typer.echo(
                json.dumps(
                    {
                        "schema_version": 1,
                        "status": "error",
                        "error_type": type(error).__name__,
                        "error": str(error),
                    },
                    sort_keys=True,
                ),
                err=True,
            )
        else:
            typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    finally:
        _close_progress_renderer(renderer)

    summary = invocation.summary
    if json_output:
        typer.echo(
            json.dumps(
                {
                    "schema_version": 1,
                    "status": "ok",
                    "adapter": invocation.adapter,
                    "result_schema": invocation.result_schema_id,
                    "counts": {
                        "input_records": summary.input_records,
                        "unique_sequences": summary.unique_sequences,
                        "cache_hits": summary.cache_hits,
                        "computed": summary.computed,
                        "hits": summary.hits,
                        "no_hits": summary.no_hits,
                    },
                    "output": str(summary.output_dir),
                    "metrics": {
                        "elapsed_seconds": summary.metrics.elapsed_seconds,
                        "package_seconds": summary.metrics.package_seconds,
                        "configured_threads": summary.metrics.configured_threads,
                        "existing_finalizations": (
                            summary.metrics.existing_finalizations
                        ),
                    },
                },
                sort_keys=True,
            )
        )
        return

    typer.echo(
        f"✓ Annotated {summary.unique_sequences} unique sequences "
        f"in {_format_elapsed(time.monotonic() - invocation_started)} "
        f"({summary.cache_hits} cached, {summary.computed} computed); "
        f"output: {summary.output_dir}"
    )


@app.command("setup")
def setup_command(
    kit: Annotated[
        str,
        typer.Argument(help="Managed kit to plan (currently dbcan-cazyme)."),
    ],
    resource: Annotated[
        Path | None,
        typer.Option(
            "--resource",
            metavar="PATH",
            help="Caller-owned database root; required on first non-interactive setup.",
        ),
    ] = None,
    profile_name: Annotated[
        str | None,
        typer.Option(
            "--profile-name",
            help="Named v2 profile to inspect or publish (default: kit name).",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Build and display a read-only setup plan; never mutate state.",
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            help="Apply the validated setup plan without confirmation.",
        ),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit the same setup plan as one JSON document.",
        ),
    ] = False,
) -> None:
    """Plan managed adapter setup without installing tools or databases."""

    if resource is None and not json_output and sys.stdin.isatty():
        resource_text = typer.prompt("Caller-owned dbCAN resource directory")
        resource = Path(resource_text)
    try:
        plan = build_setup_plan(
            kit,
            resource=resource,
            profile_name=profile_name,
            stdin_isatty=sys.stdin.isatty(),
        )
    except (AnnotationError, SetupError) as error:
        _emit_setup_error(error, json_output=json_output)

    if dry_run or not plan.ready_for_apply:
        if json_output:
            typer.echo(json.dumps(plan.as_dict(), sort_keys=True))
        else:
            typer.echo(_render_setup_plan(plan), nl=False)
        if not plan.ready_for_apply:
            raise typer.Exit(code=1)
        return

    if not yes and not sys.stdin.isatty():
        _emit_setup_error(
            SetupError("non-interactive setup apply requires --yes"),
            json_output=json_output,
        )
    if not json_output:
        typer.echo(_render_setup_plan(plan), nl=False)
    if not yes:
        confirmed = typer.confirm(
            "Apply this setup plan?",
            default=False,
            err=json_output,
        )
        if not confirmed:
            raise typer.Exit(code=1)
    try:
        applied = apply_setup(plan)
    except (AnnotationError, SetupError) as error:
        _emit_setup_error(error, json_output=json_output)

    if json_output:
        typer.echo(json.dumps(applied.as_dict(), sort_keys=True))
    else:
        typer.echo(_render_setup_plan(applied), nl=False)
    if applied.status != "applied":
        raise typer.Exit(code=1)


def _emit_setup_error(error: AnnotationError, *, json_output: bool) -> NoReturn:
    if json_output:
        typer.echo(
            json.dumps(
                {
                    "schema_version": 1,
                    "status": "error",
                    "error_type": type(error).__name__,
                    "error": str(error),
                },
                sort_keys=True,
            ),
            err=True,
        )
    else:
        typer.echo(f"Error: {error}", err=True)
    raise typer.Exit(code=1) from error


def _render_setup_plan(plan: SetupPlan) -> str:
    """Render the typed plan without progress output or secret material."""

    lines = [
        "Setup plan (read-only)",
        f"status: {plan.status}",
        f"adapter: {plan.adapter}",
        f"kit_id: {plan.kit_id}",
        "runtime:",
        f"  platform: {plan.runtime.platform}",
        f"  engine: {plan.runtime.engine}",
        f"  image: {plan.runtime.image}",
        f"  image_status: {plan.runtime.image_status}",
        f"  dbCAN: {plan.runtime.dbcan_version}",
        f"  DIAMOND: {plan.runtime.diamond_version}",
        "resource:",
        f"  path: {plan.resource.path if plan.resource.path is not None else '(unresolved)'}",
        f"  status: {plan.resource.status}",
        f"  lock: {plan.resource.lock_path if plan.resource.lock_path is not None else '(none)'}",
        "profile:",
        f"  name: {plan.profile.name}",
        f"  path: {plan.profile.path}",
        f"  status: {plan.profile.status}",
        "actions:",
    ]
    lines.extend(f"  - {action}" for action in plan.actions)
    lines.extend(
        (
            f"smoke: {plan.smoke_status} ({plan.smoke_reason})",
            "next_command: "
            + (
                plan.next_command
                or (
                    "seqevi annotate "
                    f"--profile {plan.profile.name} "
                    "--fasta FASTA --output RESULT.duckdb"
                    if plan.status == "applied"
                    else "(setup apply is available with --yes)"
                )
            ),
        )
    )
    if plan.issues:
        lines.append("issues:")
        lines.extend(f"  - {issue}" for issue in plan.issues)
    return "\n".join(lines) + "\n"


@profile_app.command("example")
def profile_example_command(
    adapter: Annotated[
        AdapterName,
        typer.Option("--adapter", help="Official adapter for the example profile."),
    ],
) -> None:
    """Print a complete execution-profile TOML example."""

    typer.echo(profile_example(adapter), nl=False)


@profile_app.command("validate")
def profile_validate_command(
    config: Annotated[
        Path,
        typer.Option(
            "--config",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Execution-profile TOML file to validate.",
        ),
    ],
) -> None:
    """Validate one profile without launching its annotation tool."""

    try:
        validated = load_execution_profile(config)
    except AnnotationError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    typer.echo(
        f"Valid {validated.adapter.value} profile: {validated.source}; "
        f"resource: {validated.resource}"
    )


@profile_app.command("init")
def profile_init_command(
    name: Annotated[str, typer.Argument(help="Name for the new user profile.")],
    adapter: Annotated[
        AdapterName,
        typer.Option("--adapter", help="Official adapter for the new profile."),
    ],
) -> None:
    """Create a complete named profile without overwriting an existing file."""

    try:
        destination = initialize_named_profile(name, adapter)
    except AnnotationError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    typer.echo(f"Created {adapter.value} profile: {destination}")


@profile_app.command("list")
def profile_list_command() -> None:
    """List named profiles in deterministic order."""

    try:
        names = list_named_profiles()
    except AnnotationError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    for name in names:
        typer.echo(name)


@profile_app.command("show")
def profile_show_command(
    name: Annotated[str, typer.Argument(help="Named user profile to inspect.")],
) -> None:
    """Show resolved profile configuration with environment values redacted."""

    try:
        selected = load_named_profile(name)
    except AnnotationError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    typer.echo(redacted_effective_configuration(selected), nl=False)


@resource_app.command("verify")
def verify_resource_command(
    adapter: Annotated[
        AdapterName,
        typer.Option("--adapter", help="Official annotation adapter to verify."),
    ],
    executable: Annotated[
        Path,
        typer.Option(
            "--executable",
            parser=_resolve_executable,
            metavar="EXECUTABLE",
            help="Adapter tool executable or command name.",
        ),
    ],
    resource: Annotated[
        Path,
        typer.Option(
            "--resource",
            exists=True,
            file_okay=False,
            dir_okay=True,
            readable=True,
            resolve_path=True,
            help="Native upstream annotation resource directory.",
        ),
    ],
) -> None:
    """Fully hash a database and verify its SeqEvi resource lock."""

    try:
        configured_adapter = create_adapter(
            AdapterConfiguration(
                name=adapter,
                executable=executable,
                database=resource,
                verify_resource=True,
            )
        )
    except AnnotationError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error

    lock_path = resource_lock_path(resource)
    lock_status = str(lock_path) if lock_path.is_file() else "not persisted (read-only)"
    typer.echo(
        f"Verified resource {configured_adapter.contract.resource_id}; "
        f"lock: {lock_status}"
    )


@app.command("serve")
def serve_command(
    database_url: Annotated[
        str,
        typer.Option(
            "--database-url",
            envvar="SEQEVI_DATABASE_URL",
            help="PostgreSQL SQLAlchemy URL for shared metadata.",
        ),
    ],
    artifacts_dir: Annotated[
        Path,
        typer.Option(
            "--artifacts-dir",
            envvar="SEQEVI_ARTIFACTS_DIR",
            file_okay=False,
            dir_okay=True,
            writable=True,
            resolve_path=True,
            help="Writable POSIX directory for content-addressed artifacts.",
        ),
    ],
    artifact_backend: Annotated[
        Literal["legacy-posix", "oci-registry"],
        typer.Option(
            "--artifact-backend",
            envvar="SEQEVI_ARTIFACT_BACKEND",
            help="Artifact storage backend for new shared artifacts.",
        ),
    ] = "legacy-posix",
    oci_registry_id: Annotated[
        str | None,
        typer.Option("--oci-registry-id", envvar="SEQEVI_OCI_REGISTRY_ID"),
    ] = None,
    oci_registry_endpoint: Annotated[
        str | None,
        typer.Option("--oci-registry-endpoint", envvar="SEQEVI_OCI_REGISTRY_ENDPOINT"),
    ] = None,
    oci_registry_repository: Annotated[
        str | None,
        typer.Option(
            "--oci-registry-repository",
            envvar="SEQEVI_OCI_REGISTRY_REPOSITORY",
        ),
    ] = None,
    oci_oras_executable: Annotated[
        Path | None,
        typer.Option(
            "--oci-oras-executable",
            envvar="SEQEVI_OCI_ORAS_EXECUTABLE",
            file_okay=True,
            dir_okay=False,
        ),
    ] = None,
    oci_registry_config: Annotated[
        Path | None,
        typer.Option(
            "--oci-registry-config",
            envvar="SEQEVI_OCI_REGISTRY_CONFIG",
            file_okay=True,
            dir_okay=False,
        ),
    ] = None,
    oci_registry_ca_file: Annotated[
        Path | None,
        typer.Option(
            "--oci-registry-ca-file",
            envvar="SEQEVI_OCI_REGISTRY_CA_FILE",
            file_okay=True,
            dir_okay=False,
        ),
    ] = None,
    host: Annotated[
        str,
        typer.Option("--host", help="HTTP bind address."),
    ] = "127.0.0.1",
    port: Annotated[
        int,
        typer.Option("--port", min=1, max=65535, help="HTTP bind port."),
    ] = 8000,
    maximum_batch_size: Annotated[
        int,
        typer.Option("--maximum-batch-size", min=1, max=10000),
    ] = 1000,
    maximum_artifact_bytes: Annotated[
        int,
        typer.Option("--maximum-artifact-bytes", min=1),
    ] = 512 * 1024 * 1024,
) -> None:
    """Run the passive PostgreSQL shared Store service."""

    try:
        import uvicorn

        from .service import (
            ServiceSettings,
            configure_claim_logging,
            create_service_app,
        )

        settings = ServiceSettings(
            database_url=database_url,
            artifacts_dir=artifacts_dir,
            artifact_backend=artifact_backend,
            oci_registry_id=oci_registry_id,
            oci_registry_endpoint=oci_registry_endpoint,
            oci_registry_repository=oci_registry_repository,
            oci_oras_executable=oci_oras_executable,
            oci_registry_config=oci_registry_config,
            oci_registry_ca_file=oci_registry_ca_file,
            maximum_batch_size=maximum_batch_size,
            maximum_artifact_bytes=maximum_artifact_bytes,
        )
        configure_claim_logging()
        uvicorn.run(create_service_app(settings), host=host, port=port)
    except ImportError as error:
        typer.echo(
            "Error: shared Store dependencies are missing; install seqevi[server]",
            err=True,
        )
        raise typer.Exit(code=1) from error


@app.command("store-maintenance-upgrade")
def store_maintenance_upgrade_command(
    database_url: Annotated[
        str,
        typer.Option("--database-url", envvar="SEQEVI_DATABASE_URL"),
    ],
    acknowledge_database: Annotated[
        str,
        typer.Option("--acknowledge-database"),
    ],
    acknowledge_revision: Annotated[
        str,
        typer.Option("--acknowledge-revision"),
    ] = "0003_evidence_claim_leases",
    store_root: Annotated[
        Path | None,
        typer.Option("--store-root", file_okay=False, resolve_path=True),
    ] = None,
) -> None:
    """Run the acknowledgement-bound, fail-closed Store schema upgrade."""

    try:
        from sqlalchemy import create_engine

        from .store.migration import (
            MaintenanceAcknowledgement,
            _maintenance_upgrade_target,
            maintenance_upgrade_database,
        )

        from .service.config import _normalize_postgres_database_url

        engine = create_engine(
            _normalize_postgres_database_url(database_url)
            if database_url.startswith("postgresql")
            else database_url
        )
        try:
            maintenance_upgrade_database(
                engine,
                store_root,
                MaintenanceAcknowledgement(
                    database_identity=acknowledge_database,
                    expected_revision=acknowledge_revision,
                ),
            )
        finally:
            engine.dispose()
    except Exception as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    typer.echo(
        f"Store maintenance upgrade completed at {_maintenance_upgrade_target(acknowledge_revision)}"
    )


@app.command("store-maintenance-prepare")
def store_maintenance_prepare_command(
    database_url: Annotated[
        str, typer.Option("--database-url", envvar="SEQEVI_DATABASE_URL")
    ],
    acknowledge_database: Annotated[str, typer.Option("--acknowledge-database")],
    acknowledge_revision: Annotated[
        str, typer.Option("--acknowledge-revision")
    ] = "0002_artifact_byte_size_bigint",
    store_root: Annotated[
        Path | None,
        typer.Option("--store-root", file_okay=False, resolve_path=True),
    ] = None,
) -> None:
    """Prepare an acknowledged 0002 Store at the automatic 0003 ceiling."""

    _run_store_preparation(
        database_url,
        acknowledge_database,
        acknowledge_revision,
        store_root,
        rollback=False,
    )
    typer.echo("Store maintenance preparation completed at 0003_evidence_claim_leases")


@app.command("store-maintenance-prepare-rollback")
def store_maintenance_prepare_rollback_command(
    database_url: Annotated[
        str, typer.Option("--database-url", envvar="SEQEVI_DATABASE_URL")
    ],
    acknowledge_database: Annotated[str, typer.Option("--acknowledge-database")],
    acknowledge_revision: Annotated[
        str, typer.Option("--acknowledge-revision")
    ] = "0003_evidence_claim_leases",
    store_root: Annotated[
        Path | None,
        typer.Option("--store-root", file_okay=False, resolve_path=True),
    ] = None,
) -> None:
    """Roll an acknowledged preparation-only 0003 Store back to 0002."""

    _run_store_preparation(
        database_url,
        acknowledge_database,
        acknowledge_revision,
        store_root,
        rollback=True,
    )
    typer.echo(
        "Store maintenance preparation rollback completed at 0002_artifact_byte_size_bigint"
    )


def _run_store_preparation(
    database_url: str,
    acknowledge_database: str,
    acknowledge_revision: str,
    store_root: Path | None,
    *,
    rollback: bool,
) -> None:
    try:
        from sqlalchemy import create_engine

        from .store.migration import (
            MaintenanceAcknowledgement,
            maintenance_prepare_database,
        )

        from .service.config import _normalize_postgres_database_url

        engine = create_engine(
            _normalize_postgres_database_url(database_url)
            if database_url.startswith("postgresql")
            else database_url
        )
        try:
            maintenance_prepare_database(
                engine,
                store_root,
                MaintenanceAcknowledgement(
                    database_identity=acknowledge_database,
                    expected_revision=acknowledge_revision,
                ),
                rollback=rollback,
            )
        finally:
            engine.dispose()
    except Exception as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error


@app.command("store-maintenance-downgrade")
def store_maintenance_downgrade_command(
    database_url: Annotated[
        str, typer.Option("--database-url", envvar="SEQEVI_DATABASE_URL")
    ],
    acknowledge_database: Annotated[str, typer.Option("--acknowledge-database")],
    acknowledge_revision: Annotated[
        str, typer.Option("--acknowledge-revision")
    ] = "0004_claim_sessions",
    store_root: Annotated[
        Path | None,
        typer.Option("--store-root", file_okay=False, resolve_path=True),
    ] = None,
) -> None:
    """Run one acknowledgement-bound, fail-closed Store schema rollback."""

    try:
        from sqlalchemy import create_engine

        from .store.migration import (
            MaintenanceAcknowledgement,
            _maintenance_downgrade_target,
            maintenance_downgrade_database,
        )

        from .service.config import _normalize_postgres_database_url

        engine = create_engine(
            _normalize_postgres_database_url(database_url)
            if database_url.startswith("postgresql")
            else database_url
        )
        try:
            maintenance_downgrade_database(
                engine,
                store_root,
                MaintenanceAcknowledgement(
                    database_identity=acknowledge_database,
                    expected_revision=acknowledge_revision,
                ),
            )
        finally:
            engine.dispose()
    except Exception as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(code=1) from error
    typer.echo(
        "Store maintenance downgrade completed at "
        f"{_maintenance_downgrade_target(acknowledge_revision)}"
    )


def main() -> None:
    """Run the SeqEvi CLI."""

    app()
