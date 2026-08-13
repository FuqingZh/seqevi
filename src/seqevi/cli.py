"""SeqEvi command-line entrypoint."""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
from typing import Annotated, NoReturn

import typer

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
from .resource_lock import resource_lock_path

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
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit one machine-readable JSON result or error document.",
        ),
    ] = False,
) -> None:
    """Reuse exact evidence and publish one immutable DuckDB result."""

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
            adapter_factory=create_adapter,
        )
    except (AnnotationError, FastaValidationError, StoreError) as error:
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
                    },
                },
                sort_keys=True,
            )
        )
        return

    typer.echo(
        f"Annotated {summary.unique_sequences} unique sequences "
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
        "Setup plan (read-only; Slice A)",
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
            f"next_command: {plan.next_command or ('managed OCI annotation is Slice C' if plan.status == 'applied' else '(setup apply is available with --yes)')}",
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
    """Run the passive PostgreSQL/POSIX shared Store service."""

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
    """Run the acknowledgement-bound, fail-closed ClaimSession Store upgrade."""

    try:
        from sqlalchemy import create_engine

        from .store.migration import (
            MaintenanceAcknowledgement,
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
    typer.echo("Store maintenance upgrade completed at 0004_claim_sessions")


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
    """Run the acknowledgement-bound, fail-closed rollback to empty 0003."""

    try:
        from sqlalchemy import create_engine

        from .store.migration import (
            MaintenanceAcknowledgement,
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
    typer.echo("Store maintenance downgrade completed at 0003_evidence_claim_leases")


def main() -> None:
    """Run the SeqEvi CLI."""

    app()
