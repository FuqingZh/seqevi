"""Read-only managed setup planning for the first-party dbCAN kit."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from seqevi.errors import ProfileConfigurationError, ResourceLockError, SetupError
from seqevi.execution_profile import (
    ExecutionProfile,
    load_execution_profile,
    named_profile_path,
)
from seqevi.resource_lock import (
    ResourceComponent,
    ResourceLock,
    read_resource_lock,
    resolve_resource_lock,
)

from .manifest import KitManifest, load_kit_manifest

_SMOKE_RESOURCE_ROOT = "/mnt/seqevi-dbcan-resource"
_SMOKE_TIMEOUT_SECONDS = 120
_PULL_TIMEOUT_SECONDS = 900


@dataclass(frozen=True, slots=True)
class SetupComponentPlan:
    """Read-only inspection of one caller-owned resource component."""

    name: str
    path: str
    expected_size: int
    actual_size: int | None
    expected_sha256: str
    actual_sha256: str | None
    status: str


@dataclass(frozen=True, slots=True)
class SetupResourcePlan:
    """Resource inspection included in a setup plan."""

    path: Path | None
    status: str
    lock_path: Path | None
    components: tuple[SetupComponentPlan, ...]
    error: str | None = None


@dataclass(frozen=True, slots=True)
class SetupRuntimePlan:
    """Runtime image inspection included in a setup plan."""

    platform: str
    engine: str
    image: str
    kit_id: str
    dbcan_version: str
    diamond_version: str
    image_status: str


@dataclass(frozen=True, slots=True)
class SetupProfilePlan:
    """Named profile destination and conflict state."""

    name: str
    path: Path
    status: str


@dataclass(frozen=True, slots=True)
class SetupPlan:
    """Complete typed, read-only plan shared by human and JSON presenters."""

    schema_version: int
    adapter: str
    kit_id: str
    status: str
    runtime: SetupRuntimePlan
    resource: SetupResourcePlan
    profile: SetupProfilePlan
    actions: tuple[str, ...]
    smoke_status: str
    smoke_reason: str
    next_command: str | None
    issues: tuple[str, ...] = ()

    @property
    def ready_for_apply(self) -> bool:
        """Whether the plan has no known blocker for the later apply slice."""

        return self.status == "planned" and not self.issues

    def as_dict(self) -> dict[str, Any]:
        """Render the plan as one stable, secret-free JSON-compatible object."""

        return {
            "schema_version": self.schema_version,
            "adapter": self.adapter,
            "kit_id": self.kit_id,
            "status": self.status,
            "runtime": {
                "platform": self.runtime.platform,
                "engine": self.runtime.engine,
                "image": self.runtime.image,
                "kit_id": self.runtime.kit_id,
                "dbcan_version": self.runtime.dbcan_version,
                "diamond_version": self.runtime.diamond_version,
                "image_status": self.runtime.image_status,
            },
            "resource": {
                "path": str(self.resource.path)
                if self.resource.path is not None
                else None,
                "status": self.resource.status,
                "lock_path": (
                    str(self.resource.lock_path)
                    if self.resource.lock_path is not None
                    else None
                ),
                "error": self.resource.error,
                "components": [
                    {
                        "name": component.name,
                        "path": component.path,
                        "expected_size": component.expected_size,
                        "actual_size": component.actual_size,
                        "expected_sha256": component.expected_sha256,
                        "actual_sha256": component.actual_sha256,
                        "status": component.status,
                    }
                    for component in self.resource.components
                ],
            },
            "profile": {
                "name": self.profile.name,
                "path": str(self.profile.path),
                "status": self.profile.status,
            },
            "actions": list(self.actions),
            "smoke": {"status": self.smoke_status, "reason": self.smoke_reason},
            "next_command": self.next_command,
            "issues": list(self.issues),
        }


def build_setup_plan(
    kit_name: str,
    *,
    resource: str | Path | None = None,
    profile_name: str | None = None,
    environment: Mapping[str, str] | None = None,
    stdin_isatty: bool | None = None,
) -> SetupPlan:
    """Build a setup plan without pulling, hashing, writing or launching."""

    manifest = load_kit_manifest(kit_name)
    values = os.environ if environment is None else environment
    selected_name = kit_name if profile_name is None else profile_name
    profile_path = named_profile_path(selected_name, environment=values)
    issues: list[str] = []
    existing: ExecutionProfile | None = None
    profile_status = "absent"
    if profile_path.exists():
        try:
            existing = load_execution_profile(profile_path)
        except ProfileConfigurationError as error:
            profile_status = "invalid"
            issues.append(f"existing profile cannot be read: {error}")
        else:
            profile_status = "existing"
            if not _profile_matches_manifest(existing, manifest):
                profile_status = "conflict"
                issues.append(
                    "existing profile conflicts with the selected kit; use "
                    "--profile-name for a new managed profile"
                )

    selected_resource = _resolve_resource_input(
        resource=resource,
        existing=existing,
        profile_status=profile_status,
        stdin_isatty=(sys.stdin.isatty() if stdin_isatty is None else stdin_isatty),
    )
    if selected_resource is None:
        issues.append(
            "a first-run managed setup requires --resource in non-interactive mode"
        )
    elif existing is not None and profile_status == "existing":
        if existing.resource != selected_resource:
            profile_status = "conflict"
            issues.append(
                "the supplied resource differs from the existing profile; use "
                "--profile-name for a different resource"
            )

    resource_plan = _inspect_resource(selected_resource, manifest)
    if resource_plan.status in {"missing", "invalid"}:
        if resource_plan.error is not None:
            issues.append(resource_plan.error)
        else:
            issues.append(f"resource is not ready: {resource_plan.status}")

    runtime = SetupRuntimePlan(
        platform=manifest.platform,
        engine="docker",
        image=manifest.image,
        kit_id=manifest.kit_id,
        dbcan_version=manifest.dbcan_version,
        diamond_version=manifest.diamond_version,
        image_status=(
            _inspect_image(manifest.image)
            if selected_resource is not None
            else "not-checked"
        ),
    )
    profile = SetupProfilePlan(
        name=selected_name,
        path=profile_path,
        status=(
            "equal" if profile_status == "existing" and not issues else profile_status
        ),
    )
    actions = _actions(runtime, resource_plan, profile)
    status = "blocked" if issues else "planned"
    return SetupPlan(
        schema_version=1,
        adapter=manifest.adapter.value,
        kit_id=manifest.kit_id,
        status=status,
        runtime=runtime,
        resource=resource_plan,
        profile=profile,
        actions=actions,
        smoke_status="deferred",
        smoke_reason="setup apply will run the ephemeral runtime smoke",
        next_command=None,
        issues=tuple(issues),
    )


def apply_setup(plan: SetupPlan) -> SetupPlan:
    """Apply one previously inspected plan and publish its profile atomically."""

    if not plan.ready_for_apply:
        detail = "; ".join(plan.issues) or f"setup plan is {plan.status}"
        raise SetupError(f"setup plan is not ready to apply: {detail}")
    manifest = load_kit_manifest(plan.adapter)
    if manifest.kit_id != plan.kit_id or manifest.image != plan.runtime.image:
        raise SetupError("setup plan no longer matches the bundled managed kit")
    resource = plan.resource.path
    if resource is None:
        raise SetupError("setup plan has no caller-owned resource path")

    docker = shutil.which("docker")
    if docker is None:
        raise SetupError("Docker executable is not available on PATH")
    _ensure_image(docker, manifest, image=plan.runtime.image)
    lock = _verify_resource(resource, manifest, full=plan.resource.status != "ready")
    _run_smoke(docker, manifest, resource)
    profile_published = _publish_profile(plan, manifest)

    locked_by_path = {
        component.relative_path: component for component in lock.components
    }
    components = tuple(
        replace(
            component,
            actual_sha256=locked_by_path[component.path].sha256,
            status="ready",
        )
        for component in plan.resource.components
    )
    resource_plan = replace(plan.resource, status="ready", components=components)
    profile_plan = replace(
        plan.profile,
        status="published" if profile_published else "equal",
    )
    return replace(
        plan,
        status="applied",
        resource=resource_plan,
        profile=profile_plan,
        smoke_status="passed",
        smoke_reason="dbCAN runtime and caller-mounted read-only resource passed",
    )


def _resolve_resource_input(
    *,
    resource: str | Path | None,
    existing: ExecutionProfile | None,
    profile_status: str,
    stdin_isatty: bool,
) -> Path | None:
    if resource is not None:
        return Path(resource).expanduser().resolve()
    if existing is not None and profile_status == "existing" and existing.version == 2:
        return existing.resource
    # Prompting belongs to the CLI boundary.  The pure plan builder remains
    # deterministic and reports the missing first-run input instead.
    del stdin_isatty
    return None


def _profile_matches_manifest(profile: ExecutionProfile, manifest: KitManifest) -> bool:
    runtime = profile.runtime
    return (
        profile.version == 2
        and profile.adapter is manifest.adapter
        and runtime is not None
        and runtime.kind == "oci"
        and runtime.engine == "docker"
        and runtime.kit_id == manifest.kit_id
        and runtime.image == manifest.image
    )


def _inspect_resource(path: Path | None, manifest: KitManifest) -> SetupResourcePlan:
    if path is None:
        return SetupResourcePlan(None, "unresolved", None, ())
    lock_path = path / "seqevi.lock"
    if not path.exists():
        return SetupResourcePlan(
            path, "missing", lock_path, (), f"resource directory does not exist: {path}"
        )
    if not path.is_dir() or not os.access(path, os.R_OK):
        return SetupResourcePlan(
            path,
            "invalid",
            lock_path,
            (),
            f"resource is not a readable directory: {path}",
        )

    components: list[SetupComponentPlan] = []
    for declaration in manifest.components:
        component_path = path / declaration.path
        if not component_path.is_file():
            components.append(
                SetupComponentPlan(
                    declaration.name,
                    declaration.path,
                    declaration.size,
                    None,
                    declaration.sha256,
                    None,
                    "missing",
                )
            )
            continue
        actual_size = component_path.stat().st_size
        components.append(
            SetupComponentPlan(
                declaration.name,
                declaration.path,
                declaration.size,
                actual_size,
                declaration.sha256,
                None,
                "size-ok" if actual_size == declaration.size else "size-mismatch",
            )
        )
    if any(component.status != "size-ok" for component in components):
        return SetupResourcePlan(
            path,
            "invalid",
            lock_path,
            tuple(components),
            f"resource components do not match the dbCAN kit manifest: {path}",
        )

    try:
        lock = read_resource_lock(path)
    except ResourceLockError as error:
        return SetupResourcePlan(
            path, "invalid", lock_path, tuple(components), str(error)
        )
    if lock is None:
        return SetupResourcePlan(path, "needs-lock", lock_path, tuple(components))
    lock_error = _lock_mismatch(lock, manifest)
    if lock_error is not None:
        return SetupResourcePlan(
            path, "invalid", lock_path, tuple(components), lock_error
        )
    locked_by_path = {
        component.relative_path: component for component in lock.components
    }
    enriched = tuple(
        SetupComponentPlan(
            component.name,
            component.path,
            component.expected_size,
            component.actual_size,
            component.expected_sha256,
            locked_by_path[component.path].sha256,
            "ready",
        )
        for component in components
    )
    return SetupResourcePlan(path, "ready", lock_path, enriched)


def _lock_mismatch(lock: ResourceLock, manifest: KitManifest) -> str | None:
    if (lock.resource_name, lock.resource_version) != (
        manifest.resource_name,
        manifest.resource_version,
    ):
        return "seqevi.lock resource identity conflicts with the selected kit"
    expected = {
        component.path: (component.name, component.size, component.sha256)
        for component in manifest.components
    }
    actual = {
        component.relative_path: (component.name, component.size, component.sha256)
        for component in lock.components
    }
    if actual != expected:
        return "seqevi.lock components conflict with the selected kit manifest"
    return None


def _inspect_image(image: str) -> str:
    docker = shutil.which("docker")
    if docker is None:
        return "docker-unavailable"
    try:
        result = subprocess.run(
            [docker, "image", "inspect", image],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return "docker-unavailable"
    return "present" if result.returncode == 0 else "missing"


def _ensure_image(docker: str, manifest: KitManifest, *, image: str) -> None:
    inspected = _docker_run(
        docker,
        ("image", "inspect", image),
        timeout_seconds=30,
        action="inspect the managed runtime image",
    )
    if inspected.returncode != 0:
        pulled = _docker_run(
            docker,
            ("pull", "--platform", manifest.platform, image),
            timeout_seconds=_PULL_TIMEOUT_SECONDS,
            action="pull the managed runtime image",
        )
        if pulled.returncode != 0:
            raise SetupError(
                "Docker could not pull the exact managed runtime image: "
                + _process_detail(pulled)
            )
    verified = _docker_run(
        docker,
        ("image", "inspect", image),
        timeout_seconds=30,
        action="verify the managed runtime image",
    )
    if verified.returncode != 0:
        raise SetupError(
            "Docker did not retain the exact digest-pinned managed runtime image: "
            + _process_detail(verified)
        )


def _verify_resource(
    resource: Path, manifest: KitManifest, *, full: bool
) -> ResourceLock:
    declarations = tuple(
        ResourceComponent(component.name, component.path)
        for component in manifest.components
    )
    try:
        return resolve_resource_lock(
            database=resource,
            resource_name=manifest.resource_name,
            resource_version=manifest.resource_version,
            components=declarations,
            verify=full,
        )
    except ResourceLockError as error:
        raise SetupError(
            f"caller-owned dbCAN resource verification failed: {error}"
        ) from error


def _run_smoke(docker: str, manifest: KitManifest, resource: Path) -> None:
    expected = {component.path: component.size for component in manifest.components}
    script = "\n".join(
        (
            "from pathlib import Path",
            "import subprocess",
            f"root = Path({_SMOKE_RESOURCE_ROOT!r})",
            f"expected = {expected!r}",
            "version = subprocess.run(",
            "    ['/opt/dbcan-venv/bin/run_dbcan', 'version'], check=False",
            ")",
            "if version.returncode != 0:",
            "    raise SystemExit('dbCAN version probe failed')",
            "for name, size in expected.items():",
            "    path = root / name",
            "    if not path.is_file() or path.stat().st_size != size:",
            "        raise SystemExit(f'resource component failed: {name}')",
            "probe = root / '.seqevi-smoke-write-test'",
            "try:",
            "    probe.write_text('unexpected', encoding='utf-8')",
            "except OSError:",
            "    pass",
            "else:",
            "    probe.unlink(missing_ok=True)",
            "    raise SystemExit('resource mount is writable')",
            "print('seqevi dbCAN setup smoke passed')",
        )
    )
    _run_ephemeral_container(
        docker,
        image=manifest.image,
        resource=resource,
        command=("-c", script),
        entrypoint="/opt/venv/bin/python",
    )


def _run_ephemeral_container(
    docker: str,
    *,
    image: str,
    resource: Path,
    command: tuple[str, ...],
    entrypoint: str,
) -> None:
    container_name = f"seqevi-setup-smoke-{uuid.uuid4().hex[:16]}"
    uid = getattr(os, "getuid", lambda: 0)()
    gid = getattr(os, "getgid", lambda: 0)()
    mount = f"type=bind,source={resource},target={_SMOKE_RESOURCE_ROOT},readonly"
    created = _docker_run(
        docker,
        (
            "create",
            "--name",
            container_name,
            "--user",
            f"{uid}:{gid}",
            "--network",
            "none",
            "--mount",
            mount,
            "--entrypoint",
            entrypoint,
            image,
            *command,
        ),
        timeout_seconds=30,
        action="create the setup smoke container",
    )
    if created.returncode != 0:
        raise SetupError(
            "Docker could not create the setup smoke container: "
            + _process_detail(created)
        )

    failure: SetupError | None = None
    try:
        started = _docker_run(
            docker,
            ("start", "--attach", container_name),
            timeout_seconds=_SMOKE_TIMEOUT_SECONDS,
            action="run the setup smoke container",
        )
        if started.returncode != 0:
            failure = SetupError(
                "managed dbCAN setup smoke failed: " + _process_detail(started)
            )
    except SetupError as error:
        failure = error
    finally:
        try:
            removed = _docker_run(
                docker,
                ("rm", "--force", container_name),
                timeout_seconds=30,
                action="remove the setup smoke container",
            )
        except SetupError as error:
            removed = None
            if failure is None:
                failure = error
        if removed is not None and removed.returncode != 0 and failure is None:
            failure = SetupError(
                "Docker could not remove the setup smoke container: "
                + _process_detail(removed)
            )
    if failure is not None:
        raise failure


def _publish_profile(plan: SetupPlan, manifest: KitManifest) -> bool:
    destination = plan.profile.path
    resource = plan.resource.path
    if resource is None:
        raise SetupError("cannot publish a managed profile without a resource")
    if destination.exists():
        try:
            existing = load_execution_profile(destination)
        except ProfileConfigurationError as error:
            raise SetupError(f"existing profile cannot be read: {error}") from error
        if (
            not _profile_matches_manifest(existing, manifest)
            or existing.resource != resource
        ):
            raise SetupError(
                "existing profile conflicts with the selected managed kit; "
                "choose another --profile-name"
            )
        return False

    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise SetupError(f"cannot create profile directory: {error}") from error
    temporary: Path | None = None
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.stem}.",
            suffix=".tmp",
            dir=destination.parent,
            text=True,
        )
        temporary = Path(temporary_name)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(_profile_document(manifest, resource))
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, 0o600)
        try:
            os.link(temporary, destination)
        except FileExistsError as error:
            raise SetupError(
                "managed profile appeared during setup; rerun to inspect the conflict"
            ) from error
        _fsync_directory(destination.parent)
        return True
    except OSError as error:
        raise SetupError(
            f"cannot publish managed profile {destination}: {error}"
        ) from error
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass


def _profile_document(manifest: KitManifest, resource: Path) -> str:
    return (
        "version = 2\n"
        f"adapter = {_toml_string(manifest.adapter.value)}\n"
        f"resource = {_toml_string(str(resource))}\n\n"
        "[runtime]\n"
        'kind = "oci"\n'
        f"kit_id = {_toml_string(manifest.kit_id)}\n"
        'engine = "docker"\n'
        f"image = {_toml_string(manifest.image)}\n"
    )


def _toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def _fsync_directory(directory: Path) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _docker_run(
    docker: str,
    arguments: tuple[str, ...],
    *,
    timeout_seconds: float,
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
        raise SetupError(f"timed out while trying to {action}") from error
    except OSError as error:
        raise SetupError(f"could not {action}: {error}") from error


def _process_detail(result: subprocess.CompletedProcess[str]) -> str:
    detail = (result.stderr or result.stdout or "no diagnostic output").strip()
    return detail[-2000:]


def _actions(
    runtime: SetupRuntimePlan,
    resource: SetupResourcePlan,
    profile: SetupProfilePlan,
) -> tuple[str, ...]:
    actions = []
    if runtime.image_status == "present":
        actions.append("reuse the locally cached digest-pinned runtime image")
    else:
        actions.append("pull the exact digest-pinned runtime image during setup apply")
    if resource.status == "needs-lock":
        actions.append(
            "verify all four resource files and create seqevi.lock during setup apply"
        )
    elif resource.status == "ready":
        actions.append("reuse the matching read-only resource lock")
    else:
        actions.append("resolve and validate the caller-owned four-file resource")
    if profile.status == "absent":
        actions.append("atomically publish the complete v2 profile during setup apply")
    elif profile.status == "equal":
        actions.append("keep the existing equal profile unchanged")
    else:
        actions.append("repair or select a non-conflicting profile name")
    actions.append("run an ephemeral caller-mounted smoke during setup apply")
    return tuple(actions)
