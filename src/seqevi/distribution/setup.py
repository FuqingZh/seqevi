"""Read-only managed setup planning for the first-party dbCAN kit."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from seqevi.errors import ProfileConfigurationError, ResourceLockError
from seqevi.execution_profile import (
    ExecutionProfile,
    load_execution_profile,
    named_profile_path,
)
from seqevi.resource_lock import ResourceLock, read_resource_lock

from .manifest import KitManifest, load_kit_manifest


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
        image_status=_inspect_image(manifest.image),
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
        smoke_reason="setup apply and ephemeral runtime smoke are Slice B",
        next_command=None,
        issues=tuple(issues),
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
