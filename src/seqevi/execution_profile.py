"""Strict execution-profile loading for external annotation tools."""

from __future__ import annotations

import os
import math
import re
import shutil
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from .adapters import AdapterName
from .errors import ProfileConfigurationError

PROFILE_VERSION = 1
_PROFILE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_ENVIRONMENT_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_ROOT_KEYS = {
    "version",
    "adapter",
    "executable",
    "resource",
    "store",
    "threads",
    "timeout_seconds",
    "path_prepend",
    "environment",
}


@dataclass(frozen=True, slots=True)
class ExecutionProfile:
    """Resolved machine-local inputs for one official adapter."""

    source: Path
    adapter: AdapterName
    executable: Path
    resource: Path
    store: str | None = None
    threads: int | None = None
    timeout_seconds: float | None = None
    environment: tuple[tuple[str, str], ...] = ()

    @property
    def environment_overlay(self) -> dict[str, str]:
        """Return a fresh subprocess environment overlay."""

        return dict(self.environment)


def default_profile_directory(
    environment: Mapping[str, str] | None = None,
) -> Path:
    """Return the platform-conventional directory for named profiles."""

    values = os.environ if environment is None else environment
    config_home = values.get("XDG_CONFIG_HOME")
    if config_home:
        root = Path(config_home).expanduser()
    else:
        home = values.get("HOME")
        if not home:
            raise ProfileConfigurationError(
                "cannot resolve named profiles without HOME or XDG_CONFIG_HOME"
            )
        root = Path(home).expanduser() / ".config"
    return (root / "seqevi" / "profiles").resolve()


def named_profile_path(
    name: str,
    *,
    environment: Mapping[str, str] | None = None,
) -> Path:
    """Resolve a safe profile name below the conventional profile directory."""

    if _PROFILE_NAME.fullmatch(name) is None:
        raise ProfileConfigurationError(
            "profile name must start with an alphanumeric character and contain "
            "only letters, numbers, '.', '_' or '-'"
        )
    return default_profile_directory(environment) / f"{name}.toml"


def load_named_profile(
    name: str,
    *,
    environment: Mapping[str, str] | None = None,
) -> ExecutionProfile:
    """Load one profile selected by its stable user-facing name."""

    return load_execution_profile(named_profile_path(name, environment=environment))


def initialize_named_profile(
    name: str,
    adapter: AdapterName,
    *,
    environment: Mapping[str, str] | None = None,
) -> Path:
    """Create one complete named profile without replacing an existing file."""

    destination = named_profile_path(name, environment=environment)
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with destination.open("x", encoding="utf-8") as stream:
            stream.write(profile_example(adapter))
    except FileExistsError as error:
        raise ProfileConfigurationError(
            f"execution profile already exists: {destination}"
        ) from error
    except OSError as error:
        raise ProfileConfigurationError(
            f"cannot create execution profile {destination}: {error}"
        ) from error
    return destination


def list_named_profiles(
    *, environment: Mapping[str, str] | None = None
) -> tuple[str, ...]:
    """Return safe named profiles in deterministic lexical order."""

    directory = default_profile_directory(environment)
    try:
        names = tuple(
            entry.stem
            for entry in directory.iterdir()
            if entry.is_file()
            and entry.suffix == ".toml"
            and _PROFILE_NAME.fullmatch(entry.stem) is not None
        )
    except FileNotFoundError:
        return ()
    except OSError as error:
        raise ProfileConfigurationError(
            f"cannot list execution profiles in {directory}: {error}"
        ) from error
    return tuple(sorted(names))


def redacted_effective_configuration(profile: ExecutionProfile) -> str:
    """Render resolved profile inputs without subprocess environment values."""

    store = _redacted_store(profile.store)
    lines = [
        f"source: {profile.source}",
        f"adapter: {profile.adapter.value}",
        f"executable: {profile.executable}",
        f"resource: {profile.resource}",
        f"store: {store if store is not None else '(default)'}",
        f"threads: {profile.threads if profile.threads is not None else '(default)'}",
        (
            "timeout_seconds: "
            f"{profile.timeout_seconds if profile.timeout_seconds is not None else '(none)'}"
        ),
    ]
    environment_names = tuple(name for name, _value in profile.environment)
    lines.append(
        "environment_names: "
        + (", ".join(environment_names) if environment_names else "(none)")
    )
    return "\n".join(lines) + "\n"


def load_execution_profile(path: Path) -> ExecutionProfile:
    """Load and strictly validate one explicit execution-profile document."""

    source = path.expanduser().resolve()
    try:
        with source.open("rb") as stream:
            document = tomllib.load(stream)
    except FileNotFoundError as error:
        raise ProfileConfigurationError(
            f"execution profile does not exist: {source}"
        ) from error
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ProfileConfigurationError(
            f"cannot read execution profile {source}: {error}"
        ) from error

    unknown = sorted(set(document) - _ROOT_KEYS)
    if unknown:
        raise ProfileConfigurationError(
            f"execution profile contains unknown keys: {', '.join(unknown)}"
        )
    version = _required_integer(document, "version")
    if version != PROFILE_VERSION:
        raise ProfileConfigurationError(
            f"unsupported execution profile version {version}; "
            f"expected {PROFILE_VERSION}"
        )

    adapter_text = _required_string(document, "adapter")
    try:
        adapter = AdapterName(adapter_text)
    except ValueError as error:
        accepted = ", ".join(item.value for item in AdapterName)
        raise ProfileConfigurationError(
            f"unknown adapter {adapter_text!r}; expected one of: {accepted}"
        ) from error

    environment = _environment(document.get("environment"))
    path_prepend = _path_prepend(document.get("path_prepend"), source=source)
    if path_prepend:
        inherited_path = environment.get("PATH", os.environ.get("PATH", ""))
        environment["PATH"] = os.pathsep.join(
            (*map(str, path_prepend), inherited_path)
        ).rstrip(os.pathsep)
    executable = _resolve_executable(
        _required_string(document, "executable"),
        source=source,
        environment=environment,
    )
    resource = _profile_path(
        _required_string(document, "resource"),
        source=source,
    )
    if not resource.is_dir():
        raise ProfileConfigurationError(
            f"profile resource is not a readable directory: {resource}"
        )
    if not os.access(resource, os.R_OK):
        raise ProfileConfigurationError(
            f"profile resource is not a readable directory: {resource}"
        )

    store = _optional_string(document, "store")
    if store is not None and "://" not in store:
        store = str(_profile_path(store, source=source))
    threads = _optional_integer(document, "threads")
    if threads is not None and threads < 1:
        raise ProfileConfigurationError("profile threads must be at least 1")
    timeout_seconds = _optional_number(document, "timeout_seconds")
    if timeout_seconds is not None and (
        not math.isfinite(timeout_seconds) or timeout_seconds <= 0
    ):
        raise ProfileConfigurationError(
            "profile timeout_seconds must be finite and greater than zero"
        )

    return ExecutionProfile(
        source=source,
        adapter=adapter,
        executable=executable,
        resource=resource,
        store=store,
        threads=threads,
        timeout_seconds=timeout_seconds,
        environment=tuple(sorted(environment.items())),
    )


def profile_example(adapter: AdapterName) -> str:
    """Return a complete commented example for one official adapter."""

    if adapter is AdapterName.INTERPRO_PFAM:
        executable = "/opt/interproscan/interproscan.sh"
        resource = "/opt/interproscan/data"
        environment = 'path_prepend = ["/opt/jdk-17/bin"]\n'
    elif adapter is AdapterName.DBCAN_CAZYME:
        executable = "/opt/dbcan-5.2.9/bin/run_dbcan"
        resource = "/data/dbcan/db_v5-2-9_5-5-2026/raw"
        environment = ""
    else:
        executable = "/opt/eggnog-mapper/bin/emapper.py"
        resource = "/data/eggnog-5.0.2"
        environment = ""
    return (
        f"version = {PROFILE_VERSION}\n"
        f'adapter = "{adapter.value}"\n'
        f'executable = "{executable}"\n'
        f'resource = "{resource}"\n'
        'store = "/data/seqevi-store"\n'
        "threads = 8\n"
        "# timeout_seconds = 86400\n"
        f"{environment}"
    )


def _redacted_store(store: str | None) -> str | None:
    if store is None or "://" not in store:
        return store
    try:
        parsed = urlsplit(store)
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return f"{store.split('://', maxsplit=1)[0]}://<redacted>"
    if hostname is None:
        return f"{parsed.scheme}://<redacted>"
    host = f"[{hostname}]" if ":" in hostname else hostname
    if port is not None:
        host = f"{host}:{port}"
    return urlunsplit((parsed.scheme, host, parsed.path, "", ""))


def _required_string(document: Mapping[str, Any], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ProfileConfigurationError(
            f"execution profile key {key!r} must be a non-empty string"
        )
    if "\x00" in value:
        raise ProfileConfigurationError(
            f"execution profile key {key!r} contains a NUL byte"
        )
    return value


def _optional_string(document: Mapping[str, Any], key: str) -> str | None:
    if key not in document:
        return None
    return _required_string(document, key)


def _required_integer(document: Mapping[str, Any], key: str) -> int:
    value = document.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProfileConfigurationError(
            f"execution profile key {key!r} must be an integer"
        )
    return value


def _optional_integer(document: Mapping[str, Any], key: str) -> int | None:
    if key not in document:
        return None
    return _required_integer(document, key)


def _optional_number(document: Mapping[str, Any], key: str) -> float | None:
    if key not in document:
        return None
    value = document[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProfileConfigurationError(
            f"execution profile key {key!r} must be a number"
        )
    return float(value)


def _environment(value: object) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ProfileConfigurationError(
            "execution profile key 'environment' must be a TOML table"
        )
    environment: dict[str, str] = {}
    for name, item in value.items():
        if _ENVIRONMENT_NAME.fullmatch(name) is None:
            raise ProfileConfigurationError(
                f"invalid environment variable name in profile: {name!r}"
            )
        if not isinstance(item, str) or "\x00" in item:
            raise ProfileConfigurationError(
                f"profile environment value {name!r} must be a string without NUL"
            )
        environment[name] = item
    return environment


def _path_prepend(value: object, *, source: Path) -> tuple[Path, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not value:
        raise ProfileConfigurationError(
            "execution profile key 'path_prepend' must be a non-empty array"
        )
    resolved: list[Path] = []
    for item in value:
        if not isinstance(item, str) or not item or "\x00" in item:
            raise ProfileConfigurationError(
                "profile path_prepend entries must be non-empty strings without NUL"
            )
        directory = _profile_path(item, source=source)
        if not directory.is_dir():
            raise ProfileConfigurationError(
                f"profile path_prepend entry is not a directory: {directory}"
            )
        resolved.append(directory)
    return tuple(resolved)


def _profile_path(value: str, *, source: Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = source.parent / path
    return path.resolve()


def _resolve_executable(
    value: str,
    *,
    source: Path,
    environment: Mapping[str, str],
) -> Path:
    if Path(value).name != value:
        candidate = _profile_path(value, source=source)
        resolved = str(candidate) if candidate.is_file() else None
    else:
        resolved = shutil.which(value, path=environment.get("PATH"))
    if resolved is None or not os.access(resolved, os.X_OK):
        raise ProfileConfigurationError(
            f"profile executable was not found or is not executable: {value}"
        )
    return Path(resolved).resolve()
