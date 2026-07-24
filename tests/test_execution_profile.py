from __future__ import annotations

from pathlib import Path

import pytest

from seqevi.errors import ProfileConfigurationError
from seqevi.execution_profile import (
    default_profile_directory,
    initialize_named_profile,
    list_named_profiles,
    load_execution_profile,
    named_profile_path,
    redacted_effective_configuration,
)
from seqevi.adapters import AdapterName


def _write_executable(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    path.chmod(0o755)
    return path


def test_profile_loads_strict_relative_paths_and_environment(tmp_path: Path) -> None:
    executable = _write_executable(tmp_path / "bin" / "tool")
    runtime_bin = tmp_path / "runtime" / "bin"
    runtime_bin.mkdir(parents=True)
    resource = tmp_path / "resources" / "database"
    resource.mkdir(parents=True)
    profile = tmp_path / "profiles" / "eggnog.toml"
    profile.parent.mkdir()
    profile.write_text(
        "\n".join(
            (
                "version = 1",
                'adapter = "eggnog"',
                'executable = "../bin/tool"',
                'resource = "../resources/database"',
                'store = "../store"',
                "threads = 8",
                "timeout_seconds = 30.5",
                'path_prepend = ["../runtime/bin"]',
                "",
                "[environment]",
                'RUNTIME_MODE = "local"',
            )
        ),
        encoding="utf-8",
    )

    loaded = load_execution_profile(profile)

    assert loaded.adapter.value == "eggnog"
    assert loaded.executable == executable.resolve()
    assert loaded.resource == resource.resolve()
    assert loaded.store == str((tmp_path / "store").resolve())
    assert loaded.threads == 8
    assert loaded.timeout_seconds == 30.5
    assert loaded.environment_overlay["RUNTIME_MODE"] == "local"
    assert loaded.environment_overlay["PATH"].split(":")[0] == str(runtime_bin)


@pytest.mark.parametrize(
    ("extra", "message"),
    [
        ("unknown = true", "unknown keys"),
        ("threads = 0", "at least 1"),
        ("timeout_seconds = nan", "finite and greater than zero"),
        ("timeout_seconds = inf", "finite and greater than zero"),
        ('environment = "JAVA_HOME=/opt/jdk"', "must be a TOML table"),
        ('path_prepend = ["missing"]', "is not a directory"),
    ],
)
def test_profile_rejects_unknown_or_invalid_values(
    tmp_path: Path,
    extra: str,
    message: str,
) -> None:
    executable = _write_executable(tmp_path / "tool")
    resource = tmp_path / "resource"
    resource.mkdir()
    profile = tmp_path / "invalid.toml"
    profile.write_text(
        "\n".join(
            (
                "version = 1",
                'adapter = "eggnog"',
                f'executable = "{executable}"',
                f'resource = "{resource}"',
                extra,
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(ProfileConfigurationError, match=message):
        load_execution_profile(profile)


def test_named_profiles_use_xdg_config_home_and_reject_path_traversal(
    tmp_path: Path,
) -> None:
    environment = {"XDG_CONFIG_HOME": str(tmp_path / "config")}

    assert default_profile_directory(environment) == (
        tmp_path / "config" / "seqevi" / "profiles"
    )
    assert named_profile_path("interpro-pfam-38.1", environment=environment) == (
        tmp_path / "config" / "seqevi" / "profiles" / "interpro-pfam-38.1.toml"
    )
    with pytest.raises(ProfileConfigurationError, match="profile name"):
        named_profile_path("../escape", environment=environment)


def test_initialize_named_profile_is_complete_and_never_overwrites(
    tmp_path: Path,
) -> None:
    environment = {"XDG_CONFIG_HOME": str(tmp_path / "config")}

    created = initialize_named_profile(
        "eggnog-5.0.2", AdapterName.EGGNOG, environment=environment
    )

    contents = created.read_text(encoding="utf-8")
    assert 'adapter = "eggnog"' in contents
    assert 'executable = "/opt/eggnog-mapper/bin/emapper.py"' in contents
    assert 'resource = "/data/eggnog-5.0.2"' in contents
    assert "store =" in contents
    assert "threads =" in contents
    with pytest.raises(ProfileConfigurationError, match="already exists"):
        initialize_named_profile(
            "eggnog-5.0.2", AdapterName.INTERPRO_PFAM, environment=environment
        )
    assert created.read_text(encoding="utf-8") == contents


def test_list_named_profiles_is_sorted_and_ignores_non_profiles(
    tmp_path: Path,
) -> None:
    environment = {"XDG_CONFIG_HOME": str(tmp_path / "config")}
    directory = default_profile_directory(environment)
    directory.mkdir(parents=True)
    for name in ("zeta.toml", "Alpha.toml", "middle.toml", "notes.txt"):
        (directory / name).write_text("", encoding="utf-8")
    (directory / ".hidden.toml").write_text("", encoding="utf-8")
    (directory / "directory.toml").mkdir()

    assert list_named_profiles(environment=environment) == (
        "Alpha",
        "middle",
        "zeta",
    )


def test_effective_configuration_redacts_environment_and_store_secrets(
    tmp_path: Path,
) -> None:
    executable = _write_executable(tmp_path / "tool")
    resource = tmp_path / "resource"
    resource.mkdir()
    profile_path = tmp_path / "profile.toml"
    profile_path.write_text(
        "\n".join(
            (
                "version = 1",
                'adapter = "eggnog"',
                f'executable = "{executable}"',
                f'resource = "{resource}"',
                'store = "https://user:password@example.org/store?token=secret"',
                "",
                "[environment]",
                'API_TOKEN = "very-secret-value"',
            )
        ),
        encoding="utf-8",
    )

    shown = redacted_effective_configuration(load_execution_profile(profile_path))

    assert "environment_names: API_TOKEN" in shown
    assert "https://example.org/store" in shown
    for secret in ("very-secret-value", "password", "token=secret", "user"):
        assert secret not in shown
