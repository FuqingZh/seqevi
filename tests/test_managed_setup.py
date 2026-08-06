from __future__ import annotations

import json
import tomllib
from click import unstyle
from importlib.resources import files
from pathlib import Path

import pytest
from typer.testing import CliRunner

import seqevi.distribution.setup as setup_impl
from seqevi.cli import app
from seqevi.distribution.manifest import (
    load_kit_manifest,
    parse_kit_manifest,
)
from seqevi.api import resolve_annotation_inputs
from seqevi.errors import AnnotationError, ProfileConfigurationError, SetupError
from seqevi.execution_profile import load_execution_profile

runner = CliRunner()


def test_dbcan_kit_manifest_is_hash_and_digest_locked() -> None:
    manifest = load_kit_manifest("dbcan-cazyme")

    assert manifest.kit_id == "dbcan-cazyme-5.2.9-db-2026.05.05"
    assert manifest.adapter.value == "dbcan-cazyme"
    assert manifest.platform == "linux/amd64"
    assert manifest.image.endswith(
        "@sha256:57976d498711ba83835485d2621b60ad4c49fcd08ab13b5bdb9105977f7b4f66"
    )
    assert [(component.name, component.path) for component in manifest.components] == [
        ("CAZy-diamond", "CAZy.dmnd"),
        ("dbCAN-HMM", "dbCAN.hmm"),
        ("dbCAN-sub-HMM", "dbCAN-sub.hmm"),
        ("fam-substrate-mapping", "fam-substrate-mapping.tsv"),
    ]


def test_manifest_parser_rejects_unknown_and_mutable_inputs() -> None:
    manifest = {
        "schema_version": 1,
        "kit_id": "dbcan-cazyme-5.2.9-db-2026.05.05",
        "adapter": "dbcan-cazyme",
        "platform": "linux/amd64",
        "dbcan_version": "5.2.9",
        "diamond_version": "2.1.15",
        "image": "ghcr.io/fuqingzh/seqevi-dbcan:latest",
        "resource": {
            "name": "dbcan",
            "version": "db_v5-2-9_5-5-2026",
            "component": [],
        },
        "extra": True,
    }

    with pytest.raises(SetupError, match="unknown keys"):
        parse_kit_manifest(manifest)
    valid = tomllib.loads(
        files("seqevi.kits").joinpath("dbcan-cazyme.toml").read_text(encoding="utf-8")
    )
    valid["image"] = "ghcr.io/fuqingzh/seqevi-dbcan:latest"
    with pytest.raises(SetupError, match="immutable sha256"):
        parse_kit_manifest(valid)


def _write_v2_profile(path: Path, resource: Path, **extra: str) -> None:
    lines = [
        "version = 2",
        'adapter = "dbcan-cazyme"',
        f'resource = "{resource}"',
        "threads = 8",
        "",
        "[runtime]",
        'kind = "oci"',
        'kit_id = "dbcan-cazyme-5.2.9-db-2026.05.05"',
        'engine = "docker"',
        'image = "ghcr.io/fuqingzh/seqevi-dbcan@sha256:57976d498711ba83835485d2621b60ad4c49fcd08ab13b5bdb9105977f7b4f66"',
    ]
    lines.extend(f"{key} = {value!r}" for key, value in extra.items())
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_profile_v2_loads_without_reinterpreting_v1_fields(tmp_path: Path) -> None:
    resource = tmp_path / "resource"
    resource.mkdir()
    profile = tmp_path / "managed.toml"
    _write_v2_profile(profile, resource)

    loaded = load_execution_profile(profile)

    assert loaded.version == 2
    assert loaded.executable is None
    assert loaded.runtime is not None
    assert loaded.runtime.engine == "docker"
    assert loaded.resource == resource.resolve()

    profile.write_text(
        profile.read_text(encoding="utf-8").replace(
            "\n[runtime]", '\nexecutable = "/bin/true"\n\n[runtime]'
        ),
        encoding="utf-8",
    )
    with pytest.raises(ProfileConfigurationError, match="unknown keys"):
        load_execution_profile(profile)


def test_annotation_path_keeps_v2_read_only_until_oci_slice(tmp_path: Path) -> None:
    resource = tmp_path / "resource"
    resource.mkdir()
    profile = tmp_path / "managed.toml"
    _write_v2_profile(profile, resource)

    with pytest.raises(AnnotationError, match="managed execution profile v2"):
        resolve_annotation_inputs(
            profile=None,
            config=profile,
            adapter=None,
            executable=None,
            resource=None,
            store=None,
            threads=None,
            timeout_seconds=None,
        )


def test_setup_json_dry_run_is_read_only_and_does_not_pull(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_home = tmp_path / "config"
    resource = tmp_path / "missing-resource"
    monkeypatch.setattr(setup_impl.shutil, "which", lambda _name: None)

    result = runner.invoke(
        app,
        [
            "setup",
            "dbcan-cazyme",
            "--resource",
            str(resource),
            "--dry-run",
            "--json",
        ],
        env={"XDG_CONFIG_HOME": str(config_home)},
    )

    payload = json.loads(result.stdout)
    assert result.exit_code == 1
    assert payload["status"] == "blocked"
    assert payload["runtime"]["image_status"] == "docker-unavailable"
    assert payload["resource"]["status"] == "missing"
    assert not (config_home / "seqevi" / "profiles").exists()


def test_setup_requires_dry_run_until_apply_slice(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["setup", "dbcan-cazyme", "--resource", str(tmp_path / "missing")],
    )

    assert result.exit_code == 2
    assert "setup apply is not implemented yet" in result.stderr


def test_setup_help_exposes_dry_run_and_json_but_not_plan() -> None:
    result = runner.invoke(
        app,
        ["setup", "--help"],
        terminal_width=240,
        color=False,
    )

    assert result.exit_code == 0
    help_text = unstyle(result.stdout)
    assert "--dry-run" in help_text
    assert "--json" in help_text
    assert "--plan" not in help_text
