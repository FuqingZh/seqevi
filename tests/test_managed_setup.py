from __future__ import annotations

import json
import hashlib
import subprocess
import tomllib
from click import unstyle
from importlib.resources import files
from pathlib import Path

import pytest
from typer.testing import CliRunner

import seqevi.distribution.setup as setup_impl
from seqevi.adapters import AdapterName
from seqevi.cli import app
from seqevi.distribution.manifest import (
    KitComponent,
    KitManifest,
    load_kit_manifest,
    parse_kit_manifest,
)
from seqevi.distribution.setup import apply_setup, build_setup_plan
from seqevi.api import resolve_annotation_inputs
from seqevi.errors import ProfileConfigurationError, SetupError
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


def test_annotation_path_resolves_v2_for_oci_dispatch(tmp_path: Path) -> None:
    resource = tmp_path / "resource"
    resource.mkdir()
    profile = tmp_path / "managed.toml"
    _write_v2_profile(profile, resource)

    resolved = resolve_annotation_inputs(
        profile=None,
        config=profile,
        adapter=None,
        executable=None,
        resource=None,
        store=None,
        threads=None,
        timeout_seconds=None,
    )

    assert resolved.executable is None
    assert resolved.profile is not None
    assert resolved.profile.version == 2


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


def test_setup_apply_rejects_blocked_resource_before_docker(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["setup", "dbcan-cazyme", "--resource", str(tmp_path / "missing")],
    )

    assert result.exit_code == 1
    assert "resource directory does not exist" in result.stdout


def test_setup_without_resource_fails_before_docker_inspection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        setup_impl,
        "_inspect_image",
        lambda _image: pytest.fail("Docker must not be inspected without a resource"),
    )

    plan = build_setup_plan(
        "dbcan-cazyme",
        environment={
            "HOME": str(tmp_path),
            "XDG_CONFIG_HOME": str(tmp_path / "config"),
        },
        stdin_isatty=False,
    )

    assert plan.status == "blocked"
    assert plan.runtime.image_status == "not-checked"


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


def _fixture_manifest() -> KitManifest:
    files = (
        ("CAZy-diamond", "CAZy.dmnd", b"diamond"),
        ("dbCAN-HMM", "dbCAN.hmm", b"hmm"),
        ("dbCAN-sub-HMM", "dbCAN-sub.hmm", b"sub"),
        ("fam-substrate-mapping", "fam-substrate-mapping.tsv", b"mapping"),
    )
    return KitManifest(
        schema_version=1,
        kit_id="dbcan-cazyme-test",
        adapter=AdapterName.DBCAN_CAZYME,
        platform="linux/amd64",
        dbcan_version="5.2.9",
        diamond_version="2.1.15",
        image="ghcr.io/fuqingzh/seqevi-dbcan@sha256:" + "a" * 64,
        resource_name="dbcan",
        resource_version="test-resource",
        components=tuple(
            KitComponent(name, path, len(content), hashlib.sha256(content).hexdigest())
            for name, path, content in files
        ),
    )


class _FakeDocker:
    def __init__(self, *, image_present: bool = False, smoke_returncode: int = 0):
        self.image_present = image_present
        self.smoke_returncode = smoke_returncode
        self.commands: list[tuple[str, ...]] = []

    def run(
        self, arguments: list[str], **_kwargs: object
    ) -> subprocess.CompletedProcess[str]:
        command = tuple(arguments)
        self.commands.append(command)
        subcommand = command[1:]
        if subcommand[:2] == ("image", "inspect"):
            return subprocess.CompletedProcess(
                arguments,
                0 if self.image_present else 1,
                stdout="",
                stderr="",
            )
        if subcommand[:1] == ("pull",):
            self.image_present = True
            return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")
        if subcommand[:1] == ("create",):
            return subprocess.CompletedProcess(
                arguments, 0, stdout="container-id\n", stderr=""
            )
        if subcommand[:2] == ("start", "--attach"):
            return subprocess.CompletedProcess(
                arguments, self.smoke_returncode, stdout="", stderr="smoke failed"
            )
        if subcommand[:2] == ("rm", "--force"):
            return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")
        raise AssertionError(f"unexpected docker command: {command}")


def _write_fixture_resource(path: Path) -> None:
    path.mkdir()
    for _name, filename, content in (
        ("CAZy-diamond", "CAZy.dmnd", b"diamond"),
        ("dbCAN-HMM", "dbCAN.hmm", b"hmm"),
        ("dbCAN-sub-HMM", "dbCAN-sub.hmm", b"sub"),
        ("fam-substrate-mapping", "fam-substrate-mapping.tsv", b"mapping"),
    ):
        (path / filename).write_bytes(content)


def test_setup_apply_pulls_smokes_and_publishes_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = _fixture_manifest()
    resource = tmp_path / "resource"
    _write_fixture_resource(resource)
    config_home = tmp_path / "config"
    environment = {"HOME": str(tmp_path), "XDG_CONFIG_HOME": str(config_home)}
    docker = _FakeDocker()
    monkeypatch.setattr(setup_impl, "load_kit_manifest", lambda _name: manifest)
    monkeypatch.setattr(setup_impl.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(setup_impl.subprocess, "run", docker.run)

    plan = build_setup_plan(
        "dbcan-cazyme",
        resource=resource,
        environment=environment,
        stdin_isatty=False,
    )
    applied = apply_setup(plan)

    assert applied.status == "applied"
    assert applied.smoke_status == "passed"
    assert applied.profile.status == "published"
    assert (resource / "seqevi.lock").is_file()
    profile = load_execution_profile(config_home / "seqevi/profiles/dbcan-cazyme.toml")
    assert profile.version == 2
    assert profile.resource == resource.resolve()
    assert profile.runtime is not None
    assert profile.runtime.image == manifest.image
    assert any(command[1:3] == ("pull", "--platform") for command in docker.commands)
    create = next(command for command in docker.commands if command[1] == "create")
    assert "--network" in create and create[create.index("--network") + 1] == "none"
    assert "--user" in create
    mount = create[create.index("--mount") + 1]
    assert "readonly" in mount
    compile(create[create.index("-c") + 1], "<dbcan-smoke>", "exec")
    assert any(command[1:3] == ("start", "--attach") for command in docker.commands)
    assert any(command[1:3] == ("rm", "--force") for command in docker.commands)


def test_setup_apply_reuses_image_and_equal_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = _fixture_manifest()
    resource = tmp_path / "resource"
    _write_fixture_resource(resource)
    config_home = tmp_path / "config"
    environment = {"HOME": str(tmp_path), "XDG_CONFIG_HOME": str(config_home)}
    docker = _FakeDocker()
    monkeypatch.setattr(setup_impl, "load_kit_manifest", lambda _name: manifest)
    monkeypatch.setattr(setup_impl.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(setup_impl.subprocess, "run", docker.run)

    first = apply_setup(
        build_setup_plan(
            "dbcan-cazyme",
            resource=resource,
            environment=environment,
            stdin_isatty=False,
        )
    )
    docker.commands.clear()
    second_plan = build_setup_plan(
        "dbcan-cazyme",
        resource=resource,
        environment=environment,
        stdin_isatty=False,
    )
    second = apply_setup(second_plan)

    assert first.status == second.status == "applied"
    assert second.profile.status == "equal"
    assert not any(command[1] == "pull" for command in docker.commands)


def test_setup_apply_smoke_failure_removes_container_and_no_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = _fixture_manifest()
    resource = tmp_path / "resource"
    _write_fixture_resource(resource)
    config_home = tmp_path / "config"
    environment = {"HOME": str(tmp_path), "XDG_CONFIG_HOME": str(config_home)}
    docker = _FakeDocker(smoke_returncode=1)
    monkeypatch.setattr(setup_impl, "load_kit_manifest", lambda _name: manifest)
    monkeypatch.setattr(setup_impl.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(setup_impl.subprocess, "run", docker.run)

    with pytest.raises(SetupError, match="smoke failed"):
        apply_setup(
            build_setup_plan(
                "dbcan-cazyme",
                resource=resource,
                environment=environment,
                stdin_isatty=False,
            )
        )

    assert any(command[1:3] == ("rm", "--force") for command in docker.commands)
    assert not (config_home / "seqevi/profiles/dbcan-cazyme.toml").exists()


def test_setup_cli_yes_json_returns_one_applied_document(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest = _fixture_manifest()
    resource = tmp_path / "resource"
    _write_fixture_resource(resource)
    config_home = tmp_path / "config"
    docker = _FakeDocker()
    monkeypatch.setattr(setup_impl, "load_kit_manifest", lambda _name: manifest)
    monkeypatch.setattr(setup_impl.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(setup_impl.subprocess, "run", docker.run)

    result = runner.invoke(
        app,
        [
            "setup",
            "dbcan-cazyme",
            "--resource",
            str(resource),
            "--yes",
            "--json",
        ],
        env={"HOME": str(tmp_path), "XDG_CONFIG_HOME": str(config_home)},
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "applied"
    assert payload["smoke"]["status"] == "passed"
