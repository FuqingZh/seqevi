from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import duckdb
import pytest

from seqevi import __version__
import seqevi.distribution.oci as oci
from seqevi.adapters import AdapterName
from seqevi.errors import AnnotationError
from seqevi.execution_profile import ExecutionProfile, ManagedRuntime
from seqevi.distribution.manifest import KitComponent, KitManifest
from seqevi.store.oci import OciClientFiles


def _manifest(files: tuple[tuple[str, str, bytes], ...]) -> KitManifest:
    return KitManifest(
        schema_version=1,
        kit_id="dbcan-cazyme-test",
        seqevi_version="0.3.1",
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


def _resource(tmp_path: Path, manifest: KitManifest) -> Path:
    resource = tmp_path / "resource"
    resource.mkdir()
    for component in manifest.components:
        (resource / component.path).write_bytes(
            {
                "CAZy.dmnd": b"diamond",
                "dbCAN.hmm": b"hmm",
                "dbCAN-sub.hmm": b"sub",
                "fam-substrate-mapping.tsv": b"mapping",
            }[component.path]
        )
    from seqevi.resource_lock import ResourceComponent, resolve_resource_lock

    resolve_resource_lock(
        database=resource,
        resource_name=manifest.resource_name,
        resource_version=manifest.resource_version,
        components=tuple(
            ResourceComponent(component.name, component.path)
            for component in manifest.components
        ),
    )
    return resource


def _profile(tmp_path: Path, manifest: KitManifest, resource: Path) -> ExecutionProfile:
    return ExecutionProfile(
        source=tmp_path / "profile.toml",
        adapter=AdapterName.DBCAN_CAZYME,
        executable=None,
        resource=resource,
        store=str(tmp_path / "store"),
        version=2,
        runtime=ManagedRuntime(
            kind="oci",
            kit_id=manifest.kit_id,
            engine="docker",
            image=manifest.image,
        ),
    )


def _inputs(tmp_path: Path) -> tuple[KitManifest, Path, ExecutionProfile, Path, Path]:
    files = (
        ("CAZy-diamond", "CAZy.dmnd", b"diamond"),
        ("dbCAN-HMM", "dbCAN.hmm", b"hmm"),
        ("dbCAN-sub-HMM", "dbCAN-sub.hmm", b"sub"),
        ("fam-substrate-mapping", "fam-substrate-mapping.tsv", b"mapping"),
    )
    manifest = _manifest(files)
    resource = _resource(tmp_path, manifest)
    profile = _profile(tmp_path, manifest, resource)
    fasta = tmp_path / "proteins with space.faa"
    fasta.write_text(">p\nMPEPTIDE\n", encoding="utf-8")
    return manifest, resource, profile, fasta, tmp_path / "result.duckdb"


def _write_result(path: Path, resource_id: str, seqevi_version: str) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA _seqevi")
        connection.execute(
            """
            CREATE TABLE _seqevi.metadata AS SELECT * FROM (VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ) AS t(
                ResultFormatVersion, ResultSchemaID, SeqEviVersion, Adapter,
                AdapterContractVersion, UpstreamTool, UpstreamToolVersion,
                ToolRuntimeDigest, ResourceID, InputDigest, CreatedAt
            )
            """,
            [
                "seqevi-duckdb/1",
                "dbcan-cazyme/5",
                seqevi_version,
                "dbcan-cazyme",
                "dbcan-cazyme/1",
                "dbCAN",
                "5.2.9",
                "sha256:" + "b" * 64,
                resource_id,
                "input",
                "now",
            ],
        )


def test_managed_registry_files_use_fixed_readonly_container_paths(
    tmp_path: Path,
) -> None:
    config = tmp_path / "registry.json"
    ca_file = tmp_path / "registry.pem"
    files = OciClientFiles(registry_config=config, ca_file=ca_file)

    mounts = oci._registry_mounts(files)
    command = oci._inner_command(
        fasta="/mnt/seqevi/input.fasta",
        output="/mnt/seqevi/output/result.duckdb",
        resource="/mnt/seqevi/resource",
        store="https://store.example.test",
        threads=1,
        timeout_seconds=None,
        oci_files=files,
    )

    assert mounts == (
        f"type=bind,source={config},target=/run/seqevi/registry/config.json,readonly",
        f"type=bind,source={ca_file},target=/run/seqevi/registry/ca.pem,readonly",
    )
    assert "--oci-registry-config" in command
    assert "/run/seqevi/registry/config.json" in command
    assert "--oci-registry-ca-file" in command
    assert "/run/seqevi/registry/ca.pem" in command
    assert str(config) not in command
    assert str(ca_file) not in command


def test_managed_local_store_rejects_registry_files(tmp_path: Path) -> None:
    with pytest.raises(AnnotationError, match="HTTP\\(S\\) shared Store"):
        oci._resolve_store(
            tmp_path / "store",
            oci_files=OciClientFiles(registry_config=tmp_path / "registry.json"),
        )


def test_old_managed_kit_rejects_oci_files_before_docker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, _resource, profile, fasta, output = _inputs(tmp_path)
    monkeypatch.setattr(oci, "load_kit_manifest", lambda _name: manifest)

    with pytest.raises(AnnotationError, match="does not include OCI artifact support"):
        oci.run_oci_annotation(
            fasta=fasta,
            output=output,
            profile=profile,
            store="https://store.example.test",
            threads=1,
            timeout_seconds=None,
            oci_files=OciClientFiles(registry_config=tmp_path / "registry.json"),
        )


def test_managed_annotation_mounts_are_narrow_and_ephemeral(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, resource, profile, fasta, output = _inputs(tmp_path)
    calls: list[tuple[str, ...]] = []

    def fake_docker(
        _docker: str,
        arguments: tuple[str, ...],
        *,
        timeout_seconds: float | None,
        action: str,
    ) -> subprocess.CompletedProcess[str]:
        del timeout_seconds, action
        calls.append(arguments)
        if arguments[0] == "create":
            return subprocess.CompletedProcess(arguments, 0, "container", "")
        if arguments[:2] == ("start", "--attach"):
            create = next(call for call in calls if call[0] == "create")
            mount_values = [
                create[index + 1]
                for index, value in enumerate(create[:-1])
                if value == "--mount"
            ]
            output_mount = next(
                value for value in mount_values if "target=/mnt/seqevi/output" in value
            )
            source = output_mount.split("source=", 1)[1].split(",target=", 1)[0]
            staged = Path(source) / "result.duckdb"
            _write_result(
                staged,
                oci._resource_id_from_lock(resource, manifest),
                manifest.seqevi_version,
            )
            return subprocess.CompletedProcess(
                arguments,
                0,
                json.dumps(
                    {
                        "status": "ok",
                        "adapter": "dbcan-cazyme",
                        "result_schema": "dbcan-cazyme/5",
                        "counts": {
                            "input_records": 1,
                            "unique_sequences": 1,
                            "cache_hits": 0,
                            "computed": 1,
                            "hits": 1,
                            "no_hits": 0,
                        },
                        "metrics": {
                            "elapsed_seconds": 1.0,
                            "package_seconds": 0.1,
                            "configured_threads": 1,
                        },
                    }
                ),
                "",
            )
        if arguments[:2] == ("rm", "--force"):
            return subprocess.CompletedProcess(arguments, 0, "", "")
        if arguments[:2] == ("image", "inspect"):
            return subprocess.CompletedProcess(arguments, 0, "", "")
        raise AssertionError(arguments)

    monkeypatch.setattr(oci.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(oci, "load_kit_manifest", lambda _name: manifest)
    monkeypatch.setattr(oci, "_docker_call", fake_docker)

    result = oci.run_oci_annotation(
        fasta=fasta,
        output=output,
        profile=profile,
        store=tmp_path / "store",
        threads=1,
        timeout_seconds=None,
    )

    assert manifest.seqevi_version != __version__
    assert output.is_file()
    assert result.summary.output_dir == output
    assert result.summary.metrics.existing_finalizations == 0
    create = next(call for call in calls if call[0] == "create")
    assert create[create.index("--network") + 1] == "none"
    assert create[create.index("--user") + 1] == f"{os.getuid()}:{os.getgid()}"
    mounts = [
        create[index + 1]
        for index, value in enumerate(create[:-1])
        if value == "--mount"
    ]
    assert sum("readonly" in mount for mount in mounts) == 2
    assert any("target=/mnt/seqevi/output" in mount for mount in mounts)
    assert any("target=/mnt/seqevi/store" in mount for mount in mounts)
    assert "docker.sock" not in " ".join(create)
    assert any(call[:2] == ("rm", "--force") for call in calls)


def test_managed_annotation_failure_removes_container_and_keeps_output_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, resource, profile, fasta, output = _inputs(tmp_path)
    calls: list[tuple[str, ...]] = []

    def fake_docker(
        _docker: str,
        arguments: tuple[str, ...],
        *,
        timeout_seconds: float | None,
        action: str,
    ) -> subprocess.CompletedProcess[str]:
        del timeout_seconds, action
        calls.append(arguments)
        if arguments[0] == "create":
            return subprocess.CompletedProcess(arguments, 0, "container", "")
        if arguments[:2] == ("start", "--attach"):
            return subprocess.CompletedProcess(arguments, 17, "", "dbCAN failed")
        if arguments[:2] == ("rm", "--force"):
            return subprocess.CompletedProcess(arguments, 0, "", "")
        if arguments[:2] == ("image", "inspect"):
            return subprocess.CompletedProcess(arguments, 0, "", "")
        raise AssertionError(arguments)

    monkeypatch.setattr(oci.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(oci, "load_kit_manifest", lambda _name: manifest)
    monkeypatch.setattr(oci, "_docker_call", fake_docker)

    with pytest.raises(AnnotationError, match="container failed"):
        oci.run_oci_annotation(
            fasta=fasta,
            output=output,
            profile=profile,
            store=tmp_path / "store",
            threads=1,
            timeout_seconds=None,
        )

    assert not output.exists()
    assert any(call[:2] == ("rm", "--force") for call in calls)


def test_managed_annotation_attach_timeout_force_removes_exact_container(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    manifest, _resource_value, profile, fasta, output = _inputs(tmp_path)
    calls: list[tuple[str, ...]] = []
    live_containers: set[str] = set()

    def fake_run(
        command: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
        timeout: float | None,
    ) -> subprocess.CompletedProcess[str]:
        assert check is False
        assert capture_output is True
        assert text is True
        arguments = tuple(command[1:])
        calls.append(arguments)
        if arguments[:2] == ("image", "inspect"):
            return subprocess.CompletedProcess(command, 0, "", "")
        if arguments[0] == "create":
            container_name = arguments[arguments.index("--name") + 1]
            live_containers.add(container_name)
            return subprocess.CompletedProcess(command, 0, "container", "")
        if arguments[:2] == ("start", "--attach"):
            assert arguments[2] in live_containers
            assert timeout is not None
            raise subprocess.TimeoutExpired(command, timeout)
        if arguments[:2] == ("rm", "--force"):
            assert arguments[2] in live_containers
            live_containers.remove(arguments[2])
            return subprocess.CompletedProcess(command, 0, "", "")
        raise AssertionError(arguments)

    monkeypatch.setattr(oci.shutil, "which", lambda _name: "/usr/bin/docker")
    monkeypatch.setattr(oci, "load_kit_manifest", lambda _name: manifest)
    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(AnnotationError, match="timed out while trying to run"):
        oci.run_oci_annotation(
            fasta=fasta,
            output=output,
            profile=profile,
            store=tmp_path / "store",
            threads=1,
            timeout_seconds=0.01,
        )

    create = next(call for call in calls if call[0] == "create")
    container_name = create[create.index("--name") + 1]
    assert calls[-1] == ("rm", "--force", container_name)
    assert not live_containers
    assert not output.exists()


def test_managed_shared_store_rejects_embedded_credentials() -> None:
    with pytest.raises(AnnotationError, match="must not embed credentials"):
        oci._resolve_store("https://user:secret@example.org/store?token=x")
