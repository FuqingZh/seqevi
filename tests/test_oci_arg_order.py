from __future__ import annotations

from pathlib import Path

from seqevi.store.oci import OciClientFiles, OciRegistry
from seqevi.store.transport import RegistryModel


def test_pull_option_values_are_not_split_by_registry_file_flags(
    tmp_path: Path,
) -> None:
    oras = tmp_path / "oras"
    config = tmp_path / "registry.json"
    ca_file = tmp_path / "ca.pem"
    oras.write_text("fixture")
    oras.chmod(0o700)
    config.write_text("{}")
    ca_file.write_text("fixture CA")
    registry = OciRegistry(
        RegistryModel(
            id="primary",
            endpoint="http://127.0.0.1:5000",
            repository="seqevi/artifacts",
        ),
        executable=oras,
        files=OciClientFiles(registry_config=config, ca_file=ca_file),
    )

    arguments = registry._oras_args(
        "pull",
        "--concurrency",
        "1",
        "--output",
        "/tmp/output",
        "127.0.0.1:5000/seqevi/artifacts@sha256:" + "a" * 64,
    )

    assert arguments[arguments.index("--concurrency") + 1] == "1"
    assert arguments[-5:] == (
        "--registry-config",
        str(config.resolve()),
        "--ca-file",
        str(ca_file.resolve()),
        "--plain-http",
    )
