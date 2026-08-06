"""Strict parsing for bundled managed runtime kit manifests."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from importlib.resources import files
from pathlib import PurePosixPath
from typing import Any

from seqevi.adapters import AdapterName
from seqevi.errors import SetupError

_SCHEMA_VERSION = 1
_KIT_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_OCI_IMAGE = re.compile(
    r"[a-z0-9][a-z0-9./_-]*(?::[A-Za-z0-9._-]+)?@sha256:[0-9a-f]{64}\Z"
)
_ROOT_KEYS = {
    "schema_version",
    "kit_id",
    "adapter",
    "platform",
    "dbcan_version",
    "diamond_version",
    "image",
    "resource",
}
_RESOURCE_KEYS = {"name", "version", "component"}
_COMPONENT_KEYS = {"name", "path", "size", "sha256"}
_DBCAN_COMPONENTS = {
    ("CAZy-diamond", "CAZy.dmnd"),
    ("dbCAN-HMM", "dbCAN.hmm"),
    ("dbCAN-sub-HMM", "dbCAN-sub.hmm"),
    ("fam-substrate-mapping", "fam-substrate-mapping.tsv"),
}


@dataclass(frozen=True, slots=True)
class KitComponent:
    """One caller-owned resource file declared by a first-party kit."""

    name: str
    path: str
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class KitManifest:
    """Validated immutable inputs for one managed runtime kit."""

    schema_version: int
    kit_id: str
    adapter: AdapterName
    platform: str
    dbcan_version: str
    diamond_version: str
    image: str
    resource_name: str
    resource_version: str
    components: tuple[KitComponent, ...]


def load_kit_manifest(name: str) -> KitManifest:
    """Load one bundled kit manifest by its user-facing setup name."""

    if name != "dbcan-cazyme":
        raise SetupError(f"unknown managed kit {name!r}; available kits: dbcan-cazyme")
    try:
        raw = files("seqevi.kits").joinpath(f"{name}.toml").read_bytes()
        document = tomllib.loads(raw.decode("utf-8"))
    except FileNotFoundError as error:
        raise SetupError(f"managed kit manifest is missing for {name!r}") from error
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise SetupError(
            f"cannot read managed kit manifest {name!r}: {error}"
        ) from error
    return parse_kit_manifest(document, name=name)


def parse_kit_manifest(
    document: dict[str, Any], *, name: str = "<document>"
) -> KitManifest:
    """Strictly validate a decoded first-party kit manifest."""

    if set(document) != _ROOT_KEYS:
        unknown = sorted(set(document) - _ROOT_KEYS)
        missing = sorted(_ROOT_KEYS - set(document))
        detail = []
        if unknown:
            detail.append(f"unknown keys: {', '.join(unknown)}")
        if missing:
            detail.append(f"missing keys: {', '.join(missing)}")
        raise SetupError(
            f"managed kit manifest {name} has invalid schema ({'; '.join(detail)})"
        )
    schema_version = document["schema_version"]
    if isinstance(schema_version, bool) or schema_version != _SCHEMA_VERSION:
        raise SetupError(
            f"managed kit manifest schema_version must be {_SCHEMA_VERSION}"
        )
    kit_id = _string(document["kit_id"], "kit_id")
    if _KIT_ID.fullmatch(kit_id) is None:
        raise SetupError("managed kit manifest kit_id contains unsafe characters")
    adapter_text = _string(document["adapter"], "adapter")
    try:
        adapter = AdapterName(adapter_text)
    except ValueError as error:
        raise SetupError(
            f"managed kit manifest has unknown adapter {adapter_text!r}"
        ) from error
    if adapter is not AdapterName.DBCAN_CAZYME:
        raise SetupError("managed kit manifests currently support only dbcan-cazyme")
    platform = _string(document["platform"], "platform")
    if platform != "linux/amd64":
        raise SetupError("dbCAN managed kit platform must be linux/amd64")
    dbcan_version = _string(document["dbcan_version"], "dbcan_version")
    diamond_version = _string(document["diamond_version"], "diamond_version")
    image = _string(document["image"], "image")
    if _OCI_IMAGE.fullmatch(image) is None:
        raise SetupError("managed kit image must contain an immutable sha256 digest")

    resource = document["resource"]
    if not isinstance(resource, dict) or set(resource) != _RESOURCE_KEYS:
        raise SetupError(
            "managed kit resource must contain name, version and component"
        )
    resource_name = _string(resource["name"], "resource.name")
    resource_version = _string(resource["version"], "resource.version")
    raw_components = resource["component"]
    if not isinstance(raw_components, list) or not raw_components:
        raise SetupError("managed kit resource.component must be a non-empty array")
    components: list[KitComponent] = []
    names: set[str] = set()
    paths: set[str] = set()
    for index, raw in enumerate(raw_components, start=1):
        if not isinstance(raw, dict) or set(raw) != _COMPONENT_KEYS:
            raise SetupError(
                f"managed kit resource component {index} has invalid schema"
            )
        component_name = _string(raw["name"], f"resource.component[{index}].name")
        path = _relative_path(raw["path"], f"resource.component[{index}].path")
        size = raw["size"]
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise SetupError(
                f"managed kit resource component {component_name!r} has invalid size"
            )
        sha256 = _string(raw["sha256"], f"resource.component[{index}].sha256")
        if _SHA256.fullmatch(sha256) is None:
            raise SetupError(
                f"managed kit resource component {component_name!r} has invalid SHA-256"
            )
        if component_name in names or path in paths:
            raise SetupError("managed kit resource components must be unique")
        names.add(component_name)
        paths.add(path)
        components.append(KitComponent(component_name, path, size, sha256))
    pairs = {(component.name, component.path) for component in components}
    if pairs != _DBCAN_COMPONENTS:
        raise SetupError(
            "dbCAN managed kit must declare the exact four required resource files"
        )
    return KitManifest(
        schema_version=schema_version,
        kit_id=kit_id,
        adapter=adapter,
        platform=platform,
        dbcan_version=dbcan_version,
        diamond_version=diamond_version,
        image=image,
        resource_name=resource_name,
        resource_version=resource_version,
        components=tuple(components),
    )


def _string(value: object, key: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise SetupError(f"managed kit manifest key {key!r} must be a non-empty string")
    return value


def _relative_path(value: object, key: str) -> str:
    path = _string(value, key)
    if "\\" in path:
        raise SetupError(f"managed kit manifest key {key!r} must use POSIX paths")
    parsed = PurePosixPath(path)
    if (
        path != parsed.as_posix()
        or parsed.is_absolute()
        or any(part in {"", ".", ".."} for part in parsed.parts)
    ):
        raise SetupError(
            f"managed kit manifest key {key!r} must be a safe relative path"
        )
    return path
