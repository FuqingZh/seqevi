from __future__ import annotations

import csv
import importlib.metadata
import shutil
import sys
from pathlib import Path


def _source_url(metadata: importlib.metadata.PackageMetadata) -> str:
    project_urls = metadata.get_all("Project-URL") or []
    for preferred_name in ("Source", "Repository", "Homepage"):
        for item in project_urls:
            name, separator, url = item.partition(",")
            if separator and name.strip().casefold() == preferred_name.casefold():
                return url.strip()
    return metadata.get("Home-page", "")


def main() -> None:
    destination = Path(sys.argv[1])
    destination.mkdir(parents=True, exist_ok=True)
    rows: list[tuple[str, str, str, str]] = []
    for distribution in importlib.metadata.distributions():
        metadata = distribution.metadata
        name = metadata["Name"]
        rows.append(
            (
                name,
                distribution.version,
                metadata.get("License-Expression", ""),
                _source_url(metadata),
            )
        )
        for entry in distribution.files or ():
            filename = Path(str(entry)).name
            if filename.upper().startswith(("LICENSE", "COPYING", "NOTICE")):
                source = Path(distribution.locate_file(entry))
                if source.is_file():
                    package_dir = destination / "LICENSES" / name
                    package_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(source, package_dir / filename)
    with (destination / "PYTHON-PACKAGES.tsv").open(
        "w", encoding="utf-8", newline=""
    ) as output:
        writer = csv.writer(output, delimiter="\t", lineterminator="\n")
        writer.writerow(("name", "version", "license_expression", "upstream_source"))
        writer.writerows(sorted(set(rows), key=lambda row: row[0].casefold()))


if __name__ == "__main__":
    main()
