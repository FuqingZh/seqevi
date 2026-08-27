# SeqEvi

**SeqEvi: Sequence Evidence** is a content-addressed cache for reusable protein
sequence annotation evidence.

SeqEvi identifies proteins by canonical sequence content, determines which
sequences already have evidence under an exact annotation contract, runs an
external annotation tool only for cache misses, and exports an adapter-specific
single-file DuckDB result for the current FASTA.

## Status

SeqEvi 0.3.5 implements strict protein sequence identity, local SQLite/POSIX
and shared HTTP/PostgreSQL/POSIX Stores, exact cache-miss orchestration, Linux
external-tool containment, and immutable single-file DuckDB results. Python
3.12 or newer is required; direct adapter execution requires Linux.

SeqEvi 0.3.5 provides managed setup for dbCAN only. The `eggnog` and
`interpro-pfam` adapters remain supported through explicit runtimes and named
host profiles; managed setup for them is later feature work.

> **Known incomplete or unavailable work:** the original managed dbCAN public-
> user Slice D run remains incomplete; managed dispatch has no claim-before-OCI
> path, so D5 is unavailable; InterPro v2 target-Store refresh remains open;
> and global cache seeding is incomplete. See the
> [documentation index](docs/README.md) for the governing records. None of
> these states is represented as a pass.

The `eggnog`, `interpro-pfam`, and `dbcan-cazyme` adapters preserve their native
schemas and have accepted direct-runtime parity evidence. Shared Store,
resource-lock, batching, and result-publication details are routed from the
[current system architecture](docs/architecture/20260825-v1.0-current-system-architecture.md).

## Why SeqEvi

Two FASTA files do not need to be identical to reuse annotation. If a new FASTA
contains sequences seen in earlier projects, SeqEvi reuses the immutable
evidence for those sequences and annotates only novel content.

```text
FASTA A: 2000 new sequences       -> annotate 2000
FASTA B: 1000 sequences from A    -> annotate 0
FASTA C: 1000 from A + 500 novel  -> annotate 500
```

Reuse is exact. Tool runtime, annotation resource, semantic parameters, or
adapter contract changes produce a different evidence key and never silently
fall back to an older result.

## Intended CLI

For repeated use, keep one machine-local TOML per adapter runtime under
`${XDG_CONFIG_HOME:-~/.config}/seqevi/profiles/`:

```bash
seqevi profile init eggnog-5.0.2 --adapter eggnog
seqevi profile init interpro-pfam-38.1 --adapter interpro-pfam
seqevi profile init dbcan-5.2.9 --adapter dbcan-cazyme
```

Each command creates a complete adapter-specific TOML file and refuses to
replace an existing profile. After editing the machine-local paths, inspect and
validate profiles without launching either annotation runtime:

```bash
seqevi profile list
seqevi profile show eggnog-5.0.2
seqevi profile validate \
  --config "${XDG_CONFIG_HOME:-$HOME/.config}/seqevi/profiles/eggnog-5.0.2.toml"
```

`profile show` resolves paths and operational defaults but reports only
environment variable names, never their values. The original complete
templates remain available through `profile example --adapter ADAPTER`.

These profile commands configure SeqEvi; they do not install annotation
software or databases. Managed setup is available only for dbCAN and uses a
runtime image published by SeqEvi. It supports a read-only preview and an
explicit apply:

```bash
seqevi setup dbcan-cazyme \
  --resource /data/dbcan/db_v5-2-9_5-5-2026/raw \
  --dry-run

seqevi setup dbcan-cazyme \
  --resource /data/dbcan/db_v5-2-9_5-5-2026/raw \
  --dry-run --json

seqevi setup dbcan-cazyme \
  --resource /data/dbcan/db_v5-2-9_5-5-2026/raw \
  --yes
```

`--dry-run` never mutates state. `--yes` pulls the immutable image only when
needed, verifies the caller-owned four-file resource, creates `seqevi.lock`
when the resource permits it, runs an ephemeral read-only smoke, and publishes
the v2 profile atomically. It never downloads or copies the database. A managed
dbCAN annotation runs through an ephemeral Docker
container with the same caller UID/GID, read-only FASTA/resource mounts and a
local-Store `--network none` boundary:

```bash
seqevi annotate \
  --profile dbcan-cazyme \
  --store /data/seqevi-store \
  --fasta proteins.fasta \
  --output results/dbcan.duckdb
```

The dispatcher and cleanup boundary are covered by fixture tests. Real
direct-candidate versus managed-v2 scientific equality and later-process replay
passed the release gate. A validation harness used an immutable local image ID
built from the exact published inputs when site GHCR transport is unavailable;
the public setup/profile surface remains pinned to the bundled GHCR digest and
exposes no image override.

The real local/shared Store acceptance for eggNOG and InterPro/Pfam is recorded
in the [result-consumption runtime report](docs/benchmarks/20260805-v1.0-result-consumption-runtime-acceptance.md).
The managed dbCAN distribution gate is tracked in the
[runtime image release review](docs/architecture/20260805-v1.1-dbcan-runtime-image-release-review.md).

Run repeated annotations by name:

```bash
seqevi annotate \
  --profile eggnog-5.0.2 \
  --fasta proteins.fasta \
  --output results/eggnog.duckdb
```

```bash
seqevi annotate \
  --profile interpro-pfam-38.1 \
  --fasta proteins.fasta \
  --store https://seqevi.example.org \
  --output results/pfam.duckdb
```

```bash
seqevi annotate \
  --profile dbcan-5.2.9 \
  --fasta proteins.fasta \
  --output results/dbcan.duckdb
```

An exact profile file can be selected with `--config PATH`. Complete explicit
mode remains available:

```bash
seqevi annotate \
  --adapter eggnog \
  --fasta proteins.fasta \
  --store /data/seqevi-store \
  --output results/eggnog.duckdb \
  --executable /opt/eggnog-mapper/emapper.py \
  --resource /data/eggnog-5.0.2 \
  --threads 8
```

```bash
seqevi annotate \
  --adapter interpro-pfam \
  --fasta proteins.fasta \
  --store https://seqevi.example.org \
  --output results/pfam.duckdb \
  --executable /opt/interproscan/interproscan.sh \
  --resource /data/interproscan-5.77-108.0/data
```

Shared deployments expose the same Store contract:

The shared Store requires PostgreSQL 17 or newer so every mutation can enforce
one cumulative transaction deadline inside the claim lease runway.

```bash
seqevi serve \
  --database-url postgresql+psycopg://seqevi@postgres/seqevi \
  --artifacts-dir /data/seqevi-artifacts
```

The supported user-systemd deployment through the host rootful Docker daemon is
documented in the
[service runbook](docs/operations/20260727-v0.1.0-loopback-service-runbook.md).
The service image contains SeqEvi and its server dependencies only; annotation
executables and databases remain external.

Initialize or audit a database resource lock independently of annotation:

```bash
seqevi resource verify \
  --adapter eggnog \
  --executable /opt/eggnog-mapper/emapper.py \
  --resource /data/eggnog-5.0.2
```

## V1 Scope

- Protein FASTA input with strict, deterministic canonicalization.
- GA4GH `SQ.` sequence identifiers plus MD5 compatibility aliases.
- Exact, immutable evidence keys.
- Explicit `eggnog`, `interpro-pfam`, and official-runtime-validated
  `dbcan-cazyme` adapters. dbCAN direct/local/shared scientific acceptance is
  complete; publishing the managed runtime image remains separate work, and
  annotation databases remain caller supplied.
- Local SQLite/POSIX Store and shared PostgreSQL/POSIX Store service.
- One self-describing DuckDB result per invocation; adapter-native normalized
  evidence remains Parquet inside the incremental Store.

SeqEvi does not infer species, manage projects, schedule workflows, install
third-party tools, distribute annotation databases, or merge unrelated adapter
schemas.

## Documentation

Start with the [documentation index](docs/README.md), the
[current system architecture](docs/architecture/20260825-v1.0-current-system-architecture.md),
or the [first-annotation guide](docs/how-to-guides/first-annotation.md).

The index owns the current contract map, active work, incomplete evidence,
operations, and historical navigation. Superseded architecture is retained in
the [documentation archive](docs/archive/README.md), not mixed into onboarding.

## Python And Result Discovery

The public Python API returns DuckDB's native relation, so the same object can
be queried from a notebook or passed to Arrow/Polars without a SeqEvi wrapper:

```python
import seqevi

annotations = seqevi.annotate(
    "proteins.faa",
    profile="interpro-pfam-38.1",
    output="results/pfam.duckdb",
)
print(annotations.columns)
pfam = annotations.select("InputID", "SignatureAccession")
```

An existing result can be opened read-only with `seqevi.scan_annotations()`. If
the adapter columns are not known in advance, inspect the native relation or
the stable catalog first:

```python
annotations = seqevi.scan_annotations("results/pfam.duckdb")
print(annotations.columns)
print(annotations.pl(lazy=True).collect_schema())
```

The normal protein-level join key is `InputID`. `SequenceID` is the content
identity used for exact Store reuse. InterPro/Pfam keeps one-to-many domain
rows, so aggregate it before joining to a one-row-per-protein table when that
is the desired grain. SQL, R, and workflow tasks can open the same file and
query `main.annotations`; `_seqevi.column_info`, `_seqevi.table_info`, and
`_seqevi.metadata` provide column descriptions, row grain, and provenance.

SeqEvi 0.2.0 is a deliberate output cutover from the 0.1.0 directory Data
Package. Existing 0.1.0 packages remain readable by their own Data Package
tools, but new SeqEvi invocations publish DuckDB only; rerun an annotation to
produce the new result file.

## External Tools

Annotation runtimes and databases are normally supplied by the user. SeqEvi
targets
[eggNOG-mapper](https://github.com/eggnogdb/eggnog-mapper) and
[InterProScan](https://www.ebi.ac.uk/interpro/interproscan.html) with the Pfam
application, and [dbCAN](https://github.com/bcb-unl/run_dbcan) for protein-level
CAZyme annotation. Managed `seqevi setup` is implemented only for dbCAN using a
public, digest-pinned `ghcr.io/fuqingzh/seqevi-dbcan` runtime image built from
locked upstream inputs. It is SeqEvi-maintained, not an upstream-official dbCAN
image. Callers still provide the database path; annotation databases are never
bundled in the wheel or runtime image, and internal registry mirrors remain
deployment policy.

## License

SeqEvi is distributed under the [MIT License](LICENSE).
