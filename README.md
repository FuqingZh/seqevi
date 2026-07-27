# SeqEvi

**SeqEvi: Sequence Evidence** is a content-addressed cache for reusable protein
sequence annotation evidence.

SeqEvi identifies proteins by canonical sequence content, determines which
sequences already have evidence under an exact annotation contract, runs an
external annotation tool only for cache misses, and exports an adapter-specific
result package for the current FASTA.

## Status

The target architecture and v1 contracts are approved. Phases 1 and 2 implement
strict protein sequence identity, the single-host SQLite/POSIX Store, the
external tool runner, exact cache-miss orchestration, and atomic Data Package
materialization.

The `interpro-pfam` and eggNOG-mapper 2.x `eggnog` adapters are implemented with
native-output validation and fixture parity coverage. The eggNOG adapter passes
direct parity against eggNOG-mapper 2.1.13 and eggNOG DB 5.0.2. The InterPro
adapter passes direct parity against InterProScan 5.77-108.0 with InterPro data
108.0 and Pfam 38.1. The Phase 5 shared Store service, HTTP client, streamed
POSIX artifacts, and PostgreSQL persistence are implemented and covered by
local/shared plus provisioned PostgreSQL integration tests. Phase 6 resource
locks avoid repeated hashing of large immutable database files and provide an
explicit full-content verification command. Annotation now uses atomic FASTA
staging, file-backed artifacts, bounded Store batches, lazy Parquet
materialization, and an operational thread setting.

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

Run repeated annotations by name:

```bash
seqevi annotate \
  --profile eggnog-5.0.2 \
  --fasta proteins.fasta \
  --output results/eggnog
```

```bash
seqevi annotate \
  --profile interpro-pfam-38.1 \
  --fasta proteins.fasta \
  --store https://seqevi.example.org \
  --output results/pfam
```

An exact profile file can be selected with `--config PATH`. Complete explicit
mode remains available:

```bash
seqevi annotate \
  --adapter eggnog \
  --fasta proteins.fasta \
  --store /data/seqevi-store \
  --output results/eggnog \
  --executable /opt/eggnog-mapper/emapper.py \
  --resource /data/eggnog-5.0.2 \
  --threads 8
```

```bash
seqevi annotate \
  --adapter interpro-pfam \
  --fasta proteins.fasta \
  --store https://seqevi.example.org \
  --output results/pfam \
  --executable /opt/interproscan/interproscan.sh \
  --resource /data/interproscan-5.77-108.0/data
```

Shared deployments expose the same Store contract:

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
- Official `eggnog` and `interpro-pfam` adapters.
- Local SQLite/POSIX Store and shared PostgreSQL/POSIX Store service.
- Adapter-specific Parquet results and a Data Package v2 descriptor.

SeqEvi does not infer species, manage projects, schedule workflows, install
third-party tools, distribute annotation databases, or merge unrelated adapter
schemas.

## Documentation

Start with [the documentation index](docs/README.md).

- [Architecture overview](docs/architecture/20260720-v1.0-seqevi-architecture.md)
- [Sequence and evidence contract](docs/architecture/20260720-v1.0-sequence-evidence-contract.md)
- [Adapter contract](docs/architecture/20260720-v1.0-adapter-contract.md)
- [Execution profile contract](docs/architecture/20260724-v1.0-execution-profile-contract.md)
- [Storage and deployment architecture](docs/architecture/20260720-v1.0-storage-deployment-architecture.md)
- [MVP implementation plan](docs/implementation-plan/20260720-v1.0-mvp-implementation-plan.md)
- [Execution profile implementation plan](docs/implementation-plan/20260724-v1.0-execution-profile-implementation-plan.md)
- [Validation strategy](docs/testing/20260720-v1.0-validation-strategy.md)
- [Annotate runtime and bounded-memory plan](docs/implementation-plan/20260722-v1.0-annotate-bounded-memory-plan.md)
- [Bounded-memory and operational performance](docs/benchmarks/20260722-v1.0-bounded-memory-performance.md)
- [InterProScan Pfam runtime validation](docs/benchmarks/20260723-v1.0-interproscan-runtime-validation.md)

## External Tools

Annotation runtimes and databases are supplied by the user. SeqEvi v1 targets
[eggNOG-mapper](https://github.com/eggnogdb/eggnog-mapper) and
[InterProScan](https://www.ebi.ac.uk/interpro/interproscan.html) with the Pfam
application. Runtime images may be published separately where upstream
licenses permit, but annotation databases are never bundled.

## License

SeqEvi is distributed under the [MIT License](LICENSE).
