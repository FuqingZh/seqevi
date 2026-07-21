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
native-output validation and fixture parity coverage. Acceptance against the
official external runtimes and databases is still required before their
scientific parity gates are closed. The Phase 5 shared Store service, HTTP
client, streamed POSIX artifacts, and PostgreSQL persistence are implemented and
covered by local/shared plus provisioned PostgreSQL integration tests.

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

```bash
seqevi annotate \
  --adapter eggnog \
  --fasta proteins.fasta \
  --store /data/seqevi-store \
  --output results/eggnog \
  --executable /opt/eggnog-mapper/emapper.py \
  --database /data/eggnog-5.0.2
```

```bash
seqevi annotate \
  --adapter interpro-pfam \
  --fasta proteins.fasta \
  --store https://seqevi.example.org \
  --output results/pfam \
  --executable /opt/interproscan/interproscan.sh \
  --database /data/interproscan-5.77-108.0/data
```

Shared deployments expose the same Store contract:

```bash
seqevi serve \
  --database-url postgresql+psycopg://seqevi@postgres/seqevi \
  --artifacts-dir /data/seqevi-artifacts
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
- [Storage and deployment architecture](docs/architecture/20260720-v1.0-storage-deployment-architecture.md)
- [MVP implementation plan](docs/implementation-plan/20260720-v1.0-mvp-implementation-plan.md)
- [Validation strategy](docs/testing/20260720-v1.0-validation-strategy.md)

## External Tools

Annotation runtimes and databases are supplied by the user. SeqEvi v1 targets
[eggNOG-mapper](https://github.com/eggnogdb/eggnog-mapper) and
[InterProScan](https://www.ebi.ac.uk/interpro/interproscan.html) with the Pfam
application. Runtime images may be published separately where upstream
licenses permit, but annotation databases are never bundled.

## License

SeqEvi is distributed under the [MIT License](LICENSE).
