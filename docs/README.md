# SeqEvi Documentation

> Status: active documentation index
> Last updated: 2026-07-23

SeqEvi has implemented the v1 local and shared Store paths plus both initial
adapters. The documents below define the implementation contract and record
which external-runtime gates have passed.

## Start Here

1. [Architecture overview](architecture/20260720-v1.0-seqevi-architecture.md)
2. [Sequence and evidence contract](architecture/20260720-v1.0-sequence-evidence-contract.md)
3. [Adapter contract](architecture/20260720-v1.0-adapter-contract.md)
4. [Storage and deployment architecture](architecture/20260720-v1.0-storage-deployment-architecture.md)
5. [MVP implementation plan](implementation-plan/20260720-v1.0-mvp-implementation-plan.md)
6. [Validation strategy](testing/20260720-v1.0-validation-strategy.md)
7. [eggNOG runtime validation](benchmarks/20260721-v1.0-eggnog-runtime-validation.md)
8. [Annotate runtime and bounded-memory plan](implementation-plan/20260722-v1.0-annotate-bounded-memory-plan.md)
9. [Bounded-memory and operational performance](benchmarks/20260722-v1.0-bounded-memory-performance.md)
10. [eggNOG-mapper and DIAMOND tuning](benchmarks/20260722-v1.0-eggnog-diamond-tuning.md)
11. [eggNOG full-proteome scaling](benchmarks/20260723-v1.0-eggnog-full-proteome-tuning.md)

## Authority

For v1 implementation decisions, use this order:

1. The specific sequence, adapter, or storage contract.
2. The architecture overview.
3. The active implementation plan.
4. The root README.

If implementation and documentation disagree, treat the disagreement as a
contract defect. Do not silently reinterpret the documented evidence identity
or cache behavior.

## Document Lifecycle

Current architecture documents use dated, versioned filenames. A material
contract change creates a new document version; historical documents remain
available for audit. Small clarifications that do not alter behavior may update
the current document in place.
