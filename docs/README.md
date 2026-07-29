# SeqEvi Documentation

> Status: active documentation index
> Last updated: 2026-07-29

SeqEvi has implemented the v1 local and shared Store paths plus both initial
adapters. The documents below define the implementation contract and record
which external-runtime gates have passed.

## Start Here

1. [Architecture overview](architecture/20260720-v1.0-seqevi-architecture.md)
2. [Sequence and evidence contract](architecture/20260720-v1.0-sequence-evidence-contract.md)
3. [Adapter contract](architecture/20260720-v1.0-adapter-contract.md)
4. [Execution profile contract](architecture/20260724-v1.0-execution-profile-contract.md)
5. [Storage and deployment architecture](architecture/20260729-v1.1-storage-deployment-architecture.md)
6. [MVP implementation plan](implementation-plan/20260720-v1.0-mvp-implementation-plan.md)
7. [Execution profile implementation plan](implementation-plan/20260724-v1.0-execution-profile-implementation-plan.md)
8. [Validation strategy](testing/20260720-v1.0-validation-strategy.md)
9. [Shared Store deployment and acceptance plan](implementation-plan/20260726-v1.0-shared-store-deployment-acceptance-plan.md)
10. [Shared Store deployment and acceptance](benchmarks/20260726-v1.0-shared-store-deployment-acceptance.md)
11. [eggNOG runtime validation](benchmarks/20260721-v1.0-eggnog-runtime-validation.md)
12. [Annotate runtime and bounded-memory plan](implementation-plan/20260722-v1.0-annotate-bounded-memory-plan.md)
13. [Bounded-memory and operational performance](benchmarks/20260722-v1.0-bounded-memory-performance.md)
14. [eggNOG-mapper and DIAMOND tuning](benchmarks/20260722-v1.0-eggnog-diamond-tuning.md)
15. [eggNOG full-proteome scaling](benchmarks/20260723-v1.0-eggnog-full-proteome-tuning.md)
16. [InterProScan official parity plan](implementation-plan/20260723-v1.0-interproscan-parity-implementation-plan.md)
17. [InterProScan Pfam runtime validation](benchmarks/20260723-v1.0-interproscan-runtime-validation.md)
18. [SeqEvi 0.1.0 service release implementation plan](implementation-plan/20260727-v0.1.0-service-release-implementation-plan.md)
19. [SeqEvi 0.1.0 loopback service runbook](operations/20260727-v0.1.0-loopback-service-runbook.md)
20. [SeqEvi 0.1.0 stable loopback service acceptance](benchmarks/20260727-v0.1.0-stable-loopback-service-acceptance.md)
21. [Private cluster ingress implementation plan](implementation-plan/20260729-v0.1.0-private-cluster-ingress-plan.md)
22. [Private cluster ingress runbook](operations/20260729-v0.1.0-private-cluster-ingress-runbook.md)
23. [Private cluster ingress acceptance](benchmarks/20260729-v0.1.0-private-cluster-ingress-acceptance.md)

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
