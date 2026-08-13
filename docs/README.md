# SeqEvi Documentation

> Status: active documentation index
> Last updated: 2026-08-13

SeqEvi has implemented the v1 local and shared Store paths. The current tree
also contains the DuckDB result surface, fixture-backed contracts for the three
initial adapters, the public dbCAN runtime-image publication record, the Slice
B managed-setup apply path and the Slice C OCI application dispatcher. The
real direct-candidate versus managed-v2 dbCAN scientific parity gate has
passed in addition to the fixture-backed dispatcher tests.
The original Slice D public-user run remains recorded as incomplete during
public-artifact acquisition. One authorized TestPyPI staging publication passed
clean install and controlled negative paths, but production PyPI was absent and
the bundled GHCR digest did not complete its pull on the acceptance host. That
evidence has not been rewritten as a pass. The later release decision defers a
repeat pull from that site as a transport check, so it is no longer a 0.2.0
blocker.

The active optimization sequence delivered one self-describing DuckDB result
with a native Python relation API and the local `dbcan-cazyme` scientific
adapter. Official-runtime parity, incremental reuse, shared Store replay and
the real eggNOG/InterPro result-consumption matrix are accepted. Managed
onboarding is being delivered as a separate vertical slice: runtime-image
publication, strict planning, resource verification, smoke, atomic profile
publication and the application-boundary OCI dispatcher are current; real
managed-v2 scientific parity and later-process replay are accepted. SeqEvi
0.2.0 offers managed setup only for dbCAN. eggNOG and InterPro/Pfam continue to
use their supported explicit or profile runtimes; managed setup for them is
later feature work.

> **Current ClaimSession delivery authority:** the [v1.2 closure
> addendum](implementation-plan/20260813-v1.2-claim-session-delivery-closure-addendum.md)
> plus the [v1.3 conflict-unfreeze
> addendum](implementation-plan/20260813-v1.3-claim-session-conflict-unfreeze-addendum.md)
> and the [v1.4 strict-gate exception
> addendum](implementation-plan/20260813-v1.4-claim-session-strict-gate-exception-addendum.md)
> govern the unchanged PR #31 delivery. The v1.4 addendum supersedes only the
> unexecutable strict-status freshness assumption; v1.2/v1.3 otherwise remain
> authoritative, including the temporary compatibility-line treatment below.

## Capability Status

| Surface | Current state | Contract or plan |
| --- | --- | --- |
| result consumption | single-file DuckDB and native Python relation; real local/shared runtime acceptance passed | [result contract v1.1](architecture/20260804-v1.1-result-consumption-contract.md), [acceptance](benchmarks/20260805-v1.0-result-consumption-runtime-acceptance.md) |
| eggNOG and InterPro/Pfam | official-runtime parity accepted | [runtime evidence](benchmarks/20260721-v1.0-eggnog-runtime-validation.md), [InterPro evidence](benchmarks/20260723-v1.0-interproscan-runtime-validation.md) |
| dbCAN CAZyme | official dbCAN 5.2.9 direct/local parity, incremental reuse and shared Store replay accepted; managed onboarding remains separate | [runtime evidence](benchmarks/20260804-v1.0-dbcan-runtime-validation.md), [dbCAN plan](implementation-plan/20260804-v1.0-dbcan-cazyme-adapter-implementation-plan.md) |
| execution profiles | v1 host profiles remain compatible; managed v2.2 setup and OCI execution are implemented and accepted | [profile v1 contract](architecture/20260724-v1.0-execution-profile-contract.md), [managed profile v2.2](architecture/20260806-v2.2-execution-profile-contract.md) |
| managed onboarding | dbCAN only for 0.2.0; Slice B setup, Slice C delegation and the release-equivalent scientific candidate gate passed; the original Slice D run remains incomplete, with its site repeat pull deferred as a transport check | [managed roadmap v1.1](implementation-plan/20260805-v1.1-managed-adapter-onboarding-implementation-plan.md), [Slice D record](benchmarks/20260806-v1.4-dbcan-public-release-gate.md), [candidate gate record](benchmarks/20260806-v1.3-dbcan-managed-candidate-gate.md), [publication record](benchmarks/20260805-v1.1-dbcan-runtime-image-publication.md) |
| shared Store cache seeding | approved staged execution plan; execution not started | [cache-seeding plan](implementation-plan/20260807-v1.0-shared-store-cache-seeding-implementation-plan.md) |
| evidence claim leases | per-EvidenceKey duplicate-suppression implementation validation accepted | [evidence contract v1.2](architecture/20260810-v1.2-sequence-evidence-contract.md), [claim-lease plan](implementation-plan/20260810-v1.0-evidence-claim-lease-implementation-plan.md) |
| evidence claim coordination | ClaimSession PR A implementation candidate; v1.3 contracts and the maintenance runbook become current only after exact-head approval and activation gates pass | [candidate evidence contract v1.3](architecture/20260812-v1.3-sequence-evidence-contract.md), [candidate storage architecture v1.3](architecture/20260812-v1.3-storage-deployment-architecture.md), [candidate maintenance runbook](operations/20260812-v1.0-claim-session-store-maintenance.md) |
| shared Store claim contention hardening | delivery rebaselined after PR #31 review convergence exhaustion: PR A remains in progress; #33/#34/#35 remain independent; #37 owns external-tool cleanup and final PostgreSQL pressure/C2 acceptance and merges last | [delivery rebaseline addendum and settled boundaries](implementation-plan/20260813-v1.1-claim-session-delivery-rebaseline-addendum.md), [ClaimSession technical plan](implementation-plan/20260812-v1.0-claim-session-and-tool-cancellation-implementation-plan.md), [superseded per-key hardening plan](implementation-plan/20260811-v1.0-shared-store-claim-contention-hardening.md) |

## Start Here

1. [Architecture overview](architecture/20260720-v1.0-seqevi-architecture.md)
2. [Sequence and evidence contract](architecture/20260812-v1.3-sequence-evidence-contract.md)
3. [Adapter contract](architecture/20260804-v1.1-adapter-contract.md)
4. [Result consumption contract](architecture/20260804-v1.1-result-consumption-contract.md)
5. [Execution profile contract](architecture/20260724-v1.0-execution-profile-contract.md)
6. [Storage and deployment architecture](architecture/20260812-v1.3-storage-deployment-architecture.md)
7. [MVP implementation plan](implementation-plan/20260720-v1.0-mvp-implementation-plan.md)
8. [Execution profile implementation plan](implementation-plan/20260724-v1.0-execution-profile-implementation-plan.md)
9. [Validation strategy](testing/20260810-v1.1-validation-strategy.md)
10. [Shared Store deployment and acceptance plan](implementation-plan/20260726-v1.0-shared-store-deployment-acceptance-plan.md)
11. [Shared Store deployment and acceptance](benchmarks/20260726-v1.0-shared-store-deployment-acceptance.md)
12. [eggNOG runtime validation](benchmarks/20260721-v1.0-eggnog-runtime-validation.md)
13. [Annotate runtime and bounded-memory plan](implementation-plan/20260722-v1.0-annotate-bounded-memory-plan.md)
14. [Bounded-memory and operational performance](benchmarks/20260722-v1.0-bounded-memory-performance.md)
15. [eggNOG-mapper and DIAMOND tuning](benchmarks/20260722-v1.0-eggnog-diamond-tuning.md)
16. [eggNOG full-proteome scaling](benchmarks/20260723-v1.0-eggnog-full-proteome-tuning.md)
17. [InterProScan official parity plan](implementation-plan/20260723-v1.0-interproscan-parity-implementation-plan.md)
18. [InterProScan Pfam runtime validation](benchmarks/20260723-v1.0-interproscan-runtime-validation.md)
19. [SeqEvi 0.1.0 service release implementation plan](implementation-plan/20260727-v0.1.0-service-release-implementation-plan.md)
20. [SeqEvi 0.1.0 loopback service runbook](operations/20260727-v0.1.0-loopback-service-runbook.md)
21. [SeqEvi 0.1.0 stable loopback service acceptance](benchmarks/20260727-v0.1.0-stable-loopback-service-acceptance.md)
22. [Private cluster ingress implementation plan](implementation-plan/20260729-v0.1.0-private-cluster-ingress-plan.md)
23. [Private cluster ingress runbook](operations/20260729-v0.1.0-private-cluster-ingress-runbook.md)
24. [Private cluster ingress acceptance](benchmarks/20260729-v0.1.0-private-cluster-ingress-acceptance.md)
25. [DuckDB result prototype benchmark](benchmarks/20260804-v1.1-duckdb-result-prototype.md)
26. [Queryable annotation result and Python API implementation plan](implementation-plan/20260804-v1.0-result-consumption-implementation-plan.md)
27. [dbCAN CAZyme adapter implementation plan](implementation-plan/20260804-v1.0-dbcan-cazyme-adapter-implementation-plan.md)
28. [dbCAN 5.2.9 runtime validation](benchmarks/20260804-v1.0-dbcan-runtime-validation.md)
29. [DuckDB result-consumption runtime acceptance](benchmarks/20260805-v1.0-result-consumption-runtime-acceptance.md)
30. [Superseded dbCAN redistribution review](architecture/20260805-v1.0-dbcan-redistribution-license-review.md)
31. [dbCAN runtime image release review](architecture/20260805-v1.1-dbcan-runtime-image-release-review.md)
32. [dbCAN runtime image publication](benchmarks/20260805-v1.1-dbcan-runtime-image-publication.md)
33. [Managed dbCAN next-gate validation](benchmarks/20260806-v1.2-dbcan-managed-next-gate-validation.md)
34. [Managed dbCAN release-equivalent candidate gate](benchmarks/20260806-v1.3-dbcan-managed-candidate-gate.md)
35. [Managed dbCAN Slice D public release gate](benchmarks/20260806-v1.4-dbcan-public-release-gate.md)
36. [Shared Store cache-seeding implementation plan](implementation-plan/20260807-v1.0-shared-store-cache-seeding-implementation-plan.md)
37. [Evidence claim lease implementation plan](implementation-plan/20260810-v1.0-evidence-claim-lease-implementation-plan.md)
38. [ClaimSession delivery rebaseline addendum](implementation-plan/20260813-v1.1-claim-session-delivery-rebaseline-addendum.md)
39. [ClaimSession and tool cancellation technical plan](implementation-plan/20260812-v1.0-claim-session-and-tool-cancellation-implementation-plan.md)
40. [Superseded shared Store claim contention hardening plan](implementation-plan/20260811-v1.0-shared-store-claim-contention-hardening.md)
41. [ClaimSession Store maintenance runbook](operations/20260812-v1.0-claim-session-store-maintenance.md)

## Managed Boundary Contracts

These documents preserve the accepted managed-onboarding boundary. Slice B
setup apply and smoke, the Slice C OCI dispatcher, and real candidate parity
with later-process replay are accepted. The original Slice D public-user run is
still incomplete; the release decision defers its site-specific repeat pull and
does not convert that run into passing evidence:

- [Managed adapter distribution architecture v1.2](architecture/20260806-v1.2-managed-adapter-distribution-architecture.md)
- [Execution profile v2.2 contract](architecture/20260806-v2.2-execution-profile-contract.md)
- [Managed adapter onboarding and distribution roadmap v1.1](implementation-plan/20260805-v1.1-managed-adapter-onboarding-implementation-plan.md)
- [dbCAN runtime image release review v1.1](architecture/20260805-v1.1-dbcan-runtime-image-release-review.md)

## Authority

For current implementation decisions, use this order:

1. The specific document marked `Status: approved target architecture`.
2. The approved architecture overview.
3. The active implementation plan for progress and validation state.
4. The root README.

A document marked `proposed target architecture; not implemented` records a
reviewed future direction but does not override current contracts or repository
guidance. Its status, the documentation index and the repository boundary map
must be activated together when implementation starts.

If implementation and documentation disagree, treat the disagreement as a
contract defect. Do not silently reinterpret the documented evidence identity
or cache behavior.

## Document Lifecycle

Current architecture documents use dated, versioned filenames. A material
contract change creates a new document version; historical documents remain
available for audit. Small clarifications that do not alter behavior may update
the current document in place.
