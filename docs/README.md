# SeqEvi Documentation

> Status: active documentation index
> Last updated: 2026-08-24

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
managed-v2 scientific parity and later-process replay are accepted. Managed
dbCAN was introduced in SeqEvi 0.2.0 and is preserved in SeqEvi 0.3.5.
eggNOG and InterPro/Pfam continue to use their supported explicit or profile
runtimes; managed setup for them is later feature work.

The implemented [modern single-invocation progress successor
plan](implementation-plan/20260824-v1.0-modern-single-invocation-progress-implementation-plan.md)
adds cumulative unique-sequence evidence readiness, automatic capable-TTY
progress with `--no-progress`, Rich-backed width-aware presentation and total
invocation time under the authoritative [progress contract
v1.1](architecture/20260824-v1.1-progress-observability-contract.md). It
preserves upstream logs, scientific contracts, batching, Store behavior and
the existing single-document `--json` boundary. Multi-input scheduling, public
progress protocols, managed inner-event forwarding and ETA remain deferred.

> **ClaimSession delivery is closed:** generic external-tool cancellation,
> ClaimSession authority-loss handling, PostgreSQL pressure, and the eggNOG
> two-process C2 gate are accepted in the [cancellation and contention
> record](benchmarks/20260814-v1.0-claim-session-cancellation-acceptance.md).
> That 64-thread candidate-source run used a fresh Store and is separate from,
> and does not close, the target-Store cache-seeding C2 gate.
> The delivery addenda remain historical records, not active navigation for an
> open ClaimSession pull request.

> **eggNOG target-Store closeout is accepted:** the
> [2026-08-18 benchmark](benchmarks/20260818-v1.0-eggnog-target-store-closeout.md)
> records the released SeqEvi 0.3.1 service/client, fenced schema readback,
> all-present frozen-set inventory, zero-compute target replay, and unchanged
> Store/CAS state. It keeps the accepted fresh-Store C2 mechanism run distinct
> from target execution. Global cache seeding remains incomplete; the dbCAN
> scoped gate is closed through D5 with D4/D3 accepted and corpus expansion
> explicitly inventoried, unseeded outside the D4 set, and closed
> under the approved [dbCAN target-Store execution
> plan](implementation-plan/20260819-v1.0-dbcan-target-store-execution-plan.md).
> The [containment harness prerequisite](benchmarks/20260819-v1.0-interpro-preflight-containment-harness-acceptance.md)
> is accepted. [SeqEvi 0.3.2](https://github.com/FuqingZh/seqevi/releases/tag/v0.3.2)
> and `interpro-pfam/2` are released; the historical v1 Preflight remains
> accepted as recorded, while its strictly read-only v2 identity/exact-key
> refresh is next before sanity. Current managed-v2 dispatch has no claim-before-OCI
> path, so its D5 same-key managed gate is explicitly unavailable rather than
> represented as passing.

## Capability Status

| Surface | Current state | Contract or plan |
| --- | --- | --- |
| result consumption | single-file DuckDB and native Python relation; real local/shared runtime acceptance passed | [result contract v1.1](architecture/20260804-v1.1-result-consumption-contract.md), [acceptance](benchmarks/20260805-v1.0-result-consumption-runtime-acceptance.md) |
| eggNOG and InterPro/Pfam | official-runtime parity accepted; generic ToolRunner cleanup plus the acceptance-only internal-timeout/external-watchdog containment prerequisite passed; released 0.3.2 adds `interpro-pfam/2`, whose runtime digest binds the selected complete JDK content tree while its supported execution path separately freezes the resolved JDK bin/PATH and disables Java option injection; v1 evidence remains read-compatible | [SeqEvi 0.3.2 release](https://github.com/FuqingZh/seqevi/releases/tag/v0.3.2), [containment harness acceptance](benchmarks/20260819-v1.0-interpro-preflight-containment-harness-acceptance.md), [ClaimSession cancellation and C2 acceptance](benchmarks/20260814-v1.0-claim-session-cancellation-acceptance.md), [adapter contract v1.3](architecture/20260821-v1.3-adapter-contract.md), [runtime evidence](benchmarks/20260721-v1.0-eggnog-runtime-validation.md), [InterPro v1 evidence](benchmarks/20260723-v1.0-interproscan-runtime-validation.md) |
| dbCAN CAZyme | scoped target-Store D1-D4 accepted; D4 correctness/rates and D3 replays passed, D5 is unavailable, and the bounded corpus manifest leaves nine files unseeded/not executed because expansion headroom is unavailable | [closeout](benchmarks/20260819-v1.0-dbcan-target-store-closeout.md), [bounded corpus manifest](benchmarks/20260819-v1.0-dbcan-bounded-corpus-manifest.md), [D4 acceptance](benchmarks/20260819-v1.0-dbcan-d4-target-store-sizing-acceptance.md), [D3 acceptance](benchmarks/20260819-v1.0-dbcan-d3-target-store-replay-acceptance.md), [execution plan](implementation-plan/20260819-v1.0-dbcan-target-store-execution-plan.md) |
| execution profiles | v1 host profiles remain compatible; managed v2.2 setup and OCI execution are implemented and accepted | [profile v1 contract](architecture/20260724-v1.0-execution-profile-contract.md), [managed profile v2.2](architecture/20260806-v2.2-execution-profile-contract.md) |
| managed onboarding | managed dbCAN was introduced in 0.2.0 and is preserved in SeqEvi 0.3.5; its bundled kit intentionally retains producer 0.3.1 and the existing digest; Slice B setup, Slice C delegation and the release-equivalent scientific candidate gate passed; the original Slice D run remains incomplete, with its site repeat pull deferred as a transport check | [managed roadmap v1.1](implementation-plan/20260805-v1.1-managed-adapter-onboarding-implementation-plan.md), [Slice D record](benchmarks/20260806-v1.4-dbcan-public-release-gate.md), [candidate gate record](benchmarks/20260806-v1.3-dbcan-managed-candidate-gate.md), [publication record](benchmarks/20260805-v1.1-dbcan-runtime-image-publication.md) |
| shared Store cache seeding | eggNOG lane and scoped dbCAN gate closed; dbCAN corpus expansion and global seeding remain incomplete; released InterPro v2 identity/exact-key refresh is next before sanity | [historical InterPro Preflight](benchmarks/20260820-v1.0-interpro-target-store-preflight.md), [InterPro successor plan](implementation-plan/20260820-v1.0-interpro-target-store-successor-implementation-plan.md), [dbCAN closeout](benchmarks/20260819-v1.0-dbcan-target-store-closeout.md), [eggNOG closeout benchmark](benchmarks/20260818-v1.0-eggnog-target-store-closeout.md), [master cache-seeding plan](implementation-plan/20260807-v1.0-shared-store-cache-seeding-implementation-plan.md) |
| historical evidence claim leases | accepted per-EvidenceKey duplicate-suppression predecessor, superseded by ClaimSession coordination | [historical evidence contract v1.2](architecture/20260810-v1.2-sequence-evidence-contract.md), [historical claim-lease plan](implementation-plan/20260810-v1.0-evidence-claim-lease-implementation-plan.md), [current evidence contract v1.6](architecture/20260814-v1.6-sequence-evidence-contract.md) |
| evidence claim coordination | ClaimSession coordination with bounded internal protocol edges, actively cancellable per-session HTTP transport, bounded PostgreSQL maintenance acquisition, and generic external-tool cancellation accepted | [evidence contract v1.6](architecture/20260814-v1.6-sequence-evidence-contract.md), [storage architecture v1.6](architecture/20260814-v1.6-storage-deployment-architecture.md), [acceptance evidence](benchmarks/20260814-v1.0-claim-session-cancellation-acceptance.md), [maintenance runbook v1.2](operations/20260818-v1.2-claim-session-store-maintenance.md) |
| shared Store claim contention hardening | ClaimSession and issue #37 cancellation accepted: cold two-process C2 at `9d417da`, plus fresh-database-guarded PostgreSQL 17 pressure at `7edc5e7` | [acceptance evidence](benchmarks/20260814-v1.0-claim-session-cancellation-acceptance.md), [delivery rebaseline addendum and settled boundaries](implementation-plan/20260813-v1.1-claim-session-delivery-rebaseline-addendum.md), [ClaimSession technical plan](implementation-plan/20260812-v1.0-claim-session-and-tool-cancellation-implementation-plan.md) |
| PostgreSQL maintenance acquisition deadlines | implemented and validated; two independent fixed budgets include pool checkout and physical connect | [implementation plan](implementation-plan/20260814-v1.0-postgresql-maintenance-acquisition-deadline-plan.md) |
| annotation progress observability | cumulative terminal-evidence readiness for unique sequences, automatic capable-TTY Rich display, explicit `--progress/--no-progress`, compact width fallback and total invocation time are implemented; tool batches and managed OCI remain indeterminate | [progress contract v1.1](architecture/20260824-v1.1-progress-observability-contract.md), [validation strategy v1.3](testing/20260824-v1.3-validation-strategy.md), [implemented modern plan](implementation-plan/20260824-v1.0-modern-single-invocation-progress-implementation-plan.md), [historical v1.0 contract](architecture/20260824-v1.0-progress-observability-contract.md) |

## Start Here

1. [Architecture overview](architecture/20260720-v1.0-seqevi-architecture.md)
2. [Sequence and evidence contract](architecture/20260814-v1.6-sequence-evidence-contract.md)
3. [Adapter contract](architecture/20260821-v1.3-adapter-contract.md)
4. [Result consumption contract](architecture/20260804-v1.1-result-consumption-contract.md)
5. [Execution profile contract](architecture/20260724-v1.0-execution-profile-contract.md)
6. [Storage and deployment architecture](architecture/20260814-v1.6-storage-deployment-architecture.md)
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
41. [ClaimSession Store maintenance runbook](operations/20260818-v1.2-claim-session-store-maintenance.md)
42. [ClaimSession cancellation and contention acceptance](benchmarks/20260814-v1.0-claim-session-cancellation-acceptance.md)
43. [eggNOG target-Store closeout implementation plan](implementation-plan/20260817-v1.0-eggnog-target-store-closeout-implementation-plan.md)
44. [eggNOG target-Store closeout benchmark](benchmarks/20260818-v1.0-eggnog-target-store-closeout.md)
45. [dbCAN target-Store execution plan](implementation-plan/20260819-v1.0-dbcan-target-store-execution-plan.md)
46. [dbCAN D2 local-candidate acceptance](benchmarks/20260819-v1.0-dbcan-d2-local-candidate-acceptance.md)
47. [dbCAN D4 target-Store sizing acceptance](benchmarks/20260819-v1.0-dbcan-d4-target-store-sizing-acceptance.md)
48. [dbCAN D3 target-Store replay acceptance](benchmarks/20260819-v1.0-dbcan-d3-target-store-replay-acceptance.md)
49. [dbCAN target-Store closeout](benchmarks/20260819-v1.0-dbcan-target-store-closeout.md)
50. [dbCAN bounded corpus disposition manifest](benchmarks/20260819-v1.0-dbcan-bounded-corpus-manifest.md)
51. [InterPro Preflight containment harness acceptance](benchmarks/20260819-v1.0-interpro-preflight-containment-harness-acceptance.md)
52. [InterPro target-Store successor implementation plan](implementation-plan/20260820-v1.0-interpro-target-store-successor-implementation-plan.md)
53. [Historical InterPro target-Store read-only Preflight](benchmarks/20260820-v1.0-interpro-target-store-preflight.md)
54. [Modern single-invocation progress implementation plan](implementation-plan/20260824-v1.0-modern-single-invocation-progress-implementation-plan.md)
55. [Annotation progress observability contract v1.1](architecture/20260824-v1.1-progress-observability-contract.md)
56. [Progress validation strategy v1.3](testing/20260824-v1.3-validation-strategy.md)
57. [Historical minimal progress implementation plan](implementation-plan/20260824-v1.0-minimal-progress-observability-implementation-plan.md)

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
