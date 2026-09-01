# SeqEvi Documentation

> Status: active documentation index
> Last updated: 2026-09-01

This index is the navigation authority for SeqEvi documentation. It identifies
the current system description, current contracts, active work, incomplete
evidence, operations, and historical material. It does not duplicate those
documents or turn plans and reviews into runtime authority.

## Start Here

1. [Current system architecture](architecture/20260825-v1.0-current-system-architecture.md)
2. [Run a first annotation](how-to-guides/first-annotation.md)
3. [Sequence evidence contract v1.6](architecture/20260814-v1.6-sequence-evidence-contract.md)
4. [Adapter contract v1.3](architecture/20260821-v1.3-adapter-contract.md)
5. [Result consumption contract v1.1](architecture/20260804-v1.1-result-consumption-contract.md)
6. [Validation strategy v1.4](testing/20260825-v1.4-validation-strategy.md)

Operators should additionally read the
[current Store-maintenance runbook](operations/20260831-v1.3-oci-store-maintenance.md).

## Current Authority

| Boundary | Current document |
| --- | --- |
| system shape and navigation | [current system architecture](architecture/20260825-v1.0-current-system-architecture.md) |
| sequence identity, evidence, and ClaimSession | [sequence evidence v1.6](architecture/20260814-v1.6-sequence-evidence-contract.md) |
| adapter and external-tool behavior | [adapter v1.3](architecture/20260821-v1.3-adapter-contract.md) |
| DuckDB result | [result consumption v1.1](architecture/20260804-v1.1-result-consumption-contract.md) |
| host execution profiles | [profile v1](architecture/20260724-v1.0-execution-profile-contract.md) |
| managed OCI profiles | [profile v2.2](architecture/20260806-v2.2-execution-profile-contract.md) |
| local/shared Store and deployment | [storage v1.8](architecture/20260831-v1.8-storage-deployment-architecture.md) |
| managed dbCAN distribution | [managed distribution v1.2](architecture/20260806-v1.2-managed-adapter-distribution-architecture.md) |
| progress | [progress v1.1](architecture/20260824-v1.1-progress-observability-contract.md) |
| validation | [validation strategy v1.4](testing/20260825-v1.4-validation-strategy.md) |

For implementation decisions, apply this order:

1. `AGENTS.md` repository boundaries;
2. the current detailed contract for the affected boundary;
3. the current system overview for cross-boundary navigation;
4. an active implementation plan for delivery order only; and
5. the root README for the public landing-page description.

A review, plan, benchmark, or archived document does not enter runtime
authority merely because it is newer. A higher-version contract inherits only
the older rules it explicitly retains.

## Current Product State

The SeqEvi 0.4.0 release-candidate source implements local and shared evidence
Stores, immutable DuckDB results, three official host-runtime adapters,
ClaimSession coordination, Linux external-tool containment, and managed dbCAN
setup/OCI execution. Managed eggNOG and InterPro are not implemented. No 0.4.0
Python/GitHub release, production deployment, or historical-byte migration has
occurred.

The bundled managed dbCAN kit intentionally retains its producer SeqEvi 0.3.1
image and digest. The 0.4.0 candidate validates that exact kit; this is not an
implicit upgrade.

Merged R2 adds explicit native OCI-backed shared artifacts and acknowledged
schema 0005. Workflow run `33379681393` automatically published an immutable
0.3.5 revision image at
`ghcr.io/fuqingzh/seqevi-dbcan@sha256:1914939f1776fee3faac5241fc84f99f4534f37e20cc4d4d48eedf491c38488a`;
it did not register a new selectable kit. Image publication is now
workflow-dispatch-only.

## Active Work

| Work | Status | Authority |
| --- | --- | --- |
| artifact upload/finalization boundary | R2 is merged and included in the 0.4.0 release candidate. The final pre-merge canonical gate recorded 662 passed with zero skips, including SQLite, PostgreSQL and native 512 MiB OCI integration. No 0.4.0 publication, production deployment, migration, or new managed-kit registration has occurred | [implementation plan](implementation-plan/20260829-v1.0-artifact-upload-finalization-boundary-implementation-plan.md), [qualification](benchmarks/20260831-v1.0-native-oci-qualification.md), [integration specification](implementation-plan/20260831-v1.0-native-oci-integration-specification.md) |
| 0.3.5 tree-review remediation | Unit 0 was completed by the initial PR #71 delivery plus a follow-up closeout reconciling the remaining README publication wording. The plan remains active for later units, which are separately pending and unauthorized | [corrected review](architecture/20260825-v1.0-seqevi-0.3.5-tree-review.md), [implementation plan](implementation-plan/20260825-v1.0-tree-review-remediation-implementation-plan.md) |
| InterPro target-Store | v2 identity/exact-key refresh remains next before sanity | [successor plan](implementation-plan/20260820-v1.0-interpro-target-store-successor-implementation-plan.md) |
| global cache seeding | eggNOG and bounded dbCAN lanes have recorded closure; global expansion remains incomplete | [master plan](implementation-plan/20260807-v1.0-shared-store-cache-seeding-implementation-plan.md) |

## Incomplete Or Unavailable Evidence

- The original managed dbCAN public-user Slice D acquisition run remains
  incomplete. Its later transport disposition does not rewrite that run as a
  pass.
- Managed dispatch opens ClaimSession inside the container, so a claim-before-
  OCI D5 same-key managed gate is unavailable.
- InterPro v2 target-Store refresh and sanity remain open.
- Nine files outside the bounded dbCAN corpus remain unseeded; global cache
  seeding is incomplete.
- Public progress protocols, ETA, multi-input scheduling, and managed inner
  progress forwarding remain later work.

Relevant evidence:

- [Native zot/ORAS Unit 0 qualification and its explicit runtime limits](benchmarks/20260831-v1.0-native-oci-qualification.md)
- [Historical OCI qualification: Distribution v3.1.1 no-go under the now-withdrawn per-chunk durability gate](benchmarks/20260830-v1.0-oci-registry-qualification.md)
- [managed dbCAN public-release gate](benchmarks/20260806-v1.4-dbcan-public-release-gate.md)
- [dbCAN target-Store closeout](benchmarks/20260819-v1.0-dbcan-target-store-closeout.md)
- [eggNOG target-Store closeout](benchmarks/20260818-v1.0-eggnog-target-store-closeout.md)
- [historical InterPro Preflight](benchmarks/20260820-v1.0-interpro-target-store-preflight.md)
- [ClaimSession cancellation and contention acceptance](benchmarks/20260814-v1.0-claim-session-cancellation-acceptance.md)

## Operations

- [OCI Store maintenance v1.3](operations/20260831-v1.3-oci-store-maintenance.md)
- [private-cluster ingress](operations/20260729-v0.1.0-private-cluster-ingress-runbook.md)
- [loopback service](operations/20260727-v0.1.0-loopback-service-runbook.md)

The shared Store is a trusted private-network service. CIDR ingress is its
deployed trust boundary; it has no application authentication. Do not expose
`seqevi serve` publicly without a separately approved authentication or
explicit unsafe-bind contract.

## Historical Material

Superseded architecture and contract layers are retained under the
[documentation archive](archive/README.md). Implementation plans, benchmarks,
and runbooks remain in their established directories as durable execution,
measurement, or operational records, but only the current tables above are
onboarding and authority navigation.
