# SeqEvi Documentation Archive

Status: historical navigation; not current authority

This directory retains superseded documents whose replacement is explicit and
whose history remains useful. Archived files keep their original document type
but do not override the current authority map in [docs/README.md](../README.md).

## Architecture

Superseded architecture and contract layers live under
[`archive/architecture/`](architecture/). Each current contract names the
historical layers it inherits. Follow those links only when the current
document deliberately delegates unchanged detail.

| Archived series | Current replacement |
| --- | --- |
| July 20 system overview | [current system architecture](../architecture/20260825-v1.0-current-system-architecture.md) |
| adapter v1.0-v1.2 | [adapter v1.3](../architecture/20260821-v1.3-adapter-contract.md) |
| sequence evidence v1.0-v1.5 | [sequence evidence v1.6](../architecture/20260814-v1.6-sequence-evidence-contract.md) |
| storage v1.0-v1.5 | [storage v1.6](../architecture/20260814-v1.6-storage-deployment-architecture.md) |
| managed distribution v1.0-v1.1 | [managed distribution v1.2](../architecture/20260806-v1.2-managed-adapter-distribution-architecture.md) |
| managed profile v2.0-v2.1 | [managed profile v2.2](../architecture/20260806-v2.2-execution-profile-contract.md) |
| progress v1.0 | [progress v1.1](../architecture/20260824-v1.1-progress-observability-contract.md) |
| database-redistribution review v1.0 | [runtime-image release review v1.1](../architecture/20260805-v1.1-dbcan-runtime-image-release-review.md) |

## Validation

Explicitly superseded validation layers live under [`archive/testing/`](testing/).
The foundational v1.0 strategy was superseded by v1.1, and the progress-specific
v1.2 gates were superseded by v1.3. The retained v1.1 and v1.3 feature gates
remain beside the [current v1.4 gate-selection strategy](../testing/20260825-v1.4-validation-strategy.md)
because v1.4 still delegates those detailed contracts to them.

Implementation plans, benchmarks, and runbooks remain in their established
directories because they are durable execution or evidence records rather than
alternate current architecture. The documentation index exposes only active
plans and current operational authority; use repository history or targeted
search for older delivery records.
