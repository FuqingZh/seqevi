# SeqEvi Repository Guidance

## Authority

- Treat `docs/README.md` as the documentation entrypoint.
- Treat documents marked `Status: approved target architecture` as the current
  implementation contract until a newer version explicitly supersedes them.
- Keep task transcripts and temporary evidence out of `docs/`.

## Product Boundary

- SeqEvi is an independent protein sequence evidence cache. Do not couple its
  domain model to Proteomics projects, WDL, Cromwell, CephFS, Docker, or
  Kubernetes.
- Caller-owned annotation databases and resources remain external: SeqEvi
  validates and mounts them, but never downloads, copies, or redistributes them.
  At the distribution edge, SeqEvi may invoke its digest-pinned first-party OCI
  runtime ephemerally; it does not install host packages, schedule workflows,
  expose a container engine, or couple Docker to the domain model.
- Keep official adapters explicit. Do not add a plugin framework in v1.
- Preserve adapter-native result schemas. Do not merge eggNOG and Pfam output
  into a universal annotation table.

## Engineering

- Support Python 3.12 and newer; use Python 3.13 for local development.
- Keep the main `annotate` path shallow and auditable.
- Use strict typed boundaries and immutable value objects for sequence and
  evidence identities.
- Use argument arrays for subprocesses and never use `shell=True`.
- Add a dependency only when it removes meaningful complexity from SeqEvi.
- Update public contracts and their tests together.
- Validate the dominant scaling unit at representative scale before broad
  implementation or hardening.

## Validation

- Run `pdm run check` before claiming a change is complete.
- Contract changes require focused tests plus an update to the authoritative
  architecture document.
- Shared-store changes must be tested against PostgreSQL as well as SQLite.
- Evidence-only successors must preserve mechanically identifiable code,
  harness, input, ToolRuntimeDigest, ResourceID, and semantic-parameter
  lineage. Do not repeat an expensive acceptance gate when that complete
  frozen execution identity is unchanged.

## AO Delivery

- For tracked-issue work, keep each AO task branch scoped to one issue and use a
  ready-for-review pull request with the issue key in its body for delivery.
  Merge readiness requires platform-native review completion against the exact
  current head SHA and zero unresolved review threads before native auto-merge
  is enabled; do not merge a task branch directly.
- Treat exact-head review as the final-candidate gate, not a review loop for
  each development commit. Batch in-contract feedback; pause remote review and
  calibrate if feedback expands the accepted scope or loses a bounded
  convergence path.
- For freeform tasks, follow the requested delivery boundary without inventing
  an issue or pull request.
