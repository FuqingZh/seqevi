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
- External annotation tools and their databases remain external. SeqEvi calls
  configured executables and records their identities; it does not install,
  schedule, or distribute them.
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

## Validation

- Run `pdm run check` before claiming a change is complete.
- Contract changes require focused tests plus an update to the authoritative
  architecture document.
- Shared-store changes must be tested against PostgreSQL as well as SQLite.

## AO Delivery

- Keep AO task branches scoped to one tracked issue, and use a ready-for-review
  pull request with the issue key in its body for delivery.
- Enable native auto-merge only after required checks pass and review threads
  are resolved; do not merge a task branch directly.
