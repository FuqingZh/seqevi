# Run A First SeqEvi Annotation

Status: current user guide

Use this guide to configure one host-supplied annotation runtime, publish a
DuckDB result, and observe exact cache reuse. Managed dbCAN users can replace
the profile-init steps with `seqevi setup dbcan-cazyme` as documented in the
root README.

## 1. Install

SeqEvi requires Python 3.12 or newer. Direct adapter execution requires Linux.

```bash
python -m pip install seqevi
seqevi --version
```

Install and prepare the selected upstream annotation tool and its database
separately. SeqEvi does not download either one.

## 2. Create A Named Profile

Create a complete template for one official adapter:

```bash
seqevi profile init eggnog-local --adapter eggnog
```

Edit the reported TOML file so `executable` points to the supported launcher
and `resource` points to the native annotation database root. Then validate it
without launching the tool, and inspect the resolved named profile:

```bash
seqevi profile validate --config ~/.config/seqevi/profiles/eggnog-local.toml
seqevi profile show eggnog-local
```

The same flow accepts `interpro-pfam` or `dbcan-cazyme` as the adapter name.

## 3. Annotate

Choose a durable Store location. Reusing the same Store is what enables exact
evidence reuse across invocations.

```bash
seqevi annotate \
  --fasta proteins.faa \
  --output proteins.eggnog.duckdb \
  --profile eggnog-local \
  --store ./seqevi-store
```

`--output` must name a new file. `--timeout-seconds` limits each external-tool
run; it is not a whole-invocation or peer-wait deadline.

## 4. Query The Result

```python
from seqevi import scan_annotations

annotations = scan_annotations("proteins.eggnog.duckdb")
print(annotations.limit(5).pl())
```

The public relation keeps the adapter's native row grain. Aggregate a
one-to-many adapter before joining it to a one-row-per-protein table when that
is the desired output grain.

## 5. Confirm Cache Reuse

Run the same input, profile, and Store again with a different new output path:

```bash
seqevi annotate \
  --fasta proteins.faa \
  --output proteins.eggnog.replay.duckdb \
  --profile eggnog-local \
  --store ./seqevi-store \
  --json
```

The JSON summary reports `cache_hits`, `computed`, and the output path. An exact
replay normally reports the previously terminal sequences as cache hits and
does not run the external tool for them. A changed runtime, resource, adapter
contract, semantic parameter, or protein sequence intentionally selects a
different evidence key and is not a cache hit.
