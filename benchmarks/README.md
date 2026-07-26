# Benchmarks

`annotate_fixture.py` measures SeqEvi FASTA staging, Store reuse, artifact
handling, and Data Package materialization without attributing fixture timings
to a real annotation tool.

Each run creates three deterministic workloads:

- A: all sequences are new;
- B: a subset of A is fully cached;
- C: half the sequences are cached and half are new.

Example:

```bash
PYTHONPATH=src:. pdm run python benchmarks/annotate_fixture.py \
  --sequences 10000 \
  --output /tmp/seqevi-fixture-10000
```

To exercise a deployed shared Store with an independent HTTP client for every
A/B/C run:

```bash
PYTHONPATH=src:. pdm run python benchmarks/annotate_fixture.py \
  --sequences 10000 \
  --store http://127.0.0.1:18081 \
  --fresh-store-per-run \
  --output /tmp/seqevi-shared-fixture-10000
```

The target shared Store must be an isolated benchmark deployment because this
harness intentionally commits deterministic fixture evidence. Set
`--sequence-offset` to a previously unused non-negative value when rerunning
against an immutable Store.

Real-tool reports must separately record executable/runtime identity, database
resource identity, thread count, hit/no-hit composition, and external-tool time.

Use `annotate_real.py` for one official-adapter run. It invokes the same Python
orchestration as `seqevi annotate` and persists the in-process phase metrics
that are intentionally not part of the public Data Package:

```bash
PYTHONPATH=src pdm run python benchmarks/annotate_real.py \
  --adapter eggnog \
  --fasta proteins.fasta \
  --output /tmp/seqevi-real/output \
  --executable /opt/eggnog/bin/emapper.py \
  --database /data/eggnog/5.0.2 \
  --store /tmp/seqevi-real/store \
  --threads 8 \
  --report /tmp/seqevi-real/benchmark.json
```
