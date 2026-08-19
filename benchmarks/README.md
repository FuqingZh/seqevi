# Benchmarks

`annotate_fixture.py` measures SeqEvi FASTA staging, Store reuse, artifact
handling, and DuckDB result materialization without attributing fixture timings
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
that are intentionally not part of the public result catalog:

```bash
PYTHONPATH=src pdm run python benchmarks/annotate_real.py \
  --adapter eggnog \
  --fasta proteins.fasta \
  --output /tmp/seqevi-real/output.duckdb \
  --executable /opt/eggnog/bin/emapper.py \
  --database /data/eggnog/5.0.2 \
  --store /tmp/seqevi-real/store \
  --threads 8 \
    --report /tmp/seqevi-real/benchmark.json
```

`claim_session_pressure.py` runs the approved PostgreSQL ClaimSession matrix
through the production persistence implementation. It records per-operation
SQL and transaction counts, pool-acquisition timing, PostgreSQL activity and
lock-wait samples, receipt and bounded-sweeper rows, and latency while renew
traffic overlaps acquire/finalize. It then waits for retained receipts and
proves zero residual coordination, including one protected open receipt per
lane before the 120-second retention boundary and none after the bounded
sweep. Because the harness invokes persistence directly, HTTP 412/503 status
observations are explicitly unavailable; HTTP response behavior belongs to
the fault-injection gates:

```bash
PYTHONPATH=src:. pdm run python benchmarks/claim_session_pressure.py \
  --database-url postgresql+psycopg://seqevi@127.0.0.1/seqevi_pressure \
  --report /tmp/seqevi-pressure.json
```

`c2_acceptance.py` owns the frozen real two-process C2 launch and independent
replays. It verifies the exact input identity set, artifact references,
coordination cleanup, cache-only replays, and that no matching external tool
remains. Annotation children use `acceptance_containment.py`: each command has
a finite internal `--timeout-seconds`, a longer finite ToolRunner-owned
watchdog, bounded TERM/KILL cleanup, and independent stdout/stderr paths. The
benchmark-only `acceptance_annotation.py` bridge propagates watchdog TERM into
the in-process CLI so the unchanged inner adapter `ToolRunner` also cleans its
separate process group; completed failures remain stable for idempotent C2
reaping. If one initial child fails, its error remains primary while any
sibling-cancellation error is retained as attached cleanup diagnostics. C2
retries an asynchronously interrupted cancellation until the child is terminal
or its cancellation event is set, then applies a 65-second maximum cleanup wait
per child instead of falling through to the annotation watchdog. C2
invokes that frozen bridge by explicit file path under `python -P`;
the child keeps `PYTHONPATH` limited to candidate `src`, and the bridge loads
its sibling containment helper without relying on repository-root import
state. The Store endpoint and database must be fresh, isolated, and served by
the exact candidate head; the two caller-owned FASTAs and named execution
profile remain external:

```bash
PYTHONPATH=src:. pdm run python benchmarks/c2_acceptance.py \
  --blf /path/to/Trichoderma_reesei_BLF.fasta \
  --uniprot /path/to/Trichoderma_reesei_uniprot.fasta \
  --profile eggnog-5.0.2 \
  --store http://127.0.0.1:18085 \
  --database-url postgresql+psycopg://seqevi@127.0.0.1/seqevi_c2 \
  --threads 64 \
  --output-root /tmp/seqevi-c2
```
