# ClaimSession and eggNOG C2 Delivery Retro

Date: 2026-08-17

## Outcome

ClaimSession authority-loss handling, generic external-tool cancellation,
PostgreSQL pressure, and the eggNOG two-process C2 gate are closed. The
accepted benchmark record preserves the tested implementation, harness, and
input lineage and records independent zero-compute replay; this retro adds no
runtime evidence.

## Delivery Shape

**Expected:** Plan PR #29 defined two delivery PRs for ClaimSession and generic
tool cancellation/C2. Its initial technical plan was 388 lines.

**Actual:** The plan grew to 1,458 lines before implementation proceeded. After
#29, delivery used six code PRs (#31, #41, #46, #47, #48, and #49) plus four
delivery/governance documentation PRs (#36, #40, #43, and #45). In PR #49,
tests and benchmarks contributed 4,213 of 5,052 additions, versus 573 additions
under `src`, reflecting the cost of proving cancellation and acceptance
behavior rather than the runtime change alone.

**Delta:** The intended two-PR topology did not hold. Late rebaseline reduced
the scope of individual repairs but added topology and process overhead, while
decision and verification detail continued to expand.

**Rule To Carry Forward:** Validate the dominant scaling unit at representative
scale before broad implementation or hardening. Keep final evidence separate
from planning detail.

## Review Loop

**Expected:** Review would converge on bounded candidates within the frozen
delivery contract.

**Actual:** PRs #29, #31, and #49 accumulated 47 automated review rounds and
173 bot inline findings. GitHub review totals are higher because owner replies
to inline threads are also represented as review events; raw review counts are
therefore not equivalent to independent review rounds.

**Delta:** Per-change review repeatedly invalidated exact-head status and made
the review event count a noisy progress measure.

**Rule To Carry Forward:** Use exact-head review for a final candidate, batch
feedback within the frozen contract, and calibrate when feedback expands scope
or no bounded convergence path remains. Do not create universal change-size or
review-round thresholds.

## Acceptance Effort

**Expected:** Real C2 would be the dominant final scientific execution gate.

**Actual:** The accepted cold C2 took 4,378.48 seconds (about 73 minutes), while
the surrounding plan, implementation, automated review, fixes, and evidence
delivery spanned multiple calendar days. GitHub timestamps establish event
order and elapsed calendar bounds but cannot allocate exact active working
hours.

**Delta:** Delivery latency was dominated by specification, verification, and
review convergence rather than the accepted C2 runtime itself.

**Rule To Carry Forward:** Evidence-only successors must preserve mechanically
identifiable code, harness, and input lineage. When those identities are
unchanged, do not repeat an expensive acceptance run merely to attach it to a
later documentation SHA.

## Promoted Authority

The three rules above are promoted to `AGENTS.md`. Task-specific candidate
SHAs, detailed measurements, and rejected run history remain in the [accepted
benchmark record](../../docs/benchmarks/20260814-v1.0-claim-session-cancellation-acceptance.md),
not in repository instructions. The review totals in this retro describe this
delivery only and do not establish numeric repository gates.

## Continuation

Cache seeding continues one adapter family at a time. With eggNOG C2 accepted,
the next gate is InterPro-Pfam serial sanity plus independent zero-compute
replay, then its C1 and C2 gates after acceptance. The shared Store is a
test/cache-seeding Store: preserve immutable state, retain the prior
export/readback validation as historical precondition, and refresh only
health, identity, input, output-isolation, and capacity checks before that
lane.
