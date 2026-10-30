# Local differential testing: instancy vs timely — results

**Status:** DA fidelity sign-off approved (2026-10-30). Local-only experiment; no repository changes, CI, commits or PRs.

**Outcome:** 32 paired scenarios / 64 graph pairs, **no engine mismatches** on any first attempt.

## Pins

| Item | Value |
|---|---|
| instancy | path dep on worktree `c692fb10b3fd1c8756699b2cc63842968315957f` (clean), default features off |
| timely | git `492787b7cff9463aab8a793a306a619c4175885b` (timely 0.31.0), default features off |
| Toolchain | rustc/cargo 1.97.1 aarch64-pc-windows-msvc; nextest 0.9.150 |
| Lockfile | own `Cargo.lock`, SHA256 `20543156420C41D6…`, 56 packages |
| Instancy runtime | pool 2 threads, `TokioMode::Create{2}`, `spawn_multi(W)` |
| Timely runtime | `Config::process(W)` |

Settings per test, seeds 1–8 as `(workers, capacity)`: (1,1) (2,1) (3,1) (2,2) (3,2) (1,1024) (2,1024) (3,1024).

## Tests (final run: 14/14, first attempt, ~10 s)

| Test | What it checks | Time |
|---|---|---|
| `two_input_totals_release_only_when_both_inputs_pass_the_time` | Exchanged binary operator with retained per-key totals; one input closed while the other holds | 0.82 s |
| `product_notifications_follow_the_incomparable_cut` | `Product<u64,u64>` notifications at cut `{(0,2),(2,0)}`, where only `(1,1)` completes | 0.65 s |
| `nested_feedback_notifications_hold_until_the_root_epoch_passes` | Exchanged notified aggregate in a 2-iteration inner loop on the feedback path of a 1–3 iteration outer loop, plus a root aggregate; body probe in native `[e,i,j]` time | 1.4 s |
| `live_dataflows_on_one_worker_set_progress_independently` | 2/8 copies on one worker set; graph 0 held while siblings advance and one sibling really completes | 6.9 s |
| 10 helper tests | Comparator, Product order, late/incomparable frontiers, premature partial totals, model exactness, witnesses, completion witness | <0.1 s each |

## What each checkpoint enforces (per engine, ≤10 s)

- Operator receipts reached; every probe frontier `<= F` and then exactly the prescribed `F` (timely: meet of all worker views).
- No output at unfinished times for completion-gated observations; same-time, same-key additions before release.
- The completed region matches the exact model (one full `[key, sum, count]` per key/time).
- No late record in an already completed region.
- Graphs whose probes are empty have really completed (instancy join; timely dataflow retired on every worker).
- Timely and instancy completed regions are equal per phase, including actual worker, native time and multiplicity.

This is a reference-first full replay (timely, then instancy), with snapshots compared afterwards. It is not simultaneous cross-engine quiescence.

## Run history (preserved as recorded)

| Run | Result | Note |
|---|---|---|
| Early sample, first run | 9/9 | |
| Product mutant (emit on receipt) | FAIL, as intended | "premature output … F=[[0,0]]" |
| Run after restore | 8/9 | stale mutant binary (restored file kept old mtime); rebuilt → 9/9 |
| Full suite, first run | **11/12** | all 4 differential tests passed; my helper test `a_held_time_without_a_same_key_extension_is_rejected` had a wrong premise, now fixed |
| Nested mutant | FAIL, as intended | "premature output … g0.inner-totals F=[[0,0,0]]" |
| Full suite, final | 12/12 | |
| Review deltas, first run | **14/14** | capacity on exposed loop edges + per-graph completion witness |

## Cluster mode (user-directed follow-up, not DA-reviewed)

**Outcome:** instancy split over **two nodes on loopback TCP** matched in-process timely on all 32 paired scenarios / 64 graph pairs, **first attempt 20/20** (4 cluster tests + the 14 existing tests + 2 cluster helpers).

| Item | Value |
|---|---|
| How to run | `cargo nextest run --features cluster --no-tests fail` (`cluster` = `instancy/transport` + tokio; off by default) |
| Topology | `node-a` + `node-b`, separate instancy runtimes; total workers 2/3/4 split 1+1, 1+2, 2+2 |
| Transport | odd seeds: dedicated per-dataflow TCP pair; even seeds: one shared connection pool for every graph |
| Reference | unchanged in-process timely with the same total workers |
| Probes / completion | meet of every worker's view on both nodes; graph completion = both nodes joined OK |
| Codec | nested recipe's records cross exchanges as a codec'd `Wire` newtype (cluster builds only) |

| Run | Result | Note |
|---|---|---|
| Cluster build | 3 compile errors, fixed | transport-type and `ExchangeData` timestamp bounds |
| Cluster suite, first run | **20/20** | multi 15.0 s, nested 2.1 s, product 1.5 s, totals 1.0 s |
| Cluster mutant (emit on receipt) | FAIL, as intended | `[instancy-cluster-dedicated] … premature output … F=[[0,0,0]]` |
| Final source, default features | 14/14 | in-process result unchanged |
| Final source, cluster features | 20/20 | 23 s |

Signed-off in-process source was preserved byte for byte locally. The lockfile gained `crc32fast` (instancy transport) and a direct `tokio` entry.

Cluster limits: loopback only (no real network latency, loss or partitions); 2 nodes; no node failure or rejoin; instancy-vs-timely transport isn't compared (timely stays in-process).

## Known differences and limits

- Timely root timestamps must refine `()`, so its Product fixture lives in a `Product<u64,u64>` child scope.
- Instancy probe updates can coalesce; only supported checkpoint plateaus are compared, not every internal transition.
- Per-edge capacity covers exposed edges, including loop entry, fork and exit; hidden loop channels stay at 1024 and feedback is unbounded.
- No performance claims; builders accept `Observe::{Capture, Count, Discard}` for possible later local timing.
- Size: 2,857 Rust lines + 105 docs/config lines (above the 1,200–1,600 estimate, accepted by the DA).

## Reproduce

```powershell
cd differential-testing
$env:INSTANCY_PROGRESS_CHECK = 'panic'
cargo nextest run --no-tests fail                      # in-process suites
cargo nextest run --features cluster --no-tests fail   # plus cluster mode
```

## Evidence

The raw run logs, evidence receipts and the DA's sign-off note were recorded
locally during the 2026-10-30 runs and are not included in this branch. The
DA-signed-off version is the in-process package at the hashes listed in the
evidence receipts (`lib.rs` 8EE2A8A4…, `instancy_backend.rs` E013E47C…,
`timely_backend.rs` F7E6A777…); later files were added on user direction
without DA review.
