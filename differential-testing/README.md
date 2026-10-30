# instancy-differential

A standalone package that runs the same small programs on instancy and on
timely-dataflow with identical timestamped inputs, and compares exact output
multisets and probe frontiers at prescribed causal cuts. It is its own Cargo
workspace with its own lockfile: it is not a member of the repository
workspace and no CI runs it. Run it from this directory.

## Pins

- instancy: path dependency on `../instancy` (this branch's instancy).
  Results in `docs/` were recorded against base
  `c692fb10b3fd1c8756699b2cc63842968315957f`, default features off.
- timely: git `492787b7cff9463aab8a793a306a619c4175885b` (master on
  2026-10-30), default features off.
- Toolchain used: rustc/cargo 1.97.1 (aarch64-pc-windows-msvc), nextest
  0.9.150. This makes no claim about instancy's declared Rust 1.85.
- `Cargo.lock` here is this package's own lockfile.

## Run

```powershell
$env:INSTANCY_PROGRESS_CHECK = "panic"
cargo nextest run --no-tests fail
cargo fmt --check
cargo clippy --all-targets
```

Cluster mode (user-directed follow-up, not part of the DA-reviewed result):

```powershell
$env:INSTANCY_PROGRESS_CHECK = "panic"
cargo nextest run --features cluster --no-tests fail
cargo clippy --all-targets --features cluster
```

The `cluster` feature enables instancy's `transport` feature and runs the same
four recipes with instancy split over two nodes (`node-a`, `node-b`), each
with its own runtime, connected over loopback TCP. Odd seeds use dedicated
per-dataflow connections and even seeds one shared connection pool for all
graphs. Total workers per seed are `CLUSTER_SETTINGS` (2-4, split 1+1, 1+2,
2+2); timely runs in-process with the same total. Each worker's probe view is
global and the engine meets all of them; outputs, inputs and joins use the
nodes' global worker order. With `transport`, exchanged records need a codec,
so cluster builds carry the nested recipe's `(Vec<u64>, Vec<i64>)` records
across its exchanges as a codec'd `Wire` newtype (one map on each side);
default builds are unchanged.

`.config/nextest.toml` runs one test process at a time, with no retries and a
120-second per-test bound. Failure handling relies on that isolation: on the
first error a test prints its diagnostic, forgets the live engine handles
(timely's `WorkerGuards` and instancy's runtime join on drop) and panics. That
is not a general cleanup strategy.

## What a test does

Four differential tests, each over seeds 1..8 with `(logical workers,
instancy capacity)` from `SETTINGS` (32 paired scenarios, 64 graph pairs):

- `two_input_totals_release_only_when_both_inputs_pass_the_time`: exchanged
  binary operator with retained per-key totals.
- `product_notifications_follow_the_incomparable_cut`: `Product<u64, u64>`
  notifications at the cut `{(0,2), (2,0)}`.
- `nested_feedback_notifications_hold_until_the_root_epoch_passes`: an
  exchanged notified aggregate in a two-iteration inner loop on the feedback
  path of a 1-3 iteration outer loop, plus a notified root aggregate; body
  probe in native `[epoch, outer, inner]` time and a root probe.
- `live_dataflows_on_one_worker_set_progress_independently`: 2 or 8
  (alternating) copies of the nested program in one timely execution and on
  one instancy runtime; graph 0 stays held while siblings advance and the last
  sibling completes, then graph 0 is extended and released.

This is a reference-first full replay: timely runs every phase, then
instancy, each checking every prescribed cut independently; the retained
per-phase snapshots are compared afterwards. It is not a simultaneous
cross-engine quiescence check. At every phase each engine must, within 10
seconds:

- reach the receipts its gated operator should have, and the prescribed
  frontier `F` at every probe (timely: the meet of all worker views), never
  showing a frontier that is later than or incomparable to `F`;
- deliver, at times complete under `F`, exactly the modelled totals (one full
  `[key, sum, count]` per key and time) and nothing at unfinished times;
- never deliver a record into a region completed by an earlier snapshot.
- report every graph whose probes are all empty at the cut as completed:
  instancy joins that graph's `spawn_multi` handle (once, on a helper
  thread, reused at cleanup); timely requires the graph's dataflow identifier
  to be gone from `installed_dataflows()` on every worker. In the
  multi-dataflow test the closing sibling must complete while graph 0 is
  still held.

The two engines' completed regions, including actual worker, native time and
multiplicity, must be equal at every phase. Finally, all inputs close, every
frontier is empty and every graph and worker completes normally.

## Extended suites (user-directed follow-ups, not DA-reviewed)

| File | What it adds |
|---|---|
| `tests/operators.rs` | Operator breadth (broadcast, branch/merge, flat_map, distinct, per-record delay, reduce, gather, count, rebalance; exactly modelled) and 48 random programs |
| `tests/scale.rs` | Long streams (80-120 epochs, up to 40 records per epoch and graph) and 24 live dataflows |
| `tests/configs.rs` | 1 and 4 pool threads, `per_stage_parallelism`, `IoMode::Async`, combined |
| `tests/soak.rs` | `#[ignore]`; random scenarios for `DIFF_SOAK_SECS` (default 600) |
| `tests/cluster.rs` | Also breadth and 16 random programs over two loopback nodes |

```powershell
$env:INSTANCY_PROGRESS_CHECK = "panic"
cargo nextest run --release --features release-check --no-tests fail        # release + progress checker
$env:DIFF_SOAK_SECS = "600"
cargo nextest run --release --features release-check --profile soak --run-ignored only --no-capture soak
```

`DIFF_TIMING=1` prints each engine's wall clock per scenario (diagnostic,
includes checkpoint polling; not a benchmark). `take`/`take_while` and the
placement of `rebalance` depend on arrival order and are not compared
directly. Random programs have no independent model; the test requires most
programs to produce totals so empty outputs cannot pass vacuously.

## What it does not establish

- Identical construction for Product root time: timely root timestamps must
  refine `()`, so its Product fixture lives in a `Product<u64, u64>` child
  scope of an otherwise empty `u64` dataflow, with native inputs and probe.
- Lossless internal progress history: instancy's public probe is a
  coalescing watch; only supported cut plateaus are compared.
- Capacity coverage of hidden loop channels: exposed edges, including each
  loop's entry and exit edges, get the per-edge capacity; iterate-internal
  channels stay at 1024 and feedback is unbounded.
- Performance of any kind. Builders accept `Observe::{Capture, Count,
  Discard}` for possible later local timing, but nothing here is timed.
- Program shapes beyond the recipes listed in the tests.
