# TLA+ models of progress accounting and the cluster transport

This folder holds small, precise models of instancy's progress accounting and of its cluster transport, written in [TLA+](https://lamport.azurewebsites.net/tla/tla.html), and the scenarios that the TLC model checker runs against them.

- **Design:** [progress-accounting-design.md](../progress-accounting-design.md).

## In one paragraph

Each model is a tiny board game. Two or three workers run copies of a few operators. Records move between operators, and each worker keeps a book of counts and mails its changes to the other workers; mail can be late. TLC plays **every possible game** up to a size limit, and checks after every move that certain rules hold. If a rule can break, TLC prints the exact sequence of moves that breaks it.

There are three kinds of scenario:
- **Proposal scenarios** use the design's rules (port-counted progress, PCP). They must never break a rule or get stuck.
- **Bug scenarios** use today's rules, or the design with one rule removed. Each must break in the named way. That shows the model is detailed enough to catch the real bug, and that the rule is needed.
- **Sanity scenarios** show that the interesting outcomes actually happen, for example that a run with real traffic, including a loop, can finish. Without them a "pass" could be empty.

There are four specs:
- **[`PortCountedProgress.tla`](./PortCountedProgress.tla)** is generic over small graphs. It covers acyclic dataflows, and loops folded into the outer book as instancy tracks them today, with and without the guard edge an earlier plan proposed. That loop handling is no longer planned (design doc §5.1.7). The model is kept because, without the guard, it reproduces today's "probe after a loop" bug (H11).
- **[`LoopScopes.tla`](./LoopScopes.tla)** is a focused model of nested loop scopes, the end state built in step 3 of the plan, with timestamps of the form (minute, lap).
- **[`NestedScopes.tla`](./NestedScopes.tla)** puts one loop on another loop's feedback path, three levels deep, and keeps the links between levels cached as an incremental tracker does (decision log O1).
- **[`ClusterTransport.tla`](./ClusterTransport.tla)** models the cluster transport and teardown after PR9 (#300): pooled connections with pinned lanes, faults, clean closes, linger, `Cancel` and `DataflowId` reuse (decision log O16). It has its own section below.

**Old labels in the files.** Comments in the specs, MC modules and `make-models.ps1` still use the earlier plan's labels: "PR4c", "PR5", "PR6a" and "interim". All of them are now part of step 3 (design doc §5.7; label table in Appendix B), and "interim" means flattened loops. The comments were left unchanged so that the spec and config hashes recorded with the results still match.

## The rules checked

| Property | Plain meaning | Design doc |
|---|---|---|
| `FrontierSafe` | While any work is outstanding, every live worker's book still shows something that could produce it, so no frontier passes outstanding work. Work includes records on edges, records a sink has not handed over yet, and anything an operator holds. | S1 (§2.2). This is the property that Naiad, FORTE 2013 and ITP 2021 prove for timely. |
| `ObserverSafe` | The claim an earlier plan made for flattened loops. At every location *outside* loop bodies, the tracker never lets a frontier pass work that can still reach that location through the real data graph, laps included. | §5.1.7 (no longer planned) |
| `ScopeSafe`, `ParentSafe` | Nested scopes. Frontiers inside the loop, in (minute, lap) time, and after it, in minutes, never pass outstanding work. | §5.1.7, scope bridge |
| `InnerSafe`, `OuterSafe`, `RootSafe` | The same at each of three levels, with the inner loop on the outer loop's feedback path. | §5.1.7 |
| `DerivedZero` | When every raw count in a worker's book is zero, its cached imports and exports are empty too. This is the debug assertion the design authority required (decision log R38). | §5.1.7: "its exported capability set must become empty" |
| `NoDataAtDone` | No record ever reaches an operator that has shut down. | S3 |
| `NoLateData` | A counting operator never receives a record at or before a time it has already reported. | The user-visible effect of S1. |
| `CompleteMeansEmpty` | When every worker reports success, no work is left anywhere, including pending sends to the user. | S2c |
| `NoResidual` | Once every worker's seal has been applied, all counts are zero. | S2b, V5 |
| no deadlock | The run never gets stuck before every worker succeeds. | §2.6 |
| `EventuallyDone` | Under fair scheduling, every worker eventually succeeds. | L1–L4 |
| `NotifyProgress`, `ScopeProgress`, `UProgress` | Under fair scheduling, with the input held open at the last epoch, counts at earlier epochs are still eventually reported, inside and after loops. | Frontiers advance while the input is open. |

## What is modelled

**In `PortCountedProgress.tla`:**
- **Workers** that run the operators they host. Hosting can be heterogeneous.
- **Operator kinds:**
  - input (epochs and closing);
  - map;
  - branch (each record goes to one output: round the loop or out of it, with a lap budget);
  - notify (count or reduce), holding a capability per time;
  - async (`unary_async`), holding a capability per task;
  - sink, optionally with **egress**: the record is taken now and accepted by the external receiver later.
- **Edge kinds:** pipeline, exchange and broadcast. Broadcast may go to a subset of the workers.
- **Records on edges**, delivered in any order. "On the edge" covers every queue, buffer and network hop between the two ports. With `Transit`, a record must also *arrive* before it can be taken, so a worker can be idle while records are on their way.
- **Ledgers.** Each operator activation is atomic and writes only the worker's ledger.
- **Commits,** which may cover several activations. A commit applies the ledger to the worker's own book, then queues it to every peer on a per-sender FIFO lane. It may be split into two fragments.
- **The PCP decisions:**
  - operator shutdown (S2a): the input frontier is empty and the operator holds nothing;
  - seal markers;
  - scoped exact-zero worker success (S2b);
  - proven residuals.
- **Today's idle force-close.** It fires only when the worker is locally idle: no record has arrived for it, no async task is running, and no notification can fire. Like today's executor, it ignores pending sink sends. Three completion checks are modelled:
  - today's sum of all counts;
  - timely's "no positive count";
  - the design's "every count exactly zero".
- **Flattened loops,** as instancy tracks them today, plus the guard edge an earlier plan proposed:
  - the loop body is flattened into the outer time;
  - the tracker uses its own graph, which drops each loop's back-edge and adds a guard edge from the feedback operator to the loop's leave;
  - the real data graph keeps the back-edge;
  - frontiers are claimed only outside loop bodies;
  - a cyclic dataflow finishes by exact-zero force-close.

  An `ASSUME` refuses any tracker graph that has a cycle. The design rejects identity cycles when the dataflow is built, and closure-based reachability could not show the hang such a cycle causes.

**In `LoopScopes.tla`** (the end state, built in step 3). A loop is its own nested scope: `in → [enter → count n → branch → feedback (lap + 1) → n, or → leave] → count u`. Each worker keeps two books:
- the parent's, in minutes;
- the loop's, in (minute, lap).

Port counts at both levels are recorded once, by the worker that moves the record. Both levels travel in the same commit. Two links between the levels are derived locally and never broadcast:
- **Import:** the parent's frontier at the loop's input becomes support at the loop's boundary.
- **Export:** only the loop's own work becomes the loop's capability in the parent; the import is never exported.

A commit may be split across both levels, positives first. To keep the search small, two simplifications apply:
- each activation is committed before the same worker's next one (batching is checked in the generic spec);
- a worker shuts down all its eligible operators in one step. That is the harshest timing for S3, because an eligible operator has nothing left to do.

**In `NestedScopes.tla`** (decision log O1). Three levels: `in → [outer loop O] → count u`, where O's body is `[inner loop I] → branch → (exchange) feedback (outer lap + 1) → I`, or `→ leave`, and I's body is `branch → feedback (inner lap + 1) → branch`, or `→ leave`. Every outer lap passes through I, so I lies on O's feedback path. Each worker keeps one book of raw port counts per level, and **caches** the four derived links (two imports, two exports) between applies. Every apply, of its own commit or of a peer's batch, recomputes them in one pass: exports bottom-up, then imports top-down. This is the order step 3 implements. The derived links are never sent, and exact zero reads raw counts only.

The mutation `Export = "frontier"` exports a scope's frontier at its exit, which includes its import. On O's feedback path, I's export then supports O's frontier at I's input one lap later, which is I's next import. With real, unbounded iteration counters that support climbs one lap per pass and never ends. With `Sat = TRUE` the model caps O's feedback at `MaxJ`, which turns the endless climb into a fixed point TLC can see. The cap only adds paths, so it can make a run hang but never makes one finish early; safety is also checked without it.

## Switches

The scenarios are defined in **one table, in [`make-models.ps1`](./make-models.ps1)**. It writes `models/*.cfg`, which are generated and not committed. The proposal's values are:

| Switch | Proposal (PCP) | Alternatives |
|---|---|---|
| `Counting` | `"ports"`: produced and consumed recorded per edge | `"sourcekey"`: produced is filed under the operator output, not the edge (H12). `"optin"`: today's wiring. Only exchange edges in `Wired` record `+n`, exchange consumers always record `−n`, and pipeline and broadcast edges are never counted. |
| `Decide` | `"pcp"`; `"exactzero"` force-close for cyclic dataflows with flattened loops | `"signedsum"` (today), `"positive"` (timely), each with idle force-close |
| `Frag` | `"positivesfirst"` | `"capsfirst"` (today's order); `"none"` (never split). Nested scopes add `"parentfirst"` and `"childfirst"`: one level's section first. |
| `Justify` | `"local"`: capabilities only from input consumed in the same activation | `"frontier"`: today's `notify_at` (P-9) |
| `AsyncCap` | `TRUE` | `FALSE` (today) |
| `Seeding` | `"hosts"`: each input's initial capability counted once per hosting worker (A4) | `"local"`: today's start-up, where peers' capabilities arrive later (H13). `"workers"`: a wrong physical plan. |
| `BcastMult` | `"hosts"` | `"workers"` (wrong when a broadcast reaches a subset) |
| `Egress` | `"keep"`: the sink keeps its input credit until the receiver accepts | `"release"`: credit released when the sink takes the record (today, C-15). `"none"`: the sink hands over at once. |
| `Guards` | the guard edge `Out(feedback) → Tgt(leave)` | none: today's tracker, which misses the feedback path |
| `Import`, `Export` | `TRUE`, `"internal"` | `FALSE`; `"none"`. `NestedScopes` adds `"frontier"`: the scope also exports its imported frontier. |
| `Transit`, `KeepOpen`, `MaxLaps`, `MaxIter`, `Sat` | modelling options, not design choices | — |

## Results

All scenarios ran with TLC 2.19 (rev 5a47802), against these specs:
- `PortCountedProgress.tla`, sha256 `8DE13BE6FFCA…`;
- `LoopScopes.tla`, sha256 `4CA89DD127BB…`;
- `NestedScopes.tla`, sha256 `FC07CE14C1A4…`.

**Every result matched its expectation exactly.**

Bounds:
- **Workers:** 2, or 3 where the name ends in `3`.
- **Timestamps:** `0..MaxT`, with `MaxT = 1` unless "one timestamp" is noted.
- **Traffic:** each worker's input sends 1 record (2 in the force-close controls). In `LoopScopes`, 2 records in total. In `NestedScopes`, 1 record in total.
- **Laps:** one lap in total.
- **Splitting:** "whole" means commits are never split.

States are distinct states, and a passing scenario explored **all** of them. Sanity checks marked "simulated" use TLC's random simulation, which stops at the first example; they only need to show the behaviour exists.

### Acyclic dataflows

**Proposal: must pass**

| Scenario | What it adds | States |
|---|---|---|
| `direct_pcp` | input → exchange → sink | 513,444 |
| `chain_pcp` | input → exchange → count → pipeline → sink, two epochs, splitting | 13,750,763 |
| `direct3_pcp` | three workers, one timestamp, whole | 11,709,526 |
| `chain3_pcp` | a count on three workers, one timestamp, whole | 30,336,576 |
| `pipe_pcp` | a map and a pipeline edge into the count, one timestamp, whole | 164,786 |
| `async_pcp` | `unary_async` and a broadcast, one timestamp, whole | 13,934,242 |
| `fanout_pcp` | one output feeding two exchanges (the H12 shape), one timestamp, whole | 1,090,839 |
| `bcastsub_pcp` | a broadcast to a subset of the workers, heterogeneous hosts, whole | 3,063 |
| `hetero_pcp` | heterogeneous stages: input on w1, count on both, sink on w2 | 18,935 |
| `egress_pcp` | sink egress, records in transit, whole | 310,030 |
| `live_direct` | `EventuallyDone` under fairness | 1,026,425 |
| `live_hetero` | `EventuallyDone`, heterogeneous stages | 18,935 |
| `live_chain_open` | `NotifyProgress` with the input held open, whole | 30,198 |

**Sanity: must be violated, to show the behaviour is reachable**

| Scenario | Shows |
|---|---|
| `chain_pcp_reach` (simulated) | A run completes after every input sent and a count was reported. |
| `hetero_early` | w1 retires while records are still on their way to w2's sink. `hetero_pcp` then shows that the tail drains safely. |

**Completion checks under force-close.** Records are in transit and force-close fires only when the worker is idle. The bounds are one timestamp and 2 records per worker, the same for the failing and passing controls.

| Scenario | Check | Result |
|---|---|---|
| `direct_signedsum` | Today's sum of all counts | **Violates `NoDataAtDone`**: a `+1` and a `−1` cancel, the worker force-closes, and a record arrives afterwards (C1). |
| `direct_positive` | timely's "no positive count" | Passes (400,813 states). |
| `direct_exactzero` | The design's exact zero | Passes (400,813 states). |
| `egress_exactzero_keep` | Exact zero, with the sink keeping its credit | Passes. |
| `egress_exactzero_release` | Exact zero, with the sink releasing its credit when it takes the record (today) | **Violates `CompleteMeansEmpty`**: the run completes with a send still pending. With every invariant on, TLC reports `FrontierSafe` first. |

**Today's rules, or the proposal with one rule removed: must fail as named**

| Scenario | Rule removed or changed | Result |
|---|---|---|
| `chain_optin_exactzero` | Today's opt-in counting: the input records no `+n` (C2), even with the exact-zero check | `FrontierSafe` |
| `pipe_optin` | Opt-in counting with the exchange wired: the pipeline edge is still uncounted (H1) | `FrontierSafe` |
| `fanout_sourcekey` | Produced filed under the output instead of the edge (H12) | `FrontierSafe` |
| `chain_capsfirst` | Split commits send capability entries first | `FrontierSafe` |
| `chain_frontier_notify` | `notify_at` justified by the frontier alone (P-9) | `FrontierSafe` |
| `async_nocap` | Async tasks hold no capability | `FrontierSafe` |
| `hetero_localseed` | Workers start knowing only their own capabilities (H13) | `FrontierSafe`, in the initial state |
| `chain_optin_residual` | Opt-in counting, PCP decisions, only `NoResidual` checked | `NoResidual`: the missing `+n` is reported at the seal boundary instead of passing silently. |
| `hetero_workerseed` | Initial capability counted once per worker, not per host | Deadlock: a hang, not a loss. |
| `bcastsub_workersmult` | Broadcast counted once per worker, not per host | Deadlock: a hang, not a loss. |
| `live_hetero_workerseed` | As `hetero_workerseed`, checked for liveness | `EventuallyDone` violated. |

### Flattened loops, with an earlier plan's guard edge

Step 3 replaces flattened loops with nested scopes, so none of this ships. It stays because `loop_noguard` reproduces today's H11 bug, and because it shows why loops need nested scopes.

The graph is `MC_Loop`: in → branch; branch -exchange-> feedback → branch (round the loop); branch → leave → count (out of the loop). Records can change worker on every lap.

| Scenario | Expected | Result |
|---|---|---|
| `loop_interim` | pass | **Pass**, 4,291,170 states (one timestamp, whole): the count after the loop never fires while a record can still come out, nothing is lost, nothing hangs. |
| `live_loop` | pass | **Pass**: the loop dataflow always finishes. (Input on w1 only, because liveness can't use symmetry; 5,436 states.) |
| `live_loop_open` | pass | **Pass**: with the input held open at epoch 1, the count after the loop still reports epoch 0, so the guard edge doesn't freeze it (6,341 states). |
| `loop_reach` (simulated) | violated | A record goes round the loop, the count after it reports, and the run completes. |
| `loop_noguard` | `ObserverSafe` | **Fails as expected.** Without the guard edge (today's tracker misses the feedback path), a record on its way round the loop is invisible to the count after it. This is today's "probe after a loop says done too early" bug. |
| `loop_body_frontier` | `FrontierSafe` | **Fails as expected.** Inside the loop body the tracker can lose sight of a record coming round the back-edge, which is why flattened loops could claim frontiers only outside loop bodies. |
| `loop_inside_notify` | `NoLateData` | **Fails as expected.** A count *inside* a flattened loop body fires early. This is one reason the plan ships nested scopes together with port counting. |

### Nested loop scopes (the end state)

| Scenario | Expected | Result |
|---|---|---|
| `scope_pcp` | pass | **Pass**, 148,975 states (two records, whole commits): counts inside and after the loop never fire early, nothing is lost, nothing hangs. |
| `scope_pcp_split` | pass | **Pass**, 880,310 states: commits split positives-first *across both levels* (one record, which is all the split hazard needs). |
| `live_scope` | pass | **Pass**: every run finishes (297,738 states). |
| `live_scope_open` | pass | **Pass**: with the input held open at epoch 1, both counts still report epoch 0, including after a lap (184,094 states). |
| `scope_reach` (simulated) | violated | A record goes round the loop, both counts report, and the run completes. |
| `scope_noimport` | `ScopeSafe` | **Fails as expected.** Without the downward import, the loop's frontiers ignore records the outer input can still send in. |
| `scope_noexport` | `ParentSafe` | **Fails as expected.** Without the upward export, the count after the loop ignores records still inside it. |
| `scope_parentfirst` | `ScopeSafe` | **Fails as expected.** A split commit sends the parent's part first: right after a record enters, a peer applies the parent `−1` before the loop's `+1`, and the record briefly has no support. |
| `scope_childfirst` | `ParentSafe` | **Fails as expected.** A split commit sends the loop's part first: right after a record leaves, a peer sees it neither inside nor outside. |

### A loop on another loop's feedback path (`NestedScopes.tla`, decision log O1)

Bounds: 2 workers, one epoch (two with the input held open), outer laps `0..1` and inner laps `0..1`, 1 record, one lap of each loop. The outer lap goes to either worker.

| Scenario | Expected | Result |
|---|---|---|
| `nest_pcp` | pass | **Pass**, 538,731 states (whole commits): no frontier at any of the three levels passes outstanding work, nothing is lost, nothing hangs, the cached links always equal a fresh recompute (`CacheExact`), and they are empty whenever raw counts are (`DerivedZero`). |
| `nest_pcp_sat` | pass | **Pass**, 500,541 states: the same with the outer feedback capped at `MaxJ`, standing in for unbounded iteration counters. |
| `nest_pcp_split` | pass | **Pass**, 329,871 states: commits split positives-first across all three levels. One record passes through both loops with no laps; with laps the space grew past 13 million states without finishing. |
| `nest_pcp_twolaps` | pass | **Pass**, 974,079 states: two real outer laps, so a record re-enters the inner loop twice (no inner laps). |
| `live_nest` | pass | **Pass**: every run finishes under fair scheduling, including an outer lap through the inner loop (250,238 states). |
| `live_nest_open` | pass | **Pass**: with the input held open at epoch 1, the count after the outer loop still reports epoch 0 (82,264 states). |
| `nest_reach` (simulated) | violated | A record laps the inner loop, crosses workers on an outer lap, re-enters the inner loop at outer lap 1, the count reports, and the run completes. |
| `nest_frontierexport` | deadlock | **Fails as expected.** The inner loop exports its frontier, import included. After the input closes, its export `{(0,1)}` keeps supporting its own import `{(0,1)}` through the outer feedback, the outer export `{0}` holds the count, and the run never finishes. |
| `nest_frontierexport_derived` | `DerivedZero` | **Fails as expected.** The same mutation is caught by the assertion step 3 adds: every raw count is zero, but the cached export is not. |
| `live_nest_open_frontierexport` | temporal | **Fails as expected.** With the input held open, epoch 0 is never reported. |

## What this does and does not show

**It shows,** for these graphs and bounds:
- **Acyclic dataflows.** The proposal's rules never let a frontier pass outstanding work, never drop a record, never complete with work left (pending sends included), and never get stuck. Every worker eventually finishes, and frontiers advance while an input is still open.
- **Flattened loops.** With the guard edge, counts after a loop are safe and the dataflow finishes. Without it, today's bug appears. Frontiers inside flattened loop bodies really are unsafe, which is why the end state uses nested scopes.
- **Nested scopes.** The import and the internal-only export are both necessary. With both, counts inside and after the loop are safe, everything finishes, and both counts make progress while the input is open. Commits must be split positives-first across levels; splitting by level fails both ways.
- **A loop on another loop's feedback path** (`NestedScopes`). With imports and exports cached between applies and recomputed in one pass (exports bottom-up, then imports top-down), every level stays safe, everything finishes, and the count after the outer loop reports earlier epochs while the input is open. Exporting the imported frontier is not just wasteful: on a feedback path it supports itself and never drains, and `DerivedZero` catches it.
- **Each core rule is needed.** Removing any one of these breaks safety, and TLC finds the breaking sequence:
  - per-edge counting;
  - positives-first splitting;
  - local justification;
  - async capabilities;
  - agreed initial counts;
  - the sink keeping its credit;
  - the guard edge, for flattened loops;
  - the import;
  - the export.
- **Completion checks.** Today's check loses data even when counting is complete. timely's check and exact zero both pass under the same conditions.
- **Accounting faults are loud.** A wrong multiplicity hangs instead of losing data, and a missing `+n` is reported at the seal boundary.

**It does not show:**
- **Broadcasting the export.** `NestedScopes` never sends derived links. Step 3 makes that a debug assertion (decision log R38), and the Rust protocol property test's `BroadcastDerived` mutants check it.
- **Scoped worker success through a back-edge** (S2b(c) uses data reachability, back-edges included). The loop graphs host every operator on every worker, so they don't exercise it.
- **Exact implementation timing of the import.** `LoopScopes` computes it fresh at every step; `NestedScopes` caches it and recomputes it once per apply, in the order step 3 implements.
- **Several features:**
  - binary operators, and justification across inputs with different summaries;
  - `retain` and its cross-task release;
  - failures and transport loss;
  - batches of more than one record;
  - sibling loops, and more than two levels of nesting.
- **The real executor's scheduling.** Scheduling, wake-ups and backpressure are abstracted away; liveness assumes fair scheduling.
- **Larger systems.** Model checking covers only these bounds. It is strong evidence, not a proof for every size; that would need TLAPS or Isabelle (design doc §5.8).
- **That the Rust code matches the model.** The conformance matrix, the debug checker and the planned code-level checks cover that.

## The cluster transport model (`ClusterTransport.tla`, decision log O16)

The progress models treat the network as one FIFO lane per sender. That hid this round's transport bugs:
- frames lost on reconnect (C7);
- a run taking a reused `DataflowId`'s frames;
- a peer's clean close dropping our goodbye (3a77274);
- a write error with split TLS halves hanging the peer (4be0e70).

This model opens the lane up.
- **Nodes and connections.** Two nodes share a pool of one or two connections. Each direction has a write half, a read half and a wire; FIN and resets follow the socket semantics listed in the spec's header, including "dropping a split write half sends nothing".
- **Lanes.** A run's frames pass an outbox, a bridge that numbers them, and a writer queue. PR9 pins each lane to one connection, and a gap is fatal.
- **Runs.** Port-counted progress is abstracted to one record, the progress counting it, the final progress, a goodbye and an ack. Runs can be one run, a retry (with a fresh or a reused id), or two concurrent dataflows on two connections.
- **Teardown.** Link retirement, `Cancel` (forwarded failure or requested stop), pending controls and the release of a manager, with linger.

### The rules checked

| Property | Meaning |
|---|---|
| `NoLossOk` | A run whose workers succeeded consumed the record its peer sent. |
| `NoCrossRun` | A run never takes another run's frame, payload or `Cancel`, as its own. |
| `NoCorruptAccepted` | A corrupted record is never accepted. |
| `NoNeedlessDrop` | The transport never drops a lane frame while it could still deliver it (writer up, connection healthy, peer reading). |
| `NoSpuriousFail` | With no fault and no worker failure, no run fails. A requested stop drains to success. |
| `NoCollateralFail` | A failed connection fails only the runs that used it, in either direction. |
| `FreshRetryOk`, `ReuseRejected` | Safety only: on a healthy transport a retry with a fresh id never fails or is refused; one that reuses the id is never accepted. |
| `Termination` | Every run ends and unregisters, and per job every connection closes, on both nodes: no failure leaves a node waiting forever. |
| `RetryEventuallyOk` | The retry eventually succeeds on both nodes. |
| `ReuseEventuallyRejected` | A retry that reuses the id is eventually refused on both nodes. |

### Switches

| Switch | PR9 | Earlier or broken |
|---|---|---|
| `Pin` | `TRUE`: lanes pinned, numbered, a gap fatal, the pool refilled on registration | `FALSE`: lanes spread over connections, a reorder buffer, a gap timer, background redial |
| `WriteErrClose` | `TRUE`: a write error closes the whole connection (4be0e70) | `FALSE`: it drops only the write half |
| `Split` | modelling option: split halves send no FIN when dropped | `FALSE`: owned halves |
| `CleanEof` | `"keep"`: pinned lanes keep writing after the peer's clean close (3a77274) | `"fail"` |
| `Linger` | `TRUE`: a release shuts the write halves and reads until the peer's FIN | `FALSE`: close at once |
| `RunIds` | `"pr9"`: a finished id is refused, its late frames dropped, queued frames bound to their registration (52314fb) | `"old"` |
| `RejectCancel` | `FALSE`: a refused spawn sends nothing under another run's id (R53) | `TRUE`: `a7c4b1c`, which still broadcasts `Cancel` |
| `Crc` | `TRUE`: frame checksums, always on from protocol v6 | `FALSE` |
| `Conns`, `Runs`, `Concurrent`, `Crosswise`, `Reuse`, `Faults`, `MaxFaults`, `FaultsRun1`, `MaxStops`, `MaxFails`, `PerJob` | bounds and modelling options | — |

### Results

Bounds: two nodes, one record per node and run, at most one fault, one requested stop and one worker failure. TLC 2.19, 4 threads.

| Scenario | Expected | Result |
|---|---|---|
| `ct_pr9` | pass | **Pass**, 8,709 states: a healthy transport with a requested stop. |
| `ct_pr9_faults` | pass | **Pass**, 235,147 states: one reset, write error or corrupted frame, and a stop. Never `Ok` with lost or corrupted data. |
| `ct_pr9_concurrent` | pass | **Pass**, 26,648,737 states (about 15 minutes): two dataflows pinned alike to two connections and one fault of any kind; only the dataflow on the faulted connection fails. |
| `ct_pr9_crosswise` | pass | **Pass**, 226,561,926 states (about 1 hour 45 minutes): the two dataflows pinned crosswise, as when the nodes registered them in opposite orders, so each connection carries both; one reset (every fault kind breaks the whole connection the same way), managers kept. A dataflow fails only if it used the reset connection in either direction. |
| `ct_pr9_retry` | pass | **Pass**, 649,380 states: run 1 fails on one node; a retry with a fresh id never fails and never takes run 1's late frames. `live_ct_freshretry` shows it succeeds. |
| `ct_pr9_reuse` | pass | **Pass**, 22,325 states, with R53's rule: a retry that reuses the id is never accepted, and nothing else fails or crosses. `live_ct_reuse` shows it is refused on both nodes. |
| `live_ct` | pass | **Pass**, 235,147 states: with any one fault and a stop, every run ends and every connection closes. |
| `live_ct_retry` | pass | **Pass**, 381,658 states: a reset during run 1 fails it; once each node noticed, its retry refills the pool and succeeds on both nodes. |
| `live_ct_lostcancel` | pass | **Pass**, 445,807 states: the reset may hit a retry before its peer registered, so its `Cancel` is lost; the peer's retry ends by the handshake timeout, never hangs. |
| `live_ct_freshretry` | pass | **Pass**, 649,380 states: run 1 may fail on one node; the retry with a fresh id eventually succeeds on both nodes. |
| `live_ct_reuse` | pass | **Pass**, 22,325 states, with R53's rule: a retry that reuses the id is eventually refused on both nodes, and every run ends. |
| `ct_old_healthy` | pass | **Pass**, 1,039 states: the earlier design with one connection and no fault loses nothing; the reconnect is what loses frames. |
| `ct_writeerr_halfclose_owned` | pass | **Pass**, 12,064 states: a write error that drops only the write half is harmless with owned halves, whose drop sends FIN. |
| `ct_reach_stop` | violated | A requested stop drains to success on both nodes. |
| `ct_reach_retry` | violated | Run 1 fails on one node, and the retry succeeds on both. |
| `ct_reach_contained` | violated | A fault fails the dataflow on one connection while the other succeeds on both nodes (8,332,069 states). |
| `ct_c7` | `NoLossOk` | **Fails as expected** (C7). In the earlier design, frames the bridge accepted while a reset connection was being redialled are dropped without a number, and the peer completes without the record. |
| `ct_c7_hang` | deadlock | **Fails as expected.** The same loss with nothing behind it to reveal a gap: both runs wait forever. |
| `ct_h7` | `NoSpuriousFail` | **Fails as expected** (H7). With a healthy transport, the gap timer fails a run whose frames were only reordered across connections. |
| `ct_writeerr_halfclose` | deadlock | **Fails as expected** (before 4be0e70). A write error drops only a split write half; the peer sees no FIN and its run waits forever. |
| `ct_cleaneof_fails` | `NoNeedlessDrop` | **Fails as expected** (before 3a77274). A peer's clean close fails our pinned lanes, so our goodbye is dropped although the peer still reads. |
| `ct_nolinger` | `NoSpuriousFail` | **Fails as expected.** A release that closes its sockets at once drops queued frames, and the peer loses the last frames it needed. |
| `ct_reuse_old` | `NoCrossRun` | **Fails as expected** (before 52314fb). A retry that reuses the id takes run 1's late frames as its own. |
| `ct_nocrc` | `NoCorruptAccepted` | **Fails as expected.** Without checksums, a corrupted record is accepted. |
| `ct_reuse_cancel` | `NoSpuriousFail` | **Fails: open on `a7c4b1c`, fixed by R53.** Node a finishes run 1 on b's ack while b's last step is pending. a's retry reuses the id and is refused, but its spawn guard broadcasts a failing `Cancel` under that id, and b's run 1 fails. One run ends `Ok` on a and `Err` on b, with no fault and no worker failure. |

### What it shows, and what it does not

**It shows,** for two nodes and these bounds:
- PR9's transport never reports success with lost, crossed or corrupted data, and never drops a frame it could still deliver.
- A failed connection fails only the runs that used it in either direction, however each node pinned them, and no node is left waiting forever. A lost `Cancel` ends at the handshake timeout.
- A retry with a fresh id eventually succeeds on both nodes; one that reuses the id is eventually refused on both, with R53's rule.
- Each of these fixes is needed; removing it brings back its bug: closing the whole socket on a write error, keeping lanes after a clean EOF, linger, checksums and R53's rule. For pinning and id handling, the model switches whole historical designs (before PR9, before 52314fb), so it shows that each design was broken, not which of its parts mattered.

**It does not show:**
- **That a failed connection fails *every* run using it.** `NoCollateralFail` is one-way; a run on a failed connection may still succeed when it no longer needed the lost frames, and `NoLossOk` covers the case where it did.
- **More than two nodes**, or link retirement through a third node (R47's three-node shapes).
- **The lifecycle machine's soft failures.** A send failure the code settles later (`settle_unsent`) is decided at once here.
- **Timing.** Linger has no deadline, where the code caps it at 10 s, and the handshake and ready barrier are one step whose timeout fires only when the peer's run already ended.
- **Bounded id memory.** The model remembers every finished id; PR9 keeps the last 16,384.
- **Data-channel close sentinels and real progress.** Completion is the abstract "record and progress received"; the progress models cover the rest.
- **Dedicated mode,** where each dataflow has its own connections.

## Reviews

GPT-6 Astra reviewed each model for fidelity. Neither review found a counterexample to the proposal.

**First review, of the acyclic model:**

| Finding | Severity | Resolution |
|---|---|---|
| Loops not modelled | high | Now modelled: flattened loops (with and without the guard edge) and nested scopes |
| `NoDataAtDone` cannot see a record a sink has taken but not handed over | high | Added sink egress (`keep` / `release`), counted pending sends as work, and added `egress_*` |
| No fan-out graph; justification only through fixed actions | high | Added `fanout_pcp` and `fanout_sourcekey` (H12). Binary operators and `retain` remain limits. |
| Deadlock checking is not liveness | high | Added fairness, liveness properties, and a liveness bug scenario |
| Force-close could fire while the worker had work | high | Added records in transit, with force-close only when locally idle |
| Broadcast multiplicity indistinguishable; only 2 workers | medium | Added `bcastsub_*` and three-worker scenarios |
| Sanity witnesses finished with no traffic | medium | They now require traffic, a lap, or a record still on its way |
| The runner accepted any failure; no provenance | medium | Exact expected results; TLC version and spec and config hashes in `results.csv` |

**Second review, of the loop models:**

| Finding | Severity | Resolution |
|---|---|---|
| Commits applied to both levels at once, so splitting across levels was never tested | high | Commits may now be split positives-first across levels. Added `scope_parentfirst` and `scope_childfirst`, which fail. |
| The recursive-export regression cannot be expressed | high | Stated as a limit; it needs a stateful bridge model |
| Closure-based reachability cannot show identity-cycle hangs | medium | `ASSUME`s refuse cyclic tracker graphs in both specs |
| The import ignored the extra lap on the feedback path | medium | Fixed. It was over-cautious, not unsafe. |
| The interim graph had no explicit leave | medium | Added a leave operator; the guard edge now ends at its input, which is checked |
| Loop scoped retirement not exercised | medium | Stated as a limit |
| No progress check while a later epoch is open | medium | Added `live_loop_open` and `live_scope_open` |

**Third review, of `NestedScopes.tla`** (a rubber-duck review; no counterexample to the proposal, no `UNCHANGED` or `EXCEPT` errors):

| Finding | Severity | Resolution |
|---|---|---|
| The sanity witness didn't prove that the cross-worker outer lap and the re-entry into the inner loop happened | medium | Added a `Track` switch and `hist` flags; `nest_reach` now requires both |
| The mutation changed both loops, and extra propagation passes were allowed with an uncommitted ledger | medium | The mutation changes only the inner loop; extra passes need a committed ledger |
| No invariant stated that a loop never exports its import while raw counts are non-zero | medium | Added `CacheExact`: the cache always equals a fresh recompute from raw counts; `DerivedZero` is also checked after success |
| Only one real outer lap | low | Added `nest_pcp_twolaps` |
| Results lacked the new spec's hash | low | Added |

**Fourth review, of `ClusterTransport.tla`'s first version** (a rubber-duck review). It found model defects, not PR9 bugs:

| Finding | Severity | Resolution |
|---|---|---|
| `Termination` counted a failed run as done before it unregistered | high | Done now means every run ended and unregistered and, per job, every connection closed both ways |
| A write failure was fatal even on a retired link, so `ct_cleaneof_fails` failed for the wrong reason | high | A retired link excuses it, as `settle_unsent` does; the clean-EOF bug is now caught by `NoNeedlessDrop` |
| Registration never refilled the pool, pending controls were not tied to a connection, and the ready barrier was weak | high | Added `Refill`; pending controls are dropped with their connection; registration applies pending `Cancel`s in the same step; a barrier timeout ends a run whose peer's run already ended |
| A write error could fire with nothing written | high | It needs a queued frame |
| No corruption or checksums | high | Added `Corrupt` and `Crc`, and `ct_nocrc`. Close sentinels stay a limit. |
| `CHOOSE` fixed routing; a close without linger kept the queue; `"old"` ids buffered payload; no concurrent lanes | lower | Routing is `\E`; an abrupt close drops the queue; fixed; added `Concurrent` and `NoCollateralFail` |

After that review, checking the counterexamples led to three more changes:
- the barrier timeout above, which `ct_writeerr_halfclose` then exposed as hiding its run hang behind the unbounded linger. That pair now runs without per-job managers, so its deadlock must be a run hang;
- `live_ct_retry` retries only once each node noticed the reset, since a retry that registers on a connection not yet known to be dead fails loudly, which is correct. `live_ct_lostcancel` covers that race;
- the `RejectCancel` switch, after `ct_pr9_reuse` showed R53's bug.

**Fifth review, of `ClusterTransport.tla` as above** (a rubber-duck review). No high-severity finding; it confirmed `ct_reuse_cancel` against the code, and that each bug scenario fails by the mechanism it names.

| Finding | Severity | Resolution |
|---|---|---|
| Concurrent runs were pinned alike on both nodes, but each node picks its least-loaded connection by its own registration order | medium | Added `Crosswise` and `ct_pr9_crosswise`: node b pins the two runs the other way round, so each connection carries both. `NoCollateralFail` already counts either direction. |
| "A failed connection fails exactly the runs pinned to it, and both sides see it" was not asserted | medium | Claim narrowed to what is checked: only those runs fail, and no node waits forever |
| `FreshRetryOk` and `ReuseRejected` are invariants, so a retry that never ran would pass | medium | Added `RetryEventuallyOk` and `ReuseEventuallyRejected` under fairness: `live_ct_freshretry`, `live_ct_reuse` |
| `Pin` and `RunIds` switch several mechanisms at once, so they can't show each part is needed | medium | Claim narrowed: they show the earlier designs were broken. Splitting them is left as a limit. |

## Running it

You need Java 11 or newer and `tla2tools.jar` from the [TLA+ releases](https://github.com/tlaplus/tlaplus/releases).

```powershell
$env:TLC_JAVA = 'C:\path\to\java.exe'          # or have java on PATH
$env:TLA2TOOLS_JAR = 'C:\path\to\tla2tools.jar'
./run.ps1                                     # every scenario; first regenerates models/*.cfg
./run.ps1 -Filter 'loop_*' -Threads 8         # a subset
./make-models.ps1                             # only regenerate models/*.cfg
```

`models/*.cfg` are generated from the scenario table and are not committed; `run.ps1` regenerates them on every run, so they always match the table. `run.ps1` prints one line per scenario and whether the result matched exactly. It appends results, with spec and config hashes, to `results.csv`, and keeps TLC's full output, including counterexample traces, in a temporary folder.

The large scenarios take 10 to 30 minutes each on 6 threads: `chain_pcp`, `direct3_pcp`, `async_pcp` and `loop_interim`. `chain3_pcp` takes about an hour. Of the transport scenarios, `ct_pr9_crosswise` takes about 1 hour 45 minutes on 4 threads, `ct_pr9_concurrent` about 15 minutes and `ct_reach_contained` about 6. Everything else finishes in seconds to a few minutes.
