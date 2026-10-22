# A TLA+ model of progress accounting

This folder holds small, precise models of instancy's progress accounting, written in [TLA+](https://lamport.azurewebsites.net/tla/tla.html), and the scenarios that the TLC model checker runs against them.

- **Design:** [progress-accounting-design.md](../progress-accounting-design.md).

## In one paragraph

Each model is a tiny board game. Two or three workers run copies of a few operators. Records move between operators, and each worker keeps a book of counts and mails its changes to the other workers; mail can be late. TLC plays **every possible game** up to a size limit, and checks after every move that certain rules hold. If a rule can break, TLC prints the exact sequence of moves that breaks it.

There are three kinds of scenario:
- **Proposal scenarios** use the design's rules (port-counted progress, PCP). They must never break a rule or get stuck.
- **Bug scenarios** use today's rules, or the design with one rule removed. Each must break in the named way. That shows the model is detailed enough to catch the real bug, and that the rule is needed.
- **Sanity scenarios** show that the interesting outcomes actually happen, for example that a run with real traffic, including a loop, can finish. Without them a "pass" could be empty.

There are two specs:
- **[`PortCountedProgress.tla`](./PortCountedProgress.tla)** is generic over small graphs. It covers acyclic dataflows, and loops folded into the outer book as instancy tracks them today, with and without the guard edge an earlier plan proposed. That loop handling is no longer planned (design doc §5.1.7). The model is kept because, without the guard, it reproduces today's "probe after a loop" bug (H11).
- **[`LoopScopes.tla`](./LoopScopes.tla)** is a focused model of nested loop scopes, the end state built in step 3 of the plan, with timestamps of the form (minute, lap).

**Old labels in the files.** Comments in the specs, MC modules and `make-models.ps1` still use the earlier plan's labels: "PR4c", "PR5", "PR6a" and "interim". All of them are now part of step 3 (design doc §5.7; label table in Appendix B), and "interim" means flattened loops. The comments were left unchanged so that the spec and config hashes recorded with the results still match.

## The rules checked

| Property | Plain meaning | Design doc |
|---|---|---|
| `FrontierSafe` | While any work is outstanding, every live worker's book still shows something that could produce it, so no frontier passes outstanding work. Work includes records on edges, records a sink has not handed over yet, and anything an operator holds. | S1 (§2.2). This is the property that Naiad, FORTE 2013 and ITP 2021 prove for timely. |
| `ObserverSafe` | The claim an earlier plan made for flattened loops. At every location *outside* loop bodies, the tracker never lets a frontier pass work that can still reach that location through the real data graph, laps included. | §5.1.7 (no longer planned) |
| `ScopeSafe`, `ParentSafe` | Nested scopes. Frontiers inside the loop, in (minute, lap) time, and after it, in minutes, never pass outstanding work. | §5.1.7, scope bridge |
| `NoDataAtDone` | No record ever reaches an operator that has shut down. | S3 |
| `NoLateData` | A counting operator never receives a record at or before a time it has already reported. | The user-visible effect of S1. |
| `CompleteMeansEmpty` | When every worker reports success, no work is left anywhere, including pending sends to the user. | S2c |
| `NoResidual` | Once every worker's seal has been applied, all counts are zero. | S2b, V5 |
| no deadlock | The run never gets stuck before every worker succeeds. | §2.6 |
| `EventuallyDone` | Under fair scheduling, every worker eventually succeeds. | L1–L4 |
| `NotifyProgress`, `ScopeProgress` | Under fair scheduling, with the input held open at the last epoch, counts at earlier epochs are still eventually reported, inside and after loops. | Frontiers advance while the input is open. |

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
| `Import`, `Export` | `TRUE`, `"internal"` | `FALSE`; `"none"` |
| `Transit`, `KeepOpen`, `MaxLaps`, `MaxIter` | modelling options, not design choices | — |

## Results

All scenarios ran with TLC 2.19 (rev 5a47802), against these specs:
- `PortCountedProgress.tla`, sha256 `8DE13BE6FFCA…`;
- `LoopScopes.tla`, sha256 `4CA89DD127BB…`.

**Every result matched its expectation exactly.**

Bounds:
- **Workers:** 2, or 3 where the name ends in `3`.
- **Timestamps:** `0..MaxT`, with `MaxT = 1` unless "one timestamp" is noted.
- **Traffic:** each worker's input sends 1 record (2 in the force-close controls). In `LoopScopes`, 2 records in total.
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

## What this does and does not show

**It shows,** for these graphs and bounds:
- **Acyclic dataflows.** The proposal's rules never let a frontier pass outstanding work, never drop a record, never complete with work left (pending sends included), and never get stuck. Every worker eventually finishes, and frontiers advance while an input is still open.
- **Flattened loops.** With the guard edge, counts after a loop are safe and the dataflow finishes. Without it, today's bug appears. Frontiers inside flattened loop bodies really are unsafe, which is why the end state uses nested scopes.
- **Nested scopes.** The import and the internal-only export are both necessary. With both, counts inside and after the loop are safe, everything finishes, and both counts make progress while the input is open. Commits must be split positives-first across levels; splitting by level fails both ways.
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
- **The design's recursive-export regression** (§5.1.7): a scope on an *outer* feedback path must eventually export nothing. The models have no outer loop, and their import and export are computed fresh from the books rather than cached. So exporting the child's whole frontier, or broadcasting the export, can't be expressed. This needs a small stateful bridge model with cached import/export; it is the next step for loops.
- **Scoped worker success through a back-edge** (S2b(c) uses data reachability, back-edges included). The loop graphs host every operator on every worker, so they don't exercise it.
- **Exact implementation timing of the import.** The model computes it fresh at every step. The design requires the receiver to apply the parent section and refresh the import before computing child frontiers.
- **Several features:**
  - binary operators, and justification across inputs with different summaries;
  - `retain` and its cross-task release;
  - failures and transport loss;
  - batches of more than one record;
  - more than one loop.
- **The real executor's scheduling.** Scheduling, wake-ups and backpressure are abstracted away; liveness assumes fair scheduling.
- **Larger systems.** Model checking covers only these bounds. It is strong evidence, not a proof for every size; that would need TLAPS or Isabelle (design doc §5.8).
- **That the Rust code matches the model.** The conformance matrix, the debug checker and the planned code-level checks cover that.

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

The large scenarios take 10 to 30 minutes each on 6 threads: `chain_pcp`, `direct3_pcp`, `async_pcp` and `loop_interim`. `chain3_pcp` takes about an hour. Everything else finishes in seconds to a few minutes.
