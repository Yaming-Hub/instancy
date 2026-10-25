# Progress accounting: invariants, inventory and redesign

## Changes since rev 0286a9d

Rev `0286a9d` is the revision under review. It has since been rebased onto `e7ac01e` without content changes, and is now `fbf604e`. This revision re-baselines the doc on merged PR7 (#291, `e7ac01e`) and re-runs the experiments. **The recommended design (PCP) is unchanged. The migration plan has been replaced by a four-step plan (item 11).** What changed:

1. **Base and citations.** The base moved from `10a2325` to `e7ac01e`. Every `file:line` in a file that #291 changed was re-checked and updated: `runtime.rs`, `network.rs`, `network_progress.rs`, `cluster_lifecycle.rs` and the transport files. #291 did not change the other cited files, except `exchange_channel.rs`, whose changes all lie below the cited lines.
2. **Inventory (§3.2, §3.5, M-f).**
   - Rows C-7, C-8, C-9, C-12 and C-16 are rewritten for #291's final behaviour. C-9, the close sentinel, is now ✔.
   - New rows:
     - C-17: the per-peer outbox;
     - C-18: lingering readers and inbound bridges;
     - C-19: the Shared flush barrier and `close()`, including the queued frames that `unregister_dataflow` drops.
   - The re-check found **no new place where work lives outside PCP's edge contract**. The outbox carries only progress and sentinels; records still live in C-7 and C-8.
3. **§2.5 and §2.6.** Data overtaking its own progress is now a real case on `e7ac01e`, because data bypasses the outbox. It is still allowed. The teardown-ordering note now records PR8's known Shared-mode residual, in which an ack overtakes payload. PCP makes that residual unreachable. L1 now credits the outbox with delivering progress.
4. **§5.5 rewritten** for #291's final behaviour:
   - `NetworkPush` treats a closed queue as backpressure and reports `SendFailed` once;
   - `DataLoss` and `InboundDataLost` are kept as S3 assertions;
   - protocol versions: v3 today; step 2 bumps to v4 for A4 seeding, and step 3 to v5 for the seal marker (reserved count `u32::MAX − 2`) and scope ids (refined in item 12);
   - the early-finisher test oracle: the tail rule, and the minimum exact rounds;
   - `TeardownWaiter::block`;
   - which failures PR7 already makes fatal.
5. **New §5.5.1: overlap with PR8.** It sets out the split of work, records PR8's known Shared-mode residual, and lists things to re-check once PR8 merges.
6. **PR5's prerequisites changed** (§7 question 12; Appendix B round 7, finding 2, annotated). *Item 11 later folded PR5 into step 3; the findings still hold for step 3.*
   - PR5 requires PR7, which is now verified by experiment.
   - **PR8 is no longer a hard prerequisite**: PR5's consumer-side error contract covers decode errors on every transport.
   - #291's early-finisher scenarios become exact. Frontier-observing variants are added.
7. **§5.9 re-run on `e7ac01e`** (`exp-pcp-verify` @ `8a6ca5b`):
   - The PR5 prototype passes the full suite: 1974 passed and 0 failed, where it had 1 failure before.
   - The single-slot cluster test that hung under PR5 now passes 3/3.
   - C1 repros still fail 7/16 on main and 0/16 under PR5. The cluster failures on main are now loud; the in-process ones are still silent.
   - New row E: under PR5 every early-finisher round is exact. On 2 cores, main manages 4/9, 29/40 and 4/5.
   - Ignored-test results are refreshed. The shape results are unchanged.
8. **Small corrections.**
   - §0 no longer lists fatal error envelopes under PR4c; they moved to PR5 in round 7.
   - §5.1.1 and §5.3 credit PR7, not PR8, with making failures fatal cluster-wide.
   - §5.1.7 states the wire-format bumps.
9. **A from-scratch tutorial** explained the protocol, today's problems and this proposal, with no prior knowledge assumed. It is kept outside the repository.
10. **A bounded TLA+ model of PCP, checked with TLC** ([`tla/`](./tla/README.md)). See §5.8 and §5.9.
    - **The proposal:** on small graphs with 2–3 workers, every scenario explores all reachable states without breaking S1, S2c or S3, and without a hang. The fairness-based liveness checks pass too. This covers:
      - acyclic dataflows;
      - loops flattened as today, with the guard edge an earlier plan proposed;
      - nested loop scopes with the import, the internal-only export and cross-level positives-first splitting (step 3).
    - **Mutations:** each of the following, removed or reverted to today's rule, produces a counterexample:
      - per-edge port counting;
      - positives-first splitting, including across scope levels;
      - the local justification rule;
      - async capabilities;
      - A4 seeding;
      - broadcast multiplicity;
      - the egress credit rule;
      - the loop guard edge, for flattened loops;
      - the scope import;
      - the scope export.
    - **Completion checks:** today's signed-sum completion fails even with complete counting and idle-gated force-close. timely's check and exact zero both pass.
    - **Reviews:** GPT-6 Astra reviewed the acyclic model and the loop models for fidelity. Both models were revised.
    - **Limit:** the recursive-export regression (a scope on an outer feedback path) needed a stateful bridge model. *Since modelled:* [`tla/NestedScopes.tla`](./tla/NestedScopes.tla) (item 15).
11. **The migration plan is now four steps** (§5.7; §7 questions 4, 11, 12 and 13; label table at the top of Appendix B). This was directed by the user: nobody depends on instancy yet. So no step needs to keep users safe on the way:
    - **dropped:** the PR5 stopgap step and its build-time rejections; the rule that no step may weaken a success condition until counting is complete; the interim loop guard edge and the in-loop restrictions; PR12a's shadow mode; the compatibility questions;
    - **step 1, test harness** (was PR4a);
    - **step 2, one executor core**, with agreed initial state (was PR12a and PR12b). It now comes *before* the redesign, so the new accounting is built once (question 13);
    - **step 3, port-counted progress in one pull request** (was PR5, PR4b, PR4c, PR6a and PR6b), with the protocol bump to v5 (item 12);
    - **step 4, update downstream projects** (outside this repository) to the final API.

    The design itself is unchanged, and the TLA+ models check its end state. The model of flattened loops stays as evidence of today's H11 bug.
12. **Step-2 refinements, after step 1 merged** (`main` @ `ba1bd37`: step 1 #293 and PR8 #292 are both merged). The design authority made these while writing the step-2 brief. Each is also in the decision log.
    - **Positives-first splitting is already on `main`.** PR8 shipped it, and step 1's V6 check confirms it, so step 3 no longer has to (§5.1.6, §5.5).
    - **Two protocol version bumps, not one.** A4 seeding changes what a progress stream means: initial capabilities are seeded, not sent. A peer still running the old protocol would therefore count them twice or never. So step 2 bumps `PROTOCOL_VERSION` to 4, together with the plan digest. Step 3's seal marker and scope ids then bump it to 5 (§5.5).
    - **M-d is deleted in step 2, not step 3.** Its only reader, `CombinedStageExecutor`, is deleted in step 2 (§5.1.8, §5.7).
    - **Rules for operators hosted on different sets of workers** (§5.1.8):
      - a pipeline edge needs every worker that hosts its source to also host its target; a feedback edge keeps today's rule, equal host sets;
      - each operator keeps its stage's worker context;
      - a probe expects one writer per hosting worker;
      - `RuntimeHandle::spawn_multi` builds the dataflow once.
    - **Heterogeneous clusters stay rejected in step 2.** Allowing them would be a new feature, which needs the user's decision.
13. **Benchmark gates dropped for steps 2–4** (user decision, 2026-10-22). Performance testing is deferred until instancy is fully correct, and will then be done against timely. §6 now lists what that later work should measure, and the step-3 exit criteria no longer include an A/B gate.
14. **Step-3 refinements, while writing the step-3 brief** (`main` @ `6cb3ced`; decision log R26–R37). None changes what the TLA+ models check.
    - **A failed worker doesn't seal** (R34). Failure is fatal to the dataflow through cancellation, so no peer waits for its seal. Earlier text said sealing happens on failure too (§2.2, §5.1.1, §5.5).
    - **Broadcast multiplicity has a source** (R35): each planned edge carries a delivery kind, included in the plan digest (§5.1.4).
    - **`J` lives for one logic call, not until the commit** (R36). One commit batches many calls, so `J` must not leak across them. A logic `Done` is only a hint; only S2a shuts an operator down (§5.1.1, §5.1.3).
    - **Egress uses a credit token** (R37), and seals wait for release mailboxes (§2.2, §5.1.1).
15. **Step-3 implementation rulings** (decision log R38–R49), made as the design authority while step 3 was built:
    - **Nested scopes** (R38): one pass per apply, exports bottom-up then imports top-down; commits and exact zero use raw counts only; every worker keeps every scope tracker; the scope node has its own id; identity cycles are rejected twice (§5.1.7, item 7).
    - **"Exhausted" is defined** for the seal condition (R39, §2.2).
    - **Public-API deltas** approved by the coordinator (R40, R42; §5.6).
    - **Progress is per location, not per worker,** as in timely (R41): early finishing comes from heterogeneous hosting, not pipeline locality. On today's uniform clusters, no node finishes before every input closes, which corrects an earlier claim that PR7's early-finisher behaviour was kept (R47).
    - **PR7's drain heuristic** no longer treats an open inbound channel from a stopped peer as needing it; a draining worker fails fast once no more progress can arrive and its view is still non-zero (R48).
    - **Liveness rule L6** (R44): every cycle has an unbounded channel, so a loop's back-edge is unbounded (§2.6).
    - **One final call when the input frontier empties** for every closure (R45), so stash-and-flush fails loudly (§5.1.3).
    - **The recursive-export regression is modelled** in [`tla/NestedScopes.tla`](./tla/NestedScopes.tla) (decision log O1, §5.8, §5.9).

---

**Status:** design approved; implementation under way. Step 1 (#293) is merged as `ba1bd37`, and step 2 (#295) as `6cb3ced`. Step 3 is in progress; step 4 follows (§5.7). Targeted experiments ran on a local branch, `exp-pcp-verify`, last re-run on `e7ac01e`; §5.9 lists what they verified.
**Date:** 2026-10-24 (first written 2026-10-20). **instancy base:** `main` @ `6cb3ced`, which includes #285–#295, among them PR7 (#291), PR8 (#292), step 1 (#293) and step 2 (#295). Line citations elsewhere in this doc were last checked on `e7ac01e`; §5.1.8 is checked on `ba1bd37`. The step-3 brief carries citations checked on `6cb3ced`.
**timely reference:** `TimelyDataflow/timely-dataflow` `master` @ `3012f8f` (2026-10-16). The latest release is **timely v0.31.0** (2026-07-14). instancy's `timely` 0.12 dev-dependency (2021) is used only to say what changed.
**Scope:** audit root causes 1, 2 and 4 (fragile counting, opt-in accounting, duplicated paths), and how a new design interacts with root cause 3 (PR7 #291, merged, and PR8 #292). The bug tracker's PR4, PR5, PR6 and PR12 are replaced by the four steps in §5.7.

Back to the overview: [Design Overview](./README.md). Related: [Progress Tracking](./progress-tracking.md), [Execution Model](./execution-model.md).

**Evidence legend.** ✔ means verified, either in source at the cited `file:line` (instancy paths are relative to `instancy/src/`, timely paths to `timely/src/` at `3012f8f`) or in a primary paper. ◇ means inference or analysis that has not been tested.

---

## 0. Summary

1. **There isn't one accounting bug. There are eight partial mechanisms.** ✔ instancy answers "where can work still be?" with eight mechanisms, and none of them covers every place work can live (§3.1). They are: capabilities, exchange in-flight reporters, `T::minimum()` guard capabilities, per-iterate `AtomicI64` loop counters, the staged `FrontierAggregator`, end-of-stream cascades, idle-sweep force-close, and `feedback_deps` counters. Each audit finding (C1, C2, H1, H10–H14) sits in a gap between two of them.
2. **The invariant is known and has a machine-checked proof.** Safety: *if a worker's view shows nothing outstanding at or before `t`, nothing is outstanding at or before `t`, now or later*. Naiad stated it (SOSP 2013). Abadi et al. proved it for the "clocks" protocol (FORTE 2013). Brun et al. proved it in Isabelle for the "exchange" protocol, which models timely's Rust implementation (ITP 2021).
   - Five obligations make it hold:
     - every unit of work is counted (no "dark matter");
     - every positive update is *justified* by the sender's state (ITP's generalization of FORTE's "upright");
     - batches are applied in order per sender, and fragments are positives-first;
     - workers agree on the initial state;
     - true counts never go negative.
   - Views may go negative *transiently*, so frontiers must look at positive counts only, and completion must never sum signed diffs. Completion should require every pointstamp to be exactly zero (§2).
   - PCP is argued to be an instance of ITP's exchange model; the refinement is not mechanized (§2.5).
3. **Recommendation: design C, port-counted progress (PCP).** Count records at the two operator **ports**, not inside channels:
   - an operator records "produced" when it hands a batch to its output, and "consumed" when it takes a batch from its input;
   - everything in between is "on the edge" and is covered with no code in any channel: `pending_output`, tee retry, exchange buckets, bounded/SPSC queues, network outboxes, receive bridges and `InputHandle`;
   - one generic **operator shell** owns all port I/O, so user logic and operator kinds never touch a raw `Push`/`Pull`;
   - operator-held work (async tasks, notify stashes, and a new `retain` handle for closures) is covered only by capabilities. **Every capability acquisition must be justified by input consumed in the same activation, or by a capability already held.** Today's frontier-based check is unsafe (§5.1.3);
   - completion requires exact-zero counts over the worker's scope, and every success path goes through one decision module. Frontiers and shutdown use positive counts. The destructive idle-sweep force-close is replaced by timely's shutdown rule, with operator shutdown, worker termination and dataflow completion kept separate;
   - loops get a nested-scope tracker, with the parent frontier imported downward and only internal work exported upward;
   - one executor core hosts any subset of operators (heterogeneous instancing), with one progress driver per worker, which retires the staged path's separate mechanisms.
4. **The hypothesis (design A, accounting in the channel layer) has the right invariant at the wrong layer.** instancy has more than ten `Push`/`Pull` implementations and 19 channel factories, but can have exactly one operator shell. Counting on delivery leaves `InputHandle` and `pending_output` dark, and needs hand-placed RAII tokens on about 13 operator kinds. Design C keeps A's good ideas: constructor-required accounting, RAII (as capabilities), a non-signed completion predicate and one core (§4).
5. **Not a regression toward timely's execution model.** We borrow timely's *invariant* and its debug checks. We keep the async shared pool, fusion, per-stage and heterogeneous parallelism, the closure API with no user-visible capabilities, pluggable transport, and per-dataflow membership (§5.4).
6. **Migration: four steps, straight to the end state** (§5.7). Nobody uses instancy yet, so no step needs a stopgap:
   - **Step 1, test harness:** a debug invariant checker that is on in every test, including multi-worker and cluster tests, plus a path × transport conformance matrix.
   - **Step 2, one executor core:** one executor hosts any subset of operators, with one progress driver per worker and agreed initial state. The staged machinery is deleted. It comes before the redesign so that step 3 is built once.
   - **Step 3, port-counted progress, in one go:** the operator shell, port counting, capabilities, nested loop scopes, the shutdown rule, exact-zero completion and seal markers.
   - **Step 4:** update downstream projects, outside this repository, to the final API.
7. **Main risk: progress traffic under congestion.** Port counting adds two lock-free `ChangeBatch` updates per batch per edge, plus a *mandatory* worker-wide compaction after edge expansion. It *removes* a mutex lock per exchange sub-batch. Edges whose queues stay non-empty across sweeps add updates to every sweep. One non-cancelling `u64` entry is about 33 bytes per peer, so traffic depends on worker count and batch size. Performance is measured once instancy is fully correct, against timely (§6); no step is gated on a benchmark. timely's demand-driven sending is the planned mitigation, if it is needed.
8. **One limit no design removes.** A user closure can move consumed data into captured state and never emit it. That is indistinguishable from a filter; timely has the same limit. PCP makes *emitting it later* a loud error, and adds an explicit `retain` API so that keeping data is easy to do correctly (§5.1.3, §5.3).

Eight adversarial review rounds have been addressed:
- **Round 1**, a Claude-family reviewer: 10 issues, 4 critical.
- **Round 2**, GPT-6 Astra at the user's request: 17 issues, 4 critical.
- **Round 3**, a second GPT-6 Astra pass: 7 new issues, 2 critical, and it completed 6 partly resolved round-2 items.
- **Round 4**, a third GPT-6 Astra pass: 3 issues, 2 critical, both migration deadlocks.
- **Round 5**, a fourth GPT-6 Astra pass: 1 critical, a PR5 migration-interval hang. It was generalized and closed.
- **Round 6**, a fifth GPT-6 Astra pass: 1 critical, an acyclic H12 → notify hang.
- **Round 7**, a sixth GPT-6 Astra pass: 2 critical. A keying fix in PR5 would expose H1, and decode-failure credit was a missed lasting positive. (In the old plan, H12 was then rejected everywhere until PR4c, and error envelopes became fatal from PR5. Both now land in step 3.)
- **Round 8**, a seventh GPT-6 Astra pass: **no critical issues and no new findings.**

Rounds 4 to 7 mostly concerned the old plan's migration interval, which the four-step plan removes (Appendix B).

**Experiments** (§5.9, local branch `exp-pcp-verify`, re-run on merged PR7 `e7ac01e`) confirm the key findings on real code. "The PR5 prototype" below is a prototype of step 3's completion changes alone, named after the old plan:
- with H12, today's run loses the aggregate silently;
- the loop-then-broadcast cancellation case completes on main and hangs under exact zero;
- the identity-map workaround is correct;
- the PR5 prototype passes all 16 C1 repros, against 7 failures on main. On `e7ac01e`, main's cluster failures are loud, but its in-process failures are still silent;
- the PR5 prototype needs PR7's reliable close sentinels. On top of merged PR7 it passes the full suite (1974 passed, 0 failed);
- on top of PR7, the PR5 prototype makes every round of #291's early-finisher scenarios exact, even pinned to 2 cores, where main manages 4/9, 29/40 and 4/5.

Round 7's H1-exposure sequence was not reproduced under default fused scheduling.

**Model checking** (§5.8, §5.9, [`tla/`](./tla/README.md)). Bounded TLA+ models of PCP were checked with TLC, exploring every interleaving within the bounds. They cover small acyclic graphs, loops flattened as today, and nested loop scopes, with 2–3 workers.
- **The proposal** never lets a frontier pass outstanding work, never drops a record, never completes with work left, and never hangs. Every worker eventually finishes.
- **Each core rule is needed.** Removing any one of them, or reverting it to today's rule, produces a counterexample. That includes the scope import and export. Flattened loops without a guard edge reproduce today's H11 bug.
- **Today's signed-sum completion** loses a record even with complete counting.

The design's recursive-export regression (§5.1.7) is modelled too, in [`tla/NestedScopes.tla`](./tla/NestedScopes.tla): a loop on another loop's feedback path, with imports and exports cached between applies. The design passes; exporting the imported frontier never drains, and step 3's `DerivedZero` assertion catches it.

Appendix B logs every finding and how it was resolved.

---

## 1. Problem statement

The correctness audit (kept outside the repository) grouped the progress findings into four root causes. This doc cites its findings by ID (C1, H14, …); their repros are in `instancy/tests/audit_repros/`. Current evidence on `main`:

| Root cause | Current evidence (✔) |
|---|---|
| 1. Fragile counting | `Tracker::tracking_anything` returns true while `total_counts != 0` (`progress/reachability.rs:768-788`). `total_counts` adds every raw signed diff (`reachability.rs:827`, `:844`). A +1 capability and a −1 in-flight count therefore cancel. The executor force-closes every operator once that predicate says "complete" after `max_idle_sweeps` idle sweeps (`dataflow/executor.rs:1781`, `:1807-1816`). |
| 2. Opt-in accounting | Producer-side in-flight wiring has exactly two call sites: the map family's `add_unary_internal` (`dataflow/dataflow_builder.rs:4742`) and the loop concat (`:2665`). The consumer side is always wired for exchange (`:4343`). `BroadcastPush` has no reporter at all (`dataflow/channels/exchange_channel.rs:686`, which doesn't override the no-op in `channels/pushpull.rs:69`). Pipeline channels never count (`pushpull.rs:69`, `:103`). |
| 3. Shutdown bolted on | Out of scope; PR7 and PR8 own it. Interactions are in §5.5. |
| 4. Duplicated paths | Two sweep, completion and force-close implementations (`executor.rs:1705-1850`, `dataflow/stage_executor.rs:451-545`). A third convergence rule lives in `CombinedStageExecutor` (`stage_executor.rs:966-1000`). There are 13 `close_inputs` implementations and 77 `pending_output` references in `dataflow/wired_operators.rs`, and 24 operator factories in `dataflow_builder.rs`. `SimpleRuntime::spawn_multi` has no cross-worker progress (§3.7). |

**Urgency.** #290 removed the scheduler livelock, so congested in-process multi-worker runs now *complete early and silently lose data* instead of hanging. PR7 observed 336 missing records with every worker returning `Ok`.

---

## 2. Correctness invariants (Phase 1)

### 2.1 Model

- **Location.** `Source(op, out)` is an operator output, where capabilities live. `Target(op, in)` is an operator input, where messages live. This is unchanged from today (`reachability.rs`).
- **Pointstamp.** `(ℓ, t)`. **Could-result-in:** `(ℓ₁,t₁) ≼ (ℓ₂,t₂)` iff some path summary `ψ` from `ℓ₁` to `ℓ₂` has `ψ(t₁) ≤ t₂` (Naiad §2.3).
- **True state `C*`.** For each pointstamp, the number of records really sitting between a producing port and a consuming port at `t` (a message count at the `Target`), plus the capabilities really held (a count at the `Source`). By construction `C* ≥ 0` everywhere.
- **View `V_w`.** Worker `w`'s accumulated sum of all progress updates it has applied, from every worker including itself. **`V_w` may be negative.**
- **Frontier.** `F_w(ℓ)` is the minimal antichain of `{ψ(t′) : V_w(ℓ′,t′) > 0, ψ ∈ summaries(ℓ′→ℓ)}`. Only *positive* counts contribute. This is `MutableAntichain` semantics, as both instancy and timely already implement it.

### 2.2 Safety properties

- **S1, frontier conservatism.** For every worker `w`, at all times: if `C*(p) > 0` then some `p′` with `V_w(p′) > 0` has `p′ ≼ p`.
  - Naiad (§3.3): *"no local frontier ever moves ahead of the global frontier"*.
  - FORTE 2013 proves the equivalent theorem `Safe ≜ ∀t,q: IsVacantUpto(glob[q],t) ⇒ □ IsVacantUpto(nrec,t)`. Once a view is vacant up to `t`, the true state is vacant up to `t` *forever after*.
  - ITP 2021 re-proves it in Isabelle/HOL for timely's Rust implementation, with a combined theorem that *"frontiers always constitute safe lower bounds on what timestamps may still appear on the operator inputs."* ✔
- **S2, shutdown, termination and completion.** These are three separate decisions.
  - **S2a, operator shutdown.** Worker `w` may shut down its instance of operator `X` only when (a) every input frontier of `X` is empty in `V_w`, (b) `X` holds no capability, and (c) `X` has nothing left to hand over, including external egress. By S1, (a) means no record can ever arrive at `X`. This is timely's rule: `progress/subgraph.rs:366-374` shuts down a child when `frontiers_empty && no_capabilities`, and `subgraph.rs:731` *panics* if progress is later reported at a shut-down operator. ✔
  - **S2b, worker success (scoped retirement).** A worker may report success and retire only when:
    - (a) all its hosted operators are shut down (S2a);
    - (b) every one of its updates has been committed and enqueued, ahead of its end-of-stream on every peer lane (PR7);
    - (c) its view holds **no non-zero count** (positive *or* negative) at any location from which one of its hosted locations is reachable in the *data* graph. Reachability here ignores timestamps and includes back-edges, and hosted locations include the locations of every probe handle and output port it serves.

    If (c) fails, the worker keeps processing progress; it is *not ready*, not failed. A non-zero count becomes a **proven residual** (an error) only after the *final accounting boundary*: this worker has applied a **seal marker** from every peer and from itself, and the count is still non-zero.
    - **Sealing is independent of success, but not of failure.** A worker seals its progress stream once it can produce no further updates: all its hosted operators are exhausted or shut down, no capability or input credit is held, and its ledgers and release mailboxes are committed. An operator is **exhausted** (R39) once every input has seen data-plane end-of-stream from every sender, its input buffers are empty, it holds no capability, it has nothing left to hand over, and its ledger is committed; a source is exhausted once it releases its capability. Exhaustion is latched and never closes outputs; only S2a does. A worker stuck on a residual still seals. A *failed* worker does not seal (R34): failure is fatal to the whole dataflow through cancellation, so no peer waits for its seal, and a seal over a failed worker's live capabilities would make every peer report a misleading residual instead of the original error.
    - The seal marker travels *in order* on the worker's progress stream: as a marker entry in the in-process channel, and as a reserved progress frame in a cluster. There is no self lane, so a worker records its own seal locally, right after self-applying its final batch.
    - A receiver counts a peer as sealed only when it *applies* that marker, after every earlier batch from that peer. Receiving a frame at the bridge is not enough.
    - So two workers that are both exhausted and both stuck on a genuine residual still reach the boundary, and both report it.

    After retirement, peers treat the worker as retired: PR7 already discards progress sent to it. A worker that hosts nothing downstream may therefore finish before the rest of the dataflow. That keeps PR7's early-finisher behaviour **only under heterogeneous hosting**, which is in-process only today. On a cluster every node hosts every operator (uniform parallelism, O8), so every location is in every node's scope, and no node finishes until every node's input has closed and every record is consumed, as in timely (R47). After that, nodes still finish independently: `join()` doesn't wait for a peer's teardown.
  - **S2c, dataflow completion.** Every worker has succeeded under S2b. ◇ No surviving observer is needed. Every location belongs to an operator that at least one worker hosts, so the scoped checks in (c) jointly cover every location.
    - Suppose worker H retired with an exactly-zero scope.
    - Any later change at a location in H's scope needs either new production into it, which requires a capability that could reach it, or consumption of data already in it, which requires a positive count there.
    - By S1, either one would have left a positive witness in H's view, and that would have blocked (c).
- **S3, no silent drop.** A record that was accepted is never discarded while the run reports success. PR7 and PR8 enforce this across the network (`CommunicationError::DataLoss`). The same rule must hold locally, for a push into a finished local operator (bug tracker, "found while fixing").

### 2.3 Accounting obligations: what the implementation must do for S1 and S2 to hold

| # | Obligation | Source |
|---|---|---|
| **A1** | **Completeness ("no dark matter").** Every unit of work that can still produce output at `t` is counted at a pointstamp `≤ t`. It is either a message count at the consuming input, or a capability at the output of the operator that holds it. | Naiad §2.3 occurrence counts; ITP's "exchange" protocol splits work into capabilities plus in-flight `data`. ✔ |
| **A2** | **Justified updates.** Every positive update a worker sends is *justified* by that worker's state when it commits the update. Either the update is supported by a decrement at a pointstamp that could-result-in it, in the same committed delta, or the worker holds a capability that could-result-in it. FORTE's *upright* condition is the special case where support must be a strictly smaller negative entry in the same delta. ITP §3–4 shows upright is too strong for timely: an operator that holds a capability and sends again produces a positive delta with no negative entry. That is not upright, but it is justified by the held capability. ITP's *exchange* protocol therefore justifies updates against worker-local `caps`. The canonical violation is *"consuming an input message but forgetting to indicate the acquisition of its capability"* (timely book ch. 5.2), or acquiring a capability nothing justifies (§5.1.3). | FORTE `IsUpright`; ITP §3–4 (exchange protocol, `justified`); Naiad §3.3. ✔ |
| **A3** | **In-order application per sender, positives-first fragments.** Each sender's committed batches are applied in commit order. A batch split into fragments is ordered positives-first (§2.5). There is no constraint *between* senders. | FORTE assumes reliable, ordered channels, and the proof depends on it; Naiad §3.3; ITP. ✔ The fragment rule is ◇ (§2.5). |
| **A4** | **Agreed initial state.** Before any frontier or completion decision, every view contains every worker's initial capabilities. | timely seeds each initial capability with multiplicity `peers` on every worker (`dataflow/operators/generic/builder_raw.rs:243`). ✔ |
| **A5** | **Conservation.** In the true state, consumed never exceeds produced at any pointstamp, and every `+n` is eventually matched by exactly one `−n`. A mismatch is a *safety* bug, not only a liveness bug: a true negative count can hide a real positive count at the same pointstamp. | ◇ This follows from `C* ≥ 0`, which is an assumption of the FORTE model. |

### 2.4 Transient negative counts and the completion predicate

A view goes negative when worker B's "consumed −n" reaches worker C before worker A's "produced +n". Every source agrees this is legitimate (Naiad §3.3; FORTE; the timely book: *"Numbers can also be transiently negative"*). It is harmless only if frontiers look at **positive** counts, and completion never lets a negative count offset a positive one.

- timely: `total_counts` is *"a non-negative sum of post-filtration input changes … equal to the sum across all operators of the frontier sizes"* (`reachability.rs:425-431`). It is updated only from frontier changes (`:673`, `:693`), and `tracking_anything` is `pending changes || total_counts > 0` (`:521-525`). timely 0.12 is identical (`reachability.rs:476-480` in the 0.12 crate). ✔
- instancy adds raw diffs (`reachability.rs:827`, `:844`) and tests `!= 0` (`:769`). ✔ This diverges from **both** 0.12 and the latest release.

**Worked example (C1).** Worker 0 holds an input capability at `Source(in,0)@5`. Worker 1 consumes one exchanged record and applies `Target(x,0)@5: −1` before worker 0's `+1` for that record arrives.
- **instancy:** worker 1 computes `total_counts = +1 − 1 = 0`. It "completes", force-closes after its idle sweeps, and drops the record when it arrives. The run returns `Ok`.
- **Positive semantics:** the capability is a frontier element and `total_counts = 1`, so the worker does not complete.

**Positive semantics do not forgive a broken A5.** A missing `+n` leaves a negative residual. Under positive semantics that residual no longer blocks completion, *and* it can cancel a real `+n` at the same pointstamp.

Example: H12 makes two edges share one in-flight key. Message A's `+1` lands on the other binding, so A's consumption leaves `−1`. Message B is correctly counted and still queued, giving `+1`. The pointstamp nets to zero while B is outstanding.

So the **completion** predicate should be stricter than timely's:
- **Exact-zero completion.** A worker's scope, and in the end the dataflow, is complete only when *every* pointstamp count is exactly 0 and no changes are pending. Counts are checked one pointstamp at a time and never summed.
  - It is strictly safer than both today's signed sum and timely's positive count, over the same view.
  - It fixes C1's cross-location cancellation: a `+1` capability and a `−1` in-flight count are two non-zero entries.
  - A missing `+n` hangs instead of losing data, and after the seal boundary it is reported as an error (V5).
  - With correct accounting every count does reach 0 eventually, so liveness is unaffected.
- **Frontiers** keep positive semantics, as today, so transient negatives don't hold them back.
- **Same-pointstamp masking** (the H12 case above) defeats every count-based predicate. Only correct accounting (port counting, keyed by edge) removes it, and only the test oracle V7 detects it (§5.8).

Once ports make A5 hold by construction, completion could use timely's positive count instead. Exact-zero is recommended: at S2c every count must be 0 anyway, and the stricter check turns any residual leak into a visible hang (V5) instead of a silent pass.

### 2.5 Multi-worker and cross-node specifics

- **Which proof applies.** ✔ timely's Rust implementation batches many operator activations into one progress message. `Subgraph::schedule` activates every scheduled child and accumulates their extracted progress into `local_pointstamp` (`progress/subgraph.rs:292-345`, `:384`). It then sends that accumulated batch once (`:526-546`). ITP formalizes this implementation through its exchange protocol plus a local propagation algorithm. PCP's commit (§5.1.1) has the same shape: a sequence of local operations whose summed delta is transmitted as one unit. ◇ We *argue* PCP refines ITP's exchange model:
  - port queues map to ITP's in-flight `data`;
  - held capabilities, including `retain` and async, map to `caps`;
  - an uncommitted ledger maps to the worker's not-yet-sent local changes.

  This refinement has not been mechanized. The protocol property test (§5.8) checks it empirically, and a mechanized refinement is optional future work. A bounded TLA+ model of PCP (§5.8, [`tla/`](./tla/README.md)) model-checks S1, S2c, S3 and liveness for small graphs, including loops flattened as today and one nested loop scope. That is exhaustive within its bounds, but it is not a refinement proof.
- **Self-application.** instancy applies a worker's own updates to its tracker before sending them (`subgraph.rs:1012-1080`); timely routes them through the broadcast to itself. ◇ In the model, point-to-point delivery times are arbitrary. Delivering a committed batch to its own sender at commit time is therefore one admissible schedule, provided the batch is committed (it will be sent, in order) before any decision uses it.
- **Splitting a batch** (instancy splits progress batches above `max_batch_size`: `progress/network_progress.rs:112-131`, ✔). Unordered splitting is unsafe today: collection emits capability changes before in-flight target changes (`subgraph.rs:1012-1080`), so a capability release can land in an earlier fragment than the `+n` it supported. ◇ **Positives-first is safe by monotonicity, not by uprightness:**
  - Let `V` be the receiver's view before the batch and `δ = P + N` the batch, with `P ≥ 0` and `N ≤ 0`, fragmented as `P₁ … Pₖ, N₁ … Nₘ`.
  - Every intermediate view is pointwise either `≥ V` (a prefix of positives) or `≥ V + δ` (all positives plus a prefix of negatives).
  - S1 is upward-monotone: adding positive counts can only add witnesses.
  - So every intermediate view is at least as safe as `V` (the receiver's view before the batch arrives) or `V + δ` (the view after the whole batch is applied). The model already admits both views as safe.

  The argument does not need any fragment to be upright on its own. It needs only in-order application of fragments (A3).
- **Ordering of data against progress.** ◇ S1 does **not** need data frames and progress frames to share a FIFO lane:
  - If B sees A's `+n` before the data, the count is conservative.
  - If B consumes first and broadcasts `−n`, A's justification for the message is still positive in every view that hasn't applied A's batch.
  - This case is real on `e7ac01e`, not hypothetical. Data frames go straight into the bounded transport queue with `try_send` (`network.rs:347-394`). Progress frames go through the per-peer outbox and its forwarder (`peer_link.rs:269-318`), which can lag behind. A record can therefore reach a peer before the progress batch that counts it.
  - #291's teardown is the only part that depends on order across channels. ◇ In Dedicated mode the outbox forwarder feeds the same per-peer queue and connection as data (C-7, C-17), so an ack follows the acker's earlier data. In Shared mode, frames are spread over a connection pool (`shared_transport.rs:1014-1037`). PR8's description records that an end-of-stream ack can overtake payload there, a known residual whose fix needs a wire change. Such late data can exist only after a premature completion (C1/C2), which PCP removes in step 3. This design adds no new dependency on order across channels.
- **Heterogeneous parallelism.** A4 needs each operator's initial capability multiplicity to equal *the number of workers that instantiate that operator*. Today's handshake fingerprint covers the logical graph and topology. It does not cover a finalized operator-to-worker assignment, and clusters currently reject heterogeneous stages (`runtime.rs:2883`, ✔). Before heterogeneous clusters are allowed, the handshake must include a digest of the canonical physical plan (§5.1.8).
- **Membership.** ✔ A dataflow's worker set is fixed when it is spawned. `NodeJoined` only affects later dataflows, and `NodeLeft` cancels the affected dataflows (`docs/design/cluster.md:270-283`). A4 needs no runtime agreement protocol.

### 2.6 Liveness

- **L1.** Every progress batch is eventually delivered. Since #291 the per-peer outbox never drops a frame, and teardown flushes it.
- **L2.** No count leaks (A5). A leaked `+n` hangs the run, which is safe but must be detectable.
- **L3.** A frontier change wakes the affected operators (see M11, the lost wakeup).
- **L4.** Accounting never blocks data flow, so there is no cyclic wait between backpressure and progress.
- **L5.** If progress sends are delayed (demand mode), the delay is bounded. Naiad's accumulation rule is: *"Updates may be accumulated as long as every buffered pointstamp p satisfies one of two properties: either some other element of the local frontier could-result-in p, or … net update … is strictly positive"*.
- **L6.** Every cycle in the data graph contains at least one unbounded channel (R44). A cycle of bounded channels, whose operators must flush their retry queues before pulling more, can fill up and deadlock for data-plane reasons alone. In a loop scope the back-edge is unbounded and stays worker-local, and every cycle passes through it. A loop body therefore has no internal backpressure; inflow is throttled at the loop's entry, as in timely.

### 2.7 What changed in timely since 0.12 (instancy was modelled on 0.12)

| Change | Release | Relevance |
|---|---|---|
| `tracking_anything` = positive frontier count | unchanged since 0.12 | instancy's signed sum (C1) diverges from both versions. |
| On the guarded pull path, consumption is recorded when a `ConsumedGuard` is dropped (`dataflow/channels/pullers/counter.rs:19-59`). In 0.12 it was recorded at pull time (`Counter::next`). The plain `Counter::next()` still drops the guard at once, so its timing is unchanged. | between 0.12 and 0.31 | The guard lets an operator keep ownership of an input batch past the pull. Consumed and produced are still published together after the operator's `schedule()` (`builder_rc.rs:265-282`). Design C relies on that per-activation publication, and on deferring `consumed` while the operator still owns the data (egress, §5.1.1). |
| `Operate` became a builder trait; per-input `FrontierInterest` (`Never` / `IfCapability` / `Always`); columnar `Tracker`; the command-line default progress mode became `Demand` (#758) | 0.28 | Activation is driven by frontier interest. `Config::default()` has used `Demand` since 0.12 (CHANGELOG 0.12 entry; `worker.rs:67-70` in the 0.12 crate). Only the 0.12 command-line parser still defaulted to `Eager` (`worker.rs:120` in 0.12). Demand mode holds sends until a "global" decrement appears (`subgraph.rs:526-546`, `is_global`). |
| `Distributor` contract: *"the amount of records on the input matches those on the output"*; `Accountable::record_count` | 0.24 | Exchange must preserve record counts. Design C states the same contract for instancy pacts. |
| **#721:** `Counter::give` pushed to the inner pusher and *bypassed produced accounting*, corrupting progress. v0.25.0 was withdrawn. **#723** then removed that API. | 0.25.1 | Even timely shipped a "forgot to count" bypass. **Make bypass impossible by API shape, not by convention** (§5.2). |
| `PortConnectivity` builder/freeze split | 0.31 | Not relevant. |
| **Stamps:** a message carries a *multiset* of timestamps, and empty stamps make no progress claims (`progress/stamp.rs`, #813, #817, #823) | **master only, not released** | Not needed now. A later design could stamp batches with an antichain. |

---

## 3. Inventory (Phase 2)

### 3.1 The eight mechanisms in use today

| # | Mechanism | Where (✔) | What it covers | What it misses |
|---|---|---|---|---|
| M-a | Capabilities and reachability | `progress/capability.rs`, `progress/subgraph.rs`, `reachability.rs`. Held only by input (`dataflow_builder.rs:467`), source (`:685`), source_async (`:820`), broadcast (`:4459`) and `notify_at` (`dataflow/operators/handles.rs:515`). | Sources and notify stashes. | Every pass-through operator holds nothing. |
| M-b | Exchange in-flight reporters (+n push, −n pull) | `exchange_channel.rs:382-407`, `:1031-1033`; registered only at `dataflow_builder.rs:4316`. | Exchange edges whose producer factory remembered to wire them. | Most producers (C2); every non-exchange edge. The key is `(op, output)`, so two edges from the same output collide (`subgraph.rs:152-153`, `:360`; H12). |
| M-c | `T::minimum()` guard capability | `WiredUnaryOperator::with_reporter` (`wired_operators.rs:116`), used by broadcast (`dataflow_builder.rs:4455-4469`, `:4524`). | Broadcast in-flight data, coarsely. | Freezes downstream frontiers until input closes (H10). |
| M-d | Per-`iterate` `AtomicI64` loop counter | Created at `dataflow_builder.rs:2351-2356`; enter `+` at `wired_operators.rs:973`, leave `−` at `:1121`; read only by `CombinedStageExecutor` (`stage_executor.rs:972`). | Loop drain on the staged path. | It is per worker, so loops hang when a record leaves on another worker (H14). The uniform path ignores it. |
| M-e | Staged `FrontierAggregator` and `ExchangeMessage::{FrontierUpdate, SenderDone}` | `progress/frontier_aggregator.rs`, `stage_executor.rs:552-606`, `dataflow/channels/exchange_message.rs:17-32`. | It was designed to carry cross-stage frontiers. | **Not wired.** `ExchangeInput::update_frontier` and `mark_sender_done` are called only from `#[cfg(test)]` code (`stage_executor.rs:1146` onward), and `ExchangeMessage` control variants are never constructed outside their own tests. `send_frontier_updates` only records a field (`stage_executor.rs:580-602`, `:1095-1110`). Cross-stage frontiers stay at `T::minimum()`. ✔ |
| M-f | End-of-stream / exhaustion cascade | `Pull::is_exhausted`; every wired operator finishes on exhausted input (for example `wired_operators.rs:214-224`). | Completion of acyclic graphs. | Frontiers, notifications and cycles. Over the network, #291 made close sentinels reliable (H9), and PR8 stops treating EOF as exhaustion (C5). |
| M-g | Idle-sweep force-close | `executor.rs:1781-1816`; `stage_executor.rs:483-501`. | Loop termination. | It is destructive and relies on M-a/M-b plus the signed sum (C1). |
| M-h | `feedback_deps` counters | `stage_executor.rs:483`; `CombinedStageExecutor` decrements them. | Cross-stage feedback on the staged path only. | Anything per timestamp. |

### 3.2 Where work can live: channels and transport

Paths: **U** is the uniform executor (`spawn_multi_internal`, `runtime.rs:1857`); **F** is fused or stage-task activation inside U (`executor.rs:1051`, `:1092`); **S** is the staged executor (`spawn_staged_se_internal`, `runtime.rs:2085`); **SR** is `SimpleRuntime` (`runtime.rs:3979`); **CL** is a cluster (`runtime.rs:2746`), in Dedicated or Shared transport mode. Citations are against `e7ac01e`.

| # | Location | Definition (✔) | Paths | Accounted today | Covered? |
|---|---|---|---|---|---|
| C-1 | Bounded pipeline queue (`VecDeque`) | `dataflow/channels/bounded.rs:42`, `:109`, `:192` | all | no (`pushpull.rs:69`, `:103` are no-ops) | ✗ H1 |
| C-2 | SPSC ring | `dataflow/channels/spsc.rs:150`, `:258` | wherever used | no | ✗ |
| C-3 | Tee retry state (partial fan-out) | `dataflow/channels/tee.rs:44`, `:95-119` | all | only exchange branches, via propagated reporter (`tee.rs:153-158`) | ✗ pipeline branches |
| C-4 | `ExchangePush` buckets, `pending`, `owed` | `exchange_channel.rs:328-335` | U F CL | `+n` per sub-batch at target delivery (`:393-407`), *only if wired* | partial (C2) |
| C-5 | Local exchange lanes (M×N bounded) | `dataflow/channels/edge_materializer.rs:149-170` | U F S SR | via the wrapping `ExchangePush`/`ExchangePull` | partial |
| C-6 | `BroadcastPush` per-target state | `exchange_channel.rs:686-715` | U F CL | none (M-c instead) | ✗ H10 |
| C-7 | `NetworkPush` → per-peer bounded payload queue (Dedicated `transport_session.rs:278-279`; Shared payload and writer queues in `shared_transport.rs`) | `dataflow/channels/network.rs:294`; data frames go *directly* into the queue with `try_send` (`send_frame`, `:347-394`). A closed queue to a non-retired peer returns `Backpressure` and reports `SendFailed` once (`:376-393`, #291). Records to a retired peer fail with `DataLoss` (`:349-371`). | CL | via `ExchangePush` only | partial. Under PCP the batch is on the edge, counted when produced, so a stalled push holds the frontier until the coordinator cancels. |
| C-8 | Receive bridge → unbounded `std::sync::mpsc` → `NetworkPull` | `network.rs:516-570`, `:1169`, `:642-730`. Since #291 the bridge keeps running after the pull is dropped and reports undeliverable records as `InboundDataLost` (`:588-616`); the pull's `Drop` reports unconsumed frames (`:667-695`). | CL | `−n` at `ExchangePull` | partial (M7: unbounded). Records reported lost are S3 violations, never a normal path. |
| C-9 | Close sentinel | `network.rs:400-428`. Since #291 it goes through the peer's ordered outbox (C-17), so a full queue can't lose it (H9 fixed). Only standalone use without an outbox still uses best-effort `try_send`. | CL | end-of-stream, not progress | ✔ (#291) |
| C-10 | Exchange control messages | `exchange_message.rs:17-32` | S | never sent (M-e) | ✗ |
| C-11 | In-process progress channels | `progress/progress_channel.rs:126-232` (`Arc<Mutex<VecDeque>>`) | U F | progress itself; FIFO and atomic per message | ✔ |
| C-12 | Network progress send and receive | Send: `ProgressSender` encodes into the peer's outbox (`network_progress.rs:589-625`); deltas to a retired peer are dropped (`:594-596`). Encoding is chunked (`:112-131`, capped below the end-of-stream and ack counts). Receive: `progress_recv_bridge` (`:378-428`); a decode error is `LinkFailure::Decode`, which the coordinator treats as fatal (#291). | CL | progress itself, in FIFO order per lane. **Not atomic above `max_batch_size`.** On `e7ac01e`, fragments followed collection order, capabilities first (`subgraph.rs:1012-1080`). | ✔ since PR8 (#292): positives first |
| C-13 | External input queue (`InputSender` → `SyncSender`) and the source's admission gate | `dataflow/channel_operators.rs:666-747`; gate and drain state machine at `:200-326` | all | input capability (`dataflow_builder.rs:467`), released only after the gate is idle and the queue is drained; #285 enforces the input frontier | ✔. The gate invariant must be preserved by the shell's source logic. |
| C-14 | `source_async` producer task and channel | `dataflow_builder.rs:791-820`; `channel_operators.rs:210` | all | source capability | ✔ |
| C-15 | External egress: `ChannelSinkOperator::pending_send`, used when the external channel is full | `channel_operators.rs:460-477`, `:514-528` | all | nothing. The sink consumed the batch, then holds the event. Today's idle force-close (`executor.rs:1807-1816`) does not check it. | ✗. Under PCP the sink keeps its input credit until egress accepts (§5.1.1). |
| C-16 | Error and control envelopes | `NetworkPull` still turns a payload decode failure into `Envelope::error` (`network.rs:719-722`; PR8 changes this), and it also delivers error envelopes that the sender encoded on purpose (`:277`). MockNetwork does the same (`dataflow/channels/mock_network.rs:299`, `:423-433`). Exchange passes control payloads through (`exchange_channel.rs:1085-1088`). Sinks and operators ignore them (`channel_operators.rs:574-576`; `wired_operators.rs:166-178`). | CL, in-process (mock), U, F | none. A counted batch that turns into an error envelope is never consumed. | ✗. The shell treats errors as fatal (§5.1.1). PR8 covers only cluster decoding. |
| C-17 | Per-peer outbox lane (#291) | `communication/peer_link.rs:134-160`; forwarder `:269-318`. It is unbounded and ordered, and its forwarder awaits capacity on the bounded transport queue. | CL | Carries progress deltas, progress end-of-stream and acks, and data close sentinels. **It carries no records**: data bypasses it (C-7). | ✔. Progress in the outbox is progress in transit, kept in FIFO order per lane (A3). Records can overtake the progress that counts them, giving transient negatives, which §2.4 allows. |
| C-18 | Lingering readers and inbound bridges after release (#291) | `Demuxer::run_lingering` (`communication/transport.rs:536-593`) keeps reading for up to `DEMUX_LINGER` (10 s) after a session is dropped (`transport_session.rs:141-159`, `:314`), and the write half is shut down explicitly (`:427-429`). `InboundBridges` (`peer_link.rs:211-265`) drain before teardown completes. | CL | Records still go to C-8 or are reported lost; nothing is delivered silently. | ✔. Same store as C-8, with a longer lifetime. |
| C-19 | Shared flush barrier and `SharedPeerManager::close()` (#291) | `shared_transport.rs:799-839` (barrier on the payload queue), `:841-870` (close flushes, then closes). `unregister_dataflow` drops the dataflow's payload frames that are still queued (`:799-800`), so #291 flushes first. | CL (Shared) | Control only. It guards records and progress that are already in the payload queue (C-7, C-17). | ✔ given #291's ordering. Frames dropped there are on the edge under PCP, so the receiver keeps the sender's `+n`. Under S2b (step 3) that is a hang, not an early completion. |

### 3.3 Where work can live: operators

| # | Location | Definition (✔) | Accounted today | Covered? |
|---|---|---|---|---|
| O-1 | `InputHandle.pending` (pulled, not yet processed) | `dataflow/operators/handles.rs:28-32`, `:49`, `:65` | no. Exchange `−n` is recorded at pull, *before* the batch lands here. | ✗ |
| O-2 | `OutputHandle` buffer / `OutputSession` | `handles.rs:114-118`, `:159-163`, `:191-201` | no | ✗ (transient) |
| O-3 | `pending_output` under backpressure | `wired_operators.rs:69`, `:146-158`, `:189-193`; 77 references across 13 operator structs | no; no capability | ✗ **H1** |
| O-4 | `unary_async` `pending_input`, in-flight tasks, result channel | `wired_operators.rs:1777-1785`, `:1888-1919`, `:1929-1938` | no. `async_waiting` blocks only *local* force-close (`executor.rs:1781-1790`). | ✗ H1 |
| O-5 | Notify stash plus capability | `handles.rs:494-515`; `progress/notificator.rs:106`, `:158` | ✔ capability, but inside `iterate` the capability is in `Product` time while the tracker is at `T` | ✗ inside loops (H11) |
| O-6 | Broadcast guard capability | `dataflow_builder.rs:4455-4469`; released at `wired_operators.rs:220`, `:246`, `:277` | over-conservative | ✗ H10 |
| O-7 | Source and input capability | `dataflow_builder.rs:467`, `:685`, `:820` | ✔ | ✔ |
| O-8 | Enter / leave loop counters | `wired_operators.rs:973`, `:1121` | M-d, per worker | ✗ H14 |
| O-9 | Feedback edge data | `dataflow/graph.rs:247-248` stores feedback edges separately; the iterate merge adds only `subgraph_builder` edges (`dataflow_builder.rs:2867-2869`) | **not in the reachability graph** | ✗ H11, probes after loops |
| O-10 | Loop-body operators in the parent `T` tracker with identity connectivity | `dataflow_builder.rs:2849-2866` | in-flight is projected to the outer time (`progress/subgraph.rs:104-118`; merged at `dataflow_builder.rs:2880-2884`) | partial (H11) |
| O-11 | User closure state (`unary`, `binary`, `for_each`) | `dataflow_builder.rs:1931`, `:2088` | nothing, and nothing detects a stash without `notify_at` | ✗ (forgettable by users) |
| O-12 | Public handle methods that inject or discard port data: `InputHandle::new`, `push_vec`, `mark_exhausted`; `OutputHandle::drain` | `handles.rs:35-55`, `:196-202`; re-exported publicly at `dataflow/mod.rs:37` | none | ✗. Under PCP they would let code forge a consumption or discard counted output (§5.2). |

### 3.4 Progress and executor layer

| # | Issue | Where (✔) | Audit |
|---|---|---|---|
| P-1 | Signed-sum completion | `reachability.rs:768-788`, `:827`, `:844` | C1 |
| P-2 | Destructive force-close on idle | `executor.rs:1781-1816`; `stage_executor.rs:483-501` | C1 |
| P-3 | `peers_heard_from` gates only force-close; frontiers are computed before peers' initial capabilities arrive | `subgraph.rs:902-914` | H13 |
| P-4 | In-flight reporter keyed by `(op, output)` | `subgraph.rs:152-153`, `:360` | H12 |
| P-5 | Debug symmetry check is single-worker only | `subgraph.rs:1055` | — |
| P-6 | Per-stage trackers drop cross-stage bindings | `subgraph.rs:513-514` (`retain_operators`) | H14 |
| P-7 | `SimpleRuntime::spawn_multi` wires cross-worker exchange channels (`runtime.rs:4010-4036`) but attaches **no** progress channels; trackers are worker-local (`runtime.rs:5848-5867`) | — | new ◇: frontiers after an exchange reflect only the local worker |
| P-8 | Graph edges (`state.graph`) and progress edges (`state.subgraph_builder`) are added separately at every factory | for example `dataflow_builder.rs:4673` and `:4690` | — (a divergence risk) |
| P-9 | `notify_at` accepts any time the observed input frontier permits, even if this worker neither consumed input at or before that time in this activation nor holds a capability there. The resulting `+1` is unjustified (A2), and a peer can notify early. | `dataflow/operators/handles.rs:438-452` (`check_time`) | new ✔ (pre-existing; found in review) |
| P-10 | The executor reports success as soon as every operator is done, before any tracker check. Staged convergence has its own success paths. | `executor.rs:1753-1756`; `stage_executor.rs:966-1000` | — (success is not funnelled through one gate) |

### 3.5 Execution path × transport matrix (today)

| | U (uniform) | F (fused or stage-task) | S (staged) | SR (`SimpleRuntime`) | CL Dedicated | CL Shared |
|---|---|---|---|---|---|---|
| Cross-worker progress | all-to-all broadcast | as U | **none across stages** (M-e unwired) | **none** (P-7) | per-peer outbox, then receive bridge (#291) | per-peer outbox, then receive bridge (#291) |
| In-flight (M-b) | partial | partial | dropped across stages | per-worker only | partial | partial |
| Completion | P-1 + force-close | as U | local done, plus M-d, M-h, and quiescence | per worker | P-1 + force-close, then #291 teardown (end-of-stream and ack; `DataLoss` makes losses loud) | as Dedicated |
| Selected when | uniform stage parallelism (`runtime.rs:1603-1651`) | always within U (`runtime.rs:5838-5850`) | default when per-stage parallelism differs, or when `per_stage_parallelism` is set without auto (`runtime.rs:1638-1648`) | `test-utils` | `spawn_cluster` | `spawn_cluster` |

### 3.6 Duplication (root cause 4)

✔ Counts on `main`:
- 13 `SchedulableOperator` structs in `wired_operators.rs`, each with its own pull, push, `pending_output` and `close_inputs` logic (13 `close_inputs`, 77 `pending_output` references).
- 24 `OperatorFactory::new` and 19 `ChannelFactory::new` sites in `dataflow_builder.rs`, each re-deriving the downcast, tee and in-flight wiring.
- Two sweep loops with completion and force-close logic, plus a third convergence rule.
- Two progress models (global broadcast, and per-stage plus unwired watermarks), plus `SimpleRuntime`'s local-only model.

---

## 4. Candidate designs (Phase 3)

### 4.1 Evaluation criteria

1. What it guarantees **by construction**, against A1–A5 and S1–S3.
2. What a contributor can still forget, and whether forgetting is *detected*.
3. Migration size and breaking changes, for custom operators and for downstream projects.
4. Hot-path cost: per batch or per record, atomics, extra progress traffic, cross-node volume.
5. Compatibility with fused stages, the shared pool, per-stage and heterogeneous parallelism, cycles, and PR7/PR8.

### 4.2 Design A: channel-embedded accounting plus RAII tokens (the hypothesis)

**Shape.**
- Every `Push` records `+n` at `(target, t)` when it accepts a batch, and every `Pull` records `−n` when it delivers one.
- Channel constructors require a reporter.
- Framework buffers outside channels (`pending_output`, async tasks, broadcast) hold RAII tokens.
- Completion uses positive semantics, and there is one executor core.

**Guarantees.** A1 for data *inside* channels. A constructor-required reporter is a compile-time guarantee per channel type.

**Still forgettable.**
- **Every buffer outside a channel needs a hand-placed token.** These are O-1 through O-4: `InputHandle`, output sessions, `pending_output` in all 13 operator kinds, and async pending input and results.
- Delivery-time `−n` happens when the operator *pulls* (`wired_operators.rs:161-187` pulls everything into `InputHandle`). The matching `+n` happens when the *downstream* channel accepts, which can be several sweeps later under backpressure. Every operator therefore needs a token across that window.
- The **reporter key** must be right at about 19 construction sites. H12 is exactly a key bug.
- Fan-out multiplicity has to be counted per branch and not in the tee (C-3); broadcast needs ×W (C-6); exchange needs per-sub-batch retraction (`exchange_channel.rs:407`, the bug class behind C4).
- Each new channel type, including future transports, must reimplement all of this.

**Migration.** Large and spread out. Every `Push`/`Pull` implementation changes: bounded, SPSC, tee, exchange, broadcast, network, mock, staged boundaries and `NullPush`. Every buffer site gains tokens. The `Push`/`Pull` traits change, and they are public.

**Hot path.** Today's reporters are `Arc<Mutex<ChangeBatch>>` (`progress/operate.rs:132-150`). A channel shared by a producer task and a consumer task needs either a lock or atomics on *every* push and pull, including pipeline edges.

**Compatibility.** Fine with fusion, the shared pool and per-stage parallelism. Cycles need the feedback-edge and nested-scope fix anyway (§5.1.7).

**Verdict.** It has the right invariant, but the choke point is the *channel*, and instancy has many channels and many buffers next to them. The opt-in problem moves from "wire the reporter" to "place the token".

### 4.3 Design B: targeted patches plus a debug invariant checker and a conformance matrix

**Shape.**
- Add `wire_inflight_source` at every missing producer factory.
- Key bindings by edge (H12).
- Make `pending_output` and async work hold capabilities.
- Make the in-flight reporter non-optional on `BroadcastPush`.
- Fix the predicate.
- Keep the two executors and the staged mechanisms, patched separately.
- Add the checker and matrix.

**Guarantees.** None by construction. Detection is good *if* the checker is strong.

**Still forgettable.** Everything, for the next operator or channel. Pipeline edges stay dark (C-1, O-1, O-2) unless every pass-through gets a guard capability, and guards freeze frontiers (H10's problem). The staged path (M-d, M-e, M-h) and `SimpleRuntime` keep their own semantics.

**Migration.** About 20 small diffs per PR, low risk each, but many PRs, and the logic duplicated across paths stays.

**Hot path.** As today.

**Verdict.** Not acceptable as the end state, because the user explicitly wants "no one can forget". **Its harness is essential** and becomes step 1 of the recommendation.

### 4.4 Design C: port-counted progress with a single operator shell (recommended)

**Shape** (detail in §5.1). Count at operator ports:
- `produced(out, t, n)` when logic hands a batch to its output handle;
- `consumed(in, t, n)` when logic takes a batch from its input handle;
- `internal(out, t, ±1)` for capabilities.

All three accumulate in a ledger owned by the operator *instance*. Its executor task drains the ledger between activations, under the commit protocol of §5.1.1. The tracker expands `produced` over every edge of that output, as timely's `extract_progress` does (`progress/subgraph.rs:752-781`). One `OperatorShell` owns all port I/O. Operator kinds supply only logic.

**Guarantees.**
- **A1, for everything between ports, by construction.** That covers C-1 through C-8 and O-1 through O-3. The edge abstraction includes them, whatever channel implements it.
- **A2, by construction, for pass-through work.** Consumed and produced are recorded in the same activation and committed together. Capability acquisitions are checked (§5.1.3).
- **A3.** Each commit is one batch, sent in order.
- **A5 by construction for edges.** One `+n` per produced batch per edge, one `−n` per consumed batch.

**Still forgettable, and caught.**
1. User code that stashes data across activations without a capability and *emits it later* is caught by the justification check. That check is V3, which ports the intent of timely's `validate_progress` (`subgraph.rs:788-819`) but uses the stricter local rule of §5.1.3. The shell makes it a hard error. Stashing data and *never* emitting it is indistinguishable from filtering, and no design can catch it (§5.3).
2. A pact's record multiplicity (broadcast ×W) is declared in one place and *detected* by the residual and oracle checks.
3. A new operator kind cannot bypass the shell: raw `Push` and `Pull` are unreachable, and the port handles cannot inject or discard data.

**Migration.** Large but mechanical, and concentrated:
- a new `dataflow/shell.rs`;
- `wired_operators.rs` rewritten as logic implementations;
- `dataflow_builder.rs` factories simplified;
- `progress/subgraph.rs`, `progress/reachability.rs` and `dataflow/executor.rs` changed;
- `channels` lose the in-flight hooks.

**Public API.** The closure *signatures* are unchanged; `retain` is added to the output handle (§5.1.3). Breaking changes:
- `SchedulableOperator`, `OperatorFactory`, `ChannelEndpoints`, `Push` and `Pull` become crate-private or sealed (re-exported today at `dataflow/mod.rs:40` and `dataflow/channels/mod.rs:26`);
- the injection and discard methods of `InputHandle` and `OutputHandle` become crate-private (O-12).

No public builder method accepts a custom factory (✔ grep), so the first break is likely invisible to downstream projects; confirm it.

**Hot path.**
- Two `ChangeBatch::update` calls per batch per edge (a `Vec` push plus amortized compaction, `progress/change_batch.rs:95-98`). There are **no atomics**: the ledger is plain data accessed through `&mut` by the executor that owns the operator.
- **Mandatory worker-wide compaction.** Consumed lives in the consumer's ledger and produced in the producer's, so they can cancel only in a worker-wide `ChangeBatch<(Location, T)>` after edge expansion. Today the outgoing buffer is cloned to every peer without that compaction (`subgraph.rs:1085-1109`). Commit must compact before sending.
- Other costs: one producer batch becomes several consumer batches (exchange), target expansion clones timestamps, broadcast allocates per peer, and justification checks compare antichains.
- Exchange edges get *cheaper* (no `Mutex` per sub-batch). Pipeline edges gain the two updates.
- **Traffic.** One current-format `u64` entry is about 33 bytes. Two non-cancelling entries sent to three peers are about 198 bytes before framing, roughly 2.4% of a 1024-record batch of 8-byte records. Traffic therefore scales with peers, timestamp reuse and batch size, not as a fixed fraction. A queue that stays non-empty across sweeps produces updates every sweep. Mitigation is timely-style demand-driven sending (§5.1.6). Measure with the prototype (§6).

**Compatibility.**
- Fusion: unaffected, since accounting is at ports.
- Shared pool: activations and drains happen on one task.
- Per-stage parallelism: needs a global progress graph with heterogeneous instancing (§5.1.8).
- Cycles: need the feedback edge in the graph inside a nested scope (§5.1.7).
- PR7/PR8: §5.5.

### 4.5 Design D: in-band watermarks or barriers (Flink, MillWheel, RisingWave; the staged path's intended design)

**Shape.** Each sender emits `FrontierUpdate` / `SenderDone` in-band on each exchange channel. Receivers take the minimum over senders. Barrier variants (RisingWave, Flink ABS) inject epochs at sources and align them at multi-input operators.

**Guarantees.** No all-to-all broadcast: progress traffic is O(edges). It fits per-stage parallelism naturally. Flink and RisingWave show it scales.

**Fatal gaps for instancy.**
1. **Cycles.** ◇ On a cycle, each sender's outgoing watermark depends on its own incoming watermark. Iterating from `minimum` reaches only the *least* fixpoint, which climbs one iteration per round and never jumps to "done" for an outer epoch. Flink's snapshot protocol has to *log in-flight records on back-edges* to handle cycles (Carbone et al. 2015, quoted in the research). Native event-time progress inside iterations is not a supported feature. Loops would still need counting, which is today's M-d hack.
2. **The sender's watermark must account for its own buffered data.** MillWheel defines the low watermark over *"the oldest unfinished (in-flight, stored, or pending-delivery) record"*. Today's `current_output_frontier` (`stage_executor.rs:739-766`) uses only stage-local capabilities, so it would pass `pending_output` and pipeline data. Producing a correct watermark needs design C's precise local counting *inside* each sender.
3. It isn't implemented: M-e is unwired.

**Verdict.** Not a primary design. It is a possible later **optimization layered on C**: hierarchical summaries at acyclic stage boundaries, to cut broadcast volume (§5.7, later and optional).

### 4.6 Design E: credit recovery or weight throwing (Mattern 1989; Huang 1989)

**Shape.** A conserved, non-negative credit travels with each message and returns to a controller. Termination is detected when the controller has recovered all of it.

**Guarantees.** Quiescence detection with no negative counts.

**Gaps.** It gives *global quiescence only*, not per-timestamp frontiers: notifications, probes and per-epoch aggregates still need reachability. It needs a central controller, a bottleneck and single point of failure. Credit must be split on every fan-out and exchange, per batch. **Rejected.** Its lesson, that the true quantity must never go negative, is A5 and the V5 residual check.

### 4.7 Comparison

| | A: channels + tokens | B: patches + checker | **C: ports + shell** | D: watermarks | E: credit |
|---|---|---|---|---|---|
| C1 (predicate) | with exact zero | with exact zero | with exact zero | n/a | yes |
| C2, H12 (opt-in, key) | channels by construction; key per site | patched | **by construction** | n/a | tokens |
| H1: pipeline | yes | ✗ or guards | **yes** | needs C locally | yes |
| H1: `pending_output`, `InputHandle` | token per buffer (13 kinds) | patched | **yes (on the edge)** | needs C locally | tokens |
| H1: `unary_async` | token | patch | capability in the shell | needs C | token |
| H10 broadcast | yes | patch | yes (remove the guard) | yes | yes |
| H11, cycles | needs nested scope | patch | **nested scope (§5.1.7)** | ✗ (least fixpoint) | quiescence only |
| H14, staged | yes with one core | separate paths remain | **one executor core (§5.1.8)** | natural, minus cycles | central controller |
| What can be forgotten | tokens, keys, multiplicity | everything new | stash-and-emit-later (*rejected*), stash-and-drop (undetectable by any design), pact multiplicity (*one place*) | buffers in the watermark, cycles | weight return |
| Atomics on hot path | per push and pull | as today | **none** (worker-wide compaction per commit) | none | per message |
| Progress traffic | all edges | as today | all edges with a non-zero net per commit; scales with peers (demand mode mitigates) | O(edges), in-band | to a controller |
| Breaking public API | `Push`/`Pull` | none | shell-internal types, handle injection methods, stricter `notify_at` | exchange protocol | many |
| Migration | large, spread out | small × many | large, mechanical, concentrated | large, and still needs C | large |

---

## 5. Recommendation (Phase 4): port-counted progress (PCP)

### 5.1 Design

#### 5.1.1 Ledger and ports

- `PortLedger<T>` belongs to each operator instance. It holds `consumed[in]`, `produced[out]` and `internal[out]` as plain `ChangeBatch<T>`. `OperatorProgress` already reserves `consumed` and `produced` fields *"for future message-flight accounting"* (`progress/operate.rs:211-218`).
- **`CountedInput<T, D>`** replaces raw `Pull` plus `InputHandle`. Taking a batch out records `consumed`. Data waiting in it, in the channel, or in a receive bridge stays counted at `Target(op, in)`.
  - Logic receives a borrowed view that can only *take* batches, read the frontier and check exhaustion. Nothing can inject or re-queue data (O-12).
  - **Envelope contract:**
    - data is counted;
    - `Control::Watermark` is advisory and never used as a frontier (frontiers come only from the tracker);
    - `Control::Error` is fatal: the operator fails the dataflow, for every transport including MockNetwork (C-16).
- **`CountedOutput<T, D>`** replaces `OutputHandle` plus raw `Push` plus `pending_output`. When the shell takes the logic's output after the logic returns, it records `produced`. Anything the logic discarded before returning was never counted, which is the same as a filter. The retry queue and the channel beneath it are "on the edge". Logic cannot drain its own counted output.
- **External egress** (a sink's `pending_send`, C-15). The sink defers recording `consumed` for a batch until the external channel *accepts* the event. `CountedInput` hands the sink a **credit token** `(t, n)` with the batch instead of recording `−n`; the sink keeps it next to the pending event in every build, and redeeming it records `−n` (R37). Until then its input credit stays in the view, so exact-zero completion cannot pass the pending event. A dropped receiver keeps today's explicit discard semantics (H15 fix); the discard redeems the token, so it is recorded, not silent. The same rule applies to any future shell store that holds consumed data off-ledger: keep the input credit until hand-off.
- **The constructor needs the edge identity and the ledger.** Only the materializer can build these types. Logic never sees a `Push` or `Pull`.
- **Commit protocol (`ActivationTxn`).** The ledger is drained only between activations. Per activation or per sweep are both fine: timely also batches many activations into one send (§2.5). A commit batches deltas only, never justification state (§5.1.3). Before the executor acts on shutdown (S2a), worker termination (S2b), a seal or a clean end-of-stream, it must, in order:
  1. drain every hosted ledger and every release mailbox, then **compact** worker-wide after expanding edges;
  2. self-apply the batch;
  3. enqueue every fragment, positives-first, on every peer lane, including any batch held back by demand-driven sending;
  4. only then mark the operator closed or queue the end-of-stream.

  A logic-level `Done` or exhaustion is only a hint (R36): it never closes outputs or counts toward completion; only S2a shuts an operator down.

  A panicking or failed activation commits nothing. Data it already put on an edge is unmatched, but the activation's consumed input stays counted upstream, so no view can complete early. Capabilities and credit committed by earlier activations are not released, and the failed worker does not seal (R34). Since #291 the failure is already fatal to the dataflow: in-process, a worker that completes with an error cancels its siblings; on a cluster, a node whose workers fail broadcasts `Cancel` (`runtime/cluster_lifecycle.rs:590-602`).

- **One decision module, three decision types.** Every way an executor can report success funnels through one module. Today there are several: natural exhaustion with all operators done (`executor.rs:1753-1756`, P-10), idle force-close, drain completion and staged convergence (`stage_executor.rs:966-1000`). The idle-sweep force-close is deleted. Each decision first requires the commit protocol to be complete. Then:
  - **operator shutdown (S2a, §5.1.5):** the operator's input frontiers are empty, it holds no capability, and it has nothing left to hand over;
  - **worker success (S2b, §2.2):** all hosted operators are shut down, and the worker's scope is exactly zero. A non-zero count in scope means *not ready*. It becomes an error only as a proven residual after the final accounting boundary;
  - **dataflow completion (S2c):** every worker has succeeded.

  `max_idle_sweeps` decides only when to park.

#### 5.1.2 Operator shell

`OperatorShell<L: OperatorLogic>` is the only `SchedulableOperator` implementation for built-in kinds. It implements, once:
- activation: pull into the counted inputs, run the logic, flush the counted outputs, and handle backpressure;
- capability bookkeeping;
- the shutdown rule (§5.1.5);
- local-data-loss detection (S3);
- the justification and late-output check V3 (an error in every build), plus the debug check V1.

The 13 `Wired*Operator` structs become `OperatorLogic` implementations: unary, notify, binary, concat, branch, fork, ok/err, source, sink, async, enter, leave and feedback. The 13 copies of `close_inputs` and `pending_output` collapse to one.

#### 5.1.3 Capabilities are the only token for operator-held work

Anything an operator keeps *across activations* and may later emit at `t` must be backed by a capability at `≤ t`. This is A1 for O-4, O-5 and O-11.

**Acquisition rule (release-enforced, all kinds).** For each logic call (one activation of the operator's logic) the shell keeps a **justification set** `J`. It starts with the times of every capability the operator holds when the call starts, and grows with:
- every capability acquired during the call, under this rule;
- `summary(in→out)(t′)` for every batch at `t′` actually *consumed* in this call, using the summary of the input that batch came from. This matters for binary operators.

Capabilities dropped during the call, for example by `next_notification()`, stay in `J` until the call returns. `J` is validated and dropped when the call returns, before the call's delta joins the worker's pending commit. A commit may batch many calls' deltas but never their `J`, so input consumed by one call can never justify output from a later one (R36). An operator may *acquire*, *downgrade to*, or *emit output at* `t` on an output only if some element of `J` for that output is `≤ t`.
- *Sound:* any emission or acquisition is committed in the same batch as the consumption or capability release that justifies it (A2).
- *Not frontier-based:* the input frontier alone is never a justification, and neither is a batch that is merely available but not consumed.

Today `notify_at` accepts any time the frontier permits (P-9, `handles.rs:438-452`). Here is a concrete early notification:
1. Worker A holds a source capability at 0 and sends B a record at 10.
2. B calls `notify_at(0)`, and its frontier still permits 0.
3. A releases its capability.
4. Observer C applies A's release before B's new `+1`, and notifies 0.
5. B later emits at 0.

This mirrors timely, where capabilities come only from an input message's capability or an existing capability. Legitimate patterns stay valid:
- forward `delay` and `delay_batch`, which are justified by the batch consumed in the same activation;
- handling a notification and then emitting, or re-arming `notify_at` at the same time; `J` keeps the capability held at the start of the activation, even after `next_notification()` drops it;
- `retain` followed by emit-and-drop in the same activation;
- source epoch downgrades, which are justified by the current source capability.

A request based only on an earlier activation's data, without a held capability, is rejected. Violations are errors in every build; the check is one antichain comparison per acquisition or output batch.

- **Notify family** (reduce, fold, count, distinct, delay): `notify_at` is unchanged in signature but obeys the acquisition rule.
- **`unary_async`:** the shell acquires a capability at `t` in the activation that consumes a batch into `pending_input`. It holds the capability through the pending, running and finished-but-not-collected states. It releases it in the activation that hands the result to `CountedOutput`, so the produced count and the release land in one commit (A2). Cancellation and task failure release it only together with failing the operator.
- **Generic `unary` / `binary` closures** get no implicit capability. Data may wait in `CountedInput`, where it is counted. To keep data across activations correctly, the borrowed **output** handle gains `output.retain(t) -> Retained<T>`: a capability for that output, released on drop. The closure signatures don't change.
  - An operator that holds a `Retained` is re-activated when its input frontier changes. The closure can read `input.frontier()` and `input.is_exhausted()`.
  - **The final flush call is triggered when the input frontier becomes empty, not by channel exhaustion.** A frontier empties as soon as no upstream work can reach the operator, which happens before end-of-stream. Every closure-based operator gets this one final call, whether or not it holds a `Retained` (R45); calls on earlier frontier changes happen only while it holds one.
  - **Rule for every shell-owned obligation:** no capability or input credit may be released *only* on channel exhaustion. Inside a loop, end-of-stream arrives only after the loop's operators shut down (S2a), and they can't shut down while a capability that reaches them around the back-edge is held, so the two would wait for each other. Elsewhere, releasing on the frontier still lets results flow before end-of-stream.
  - Inside `iterate`, `retain` uses the nested scope's frontiers (§5.1.7).
- **What V3 can and cannot catch.**
  - It *can* catch output at `t` with no element of `J` at or below `t`. Such output is *late data*, and the shell rejects it **in every build**.
  - It *cannot* catch a closure that moves consumed data into captured state and never emits it. That looks exactly like a filter, and timely has the same limitation.
  - Stashed data emitted later, including the common "flush on end of input" pattern, fails loudly instead of silently arriving behind the frontier. The fix for users is `retain`.
  - timely's `validate_progress` checks against source *implications* (`subgraph.rs:795-819`). V3 and the acquisition rule use the stricter local rule above deliberately.
- **Broadcast and the exchange pass-through:** the `T::minimum()` guard (M-c) is deleted (H10).
- **Cross-task release.** User closures, including async ones, receive `(time, data)`, never a `Capability` (✔ today: `dataflow_builder.rs:2044-2061`). `Retained<T>` is `Send`, because closures must be `Send`. Dropping it anywhere, on any task, posts the release to the owning operator's mailbox and wakes its executor through the existing `WakeHandle`. That is the only cross-task path. Port counts (`consumed`, `produced`) stay lock-free.

#### 5.1.4 Tracker changes

- **Completion predicate:** exact zero (§2.4) over the worker's scope, applied by the decision module for worker success (§5.1.1).
  - Frontier and shutdown decisions (S2a) use positive semantics, as in timely. `total_counts` is maintained as the sum of frontier sizes, updated only on frontier changes (`reachability.rs:425-431`, `:673`), for that purpose.
  - **Residual at success.** A count still non-zero after the final accounting boundary is an error in every build ("progress residual at completion"), consistent with S3.
- **Seeding (A4):** every worker seeds every operator's initial capabilities with multiplicity equal to the number of instantiating workers. It then broadcasts only its own releases. Delete `peers_heard_from` (P-3). The multiplicities come from a canonical **physical plan** (the operator-to-worker assignment, pact recipient sets and multiplicities), whose digest is part of the cluster handshake (§5.1.8).
- **Drain:** consumed goes to `Target(op, in)` as `−n`; produced goes to *every* accounting target of `(op, out)` as `+n·mult`; internal goes to `Source(op, out)`. Everything drained between two commits is compacted into one outgoing batch (§5.1.1).
  - `mult` comes from the edge's **delivery kind** in the physical plan (R35): 1 for pipeline and partitioning-exchange edges, `|hosts(target)|` for broadcast. Today's plan can't tell the two apart (`EdgeRouting` has one `Exchange` variant for both), so step 3 adds the delivery kind, derived from how the builder creates the edge, and includes it in the plan digest.
- **Edges are derived from the materialized graph**, so P-8 can't diverge. Produced lands on every data edge of an output, keyed by the edge. Each loop is its own scope (§5.1.7). Its back-edge stays in the scope's graph with a `+1` iteration summary, so no identity cycle arises, and the parent sees the scope as a single node.
- **Probes and output ports are read-only projections**, not counted targets. A probe on `(op, out)` reports the meet of the input frontiers of the *real* targets of that output. That includes data queued on those edges and everything upstream. It falls back to `F(Source(op, out))` when the output has no consumer. Nothing is ever counted at a probe, so nothing can leak there.
- **Residuals.** A non-zero count in a worker's scope is *not ready*, not an error, because a peer's batch may still be in transit (FIFO doesn't stop data overtaking its own progress update). It becomes a **proven residual** only once this worker has applied every worker's seal marker (§2.2, S2b), and then it is an error in every build. The in-process progress channel (`progress/progress_channel.rs:109-239`) has no terminal state today; the redesign adds the in-order seal marker.
- Delete M-b: `set_inflight_reporter` on `Push`/`Pull`, `ExchangePush`/`ExchangePull` in-flight fields, `register_inflight_edge`, `inflight_by_source` / `inflight_by_target`, `wire_inflight_*`, and the debug `inflight_net`.

#### 5.1.5 Shutdown replaces force-close

An operator instance is shut down by S2a, after which it closes its outputs (end of stream). A worker terminates by S2b, and the dataflow completes by S2c.

- `max_idle_sweeps` stops being a completion criterion and decides only when to park. M-g and the M-h `feedback_deps` counters are deleted.
- A record that reaches a shut-down operator is an error (S3), as in timely (`subgraph.rs:731`).
- End-of-stream exhaustion stays a data-plane signal. In debug, its consistency with the frontier is asserted (V4).
- **S2a applies everywhere,** loop bodies included: frontiers are trustworthy on every path once every worker has a whole-graph view and loops have nested scopes. Both land together in the redesign (§5.7).
- **S2b (worker success) is scoped**, exactly as defined in §2.2.
  - Computing (c) needs only location-level reachability on the data graph, including back-edges and ignoring timestamps.
  - By S1, (c) is safe even though the view may be stale.
  - A worker with an unfinished probe cannot retire. When it does retire, its probe writers finish with an empty frontier.
  - A retired worker's view is frozen, because PR7 discards later progress sent to it. Entries it may keep are outside its scope.
- **S2c (dataflow completion)** is every worker succeeding. Because every location is hosted somewhere, the scoped checks jointly cover the whole graph (§2.2), so no worker has to stay behind as a final observer, and `join()` doesn't wait for a peer's teardown. A worker can finish *early*, while peers' inputs are still open, only if it hosts nothing those inputs reach; on today's uniform clusters that never happens (§2.2, R47).

#### 5.1.6 Wire rules

- One batch per worker per commit (§5.1.1), compacted worker-wide.
- Receivers apply fragments in FIFO order per sender (unchanged).
- If the encoder must split a batch, it sorts **positives first** (C-12, A3), across scope sections too (§5.1.7). §2.5 gives the monotonicity argument, which does not need reassembly; the model checks confirm it, and that splitting by section fails (§5.9). PR8 (#292) already ships it for flat batches, and step 1's V6 check confirms it. Step 3 extends it across scope sections.
- **Demand-driven sends** (optional, later): hold the outgoing batch until it contains a decrement that could move some frontier. This is timely's `is_global` test (`subgraph.rs:526-546`), and Naiad's accumulation rule shows it is safe.

#### 5.1.7 Loops: nested scope tracker

◇ Projecting loop-body counts onto the outer time *and* keeping the back-edge would create an **identity cycle**. A pointstamp would then keep supporting its own implication around the loop, and the frontier would never advance: instancy's propagation follows edges step by step (`reachability.rs:870-940`), as timely's does. timely's `Builder::build` detects such cycles and prints a warning. It does not reject them (`reachability.rs:215-218`, ✔): it calls them *"a serious liveness issue"*. instancy should **reject** them at build time. The correct structure is timely's subgraph nesting.

**Scope bridge.** The rules below follow timely's `Subgraph` (`subgraph.rs:382-387`, `:390-470`, `:550-555`; `reachability.rs:554-577`, `:662-700`). Each scope carries **two separate computations**, and an implementation must not merge them:
- *child frontiers*, used for scheduling and notifications inside the scope, which **include** the imported parent input frontier;
- *internal-work projection*, used for the scope's capabilities in the parent, which **excludes** the imported frontier and messages that have already left.

1. **Downward import (child frontiers).** Each worker translates the parent's input frontier at `Target(scope, i)` into derived pointstamps at the child's boundary source `Source(0, i)` in its own child tracker. This is timely's `accept_frontier()`, which runs before children are scheduled.
   - The import is local and derived, never broadcast.
   - It is seeded at initialization and updated whenever the parent frontier changes. The child's frontiers, and so its notifications, therefore respect parent records that have not entered yet, or that may still be sent under a parent capability.
2. **Child-level port counts are local and broadcast once.** Each worker's commit carries a section per scope, holding the child-tracker updates of operators inside that scope, in `Product<T, I>` time.
3. **Upward export (internal-work projection).** Each worker computes the scope node's internal capabilities at `Source(scope, o)` from its own global child view. It projects only *internal child pointstamps* (child capabilities and messages held inside the child) through precomputed summaries to the scope outputs.
   - Boundary node 0 is excluded from that summary table, so imported parent support is not counted. This matches timely's exclusion of node 0 from the output-summary tables (`reachability.rs:554-577`) and its export of changes derived from internal pointstamps (`:662-700`).
   - Exporting the ordinary child frontier at the leave instead would turn the parent's statement about possible future input into self-sustaining internal work. On an outer feedback path that never drains.
   - Export results are applied to the worker's own parent view only and are never broadcast. This is timely's treatment of a non-`local` child, whose extracted progress goes to `final_pointstamp`, post-exchange. Broadcasting derived updates would count one exiting record once per worker.
4. **Boundary messages are ordinary port counts, recorded once by the worker that moves them.**
   - Enter consumes at parent `Target(scope, i)` (parent section) and produces at the child's targets (child section).
   - Leave consumes at the child `Target(leave)` (child section) and produces on the parent's edges from `Source(scope, o)` (parent section).
   - A message that has left is therefore not part of the internal-work projection.
5. **Cross-scope atomicity.** All sections of one commit travel in one batch, fragmented positives-first across sections. A receiver applies the parent section and runs the downward import before computing child frontiers.
6. **Wire format.** Progress entries gain a scope id, and each scope encodes its own timestamp type. This is a progress-frame format change, versioned in the handshake (#286). It ships together with the seal marker in step 3's version bump, to v5 (§5.5).
7. **Implementation rules** (R38, from step 3's design note):
   - **One pass per apply.** After applying every section's raw counts, compute exports bottom-up, then propagate top-down: the parent, then the import into the child, then the child, recursively. Exports don't depend on imports, so one pass is exact; `initialize` follows the same order.
   - **Raw counts only, everywhere decisions count.** Commits carry raw counts only, never import or export entries. Exact zero and residual reports ignore the derived locations: child `Source(0, i)` and the parent's `Source(scope, o)`. A debug assertion checks that a worker's derived entries are empty whenever its raw counts in scope are zero.
   - **Every worker has every scope tracker**, whether or not it hosts a body operator, because its parent view needs the scope's export. A4 seeding applies in each scope.
   - **The scope node gets its own id** in the parent, outside the operator range, so no lookup can confuse it with the `enter` operator.
   - **Identity cycles are rejected twice:** at `iterate`, with a clear error, and by a general check in the tracker builder on every scope graph.

Required regressions:
- A parent input stays open while the child temporarily holds no messages. An aggregate inside the loop must not fire.
- A nested scope lies on an outer feedback path. After all real work drains, its exported capability set must become empty.

This fixes H11, probes after loops, and nested `iterate` with exchange (#281). It deletes the outer-time projection (`progress/subgraph.rs:104-118`). M-d, the per-loop counters, goes earlier, in step 2, with its only reader (§5.1.8).

**Feasibility note.** Today the iterate merge flattens the body into the parent tracker with identity outer connectivity (`dataflow_builder.rs:2849-2884`). Execution can stay flat: the body's operators can keep running in the parent executor's fusion groups. *Progress* cannot. The redesign must keep a real child tracker per scope, with its own summaries in `Product<T, I>`, and must stop merging body operators into the parent tracker.

**No interim loop handling.** An earlier plan switched to port counting before nested scopes existed. It therefore needed a temporary guard edge, and a ban on observing frontiers inside loop bodies. With no users to protect, nested scopes land together with port counting, and the guard edge and the in-loop observation ban are dropped. The interim model in [`tla/`](./tla/README.md) is kept as evidence: without a guard, today's "probe after a loop" bug reproduces.

#### 5.1.8 One executor core (step 2)

Step 2 lands **before** the redesign, so step 3 builds port counting once, in one place, instead of building it for the staged executor and then deleting it.
- `DataflowExecutor` hosts the subset of operators assigned to its worker. Fusion groups and stage tasks stay as scheduling groups inside it; per-stage and heterogeneous parallelism are unchanged.
- ◇ It is feasible: `DataflowExecutor` already maps logical operator indices to local positions and filters fusion. The work is materialization, because today every edge is built (`executor.rs:632-787`). The staged path's participating-endpoint wiring, stage-specific worker contexts, external inputs and feedback boundaries (`runtime.rs:2309-2488`, `:2603-2668`) must move into the core.
- Each worker has one tracker over the full graph, attached to progress channels to every worker. `SimpleRuntime` already spawns the same executor on its own threads (`runtime.rs:3979-4060`); `spawn_multi` gains progress channels (P-7).
- Initial multiplicities come from the physical plan (A4 seeding, §5.1.4). Clusters currently *reject* heterogeneous stage parallelism (`validate_stage_parallelism(.., total_workers)`, `runtime.rs:3153` on `ba1bd37`, ✔). The plan digest joins the handshake fingerprint now (`communication/control_protocol.rs:390`), so two nodes can never seed different multiplicities, even before heterogeneous clusters are allowed. A4 changes the meaning of a progress stream, so step 2 bumps `PROTOCOL_VERSION` to 4 (§5.5).
- Delete `StageExecutor` and `CombinedStageExecutor` (M-e, M-h), per-stage trackers (P-6), the per-loop counters (M-d, whose only reader is `CombinedStageExecutor`) and the staged drain logic.
- **The physical plan** (refined on `ba1bd37`). One function computes it from the graph's stages, the spawn's worker count and the cluster topology. Materialization, seeding, probes, the handshake digest and the checker all read it, and nothing else decides hosting:
  - `hosts(op)` is the set of global workers running `op`: workers `0..par(stage(op))` (`runtime.rs:2575`, `:2802`);
  - each hosted operator keeps its stage's worker context `(worker, par(stage))`, as today (`runtime.rs:2851`); cross-stage exchange channels use `(worker, total)` with `(source_par, target_par)` (`runtime.rs:2442`, `:2521`, `:2659-2668`);
  - an exchange or broadcast edge is built on every worker that hosts either end;
  - a pipeline edge needs every worker that hosts its source to also host its target. That is a spawn-time error otherwise, as the docs already promise; today such data is silently dropped (H14). Workers that host only the target get an input that is already exhausted. A feedback edge keeps today's stricter rule, equal host sets (`runtime.rs:2726`): step 2 accepts no shape it used to reject, and loops that change parallelism are revisited with nested scopes in step 3;
  - the seeding multiplicity of each initial capability is `|hosts(op)|`. Counting fewer is unsafe (TLA scenario `hetero_localseed`); counting more hangs (`hetero_workerseed`);
  - a probe on `op` expects `|hosts(op)|` writers, not the total worker count (`probe.rs:126-160`). `SimpleRuntime` builds once per worker, so each of its probe handles has one writer but expects N. A finished writer's last view then keeps holding that handle, which is conservative, as on a cluster node; S2b fixes the stale view in step 3;
  - `RuntimeHandle::spawn_multi` builds the dataflow once and materializes it per worker, on every path, with no separate probe build. The plan reads the built dataflow, so `build` runs once, not `N + 1` times;
  - clusters keep rejecting heterogeneous parallelism in step 2. Allowing it is a feature, which needs the user's decision.
- **Behaviour after step 2.** Port and in-flight accounting is unchanged; what changes is which book each worker reads, and how it is initialized.
  - The staged path (S) and `SimpleRuntime` multi-worker (SR) both switch to live whole-graph books, so both inherit the uniform path's known accounting bugs (C2, H1, H12 and the rest) until step 3. On a whole-graph view, a `+n` and a `−n` from different workers do cancel, so on S and SR a frontier may move earlier than today wherever today's local view happened to be conservative.
  - A4 seeding is the one deliberate semantic change, and it is conservative: every initial capability is known before any worker runs, so no frontier can move earlier because of it (H13).
  - Tests on S or SR that fail after step 2 because of an inherited uniform-path bug are recorded as known failures in the step 1 harness, each tied to the bug that step 3 fixes. Nobody depends on today's S or SR behaviour, so a temporary regression there costs nothing.
- Broadcast scope stays all-to-all among the dataflow's workers (today's U behaviour). Reachability-filtered fan-out is an optional later improvement.

### 5.2 What becomes impossible to forget, and how

| Hazard today | Enforcement in PCP |
|---|---|
| Producer forgets `wire_inflight_source` (C2) | No such call exists. The shell records `produced` when it takes the logic's output. **Type:** logic can't reach a raw `Push` or `Pull` (they become `pub(in crate::dataflow)`, used only by channels and the shell), and the port views can't inject, re-queue or drain data (O-12). |
| Wrong or colliding reporter key (H12) | The key is the materialized edge itself. The tracker expands `produced` over the edge list. |
| New buffer outside a channel (H1) | Buffers next to a port live *inside* `CountedInput` or `CountedOutput`, so they are on the edge by construction. Shell stores that hold consumed data (egress) keep their input credit until hand-off. Any other cross-activation state needs a capability (`retain`). Emitting un-retained state later is rejected (V3, every build). Dropping it silently is indistinguishable from a filter (§5.3). |
| New operator kind bypasses accounting (compare timely #721) | `OperatorFactory` returns `OperatorShell<L>`. `SchedulableOperator` becomes crate-private and sealed. A unit test asserts every factory produces a shell. |
| New channel or transport | It needs no accounting code. It must meet the **edge contract**: deliver every accepted batch exactly once, keep `t`, and keep the record total per `t` times the declared multiplicity. Violations show up as residuals (V5) or oracle failures (V7). |
| Graph and progress edges diverge (P-8) | Accounting edges and scopes are both derived from graph edges (§5.1.4). |
| A new execution path re-implements completion | Every path reports success through one gate (§5.1.1), and step 2 leaves one core. The conformance matrix (§5.8) fails any new path. |
| An operator acquires a capability it can't justify (P-9) | The acquisition rule is checked in every build (§5.1.3). |
| An error envelope is silently ignored (C-16) | The `CountedInput` envelope contract makes it fatal centrally, for every transport. |

### 5.3 What can still be forgotten (residual risk), and how it's caught

1. **A user closure stashes consumed data without `retain` or `notify_at`.**
   - *Emitting* it later is caught by V3 in every build.
   - *Never* emitting it, or losing it when the operator shuts down, cannot be told apart from a filter. No design can catch it, timely included.
   - Mitigations: the `retain` API makes the correct pattern easy; the docs for `unary` and `binary` state the contract; the logic is called once at end of input, with retained times, so it can flush.
2. **The pact multiplicity is wrong** (for example a new broadcast-like pact). It is declared once per pact. V5 catches a net residual; V7 catches same-pointstamp masking that V5 cannot see.
3. **Data left in `CountedInput` while waiting for its own frontier** self-blocks; this is the same as timely. ◇ It hangs rather than returning wrong output. A debug liveness diagnostic flags an idle operator whose own input is the only thing holding its frontier.
4. **A transport loses or duplicates frames** (C7 is PR9's). Loss turns into a hang (positive leak) rather than silent loss. #291 already reports a loss it can see: `DataLoss` and link failures. PR8 makes the remaining sources fatal: data decode errors, and EOF treated as exhaustion. V5 reports residuals.
5. **The commit protocol or the success gate is skipped** by a new executor path, for example one that acts on `Done` before draining. Step 2 leaves one core, so there is one place to get this right. V4 and V5 assert it at every termination.
6. **A new shell store holds consumed data off-ledger** (as egress does) without keeping its input credit. This is a shell-internal rule, reviewed in one module. V7 measures the physical occupancy of every shell store independently (§5.8).

### 5.4 Not a regression toward timely's execution model

| Kept from instancy | Borrowed from timely (invariant level only) |
|---|---|
| Async shared worker pool and Tokio I/O; executor futures, not OS threads | Per-port produced/consumed/internal counts, published together per activation (`builder_rc.rs:265-282`), and many activations batched into one send (`subgraph.rs:292-345`, `:526-546`) |
| Fused stages, stage tasks, per-stage and heterogeneous parallelism | Positive-frontier semantics for frontiers and shutdown (`reachability.rs:425-431`); completion is stricter (exact zero) |
| Closure builder API; **no user-visible capabilities** | Shutdown rule and the "prematurely shut down" panic (`subgraph.rs:366-374`, `:731`); capabilities only from input or existing capabilities |
| Pluggable transport; per-dataflow membership | The intent of `validate_progress` (`subgraph.rs:788-819`), with a stricter local rule |
| Backpressure-aware channels | Nested scope tracking for loops; demand-driven sends (optional) |

The progress protocol is independent of threading. Its state lives in the executor future that hosts a worker's operators, which may migrate between pool threads.

### 5.5 Interaction with PR7 (#291, merged as `e7ac01e`) and PR8 (#292, merged as `9491e66`)

**Frames and protocol version.**
- PR7 sets `PROTOCOL_VERSION = 3` (`communication/control_protocol.rs:103`).
- It reserves two progress-frame counts, both unchanged by PCP:
  - end-of-stream is `u32::MAX` (`progress/network_progress.rs:283`);
  - ack is `u32::MAX − 1` (`:287`).
- The encoder caps real batches below the ack count (`:119`).

PCP changes the protocol twice, each time with one version bump. `ba1bd37` is still on v3; PR8 did not bump it.
- **Step 2, v4.** A4 seeding (§5.1.4): initial capabilities are seeded from the physical plan, not sent. The plan's digest joins the handshake fingerprint (`control_protocol.rs:390`).
- **Step 3, v5:**
  - the seal marker, as a third reserved count, `u32::MAX − 2`, with the encoder cap lowered below it;
  - scope ids on progress entries (§5.1.7).
- Positives-first ordering needed no bump, and PR8 already ships it.

**Ordering on `e7ac01e`.**
- Data frames bypass the outbox. They go straight into the bounded transport queue with `try_send` (`network.rs:347-394`).
- Progress deltas, progress end-of-stream, acks and close sentinels go through the per-peer outbox (`peer_link.rs:134-160`). It is unbounded and FIFO, and its forwarder awaits queue capacity (`:269-318`).
- So progress can lag data, which §2.5 allows.

PCP puts its own frames on the outbox lane:
- every committed fragment, positives first;
- the seal marker, behind the worker's last committed batch and ahead of PR7's end-of-stream.

Nothing PCP sends blocks: the outbox is unbounded.

**End-of-stream ordering** is a hard requirement. A FIFO lane orders only what has been put on it. Under the commit protocol (§5.1.1), a worker queues its seal marker and end-of-stream only after it has enqueued every committed batch, including any batch held back by demand-driven sending.

**Retirement fits the scoped S2b.**
- PR7's `ProgressSender` drops deltas meant for a retired peer (`network_progress.rs:594-596`). That is safe: by S2b(c), nothing the retired worker hosts can still be reached, probes included, and its scope was exactly zero.
- The retired view may keep entries outside its scope.
- S2c is "all workers succeed", so no worker has to survive as an observer (§2.2).

**Final accounting boundary.**
- PR7's end-of-stream is sent only after all local workers have succeeded, so it can't serve as the boundary: two workers stuck on a residual would each wait for the other.
- The boundary is the seal marker (§2.2), added in step 3. A receiver counts it only once its tracker has *applied* it. A bridge that has merely *received* it doesn't count. The bridge reports `ProgressEndOfStream` as soon as it reads the frame (`network_progress.rs:404-407`), which is not proof that the tracker has applied the batches queued before it.

**`NetworkPush` on a closed queue (#291 final).**
- A closed transport queue to a *non-retired* peer returns `Backpressure`, and the push reports `SendFailed` to the coordinator once (`network.rs:376-393`). The operator waits as it would for a full queue, and the coordinator decides; a Running-phase `SendFailed` is a soft failure until the whole link has closed.
- Under PCP the stalled batch was counted as produced when the shell took it, so it holds the target's frontier and blocks completion. Nothing can complete early, and the coordinator's cancellation ends the wait.
- A closed queue to a *retired* peer fails with `DataLoss` for records and silently discards control frames (`:349-371`).

**`DataLoss` checks stay, as assertions of S3.** They come from #291, and PCP keeps all of them. Under PCP none of them should fire.
- Sender side: records sent to a retired peer, including after the post-enqueue retirement re-check (`network.rs:349-371`).
- Receiver side, reported as `InboundDataLost`: records arriving after the channel's close marker or after the receiving operator finished (`network.rs:588-616`), and records left unconsumed when the pull is dropped (`:667-695`).

In §5.9's prototype runs, the completion changes alone (exact zero and no force-close in acyclic dataflows) already stopped them firing in #291's early-finisher scenarios.

**#291's early-finisher test oracle.**
- `cluster_early_finisher_routing.rs` accepts a *loud* early completion (`Failure::EarlyCompletion`) when every missing record is explained (`:189-204`). In the "tail rule", a feeding node that failed with `DataLoss` explains its missing records only if, at each of the receiving node's workers, they are the tail it never sent: every received record precedes every missing one.
- Minimum exact rounds:
  - routing scenario 1: 6 of 12 (`:461`);
  - routing scenario 2: 12 of 24 (`:492`);
  - routing scenario 3, the congested Dedicated links: **0 of 9** (`:532`);
  - `MIN_EXACT_SMALL = 0` for `many_small_frames_then_complete_{shared,dedicated}` (`cluster_graceful_teardown_shared.rs:449`);
  - `MIN_EXACT_LARGE = 2` for `large_records_from_early_finisher_never_vanish` (`:455`).
- All these scenarios are acyclic and have no frontier observers, so their completion depends only on exhaustion. §5.9 measured a prototype of step 3's completion changes (exact zero, no force-close in acyclic dataflows), with the processes pinned to 2 cores:

  | Scenario | `e7ac01e` | prototype |
  |---|---|---|
  | scenario 3 | 4 of 9 exact | 9 of 9 |
  | large records | 29 of 40 | 40 of 40 |
  | `many_small_frames_then_complete_shared` | 4 of 5 | 5 of 5 |

- **Exit criterion for step 3:** every minimum becomes the round count. `EarlyCompletion` stops being accepted, so any loss fails the test, and the tail rule is no longer needed for these shapes. Add frontier-observing variants too: a `count` and a probe after the exchange.

**`TeardownWaiter::block`** (`runtime/cluster_lifecycle.rs`) uses thread heuristics: `block_in_place` on multi-thread workers, and a liveness probe that returns `InvalidConfig` on a `current_thread` runtime nobody drives. A follow-up will replace the heuristics with an error raised inside runtimes. That doesn't affect progress: PCP adds no blocking wait, and its seal marker and fragments go on the unbounded outbox. Conformance-matrix cluster tests should join from an async context or `spawn_blocking`, so the follow-up doesn't change them.

**Batch splitting.** PR8 (#292) already ships positives-first ordering for flat batches. Step 3 extends it across scope sections, inside its version bump. With it, flat and scoped batches need no reassembly (§2.5).

**Failure.**
- A failed or panicking activation commits nothing (§5.1.1).
- PR7 already makes failures fatal cluster-wide:
  - a node whose workers fail broadcasts `Cancel` (`cluster_lifecycle.rs:590-602`);
  - a Running-phase hard link failure cancels at once, and a soft one (EOF) once the whole link has closed (`:880-896`).
- PR7 already makes progress decode errors fatal: the bridge ends with `LinkFailure::Decode` (`network_progress.rs:413-418`), and any progress-channel failure other than `ChannelClosed` is hard (`cluster_lifecycle.rs:733-736`).
- PR8 extends this to data-frame corruption, and to EOF that is today treated as exhaustion (C5).
- Peers can't complete early on an activation's uncommitted work, because its consumed input is still counted upstream.

**Clean close.** PR8's `close_on_drop`, under which only a successful worker closes cleanly, fits §5.1.5: shutdown under S2a is a success path. Sealing is separate, because it is accounting, not a clean close: a worker stuck on a residual still seals. A failed worker does not seal; the dataflow fails through cancellation instead (R34).

**No new ordering dependency for safety.** PCP doesn't need data and progress on one FIFO lane (§2.5), so PR9's transport changes can't break progress safety; they can only delay it. The end-of-stream rule above is a teardown rule, not a safety rule.

#### 5.5.1 Overlap with PR8 (#292, merged as `9491e66`)

| Concern | PR8 (cluster links) | PCP (step 3) |
|---|---|---|
| Decode error on a data frame | `NetworkPull` fails the channel instead of emitting `Envelope::error` (today `network.rs:719-722`), and the 10M-record decode cap is removed (H8) | Step 3 adds the **consumer-side** contract: an `Envelope::error` reaching any operator input is fatal. That also covers MockNetwork (`mock_network.rs:428-432`) and any future source. |
| Link loss and EOF | `NetworkPull` exhausts only on the exact close sentinel. Today a disconnected receiver counts as exhausted (`network.rs:705-708`, C5), and losses are reported to the coordinator. | Under PCP a falsely exhausted channel cannot complete anything: the sender's `+n` is still counted at the target, so the target's frontier holds and S2a can't fire. C5 becomes a hang instead of a loss, and V4 flags the mismatch between exhaustion and frontier. PR7's coordinator cancels on a hard link failure at once, and on a soft one (EOF) once the whole link has closed (`cluster_lifecycle.rs:880-896`). PR8 closes C5. |
| Progress decode or EOF | The bridge keeps reading after end-of-stream, and a frame after it is a violation. | Already fatal on `e7ac01e`. The seal marker must precede end-of-stream, which it does. |
| Batch splitting | Previously planned reassembly | Positives-first makes reassembly unnecessary, across scope sections too. |
| Clean close | `close_on_drop` gated on worker success | Seal marker sent by every worker that doesn't fail, successful or stuck on a residual; a failed worker doesn't seal (R34) |
| Version | Did not bump `PROTOCOL_VERSION` (still 3 on `ba1bd37`) | Step 2 bumps to v4, step 3 to v5 (above) |
| Known residual: in Shared mode an end-of-stream ack can overtake payload, so late data is detected only best effort (PR8 description; `docs/design/cluster.md` §5.5.3 on PR8's branch) | Documented, not fixed; needs a wire change | Late data exists only after a premature completion. Step 3's S2b removes premature completion, so PCP makes the residual unreachable. `DataLoss` stays as the assertion. |

**Is PR8 a prerequisite for step 3? No** (changed since rev 0286a9d).
- The reason it once was: decode-failure credit, a lasting `+n` from an ignored error envelope.
- On `e7ac01e`, progress decode errors are already fatal (PR7). Step 3's consumer-side contract makes data decode errors fatal too, whichever transport produced them.
- §5.9 shows a prototype of step 3's completion changes on PR7 alone passes the full suite.
- PR8 still matters for C5. It is independent of this plan; only the merge order needs agreeing, and whichever lands second rebases.

**Re-check once PR8 merges:**
1. `PROTOCOL_VERSION`, and PCP's bump number.
2. That `NetworkPull` no longer emits `Envelope::error` anywhere. Step 3 keeps its consumer-side contract anyway, for MockNetwork.
3. That PR8's `close_on_drop` gating and its handling of link loss after retirement match S2b. A retired worker's later link loss must stay benign, and its seal marker must never be confused with a clean close.
4. That PR8's spawn-abort guard and its barrier-gated async-source start don't interact with A4 seeding. Seeding happens before any worker runs, so they shouldn't.
5. Re-run the §5.9 cluster experiments on the merged commit: the single-slot test, the pinned early-finisher rounds and the C1 cluster repros.
6. That PR8's new protocol violations ("data after a channel's sentinel", "data for a worker that already finished") stay assertions that never fire under PCP. In step 3's runs, the pinned early-finisher rounds should show none.

### 5.6 API changes

There are no compatibility constraints. Nobody depends on instancy yet; downstream projects are updated in step 4 (§5.7).

- **Public closure API** (`map`, `unary`, `unary_notify`, `binary`, `unary_async`, `exchange`, `broadcast`, `iterate`, probes and outputs): unchanged in signature. One addition: `output.retain(t)` on the borrowed output handle. Behaviour changes:
  - late output (not justified by this activation's input or a held capability) is an error in every build;
  - `notify_at` and `retain` must be justified by input consumed in this activation or by a held capability. The input frontier alone no longer suffices (§5.1.3);
  - error envelopes are fatal on every transport;
  - data held in a closure's input queue holds that input's frontier;
  - a probe includes data queued on the probed stream;
  - every closure-based operator gets one final call when its input frontiers empty (R45), so stash-and-flush without `retain` fails with `Unjustified` instead of losing data silently;
  - on a cluster, a node finishes only once every node's input has closed and every record is consumed, as in timely; PR7's per-node early finish is gone on uniform clusters (R47);
  - a loop's feedback channel is unbounded, and `with_capacity` on the feedback pipe is ignored (R44, liveness rule L6);
  - under the `transport` feature, `iterate` requires `TInner: ExchangeData`, as `exchange` already requires of its types (R42, breaking).
- **Additions and removals approved during step 3** (R40): `InputHandle::frontier()`; `Retained<T>` (`Send`, not `Clone`); `EdgeInfo.delivery` and `Delivery { One, AllTargetHosts }`; `ProgressError` gains `Unjustified`, `UpstreamError`, `RecordAfterShutdown` and `Residual`, and loses `InvalidNotificationTime`; `ChannelSourceOperator` and `ChannelSinkOperator` leave the re-exports.
- **Public-but-internal types:** `SchedulableOperator`, `OperatorFactory`, `ChannelEndpoints`, `Push`, `Pull` and `DataflowExecutor` (`dataflow/mod.rs:34`, `:40`; `channels/mod.rs:26`), plus `InputHandle::{new, push_vec, mark_exhausted}` and `OutputHandle::drain` (`handles.rs:35-55`, `:196-202`). They become sealed or crate-private. That is a breaking change, which costs nothing today; there's no public way to register a custom factory anyway.
- **CHANGELOG** entries are needed for the behaviour changes and for removing `set_inflight_reporter`.

### 5.7 Migration plan

**Four steps, straight to the end state.** Nobody depends on instancy yet. So, unlike earlier revisions of this plan, no step has to keep users safe on the way: there is no stopgap completion rule, no shadow mode, no temporary build rejection and no interim loop guard. Each step still leaves `main` building, with every known failure written down in the harness. The audit repros are in `instancy/tests/audit_repros/` and run through `instancy/tests/audit_repros.rs`: a repro called `audit_X` here is the file `audit_repros/X.rs`. Appendix A maps them to steps, and the note at the top of Appendix B maps the old PR labels.

| Step | Content | Audit IDs closed | Exit criteria |
|---|---|---|---|
| **1: test harness** | Debug checker V1, V4, V5, V6 and V7 (§5.8). V7 measures physical occupancy independently, at **every** materialization point (`DataflowExecutor::materialize`, `materialize_stage_executor`, the network endpoint materializer) and in every store that exists today (`InputHandle`, `pending_output`, sink `pending_send`, async queues). Conformance matrix in `tests/progress_conformance.rs` with an `EXPECTED_FAILURES` table keyed by audit ID. Protocol property test against `reachability::Tracker`. | none; detection only | The matrix runs in CI on every path and transport. Every listed failure reproduces. Nothing else fails. |
| **2: one executor core** | §5.1.8. `DataflowExecutor` hosts any subset of operators; the staged wiring moves into it; fusion groups and stage tasks become scheduling groups inside it. One full-graph tracker per worker, with progress channels on every path, `SimpleRuntime` included. A4 seeding from the physical plan, *replacing* the initial-capability broadcast in `initialize()` (`subgraph.rs:810-850` on `ba1bd37`); delete `peers_heard_from` and `all_peers_synced`; the plan digest joins the cluster handshake, and `PROTOCOL_VERSION` goes to 4. Delete `StageExecutor`, `CombinedStageExecutor`, M-d, M-e, M-h, per-stage trackers, and the staged drain logic. Port and in-flight accounting is unchanged; S and SR inherit the uniform path's known bugs until step 3 (§5.1.8). | H14 (per-stage trackers, cross-stage pipeline edges, staged `drain_on_cancel`), H13 (startup race), M12 (staged drain), P-3, P-6, P-7 | `audit_netprogress_startup`, `audit_executor_staged` and `audit_builder_stages` pass. `audit_netprogress_stage_starvation` (C3, fixed by #290) still passes. In the matrix, every S and SR configuration gives the same result as the uniform path on the same shape. A cluster whose nodes disagree on the physical plan fails the handshake. No test that passed after step 1 fails, except S and SR tests recorded as known failures and tied to a uniform-path bug that step 3 fixes. |
| **3: port-counted progress, in one go** | Everything in §5.1, on the one core: the operator shell, `CountedInput`, `CountedOutput`, the ledger and typed endpoints, with the 13 operators, the channel sink and the source migrated (§5.1.2, §5.2); the tracker consuming ledgers with `produced` keyed by edge (§5.1.4); the commit protocol and the one decision module, with force-close deleted (§5.1.1); capabilities for async work, egress credit, acquisition with justification sets, `retain`, and late output as an error (§5.1.3); nested loop scopes (§5.1.7); S2a, scoped S2b with seal markers, and S2c (§5.1.5); exact-zero completion with proven residuals as errors (§2.4); probes as projections; positives-first fragments across scope sections, with the version bump to v5 (§5.1.6, §5.5); error envelopes fatal on every transport (C-16); drain-path peer sync. Delete M-b, M-c, M-g and the outer-time projection. | C1, C2, H1 (pipeline, `pending_output`, `InputHandle`, exchange buffers, async, external egress), H8 (mock), H10, H11, H12, M10 remainder, M12 (drain peer sync), P-2, P-9, #281, `unary_async` spin, batch fragmentation, "probes after loop", "push into finished local operator" | Every audit repro that Appendix A maps to step 3 passes, and `EXPECTED_FAILURES` is empty: the matrix is green on every path, transport, worker count and capacity. Regressions: the two scope-bridge regressions (§5.1.7); a decode failure plus an injected residual fails promptly; two exhausted workers with an injected residual both report it through seal markers; the shapes in §5.9's table (rows X1a–X1e) become permanent tests, all completing with exact output. `multi_worker_loop_body_probe_is_conservative`, PR2's backpressure tests, and step 2's two ignored staged-loop tests (`multi_worker_iterate::staged_iterate_cross_stage_feedback` and `staged_iterate_cross_stage_immediate_exit`, C2) are un-ignored, and M11 is re-tested. `probe_cluster::*_probes_are_done_after_join` is un-ignored, and every probe on `SimpleRuntime::spawn_multi` and on a cluster, including one inside a loop body, is done after a successful run. After step 2 they usually stay at a stale frontier (R15, R16); under S2b a retiring worker's probe writers finish with an empty frontier (§5.1.5). Stage inference places each loop's feedback operator in its loop's stage, so `input → exchange_to(k) → iterate` spawns on any worker count (R18). #291's early-finisher oracles are exact, with frontier-observing variants added (§5.5). Every progress-related ignored test is un-ignored, and the inherited lost-record CI flakes pass at least 300 runs each pinned to 2 cores (R31, R32; the step-3 brief lists both). If the design changed during implementation, the TLA+ models are updated and re-run (§5.8). |
| **4: update downstream projects** | Outside this repository: update them for the sealed types, `retain` and the new error behaviour. | — | Their test suites pass against step 3's instancy. |
| later, optional | Demand-driven sends; reachability-filtered broadcast; hierarchical stage summaries (design D as an optimization). | — | Only if the performance work after correctness (§6) requires it. |

**Inside step 3.** One pull request, reviewed as a series of commits, so the large mechanical diff is read apart from the semantic changes:
1. the shell, ports, ledger and typed endpoints, with every operator migrated (mechanical);
2. the tracker consuming ledgers, the commit protocol and the decision module, with force-close deleted;
3. capabilities: async, egress credit, acquisition and justification, `retain`, late output;
4. nested loop scopes;
5. shutdown, scoped worker success, seal markers, exact zero, positives-first across scope sections and the bump to v5;
6. deletions and regressions.

Intermediate commits may fail tests; only the merged result is gated.

**Why this order.**
- **Harness first.** Every later step is judged by it. It also writes down today's failures, so step 2 can't hide a new one among them.
- **One core before the redesign.** Port counting splits `+n` and `−n` across workers and stages, so every path needs one whole-graph view either way. Building the new accounting into three executors and then deleting two would be wasted work, and three places to get the commit protocol wrong. Merging first means step 3 is written once. The cost: until step 3, the staged path and `SimpleRuntime` inherit the uniform path's known accounting bugs (§5.1.8). With no users, that costs nothing.
- **The redesign in one go.** Its pieces need each other:
  - port counting without capabilities for async and egress lets frontiers pass uncounted work;
  - port counting without nested scopes needs a temporary loop guard edge, and a ban on reading frontiers inside loops;
  - the shutdown rule without nested scopes trusts wrong frontiers inside loops (O-9);
  - exact-zero completion without port counting hangs on today's unmatched `−n` counts (C2).

  Earlier revisions sequenced these pieces with stopgaps, so that every intermediate release stayed safe for users. There are none to protect.
- **Downstream projects last.** They are consumers, so they are updated once, against the final API.
- **PR8 (#292) is independent.** Only the merge order needs agreeing; whichever lands second rebases (§5.5.1).

**What the old interim argument still contributes.** Earlier revisions proved that exact-zero completion could ship before port counting without new hangs, provided three sources of lasting positive counts were blocked: M-c broadcast guards, H12 leaks, and decode-failure credit. In the end state all three are gone: M-c is deleted, edge keys remove H12, and error envelopes are fatal. Appendix B keeps that analysis as history.

### 5.8 Verification plan

**Debug invariant checker**, on in all `cfg(debug_assertions)` tests and behind a `progress-check` feature for release:

| Check | Statement | Cost |
|---|---|---|
| **V1 late data** | Every consumed batch at `Target(X, i)@t` satisfies `F_w(X, i) ≤ t`, so data never arrives behind the frontier. This is the observable form of S1. Inside loops, `F_w` is the nested scope's frontier (§5.1.7). | One antichain comparison per batch. |
| **V2 notification order** | Notification at `t` is never followed by data at `≤ t` (follows from V1 plus monotone frontiers). | Free. |
| **V3 justification** | Per activation, every output batch at `t` and every capability acquisition or downgrade at `t` must have some element of the activation's justification set `J` at or below `t` (§5.1.3). This is the intent of timely's `validate_progress`, with a stricter local rule. It is a hard error in every build. It cannot see work that is consumed and never emitted. | Per output batch and per acquisition. |
| **V4 end of stream vs frontier, and commit** | Exhausted input implies an empty frontier within bounded sweeps after all peers' final batches. Data at a shut-down operator panics. No `Done`, shutdown or end-of-stream is acted on with an undrained ledger, and every success passes through the one gate (§5.1.1). | Cheap. |
| **V5 residual** | A worker's scope (S2b(c)) that is non-zero at the final accounting boundary is a proven residual: negative means a missing `+n`, positive means a leak. Before the boundary, a non-zero scope only means *not ready*. In step 1's harness, V5 is a debug report on today's code; from step 3 it is an error in every build. Limitation: V5 cannot see a `+1` and a `−1` masking each other at the same pointstamp. V7 can. | At worker success. |
| **V6 wire** | Encoder fragments are positives-first, across scope sections too; each worker's seal marker follows its last committed fragment, and end-of-stream follows the seal. There are no per-sender sequence numbers: per-sender lanes are FIFO by construction (R27). | Per frame. |
| **V7 oracle** (in-process tests) | A test-only model of the true state that is **independent of the accounting under test**. True occupancy per `(location, t)` comes from the *contents* of every store: channel queues, wrapped at every materialization point; `CountedInput` buffers; `CountedOutput` retry queues; output sessions; egress; async pending, running and finished queues. Ledger calls are never used as input. Capabilities are counted from the capability objects themselves. Oracle reachability comes from the *data* graph, feedback edges included. At every propagation, assert S1 for each worker's view against that model. Cluster tests use a per-node oracle plus an end-state check. Exact output-multiset checks stay in every matrix test. | Test only. |

**Conformance matrix** (`tests/progress_conformance.rs`):
- **Shapes:** pipeline; exchange; exchange → exchange; fan-out of two exchanges from one output; broadcast; binary; concat; branch / fork / ok-err; unary_notify, reduce, count, distinct, delay; `notify_at` at a time justified only by the frontier (must be rejected); unary_async with a suspended task and with a task failure; iterate with and without exchange; stateless loop with a feedback-only branch and a cross-worker feedback path, observed by a probe and an aggregate downstream of the loop; a finite loop feeding a downstream `retain` that flushes when its frontier empties (must complete); nested iterate; probe after loop; multi-epoch input; external output with a full and paused channel and with a dropped receiver; input with `drain_on_cancel`; MockNetwork decode error; heterogeneous stages where an upstream-only worker finishes first while a downstream probe is live; two exhausted workers with an injected residual (both must report it through seal markers, not hang); a progress batch larger than `max_batch_size`.
- **Paths:** U-unfused, U-fused, U-stage-task, staged (S: the separate staged executor in step 1, staged configurations of the one core from step 2), SR.
- **Transports:** in-process, MockNetwork, cluster Dedicated, cluster Shared.
- **Workers:** 1, 2, and 3 heterogeneous.
- **Capacity:** 1 and default.
- **Assertions:** exact output multiset; per-epoch aggregates not split; probe monotone and never done early; no hang (timeout); no silent loss; V1 through V7 green.

**Protocol property test.** A pure model with workers, random graphs, per-pair FIFO channels and random delivery interleavings.
- Operators take random *justified* steps, in ITP's exchange-model sense, including repeated sends under a held capability.
- Workers batch several steps per commit, apply their own commits immediately, and split commits positives-first.
- Each view is the real `reachability::Tracker`; FORTE's `Safe` is checked after every step.
- Mutants must be caught: unjustified acquisitions, a dropped `+n`, a negatives-first split, and derived scope updates that are broadcast.

This tests the protocol and PCP's claimed refinement (§2.5), not just examples.

**Model checking (TLA+ and TLC).** Two specs are in place.

[`tla/PortCountedProgress.tla`](./tla/PortCountedProgress.tla) models:
- workers;
- records on edges, optionally in transit;
- capabilities, notify stashes, async tasks and sink egress;
- ledgers, commits and per-sender FIFO lanes, with two-fragment splits;
- views;
- S2a, seals, scoped S2b and proven residuals;
- today's idle force-close;
- loops flattened into the parent tracker, as today: separate data and tracker graphs, with and without the guard edge that an earlier plan proposed. This is no longer a planned state (§5.1.7). It is kept because the case without a guard reproduces H11.

[`tla/LoopScopes.tla`](./tla/LoopScopes.tla) models one nested scope in (time, iteration):
- two views per worker;
- the downward import and the internal-only upward export;
- enter and leave as port counts;
- commits split across both levels.

[`tla/NestedScopes.tla`](./tla/NestedScopes.tla) puts one loop on another's feedback path, three levels deep, and caches imports and exports between applies, recomputing them in one pass per apply as step 3 implements them. Its mutation exports the imported frontier.

Switches select PCP, today's rules, or PCP with one rule removed. Each scenario in [`tla/make-models.ps1`](./tla/make-models.ps1) declares its exact expected result, and [`tla/run.ps1`](./tla/run.ps1) checks it. Results and limits are in §5.9 and the [model README](./tla/README.md).

Next models, in order of risk:
1. ~~**The recursive-export regression.**~~ Done: `NestedScopes.tla` (decision log O1). Broadcasting the export is left to a debug assertion and the Rust protocol test (R38).
2. **Binary operators and `retain`.** Justification across inputs with different summaries.
3. **Scoped retirement through a back-edge,** with heterogeneous hosting of loop operators.

A machine-checked proof for every size (TLAPS, or Isabelle reusing the ITP 2021 development) stays optional.

**Deterministic simulation.**
- `shuttle` for in-process multi-worker executors and progress channels, which needs the pool behind a thin spawn trait.
- `loom` for wake and park primitives (M11).
- `turmoil` (Tokio network simulation) for clusters: partitions, latency and cross-connection reordering, through instancy's pluggable transport.

**Fault injection.** PR7's valve harness; capacity 1 everywhere; `max_idle_sweeps = 1`; yield injection in the pool; progress-frame drop and duplication (expected to fail loudly).

**Mutation testing.** `cargo-mutants` on `progress/` and the shell. Every surviving mutant is either a missing check or dead code.

### 5.9 Experimental verification so far

Experiments ran on a local branch, `exp-pcp-verify`, which is not pushed. It was first run at commit `b7556dc` on main `10a2325`. It was then rebased onto merged PR7 as commit `8a6ca5b` on main `e7ac01e`, and every experiment was re-run. In the rebase, `network.rs` takes main's version, which drops the old close-sentinel tracing: since #291, sentinels go through the outbox.

The branch adds a minimal prototype of the completion changes, named after the old PR5 step and switched on by `INSTANCY_EXP_PR5=1`, so a single binary runs both modes. The four-step plan no longer ships these changes on their own; they are a subset of step 3 (§5.7). The prototype has:
- an exact-zero completion predicate on global-view trackers (U, and single-worker);
- no idle force-close in acyclic dataflows on those paths;
- S and SR unchanged.

It does **not** include A4 seeding, positives-first splitting, fatal error envelopes or the build-time rejections. Shape tests live in `tests/exp_pcp_shapes.rs` on that branch, and four C1 repros were copied from the audit set. None of the experiment code is in the repository; the tables below describe each shape.

**Run conditions.**
- All runs were debug builds.
- The `10a2325` runs shared the machine with PR7's multi-node soak.
- The `e7ac01e` runs held the machine-wide lock while it was free, on a fairly quiet machine.
- The "2 cores" columns pin the test process to two logical CPUs (affinity `0x3`) to force congestion.
- Results are from targeted runs. Where timing matters they are indicative, not statistical.
- No benchmark has run; performance testing is deferred until instancy is correct (§6).

| # | Claim in this doc | Experiment | Main | PR5 prototype | Verdict |
|---|---|---|---|---|---|
| X1a | Today, H12 feeding a notify operator completes "successfully" with the aggregate silently lost. Under PR5 it hangs, so H12 must be rejected (rounds 6–7). | `x1a_h12_fanout_notify`: map → {X → Z → out1, Y → count → out2}, one record | `Ok([[7], []])`: **count lost, run reports Ok** (both bases) | **hang** (both bases) | ✔ verified |
| X1b | One identity map per branch avoids the H12 collision (round 8). | `x1b_h12_workaround_identity_maps` | `Ok([[7],[1]])` | `Ok([[7],[1]])` | ✔ verified |
| X1c | A broadcast after a loop is a lasting positive; it hangs today, with no coincidental cancellation (round 5). | `x1c_loop_then_broadcast` | hang | hang | ✔ verified |
| X1d | loop → exchange → broadcast completes today only because the `leave` C2 residual cancels the broadcast guard, and it hangs under exact zero. This is why M-c-after-cycle is rejected in PR5 (round 5). | `x1d_loop_leave_exchange_broadcast` | `Ok` (4 records) | **hang** | ✔ verified (both bases) |
| X1e | A fully wired loop with an exchange still completes under PR5 (control). | `x1e_loop_exchange_map_wired_control` | `Ok` | `Ok` | ✔ verified |
| X1f | Balancing H12 counts (a fix or the workaround) can expose H1: a count fires before parked records arrive (round 7, finding 1). | `x1f_workaround_exposes_h1_partial_counts`: 10 runs, capacity-1 edge | 0/10 partial | 0/10 partial | **not reproduced** on either base. Default scheduling fuses `Y` and `count`, closing the window, and the reviewer's sequence needs unfused activation. Reasoning only; the design keeps the conservative rejection. |
| C1 | PR5 removes the signed-sum premature completion (C1). | `audit_progress_total_counts`, `audit_netprogress_signed_sum`, `audit_multinode_premature`, `audit_progress_cluster_spurious_complete` (16 tests: in-process and TCP cluster) | **7/16 fail** on both bases. On `e7ac01e` the *cluster* failures are loud (`DataLoss`), though in one case node b returns `Ok` with a record missing while node a reports the loss. The in-process `spawn_multi` failures are still **silent**: join is `Ok` and worker 0's results are missing. | **0/16 fail** (both bases) | ✔ verified. PR7 made the cluster form loud; only PR5 removes the in-process form. |
| S | PR5 adds no new hang beyond the rejected shapes (§5.7). | Full `cargo test -p instancy --all-features --lib --tests` in PR5 mode | — | `10a2325`: 1872 passed, 28 ignored, 1 failed (row P7). **`e7ac01e`: 1974 passed, 0 failed, 28 ignored.** | ✔ verified for U and CL on top of PR7 |
| P7 | PR5 requires PR7's reliable close sentinels (H9). | `cluster_exchange_backpressure::exchange_with_single_slot_peer_queue_makes_progress` (Dedicated, capacity 1) | passes on both bases. On `10a2325`, sentinel tracing showed 5 close sentinels lost per run, hidden by force-close. | `10a2325`: **hangs, deterministically (2/2)**. **`e7ac01e`: passes 3/3** in 0.02 s. | ✔ verified both ways: PR7 is necessary, and sufficient for this test. |
| E | PR5 makes #291's early-finisher scenarios exact. They are acyclic, so completion depends only on exhaustion, and no frontier is observed (§5.5). | Rounds that delivered every record, on `e7ac01e`, unpinned and pinned to 2 cores (see table below) | Every non-exact round failed loudly (`DataLoss`); none was silent. | **Every round exact, in every configuration.** | ✔ verified (one run per configuration). Inference: with the completion changes alone, H1 cannot cause premature *completion* of an acyclic dataflow, but it still moves frontiers and notifications early. Step 3's port counting removes that. |
| F | Disabling acyclic force-close removes acyclic force-close failures, including the bug-tracker entry "peer force-close while another worker holds `pending_output` spins forever". | `10a2325`: the ignored tests `exchange_to_other_workers_with_one_slot_channels`, `round_robin_to_other_workers_with_one_slot_channels` and `small_capacity_exchange_then_complete`, 4 runs. `e7ac01e`: all 28 ignored tests, 2 runs per mode. | `10a2325`: **6/12 runs hang**. `e7ac01e`: `small_capacity_exchange_then_complete` failed 2/2, and `branch_into_two_exchanges` and `exchange_from_every_worker` failed 1/2 each. | `10a2325`: 0/12. `e7ac01e`: **none of these fail.** | ✔ verified (provisional) |
| B | A broadcast with a single-slot peer queue needs reliable sentinels, not PCP. | Ignored `broadcast_with_single_slot_peer_queue_makes_progress` | passes on `e7ac01e` | passes on `e7ac01e` | Fixed by PR7. It can be un-ignored independently of this design. |
| G | Worker success must wait for its scope (S2b), which is in step 3 but not in the prototype. | Ignored `probe_cluster::*_probes_are_done_after_join` | fail | fail (both bases) | Consistent with the design: the prototype alone doesn't fix it, and full step 3 must. |

**Row E detail** (`e7ac01e`, exact rounds / rounds):

| Scenario | Main, unpinned | PR5, unpinned | Main, 2 cores | PR5, 2 cores |
|---|---|---|---|---|
| Routing scenario 1 (`cluster_early_finisher_routing.rs`) | 12/12 | 12/12 | — | — |
| Routing scenario 2 | 24/24 | 24/24 | — | — |
| Routing scenario 3, congested Dedicated links | 9/9 | 9/9 | **4/9** | **9/9** |
| `large_records_from_early_finisher_never_vanish` (Dedicated and Shared × Input and Map, 10 each) | **31/40** (9, 6, 9, 7) | 40/40 | **29/40** | **40/40** |
| `many_small_frames_then_complete_shared` | 5/5 | 5/5 | **4/5** | **5/5** |
| `many_small_frames_then_complete_dedicated` | 5/5 | 5/5 | — | — |

**Reading these results under the four-step plan.**
- Rows X1a to X1d show why the old plan needed build-time rejections while exact zero ran ahead of port counting. Step 3 removes their causes (H12, the M-c guard and the `leave` C2 residual), so all four become step 3 regressions that must complete with exact output.
- Rows C1, S, P7, E and F show the completion changes are safe on top of PR7 even before port counting. Step 3 includes them.
- Row G needs scoped worker success (S2b), which is in step 3.

**Model checking (TLA+ and TLC).** The setup:
- **Bounds:** 2 workers (3 in two scenarios), timestamps 0..1 or 0, 1–2 records per input, and one lap in loop scenarios.
- **Graphs:** acyclic graphs, a loop flattened as today, and a nested loop scope.
- **Software:** TLC 2.19.

Every scenario's result matched its expectation exactly. Full table: [model README](./tla/README.md).

| Group | Scenarios | Result |
|---|---|---|
| PCP must pass | direct, two-epoch chain with splitting (13.75M states), pipeline, async + broadcast, fan-out of two exchanges, broadcast to a subset, heterogeneous stages, sink egress, three workers (11.7M and 30.3M states); liveness: every worker eventually succeeds, and a count is reported while the input stays open | all pass: no S1, S2c or S3 violation, no deadlock, liveness holds |
| Loops flattened as today, with the guard edge an earlier plan proposed (no longer planned), must pass | loop with an exchange inside, explicit leave and a count after it (4.3M states); liveness; progress while the input stays open | all pass: the count after the loop never fires while a record can still leave the loop |
| Nested scopes (step 3), must pass | two records, whole commits; one record, commits split positives-first across both levels; liveness; progress of both counts while the input stays open, including after a lap | all pass: S1 inside and after the loop |
| A loop on another loop's feedback path (O1), must pass | three levels with cached imports and exports; whole and split commits; capped outer iterations; two real outer laps; liveness; progress while the input stays open | all pass: S1 at every level, and the cached links equal a fresh recompute (`CacheExact`) |
| Recursive-export mutation (O1) | the inner loop exports its imported frontier | never drains: deadlock, a `DerivedZero` violation, and no progress while the input is open |
| Completion checks under idle-gated force-close, with records in transit | today's signed sum; timely's positive count; exact zero | **signed sum loses a record** (C1, even with complete counting); positive and exact zero pass |
| Mutations of PCP, or today's rules | opt-in counting (C2, H1); per-output key (H12); capabilities-first split; frontier-justified `notify_at` (P-9); async without capability; local-only seeding (H13); sink releasing credit early (C-15) | each produces a counterexample to S1, or completes with a pending send |
| Loop mutations | no guard edge (today's tracker); frontiers observed inside a flattened loop body; a count inside a flattened body; no scope import; no scope export; split by level, parent first or child first | each produces a counterexample. No guard edge reproduces "probe after a loop is done too early" (H11). |
| Accounting faults that should be loud | wrong initial or broadcast multiplicity; missing `+n` under PCP decisions | hang, never loss (detected as deadlock and by the liveness check); missing `+n` reported as a proven residual at the seal boundary |
| Sanity | completion with real traffic; early finisher retiring while a peer drains a tail; completion after a lap, in both loop models | all reachable |

GPT-6 Astra reviewed the acyclic model, then the loop models, for fidelity. Neither review found a counterexample to PCP.
- **The first review drove:**
  - sink egress;
  - fan-out;
  - records in transit, with idle-gated force-close;
  - liveness under fairness;
  - broadcast to a subset;
  - three workers;
  - sanity checks with real traffic;
  - exact expected results.
- **The second review drove:**
  - splitting across scope levels;
  - an explicit leave operator;
  - `ASSUME`s refusing cyclic tracker graphs;
  - the lap rule in the import;
  - progress checks while the input stays open.

**Still reasoning only, not yet tested by experiment or model:**
- broadcasting a scope's export (a debug assertion in step 3, R38);
- scoped retirement through a back-edge;
- binary operators, `retain` and the full justification set `J`;
- the refinement to ITP's exchange model for all sizes;
- the decode-failure credit (round 7, finding 2). This includes the claim that step 3's consumer-side contract makes PR8 unnecessary for it, because no decode fault was injected;
- step 2's claim that merging the executors changes no accounting;
- all hot-path and traffic costs.

**Now model-checked, within the bounds above:**
- port counting;
- S2a, seals, scoped S2b and S2c;
- the acquisition rule (P-9);
- positives-first splitting, within and across scope levels;
- A4 seeding;
- broadcast multiplicity;
- the egress credit rule;
- the loop guard edge an earlier plan proposed (no longer planned);
- the scope bridge's import and internal-only export.

Costs are measured after correctness, against timely (§6).

---

## 6. Performance (deferred until correct)

**No benchmark gates in steps 2–4** (user decision, 2026-10-22). Performance testing is deferred until instancy is fully correct, and will then be done against timely. Until then, no step is held back by a benchmark, and no step runs one as an acceptance check. Earlier revisions planned a throwaway prototype with an interleaved A/B gate before step 3; that plan is dropped.

**What the later performance work should measure** (kept from the dropped plan, as a starting point):
- **Hot path:** pipeline and exchange throughput (`dataflow_throughput`: `pipeline_throughput`, `exchange_throughput`, `worker_scaling`, `stateful_operator`, `input_frontier`; `comparative`: `MapChain20`, `SmallPipeline`, `MultiEpoch`, `ScanFilterAgg`), with every row asserting the delivered record count, so a lossy run can't look faster. `staged_exchange` (added in step 2) covers stages with different parallelism.
- **Progress traffic:** progress bytes per data byte for {2, 4, 8} workers × batch {64, 1024} × distinct timestamps per batch {1, 64}, after worker-wide compaction, including a congested slow consumer.
- **Worst cases for non-cancelling counts:** a 10-map chain × batch size {1, 16, 1024} × capacity {1, default}, at 1 and 4 workers.
- **Clusters:** a real `spawn_cluster` on loopback TCP, Dedicated and Shared, 2 nodes × {1, 4} workers, map chain + exchange + reduce, with exact output. `transport_throughput` sends synthetic frames without operators, so it can't measure this.
- **Frontier latency:** from a source's capability release to the downstream probe advancing.

**Planned mitigations, if the measurements call for them:** demand-driven sends (timely's `is_global` test, §5.1.6), reachability-filtered broadcast, and hierarchical stage summaries (design D). Worker-wide compaction is part of the design from the start (§5.1.1), not a mitigation.

---
## 7. Open questions, with recommended answers

The coordinator recommended an answer to each question (✓). Where the reviews changed an answer, the change is stated. All answers still need the user's approval.

1. **Seal internal types.** ✓ Yes: `SchedulableOperator`, `OperatorFactory`, `ChannelEndpoints`, `Push`, `Pull` and `DataflowExecutor`. *Amended:* also make `InputHandle::{new, push_vec, mark_exhausted}` and `OutputHandle::drain` crate-private (O-12), or the shell can be bypassed. *Updated for the four-step plan:* with no users, downstream code that names them is simply changed in step 4.
2. **Late output a hard error in release, no opt-out.** ✓ Yes. *Amended:* this also covers capability *acquisition* (`notify_at`, `retain`, async) under the rule in §5.1.3. That is a behaviour change for code that calls `notify_at` at a time justified only by the frontier.
3. **Add `retain`.** ✓ Yes. *Amended:* it is exposed as `output.retain(t)` on the borrowed output handle, so closure signatures don't change. Cross-task drop goes through a mailbox with a wake. The final flush call happens when the input frontier empties, not on channel exhaustion, so it cannot deadlock with a loop around it, whose end-of-stream comes only after shutdown.
4. **No temporary loop restrictions.** *Updated for the four-step plan:* earlier revisions rejected notify-family operators, `retain`, probes and frontier reads inside `iterate` until nested scopes landed, and added a loop guard edge to protect observers downstream of a loop. Nested scopes now ship together with port counting in step 3, so neither is needed. Identity cycles (a loop whose summary doesn't advance the iteration) stay rejected at build time, permanently (§5.1.7).
5. **Probes include queued data.** ✓ Yes, as read-only projections.
6. **Use exact-zero completion in the end state.** ✓ Yes. *Amended:* worker success uses exact zero over the worker's scope (S2b). Before the final accounting boundary (every worker's seal marker applied), a non-zero scope means "not ready", not an error. Dataflow completion is all workers succeeding, so no surviving observer is needed (§2.2).
7. **Demand-driven sends only if the later performance work (§6) requires them.** ✓ Yes. Worker-wide compaction, however, is mandatory from the start.
8. **Broadcast scope all-to-all.** ✓ Yes, with the supported scale stated from the traffic matrix (§6).
9. **SimpleRuntime gets progress channels.** ✓ Yes. *Updated for the four-step plan:* in step 2, with the executor merge.
10. **Positives-first here, and PR8 skips reassembly.** ✓ Done: PR8 (#292) shipped positives-first ordering, and step 1's V6 check confirms it. Step 3 extends it across scope sections.
11. **Build-time rejections during a migration interval.** *Withdrawn in the four-step plan.* Earlier revisions rejected two shapes (an output feeding two or more exchanges, and a broadcast reachable from a loop) while exact-zero completion ran ahead of port counting. There is no such interval now, and both shapes complete correctly after step 3.
12. **Order relative to PR8 (#292).** *Updated for the four-step plan:* PR8 is independent of all four steps.
    - PR8 was once a prerequisite only because of decode-failure credit, and step 3's consumer-side error contract covers that on every transport (§5.5.1).
    - Whichever lands second resolves the `network_progress.rs` encoder ordering and the protocol version.
    - If PR8 merges first, re-run the §5.9 cluster experiments on top of it before step 3 merges.
13. **Merge the executors before the redesign? (new)**
    - *Recommended:* yes, step 2 before step 3, so the new accounting is built once, in one core, instead of being built for the staged executor and then deleted.
    - The cost: between steps 2 and 3, the staged path and `SimpleRuntime` have the uniform path's known progress bugs instead of their own.
    - With no users, that costs nothing. The alternative, step 3 first, would mean building port counting and nested scopes into `StageExecutor` too.

---

## 8. References

- D. G. Murray, F. McSherry, R. Isaacs, M. Isard, P. Barham, M. Abadi. *Naiad: A Timely Dataflow System.* SOSP 2013, pp. 439–455. https://www.sigops.org/s/conferences/sosp/2013/papers/p439-murray.pdf (§2.3, §3.3).
- M. Abadi, F. McSherry, D. G. Murray, T. L. Rodeheffer. *Formal Analysis of a Distributed Algorithm for Tracking Progress.* FMOODS/FORTE 2013, LNCS 7892. https://www.microsoft.com/en-us/research/wp-content/uploads/2013/06/clock-verif2.pdf (the `IsUpright` condition, the send rule, and the `Safe` theorem).
- M. Brun, S. Decova, A. Lattuada, D. Traytel. *Verified Progress Tracking for Timely Dataflow.* ITP 2021, LIPIcs 193, 10:1–10:20. https://doi.org/10.4230/LIPIcs.ITP.2021.10. AFP entry *Progress_Tracking*: https://www.isa-afp.org/entries/Progress_Tracking.html.
- timely-dataflow book, chapter 5.2 (progress tracking): https://timelydataflow.github.io/timely-dataflow/chapter_5/chapter_5_2.html.
- timely-dataflow source at `3012f8f`: `progress/reachability.rs`, `progress/subgraph.rs`, `progress/operate.rs`, `progress/broadcast.rs`, `progress/stamp.rs`, `dataflow/channels/pushers/counter.rs`, `dataflow/channels/pullers/counter.rs`, `dataflow/operators/generic/builder_raw.rs`, `dataflow/operators/generic/builder_rc.rs`; `CHANGELOG.md` (0.24–0.31); PRs #721, #723, #813.
- F. Mattern. *Algorithms for distributed termination detection.* Distributed Computing 2:161–175, 1987. F. Mattern. *Global quiescence detection based on credit distribution and recovery.* IPL 30:195–200, 1989. S.-T. Huang, weight throwing, IPL 1989 (this citation was not verified against the primary source).
- P. Carbone, G. Fóra, S. Ewen, S. Haridi, K. Tzoumas. *Lightweight Asynchronous Snapshots for Distributed Dataflows.* arXiv:1506.08603, 2015.
- T. Akidau et al. *MillWheel: Fault-Tolerant Stream Processing at Internet Scale.* PVLDB 6(11), 2013.
- E. Begoli et al. *Watermarks in Stream Processing Systems.* PVLDB 14(12), 2021.
- Apache Flink: *Generating Watermarks* documentation; FLIP-147 *Support Checkpoints After Tasks Finished*.
- RisingWave architecture blog (barrier injection and alignment).

---

## Appendix A: audit repro → step

| Step | Repro files (`instancy/tests/audit_repros/`; `audit_X` is `X.rs`) |
|---|---|
| 1: test harness | all of the files below, run under the matrix with expected-failure entries |
| 2: one executor core | `audit_netprogress_startup` (seeding), `audit_executor_staged`, `audit_builder_stages`; `audit_netprogress_stage_starvation` (C3, fixed by #290) must keep passing |
| 3: port-counted progress | completion: `audit_progress_total_counts`, `audit_netprogress_signed_sum`, `audit_multinode_premature`, `audit_progress_cluster_spurious_complete`. Accounting and buffers: `audit_netprogress_inflight`, `audit_progress_unwired_exchange`, `audit_progress_cluster_unwired`, `audit_progress_fanout_exchange`, `audit_progress_fanout_probe`, `audit_executor_pipeline_inflight`, `audit_netprogress_pending`, `audit_multinode_late`, `audit_netdata_frontier`, `audit_builder_broadcast`, `audit_operators_broadcast`, `audit_netdata_broadcast` (with PR7), `audit_operators_core` (async). Loops: `audit_operators_iterate`, `audit_builder_iterate`, `audit_progress_loop_frontier`, `audit_progress_notify_backpressure`, `audit_netprogress_staged_loop` |
| Re-test after step 3 | M11: `multi_worker_iterate_collatz`, `shared_multiple_dataflows_same_connections` |

## Appendix B: adversarial review log

**Old labels.** The eight review rounds below were run against an earlier, seven-phase plan, and use its labels. They map to the four-step plan (§5.7) as follows:

| Old label | Content | Now |
|---|---|---|
| PR4a | harness | step 1 |
| PR12a | one progress driver per worker, in shadow mode | step 2, without shadow mode |
| PR12b | one executor core | step 2 |
| PR5 | exact zero, no acyclic force-close, decision module, seeding, positives-first, fatal error envelopes, build-time rejections | step 3, except seeding (step 2); the rejections are dropped |
| PR4b | operator shell | step 3 |
| PR4c | switch to port counting, async and egress capabilities, interim loop guard edge | step 3; the guard edge is dropped |
| PR6a | nested loop scopes | step 3 |
| PR6b | shutdown rule | step 3 |
| — | update downstream projects | step 4 |

Findings about the migration interval between old PR5 and PR4c (rounds 4 to 7) no longer apply to any planned state. Their end-state conclusions still hold.

Each review was asked to find a sequence of events under this design that loses data, completes or notifies early, duplicates or hangs, and an accounting site a contributor could still forget. Every finding was checked against the source before it was accepted.

### Round 1: rubber-duck reviewer (Claude family)

It found ten issues, all verified and addressed.

| # | Severity | Finding | Resolution in this revision |
|---|---|---|---|
| 1 | critical | V3 cannot see a closure that consumes data, stashes it and emits nothing. The data is dropped at shutdown and every check passes. | Claim corrected. Stash-and-drop is indistinguishable from a filter (timely has the same limit). Added the `ctx.retain(t)` API, made late output an error in every build, and listed it as residual risk 1 (§5.1.3, §5.3; question 3). |
| 2 | critical | "All hosted operators shut down" is not a safe worker-completion rule under heterogeneous instancing. | S2 split into S2a operator shutdown, S2b worker termination (scoped to reachable locations, flushed before end-of-stream, then retired per PR7) and S2c dataflow completion. V5 scoped to match (§2.2, §5.8). |
| 3 | critical | The switch to port accounting came before paths S and SR had a global view, so `+n` and `−n` would never meet. Phase 0 claimed a single materialization point. | Migration reordered: PR4b (neutral) → PR12 (one core, global view everywhere) → PR4c (switch). V7 now wraps every materialization point (§5.7, §5.8). |
| 4 | critical | PR5's positive predicate alone could complete early, because a missing `+1` and a real `+1` net to zero at one pointstamp, and V5 can't see it. | PR5 now uses an **exact-zero** predicate: strictly stricter than today's, so no hang becomes a loss. Masking is documented as fixed only by PR4c, and detected only by V7's "true count never negative" assertion (§2.4, §5.7). |
| 5 | high | External egress (`ChannelSinkOperator::pending_send`) and the source's admission gate were not covered. | Added `CountedEgress` (gates shutdown) and the gate invariant (C-13, C-15, §5.1.1); added matrix shapes. |
| 6 | high | The ledger commit boundary was contradictory, and end-of-stream could overtake uncommitted or demand-held progress. | Added the `ActivationTxn` commit protocol, the end-of-stream ordering requirement, the failure rule, and V4/V6 checks (§5.1.1, §5.5). |
| 7 | high | The interim loop restriction was too narrow: feedback data is invisible even in stateless loops. | S2a is never used inside loops before the nested tracker. Loop counts are projected so exact-zero completion can't pass them. PR6a (nested scopes) now lands before PR6b (shutdown rule) (§5.1.5, §5.1.7, §5.7). |
| 8 | high | Virtual probe targets would never be decremented. | Probes are now read-only projections over the real targets' frontiers (§5.1.4; question 5). |
| 9 | medium | The handshake doesn't cover a heterogeneous physical plan, and clusters currently reject heterogeneous stages (`runtime.rs:2883`). | The physical-plan digest is required in the handshake before PR12 enables heterogeneous clusters (§5.1.4, §5.1.8). |
| 10 | low | timely warns about, but does not reject, non-incrementing cycles. | Corrected with a citation to `reachability.rs:215-218`. instancy should reject them (§5.1.7). |

Some round-1 resolutions were revised in round 2: egress now keeps input credit instead of having a shutdown gate; PR12 is split into PR12a and PR12b; and V7 is made independent of the accounting it checks.

### Round 2: rubber-duck reviewer, GPT-6 Astra (`gpt-6-astra`, at the user's request)

It found 17 issues: 4 critical, 10 high, 2 medium and 1 low. All were verified against the source or the cited sources, accepted, and addressed. None was rejected.

| # | Severity | Finding | Resolution |
|---|---|---|---|
| 1 | critical | `notify_at` accepts any time the observed frontier permits (`handles.rs:438-452`). An unjustified `+1` lets a peer notify early, even though all counts balance. | Release-enforced acquisition rule for every capability source: an acquisition must be justified by input consumed in the same activation or by a held capability (§5.1.3, P-9). It ships in PR4c. |
| 2 | critical | PR4c counted async consumption before PR6a gave async work capabilities. Downstream frontiers pass suspended tasks. | Async capabilities now ship *with* PR4c and cover pending, running and finished-but-uncollected tasks, cancellation and failure (§5.1.3, §5.7). |
| 3 | critical | `CountedEgress` gated only S2a, but the interim force-close (`executor.rs:1807-1816`) doesn't check it, so a paused external receiver loses an event. | Egress now keeps its input credit, recording `consumed` only when the external channel accepts. Exact-zero blocks every interim path (§5.1.1). |
| 4 | critical | The interim loop policy still let observers *downstream* of a loop notify early (a feedback-only branch has no reachability path to the exit). "Derive all edges" would re-create the identity cycle. | Accounting edges and reachability edges are now separate. Reachability drops each back-edge and adds a loop guard edge `Source(feedback) → Target(leave)` (§5.1.4, §5.1.7). |
| 5 | high | Exact-zero is stricter, but it isn't the only way to report success: the all-done shortcut (`executor.rs:1753-1756`) and staged convergence bypass it. S and SR aren't excluded explicitly. | One success gate for every success path. S and SR stay on the legacy predicate through an explicit `view_scope` until PR12a. The residual at success is debug-only before PR4c and an error after (§5.1.1, §5.1.4). |
| 6 | high | Early retirement contradicts "every view is exact-zero at S2c", and a retiring worker's probe would freeze. | S2b is scoped to hosted locations, probes included. S2c requires exact zero only in non-retired views. Workers with live probes don't retire (§5.1.5). |
| 7 | high | Positives-first was scheduled for PR4c, but today's capability-first order already has the fragmentation hole. | Positives-first moves to PR5, or PR8 if it lands first (§5.1.6; question 10). |
| 8 | high | The proof was attributed to FORTE's "upright" condition, which ITP shows timely violates (repeated sends under a held capability). The prefix argument was wrong. | A2 is restated as ITP's *justified* condition. PCP is argued to refine ITP's exchange model; that refinement is marked unmechanized. Splitting now uses a monotonicity argument, and self-application an admissible-schedule argument (§2.3, §2.5). |
| 9 | high (speculative) | The nested scope design was underspecified: broadcasting derived updates would double-count exits, and the frame format changes. | Scope-bridge algorithm specified after timely's `Subgraph`: local versus derived updates, boundary counts, cross-scope atomicity, and the format version bump (§5.1.7, §5.5). |
| 10 | high | Deleting the staged executor isn't needed before PR4c, and removing its barriers is unsafe while accounting is incomplete. | PR12 is split. PR12a (one progress driver per worker, *added* as an AND, nothing deleted) lands before PR4c; PR12b (delete the staged machinery) lands last. Feasibility notes added (§5.1.8). |
| 11 | high (speculative) | The public `InputHandle::push_vec`, `new` and `mark_exhausted` and `OutputHandle::drain` would let code forge a consumption or discard counted output. | Port views are borrowed and can't inject or drain; those methods become crate-private (O-12, §5.1.1, §5.6). |
| 12 | high | MockNetwork error envelopes, and control payloads generally, are ignored downstream, so a counted batch is never consumed (C-16). | The shell's envelope contract makes errors fatal on every transport and watermarks advisory (§5.1.1). |
| 13 | medium | `Retained` dropped on another task had no wake. `ctx` isn't signature-neutral. | Release goes through the owner's mailbox and wakes the executor. The API moves to `output.retain(t)`, and a final flush call is specified (§5.1.3). |
| 14 | medium | V7 was fed by the accounting it checks, and it missed shell stores. | V7 now measures the contents of every store independently and uses data-graph reachability (§5.8). |
| 15 | high | Benchmarks: `transport_throughput` bypasses dataflow; `ScanFilterAgg` uses an uncovered notify operator; exchange rows don't verify delivered counts. | The prototype covers notify and sink egress and measures closed shapes only. A real `spawn_cluster` benchmark, per-scenario gates, cardinality checks and frontier-latency metrics are added (§6). |
| 16 | medium | The cost estimate omitted worker-wide compaction, which is required for cancellation, and the 1% traffic gate isn't scale-independent (about 2.4% at 3 peers). | Compaction is mandatory in the commit, the costs are listed, and the traffic gate is now a matrix with a stated supported scale (§4.4, §6). |
| 17 | low | Chronology: `Config::default()` has used Demand mode since 0.12, and the plain `Counter::next()` drops `ConsumedGuard` immediately. `validate_progress` uses implications. | §2.7 corrected; V3 is described as stricter by design. |

### Unresolved disagreements

None in round 2. Two items are judgement calls for the user rather than open disputes:
- the "consume and never emit" limitation (residual risk 1), which every reviewer agreed no design can close;
- the expected cost of all-to-all eager progress at large worker counts, which only the cluster benchmark can settle.

### Round 3: second pass by GPT-6 Astra (`gpt-6-astra`, a fresh instance with the same brief plus the round-2 log)

**Status of the 17 round-2 findings:** 11 resolved and 6 partially resolved (1, 4, 5, 6, 9, 10). Each partial one is completed by the new findings below. The pass also confirmed that:
- the positives-first monotonicity argument extends to receivers that interleave several senders across separate `propagate()` calls;
- loop guards compose across nested loops;
- `delay`, binary and source-epoch patterns are justified under the acquisition rule;
- deferred egress credit is sound;
- runtime success is interpreted in one place (`runtime.rs:4310-4321`).

It found 7 new issues, 2 critical and 5 high. All were verified and accepted; none was rejected.

| # | Severity | Finding | Resolution |
|---|---|---|---|
| B1 | critical | The scope bridge never imported the parent input frontier into the child. An in-loop aggregate could fire while parent input at that epoch was still open. | Downward import added, like timely's `accept_frontier` (`subgraph.rs:394`): it is local, derived, seeded at initialization, and applied before child frontiers are computed. Regression test added (§5.1.7). |
| B2 | critical | PR12a changed *frontiers*, not just completion. On S and SR, global cancellation could advance a frontier past uncounted async data before PR4c, causing an early notification. | PR12a runs global frontiers in **shadow mode** on S and SR. Exit criteria require that no delivered frontier or notification trace advances earlier. S and SR switch frontiers in PR4c (§5.1.8, §5.7). |
| B3 | high | The acquisition rule lost same-activation justification: `next_notification()` drops the capability before the handler emits (`handles.rs:556-565`), and re-arming fails. | An activation-scoped justification set `J` keeps capabilities held during the activation until commit, plus consumed inputs through their own summaries (§5.1.3, V3). |
| B4 | high | Exporting the ordinary child leave frontier would turn imported parent support into self-sustaining scope capabilities on an outer feedback path. | Two separate computations: child frontiers include the import, while the upward export projects internal child work only, excluding node 0 as timely does (`reachability.rs:554-577`). Regression test added (§5.1.7). |
| B5 | high | The cut reachability graph made V1 and S1 false for permitted operators inside loop bodies, such as concat after feedback, or exchange. | The interim invariant is narrowed explicitly: S1 and V1 are claimed only outside loop bodies, and nothing inside a body may observe frontiers (§5.1.7, V1). |
| B6 | high | "All done with a non-zero view" was treated as a residual, but a peer's progress batch can still be in transit after its data. | *Not ready* is separated from a *proven residual*, which is declared only after the final accounting boundary: every peer's end-of-stream, or in-process sender closure (§2.2, §5.1.4, V5). |
| B7 | high | The global success gate contradicted scoped retirement, and nothing guaranteed a surviving final observer. | One decision module with three decision types. Worker success is scoped S2b with exact zero over everything that can reach the worker's hosted locations. Dataflow completion is all workers succeeding: the scoped checks jointly cover every hosted location, so no observer is needed (§2.2 argument, §5.1.1, §5.1.5). |

### Round 4: third pass by GPT-6 Astra (`gpt-6-astra`, a fresh instance)

**Status of round 3:** B1, B2, B3, B4, B5 and B7 resolved; B6 partially resolved, and completed by finding 3 below. The pass also checked the following attacks, and none broke the design:
- the S2c no-surviving-observer argument, under concurrent retirement, transient negatives, exchange targets hosted by several workers, PR7 discarding progress to retired peers, and probes. The argument holds while A1–A5 hold, and §2.4 already says it cannot detect every violation of A1 or A5;
- stale views at retirement, provided commit-before-end-of-stream is enforced;
- the justification set `J`, including drops and downgrades.

It found 3 issues, 2 critical and 1 high. All were verified and accepted; none was rejected.

| # | Severity | Finding | Resolution |
|---|---|---|---|
| 1 | critical | PR12a's added exact-zero conjunct would permanently block exhaustion success and staged convergence on S and SR, because legacy C2 `−n` residuals never cancel before PR4c. | PR12a now runs **fully in shadow mode**: legacy frontiers and success decisions are unchanged. A regression requires S/SR completion with a known C2 residual (§5.1.8, §5.7). |
| 2 | critical | Interim loop force-close waits for the whole view to reach zero. A downstream `retain` that flushes on input *exhaustion* waits for the loop to close, which is a deadlock with no accounting error. | The final flush is triggered when the input *frontier* empties, which happens before end-of-stream. The new rule: no capability or credit may be released only on channel exhaustion. Regression test added (§5.1.3, §5.8). |
| 3 | high | The final accounting boundary did not exist in-process (`progress_channel.rs` has no terminal state), and PR7's end-of-stream depends on success, so residuals could stay "not ready" forever. | Added an in-order **seal marker**, independent of success, which counts only once the tracker applies it. It is a reserved frame in clusters and a marker entry in-process (§2.2, §5.1.4, §5.5). |
| note | — | The builder flattens loop bodies into the parent tracker; PR6a needs a real progress hierarchy. | Feasibility note added (§5.1.7). |

It also confirmed one inherent trade-off, now documented: under PR5, H12 inside a loop changes from "force-closes by signed-sum luck" to "hangs until PR4c". No sound predicate can avoid this (§5.7). *Superseded in round 5: the shape is now rejected at build time instead.*

### Round 5: fourth pass by GPT-6 Astra (`gpt-6-astra`, a fresh instance)

**Status of round 4:** all three fixes resolved, namely full shadow mode for PR12a, the frontier-triggered flush and the seal marker. It again found no counterexample to the end-state S2b/S2c argument or to sealing. It confirmed that A4 seeding must *replace* the initial-capability broadcast in `initialize()`, not add to it; the PR5 row now says so.

It found one new issue, which is critical.

| # | Severity | Finding | Resolution |
|---|---|---|---|
| 1 | critical | A PR5 → PR4c hang without H12. In `input → iterate → leave → exchange → broadcast` on two workers, `leave` doesn't wire its exchange producer, which leaves `−2`, and the broadcast guards leave `+2`. Today the signed sum cancels them and the loop force-closes. Under exact zero the loop can't close, so the guard, which is released only on exhaustion, never is. | **Generalized and closed.** Before PR4c, a cancellation-dependent completion needs a *lasting* positive, and only two sources exist: M-c guards (exhaustion-only) and H12 leaks. PR5 now (1) disables force-close in acyclic dataflows on U and CL, which finish by exhaustion now that PR7 makes close sentinels reliable, and (2) rejects both sources at build time in cyclic dataflows until PR4c removes them. No sound predicate could complete those shapes, and without the coincidence they hang today as well (§5.1.1, §5.7; question 11). *Revised in round 6.* |

### Round 6: fifth pass by GPT-6 Astra (`gpt-6-astra`, a fresh instance)

**Status of round 5:** partially resolved. The cyclic broadcast-guard case is covered, and the A4 replacement is confirmed. The pass checked source and input lifetimes, `InputSender` clones, the dropped-output discard, finite async work, the drain admission gate and PR7 sentinels, and found no separate counterexample in any of them.

It found one new issue, which is critical.

| # | Severity | Finding | Resolution |
|---|---|---|---|
| 1 | critical | "Acyclic dataflows always finish by exhaustion" is false before PR4c. In `input → map → {exchange X → exchange Z → sink, exchange Y → reduce R}`, the H12 leak gives `Target(Y)` `+1` forever, so R's notification never fires and R never reports `Done` (`wired_operators.rs:1527-1594`). Today the counts sum to zero and force-close completes, unsafely. With acyclic force-close disabled and H12 rejected only in cycles, it hangs. | *First resolution, withdrawn in round 7:* fix H12 keying in PR5. *Final resolution:* H12 fan-outs are rejected in **every** dataflow until PR4c (§5.7; question 11). |

### Round 7: sixth pass by GPT-6 Astra (`gpt-6-astra`, a fresh instance)

**Status of round 6:** partially resolved. The pass confirmed that the H12 keying fix is feasible, since materialization already knows each pusher's target (`executor.rs:680-777`). It confirmed that a negative count can't hold a frontier (`mutable_antichain.rs:175-193`). It found no healthy-path leak in partial-push retraction, nested projections, probes or `source_async`. It also caught a PR5 row that did not match the rationale: a PowerShell edit had been overwritten. That is now fixed, and every later change was made with one editing tool.

It found two new issues, both critical. Both were verified and accepted; none was rejected.

| # | Severity | Finding | Resolution |
|---|---|---|---|
| 1 | critical | Fixing H12 keying in PR5 exposes H1. In `input → map → {X → sink, Y → count R → sink}`, today the leak plus R's capability makes the signed sum `+1`, so the run hangs. After the fix, X and Y balance, Y's backpressured `pending_output` is uncounted, and R notifies early with a partial count. That is a hang-to-wrong-output transition. | The H12 keying fix is **withdrawn from PR5**. H12 fan-outs are rejected at build time in every dataflow until PR4c, whose port accounting also covers `pending_output`. The workaround is one identity `map` per branch, which yields an ordinary exchange shape with today's known exposure and nothing new (§5.7; question 11). |
| 2 | critical | The inventory of lasting positives missed decode-failure credit. At PR7's head, a payload decode failure becomes an ignored error envelope, so its `+n` never retires. Combined with a C2 `−2`, today's signed sum hides it; with PR5's acyclic force-close disabled, R hangs. MockNetwork has the same hole. | **Error envelopes become fatal in PR5**, on every transport, moved from PR4c. **PR8 is a prerequisite for PR5 on CL.** A regression of a decode failure plus a C2 residual must fail promptly (§5.7 PR5 row and rationale). *Revised after #291 merged: PR8 is no longer a hard prerequisite, because progress decode is already fatal and PR5's consumer-side contract covers data decode errors on every transport (§5.5.1).* |

### Round 8: seventh pass by GPT-6 Astra (`gpt-6-astra`, a fresh instance)

**Status of round 7:** all three fixes verified.
- **The identity-map workaround avoids the H12 reporter collision** (`subgraph.rs:350-366`, `dataflow_builder.rs:4660-4742`).
- **H12 can be detected statically.** Group registered in-flight bindings by source `(op, output)`, before stage filtering. This covers `exchange_to`, `gather`, `rebalance` and nested loops. It must not rely on `ChannelKind::Exchange`, because broadcast carries that kind but registers no binding.
- **Fatal error envelopes create no new transition.** Errors raised during drain still fail the worker, and PR8 records corruption durably.

**No fourth source of lasting positives.** Checked and ruled out:
- `take` and `take_while`, which keep draining their input;
- an exchange target that finishes;
- `branch` with an unused side;
- a dropped `OutputReceiver`, after which the sink consumes and discards;
- an exchange target that errors;
- a `source_async` producer error;
- partial exchange delivery.

**New findings: none.** **Verdict: CRITICAL ISSUES REMAINING: NO.** The pass also re-checked shadow-mode PR12a, PR4c's coverage, the interim loop guards, the scope bridge and scoped retirement.
