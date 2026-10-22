# Progress accounting: decision log

The design authority (the design session) keeps this log. It is short on purpose: one line per decision, with what, why, who and where. **Who:** *user* means the user decided; *DA* means the design authority decided within its remit; *coord* means the coordinator. "Ref" points into [the design doc](./progress-accounting-design.md) (`§…`, `Q…` = its §7 questions) or into the step briefs, which the coordinator keeps. The TLA+ models are in [`tla/`](./tla/README.md).

Last updated: 2026-10-22, against `main` @ `ba1bd37`.

## Ground rules

| # | Decision | Why | Who | Ref |
|---|---|---|---|---|
| G1 | Don't regress toward timely's execution model. Keep the async shared pool, Tokio I/O, per-stage and heterogeneous parallelism, fusion, the closure API without user-visible capabilities, pluggable transport and dynamic membership. Borrowing invariants and proof techniques is fine. | timely's 1-OS-thread-per-worker, uniform model doesn't fit instancy's scaling goals | user | §5.4 |
| G2 | Correctness must hold by construction: a contributor must not be able to forget accounting. Enforce it with types, required constructor parameters, and tests or lints that fail CI. | Opt-in accounting caused C2, H1 and H12 | user | §5.2 |
| G3 | Roles from step 2 on: the DA owns the design doc and models, writes the step briefs, answers design questions and reviews PRs for design fidelity. The coordinator owns sequencing, CI, benchmarks, merges, the bug tracker and talking to the user. | Durable division of work | user + coord | — |
| G4 | Escalate to the coordinator anything that changes scope, the plan or the public API. Heavy commands (TLC, experiments) only when no implementing session is active, or with the coordinator's go-ahead. | Shared machine; the user decides scope | user + coord | — |
| G5 | Steps are named "Step N: name", not PR numbers. | The old PR labels were confusing | user | App. B (label table) |
| G6 | Benchmark gate: per row, not flagged beyond max(5%, the reference's noise) after a 5-round recheck of flagged groups; instancy geomean ≤ ×1.02 with timely controls within ±3%; `spawn_latency` ≤ ×1.10; `staged_exchange` ≤ ×1.10. The implementer runs it under the lock, A/B-ing `main+bench` against its head. Supersedes the flat ×1.03 per-row limit. | The reference's round-to-round spread has a median of about ×1.05–1.09 | coord | §6; step-2 brief §7 |
| G7 | The design doc, TLA+ models and this log are on `main` through a docs-only PR. The tutorial stays outside the repository. | Implementers need the design in the repository; the tutorial would duplicate it and drift | coord; user (tutorial) | — |
| G8 | Trim the TLA+ folder: keep the two specs, the `MC_*` wrappers, `make-models.ps1`, `run.ps1` and the README. The 47 generated `models/*.cfg` are not committed; `run.ps1` regenerates them, byte-identical, on every run. | The models back the doc's verification claims and step 3's re-checks; the generated configs are pure output | user | `tla/README.md` |

## Design

| # | Decision | Why | Who | Ref |
|---|---|---|---|---|
| D1 | Recommended design: **C, port-counted progress (PCP)**. Count at operator ports, with one operator shell, rather than design A (accounting inside the channel layer). | One shell instead of more than 10 `Push`/`Pull` implementations and 19 factories. Counting in channels leaves `InputHandle` and `pending_output` invisible. | DA; the user approved the plan built on it | §4, §5 |
| D2 | Completion = exact zero per pointstamp over the worker's scope. Frontiers and shutdown use positive counts. Signed sums are never used. | Fixes C1's cancellation; a residual leak hangs visibly instead of passing | DA | §2.4 |
| D3 | Every capability acquisition must be justified by input consumed in the same activation or by a capability already held. Checked in every build (V3); late output is a hard error in release, with no opt-out. | Frontier-justified `notify_at` is unsafe (P-9) | DA | §5.1.3; §7 Q2 |
| D4 | `output.retain(t)` on the borrowed output handle. The final flush fires when the input frontier empties, not on channel exhaustion. No user-visible capabilities. | Makes keeping data correct and easy; avoids a deadlock with loops | DA | §5.1.3; Q3 |
| D5 | `unary_async` holds a capability from consuming a batch until the shell hands its result on. Sink egress keeps its input credit until the external channel accepts. | Work held outside an edge must stay visible | DA | §5.1.3, §3 |
| D6 | Probes are read-only projections that include queued data; nothing is counted at a probe. | No leak site; a probe after an exchange stays correct | DA | §5.1.4; Q5 |
| D7 | Loops get a nested scope tracker: the parent frontier is imported downward, only internal work is exported upward, and identity cycles are rejected at build time. | timely's subgraph rule; a flattened loop either never advances or passes work (H11) | DA | §5.1.7 |
| D8 | Shutdown S2a per operator, scoped worker success S2b with seal markers, dataflow completion S2c = every worker succeeds. Idle force-close is deleted. | Keeps PR7's early-finisher behaviour without a surviving observer | DA | §2.2, §5.1.5 |
| D9 | Split progress batches go positives first, across scope sections too. **Shipped by PR8 (#292)** for flat batches; step 3 extends it to sections. | Monotonicity without reassembly (§2.5) | DA | §5.1.6; Q10 |
| D10 | A4: seed initial capabilities from the physical plan, times the number of hosting workers, and broadcast only releases. | Fixes H13; undercounting is unsafe and overcounting hangs (TLA `hetero_*`) | DA | §5.1.4, §5.1.8 |
| D11 | Worker-wide compaction is mandatory from the start. Demand-driven sends only if the A/B requires them. Broadcast stays all-to-all. | Progress traffic is the main cost risk | DA | §6; Q7, Q8 |
| D12 | Seal the internal types (`SchedulableOperator`, `OperatorFactory`, `ChannelEndpoints`, `Push`, `Pull`, `DataflowExecutor`) and make the O-12 handle methods crate-private. | Otherwise the shell can be bypassed | DA | §5.6; Q1 |
| D13 | Error envelopes are fatal on every transport (a contract on the consumer side). | Decode-failure credit was a lasting `+n` | DA | §5.5.1; Q12 |
| D14 | "Port" is a logical (operator id, port index) pair: no network ports, sockets or threads. | Clarification asked by the user | DA (confirmed to user) | — |

## Plan

| # | Decision | Why | Who | Ref |
|---|---|---|---|---|
| P1 | Nobody depends on instancy yet, so go straight to the end state with no stopgaps. Dropped: the PR5 stopgap and its build-time rejections, the never-loosen rule, the interim loop guard edge and the in-loop bans, PR12a's shadow mode, the compatibility questions. | No users to protect between steps | user | Changes item 11; §5.7 |
| P2 | Four steps: (1) test harness, (2) one executor core, (3) PCP in one PR, (4) update downstream projects (outside this repository). | Simplest correct path | user | §5.7 |
| P3 | Step 2 (executor merge) comes **before** step 3. | Build the new accounting once, in one core; S and SR inherit uniform bugs briefly, at no cost | DA (flagged to the user; the coordinator scheduled it) | §5.7 rationale; Q13 |
| P4 | PR8 (#292) is independent of the four steps. (Merged as `9491e66`.) | Its only coupling was decode credit, now covered by D13 | DA | Q12 |
| P5 | Isabelle/TLAPS deferred. Bounded TLA+ models plus GPT-6 Astra fidelity reviews are the verification so far. | Cost now vs. value; maybe a narrow lemma later | user | §5.8 |
| P6 | Step 1 merged as #293 (`ba1bd37`). The checker defaults to `report` mode; a golden file pins today's failures. | — | coord | #293 |

## Step-2 refinements (DA, 2026-10-22, while writing the step-2 brief)

| # | Decision | Why | Who | Ref |
|---|---|---|---|---|
| R1 | Two protocol bumps: step 2 → v4 (A4 seeding plus the plan digest in the fingerprint), step 3 → v5 (seal marker plus scope ids). Supersedes "one bump in step 3". | A4 changes what a progress stream means; a v3 peer would count initial capabilities twice or never | DA | §5.5; Changes item 12 |
| R2 | M-d (per-loop `AtomicI64` counters) is deleted in step 2, not step 3. | Its only reader, `CombinedStageExecutor`, goes in step 2 | DA | §5.1.8, §5.7 |
| R3 | One `PhysicalPlan` function decides hosting; nothing else may. | One place for materialization, seeding, probes, digest and checker | DA | §5.1.8 |
| R4 | A pipeline or feedback edge requires every worker hosting the source to host the target; otherwise spawn fails. Workers hosting only the target get an input that is already exhausted. | Today's staged path silently drops such data (H14); the docs already promise an error | DA | §5.1.8; brief §2.1 |
| R5 | Each hosted operator keeps its stage's worker context `(worker, par(stage))`. Boundary exchanges use `(worker, total)`. | Preserves exchange routing and `peers()` | DA | brief §2.1 |
| R6 | A probe on `op` expects `|hosts(op)|` writers. | Otherwise a finished writer's last view holds the probe forever | DA | brief T5 |
| R7 | The heterogeneous path builds once and materializes per worker, so `build` runs twice, not N+1 times. Escalate if anything depends on N+1. | Same model as the uniform path; the docs already require no one-time side effects | DA | brief §2.2, T11 |
| R8 | Heterogeneous clusters stay rejected in step 2. Lifting that is a feature and needs the user's decision. | Scope | DA (escalate if needed) | §5.1.8 |
| R9 | The checker's worker-local view mode goes; every multi-worker tracker must have progress channels (debug assertion). | S and SR now have global views | DA | brief §2.5 |
| R10 | Any construction-time capability must be declared through `add_operator_with_capabilities`; a debug assertion catches undeclared ones. | Otherwise A4 misses it, which is H13 again | DA | brief §2.3, T2 |
| R11 | Step-2 golden rule: S and SR failures must be explained by uniform-path entries, with the same kind seen on a uniform path for that shape; uniform clean cases stay clean. | Separates "inherited" from "new" bugs | DA | brief §5.2 |

## Open

| # | Item | Owner / next |
|---|---|---|
| O1 | The recursive-export regression (a scope on an outer feedback path; cached import and export) is not modelled. | DA: before step 3's loop work. Running the TLC model checker needs the coordinator's go-ahead. |
| O2 | Binary operators, `retain` and the justification set `J` are not modelled. | DA: before or during step 3 |
| O3 | Scoped retirement through a back-edge, with heterogeneous hosting of loop operators, is not modelled. | DA |
| O4 | The §6 cost gates for step 3 have not run; hot-path and traffic cost is unknown. | coord (benchmarks); DA sets the gates |
| O5 | V6 cannot check per-sender sequence numbers because the wire has none. Add them in step 3? | DA: decide in the step-3 brief |
| O6 | V3 "hard error in every build" needs a release-cost budget (harness finding 8). | DA: step-3 brief |
| O7 | Planned matrix shapes not built: MockNetwork decode error, injected residual, large progress batch, probe shapes. Step 3 needs the knobs. | DA: step-3 brief |
| O8 | Allowing heterogeneous clusters. | user, if wanted |
| O10 | Step 2 is in progress in its own session, with the step-2 brief as its spec. When it reports done, the DA reviews it for design fidelity. | coord schedules; DA reviews |
