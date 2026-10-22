------------------------- MODULE PortCountedProgress -------------------------
(***************************************************************************)
(* A model of progress accounting in instancy, for the TLC model checker.  *)
(*                                                                         *)
(* It models the design in ../progress-accounting-design.md ("port-counted *)
(* progress", PCP) and, through switches, today's behaviour and single-    *)
(* rule mutations of PCP. Every switch value is a constant set in a .cfg.  *)
(*                                                                         *)
(* What is modelled:                                                       *)
(*  - several workers, each running the operators it hosts;                *)
(*  - records on edges ("msgs"): everything between an operator handing a *)
(*    record over and the next operator taking it, whatever channel,       *)
(*    buffer or network sits in between. Delivery order is arbitrary.      *)
(*    With Transit, a record must also arrive ("ready") before it can be  *)
(*    taken, so a worker can be idle while records are on their way;      *)
(*  - capabilities, notify stashes, async tasks and sink egress;           *)
(*  - per-worker ledgers of uncommitted progress changes, commits, and     *)
(*    per-sender FIFO progress lanes whose batches may be split in two;    *)
(*  - each worker's view (its copy of the progress counts);                *)
(*  - operator shutdown (S2a), seal markers, scoped worker success (S2b),  *)
(*    proven residuals, and today's idle force-close;                      *)
(*  - loops as the interim design (PR4c until PR6a) tracks them: the body  *)
(*    is flattened into the outer time, the tracker drops each back-edge   *)
(*    and adds a guard edge, and frontiers are claimed outside loops only. *)
(*                                                                         *)
(* What is not modelled: nested loop scopes (see LoopScopes.tla), iteration*)
(* timestamps, backpressure (a full channel only delays), end-of-stream    *)
(* exhaustion, failures, binary operators, retain, and record payloads.    *)
(* One record is one batch.                                                *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS
    Workers,    \* the workers of the dataflow
    MaxT,       \* timestamps are 0..MaxT
    Ops,        \* operator ids
    Edges,      \* edge ids
    Kind,       \* [Ops -> {"input", "map", "branch", "notify", "async", "sink"}]
    Src, Dst,   \* [Edges -> Ops]
    Pact,       \* [Edges -> {"pipe", "exchange", "broadcast"}]
    Hosts,      \* [Ops -> SUBSET Workers]: which workers run each operator
    MaxSends,   \* records each worker's input may send in total
    \* ---- switches ----
    Counting,   \* "ports": PCP, keyed by edge.
                \* "sourcekey": PCP, but produced is filed under the operator
                \*   output's first edge (today's H12 key collision).
                \* "optin": today's wiring (see Produced).
    Wired,      \* with "optin": exchange edges whose producer records +n
    Decide,     \* "pcp": S2a + seal + scoped S2b. Or today's idle force-close
                \* with predicate "signedsum" (today), "positive" (timely)
                \* or "exactzero" (PR5).
    Frag,       \* how a commit may be split on the wire: "positivesfirst",
                \* "capsfirst" (today's collection order) or "none"
    Justify,    \* "local": capabilities only from consumed input (PCP).
                \* "frontier": notify_at at any time the frontier permits (today).
    AsyncCap,   \* TRUE: async tasks hold a capability (PCP). FALSE: today.
    Seeding,    \* initial views: "hosts" (A4: multiplicity = hosting workers),
                \* "local" (each worker only knows its own; peers' arrive later),
                \* "workers" (multiplicity = all workers, wrong when hosts differ)
    BcastMult,  \* broadcast multiplicity: "hosts" (PCP) or "workers" (wrong)
    Egress,     \* sinks: "none" (hand over at once), "keep" (PCP: the input
                \* credit is kept until the external receiver accepts) or
                \* "release" (consumed is recorded when the sink takes it)
    Transit,    \* TRUE: records must arrive before they can be taken
    KeepOpen,   \* TRUE: inputs never close after the last epoch (liveness checks)
    \* ---- loops (interim, PR4c until PR6a) ----
    \* Loop bodies are flattened into the outer time: an iteration does not
    \* change a record's timestamp here, exactly as the interim design counts.
    BackEdges,  \* each loop's back-edge (feedback -> loop head): a data edge,
                \* dropped from the tracker's reachability
    Guards,     \* progress-only guard edges <<Out(feedback), Tgt(exit edge)>>
    LoopOps,    \* operators inside loop bodies (no frontier claims there)
    LapEdges,   \* edges whose use costs one lap of the budget
    MaxLaps     \* laps in total, over all workers

Times == 0..MaxT

---------------------------------------------------------------------------
(* Locations and reachability *)

Tgt(e) == <<"tgt", e>>        \* an operator input; records are counted here
Out(o) == <<"src", o>>        \* an operator output; capabilities are counted here
Locs == {Out(o) : o \in Ops} \cup {Tgt(e) : e \in Edges}
PS == Locs \X Times           \* pointstamps
Zero == [p \in PS |-> 0]

OutEdges(o) == {e \in Edges : Src[e] = o}
InEdges(o) == {e \in Edges : Dst[e] = o}

\* The data graph: where records can actually flow, back-edges included.
DataSucc(l) == IF l[1] = "tgt" THEN {Out(Dst[l[2]])}
                               ELSE {Tgt(e) : e \in OutEdges(l[2])}

\* The tracker's graph: back-edges dropped, guard edges added (PR4c).
ProgSucc(l) ==
    (IF l[1] = "tgt" THEN {Out(Dst[l[2]])}
                     ELSE {Tgt(e) : e \in OutEdges(l[2]) \ BackEdges})
    \cup {g[2] : g \in {x \in Guards : x[1] = l}}

RECURSIVE DataClosure(_, _), ProgClosure(_, _)
DataClosure(S, n) == IF n = 0 THEN S ELSE DataClosure(S \cup UNION {DataSucc(l) : l \in S}, n - 1)
ProgClosure(S, n) == IF n = 0 THEN S ELSE ProgClosure(S \cup UNION {ProgSucc(l) : l \in S}, n - 1)

\* <<a, b>> \in DataReach iff work at a could result in work at b (reflexive).
\* ProgReach is the same question as the tracker answers it. Timestamps are
\* unchanged along every path: loops are flattened into the outer time.
DataReach == {<<a, b>> \in Locs \X Locs : b \in DataClosure({a}, Cardinality(Locs))}
ProgReach == {<<a, b>> \in Locs \X Locs : b \in ProgClosure({a}, Cardinality(Locs))}

\* Precomputed per location, to keep each check cheap.
UpProg == [l \in Locs |-> {a \in Locs : <<a, l>> \in ProgReach}]
DownData == [l \in Locs |-> {b \in Locs : <<l, b>> \in DataReach}]

\* The tracker's graph must have no cycles: the design rejects identity
\* cycles at build time (§5.1.7). Closure-based reachability could not show
\* the liveness failure such a cycle causes, so the model refuses them.
ASSUME \A a \in Locs : a \notin ProgClosure(ProgSucc(a), Cardinality(Locs))

\* Some positive count in view V could still result in work at (l, t),
\* according to the tracker.
Witnessed(V, l, t) == \E a \in UpProg[l], s \in 0..t : V[<<a, s>>] > 0

\* The frontier at l has passed t: nothing at or before t can arrive.
Passed(V, l, t) == ~Witnessed(V, l, t)

---------------------------------------------------------------------------
(* Ledger arithmetic *)

Unit(p, d) == [q \in PS |-> IF q = p THEN d ELSE 0]
Plus(F, G) == [q \in PS |-> F[q] + G[q]]

\* Records one hand-over puts on edge e: broadcast reaches every host.
Mult(e) ==
    IF Pact[e] # "broadcast" THEN 1
    ELSE IF BcastMult = "hosts" THEN Cardinality(Hosts[Dst[e]]) ELSE Cardinality(Workers)

\* The edge whose input a "sourcekey" binding files every produced count under.
Canon(o) == CHOOSE x \in OutEdges(o) : TRUE

\* "produced": +n at the edge's input.
Produced(e, t) ==
    CASE Counting = "ports"     -> Unit(<<Tgt(e), t>>, Mult(e))
      [] Counting = "sourcekey" -> Unit(<<Tgt(Canon(Src[e])), t>>, Mult(e))
      [] Counting = "optin"     ->
            \* only wired exchange producers record it; pipeline and
            \* broadcast edges are never counted
            IF Pact[e] = "exchange" /\ e \in Wired THEN Unit(<<Tgt(e), t>>, 1) ELSE Zero

\* "consumed": -1 at the edge's input. Under "optin" exchange consumers
\* always record it.
Consumed(e, t) ==
    IF Counting # "optin" \/ Pact[e] = "exchange" THEN Unit(<<Tgt(e), t>>, -1)
    ELSE Zero

CapChange(o, t, d) == Unit(<<Out(o), t>>, d)

RECURSIVE EmitOver(_, _)
EmitOver(E, t) ==
    IF E = {} THEN Zero
    ELSE LET e == CHOOSE x \in E : TRUE IN Plus(Produced(e, t), EmitOver(E \ {e}, t))

\* Ledger change for handing one record at t to every output edge of o.
EmitDelta(o, t) == EmitOver(OutEdges(o), t)

---------------------------------------------------------------------------
(* Moving records *)

\* Choices of receiving worker for each output edge of o on worker w.
Dests(w, o) ==
    {d \in [OutEdges(o) -> Workers] :
        \A e \in OutEdges(o) :
            /\ d[e] \in Hosts[Dst[e]]
            /\ Pact[e] = "pipe" => d[e] = w
            /\ Pact[e] = "broadcast" => d[e] = CHOOSE x \in Hosts[Dst[e]] : TRUE}

\* Hand one record at t to each edge in E (all from the same operator).
EmitE(m, E, t, d) ==
    [e \in Edges |->
        IF e \notin E THEN m[e]
        ELSE [x \in Workers |-> [s \in Times |->
                m[e][x][s] +
                (IF s = t /\ (IF Pact[e] = "broadcast" THEN x \in Hosts[Dst[e]]
                                                       ELSE x = d[e])
                 THEN 1 ELSE 0)]]]

Emit(m, o, t, d) == EmitE(m, OutEdges(o), t, d)

\* Without Transit a record is ready to be taken as soon as it is handed over.
Arrive(r, o, t, d) == IF Transit THEN r ELSE Emit(r, o, t, d)
ArriveE(r, E, t, d) == IF Transit THEN r ELSE EmitE(r, E, t, d)

Take(m, e, w, t) == [m EXCEPT ![e][w][t] = @ - 1]

---------------------------------------------------------------------------
(* Progress lanes *)

Ents(L) == {<<p, L[p]>> : p \in {q \in PS : L[q] # 0}}

Delta(S, q) == IF \E x \in S : x[1] = q THEN (CHOOSE x \in S : x[1] = q)[2] ELSE 0
Apply(V, S) == [q \in PS |-> V[q] + Delta(S, q)]

Upd(S) == [kind |-> "upd", entries |-> S]
SealMsg == [kind |-> "seal", entries |-> {}]

\* A commit is sent whole, or split into two fragments in the configured order.
Split(L) ==
    LET first == IF Frag = "positivesfirst" THEN {x \in Ents(L) : x[2] > 0}
                 ELSE {x \in Ents(L) : x[1][1][1] = "src"}
        rest == Ents(L) \ first
    IN SelectSeq(<<Upd(first), Upd(rest)>>, LAMBDA m : m.entries # {})

Encodings(L) == IF Frag = "none" THEN {<<Upd(Ents(L))>>}
                ELSE {<<Upd(Ents(L))>>, Split(L)}

---------------------------------------------------------------------------
(* Scope of a worker (S2b(c)): every location that can reach one it hosts *)
(* in the data graph, back-edges included (?5.1.5).                        *)

HostLocs(o) == {Out(o)} \cup {Tgt(e) : e \in InEdges(o)}
ScopeOf == [w \in Workers |->
              {l \in Locs : \E o \in Ops : w \in Hosts[o] /\
                              \E h \in HostLocs(o) : <<l, h>> \in DataReach}]

\* Locations outside loop bodies. In the interim, S1 is claimed only there.
LoopLocs == {Out(o) : o \in LoopOps} \cup {Tgt(e) : e \in {x \in Edges : Dst[x] \in LoopOps}}
Observed == Locs \ LoopLocs

---------------------------------------------------------------------------
VARIABLES
    msgs,       \* [Edges -> [Workers -> [Times -> Nat]]]: records on edges, by receiver
    ready,      \* same shape: of those, records that have arrived and can be taken
    egress,     \* same shape: records a sink took but the external receiver has not accepted
    caps,       \* [Workers -> [Ops -> [Times -> Nat]]]: capabilities held
    stash,      \* [Workers -> [Ops -> [Times -> Nat]]]: records kept by notify operators
    pend,       \* [Workers -> [Ops -> [Times -> Nat]]]: async tasks in flight
    sends,      \* [Workers -> Nat]: records sent by inputs so far
    ledger,     \* [Workers -> counts]: uncommitted changes
    view,       \* [Workers -> counts]: each worker's progress counts
    lane,       \* [Workers -> [Workers -> Seq(message)]]: progress lanes, sender -> receiver
    done,       \* [Workers -> [Ops -> BOOLEAN]]: operator instance shut down
    notified,   \* [Workers -> [Ops -> SUBSET Times]]: notifications delivered
    sealed,     \* [Workers -> BOOLEAN]
    sealsSeen,  \* [Workers -> SUBSET Workers]: seal markers applied
    success,    \* [Workers -> BOOLEAN]: worker reported success (retired)
    residual,   \* BOOLEAN: a proven residual was reported
    laps        \* Nat: laps run so far, over all workers

vars == <<msgs, ready, egress, caps, stash, pend, sends, ledger, view, lane, done,
          notified, sealed, sealsSeen, success, residual, laps>>

PerOp(v) == [w \in Workers |-> [o \in Ops |-> [t \in Times |-> v]]]
PerEdge(v) == [e \in Edges |-> [w \in Workers |-> [t \in Times |-> v]]]

InitialCaps ==
    [w \in Workers |-> [o \in Ops |-> [t \in Times |->
        IF Kind[o] = "input" /\ w \in Hosts[o] /\ t = 0 THEN 1 ELSE 0]]]

InputSeed(w, o) ==
    CASE Seeding = "hosts"   -> Cardinality(Hosts[o])
      [] Seeding = "workers" -> Cardinality(Workers)
      [] Seeding = "local"   -> IF w \in Hosts[o] THEN 1 ELSE 0

SeedView(w) ==
    [p \in PS |-> IF p[2] = 0 /\ p[1][1] = "src" /\ Kind[p[1][2]] = "input"
                  THEN InputSeed(w, p[1][2]) ELSE 0]

\* With "local" seeding a worker's initial capabilities reach peers as a batch.
InitBatch(s) == {<<<<Out(o), 0>>, 1>> : o \in {x \in Ops : Kind[x] = "input" /\ s \in Hosts[x]}}

Init ==
    /\ msgs = PerEdge(0)
    /\ ready = PerEdge(0)
    /\ egress = PerEdge(0)
    /\ caps = InitialCaps
    /\ stash = PerOp(0)
    /\ pend = PerOp(0)
    /\ sends = [w \in Workers |-> 0]
    /\ ledger = [w \in Workers |-> Zero]
    /\ view = [w \in Workers |-> SeedView(w)]
    /\ lane = [s \in Workers |-> [r \in Workers |->
                 IF Seeding = "local" /\ s # r /\ InitBatch(s) # {}
                 THEN <<Upd(InitBatch(s))>> ELSE <<>>]]
    /\ done = [w \in Workers |-> [o \in Ops |-> FALSE]]
    /\ notified = [w \in Workers |-> [o \in Ops |-> {}]]
    /\ sealed = [w \in Workers |-> FALSE]
    /\ sealsSeen = [w \in Workers |-> {}]
    /\ success = [w \in Workers |-> FALSE]
    /\ residual = FALSE
    /\ laps = 0

---------------------------------------------------------------------------
(* Operator activations. Each one is atomic and only writes the worker's   *)
(* ledger; the commit publishes it.                                        *)

CanRun(w, o) == ~success[w] /\ w \in Hosts[o] /\ ~done[w][o]

AddLedger(w, D) == ledger' = [ledger EXCEPT ![w] = Plus(@, D)]

InputSend(w, o) ==
    /\ Kind[o] = "input" /\ CanRun(w, o)
    /\ sends[w] < MaxSends
    /\ \E t \in Times, d \in Dests(w, o) :
         /\ caps[w][o][t] > 0
         /\ msgs' = Emit(msgs, o, t, d)
         /\ ready' = Arrive(ready, o, t, d)
         /\ AddLedger(w, EmitDelta(o, t))
    /\ sends' = [sends EXCEPT ![w] = @ + 1]
    /\ UNCHANGED <<laps, egress, caps, stash, pend, view, lane, done, notified, sealed, sealsSeen, success, residual>>

\* Move to the next epoch, or close the input after the last one.
InputAdvance(w, o) ==
    /\ Kind[o] = "input" /\ CanRun(w, o)
    /\ \E t \in Times :
         /\ caps[w][o][t] > 0
         /\ t = MaxT => ~KeepOpen
         /\ IF t < MaxT
              THEN /\ caps' = [caps EXCEPT ![w][o][t] = 0, ![w][o][t + 1] = 1]
                   /\ AddLedger(w, Plus(CapChange(o, t, -1), CapChange(o, t + 1, 1)))
              ELSE /\ caps' = [caps EXCEPT ![w][o][t] = 0]
                   /\ AddLedger(w, CapChange(o, t, -1))
    /\ UNCHANGED <<laps, msgs, ready, egress, stash, pend, sends, view, lane, done, notified, sealed, sealsSeen, success, residual>>

\* A record on its way arrives at the receiving worker.
Deliver(e, w, t) ==
    /\ Transit
    /\ msgs[e][w][t] > ready[e][w][t]
    /\ ready' = [ready EXCEPT ![e][w][t] = @ + 1]
    /\ UNCHANGED <<laps, msgs, egress, caps, stash, pend, sends, ledger, view, lane, done, notified, sealed, sealsSeen, success, residual>>

MapStep(w, o) ==
    /\ Kind[o] = "map" /\ CanRun(w, o)
    /\ \E e \in InEdges(o), t \in Times, d \in Dests(w, o) :
         /\ ready[e][w][t] > 0
         /\ msgs' = Emit(Take(msgs, e, w, t), o, t, d)
         /\ ready' = Arrive(Take(ready, e, w, t), o, t, d)
         /\ AddLedger(w, Plus(Consumed(e, t), EmitDelta(o, t)))
    /\ UNCHANGED <<laps, egress, caps, stash, pend, sends, view, lane, done, notified, sealed, sealsSeen, success, residual>>

\* A branch sends each record to exactly one of its outputs, e.g. around the
\* loop or out of it. A lap edge costs one lap of the worker's budget.
BranchStep(w, o) ==
    /\ Kind[o] = "branch" /\ CanRun(w, o)
    /\ \E e \in InEdges(o), t \in Times, x \in OutEdges(o), d \in Dests(w, o) :
         /\ ready[e][w][t] > 0
         /\ x \in LapEdges => laps < MaxLaps
         /\ msgs' = EmitE(Take(msgs, e, w, t), {x}, t, d)
         /\ ready' = ArriveE(Take(ready, e, w, t), {x}, t, d)
         /\ AddLedger(w, Plus(Consumed(e, t), Produced(x, t)))
         /\ laps' = IF x \in LapEdges THEN laps + 1 ELSE laps
    /\ UNCHANGED <<egress, caps, stash, pend, sends, view, lane, done, notified, sealed, sealsSeen, success, residual>>

\* A sink hands the record to the external receiver at once (Egress = "none").
SinkStep(w, o) ==
    /\ Egress = "none"
    /\ Kind[o] = "sink" /\ CanRun(w, o)
    /\ \E e \in InEdges(o), t \in Times :
         /\ ready[e][w][t] > 0
         /\ msgs' = Take(msgs, e, w, t)
         /\ ready' = Take(ready, e, w, t)
         /\ AddLedger(w, Consumed(e, t))
    /\ UNCHANGED <<laps, egress, caps, stash, pend, sends, view, lane, done, notified, sealed, sealsSeen, success, residual>>

\* A sink takes the record; the external receiver accepts it later.
SinkTake(w, o) ==
    /\ Egress # "none"
    /\ Kind[o] = "sink" /\ CanRun(w, o)
    /\ \E e \in InEdges(o), t \in Times :
         /\ ready[e][w][t] > 0
         /\ msgs' = Take(msgs, e, w, t)
         /\ ready' = Take(ready, e, w, t)
         /\ egress' = [egress EXCEPT ![e][w][t] = @ + 1]
         /\ AddLedger(w, IF Egress = "release" THEN Consumed(e, t) ELSE Zero)
    /\ UNCHANGED <<laps, caps, stash, pend, sends, view, lane, done, notified, sealed, sealsSeen, success, residual>>

SinkAccept(w, o) ==
    /\ Egress # "none"
    /\ Kind[o] = "sink" /\ CanRun(w, o)
    /\ \E e \in InEdges(o), t \in Times :
         /\ egress[e][w][t] > 0
         /\ egress' = [egress EXCEPT ![e][w][t] = @ - 1]
         /\ AddLedger(w, IF Egress = "keep" THEN Consumed(e, t) ELSE Zero)
    /\ UNCHANGED <<laps, msgs, ready, caps, stash, pend, sends, view, lane, done, notified, sealed, sealsSeen, success, residual>>

\* A notify operator (count, reduce, ...) keeps the record and holds a
\* capability at its time, justified by consuming it in this activation.
NotifyTake(w, o) ==
    /\ Kind[o] = "notify" /\ CanRun(w, o)
    /\ \E e \in InEdges(o), t \in Times :
         /\ ready[e][w][t] > 0
         /\ msgs' = Take(msgs, e, w, t)
         /\ ready' = Take(ready, e, w, t)
         /\ stash' = [stash EXCEPT ![w][o][t] = @ + 1]
         /\ IF caps[w][o][t] = 0
              THEN /\ caps' = [caps EXCEPT ![w][o][t] = 1]
                   /\ AddLedger(w, Plus(Consumed(e, t), CapChange(o, t, 1)))
              ELSE /\ caps' = caps
                   /\ AddLedger(w, Consumed(e, t))
    /\ UNCHANGED <<laps, egress, pend, sends, view, lane, done, notified, sealed, sealsSeen, success, residual>>

\* Today's notify_at: a capability at any time the input frontier still
\* permits, justified by nothing this worker consumed or holds (P-9).
NotifyRequest(w, o) ==
    /\ Justify = "frontier"
    /\ Kind[o] = "notify" /\ CanRun(w, o)
    /\ \E t \in Times :
         /\ caps[w][o][t] = 0
         /\ t \notin notified[w][o]
         /\ \E e \in InEdges(o) : Witnessed(view[w], Tgt(e), t)
         /\ caps' = [caps EXCEPT ![w][o][t] = 1]
         /\ AddLedger(w, CapChange(o, t, 1))
    /\ UNCHANGED <<laps, msgs, ready, egress, stash, pend, sends, view, lane, done, notified, sealed, sealsSeen, success, residual>>

CanFire(w, o, t) ==
    /\ Kind[o] = "notify" /\ caps[w][o][t] > 0
    /\ \A e \in InEdges(o) : Passed(view[w], Tgt(e), t)

\* The input frontier passed t: emit the aggregate for t and drop the capability.
NotifyFire(w, o) ==
    /\ Kind[o] = "notify" /\ CanRun(w, o)
    /\ \E t \in Times, d \in Dests(w, o) :
         /\ CanFire(w, o, t)
         /\ notified' = [notified EXCEPT ![w][o] = @ \cup {t}]
         /\ caps' = [caps EXCEPT ![w][o][t] = 0]
         /\ stash' = [stash EXCEPT ![w][o][t] = 0]
         /\ IF stash[w][o][t] > 0
              THEN /\ msgs' = Emit(msgs, o, t, d)
                   /\ ready' = Arrive(ready, o, t, d)
                   /\ AddLedger(w, Plus(EmitDelta(o, t), CapChange(o, t, -1)))
              ELSE /\ UNCHANGED <<msgs, ready>>
                   /\ AddLedger(w, CapChange(o, t, -1))
    /\ UNCHANGED <<laps, egress, pend, sends, view, lane, done, sealed, sealsSeen, success, residual>>

AsyncStart(w, o) ==
    /\ Kind[o] = "async" /\ CanRun(w, o)
    /\ \E e \in InEdges(o), t \in Times :
         /\ ready[e][w][t] > 0
         /\ msgs' = Take(msgs, e, w, t)
         /\ ready' = Take(ready, e, w, t)
         /\ pend' = [pend EXCEPT ![w][o][t] = @ + 1]
         /\ IF AsyncCap
              THEN /\ caps' = [caps EXCEPT ![w][o][t] = @ + 1]
                   /\ AddLedger(w, Plus(Consumed(e, t), CapChange(o, t, 1)))
              ELSE /\ caps' = caps
                   /\ AddLedger(w, Consumed(e, t))
    /\ UNCHANGED <<laps, egress, stash, sends, view, lane, done, notified, sealed, sealsSeen, success, residual>>

AsyncFinish(w, o) ==
    /\ Kind[o] = "async" /\ CanRun(w, o)
    /\ \E t \in Times, d \in Dests(w, o) :
         /\ pend[w][o][t] > 0
         /\ pend' = [pend EXCEPT ![w][o][t] = @ - 1]
         /\ msgs' = Emit(msgs, o, t, d)
         /\ ready' = Arrive(ready, o, t, d)
         /\ IF AsyncCap
              THEN /\ caps' = [caps EXCEPT ![w][o][t] = @ - 1]
                   /\ AddLedger(w, Plus(EmitDelta(o, t), CapChange(o, t, -1)))
              ELSE /\ caps' = caps
                   /\ AddLedger(w, EmitDelta(o, t))
    /\ UNCHANGED <<laps, egress, stash, sends, view, lane, done, notified, sealed, sealsSeen, success, residual>>

---------------------------------------------------------------------------
(* Commit and progress lanes *)

\* Publish the ledger: apply it to the own view, then queue it to every peer.
Commit(w) ==
    /\ ~success[w]
    /\ ledger[w] # Zero
    /\ view' = [view EXCEPT ![w] = Plus(@, ledger[w])]
    /\ \E enc \in Encodings(ledger[w]) :
         lane' = [s \in Workers |-> [r \in Workers |->
                    IF s = w /\ r # w THEN lane[s][r] \o enc ELSE lane[s][r]]]
    /\ ledger' = [ledger EXCEPT ![w] = Zero]
    /\ UNCHANGED <<laps, msgs, ready, egress, caps, stash, pend, sends, done, notified, sealed, sealsSeen, success, residual>>

\* Apply the oldest message from s at r. A retired worker discards it.
Receive(s, r) ==
    /\ s # r
    /\ lane[s][r] # <<>>
    /\ LET m == Head(lane[s][r]) IN
         IF success[r]
           THEN UNCHANGED <<view, sealsSeen>>
           ELSE IF m.kind = "seal"
                  THEN /\ sealsSeen' = [sealsSeen EXCEPT ![r] = @ \cup {s}]
                       /\ UNCHANGED view
                  ELSE /\ view' = [view EXCEPT ![r] = Apply(@, m.entries)]
                       /\ UNCHANGED sealsSeen
    /\ lane' = [lane EXCEPT ![s][r] = Tail(@)]
    /\ UNCHANGED <<laps, msgs, ready, egress, caps, stash, pend, sends, ledger, done, notified, sealed, success, residual>>

---------------------------------------------------------------------------
(* PCP decisions (Decide = "pcp") *)

\* Holds nothing and has nothing left to hand over, external egress included.
OpIdle(w, o) ==
    /\ \A t \in Times : caps[w][o][t] = 0 /\ stash[w][o][t] = 0 /\ pend[w][o][t] = 0
    /\ \A e \in InEdges(o), t \in Times : egress[e][w][t] = 0

\* S2a: committed, holds nothing, and nothing can reach any of its inputs.
Shutdown(w, o) ==
    /\ Decide = "pcp"
    /\ CanRun(w, o)
    /\ ledger[w] = Zero
    /\ OpIdle(w, o)
    /\ \A e \in InEdges(o) : Passed(view[w], Tgt(e), MaxT)
    /\ done' = [done EXCEPT ![w][o] = TRUE]
    /\ UNCHANGED <<laps, msgs, ready, egress, caps, stash, pend, sends, ledger, view, lane, notified, sealed, sealsSeen, success, residual>>

AllHostedDone(w) == \A o \in Ops : w \in Hosts[o] => done[w][o]

\* The seal follows every committed batch on each lane.
Seal(w) ==
    /\ Decide = "pcp"
    /\ ~success[w] /\ ~sealed[w]
    /\ AllHostedDone(w) /\ ledger[w] = Zero
    /\ sealed' = [sealed EXCEPT ![w] = TRUE]
    /\ lane' = [s \in Workers |-> [r \in Workers |->
                  IF s = w /\ r # w THEN Append(lane[s][r], SealMsg) ELSE lane[s][r]]]
    /\ UNCHANGED <<laps, msgs, ready, egress, caps, stash, pend, sends, ledger, view, done, notified, sealsSeen, success, residual>>

ScopeZero(w) == \A l \in ScopeOf[w], t \in Times : view[w][<<l, t>>] = 0

\* S2b: every count in the worker's scope is exactly zero.
Succeed(w) ==
    /\ Decide = "pcp"
    /\ ~success[w] /\ sealed[w]
    /\ ScopeZero(w)
    /\ success' = [success EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<laps, msgs, ready, egress, caps, stash, pend, sends, ledger, view, lane, done, notified, sealed, sealsSeen, residual>>

\* Every seal applied, and still a non-zero count in scope: a proven residual.
ProvenResidual(w) ==
    /\ Decide = "pcp"
    /\ ~success[w] /\ sealed[w] /\ ~residual
    /\ sealsSeen[w] = Workers \ {w}
    /\ ~ScopeZero(w)
    /\ residual' = TRUE
    /\ UNCHANGED <<laps, msgs, ready, egress, caps, stash, pend, sends, ledger, view, lane, done, notified, sealed, sealsSeen, success>>

---------------------------------------------------------------------------
(* Today's decision: idle force-close (Decide # "pcp") *)

RECURSIVE SumOver(_, _)
SumOver(V, S) == IF S = {} THEN 0
                 ELSE LET p == CHOOSE x \in S : TRUE IN V[p] + SumOver(V, S \ {p})

NothingTracked(V) ==
    CASE Decide = "signedsum" -> SumOver(V, PS) = 0
      [] Decide = "positive"  -> \A p \in PS : V[p] <= 0
      [] Decide = "exactzero" -> V = Zero

\* The worker has nothing it could do now: no record has arrived for it, no
\* async task is running, and no notification can fire. Like today's
\* executor, it ignores a sink's pending external sends.
LocallyIdle(w) ==
    /\ \A e \in Edges, t \in Times : ready[e][w][t] = 0
    /\ \A o \in Ops, t \in Times : pend[w][o][t] = 0 /\ ~CanFire(w, o, t)

\* Close every hosted operator and report success. Late records are lost.
ForceClose(w) ==
    /\ Decide # "pcp"
    /\ ~success[w]
    /\ ledger[w] = Zero
    /\ LocallyIdle(w)
    /\ NothingTracked(view[w])
    /\ done' = [done EXCEPT ![w] = [o \in Ops |-> TRUE]]
    /\ success' = [success EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<laps, msgs, ready, egress, caps, stash, pend, sends, ledger, view, lane, notified, sealed, sealsSeen, residual>>

---------------------------------------------------------------------------
AllSucceeded == \A w \in Workers : success[w]

\* The run is over; stutter so that TLC does not report a deadlock.
Terminated == AllSucceeded /\ UNCHANGED vars

OpStep(w, o) ==
    \/ InputSend(w, o) \/ InputAdvance(w, o) \/ MapStep(w, o) \/ BranchStep(w, o)
    \/ SinkStep(w, o) \/ SinkTake(w, o) \/ SinkAccept(w, o)
    \/ NotifyTake(w, o) \/ NotifyRequest(w, o) \/ NotifyFire(w, o)
    \/ AsyncStart(w, o) \/ AsyncFinish(w, o) \/ Shutdown(w, o)

Next ==
    \/ \E w \in Workers, o \in Ops : OpStep(w, o)
    \/ \E e \in Edges, w \in Workers, t \in Times : Deliver(e, w, t)
    \/ \E w \in Workers :
         Commit(w) \/ Seal(w) \/ Succeed(w) \/ ProvenResidual(w) \/ ForceClose(w)
    \/ \E s, r \in Workers : Receive(s, r)
    \/ Terminated

Spec == Init /\ [][Next]_vars

\* For liveness checks: every enabled internal step eventually happens.
\* Inputs eventually advance; they need not send.
Fairness ==
    /\ \A w \in Workers, o \in Ops :
         /\ WF_vars(InputAdvance(w, o)) /\ WF_vars(MapStep(w, o)) /\ WF_vars(BranchStep(w, o))
         /\ WF_vars(SinkStep(w, o)) /\ WF_vars(SinkTake(w, o)) /\ WF_vars(SinkAccept(w, o))
         /\ WF_vars(NotifyTake(w, o)) /\ WF_vars(NotifyFire(w, o))
         /\ WF_vars(AsyncStart(w, o)) /\ WF_vars(AsyncFinish(w, o)) /\ WF_vars(Shutdown(w, o))
    /\ \A e \in Edges, w \in Workers, t \in Times : WF_vars(Deliver(e, w, t))
    /\ \A w \in Workers :
         WF_vars(Commit(w)) /\ WF_vars(Seal(w)) /\ WF_vars(Succeed(w)) /\ WF_vars(ForceClose(w))
    /\ \A s, r \in Workers : WF_vars(Receive(s, r))

LiveSpec == Spec /\ Fairness

---------------------------------------------------------------------------
(* Properties. Deadlock (a stuck state before every worker succeeded) is a  *)
(* hang; TLC reports it unless run with -deadlock.                         *)

\* Outstanding work held by worker w at pointstamp p.
WorkAt(w, p) ==
    IF p[1][1] = "tgt" THEN msgs[p[1][2]][w][p[2]] + egress[p[1][2]][w][p[2]]
    ELSE caps[w][p[1][2]][p[2]] + stash[w][p[1][2]][p[2]] + pend[w][p[1][2]][p[2]]

RECURSIVE SumWork(_, _)
SumWork(p, S) == IF S = {} THEN 0
                 ELSE LET w == CHOOSE x \in S : TRUE IN WorkAt(w, p) + SumWork(p, S \ {w})

\* True outstanding work at a pointstamp, over all workers.
TrueWork(p) == SumWork(p, Workers)

\* S1 (Naiad, FORTE 2013, ITP 2021): every live worker's view has a positive
\* count that could result in each piece of outstanding work.
FrontierSafe ==
    \A w \in Workers : ~success[w] =>
        \A p \in PS : TrueWork(p) > 0 => Witnessed(view[w], p[1], p[2])

\* The interim loop claim (§5.1.7): at every location outside loop bodies,
\* the tracker never lets the frontier pass work that can still reach it
\* through the real data graph, laps included.
ObserverSafe ==
    \A w \in Workers : ~success[w] =>
        \A p \in PS : TrueWork(p) > 0 =>
            \A l \in DownData[p[1]] \cap Observed : Witnessed(view[w], l, p[2])

\* S3: no record sits at an operator instance that has shut down.
NoDataAtDone ==
    \A e \in Edges, w \in Workers, t \in Times : done[w][Dst[e]] => msgs[e][w][t] = 0

\* No record reaches a notify operator at or before a time it was notified for.
NoLateData ==
    \A w \in Workers, o \in Ops, t \in Times, s \in Times, e \in Edges :
        (Kind[o] = "notify" /\ Dst[e] = o /\ t \in notified[w][o] /\ s <= t)
            => msgs[e][w][s] = 0

\* S2c: when every worker has succeeded, no work is left anywhere,
\* including records a sink has not yet handed to its receiver.
CompleteMeansEmpty == AllSucceeded => \A p \in PS : TrueWork(p) = 0

\* With complete accounting, sealing never reveals a residual.
NoResidual == ~residual

\* ---- Sanity checks: each is expected to be violated, which shows the ----
\* ---- behaviour is reachable with real traffic.                        ----

AllSent ==
    \A w \in Workers :
        (\E o \in Ops : Kind[o] = "input" /\ w \in Hosts[o]) => sends[w] = MaxSends
SomeNotified == \E w \in Workers, o \in Ops : notified[w][o] # {}

\* Completion is reachable after every input sent everything and a
\* notification fired.
NeverCompletesWithTraffic == ~(AllSucceeded /\ AllSent /\ SomeNotified)

\* Completion is reachable after a record went around a loop and a
\* notification after the loop fired.
NeverCompletesAfterLap ==
    ~(AllSucceeded /\ SomeNotified /\ laps > 0)

---------------------------------------------------------------------------
(* Liveness *)

\* Every worker eventually succeeds.
EventuallyDone == <>AllSucceeded

\* With KeepOpen, a record stashed at a time before the last epoch is
\* eventually notified, although the input never closes.
NotifyProgress ==
    \A w \in Workers, o \in Ops, t \in Times :
        (Kind[o] = "notify" /\ t < MaxT) => ((stash[w][o][t] > 0) ~> (t \in notified[w][o]))
=============================================================================
