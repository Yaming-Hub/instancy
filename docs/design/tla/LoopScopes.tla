------------------------------ MODULE LoopScopes ------------------------------
(***************************************************************************)
(* A model of PCP's nested loop scopes (design doc §5.1.7, PR6a), for TLC. *)
(*                                                                         *)
(* The parent dataflow is                                                  *)
(*   in -pipe-> [ loop scope L ] -pipe-> u (count after the loop)          *)
(* and the scope L contains, in (outer time, iteration) time:              *)
(*   enter -> n (count inside the loop) -pipe-> body (branch)              *)
(*   body -exchange-> fb (feedback, iteration + 1) -pipe-> n               *)
(*   body -pipe-> leave                                                    *)
(* Records change worker only on a lap; every worker's views still see    *)
(* every worker's counts.                                                  *)
(*                                                                         *)
(* Each worker keeps two views: the parent's, in outer time, and the       *)
(* scope's, in (time, iteration). Port counts at both levels are recorded  *)
(* once, by the worker that moves the record, and committed together.      *)
(* Two derived, never-broadcast links connect the levels:                  *)
(*  - import: the parent's frontier at the scope's input becomes a          *)
(*    pointstamp at the scope's boundary (B0), so counts inside the loop    *)
(*    wait for input that the parent may still send;                        *)
(*  - export: the scope's own internal work (never the import) becomes the  *)
(*    scope node's capability in the parent, so counts after the loop wait *)
(*    for records still inside it.                                          *)
(* Decisions are the end state: S2a shutdown, seals, exact-zero success.   *)
(*                                                                         *)
(* To keep the search small, a worker commits each activation before its   *)
(* next one (other workers still act in between), and shuts down all its   *)
(* eligible operators in one step. Batching several activations into one   *)
(* commit is checked in PortCountedProgress.tla.                           *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Workers,
    MaxT,       \* outer timestamps are 0..MaxT
    MaxIter,    \* iterations are 0..MaxIter
    MaxSends,   \* records the inputs may send in total, over all workers
    MaxLaps,    \* laps in total, over all workers
    Import,     \* TRUE: import the parent frontier (PCP). FALSE: mutation.
    Export,     \* "internal" (PCP) or "none" (mutation: the scope exports nothing)
    Frag,       \* how a commit may be split into two fragments on the wire:
                \* "none"; "positivesfirst" (PCP: positive entries of both
                \* levels, then negative entries of both); or the mutations
                \* "parentfirst" / "childfirst" (one level's section, then the other)
    KeepOpen    \* TRUE: inputs never close after the last epoch (progress checks)

Times == 0..MaxT
Iters == 0..MaxIter
CTimes == Times \X Iters
Ops == {"in", "ent", "n", "body", "fb", "lv", "u"}

---------------------------------------------------------------------------
(* Parent level, in outer time *)

PTgt(e) == <<"tgt", e>>
POut(o) == <<"src", o>>
PLocs == {POut("in"), PTgt("ein"), POut("L"), PTgt("eout"), POut("u")}
PPS == PLocs \X Times
PZero == [p \in PPS |-> 0]

PSucc(l) ==
    CASE l = POut("in")    -> {PTgt("ein")}
      [] l = PTgt("ein")   -> {POut("L")}
      [] l = POut("L")     -> {PTgt("eout")}
      [] l = PTgt("eout")  -> {POut("u")}
      [] l = POut("u")     -> {}

RECURSIVE PClosure(_, _)
PClosure(S, n) == IF n = 0 THEN S ELSE PClosure(S \cup UNION {PSucc(l) : l \in S}, n - 1)
PReach == {<<a, b>> \in PLocs \X PLocs : b \in PClosure({a}, Cardinality(PLocs))}

---------------------------------------------------------------------------
(* Scope level, in (time, iteration) *)

CTgt(e) == <<"ctgt", e>>
COut(o) == <<"csrc", o>>
B0 == <<"boundary", "enter">>       \* where the imported parent frontier lives
CExit == CTgt("c5")                 \* the leave's input
CLocs == {B0, CTgt("c1"), COut("n"), CTgt("c2"), COut("body"), CTgt("c3"),
          COut("fb"), CTgt("c4"), CTgt("c5")}
CPS == CLocs \X Times \X Iters
CZero == [p \in CPS |-> 0]

\* Edges inside the scope. Only Out(fb) -> Tgt(c4) adds one iteration.
CSucc0(l) ==
    CASE l = B0          -> {CTgt("c1")}
      [] l = CTgt("c1")  -> {COut("n")}
      [] l = CTgt("c4")  -> {COut("n")}
      [] l = COut("n")   -> {CTgt("c2")}
      [] l = CTgt("c2")  -> {COut("body")}
      [] l = COut("body") -> {CTgt("c3"), CTgt("c5")}
      [] l = CTgt("c3")  -> {COut("fb")}
      [] l = COut("fb")  -> {}
      [] l = CTgt("c5")  -> {}
CSuccAll(l) == IF l = COut("fb") THEN {CTgt("c4")} ELSE CSucc0(l)

RECURSIVE CClosure0(_, _), CClosureAll(_, _)
CClosure0(S, n) == IF n = 0 THEN S ELSE CClosure0(S \cup UNION {CSucc0(l) : l \in S}, n - 1)
CClosureAll(S, n) == IF n = 0 THEN S ELSE CClosureAll(S \cup UNION {CSuccAll(l) : l \in S}, n - 1)
\* Reachable without the feedback step (summary +0), or at all (at least +1
\* when only reachable through feedback: one lap leads back to n, from which
\* everything else is reachable without another).
CReach0 == {<<a, b>> \in CLocs \X CLocs : b \in CClosure0({a}, Cardinality(CLocs))}
CReachAll == {<<a, b>> \in CLocs \X CLocs : b \in CClosureAll({a}, Cardinality(CLocs))}

\* Precomputed per location, to keep each check cheap.
PUp == [l \in PLocs |-> {a \in PLocs : <<a, l>> \in PReach}]
CUp0 == [l \in CLocs |-> {a \in CLocs : <<a, l>> \in CReach0}]
CUpAll == [l \in CLocs |-> {a \in CLocs : <<a, l>> \in CReachAll}]
CToExit == {a \in CLocs \ {B0} : <<a, CExit>> \in CReachAll}

\* Without the feedback step the scope's graph has no cycle.
ASSUME \A a \in CLocs : a \notin CClosure0(CSucc0(a), Cardinality(CLocs))

---------------------------------------------------------------------------
VARIABLES
    pmsgs,      \* [{"ein", "eout"} -> [Workers -> [Times -> Nat]]]
    cmsgs,      \* [{"c1", ..., "c5"} -> [Workers -> [CTimes -> Nat]]]
    incap,      \* [Workers -> [Times -> Nat]]: input capabilities
    ncap,       \* [Workers -> [CTimes -> Nat]]: the inner count's capabilities
    nstash,     \* [Workers -> [CTimes -> Nat]]
    ucap,       \* [Workers -> [Times -> Nat]]: the outer count's capabilities
    ustash,     \* [Workers -> [Times -> Nat]]
    sends, laps,
    pledger, cledger,   \* uncommitted changes at each level
    pview, cview,       \* each worker's views at each level
    lane,       \* [Workers -> [Workers -> Seq([kind, p, c])]]
    done,       \* [Workers -> [Ops -> BOOLEAN]]
    nnotified,  \* [Workers -> SUBSET CTimes]
    unotified,  \* [Workers -> SUBSET Times]
    sealed, sealsSeen, success, residual

vars == <<pmsgs, cmsgs, incap, ncap, nstash, ucap, ustash, sends, laps, pledger,
          cledger, pview, cview, lane, done, nnotified, unotified, sealed,
          sealsSeen, success, residual>>

---------------------------------------------------------------------------
(* Frontiers *)

\* Derived export: the scope's own work that can still leave it at t.
ExportAt(w, t) ==
    /\ Export = "internal"
    /\ \E a \in CToExit, s \in 0..t, i \in Iters : cview[w][<<a, s, i>>] > 0

\* The parent tracker: committed parent counts plus the export at Out(L).
PWitness(w, l, t) ==
    \/ \E a \in PUp[l], s \in 0..t : pview[w][<<a, s>>] > 0
    \/ POut("L") \in PUp[l] /\ ExportAt(w, t)

\* The scope tracker: committed scope counts plus the import at B0, which is
\* (t, 0) and reaches a location through feedback only at iteration >= 1.
CWitness(w, l, t, i) ==
    \/ \E a \in CUp0[l], s \in 0..t, j \in 0..i : cview[w][<<a, s, j>>] > 0
    \/ i >= 1 /\ \E a \in CUpAll[l], s \in 0..t, j \in 0..(i - 1) : cview[w][<<a, s, j>>] > 0
    \/ /\ Import /\ PWitness(w, PTgt("ein"), t)
       /\ \/ B0 \in CUp0[l]
          \/ i >= 1 /\ B0 \in CUpAll[l]

---------------------------------------------------------------------------
(* Helpers *)

PUnit(p, d) == [q \in PPS |-> IF q = p THEN d ELSE 0]
CUnit(p, d) == [q \in CPS |-> IF q = p THEN d ELSE 0]
PPlus(F, G) == [q \in PPS |-> F[q] + G[q]]
CPlus(F, G) == [q \in CPS |-> F[q] + G[q]]
AddP(w, D) == pledger' = [pledger EXCEPT ![w] = PPlus(@, D)]
AddC(w, D) == cledger' = [cledger EXCEPT ![w] = CPlus(@, D)]

Live(w) == ~success[w]
CanRun(w, o) == Live(w) /\ ~done[w][o]
Committed(w) == pledger[w] = PZero /\ cledger[w] = CZero

Init ==
    /\ pmsgs = [e \in {"ein", "eout"} |-> [w \in Workers |-> [t \in Times |-> 0]]]
    /\ cmsgs = [e \in {"c1", "c2", "c3", "c4", "c5"} |-> [w \in Workers |-> [c \in CTimes |-> 0]]]
    /\ incap = [w \in Workers |-> [t \in Times |-> IF t = 0 THEN 1 ELSE 0]]
    /\ ncap = [w \in Workers |-> [c \in CTimes |-> 0]]
    /\ nstash = [w \in Workers |-> [c \in CTimes |-> 0]]
    /\ ucap = [w \in Workers |-> [t \in Times |-> 0]]
    /\ ustash = [w \in Workers |-> [t \in Times |-> 0]]
    /\ sends = 0
    /\ laps = 0
    /\ pledger = [w \in Workers |-> PZero]
    /\ cledger = [w \in Workers |-> CZero]
    /\ pview = [w \in Workers |-> [p \in PPS |->
                  IF p = <<POut("in"), 0>> THEN Cardinality(Workers) ELSE 0]]
    /\ cview = [w \in Workers |-> CZero]
    /\ lane = [s \in Workers |-> [r \in Workers |-> <<>>]]
    /\ done = [w \in Workers |-> [o \in Ops |-> FALSE]]
    /\ nnotified = [w \in Workers |-> {}]
    /\ unotified = [w \in Workers |-> {}]
    /\ sealed = [w \in Workers |-> FALSE]
    /\ sealsSeen = [w \in Workers |-> {}]
    /\ success = [w \in Workers |-> FALSE]
    /\ residual = FALSE

---------------------------------------------------------------------------
(* Parent operators *)

InputSend(w) ==
    /\ CanRun(w, "in") /\ Committed(w) /\ sends < MaxSends
    /\ \E t \in Times :
         /\ incap[w][t] > 0
         /\ pmsgs' = [pmsgs EXCEPT !["ein"][w][t] = @ + 1]
         /\ AddP(w, PUnit(<<PTgt("ein"), t>>, 1))
    /\ sends' = sends + 1
    /\ UNCHANGED <<cmsgs, incap, ncap, nstash, ucap, ustash, laps, cledger, pview, cview, lane, done, nnotified, unotified, sealed, sealsSeen, success, residual>>

InputAdvance(w) ==
    /\ CanRun(w, "in") /\ Committed(w)
    /\ \E t \in Times :
         /\ incap[w][t] > 0
         /\ t = MaxT => ~KeepOpen
         /\ IF t < MaxT
              THEN /\ incap' = [incap EXCEPT ![w][t] = 0, ![w][t + 1] = 1]
                   /\ AddP(w, PPlus(PUnit(<<POut("in"), t>>, -1), PUnit(<<POut("in"), t + 1>>, 1)))
              ELSE /\ incap' = [incap EXCEPT ![w][t] = 0]
                   /\ AddP(w, PUnit(<<POut("in"), t>>, -1))
    /\ UNCHANGED <<pmsgs, cmsgs, ncap, nstash, ucap, ustash, sends, laps, cledger, pview, cview, lane, done, nnotified, unotified, sealed, sealsSeen, success, residual>>

\* Enter: consume at the parent's Tgt(ein), produce at the scope's Tgt(c1) at (t, 0).
Enter(w) ==
    /\ CanRun(w, "ent") /\ Committed(w)
    /\ \E t \in Times :
         /\ pmsgs["ein"][w][t] > 0
         /\ pmsgs' = [pmsgs EXCEPT !["ein"][w][t] = @ - 1]
         /\ cmsgs' = [cmsgs EXCEPT !["c1"][w][<<t, 0>>] = @ + 1]
         /\ AddP(w, PUnit(<<PTgt("ein"), t>>, -1))
         /\ AddC(w, CUnit(<<CTgt("c1"), t, 0>>, 1))
    /\ UNCHANGED <<incap, ncap, nstash, ucap, ustash, sends, laps, pview, cview, lane, done, nnotified, unotified, sealed, sealsSeen, success, residual>>

\* Leave: consume at the scope's exit, produce on the parent's eout at the outer time.
Leave(w) ==
    /\ CanRun(w, "lv") /\ Committed(w)
    /\ \E c \in CTimes :
         /\ cmsgs["c5"][w][c] > 0
         /\ cmsgs' = [cmsgs EXCEPT !["c5"][w][c] = @ - 1]
         /\ pmsgs' = [pmsgs EXCEPT !["eout"][w][c[1]] = @ + 1]
         /\ AddC(w, CUnit(<<CTgt("c5"), c[1], c[2]>>, -1))
         /\ AddP(w, PUnit(<<PTgt("eout"), c[1]>>, 1))
    /\ UNCHANGED <<incap, ncap, nstash, ucap, ustash, sends, laps, pview, cview, lane, done, nnotified, unotified, sealed, sealsSeen, success, residual>>

\* The count after the loop.
UTake(w) ==
    /\ CanRun(w, "u") /\ Committed(w)
    /\ \E t \in Times :
         /\ pmsgs["eout"][w][t] > 0
         /\ pmsgs' = [pmsgs EXCEPT !["eout"][w][t] = @ - 1]
         /\ ustash' = [ustash EXCEPT ![w][t] = @ + 1]
         /\ IF ucap[w][t] = 0
              THEN /\ ucap' = [ucap EXCEPT ![w][t] = 1]
                   /\ AddP(w, PPlus(PUnit(<<PTgt("eout"), t>>, -1), PUnit(<<POut("u"), t>>, 1)))
              ELSE /\ ucap' = ucap
                   /\ AddP(w, PUnit(<<PTgt("eout"), t>>, -1))
    /\ UNCHANGED <<cmsgs, incap, ncap, nstash, sends, laps, cledger, pview, cview, lane, done, nnotified, unotified, sealed, sealsSeen, success, residual>>

UFire(w) ==
    /\ CanRun(w, "u") /\ Committed(w)
    /\ \E t \in Times :
         /\ ucap[w][t] > 0
         /\ ~PWitness(w, PTgt("eout"), t)
         /\ unotified' = [unotified EXCEPT ![w] = @ \cup {t}]
         /\ ucap' = [ucap EXCEPT ![w][t] = 0]
         /\ ustash' = [ustash EXCEPT ![w][t] = 0]
         /\ AddP(w, PUnit(<<POut("u"), t>>, -1))
    /\ UNCHANGED <<pmsgs, cmsgs, incap, ncap, nstash, sends, laps, cledger, pview, cview, lane, done, nnotified, sealed, sealsSeen, success, residual>>

---------------------------------------------------------------------------
(* Operators inside the scope *)

\* The count inside the loop, per (time, iteration); it has two inputs.
NTake(w) ==
    /\ CanRun(w, "n") /\ Committed(w)
    /\ \E e \in {"c1", "c4"}, c \in CTimes :
         /\ cmsgs[e][w][c] > 0
         /\ cmsgs' = [cmsgs EXCEPT ![e][w][c] = @ - 1]
         /\ nstash' = [nstash EXCEPT ![w][c] = @ + 1]
         /\ IF ncap[w][c] = 0
              THEN /\ ncap' = [ncap EXCEPT ![w][c] = 1]
                   /\ AddC(w, CPlus(CUnit(<<CTgt(e), c[1], c[2]>>, -1), CUnit(<<COut("n"), c[1], c[2]>>, 1)))
              ELSE /\ ncap' = ncap
                   /\ AddC(w, CUnit(<<CTgt(e), c[1], c[2]>>, -1))
    /\ UNCHANGED <<pmsgs, incap, ucap, ustash, sends, laps, pledger, pview, cview, lane, done, nnotified, unotified, sealed, sealsSeen, success, residual>>

NFire(w) ==
    /\ CanRun(w, "n") /\ Committed(w)
    /\ \E c \in CTimes :
         /\ ncap[w][c] > 0
         /\ ~CWitness(w, CTgt("c1"), c[1], c[2])
         /\ ~CWitness(w, CTgt("c4"), c[1], c[2])
         /\ nnotified' = [nnotified EXCEPT ![w] = @ \cup {c}]
         /\ ncap' = [ncap EXCEPT ![w][c] = 0]
         /\ nstash' = [nstash EXCEPT ![w][c] = 0]
         /\ cmsgs' = [cmsgs EXCEPT !["c2"][w][c] = @ + 1]
         /\ AddC(w, CPlus(CUnit(<<CTgt("c2"), c[1], c[2]>>, 1), CUnit(<<COut("n"), c[1], c[2]>>, -1)))
    /\ UNCHANGED <<pmsgs, incap, ucap, ustash, sends, laps, pledger, pview, cview, lane, done, unotified, sealed, sealsSeen, success, residual>>

\* The branch: around the loop (one lap) or out of it.
Body(w) ==
    /\ CanRun(w, "body") /\ Committed(w)
    /\ \E c \in CTimes, around \in BOOLEAN, d \in Workers :
         /\ cmsgs["c2"][w][c] > 0
         /\ around => (laps < MaxLaps /\ c[2] < MaxIter)
         /\ LET e == IF around THEN "c3" ELSE "c5"
                to == IF around THEN d ELSE w
            IN /\ cmsgs' = [cmsgs EXCEPT !["c2"][w][c] = @ - 1, ![e][to][c] = @ + 1]
               /\ AddC(w, CPlus(CUnit(<<CTgt("c2"), c[1], c[2]>>, -1), CUnit(<<CTgt(e), c[1], c[2]>>, 1)))
         /\ laps' = IF around THEN laps + 1 ELSE laps
    /\ UNCHANGED <<pmsgs, incap, ncap, nstash, ucap, ustash, sends, pledger, pview, cview, lane, done, nnotified, unotified, sealed, sealsSeen, success, residual>>

\* Feedback: the record comes back to n at the next iteration.
Feedback(w) ==
    /\ CanRun(w, "fb") /\ Committed(w)
    /\ \E c \in CTimes :
         /\ cmsgs["c3"][w][c] > 0
         /\ c[2] < MaxIter
         /\ LET nx == <<c[1], c[2] + 1>> IN
              /\ cmsgs' = [cmsgs EXCEPT !["c3"][w][c] = @ - 1, !["c4"][w][nx] = @ + 1]
              /\ AddC(w, CPlus(CUnit(<<CTgt("c3"), c[1], c[2]>>, -1), CUnit(<<CTgt("c4"), nx[1], nx[2]>>, 1)))
    /\ UNCHANGED <<pmsgs, incap, ncap, nstash, ucap, ustash, sends, laps, pledger, pview, cview, lane, done, nnotified, unotified, sealed, sealsSeen, success, residual>>

---------------------------------------------------------------------------
(* Commits: both levels in one batch. A batch may be split into two         *)
(* fragments; peers apply each fragment on its own, in order.              *)

PPos(F) == [q \in PPS |-> IF F[q] > 0 THEN F[q] ELSE 0]
PNeg(F) == [q \in PPS |-> IF F[q] < 0 THEN F[q] ELSE 0]
CPos(F) == [q \in CPS |-> IF F[q] > 0 THEN F[q] ELSE 0]
CNeg(F) == [q \in CPS |-> IF F[q] < 0 THEN F[q] ELSE 0]
Upd(pp, cc) == [kind |-> "upd", p |-> pp, c |-> cc]

Encodings(pl, cl) ==
    LET Two(a, b) == SelectSeq(<<a, b>>, LAMBDA m : m.p # PZero \/ m.c # CZero)
        whole == <<Upd(pl, cl)>>
    IN CASE Frag = "none"           -> {whole}
         [] Frag = "positivesfirst" -> {whole, Two(Upd(PPos(pl), CPos(cl)), Upd(PNeg(pl), CNeg(cl)))}
         [] Frag = "parentfirst"    -> {whole, Two(Upd(pl, CZero), Upd(PZero, cl))}
         [] Frag = "childfirst"     -> {whole, Two(Upd(PZero, cl), Upd(pl, CZero))}

Commit(w) ==
    /\ Live(w) /\ ~Committed(w)
    /\ pview' = [pview EXCEPT ![w] = PPlus(@, pledger[w])]
    /\ cview' = [cview EXCEPT ![w] = CPlus(@, cledger[w])]
    /\ \E enc \in Encodings(pledger[w], cledger[w]) :
         lane' = [s \in Workers |-> [r \in Workers |->
                    IF s = w /\ r # w THEN lane[s][r] \o enc ELSE lane[s][r]]]
    /\ pledger' = [pledger EXCEPT ![w] = PZero]
    /\ cledger' = [cledger EXCEPT ![w] = CZero]
    /\ UNCHANGED <<pmsgs, cmsgs, incap, ncap, nstash, ucap, ustash, sends, laps, done, nnotified, unotified, sealed, sealsSeen, success, residual>>

Receive(s, r) ==
    /\ s # r /\ lane[s][r] # <<>>
    /\ LET m == Head(lane[s][r]) IN
         IF success[r] THEN UNCHANGED <<pview, cview, sealsSeen>>
         ELSE IF m.kind = "seal"
                THEN /\ sealsSeen' = [sealsSeen EXCEPT ![r] = @ \cup {s}]
                     /\ UNCHANGED <<pview, cview>>
                ELSE /\ pview' = [pview EXCEPT ![r] = PPlus(@, m.p)]
                     /\ cview' = [cview EXCEPT ![r] = CPlus(@, m.c)]
                     /\ UNCHANGED sealsSeen
    /\ lane' = [lane EXCEPT ![s][r] = Tail(@)]
    /\ UNCHANGED <<pmsgs, cmsgs, incap, ncap, nstash, ucap, ustash, sends, laps, pledger, cledger, done, nnotified, unotified, sealed, success, residual>>

---------------------------------------------------------------------------
(* S2a shutdown, seals and exact-zero success *)

Idle(w, o) ==
    CASE o = "in" -> \A t \in Times : incap[w][t] = 0
      [] o = "n"  -> \A c \in CTimes : ncap[w][c] = 0 /\ nstash[w][c] = 0
      [] o = "u"  -> \A t \in Times : ucap[w][t] = 0 /\ ustash[w][t] = 0
      [] OTHER    -> TRUE

NothingCanArrive(w, o) ==
    CASE o = "in"   -> TRUE
      [] o = "ent"  -> ~PWitness(w, PTgt("ein"), MaxT)
      [] o = "n"    -> ~CWitness(w, CTgt("c1"), MaxT, MaxIter) /\ ~CWitness(w, CTgt("c4"), MaxT, MaxIter)
      [] o = "body" -> ~CWitness(w, CTgt("c2"), MaxT, MaxIter)
      [] o = "fb"   -> ~CWitness(w, CTgt("c3"), MaxT, MaxIter)
      [] o = "lv"   -> ~CWitness(w, CExit, MaxT, MaxIter)
      [] o = "u"    -> ~PWitness(w, PTgt("eout"), MaxT)

Eligible(w, o) == CanRun(w, o) /\ Idle(w, o) /\ NothingCanArrive(w, o)

\* S2a. A worker shuts down every eligible operator in one step. Shutdowns
\* do not affect each other, and shutting down as early as possible is the
\* harshest timing for S3, so this loses no bad behaviour and keeps the
\* search small.
Shutdown(w) ==
    /\ Live(w) /\ Committed(w)
    /\ \E o \in Ops : Eligible(w, o)
    /\ done' = [done EXCEPT ![w] = [o \in Ops |-> @[o] \/ Eligible(w, o)]]
    /\ UNCHANGED <<pmsgs, cmsgs, incap, ncap, nstash, ucap, ustash, sends, laps, pledger, cledger, pview, cview, lane, nnotified, unotified, sealed, sealsSeen, success, residual>>

Seal(w) ==
    /\ Live(w) /\ ~sealed[w] /\ Committed(w)
    /\ \A o \in Ops : done[w][o]
    /\ sealed' = [sealed EXCEPT ![w] = TRUE]
    /\ lane' = [s \in Workers |-> [r \in Workers |->
                  IF s = w /\ r # w THEN Append(lane[s][r], [kind |-> "seal", p |-> PZero, c |-> CZero])
                  ELSE lane[s][r]]]
    /\ UNCHANGED <<pmsgs, cmsgs, incap, ncap, nstash, ucap, ustash, sends, laps, pledger, cledger, pview, cview, done, nnotified, unotified, sealsSeen, success, residual>>

ViewsZero(w) == pview[w] = PZero /\ cview[w] = CZero

Succeed(w) ==
    /\ Live(w) /\ sealed[w] /\ ViewsZero(w)
    /\ success' = [success EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<pmsgs, cmsgs, incap, ncap, nstash, ucap, ustash, sends, laps, pledger, cledger, pview, cview, lane, done, nnotified, unotified, sealed, sealsSeen, residual>>

ProvenResidual(w) ==
    /\ Live(w) /\ sealed[w] /\ ~residual
    /\ sealsSeen[w] = Workers \ {w}
    /\ ~ViewsZero(w)
    /\ residual' = TRUE
    /\ UNCHANGED <<pmsgs, cmsgs, incap, ncap, nstash, ucap, ustash, sends, laps, pledger, cledger, pview, cview, lane, done, nnotified, unotified, sealed, sealsSeen, success>>

AllSucceeded == \A w \in Workers : success[w]
Terminated == AllSucceeded /\ UNCHANGED vars

Next ==
    \/ \E w \in Workers :
         \/ InputSend(w) \/ InputAdvance(w) \/ Enter(w) \/ Leave(w) \/ UTake(w) \/ UFire(w)
         \/ NTake(w) \/ NFire(w) \/ Body(w) \/ Feedback(w)
         \/ Commit(w) \/ Seal(w) \/ Succeed(w) \/ ProvenResidual(w) \/ Shutdown(w)
    \/ \E s, r \in Workers : Receive(s, r)
    \/ Terminated

Spec == Init /\ [][Next]_vars

Fairness ==
    /\ \A w \in Workers :
         /\ WF_vars(InputAdvance(w)) /\ WF_vars(Enter(w)) /\ WF_vars(Leave(w))
         /\ WF_vars(UTake(w)) /\ WF_vars(UFire(w)) /\ WF_vars(NTake(w)) /\ WF_vars(NFire(w))
         /\ WF_vars(Body(w)) /\ WF_vars(Feedback(w)) /\ WF_vars(Commit(w))
         /\ WF_vars(Seal(w)) /\ WF_vars(Succeed(w)) /\ WF_vars(Shutdown(w))
    /\ \A s, r \in Workers : WF_vars(Receive(s, r))
LiveSpec == Spec /\ Fairness

Sym == Permutations(Workers)

---------------------------------------------------------------------------
(* Properties *)

\* Outstanding work inside the scope held by worker w.
CWorkW(w, l, t, i) ==
    IF l = COut("n") THEN ncap[w][<<t, i>>] + nstash[w][<<t, i>>]
    ELSE IF l[1] = "ctgt" THEN cmsgs[l[2]][w][<<t, i>>]
    ELSE 0
CBusy(l, t, i) == \E w \in Workers : CWorkW(w, l, t, i) > 0

\* Outstanding work in the parent; work inside the scope counts at Out(L).
PBusyW(w, l, t) ==
    CASE l = POut("in")   -> incap[w][t] > 0
      [] l = PTgt("ein")  -> pmsgs["ein"][w][t] > 0
      [] l = PTgt("eout") -> pmsgs["eout"][w][t] > 0
      [] l = POut("u")    -> ucap[w][t] + ustash[w][t] > 0
      [] l = POut("L")    -> \E cl \in CLocs, i \in Iters : CWorkW(w, cl, t, i) > 0
PBusy(l, t) == \E w \in Workers : PBusyW(w, l, t)

\* S1 in the scope: frontiers inside the loop never pass outstanding work.
ScopeSafe ==
    \A w \in Workers : Live(w) =>
        \A l \in CLocs, t \in Times, i \in Iters : CBusy(l, t, i) => CWitness(w, l, t, i)

\* S1 in the parent: frontiers after the loop never pass outstanding work.
ParentSafe ==
    \A w \in Workers : Live(w) =>
        \A l \in PLocs, t \in Times : PBusy(l, t) => PWitness(w, l, t)

\* Neither count ever receives a record at or before a time it reported.
NoLateData ==
    /\ \A w \in Workers : \A c \in nnotified[w] : \A e \in {"c1", "c4"}, x \in CTimes :
         (x[1] <= c[1] /\ x[2] <= c[2]) => cmsgs[e][w][x] = 0
    /\ \A w \in Workers : \A t \in unotified[w] : \A s \in Times : s <= t => pmsgs["eout"][w][s] = 0

DstOp == ("ein" :> "ent") @@ ("eout" :> "u")
CDstOp == ("c1" :> "n") @@ ("c2" :> "body") @@ ("c3" :> "fb") @@ ("c4" :> "n") @@ ("c5" :> "lv")
NoDataAtDone ==
    /\ \A e \in {"ein", "eout"}, w \in Workers, t \in Times : done[w][DstOp[e]] => pmsgs[e][w][t] = 0
    /\ \A e \in {"c1", "c2", "c3", "c4", "c5"}, w \in Workers, c \in CTimes : done[w][CDstOp[e]] => cmsgs[e][w][c] = 0

CompleteMeansEmpty ==
    AllSucceeded =>
        /\ \A l \in PLocs, t \in Times : ~PBusy(l, t)
        /\ \A l \in CLocs, t \in Times, i \in Iters : ~CBusy(l, t, i)

NoResidual == ~residual

\* Sanity (expected to be violated): a record went around the loop, both
\* counts reported, and the run completed.
NeverCompletesAfterLap ==
    ~(AllSucceeded /\ laps > 0
      /\ (\E w \in Workers : nnotified[w] # {}) /\ (\E w \in Workers : unotified[w] # {}))

EventuallyDone == <>AllSucceeded

\* With KeepOpen, both counts report every earlier epoch, including after a
\* lap, although the input never closes.
ScopeProgress ==
    /\ \A w \in Workers, c \in CTimes : c[1] < MaxT => ((nstash[w][c] > 0) ~> (c \in nnotified[w]))
    /\ \A w \in Workers, t \in Times : t < MaxT => ((ustash[w][t] > 0) ~> (t \in unotified[w]))
=============================================================================
