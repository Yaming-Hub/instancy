---------------------------- MODULE NestedScopes ----------------------------
(***************************************************************************)
(* Open item O1 of the decision log: a loop scope on an outer loop's       *)
(* feedback path (design doc §5.1.7). For TLC.                             *)
(*                                                                         *)
(* Three levels:                                                           *)
(*   root, in t:      in -pipe-> [ outer loop O ] -pipe-> u (a count)       *)
(*   O, in (t, j):    enter -> [ inner loop I ] -pipe-> bO (branch);        *)
(*                    bO -exchange-> fbO (feedback, j + 1) -> I's input;    *)
(*                    bO -pipe-> leave                                     *)
(*   I, in (t,j,k):   enter -> bI (branch) -pipe-> fbI (feedback, k + 1)    *)
(*                    -> bI; bI -pipe-> leave                              *)
(* Every lap of O passes through I, so I lies on O's feedback path.        *)
(*                                                                         *)
(* Each worker keeps one view of raw port counts per level, and caches the *)
(* four derived links between applies, as an incremental tracker does:     *)
(*  - impO, impI: imports, the parent's frontier at the scope's input;     *)
(*  - expI, expO: exports, the scope's internal work in the parent's time. *)
(* Every apply (the worker's own commit, or a peer's batch) recomputes them *)
(* in one pass: exports bottom-up from the new views, then imports         *)
(* top-down. Derived links are never sent, and exact zero reads raw counts *)
(* only (R38).                                                             *)
(*                                                                         *)
(* Export = "frontier" is the mutation the design warns about: the inner    *)
(* scope I exports its frontier at its exit, which includes its import. On *)
(* O's feedback path, I's export then supports O's frontier at I's input   *)
(* one iteration later, which is I's next import: support that climbs one  *)
(* iteration per pass and, with unbounded iterations, never ends. With     *)
(* Sat = TRUE, O's feedback summary saturates at MaxJ, which turns that    *)
(* endless climb into a fixed point TLC can see. Saturation only adds      *)
(* paths, so it can make a run hang but never makes one finish early;      *)
(* safety is also checked with Sat = FALSE.                                *)
(*                                                                         *)
(* As in LoopScopes.tla, a worker commits each activation before its next  *)
(* one, and shuts down all its eligible operators in one step.             *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Workers,
    MaxT,       \* root timestamps are 0..MaxT
    MaxJ,       \* outer-loop iterations are 0..MaxJ
    MaxK,       \* inner-loop iterations are 0..MaxK
    MaxSends,   \* records the inputs may send in total, over all workers
    LapsO,      \* outer laps in total
    LapsI,      \* inner laps in total
    Export,     \* "internal" (PCP) or "frontier" (mutation)
    Sat,        \* TRUE: O's feedback summary saturates at MaxJ
    Frag,       \* "none", or "positivesfirst": a commit may be split in two
    KeepOpen,   \* TRUE: the input never closes after the last epoch
    Track       \* TRUE: record which paths were taken (sanity scenarios only)

Times == 0..MaxT
Js == 0..MaxJ
Ks == 0..MaxK
OT == Times \X Js
IT == Times \X Js \X Ks
Ops == {"in", "eO", "eI", "bI", "fbI", "lI", "bO", "fbO", "lO", "u"}

\* The iteration O's feedback leads to.
NextJ(j) == IF Sat /\ j = MaxJ THEN MaxJ ELSE j + 1

---------------------------------------------------------------------------
(* Locations. Raw ones hold port counts; SO.out, BO, SI.out and BI hold     *)
(* only derived support (exports and imports).                             *)

RRaw == {"in.out", "eO.in", "u.in", "u.out"}
RLocs == RRaw \cup {"SO.out"}
RSucc(l) ==
    CASE l = "in.out" -> {"eO.in"}
      [] l = "eO.in"  -> {"SO.out"}
      [] l = "SO.out" -> {"u.in"}
      [] l = "u.in"   -> {"u.out"}
      [] l = "u.out"  -> {}

ORaw == {"TI", "bO.in", "fbO.in", "lO.in"}
OLocs == ORaw \cup {"BO", "SI.out", "bO.out", "fbO.out"}
OSucc0(l) ==
    CASE l = "BO"      -> {"TI"}
      [] l = "TI"      -> {"SI.out"}
      [] l = "SI.out"  -> {"bO.in"}
      [] l = "bO.in"   -> {"bO.out"}
      [] l = "bO.out"  -> {"fbO.in", "lO.in"}
      [] l = "fbO.in"  -> {"fbO.out"}
      [] l = "fbO.out" -> {}
      [] l = "lO.in"   -> {}
OSuccAll(l) == IF l = "fbO.out" THEN {"TI"} ELSE OSucc0(l)

IRaw == {"bI.in", "fbI.in", "lI.in"}
ILocs == IRaw \cup {"BI", "bI.out", "fbI.out"}
ISucc0(l) ==
    CASE l = "BI"      -> {"bI.in"}
      [] l = "bI.in"   -> {"bI.out"}
      [] l = "bI.out"  -> {"fbI.in", "lI.in"}
      [] l = "fbI.in"  -> {"fbI.out"}
      [] l = "fbI.out" -> {}
      [] l = "lI.in"   -> {}
ISuccAll(l) == IF l = "fbI.out" THEN {"bI.in"} ELSE ISucc0(l)

RECURSIVE RClosure(_, _), OClosure0(_, _), OClosureAll(_, _), IClosure0(_, _), IClosureAll(_, _)
RClosure(S, n) == IF n = 0 THEN S ELSE RClosure(S \cup UNION {RSucc(l) : l \in S}, n - 1)
OClosure0(S, n) == IF n = 0 THEN S ELSE OClosure0(S \cup UNION {OSucc0(l) : l \in S}, n - 1)
OClosureAll(S, n) == IF n = 0 THEN S ELSE OClosureAll(S \cup UNION {OSuccAll(l) : l \in S}, n - 1)
IClosure0(S, n) == IF n = 0 THEN S ELSE IClosure0(S \cup UNION {ISucc0(l) : l \in S}, n - 1)
IClosureAll(S, n) == IF n = 0 THEN S ELSE IClosureAll(S \cup UNION {ISuccAll(l) : l \in S}, n - 1)

\* Locations upstream of l: without a feedback step (summary +0), or at all.
RUp == [l \in RLocs |-> {a \in RLocs : l \in RClosure({a}, Cardinality(RLocs))}]
OUp0 == [l \in OLocs |-> {a \in OLocs : l \in OClosure0({a}, Cardinality(OLocs))}]
OUpAll == [l \in OLocs |-> {a \in OLocs : l \in OClosureAll({a}, Cardinality(OLocs))}]
IUp0 == [l \in ILocs |-> {a \in ILocs : l \in IClosure0({a}, Cardinality(ILocs))}]
IUpAll == [l \in ILocs |-> {a \in ILocs : l \in IClosureAll({a}, Cardinality(ILocs))}]

\* Without the feedback steps, neither scope's graph has a cycle.
ASSUME \A a \in OLocs : a \notin OClosure0(OSucc0(a), Cardinality(OLocs))
ASSUME \A a \in ILocs : a \notin IClosure0(ISucc0(a), Cardinality(ILocs))
\* Every raw location inside a scope can reach its exit.
ASSUME \A a \in ORaw : "lO.in" \in OClosureAll({a}, Cardinality(OLocs))
ASSUME \A a \in IRaw : "lI.in" \in IClosureAll({a}, Cardinality(ILocs))

RPS == RRaw \X Times
OPS == ORaw \X Times \X Js
IPS == IRaw \X Times \X Js \X Ks
RZero == [p \in RPS |-> 0]
OZero == [p \in OPS |-> 0]
IZero == [p \in IPS |-> 0]

---------------------------------------------------------------------------
(* Frontiers, from a view of raw counts plus the cached derived links. A   *)
(* witness at (l, time) means the frontier at l is at or before that time. *)

RWc(rv, eO, l, t) ==
    \/ \E a \in RUp[l] \cap RRaw : \E s \in 0..t : rv[<<a, s>>] > 0
    \/ /\ "SO.out" \in RUp[l]
       /\ \E s \in eO : s <= t

OWc(ov, eI, iO, l, t, j) ==
    \/ \E a \in OUp0[l] \cap ORaw : \E s \in 0..t, i \in 0..j : ov[<<a, s, i>>] > 0
    \/ \E a \in OUpAll[l] \cap ORaw : \E s \in 0..t, i \in Js : NextJ(i) <= j /\ ov[<<a, s, i>>] > 0
    \/ \E e \in eI : /\ e[1] <= t
                     /\ \/ "SI.out" \in OUp0[l] /\ e[2] <= j
                        \/ "SI.out" \in OUpAll[l] /\ NextJ(e[2]) <= j
    \/ /\ "BO" \in OUp0[l]
       /\ \E s \in iO : s <= t

IWc(iv, iI, l, t, j, k) ==
    \/ \E a \in IUp0[l] \cap IRaw : \E s \in 0..t, i \in 0..j, q \in 0..k : iv[<<a, s, i, q>>] > 0
    \/ /\ k >= 1
       /\ \E a \in IUpAll[l] \cap IRaw : \E s \in 0..t, i \in 0..j, q \in 0..(k - 1) : iv[<<a, s, i, q>>] > 0
    \/ /\ "BI" \in IUp0[l]
       /\ \E e \in iI : e[1] <= t /\ e[2] <= j

\* One pass over the derived links. Exports first, bottom-up: I's internal
\* work, then O's internal work including I's export. Then imports, top-down.
\* Only the mutation reads the previous imports, and only in I, the scope on
\* O's feedback path.
ExpIc(iv, iIold) ==
    {e \in OT : \E a \in IRaw, k \in Ks : iv[<<a, e[1], e[2], k>>] > 0}
        \cup (IF Export = "frontier" THEN iIold ELSE {})
ExpOc(ov, eI) ==
    {t \in Times : \/ \E a \in ORaw, j \in Js : ov[<<a, t, j>>] > 0
                   \/ \E j \in Js : <<t, j>> \in eI}
Derive(rv, ov, iv, iIold) ==
    LET eI == ExpIc(iv, iIold)
        eO == ExpOc(ov, eI)
        iO == {t \in Times : RWc(rv, eO, "eO.in", t)}
        iI == {c \in OT : OWc(ov, eI, iO, "TI", c[1], c[2])}
    IN [eI |-> eI, eO |-> eO, iO |-> iO, iI |-> iI]

RView0 == [p \in RPS |-> IF p = <<"in.out", 0>> THEN Cardinality(Workers) ELSE 0]
Derived0 == Derive(RView0, OZero, IZero, {})

---------------------------------------------------------------------------
VARIABLES
    rmsgs,      \* [{"eO.in", "u.in"} -> [Workers -> [Times -> Nat]]]
    omsgs,      \* [ORaw -> [Workers -> [OT -> Nat]]]
    imsgs,      \* [IRaw -> [Workers -> [IT -> Nat]]]
    incap,      \* [Workers -> [Times -> Nat]]: input capabilities
    ucap,       \* [Workers -> [Times -> Nat]]: the count's capabilities
    ustash,     \* [Workers -> [Times -> Nat]]
    sends, olaps, ilaps,
    rledger, oledger, iledger,  \* uncommitted port counts at each level
    rview, oview, iview,        \* each worker's raw views at each level
    expI, expO, impO, impI,     \* each worker's cached derived links
    lane,       \* [Workers -> [Workers -> Seq(message)]]
    done,       \* [Workers -> [Ops -> BOOLEAN]]
    unotified,  \* [Workers -> SUBSET Times]
    sealed, sealsSeen, success, residual,
    hist        \* with Track: paths taken, for the sanity scenario

msgVars == <<rmsgs, omsgs, imsgs>>
opVars == <<incap, ucap, ustash, sends, olaps, ilaps, unotified, hist>>
ledgerVars == <<rledger, oledger, iledger>>
viewVars == <<rview, oview, iview>>
derivedVars == <<expI, expO, impO, impI>>
protoVars == <<lane, done, sealed, sealsSeen, success, residual>>
vars == <<msgVars, opVars, ledgerVars, viewVars, derivedVars, protoVars>>

\* Frontiers in worker w's current view.
RW(w, l, t) == RWc(rview[w], expO[w], l, t)
OW(w, l, t, j) == OWc(oview[w], expI[w], impO[w], l, t, j)
IW(w, l, t, j, k) == IWc(iview[w], impI[w], l, t, j, k)

---------------------------------------------------------------------------
(* Helpers *)

RUnit(p, d) == [q \in RPS |-> IF q = p THEN d ELSE 0]
OUnit(p, d) == [q \in OPS |-> IF q = p THEN d ELSE 0]
IUnit(p, d) == [q \in IPS |-> IF q = p THEN d ELSE 0]
RPlus(F, G) == [q \in RPS |-> F[q] + G[q]]
OPlus(F, G) == [q \in OPS |-> F[q] + G[q]]
IPlus(F, G) == [q \in IPS |-> F[q] + G[q]]
AddR(w, D) == rledger' = [rledger EXCEPT ![w] = RPlus(@, D)]
AddO(w, D) == oledger' = [oledger EXCEPT ![w] = OPlus(@, D)]
AddI(w, D) == iledger' = [iledger EXCEPT ![w] = IPlus(@, D)]

SetDerived(w, d) ==
    /\ expI' = [expI EXCEPT ![w] = d.eI]
    /\ expO' = [expO EXCEPT ![w] = d.eO]
    /\ impO' = [impO EXCEPT ![w] = d.iO]
    /\ impI' = [impI EXCEPT ![w] = d.iI]

Live(w) == ~success[w]
CanRun(w, o) == Live(w) /\ ~done[w][o]
Committed(w) == rledger[w] = RZero /\ oledger[w] = OZero /\ iledger[w] = IZero

Init ==
    /\ rmsgs = [e \in {"eO.in", "u.in"} |-> [w \in Workers |-> [t \in Times |-> 0]]]
    /\ omsgs = [e \in ORaw |-> [w \in Workers |-> [c \in OT |-> 0]]]
    /\ imsgs = [e \in IRaw |-> [w \in Workers |-> [x \in IT |-> 0]]]
    /\ incap = [w \in Workers |-> [t \in Times |-> IF t = 0 THEN 1 ELSE 0]]
    /\ ucap = [w \in Workers |-> [t \in Times |-> 0]]
    /\ ustash = [w \in Workers |-> [t \in Times |-> 0]]
    /\ sends = 0 /\ olaps = 0 /\ ilaps = 0
    /\ rledger = [w \in Workers |-> RZero]
    /\ oledger = [w \in Workers |-> OZero]
    /\ iledger = [w \in Workers |-> IZero]
    /\ rview = [w \in Workers |-> RView0]
    /\ oview = [w \in Workers |-> OZero]
    /\ iview = [w \in Workers |-> IZero]
    /\ expI = [w \in Workers |-> Derived0.eI]
    /\ expO = [w \in Workers |-> Derived0.eO]
    /\ impO = [w \in Workers |-> Derived0.iO]
    /\ impI = [w \in Workers |-> Derived0.iI]
    /\ lane = [s \in Workers |-> [r \in Workers |-> <<>>]]
    /\ done = [w \in Workers |-> [o \in Ops |-> FALSE]]
    /\ unotified = [w \in Workers |-> {}]
    /\ sealed = [w \in Workers |-> FALSE]
    /\ sealsSeen = [w \in Workers |-> {}]
    /\ success = [w \in Workers |-> FALSE]
    /\ residual = FALSE
    /\ hist = {}

---------------------------------------------------------------------------
(* Root operators *)

InputSend(w) ==
    /\ CanRun(w, "in") /\ Committed(w) /\ sends < MaxSends
    /\ \E t \in Times :
         /\ incap[w][t] > 0
         /\ rmsgs' = [rmsgs EXCEPT !["eO.in"][w][t] = @ + 1]
         /\ AddR(w, RUnit(<<"eO.in", t>>, 1))
    /\ sends' = sends + 1
    /\ UNCHANGED <<omsgs, imsgs, oledger, iledger, incap, ucap, ustash, olaps, ilaps, unotified, hist, viewVars, derivedVars, protoVars>>

InputAdvance(w) ==
    /\ CanRun(w, "in") /\ Committed(w)
    /\ \E t \in Times :
         /\ incap[w][t] > 0
         /\ t = MaxT => ~KeepOpen
         /\ IF t < MaxT
              THEN /\ incap' = [incap EXCEPT ![w][t] = 0, ![w][t + 1] = 1]
                   /\ AddR(w, RPlus(RUnit(<<"in.out", t>>, -1), RUnit(<<"in.out", t + 1>>, 1)))
              ELSE /\ incap' = [incap EXCEPT ![w][t] = 0]
                   /\ AddR(w, RUnit(<<"in.out", t>>, -1))
    /\ UNCHANGED <<msgVars, oledger, iledger, ucap, ustash, sends, olaps, ilaps, unotified, hist, viewVars, derivedVars, protoVars>>

\* O's enter: consume at the root's eO.in, produce at O's TI at (t, 0).
EnterO(w) ==
    /\ CanRun(w, "eO") /\ Committed(w)
    /\ \E t \in Times :
         /\ rmsgs["eO.in"][w][t] > 0
         /\ rmsgs' = [rmsgs EXCEPT !["eO.in"][w][t] = @ - 1]
         /\ omsgs' = [omsgs EXCEPT !["TI"][w][<<t, 0>>] = @ + 1]
         /\ AddR(w, RUnit(<<"eO.in", t>>, -1))
         /\ AddO(w, OUnit(<<"TI", t, 0>>, 1))
    /\ UNCHANGED <<imsgs, iledger, opVars, viewVars, derivedVars, protoVars>>

\* O's leave: consume at O's exit, produce at the root's u.in at the outer time.
LeaveO(w) ==
    /\ CanRun(w, "lO") /\ Committed(w)
    /\ \E c \in OT :
         /\ omsgs["lO.in"][w][c] > 0
         /\ omsgs' = [omsgs EXCEPT !["lO.in"][w][c] = @ - 1]
         /\ rmsgs' = [rmsgs EXCEPT !["u.in"][w][c[1]] = @ + 1]
         /\ AddO(w, OUnit(<<"lO.in", c[1], c[2]>>, -1))
         /\ AddR(w, RUnit(<<"u.in", c[1]>>, 1))
    /\ UNCHANGED <<imsgs, iledger, opVars, viewVars, derivedVars, protoVars>>

\* The count after the outer loop.
UTake(w) ==
    /\ CanRun(w, "u") /\ Committed(w)
    /\ \E t \in Times :
         /\ rmsgs["u.in"][w][t] > 0
         /\ rmsgs' = [rmsgs EXCEPT !["u.in"][w][t] = @ - 1]
         /\ ustash' = [ustash EXCEPT ![w][t] = @ + 1]
         /\ IF ucap[w][t] = 0
              THEN /\ ucap' = [ucap EXCEPT ![w][t] = 1]
                   /\ AddR(w, RPlus(RUnit(<<"u.in", t>>, -1), RUnit(<<"u.out", t>>, 1)))
              ELSE /\ ucap' = ucap
                   /\ AddR(w, RUnit(<<"u.in", t>>, -1))
    /\ UNCHANGED <<omsgs, imsgs, oledger, iledger, incap, sends, olaps, ilaps, unotified, hist, viewVars, derivedVars, protoVars>>

UFire(w) ==
    /\ CanRun(w, "u") /\ Committed(w)
    /\ \E t \in Times :
         /\ ucap[w][t] > 0
         /\ ~RW(w, "u.in", t)
         /\ unotified' = [unotified EXCEPT ![w] = @ \cup {t}]
         /\ ucap' = [ucap EXCEPT ![w][t] = 0]
         /\ ustash' = [ustash EXCEPT ![w][t] = 0]
         /\ AddR(w, RUnit(<<"u.out", t>>, -1))
    /\ UNCHANGED <<msgVars, oledger, iledger, incap, sends, olaps, ilaps, hist, viewVars, derivedVars, protoVars>>

---------------------------------------------------------------------------
(* Operators of the outer loop O *)

\* I's enter: consume at O's TI, produce at I's bI.in at (t, j, 0).
EnterI(w) ==
    /\ CanRun(w, "eI") /\ Committed(w)
    /\ \E c \in OT :
         /\ omsgs["TI"][w][c] > 0
         /\ omsgs' = [omsgs EXCEPT !["TI"][w][c] = @ - 1]
         /\ imsgs' = [imsgs EXCEPT !["bI.in"][w][<<c[1], c[2], 0>>] = @ + 1]
         /\ AddO(w, OUnit(<<"TI", c[1], c[2]>>, -1))
         /\ AddI(w, IUnit(<<"bI.in", c[1], c[2], 0>>, 1))
         /\ hist' = IF Track /\ c[2] >= 1 THEN hist \cup {"reenter"} ELSE hist
    /\ UNCHANGED <<rmsgs, rledger, incap, ucap, ustash, sends, olaps, ilaps, unotified, viewVars, derivedVars, protoVars>>

\* The outer branch: around O (to any worker), or out of it.
BodyO(w) ==
    /\ CanRun(w, "bO") /\ Committed(w)
    /\ \E c \in OT, around \in BOOLEAN, d \in Workers :
         /\ omsgs["bO.in"][w][c] > 0
         /\ around => (olaps < LapsO /\ c[2] < MaxJ)
         /\ ~around => d = w
         /\ LET e == IF around THEN "fbO.in" ELSE "lO.in"
            IN /\ omsgs' = [omsgs EXCEPT !["bO.in"][w][c] = @ - 1, ![e][d][c] = @ + 1]
               /\ AddO(w, OPlus(OUnit(<<"bO.in", c[1], c[2]>>, -1), OUnit(<<e, c[1], c[2]>>, 1)))
         /\ olaps' = IF around THEN olaps + 1 ELSE olaps
         /\ hist' = IF Track /\ d # w THEN hist \cup {"xlap"} ELSE hist
    /\ UNCHANGED <<rmsgs, imsgs, rledger, iledger, incap, ucap, ustash, sends, ilaps, unotified, viewVars, derivedVars, protoVars>>

\* O's feedback: back to I's input at the next outer iteration.
FeedbackO(w) ==
    /\ CanRun(w, "fbO") /\ Committed(w)
    /\ \E c \in OT :
         /\ omsgs["fbO.in"][w][c] > 0
         /\ c[2] < MaxJ
         /\ LET nc == <<c[1], c[2] + 1>> IN
              /\ omsgs' = [omsgs EXCEPT !["fbO.in"][w][c] = @ - 1, !["TI"][w][nc] = @ + 1]
              /\ AddO(w, OPlus(OUnit(<<"fbO.in", c[1], c[2]>>, -1), OUnit(<<"TI", nc[1], nc[2]>>, 1)))
    /\ UNCHANGED <<rmsgs, imsgs, rledger, iledger, opVars, viewVars, derivedVars, protoVars>>

---------------------------------------------------------------------------
(* Operators of the inner loop I *)

BodyI(w) ==
    /\ CanRun(w, "bI") /\ Committed(w)
    /\ \E x \in IT, around \in BOOLEAN :
         /\ imsgs["bI.in"][w][x] > 0
         /\ around => (ilaps < LapsI /\ x[3] < MaxK)
         /\ LET e == IF around THEN "fbI.in" ELSE "lI.in"
            IN /\ imsgs' = [imsgs EXCEPT !["bI.in"][w][x] = @ - 1, ![e][w][x] = @ + 1]
               /\ AddI(w, IPlus(IUnit(<<"bI.in", x[1], x[2], x[3]>>, -1), IUnit(<<e, x[1], x[2], x[3]>>, 1)))
         /\ ilaps' = IF around THEN ilaps + 1 ELSE ilaps
    /\ UNCHANGED <<rmsgs, omsgs, rledger, oledger, incap, ucap, ustash, sends, olaps, unotified, hist, viewVars, derivedVars, protoVars>>

FeedbackI(w) ==
    /\ CanRun(w, "fbI") /\ Committed(w)
    /\ \E x \in IT :
         /\ imsgs["fbI.in"][w][x] > 0
         /\ x[3] < MaxK
         /\ LET nx == <<x[1], x[2], x[3] + 1>> IN
              /\ imsgs' = [imsgs EXCEPT !["fbI.in"][w][x] = @ - 1, !["bI.in"][w][nx] = @ + 1]
              /\ AddI(w, IPlus(IUnit(<<"fbI.in", x[1], x[2], x[3]>>, -1), IUnit(<<"bI.in", nx[1], nx[2], nx[3]>>, 1)))
    /\ UNCHANGED <<rmsgs, omsgs, rledger, oledger, opVars, viewVars, derivedVars, protoVars>>

\* I's leave: consume at I's exit, produce at O's bO.in at (t, j).
LeaveI(w) ==
    /\ CanRun(w, "lI") /\ Committed(w)
    /\ \E x \in IT :
         /\ imsgs["lI.in"][w][x] > 0
         /\ imsgs' = [imsgs EXCEPT !["lI.in"][w][x] = @ - 1]
         /\ omsgs' = [omsgs EXCEPT !["bO.in"][w][<<x[1], x[2]>>] = @ + 1]
         /\ AddI(w, IUnit(<<"lI.in", x[1], x[2], x[3]>>, -1))
         /\ AddO(w, OUnit(<<"bO.in", x[1], x[2]>>, 1))
    /\ UNCHANGED <<rmsgs, rledger, opVars, viewVars, derivedVars, protoVars>>

---------------------------------------------------------------------------
(* Commits: all three levels' raw counts in one batch, never derived links. *)
(* A batch may be split into two fragments, positives first.               *)

RPos(F) == [q \in RPS |-> IF F[q] > 0 THEN F[q] ELSE 0]
RNeg(F) == [q \in RPS |-> IF F[q] < 0 THEN F[q] ELSE 0]
OPos(F) == [q \in OPS |-> IF F[q] > 0 THEN F[q] ELSE 0]
ONeg(F) == [q \in OPS |-> IF F[q] < 0 THEN F[q] ELSE 0]
IPos(F) == [q \in IPS |-> IF F[q] > 0 THEN F[q] ELSE 0]
INeg(F) == [q \in IPS |-> IF F[q] < 0 THEN F[q] ELSE 0]
Upd(rr, oo, ii) == [kind |-> "upd", r |-> rr, o |-> oo, i |-> ii]
SealMsg == [kind |-> "seal", r |-> RZero, o |-> OZero, i |-> IZero]

Encodings(rl, ol, il) ==
    LET Two(a, b) == SelectSeq(<<a, b>>, LAMBDA m : m.r # RZero \/ m.o # OZero \/ m.i # IZero)
        whole == <<Upd(rl, ol, il)>>
    IN IF Frag = "none" THEN {whole}
       ELSE {whole, Two(Upd(RPos(rl), OPos(ol), IPos(il)), Upd(RNeg(rl), ONeg(ol), INeg(il)))}

\* Self-apply, recompute the derived links, and send to every peer.
Commit(w) ==
    /\ Live(w) /\ ~Committed(w)
    /\ LET rv == RPlus(rview[w], rledger[w])
           ov == OPlus(oview[w], oledger[w])
           iv == IPlus(iview[w], iledger[w])
       IN /\ rview' = [rview EXCEPT ![w] = rv]
          /\ oview' = [oview EXCEPT ![w] = ov]
          /\ iview' = [iview EXCEPT ![w] = iv]
          /\ SetDerived(w, Derive(rv, ov, iv, impI[w]))
    /\ \E enc \in Encodings(rledger[w], oledger[w], iledger[w]) :
         lane' = [s \in Workers |-> [r \in Workers |->
                    IF s = w /\ r # w THEN lane[s][r] \o enc ELSE lane[s][r]]]
    /\ rledger' = [rledger EXCEPT ![w] = RZero]
    /\ oledger' = [oledger EXCEPT ![w] = OZero]
    /\ iledger' = [iledger EXCEPT ![w] = IZero]
    /\ UNCHANGED <<msgVars, opVars, done, sealed, sealsSeen, success, residual>>

Receive(s, r) ==
    /\ s # r /\ lane[s][r] # <<>>
    /\ LET m == Head(lane[s][r]) IN
         IF success[r] THEN UNCHANGED <<viewVars, derivedVars, sealsSeen>>
         ELSE IF m.kind = "seal"
                THEN /\ sealsSeen' = [sealsSeen EXCEPT ![r] = @ \cup {s}]
                     /\ UNCHANGED <<viewVars, derivedVars>>
                ELSE LET rv == RPlus(rview[r], m.r)
                         ov == OPlus(oview[r], m.o)
                         iv == IPlus(iview[r], m.i)
                     IN /\ rview' = [rview EXCEPT ![r] = rv]
                        /\ oview' = [oview EXCEPT ![r] = ov]
                        /\ iview' = [iview EXCEPT ![r] = iv]
                        /\ SetDerived(r, Derive(rv, ov, iv, impI[r]))
                        /\ UNCHANGED sealsSeen
    /\ lane' = [lane EXCEPT ![s][r] = Tail(@)]
    /\ UNCHANGED <<msgVars, opVars, ledgerVars, done, sealed, success, residual>>

\* The next propagation pass (the executor propagates once per sweep, after
\* committing), with unchanged views. Under PCP one pass is exact, so this is
\* never enabled; the mutation keeps feeding I's import back into its export.
Repropagate(w) ==
    /\ Live(w) /\ Committed(w)
    /\ LET d == Derive(rview[w], oview[w], iview[w], impI[w]) IN
         /\ d # [eI |-> expI[w], eO |-> expO[w], iO |-> impO[w], iI |-> impI[w]]
         /\ SetDerived(w, d)
    /\ UNCHANGED <<msgVars, opVars, ledgerVars, viewVars, protoVars>>

---------------------------------------------------------------------------
(* S2a shutdown, seals and exact-zero success over raw counts *)

Idle(w, o) ==
    CASE o = "in" -> \A t \in Times : incap[w][t] = 0
      [] o = "u"  -> \A t \in Times : ucap[w][t] = 0 /\ ustash[w][t] = 0
      [] OTHER    -> TRUE

NothingCanArrive(w, o) ==
    CASE o = "in"  -> TRUE
      [] o = "eO"  -> ~RW(w, "eO.in", MaxT)
      [] o = "eI"  -> ~OW(w, "TI", MaxT, MaxJ)
      [] o = "bO"  -> ~OW(w, "bO.in", MaxT, MaxJ)
      [] o = "fbO" -> ~OW(w, "fbO.in", MaxT, MaxJ)
      [] o = "lO"  -> ~OW(w, "lO.in", MaxT, MaxJ)
      [] o = "bI"  -> ~IW(w, "bI.in", MaxT, MaxJ, MaxK)
      [] o = "fbI" -> ~IW(w, "fbI.in", MaxT, MaxJ, MaxK)
      [] o = "lI"  -> ~IW(w, "lI.in", MaxT, MaxJ, MaxK)
      [] o = "u"   -> ~RW(w, "u.in", MaxT)

Eligible(w, o) == CanRun(w, o) /\ Idle(w, o) /\ NothingCanArrive(w, o)

Shutdown(w) ==
    /\ Live(w) /\ Committed(w)
    /\ \E o \in Ops : Eligible(w, o)
    /\ done' = [done EXCEPT ![w] = [o \in Ops |-> @[o] \/ Eligible(w, o)]]
    /\ UNCHANGED <<msgVars, opVars, ledgerVars, viewVars, derivedVars, lane, sealed, sealsSeen, success, residual>>

Seal(w) ==
    /\ Live(w) /\ ~sealed[w] /\ Committed(w)
    /\ \A o \in Ops : done[w][o]
    /\ sealed' = [sealed EXCEPT ![w] = TRUE]
    /\ lane' = [s \in Workers |-> [r \in Workers |->
                  IF s = w /\ r # w THEN Append(lane[s][r], SealMsg) ELSE lane[s][r]]]
    /\ UNCHANGED <<msgVars, opVars, ledgerVars, viewVars, derivedVars, done, sealsSeen, success, residual>>

\* Exact zero reads raw counts only; derived links are not counts.
ViewsZero(w) == rview[w] = RZero /\ oview[w] = OZero /\ iview[w] = IZero

Succeed(w) ==
    /\ Live(w) /\ sealed[w] /\ ViewsZero(w)
    /\ success' = [success EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<msgVars, opVars, ledgerVars, viewVars, derivedVars, lane, done, sealed, sealsSeen, residual>>

ProvenResidual(w) ==
    /\ Live(w) /\ sealed[w] /\ ~residual
    /\ sealsSeen[w] = Workers \ {w}
    /\ ~ViewsZero(w)
    /\ residual' = TRUE
    /\ UNCHANGED <<msgVars, opVars, ledgerVars, viewVars, derivedVars, lane, done, sealed, sealsSeen, success>>

AllSucceeded == \A w \in Workers : success[w]
Terminated == AllSucceeded /\ UNCHANGED vars

Next ==
    \/ \E w \in Workers :
         \/ InputSend(w) \/ InputAdvance(w) \/ EnterO(w) \/ LeaveO(w) \/ UTake(w) \/ UFire(w)
         \/ EnterI(w) \/ BodyO(w) \/ FeedbackO(w) \/ BodyI(w) \/ FeedbackI(w) \/ LeaveI(w)
         \/ Commit(w) \/ Repropagate(w) \/ Shutdown(w) \/ Seal(w) \/ Succeed(w) \/ ProvenResidual(w)
    \/ \E s, r \in Workers : Receive(s, r)
    \/ Terminated

Spec == Init /\ [][Next]_vars

Fairness ==
    /\ \A w \in Workers :
         /\ WF_vars(InputAdvance(w)) /\ WF_vars(EnterO(w)) /\ WF_vars(LeaveO(w))
         /\ WF_vars(UTake(w)) /\ WF_vars(UFire(w)) /\ WF_vars(EnterI(w))
         /\ WF_vars(BodyO(w)) /\ WF_vars(FeedbackO(w)) /\ WF_vars(BodyI(w))
         /\ WF_vars(FeedbackI(w)) /\ WF_vars(LeaveI(w)) /\ WF_vars(Commit(w))
         /\ WF_vars(Repropagate(w)) /\ WF_vars(Shutdown(w)) /\ WF_vars(Seal(w))
         /\ WF_vars(Succeed(w))
    /\ \A s, r \in Workers : WF_vars(Receive(s, r))
LiveSpec == Spec /\ Fairness

Sym == Permutations(Workers)

---------------------------------------------------------------------------
(* Properties *)

IWorkAt(w, t, j) == \E l \in IRaw, k \in Ks : imsgs[l][w][<<t, j, k>>] > 0
OWorkAt(w, t) ==
    \E j \in Js : (\E l \in ORaw : omsgs[l][w][<<t, j>>] > 0) \/ IWorkAt(w, t, j)

\* Outstanding work; work inside a scope counts at the scope node's output.
OBusyW(w, l, t, j) == IF l = "SI.out" THEN IWorkAt(w, t, j) ELSE omsgs[l][w][<<t, j>>] > 0
RBusyW(w, l, t) ==
    CASE l = "in.out" -> incap[w][t] > 0
      [] l = "eO.in"  -> rmsgs["eO.in"][w][t] > 0
      [] l = "u.in"   -> rmsgs["u.in"][w][t] > 0
      [] l = "u.out"  -> ucap[w][t] + ustash[w][t] > 0
      [] l = "SO.out" -> OWorkAt(w, t)

\* S1 at each level: no frontier ever passes outstanding work.
InnerSafe ==
    \A w \in Workers : Live(w) =>
        \A l \in IRaw, x \in IT :
            (\E v \in Workers : imsgs[l][v][x] > 0) => IW(w, l, x[1], x[2], x[3])
OuterSafe ==
    \A w \in Workers : Live(w) =>
        \A l \in ORaw \cup {"SI.out"}, c \in OT :
            (\E v \in Workers : OBusyW(v, l, c[1], c[2])) => OW(w, l, c[1], c[2])
RootSafe ==
    \A w \in Workers : Live(w) =>
        \A l \in RLocs, t \in Times :
            (\E v \in Workers : RBusyW(v, l, t)) => RW(w, l, t)

\* The count never receives a record at or before a time it reported.
NoLateData ==
    \A w \in Workers : \A t \in unotified[w] : \A s \in Times :
        s <= t => rmsgs["u.in"][w][s] = 0

DstR == ("eO.in" :> "eO") @@ ("u.in" :> "u")
DstO == ("TI" :> "eI") @@ ("bO.in" :> "bO") @@ ("fbO.in" :> "fbO") @@ ("lO.in" :> "lO")
DstI == ("bI.in" :> "bI") @@ ("fbI.in" :> "fbI") @@ ("lI.in" :> "lI")
NoDataAtDone ==
    /\ \A e \in {"eO.in", "u.in"}, w \in Workers, t \in Times : done[w][DstR[e]] => rmsgs[e][w][t] = 0
    /\ \A e \in ORaw, w \in Workers, c \in OT : done[w][DstO[e]] => omsgs[e][w][c] = 0
    /\ \A e \in IRaw, w \in Workers, x \in IT : done[w][DstI[e]] => imsgs[e][w][x] = 0

CompleteMeansEmpty ==
    AllSucceeded =>
        /\ \A l \in RLocs, t \in Times, v \in Workers : ~RBusyW(v, l, t)
        /\ \A l \in IRaw, x \in IT, v \in Workers : imsgs[l][v][x] = 0

NoResidual == ~residual

\* R38 condition 2: when every raw count in a worker's view is zero, every
\* derived link it caches is empty too ("its exported capability set must
\* become empty", design §5.1.7). Checked after success as well.
DerivedZero ==
    \A w \in Workers : ViewsZero(w) =>
        expI[w] = {} /\ expO[w] = {} /\ impO[w] = {} /\ impI[w] = {}

\* PCP only: the cache always equals a fresh recompute from the raw views
\* alone, so no scope ever exports its import, at any level.
CacheExact ==
    \A w \in Workers :
        LET d == Derive(rview[w], oview[w], iview[w], {}) IN
            expI[w] = d.eI /\ expO[w] = d.eO /\ impO[w] = d.iO /\ impI[w] = d.iI

\* Sanity (expected to be violated): a record went around both loops, an
\* outer lap crossed workers and re-entered the inner loop at lap 1, the
\* count reported, and the run completed. Needs Track.
NeverCompletesAfterLaps ==
    ~(AllSucceeded /\ olaps > 0 /\ ilaps > 0 /\ hist = {"xlap", "reenter"}
      /\ \E w \in Workers : unotified[w] # {})

EventuallyDone == <>AllSucceeded

\* With KeepOpen, the count reports every earlier epoch although the input
\* never closes: epoch 0's export drains while epoch 1 is still open.
UProgress ==
    \A w \in Workers, t \in Times : t < MaxT => ((ustash[w][t] > 0) ~> (t \in unotified[w]))
=============================================================================
