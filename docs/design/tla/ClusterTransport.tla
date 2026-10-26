--------------------------- MODULE ClusterTransport ---------------------------
(***************************************************************************)
(* A model of instancy's cluster transport after PR9 (#300), under a       *)
(* simplified teardown (decision log O16). For TLC.                        *)
(*                                                                         *)
(* What it checks: the transport contract. instancy never reports success  *)
(* with lost, crossed or corrupted data; a run never takes another run's   *)
(* frames; the transport never drops a frame it could still deliver;       *)
(* every failure the transport can detect is seen by both nodes, nothing   *)
(* hangs, and a failed connection fails only the runs that use it.         *)
(*                                                                         *)
(* Two nodes, a and b, run one dataflow, or two: a retry that starts on a  *)
(* node once run 1 ended there, or (Concurrent) a second dataflow running  *)
(* alongside, its lane pinned to another connection.                       *)
(*                                                                         *)
(* Connections. Each pooled connection has two directions; each node has   *)
(* its own write half and read half. A writer drains its queue onto the    *)
(* wire; the peer's reader takes frames off it in order (TCP). Modelled:   *)
(*  - shutting a write half explicitly sends FIN after the written frames; *)
(*  - dropping only a write half sends FIN for owned halves, nothing for   *)
(*    split halves (TLS streams, pipes): Split = TRUE;                     *)
(*  - closing a whole socket sends FIN, or a reset if input is unread; a   *)
(*    write to a closed socket draws a reset;                              *)
(*  - a reset may discard every frame not yet read, both ways (the worst   *)
(*    case; delivering a prefix first is an earlier interleaving), and    *)
(*    both ends then fail;                                                 *)
(*  - a corrupted frame is caught by its checksum (Crc), or else accepted. *)
(*                                                                         *)
(* Lanes. A run's frames pass an outbox, a bridge that numbers each        *)
(* payload frame, and a writer queue. PR9 (Pin) pins the lane to one       *)
(* connection; the receiver takes exactly the next number, anything else   *)
(* fails the run; a failed connection fails its lanes, and the next        *)
(* registration refills the pool. A Cancel is a control frame: it skips    *)
(* the outbox and is not numbered. The earlier design spread a lane over   *)
(* every live connection, restored order with a reorder buffer and a gap   *)
(* timer, and redialled in the background without telling the runs; a     *)
(* frame the bridge accepted while no connection was up was dropped        *)
(* without a number (audit C7).                                            *)
(*                                                                         *)
(* Runs, abstracted from port-counted progress: each node sends a record   *)
(* ("data"), the progress counting it ("p1") and its final progress ("p2", *)
(* its input capability released). Its workers succeed once its view is    *)
(* exactly zero; then it queues its goodbye ("bye"); a node acks a peer's  *)
(* goodbye ("ack") at once. A link is retired by the peer's goodbye or its *)
(* ack of ours; a Cancel stops a node waiting for that peer; a requested   *)
(* stop drains; a forwarded failure is fatal.                              *)
(*                                                                         *)
(* Simplified, not checked here: the lifecycle machine's soft failures     *)
(* (deferred until the link fully closes or a grace timer expires, then    *)
(* settled) are decided at once; transport events reach a run in the same  *)
(* step as the read that causes them; data-channel close sentinels are not *)
(* modelled (completion is exact zero); linger has no deadline; the        *)
(* handshake and ready barrier are one step, whose timeout fires only when *)
(* the peer's run already ended; and a manager remembers every finished    *)
(* DataflowId (PR9 keeps the last 16,384).                                 *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Conns,          \* the pooled connections, e.g. {1} or {1, 2}
    Runs,           \* {1}, or {1, 2}
    Concurrent,     \* TRUE: run 2 is another dataflow running alongside run 1,
                    \* pinned to the highest connection; FALSE: run 2 is a retry
    Crosswise,      \* with Concurrent: node b pins the two runs the other way
                    \* round, so each connection carries both runs
    Reuse,          \* TRUE: the retry reuses run 1's DataflowId (a contract violation)
    Faults,         \* the transport faults that may occur:
                    \* a subset of {"reset", "writeerr", "corrupt"}
    MaxFaults,      \* how many, in total
    FaultsRun1,     \* TRUE: faults happen only while run 1 is registered
                    \* somewhere and before run 2 starts anywhere, and a node
                    \* retries only once it noticed them
    MaxStops,       \* requested stops (Cancel, then drain), in total
    MaxFails,       \* local worker failures of run 1, in total
    PerJob,         \* TRUE: a node releases its manager once its runs are over
    Pin,            \* TRUE: PR9's pinned lanes; FALSE: the earlier design
    WriteErrClose,  \* TRUE: a write error closes the whole connection (PR9, 4be0e70)
    Split,          \* TRUE: dropping a write half sends no FIN (split halves)
    CleanEof,       \* "keep": a peer's clean close lets pinned lanes keep
                    \* writing (PR9, 3a77274); "fail": it fails them (before)
    Linger,         \* TRUE: a released manager shuts its write halves and
                    \* reads until the peer's FIN; FALSE: it closes the sockets
    RunIds,         \* "pr9": reject a finished DataflowId, drop its late frames,
                    \* bind queued frames to their registration (52314fb); "old"
    RejectCancel,   \* TRUE: a spawn refused for a reused DataflowId still
                    \* broadcasts Cancel under that id (SpawnAbortGuard, a7c4b1c);
                    \* FALSE: it sends nothing under an id that is another run's
    Crc             \* TRUE: frame checksums (always on from protocol v6)

N == {"a", "b"}
Other(n) == IF n = "a" THEN "b" ELSE "a"
MinConn == CHOOSE c \in Conns : \A d \in Conns : c <= d
MaxConn == CHOOSE c \in Conns : \A d \in Conns : c >= d
Id(r) == IF r = 2 /\ (Concurrent \/ ~Reuse) THEN "y" ELSE "x"
Terminal(s) == s \in {"ok", "fail", "rej"}

\* A frame: its kind, DataflowId, sequence number (-1: not numbered), for a
\* Cancel whether it forwards a failure, and whether it was corrupted on the
\* wire. `run` is a ghost: the run that really sent it.
Frame(k, id, r, s, fl) == [k |-> k, id |-> id, run |-> r, seq |-> s, fail |-> fl, bad |-> FALSE]

---------------------------------------------------------------------------
(* State *)

VARIABLES
    rs,     \* [N -> [Runs -> run record]]
    cs,     \* [N -> [Conns -> connection end record]]
    br,     \* [Conns -> BOOLEAN]: the connection was reset
    rel,    \* [N -> BOOLEAN]: the node released its manager
    mem,    \* [N -> SUBSET ids]: DataflowIds of runs that ended here
    pend,   \* [N -> Seq([f, c])]: Cancels for a DataflowId not registered yet,
            \* with the connection they arrived on
    cnt     \* counters, and ghosts for the properties

vars == <<rs, cs, br, rel, mem, pend, cnt>>

\* A run on one node.
\*  st: off, run (workers running), done (workers succeeded, tearing down),
\*      ok, fail, or rej (its registration was refused);
\*  reg: none, reg, gone; pin: the connection its lane is pinned to;
\*  sent: it queued its record and progress; v, cap: its view of the peer's
\*  counts (p1 minus records consumed; the peer's p2 arrived); gd: ghost,
\*  this run's records consumed; bye, ack: the peer's goodbye, or its ack of
\*  ours, arrived (retired); gc: the peer's Cancel arrived; cx: it broadcast
\*  a requested stop; ended: its link ended for good after its workers
\*  succeeded; pf: it failed on the peer's Cancel; obf: its lane failed;
\*  tx, rx: lane sequence numbers; rb: the earlier design's reorder buffer;
\*  ob: its outbox; cross: ghost, it took another run's frame.
RunInit(r) ==
    LET live == r = 1 \/ Concurrent IN
    [st |-> IF live THEN "run" ELSE "off", reg |-> IF live THEN "reg" ELSE "none",
     pin |-> IF r = 2 /\ Concurrent THEN MaxConn ELSE MinConn,
     sent |-> FALSE, v |-> 0, cap |-> FALSE, gd |-> 0,
     bye |-> FALSE, ack |-> FALSE, gc |-> FALSE, cx |-> FALSE, ended |-> FALSE,
     pf |-> FALSE, obf |-> FALSE, tx |-> 0, rx |-> 0, rb |-> {}, ob |-> <<>>,
     cross |-> FALSE]

\* One node's end of one connection.
\*  wst: its writer is up, shut (FIN sent) or dead; rds: its reader is up or
\*  done; pool: live, peerclosed (the peer closed cleanly while lanes are
\*  pinned here) or gone; fin: a FIN follows its wire; closed: the whole
\*  socket is closed; wq: writer queue; wire: frames written, unread.
ConnInit == [wst |-> "up", rds |-> "up", pool |-> "live", fin |-> FALSE,
             closed |-> FALSE, wq |-> <<>>, wire |-> <<>>]

\* Each node pins a lane to its least-loaded connection, by its own
\* registration order, so concurrent runs may be pinned crosswise
\* (Crosswise): node b puts run 1 where node a put run 2. Connections are
\* interchangeable, so node a's choice is fixed.
Init ==
    /\ LET swap == Concurrent /\ Crosswise IN
         rs = [n \in N |-> [r \in Runs |->
                 [RunInit(r) EXCEPT !.pin = IF n = "b" /\ swap
                                            THEN IF r = 1 THEN MaxConn ELSE MinConn
                                            ELSE @]]]
    /\ cs = [n \in N |-> [c \in Conns |-> ConnInit]]
    /\ br = [c \in Conns |-> FALSE]
    /\ rel = [n \in N |-> FALSE]
    /\ mem = [n \in N |-> {}]
    /\ pend = [n \in N |-> <<>>]
    \* f, s, x: faults, stops, failures. faulted: connections a fault hit.
    \* needless: a lane frame was dropped though its connection could still
    \* carry it. bad: a run took a corrupted frame.
    /\ cnt = [f |-> 0, s |-> 0, x |-> 0, faulted |-> {}, needless |-> FALSE, bad |-> FALSE]

---------------------------------------------------------------------------
(* Helpers *)

RegOf(n, id) ==
    IF \E r \in Runs : rs[n][r].reg = "reg" /\ Id(r) = id
    THEN CHOOSE r \in Runs : rs[n][r].reg = "reg" /\ Id(r) = id
    ELSE 0

\* Registered runs whose lane is pinned to c, and whose frames from the peer
\* arrive on c (the peer pinned its lane there).
Pinned(n, c) == {r \in Runs : rs[n][r].reg = "reg" /\ rs[n][r].pin = c}
Arrive(n, c) == {r \in Runs : rs[n][r].reg = "reg" /\ rs[Other(n)][r].pin = c}

Live(n) == {c \in Conns : cs[n][c].pool = "live" /\ cs[n][c].wst = "up"}

\* PR9 pins a new lane to the live connection with the fewest lanes.
Least(n) ==
    CHOOSE c \in Live(n) : \A d \in Live(n) :
        \/ Cardinality(Pinned(n, c)) < Cardinality(Pinned(n, d))
        \/ Cardinality(Pinned(n, c)) = Cardinality(Pinned(n, d)) /\ c <= d

Retired(rec) == rec.bye \/ rec.ack

\* R48: the peer requested a stop and we need nothing more from it.
NeedsNothing(rec) == rec.gc /\ rec.cap /\ rec.v = 0

\* A transport event reaches a run. While its workers run: a failed send
\* ("wfail") or a closed or failed inbound link ("closed", "rfail") is fatal
\* unless the link is retired (cluster_lifecycle's `settle_unsent` excuses a
\* send failure once the peer said goodbye and its link closed); an inbound
\* end is also excused when the peer requested a stop and nothing more is
\* needed from it (R48). After its workers succeeded, an unresolved link has
\* ended for good: teardown stops waiting, and the result stays the
\* workers'. Corruption ("corrupt") is fatal in every phase.
HitRec(rec, e) ==
    CASE e = "corrupt" /\ rec.st \in {"run", "done"} -> [rec EXCEPT !.st = "fail"]
      [] rec.st = "run" ->
            IF Retired(rec) \/ (e # "wfail" /\ NeedsNothing(rec))
            THEN rec ELSE [rec EXCEPT !.st = "fail"]
      [] rec.st = "done" ->
            IF Retired(rec) \/ rec.gc \/ rec.cx THEN rec ELSE [rec EXCEPT !.ended = TRUE]
      [] OTHER -> rec

WfailRec(rec) == [HitRec(rec, "wfail") EXCEPT !.obf = TRUE]

\* What a connection's failure does to the runs using it: lanes pinned to
\* it fail; runs whose frames arrive on it are told the inbound side ended.
Broken(n, c, inbound) ==
    [r \in Runs |->
        LET a == IF r \in Pinned(n, c) THEN WfailRec(rs[n][r]) ELSE rs[n][r]
        IN IF r \in Arrive(n, c) THEN HitRec(a, inbound) ELSE a]

\* Consume the next payload frame of the lane.
ConsumeRec(rec, f, r) ==
    [rec EXCEPT !.rx = @ + 1,
                !.cross = @ \/ f.run # r,
                !.v = CASE f.k = "data" -> @ - 1 [] f.k = "p1" -> @ + 1 [] OTHER -> @,
                !.gd = IF f.k = "data" /\ f.run = r THEN @ + 1 ELSE @,
                !.cap = @ \/ f.k = "p2",
                !.bye = @ \/ f.k = "bye",
                !.ack = @ \/ f.k = "ack",
                !.ob = IF f.k = "bye" /\ rec.st \in {"run", "done"}
                       THEN Append(@, Frame("ack", Id(r), r, -1, FALSE)) ELSE @]

\* The earlier design: deliver buffered frames that are now next in order.
RECURSIVE Drain(_, _)
Drain(rec, r) ==
    IF \E g \in rec.rb : g.seq = rec.rx
    THEN LET g == CHOOSE g \in rec.rb : g.seq = rec.rx
         IN Drain([ConsumeRec(rec, g, r) EXCEPT !.rb = @ \ {g}], r)
    ELSE rec

PayloadRec(rec, f, r) ==
    IF Pin
    THEN IF f.seq = rec.rx
         THEN ConsumeRec(rec, f, r)
         \* OutOfSequence: corruption, fatal in every phase.
         ELSE [rec EXCEPT !.st = IF @ \in {"run", "done"} THEN "fail" ELSE @]
    ELSE CASE f.seq = rec.rx -> Drain(ConsumeRec(rec, f, r), r)
           [] f.seq > rec.rx -> [rec EXCEPT !.rb = @ \cup {f}]
           [] OTHER -> rec

\* A peer's Cancel: a forwarded failure fails a running run; a requested
\* stop (or a Cancel after our workers succeeded) only stops us waiting for
\* that peer. Frames after it are still applied (R52).
CxlRec(rec, f, r) ==
    LET c == [rec EXCEPT !.cross = @ \/ f.run # r] IN
    IF f.fail /\ rec.st = "run"
    THEN [c EXCEPT !.st = "fail", !.pf = TRUE]
    ELSE [c EXCEPT !.gc = TRUE]

\* Apply every pending Cancel for `id` in order (registration delivers them
\* at once, before the run can start).
RECURSIVE ApplyPend(_, _, _)
ApplyPend(rec, q, r) ==
    IF q = <<>> THEN rec ELSE ApplyPend(CxlRec(rec, Head(q).f, r), Tail(q), r)

---------------------------------------------------------------------------
(* Frames received *)

\* A frame read from connection c is routed by its DataflowId to the run
\* registered for it. With no registration, payload is dropped, and a
\* Cancel waits for the registration (pending control) unless the id ended
\* here (PR9). A corrupted payload frame that is accepted marks `bad`.
Process(n, f, c) ==
    LET r == RegOf(n, f.id) IN
    IF r = 0
    THEN /\ pend' = IF f.k = "cxl" /\ ~(RunIds = "pr9" /\ f.id \in mem[n])
                    THEN [pend EXCEPT ![n] = Append(@, [f |-> f, c |-> c])] ELSE pend
         /\ UNCHANGED <<rs, cnt>>
    ELSE /\ rs' = [rs EXCEPT ![n][r] = IF f.k = "cxl" THEN CxlRec(@, f, r) ELSE PayloadRec(@, f, r)]
         /\ cnt' = IF f.bad /\ rs[n][r].st \in {"run", "done"}
                   THEN [cnt EXCEPT !.bad = TRUE] ELSE cnt
         /\ UNCHANGED pend

ForgetConn(n, c) == [pend EXCEPT ![n] = SelectSeq(@, LAMBDA e : e.c # c)]

Read(n, c) ==
    LET m == Other(n)
        f == Head(cs[m][c].wire)
    IN
    /\ cs[n][c].rds = "up" /\ cs[m][c].wire # <<>>
    /\ IF f.bad /\ Crc
       THEN \* The checksum catches it: corruption. The frame's own run fails
            \* in every phase; the others using c are told it was lost. The
            \* connection breaks.
            /\ br' = [br EXCEPT ![c] = TRUE]
            /\ cs' = [cs EXCEPT ![n][c] = [@ EXCEPT !.rds = "done", !.wq = <<>>, !.pool = "gone",
                                                    !.wst = IF @ = "up" THEN "dead" ELSE @, !.wire = <<>>],
                                ![m][c].wire = <<>>]
            /\ rs' = IF Pin
                     THEN [rs EXCEPT ![n] = [r \in Runs |->
                             LET a == Broken(n, c, "rfail")[r]
                             IN IF Id(r) = f.id /\ rs[n][r].reg = "reg" THEN HitRec(a, "corrupt") ELSE a]]
                     ELSE rs
            /\ pend' = ForgetConn(n, c)
            /\ UNCHANGED cnt
       ELSE /\ cs' = [cs EXCEPT ![m][c].wire = Tail(@)]
            /\ Process(n, f, c)
            /\ UNCHANGED br
    /\ UNCHANGED <<rel, mem>>

\* A read error: the peer's frames on c may be lost. In PR9 it breaks the
\* whole connection: the lanes pinned to c fail too.
ReadErr(n, c) ==
    LET m == Other(n) IN
    /\ cs[n][c].rds = "up" /\ br[c] /\ cs[m][c].wire = <<>>
    /\ rs' = IF Pin THEN [rs EXCEPT ![n] = Broken(n, c, "rfail")] ELSE rs
    /\ cs' = [cs EXCEPT ![n][c] = [@ EXCEPT !.rds = "done", !.wq = <<>>, !.pool = "gone",
                                            !.wst = IF @ = "up" THEN "dead" ELSE @]]
    /\ pend' = ForgetConn(n, c)
    /\ UNCHANGED <<br, rel, mem, cnt>>

\* The peer closed its side cleanly (FIN after its last frame).
\*  - PR9 (CleanEof = "keep"): every run using c is told (Closed); lanes
\*    pinned here keep writing, since the peer may still be reading; the
\*    connection takes no new lanes.
\*  - Before 3a77274 ("fail"): the lanes pinned here were failed.
CleanEofA(n, c) ==
    LET m == Other(n)
        keep == CleanEof = "keep"
        pinned == Pinned(n, c)
    IN
    /\ cs[n][c].rds = "up" /\ ~br[c] /\ cs[m][c].wire = <<>> /\ cs[m][c].fin
    /\ rs' = IF Pin
             THEN [rs EXCEPT ![n] = [r \in Runs |->
                     LET a == IF r \in pinned /\ ~keep THEN WfailRec(rs[n][r]) ELSE rs[n][r]
                     IN IF r \in pinned \cup Arrive(n, c) THEN HitRec(a, "closed") ELSE a]]
             ELSE rs
    /\ cs' = [cs EXCEPT ![n][c].rds = "done",
                        ![n][c].pool = IF Pin /\ keep /\ pinned # {} THEN "peerclosed" ELSE "gone"]
    /\ pend' = ForgetConn(n, c)
    /\ UNCHANGED <<br, rel, mem, cnt>>

---------------------------------------------------------------------------
(* Frames sent *)

\* The bridge numbers a run's next outbox frame and hands it to a writer
\* queue. `needless` (ghost): the lane drops a frame although its writer is
\* up, the connection is healthy and the peer is still reading.
Bridge(n, r) ==
    LET rec == rs[n][r]
        f == Head(rec.ob)
        p == rec.pin
        drop(needless) == /\ rs' = [rs EXCEPT ![n][r].ob = Tail(@)]
                          /\ cnt' = IF needless THEN [cnt EXCEPT !.needless = TRUE] ELSE cnt
                          /\ UNCHANGED cs
    IN
    /\ rec.ob # <<>>
    /\ IF Pin
       THEN IF rec.reg # "reg"
            THEN \* The registration ended. PR9 drops the frame (it is bound to
                 \* its registration); earlier, the bridge numbered it into the
                 \* lane of a later registration of the same id.
                 LET q == RegOf(n, f.id) IN
                 IF RunIds = "old" /\ q # 0 /\ ~rs[n][q].obf /\ cs[n][rs[n][q].pin].wst = "up"
                 THEN /\ rs' = [rs EXCEPT ![n][r].ob = Tail(@), ![n][q].tx = @ + 1]
                      /\ cs' = [cs EXCEPT ![n][rs[n][q].pin].wq = Append(@, [f EXCEPT !.seq = rs[n][q].tx])]
                      /\ UNCHANGED cnt
                 ELSE drop(FALSE)
            ELSE IF rec.obf \/ cs[n][p].wst # "up"
                 THEN drop(cs[n][p].wst = "up" /\ ~br[p] /\ cs[Other(n)][p].rds = "up")
                 ELSE /\ rs' = [rs EXCEPT ![n][r].ob = Tail(@), ![n][r].tx = @ + 1]
                      /\ cs' = [cs EXCEPT ![n][p].wq = Append(@, [f EXCEPT !.seq = rec.tx])]
                      /\ UNCHANGED cnt
       ELSE IF rec.reg # "reg" \/ Live(n) = {}
            THEN drop(FALSE)    \* earlier design: accepted during a reconnect and
                                \* dropped without a number, so no gap shows (C7)
            ELSE \E c \in Live(n) :
                    /\ rs' = [rs EXCEPT ![n][r].ob = Tail(@), ![n][r].tx = @ + 1]
                    /\ cs' = [cs EXCEPT ![n][c].wq = Append(@, [f EXCEPT !.seq = rec.tx])]
                    /\ UNCHANGED cnt
    /\ UNCHANGED <<br, rel, mem, pend>>

\* A connection's writer fails: on a reset connection, or a local write
\* error while writing. Its queue is lost. PR9: the lanes pinned to it fail
\* and, with WriteErrClose, the whole connection closes (its reader stops,
\* the runs whose frames arrive on it are told, the peer sees FIN or a
\* reset). Without it, only the write half is dropped. The earlier design
\* tells no run.
WriteFail(n, c) ==
    LET m == Other(n)
        closeAll == ~Pin \/ WriteErrClose
        rst == closeAll /\ cs[m][c].wire # <<>> /\ ~br[c]
    IN
    /\ rs' = IF ~Pin THEN rs
             ELSE IF closeAll THEN [rs EXCEPT ![n] = Broken(n, c, "rfail")]
             ELSE [rs EXCEPT ![n] = [r \in Runs |->
                     IF r \in Pinned(n, c) THEN WfailRec(rs[n][r]) ELSE rs[n][r]]]
    /\ cs' = [cs EXCEPT ![n][c] = [@ EXCEPT !.wst = "dead", !.wq = <<>>, !.pool = "gone",
                                            !.rds = IF closeAll THEN "done" ELSE @,
                                            !.closed = @ \/ closeAll,
                                            !.fin = @ \/ (IF closeAll THEN ~rst ELSE ~Split),
                                            !.wire = IF rst THEN <<>> ELSE @],
                        ![m][c].wire = IF rst THEN <<>> ELSE @]
    /\ br' = IF rst THEN [br EXCEPT ![c] = TRUE] ELSE br
    /\ pend' = IF closeAll THEN ForgetConn(n, c) ELSE pend

\* The writer writes the next queued frame. Writing to a reset connection
\* fails; writing to a socket the peer closed draws a reset.
Write(n, c) ==
    LET m == Other(n) IN
    /\ cs[n][c].wst = "up" /\ cs[n][c].wq # <<>>
    /\ IF br[c]
       THEN WriteFail(n, c) /\ UNCHANGED <<rel, mem, cnt>>
       ELSE IF cs[m][c].closed
       THEN /\ br' = [br EXCEPT ![c] = TRUE]
            /\ cs' = [cs EXCEPT ![n][c].wq = Tail(@), ![n][c].wire = <<>>, ![m][c].wire = <<>>]
            /\ UNCHANGED <<rs, rel, mem, pend, cnt>>
       ELSE /\ cs' = [cs EXCEPT ![n][c].wq = Tail(@), ![n][c].wire = Append(@, Head(cs[n][c].wq))]
            /\ UNCHANGED <<rs, br, rel, mem, pend, cnt>>

\* The earlier design redials a failed connection in the background and
\* carries its lanes on: nothing tells the runs.
Redial(c) ==
    /\ ~Pin /\ br[c]
    /\ \A n \in N : ~rel[n]
    /\ br' = [br EXCEPT ![c] = FALSE]
    /\ cs' = [cs EXCEPT !["a"][c] = ConnInit, !["b"][c] = ConnInit]
    /\ UNCHANGED <<rs, rel, mem, pend, cnt>>

\* PR9 has no background redial: a registration that finds the pool short
\* re-establishes a connection both nodes have given up on.
Refill(c) ==
    /\ Pin
    /\ \E n \in N, r \in Runs : rs[n][r].st = "off" /\ Live(n) = {}
    /\ \A n \in N : /\ cs[n][c].pool # "live" /\ cs[n][c].rds = "done"
                    /\ cs[n][c].wst # "up" /\ Pinned(n, c) = {}
    /\ br' = [br EXCEPT ![c] = FALSE]
    /\ cs' = [cs EXCEPT !["a"][c] = ConnInit, !["b"][c] = ConnInit]
    /\ UNCHANGED <<rs, rel, mem, pend, cnt>>

\* A node closes its side of a connection: when it released its manager, or
\* when the connection left the pool and its last pinned lane unregistered.
\*  - Linger (PR9, and PR7's release): write what is queued, shut the write
\*    half explicitly (FIN for every stream kind), keep reading until the
\*    peer's FIN.
\*  - Otherwise a release closes the whole socket at once, dropping what is
\*    queued; with input still unread, that resets the connection.
CloseOut(n, c) ==
    LET m == Other(n)
        conn == cs[n][c]
        abrupt == rel[n] /\ ~Linger /\ conn.rds = "up"
    IN
    /\ conn.wst = "up" /\ ~conn.closed
    /\ \/ rel[n]
       \/ conn.pool # "live" /\ conn.rds = "done" /\ Pinned(n, c) = {}
    /\ abrupt \/ conn.wq = <<>>
    /\ IF abrupt /\ cs[m][c].wire # <<>> /\ ~br[c]
       THEN /\ br' = [br EXCEPT ![c] = TRUE]
            /\ cs' = [cs EXCEPT ![n][c] = [@ EXCEPT !.wst = "shut", !.rds = "done", !.closed = TRUE,
                                                    !.wq = <<>>, !.wire = <<>>],
                                ![m][c].wire = <<>>]
       ELSE /\ cs' = [cs EXCEPT ![n][c] = [@ EXCEPT !.wst = "shut", !.fin = TRUE, !.wq = <<>>,
                                                    !.rds = IF abrupt THEN "done" ELSE @,
                                                    !.closed = abrupt]]
            /\ UNCHANGED br
    /\ UNCHANGED <<rs, rel, mem, pend, cnt>>

---------------------------------------------------------------------------
(* Runs *)

\* Queue the record and its progress once both nodes registered and the
\* peer's run is live (the handshake and ready barrier, abstracted).
Send(n, r) ==
    LET rec == rs[n][r] IN
    /\ rec.st = "run" /\ ~rec.sent
    /\ rs[Other(n)][r].st = "run"
    /\ rs' = [rs EXCEPT ![n][r].sent = TRUE,
                        ![n][r].ob = @ \o <<Frame("data", Id(r), r, -1, FALSE),
                                            Frame("p1", Id(r), r, -1, FALSE),
                                            Frame("p2", Id(r), r, -1, FALSE)>>]
    /\ UNCHANGED <<cs, br, rel, mem, pend, cnt>>

\* The workers succeed once the view is exactly zero; the goodbye follows.
Workers(n, r) ==
    LET rec == rs[n][r] IN
    /\ rec.st = "run" /\ rec.sent /\ rec.cap /\ rec.v = 0
    /\ rs' = [rs EXCEPT ![n][r].st = "done",
                        ![n][r].ob = Append(@, Frame("bye", Id(r), r, -1, FALSE))]
    /\ UNCHANGED <<cs, br, rel, mem, pend, cnt>>

\* Teardown: once the link is retired, the peer cancelled, we broadcast a
\* requested stop, or the link ended for good, and everything queued was
\* handed to the transport (the flush), the run unregisters with success.
Teardown(n, r) ==
    LET rec == rs[n][r] IN
    /\ rec.st = "done" /\ rec.ob = <<>>
    /\ Retired(rec) \/ rec.gc \/ rec.cx \/ rec.ended
    /\ rs' = [rs EXCEPT ![n][r].st = "ok", ![n][r].reg = "gone"]
    /\ mem' = [mem EXCEPT ![n] = @ \cup {Id(r)}]
    /\ UNCHANGED <<cs, br, rel, pend, cnt>>

\* A failed run unregisters and broadcasts Cancel (unless it failed on the
\* peer's Cancel), on its pinned connection, ahead of its outbox.
Cleanup(n, r) ==
    LET rec == rs[n][r]
        to == IF Pin THEN {rec.pin} \cap {c \in Conns : cs[n][c].wst = "up"} ELSE Live(n)
    IN
    /\ rec.st = "fail" /\ rec.reg = "reg"
    /\ rs' = [rs EXCEPT ![n][r].reg = "gone"]
    /\ mem' = [mem EXCEPT ![n] = @ \cup {Id(r)}]
    /\ IF ~rec.pf /\ to # {}
       THEN \E c \in to : cs' = [cs EXCEPT ![n][c].wq = Append(@, Frame("cxl", Id(r), r, -1, TRUE))]
       ELSE UNCHANGED cs
    /\ UNCHANGED <<br, rel, pend, cnt>>

\* A requested stop: broadcast Cancel, then drain (keep running).
RequestStop(n, r) ==
    LET rec == rs[n][r]
        to == IF Pin THEN {rec.pin} \cap {c \in Conns : cs[n][c].wst = "up"} ELSE Live(n)
    IN
    /\ cnt.s < MaxStops /\ rec.st = "run" /\ ~rec.cx
    /\ rs' = [rs EXCEPT ![n][r].cx = TRUE]
    /\ IF to # {}
       THEN \E c \in to : cs' = [cs EXCEPT ![n][c].wq = Append(@, Frame("cxl", Id(r), r, -1, FALSE))]
       ELSE UNCHANGED cs
    /\ cnt' = [cnt EXCEPT !.s = @ + 1]
    /\ UNCHANGED <<br, rel, mem, pend>>

\* A local worker failure in run 1.
LocalFail(n) ==
    /\ cnt.x < MaxFails /\ rs[n][1].st = "run"
    /\ rs' = [rs EXCEPT ![n][1].st = "fail"]
    /\ cnt' = [cnt EXCEPT !.x = @ + 1]
    /\ UNCHANGED <<cs, br, rel, mem, pend>>

\* The handshake or ready barrier times out (`handshake_timeout`): the
\* peer's run ended before the barrier passed and its Cancel, if any, was
\* lost with a connection. A healthy peer always answers in time, so the
\* timeout fires only when the peer never can.
BarrierTimeout(n, r) ==
    /\ rs[n][r].st = "run" /\ ~rs[n][r].sent /\ Terminal(rs[Other(n)][r].st)
    /\ rs' = [rs EXCEPT ![n][r].st = "fail"]
    /\ UNCHANGED <<cs, br, rel, mem, pend, cnt>>

\* The earlier design's gap timer fires while a frame is missing (a timer
\* abstracts time: under load any delay may exceed it, audit H7).
GapTimeout(n, r) ==
    /\ ~Pin /\ rs[n][r].rb # {} /\ rs[n][r].st \in {"run", "done"}
    /\ rs' = [rs EXCEPT ![n][r].st = "fail"]
    /\ UNCHANGED <<cs, br, rel, mem, pend, cnt>>

\* The retry registers once run 1 ended here. PR9 refuses an id that ended
\* here (InvalidRegistration): the run fails loudly. With RejectCancel its
\* spawn guard then broadcasts a failure Cancel under that id on any
\* connection. Otherwise the retry pins its lane and, in the same step,
\* applies the Cancels that arrived before it registered.
Start(n) ==
    /\ 2 \in Runs /\ ~Concurrent /\ rs[n][2].st = "off"
    /\ Terminal(rs[n][1].st) /\ rs[n][1].reg = "gone"
    /\ FaultsRun1 => \A c \in Conns : br[c] => cs[n][c].rds = "done"
    /\ IF RunIds = "pr9" /\ Id(2) \in mem[n]
       THEN /\ rs' = [rs EXCEPT ![n][2].st = "rej"]
            /\ \/ (~RejectCancel \/ Live(n) = {}) /\ UNCHANGED cs
               \/ \E c \in Live(n) :
                    /\ RejectCancel
                    /\ cs' = [cs EXCEPT ![n][c].wq = Append(@, Frame("cxl", Id(2), 2, -1, TRUE))]
            /\ UNCHANGED pend
       ELSE /\ Live(n) # {}
            /\ LET q == SelectSeq(pend[n], LAMBDA e : e.f.id = Id(2))
                   rec0 == [rs[n][2] EXCEPT !.st = "run", !.reg = "reg", !.pin = Least(n)]
               IN /\ rs' = [rs EXCEPT ![n][2] = ApplyPend(rec0, q, 2)]
                  /\ pend' = [pend EXCEPT ![n] = SelectSeq(@, LAMBDA e : e.f.id # Id(2))]
            /\ UNCHANGED cs
    /\ UNCHANGED <<br, rel, mem, cnt>>

Release(n) ==
    /\ PerJob /\ ~rel[n]
    /\ \A r \in Runs : Terminal(rs[n][r].st) /\ rs[n][r].reg # "reg"
    /\ rel' = [rel EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<rs, cs, br, mem, pend, cnt>>

---------------------------------------------------------------------------
(* Faults *)

FaultAllowed(kind) ==
    /\ kind \in Faults /\ cnt.f < MaxFaults
    /\ FaultsRun1 => /\ \E n \in N : rs[n][1].reg = "reg"
                     /\ \A n \in N : rs[n][2].st = "off"

Hit(c) == [cnt EXCEPT !.f = @ + 1, !.faulted = @ \cup {c}]

Reset(c) ==
    /\ FaultAllowed("reset") /\ ~br[c]
    /\ \E n \in N : cs[n][c].rds = "up"
    /\ br' = [br EXCEPT ![c] = TRUE]
    /\ cs' = [cs EXCEPT !["a"][c].wire = <<>>, !["b"][c].wire = <<>>]
    /\ cnt' = Hit(c)
    /\ UNCHANGED <<rs, rel, mem, pend>>

\* A local write error (a TLS error, say) while writing the next frame: the
\* peer's side is untouched.
WriteErr(n, c) ==
    /\ FaultAllowed("writeerr")
    /\ cs[n][c].wst = "up" /\ cs[n][c].wq # <<>> /\ ~br[c]
    /\ WriteFail(n, c)
    /\ cnt' = Hit(c)
    /\ UNCHANGED <<rel, mem>>

\* A payload frame is corrupted on the wire.
Corrupt(n, c) ==
    /\ FaultAllowed("corrupt")
    /\ \E i \in 1..Len(cs[n][c].wire) :
         /\ cs[n][c].wire[i].k # "cxl" /\ ~cs[n][c].wire[i].bad
         /\ cs' = [cs EXCEPT ![n][c].wire[i].bad = TRUE]
    /\ cnt' = Hit(c)
    /\ UNCHANGED <<rs, br, rel, mem, pend>>

---------------------------------------------------------------------------
(* Specification *)

\* Done: every run ended and unregistered on both nodes and, for per-job
\* managers, every connection closed both ways.
RunsDone == \A n \in N, r \in Runs : Terminal(rs[n][r].st) /\ rs[n][r].reg # "reg"
ConnsDone == \A n \in N, c \in Conns : cs[n][c].wst # "up" /\ cs[n][c].rds = "done"
AllDone == RunsDone /\ (PerJob => ConnsDone)
Terminated == AllDone /\ UNCHANGED vars

Next ==
    \/ \E n \in N :
         \/ \E r \in Runs : Send(n, r) \/ Bridge(n, r) \/ Workers(n, r) \/ Teardown(n, r)
                            \/ Cleanup(n, r) \/ RequestStop(n, r) \/ GapTimeout(n, r)
                            \/ BarrierTimeout(n, r)
         \/ LocalFail(n) \/ Start(n) \/ Release(n)
         \/ \E c \in Conns : Write(n, c) \/ Read(n, c) \/ ReadErr(n, c) \/ CleanEofA(n, c)
                             \/ CloseOut(n, c) \/ WriteErr(n, c) \/ Corrupt(n, c)
    \/ \E c \in Conns : Reset(c) \/ Redial(c) \/ Refill(c)
    \/ Terminated

Spec == Init /\ [][Next]_vars

\* Every protocol step is fair, and the application retries (Start);
\* faults, stops and failures are not fair.
Fairness ==
    /\ \A n \in N :
        /\ WF_vars(Start(n)) /\ WF_vars(Release(n))
        /\ \A r \in Runs : /\ WF_vars(Send(n, r)) /\ WF_vars(Bridge(n, r))
                           /\ WF_vars(Workers(n, r)) /\ WF_vars(Teardown(n, r))
                           /\ WF_vars(Cleanup(n, r)) /\ WF_vars(BarrierTimeout(n, r))
        /\ \A c \in Conns : /\ WF_vars(Write(n, c)) /\ WF_vars(Read(n, c))
                            /\ WF_vars(ReadErr(n, c)) /\ WF_vars(CleanEofA(n, c))
                            /\ WF_vars(CloseOut(n, c))
    /\ \A c \in Conns : WF_vars(Redial(c)) /\ WF_vars(Refill(c))
LiveSpec == Spec /\ Fairness

---------------------------------------------------------------------------
(* Properties *)

TypeOK ==
    /\ \A n \in N, r \in Runs :
         /\ rs[n][r].st \in {"off", "run", "done", "ok", "fail", "rej"}
         /\ rs[n][r].reg \in {"none", "reg", "gone"}
         /\ rs[n][r].pin \in Conns
         /\ rs[n][r].v \in -2..2 /\ rs[n][r].gd \in 0..2
    /\ \A n \in N, c \in Conns :
         /\ cs[n][c].wst \in {"up", "shut", "dead"} /\ cs[n][c].rds \in {"up", "done"}
         /\ cs[n][c].pool \in {"live", "peerclosed", "gone"}
    /\ cnt.faulted \subseteq Conns

\* Never Ok with lost data: a run whose workers succeeded consumed the record
\* the peer sent it.
NoLossOk == \A n \in N, r \in Runs : rs[n][r].st \in {"done", "ok"} => rs[n][r].gd = 1

\* A run never takes another run's frame (payload or Cancel) for its own.
NoCrossRun == \A n \in N, r \in Runs : ~rs[n][r].cross

\* A run never takes a corrupted record.
NoCorruptAccepted == ~cnt.bad

\* The transport never drops a lane frame while it could still deliver it:
\* the writer up, the connection healthy and the peer reading.
NoNeedlessDrop == ~cnt.needless

\* With a healthy transport and no worker failure, no run fails: a hang or
\* failure inside instancy is a bug (the transport contract). A requested
\* stop drains, so it is still a success.
NoSpuriousFail ==
    cnt.f = 0 /\ cnt.x = 0 => \A n \in N, r \in Runs : rs[n][r].st # "fail"

\* A failed connection fails only the runs that use it (PR9 containment).
NoCollateralFail ==
    cnt.x = 0 =>
        \A n \in N, r \in Runs :
            rs[n][r].st = "fail" =>
                rs[n][r].pin \in cnt.faulted \/ rs[Other(n)][r].pin \in cnt.faulted

\* With a fresh DataflowId and a healthy transport, the retry succeeds on
\* both nodes, whatever happened to run 1.
FreshRetryOk ==
    (2 \in Runs /\ cnt.f = 0) => \A n \in N : rs[n][2].st \notin {"fail", "rej"}

\* A retry that reuses the DataflowId is refused on both nodes.
ReuseRejected == \A n \in N : rs[n][2].st \in {"off", "rej"}

\* Liveness: every run ends and, per job, every connection closes, on both
\* nodes: every failure the transport can detect is seen by both sides, and
\* nothing hangs.
Termination == <>AllDone

\* Liveness: the retry eventually succeeds on both nodes.
RetryEventuallyOk == <>(\A n \in N : rs[n][2].st = "ok")

\* Liveness: a retry that reuses the DataflowId is eventually refused on
\* both nodes.
ReuseEventuallyRejected == <>(\A n \in N : rs[n][2].st = "rej")

\* Sanity (expected to be violated): a requested stop drains to success on
\* both nodes.
NeverOkAfterStop == ~(cnt.s > 0 /\ \A n \in N : rs[n][1].st = "ok")

\* Sanity (expected to be violated): run 1 fails on one node, and the retry
\* succeeds on both.
NeverRetryOk == ~(cnt.x > 0 /\ \A n \in N : rs[n][2].st = "ok")

\* Sanity (expected to be violated): a fault hits a connection and the run
\* on the other connection still succeeds on both nodes.
NeverContained == ~(cnt.f > 0 /\ \A n \in N : rs[n][2].st = "ok" /\ rs[n][1].st = "fail")
=============================================================================
