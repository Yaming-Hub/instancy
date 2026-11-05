----------------------------- MODULE ConnSilence -----------------------------
(***************************************************************************)
(* The timing of the connection liveness rule (connection-liveness.md) on *)
(* one direction of one pooled connection: node x writes, node y reads and *)
(* judges. For TLC.                                                        *)
(*                                                                         *)
(* The rule: x's probe loop queues a probe every P ticks, best effort (it *)
(* is skipped when x's writer queue is full); y's probe loop, every P      *)
(* ticks, declares the connection dead once y has read nothing for T       *)
(* ticks. What it checks: a connection that keeps delivering is never      *)
(* declared dead (NoFalseDeath), and one that falls silent is, within      *)
(* T + P + S ticks (DetectInTime).                                         *)
(*                                                                         *)
(* Time is discrete, and Tick waits until everything due has happened: a  *)
(* probe loop that is due, a writer with bytes to write, a byte due for    *)
(* delivery, a reader with bytes to read. The link carries one byte per    *)
(* tick (the slowest link), so a frame of k bytes takes k ticks to write,  *)
(* and a healthy link delivers each byte within D ticks of its writing.    *)
(* Each node's network tasks (writer, reader, probe loop) can pause for up *)
(* to S ticks, MaxPauses times. y's reader reads whatever arrived unless   *)
(* paused, or, with Blocking, unless a consumer holds it up: a reader that *)
(* waits for room in a full queue, which the rule's precondition CL6 (the *)
(* reader never blocks) excludes. Unit = "frames" resets the timer only    *)
(* on a complete frame. Silence drops what is on the wire and everything   *)
(* written after it.                                                       *)
(***************************************************************************)
EXTENDS Integers, Sequences, TLC

CONSTANTS
    P,          \* probe interval, in ticks
    D,          \* a healthy link delivers a byte within D ticks of its writing
    S,          \* longest pause of one node's network tasks
    T,          \* silence timeout
    F,          \* largest application frame, in bytes
    Q,          \* x's writer queue capacity, in frames
    MaxApp,     \* application frames x sends, in total
    MaxPauses,  \* pauses per node
    Unit,       \* "bytes": any byte read resets the timer (the rule);
                \* "frames": only the last byte of a frame does
    Blocking,   \* TRUE: y's reader may stop reading while a consumer is full
    Silence,    \* TRUE: the connection may fall silent (once)
    H           \* time horizon

ASSUME P >= 1 /\ D >= 0 /\ S >= 0 /\ T >= 1 /\ F >= 1 /\ Q >= 1 /\ H >= 1
ASSUME Unit \in {"bytes", "frames"}

VARIABLES
    now,        \* the current tick
    q,          \* x's writer queue: the sizes of the frames waiting, in bytes
    cur,        \* bytes of the frame being written still to write (0: none)
    wire,       \* bytes on the link: Seq([at: tick written, eof: last of its frame])
    rb,         \* bytes delivered to y, unread: Seq(eof)
    wrote,      \* x's writer has written a byte this tick
    lastRead,   \* when y last reset its timer
    xl, yl,     \* ticks until x's and y's probe loops run next
    xp, yp,     \* ticks left of x's and y's current pause
    np,         \* pauses so far, per node
    apps,       \* application frames queued so far
    blocked,    \* y's reader is held up by a full consumer (Blocking only)
    silentAt,   \* -1, or the tick the connection fell silent
    deadAt,     \* -1, or the tick y declared the connection dead
    dropped     \* ghost: a probe was skipped because x's writer queue was full

vars == <<now, q, cur, wire, rb, wrote, lastRead, xl, yl, xp, yp, np, apps,
          blocked, silentAt, deadAt, dropped>>

Max(a, b) == IF a > b THEN a ELSE b

Init ==
    /\ now = 0
    /\ q = <<>> /\ cur = 0 /\ wire = <<>> /\ rb = <<>> /\ wrote = FALSE
    /\ lastRead = 0                         \* the connection was just established
    /\ xl \in 1..P /\ yl \in 1..P           \* the loops run in any phase
    /\ xp = 0 /\ yp = 0 /\ np = [n \in {"x", "y"} |-> 0]
    /\ apps = 0 /\ blocked = FALSE
    /\ silentAt = -1 /\ deadAt = -1 /\ dropped = FALSE

---------------------------------------------------------------------------
(* x *)

\* The application queues a frame of k bytes, if the writer queue has room
\* (a full queue is backpressure to the operator).
App(k) ==
    /\ apps < MaxApp /\ Len(q) < Q
    /\ q' = Append(q, k)
    /\ apps' = apps + 1
    /\ UNCHANGED <<now, cur, wire, rb, wrote, lastRead, xl, yl, xp, yp, np, blocked,
                   silentAt, deadAt, dropped>>

\* x's probe loop queues a probe (one byte), unless the queue is full.
XLoop ==
    /\ xl = 0 /\ xp = 0
    /\ IF Len(q) < Q
       THEN q' = Append(q, 1) /\ UNCHANGED dropped
       ELSE dropped' = TRUE /\ UNCHANGED q
    /\ xl' = P
    /\ UNCHANGED <<now, cur, wire, rb, wrote, lastRead, yl, xp, yp, np, apps, blocked,
                   silentAt, deadAt>>

\* x's writer writes the next byte, one per tick. After the connection fell
\* silent, what it writes never arrives.
Write ==
    LET k == IF cur > 0 THEN cur ELSE Head(q)   \* bytes left of the frame
    IN
    /\ xp = 0 /\ ~wrote /\ (cur > 0 \/ q # <<>>)
    /\ q' = IF cur > 0 THEN q ELSE Tail(q)
    /\ cur' = k - 1
    /\ wire' = IF silentAt >= 0 THEN wire ELSE Append(wire, [at |-> now, eof |-> k = 1])
    /\ wrote' = TRUE
    /\ UNCHANGED <<now, rb, lastRead, xl, yl, xp, yp, np, apps, blocked, silentAt,
                   deadAt, dropped>>

---------------------------------------------------------------------------
(* The link *)

Deliver ==
    /\ wire # <<>>
    /\ rb' = Append(rb, Head(wire).eof)
    /\ wire' = Tail(wire)
    /\ UNCHANGED <<now, q, cur, wrote, lastRead, xl, yl, xp, yp, np, apps, blocked,
                   silentAt, deadAt, dropped>>

\* The connection falls silent, with no error: what is on the wire is lost,
\* and nothing written later arrives. What y already received stays.
GoSilent ==
    /\ Silence /\ silentAt < 0
    /\ silentAt' = now
    /\ wire' = <<>>
    /\ UNCHANGED <<now, q, cur, rb, wrote, lastRead, xl, yl, xp, yp, np, apps, blocked,
                   deadAt, dropped>>

---------------------------------------------------------------------------
(* y *)

\* y's reader reads everything that arrived; any byte read resets the timer
\* (Unit = "frames": only the last byte of a frame does).
Read ==
    /\ yp = 0 /\ ~blocked /\ rb # <<>> /\ deadAt < 0
    /\ lastRead' = IF Unit = "bytes" \/ \E i \in 1..Len(rb) : rb[i] THEN now ELSE lastRead
    /\ rb' = <<>>
    /\ UNCHANGED <<now, q, cur, wire, wrote, xl, yl, xp, yp, np, apps, blocked, silentAt,
                   deadAt, dropped>>

\* y's probe loop applies the rule.
YLoop ==
    /\ yl = 0 /\ yp = 0 /\ deadAt < 0
    /\ deadAt' = IF now - lastRead >= T THEN now ELSE deadAt
    /\ yl' = P
    /\ UNCHANGED <<now, q, cur, wire, rb, wrote, lastRead, xl, xp, yp, np, apps, blocked,
                   silentAt, dropped>>

\* Blocking: a consumer's full queue holds y's reader up, for any time.
Block ==
    /\ Blocking /\ ~blocked
    /\ blocked' = TRUE
    /\ UNCHANGED <<now, q, cur, wire, rb, wrote, lastRead, xl, yl, xp, yp, np, apps,
                   silentAt, deadAt, dropped>>

Unblock ==
    /\ blocked
    /\ blocked' = FALSE
    /\ UNCHANGED <<now, q, cur, wire, rb, wrote, lastRead, xl, yl, xp, yp, np, apps,
                   silentAt, deadAt, dropped>>

---------------------------------------------------------------------------
(* Time *)

\* A node's network tasks pause (a busy runtime, a stop-the-world event).
Pause(n) ==
    /\ np[n] < MaxPauses
    /\ IF n = "x" THEN xp = 0 /\ xp' \in 1..S /\ UNCHANGED yp
                  ELSE yp = 0 /\ yp' \in 1..S /\ UNCHANGED xp
    /\ np' = [np EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<now, q, cur, wire, rb, wrote, lastRead, xl, yl, apps, blocked,
                   silentAt, deadAt, dropped>>

\* A tick passes once nothing that is due is pending.
Tick ==
    /\ now < H
    /\ ~(xl = 0 /\ xp = 0)
    /\ ~(yl = 0 /\ yp = 0 /\ deadAt < 0)
    /\ ~(xp = 0 /\ ~wrote /\ (cur > 0 \/ q # <<>>))
    /\ IF wire = <<>> THEN TRUE ELSE Head(wire).at + D > now
    /\ ~(rb # <<>> /\ yp = 0 /\ ~blocked /\ deadAt < 0)
    /\ now' = now + 1
    /\ xl' = Max(xl - 1, 0) /\ yl' = Max(yl - 1, 0)
    /\ xp' = Max(xp - 1, 0) /\ yp' = Max(yp - 1, 0)
    /\ wrote' = FALSE
    /\ UNCHANGED <<q, cur, wire, rb, lastRead, np, apps, blocked, silentAt, deadAt, dropped>>

Next ==
    \/ \E k \in 1..F : App(k)
    \/ XLoop \/ Write \/ Deliver \/ GoSilent \/ Read \/ YLoop \/ Block \/ Unblock
    \/ \E n \in {"x", "y"} : Pause(n)
    \/ Tick

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
(* Properties *)

TypeOK ==
    /\ now \in 0..H /\ cur \in 0..F /\ lastRead \in 0..H
    /\ Len(q) <= Q /\ \A i \in 1..Len(q) : q[i] \in 1..F
    /\ xl \in 0..P /\ yl \in 0..P /\ xp \in 0..S /\ yp \in 0..S
    /\ silentAt \in -1..H /\ deadAt \in -1..H

\* A connection is declared dead only after it fell silent.
NoFalseDeath == deadAt >= 0 => silentAt >= 0 /\ silentAt <= deadAt

\* A silent connection is declared dead within T + P + S ticks (one pause).
DetectInTime == silentAt >= 0 /\ now >= silentAt + T + P + S => deadAt >= 0

\* Reachability witnesses, deliberately false.
NeverDead == deadAt < 0
NeverProbeSkipped == ~dropped
=============================================================================
