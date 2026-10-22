------------------------------ MODULE MC_Direct ------------------------------
(* in -exchange-> k (sink): the smallest shape for today's completion bug. *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2
MCWorkers == {w1, w2}
MCOps == {"in", "k"}
MCEdges == {"e1"}
MCKind == ("in" :> "input") @@ ("k" :> "sink")
MCSrc == ("e1" :> "in")
MCDst == ("e1" :> "k")
MCPact == ("e1" :> "exchange")
MCHosts == [o \in MCOps |-> MCWorkers]
Sym == Permutations(MCWorkers)
=============================================================================
