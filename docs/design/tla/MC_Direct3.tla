------------------------------ MODULE MC_Direct3 ------------------------------
(* in -exchange-> k (sink) on three workers, so that a third worker can    *)
(* observe a producer and a consumer through two separate lanes.           *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2, w3
MCWorkers == {w1, w2, w3}
MCOps == {"in", "k"}
MCEdges == {"e1"}
MCKind == ("in" :> "input") @@ ("k" :> "sink")
MCSrc == ("e1" :> "in")
MCDst == ("e1" :> "k")
MCPact == ("e1" :> "exchange")
MCHosts == [o \in MCOps |-> MCWorkers]
Sym == Permutations(MCWorkers)
=============================================================================
