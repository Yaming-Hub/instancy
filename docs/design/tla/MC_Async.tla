------------------------------- MODULE MC_Async ------------------------------
(* in -exchange-> a (unary_async) -broadcast-> k (sink).                   *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2
MCWorkers == {w1, w2}
MCOps == {"in", "a", "k"}
MCEdges == {"e1", "e2"}
MCKind == ("in" :> "input") @@ ("a" :> "async") @@ ("k" :> "sink")
MCSrc == ("e1" :> "in") @@ ("e2" :> "a")
MCDst == ("e1" :> "a") @@ ("e2" :> "k")
MCPact == ("e1" :> "exchange") @@ ("e2" :> "broadcast")
MCHosts == [o \in MCOps |-> MCWorkers]
Sym == Permutations(MCWorkers)
=============================================================================
