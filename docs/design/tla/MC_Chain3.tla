------------------------------- MODULE MC_Chain3 ------------------------------
(* in -exchange-> u (count) -pipe-> k (sink) on three workers.             *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2, w3
MCWorkers == {w1, w2, w3}
MCOps == {"in", "u", "k"}
MCEdges == {"e1", "e2"}
MCKind == ("in" :> "input") @@ ("u" :> "notify") @@ ("k" :> "sink")
MCSrc == ("e1" :> "in") @@ ("e2" :> "u")
MCDst == ("e1" :> "u") @@ ("e2" :> "k")
MCPact == ("e1" :> "exchange") @@ ("e2" :> "pipe")
MCHosts == [o \in MCOps |-> MCWorkers]
Sym == Permutations(MCWorkers)
=============================================================================
