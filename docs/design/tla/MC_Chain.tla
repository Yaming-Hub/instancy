------------------------------- MODULE MC_Chain -------------------------------
(* in -exchange-> u (count) -pipe-> k (sink); every operator on every worker. *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2
MCWorkers == {w1, w2}
MCOps == {"in", "u", "k"}
MCEdges == {"e1", "e2"}
MCKind == ("in" :> "input") @@ ("u" :> "notify") @@ ("k" :> "sink")
MCSrc == ("e1" :> "in") @@ ("e2" :> "u")
MCDst == ("e1" :> "u") @@ ("e2" :> "k")
MCPact == ("e1" :> "exchange") @@ ("e2" :> "pipe")
MCHosts == [o \in MCOps |-> MCWorkers]
Sym == Permutations(MCWorkers)
=============================================================================
