------------------------------- MODULE MC_Fanout ------------------------------
(* One output feeding two exchanges (today's H12 shape):                   *)
(*   in -exchange-> k1 (sink)                                              *)
(*   in -exchange-> u (count) -pipe-> k2 (sink)                            *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2
MCWorkers == {w1, w2}
MCOps == {"in", "k1", "u", "k2"}
MCEdges == {"e1", "e2", "e3"}
MCKind == ("in" :> "input") @@ ("k1" :> "sink") @@ ("u" :> "notify") @@ ("k2" :> "sink")
MCSrc == ("e1" :> "in") @@ ("e2" :> "in") @@ ("e3" :> "u")
MCDst == ("e1" :> "k1") @@ ("e2" :> "u") @@ ("e3" :> "k2")
MCPact == ("e1" :> "exchange") @@ ("e2" :> "exchange") @@ ("e3" :> "pipe")
MCHosts == [o \in MCOps |-> MCWorkers]
Sym == Permutations(MCWorkers)
=============================================================================
