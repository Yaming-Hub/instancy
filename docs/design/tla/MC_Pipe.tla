------------------------------- MODULE MC_Pipe -------------------------------
(* in -exchange-> m (map) -pipe-> u (count) -pipe-> k (sink).              *)
(* The pipeline edge into the count is where today's H1 hides records.     *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2
MCWorkers == {w1, w2}
MCOps == {"in", "m", "u", "k"}
MCEdges == {"e1", "e2", "e3"}
MCKind == ("in" :> "input") @@ ("m" :> "map") @@ ("u" :> "notify") @@ ("k" :> "sink")
MCSrc == ("e1" :> "in") @@ ("e2" :> "m") @@ ("e3" :> "u")
MCDst == ("e1" :> "m") @@ ("e2" :> "u") @@ ("e3" :> "k")
MCPact == ("e1" :> "exchange") @@ ("e2" :> "pipe") @@ ("e3" :> "pipe")
MCHosts == [o \in MCOps |-> MCWorkers]
Sym == Permutations(MCWorkers)
=============================================================================
