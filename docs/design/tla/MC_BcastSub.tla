------------------------------ MODULE MC_BcastSub -----------------------------
(* A broadcast to a proper subset of the workers:                          *)
(*   in {w1} -exchange-> m (map) {w1, w2} -broadcast-> k (sink) {w2}       *)
(* Its multiplicity is 1 (the hosts of k), not 2 (all workers).            *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2
MCWorkers == {w1, w2}
MCOps == {"in", "m", "k"}
MCEdges == {"e1", "e2"}
MCKind == ("in" :> "input") @@ ("m" :> "map") @@ ("k" :> "sink")
MCSrc == ("e1" :> "in") @@ ("e2" :> "m")
MCDst == ("e1" :> "m") @@ ("e2" :> "k")
MCPact == ("e1" :> "exchange") @@ ("e2" :> "broadcast")
MCHosts == ("in" :> {w1}) @@ ("m" :> {w1, w2}) @@ ("k" :> {w2})
=============================================================================
