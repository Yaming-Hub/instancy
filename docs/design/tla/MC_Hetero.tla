------------------------------ MODULE MC_Hetero ------------------------------
(* Heterogeneous stages: the input runs only on w1, the count on both,     *)
(* the sink only on w2.                                                    *)
(*   in {w1} -exchange-> u (count) {w1, w2} -exchange-> k (sink) {w2}      *)
(* w1 hosts nothing downstream of u, so it may finish before w2 (PR7's     *)
(* early finisher).                                                        *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2
MCWorkers == {w1, w2}
MCOps == {"in", "u", "k"}
MCEdges == {"e1", "e2"}
MCKind == ("in" :> "input") @@ ("u" :> "notify") @@ ("k" :> "sink")
MCSrc == ("e1" :> "in") @@ ("e2" :> "u")
MCDst == ("e1" :> "u") @@ ("e2" :> "k")
MCPact == ("e1" :> "exchange") @@ ("e2" :> "exchange")
MCHosts == ("in" :> {w1}) @@ ("u" :> {w1, w2}) @@ ("k" :> {w2})

\* Expected to be violated: w1 retires while records are still on their way
\* to w2's sink, outside w1's scope.
NoEarlyFinishWithTail == ~(success[w1] /\ ~success[w2] /\ \E t \in Times : msgs["e2"][w2][t] > 0)
=============================================================================
