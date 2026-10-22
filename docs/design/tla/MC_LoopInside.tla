---------------------------- MODULE MC_LoopInside ----------------------------
(* A count inside a loop body, which the interim design rejects:           *)
(*   in -exchange-> cat -pipe-> n (count, inside) -pipe-> body (branch)    *)
(*   body -exchange-> fb -pipe-> cat                   (around the loop)   *)
(*   body -exchange-> k (sink)                         (out of the loop)   *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2
MCWorkers == {w1, w2}
MCOps == {"in", "cat", "n", "body", "fb", "k"}
MCEdges == {"e1", "e2", "e3", "e4", "e5", "e6"}
MCKind == ("in" :> "input") @@ ("cat" :> "map") @@ ("n" :> "notify")
          @@ ("body" :> "branch") @@ ("fb" :> "map") @@ ("k" :> "sink")
MCSrc == ("e1" :> "in") @@ ("e2" :> "cat") @@ ("e3" :> "n") @@ ("e4" :> "body")
         @@ ("e5" :> "fb") @@ ("e6" :> "body")
MCDst == ("e1" :> "cat") @@ ("e2" :> "n") @@ ("e3" :> "body") @@ ("e4" :> "fb")
         @@ ("e5" :> "cat") @@ ("e6" :> "k")
MCPact == ("e1" :> "exchange") @@ ("e2" :> "pipe") @@ ("e3" :> "pipe")
          @@ ("e4" :> "exchange") @@ ("e5" :> "pipe") @@ ("e6" :> "exchange")
MCHosts == [o \in MCOps |-> MCWorkers]
MCBackEdges == {"e5"}
MCGuards == {<<Out("fb"), Tgt("e6")>>}
MCNoGuards == {}
MCLoopOps == {"cat", "n", "body", "fb"}
MCLapEdges == {"e4"}
Sym == Permutations(MCWorkers)
=============================================================================
