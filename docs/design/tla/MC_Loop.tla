------------------------------- MODULE MC_Loop -------------------------------
(* A loop whose records may change worker on each lap, an explicit leave,  *)
(* and a count after it:                                                   *)
(*   in -pipe-> body (branch, loop head)                                   *)
(*   body -exchange-> fb (feedback) -pipe-> body       (around the loop)   *)
(*   body -pipe-> lv (leave) -pipe-> u (count)         (out of the loop)   *)
(* e3 (fb -> body) is the back-edge; e2 costs one lap. The interim tracker *)
(* drops e3 and adds the guard edge Out(fb) -> Tgt(e4), the leave's input. *)
(* The leave and everything after it are outside the loop body.            *)
EXTENDS PortCountedProgress, TLC
CONSTANTS w1, w2
MCWorkers == {w1, w2}
MCOps == {"in", "body", "fb", "lv", "u"}
MCEdges == {"e1", "e2", "e3", "e4", "e5"}
MCKind == ("in" :> "input") @@ ("body" :> "branch") @@ ("fb" :> "map")
          @@ ("lv" :> "map") @@ ("u" :> "notify")
MCSrc == ("e1" :> "in") @@ ("e2" :> "body") @@ ("e3" :> "fb") @@ ("e4" :> "body") @@ ("e5" :> "lv")
MCDst == ("e1" :> "body") @@ ("e2" :> "fb") @@ ("e3" :> "body") @@ ("e4" :> "lv") @@ ("e5" :> "u")
MCPact == ("e1" :> "pipe") @@ ("e2" :> "exchange") @@ ("e3" :> "pipe")
          @@ ("e4" :> "pipe") @@ ("e5" :> "pipe")
MCHosts == [o \in MCOps |-> MCWorkers]
MCBackEdges == {"e3"}
MCGuards == {<<Out("fb"), Tgt("e4")>>}
MCNoGuards == {}
MCLoopOps == {"body", "fb"}
MCLapEdges == {"e2"}
Sym == Permutations(MCWorkers)
=============================================================================
