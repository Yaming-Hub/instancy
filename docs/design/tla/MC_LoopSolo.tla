----------------------------- MODULE MC_LoopSolo -----------------------------
(* As MC_Loop, with the input on w1 only: one record source, for the       *)
(* liveness checks (which cannot use worker symmetry).                     *)
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
MCHosts == [o \in MCOps |-> IF o = "in" THEN {w1} ELSE MCWorkers]
MCBackEdges == {"e3"}
MCGuards == {<<Out("fb"), Tgt("e4")>>}
MCNoGuards == {}
MCLoopOps == {"body", "fb"}
MCLapEdges == {"e2"}
=============================================================================
