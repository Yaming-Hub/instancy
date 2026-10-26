<#
.SYNOPSIS
  Generate models/*.cfg from the scenario table below. This table is the one
  place that lists every scenario, what it changes, and the exact result
  expected from TLC.

.DESCRIPTION
  Expected results:
    pass          no invariant or property violated, and no deadlock (hang)
    violated:X    TLC finds a state that violates invariant X
    deadlock      TLC finds a stuck state before every worker succeeded
    temporal      TLC finds a behaviour that violates a liveness property
#>
$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$dir = Join-Path $here 'models'
New-Item -ItemType Directory -Force $dir | Out-Null
Get-ChildItem $dir -Filter *.cfg | Remove-Item

# The proposal (PCP), and the bounds used unless a scenario overrides them.
$base = [ordered]@{
    MaxT = '1'; MaxSends = '1'
    Counting = '"ports"'; Wired = '{}'; Decide = '"pcp"'; Frag = '"positivesfirst"'
    Justify = '"local"'; AsyncCap = 'TRUE'; Seeding = '"hosts"'; BcastMult = '"hosts"'
    Egress = '"none"'; Transit = 'FALSE'; KeepOpen = 'FALSE'
    BackEdges = '{}'; Guards = '{}'; LoopOps = '{}'; LapEdges = '{}'; MaxLaps = '0'
}
$safety = @('FrontierSafe', 'NoDataAtDone', 'NoLateData', 'CompleteMeansEmpty', 'NoResidual')

# Interim loops (PR4c until PR6a): the tracker drops back-edges and adds guard
# edges; a cyclic dataflow finishes by exact-zero force-close; frontiers are
# claimed only outside loop bodies (ObserverSafe instead of FrontierSafe).
$loop = @{ BackEdges = '<- MCBackEdges'; Guards = '<- MCGuards'; LoopOps = '<- MCLoopOps'
           LapEdges = '<- MCLapEdges'; MaxLaps = '1'; Decide = '"exactzero"'; Frag = '"none"'; MaxT = '0' }
$loopSafety = @('ObserverSafe', 'NoLateData', 'NoDataAtDone', 'CompleteMeansEmpty')

# Force-close scenarios: records can be in transit, and force-close waits for local idleness.
$fc = @{ Transit = 'TRUE'; Frag = '"none"'; MaxSends = '2'; MaxT = '0' }
function With($a, $b) { $r = @{}; foreach ($h in $a, $b) { foreach ($k in $h.Keys) { $r[$k] = $h[$k] } }; $r }

$scenarios = @(
    # ---- The proposal: must pass ----
    @{ n = 'direct_pcp';   m = 'MC_Direct'; e = 'pass'; d = 'PCP: port counting, S2a shutdown, seals, scoped exact-zero success.' }
    @{ n = 'chain_pcp';    m = 'MC_Chain';  e = 'pass'; d = 'PCP on input -exchange-> count -pipe-> sink, two epochs.' }
    @{ n = 'pipe_pcp';     m = 'MC_Pipe';   e = 'pass'; d = 'PCP with a map and a pipeline edge into the count.'; o = @{ MaxT = '0'; Frag = '"none"' } }
    @{ n = 'async_pcp';    m = 'MC_Async';  e = 'pass'; d = 'PCP with unary_async (a capability per task) and a broadcast.'; o = @{ MaxT = '0'; Frag = '"none"' } }
    @{ n = 'fanout_pcp';   m = 'MC_Fanout'; e = 'pass'; d = 'PCP with one output feeding two exchanges (H12 shape).'; o = @{ MaxT = '0'; Frag = '"none"' } }
    @{ n = 'bcastsub_pcp'; m = 'MC_BcastSub'; e = 'pass'; sym = $false; d = 'PCP with a broadcast to a subset of the workers.'; o = @{ Frag = '"none"' } }
    @{ n = 'hetero_pcp';   m = 'MC_Hetero'; e = 'pass'; sym = $false; d = 'PCP with heterogeneous stages: input on w1, count on both, sink on w2.' }
    @{ n = 'egress_pcp';   m = 'MC_Direct'; e = 'pass'; d = 'PCP with sink egress: the input credit is kept until the receiver accepts.'; o = @{ Egress = '"keep"'; Transit = 'TRUE'; Frag = '"none"' } }
    @{ n = 'direct3_pcp';  m = 'MC_Direct3'; e = 'pass'; d = 'PCP on three workers.'; o = @{ MaxT = '0'; Frag = '"none"' } }
    @{ n = 'chain3_pcp';   m = 'MC_Chain3'; e = 'pass'; d = 'PCP with a count on three workers.'; o = @{ MaxT = '0'; Frag = '"none"' } }

    # ---- Sanity: each must be violated, which shows the behaviour is reachable ----
    @{ n = 'chain_pcp_reach'; m = 'MC_Chain'; flags = '-simulate num=5000000 -depth 150'; e = 'violated:NeverCompletesWithTraffic'; d = 'Completion is reachable after every input sent and a notification fired.'; inv = $safety + 'NeverCompletesWithTraffic' }
    @{ n = 'hetero_early'; m = 'MC_Hetero'; e = 'violated:NoEarlyFinishWithTail'; sym = $false; d = 'w1 can retire while records are still on their way to the sink on w2.'; inv = $safety + 'NoEarlyFinishWithTail' }

    # ---- Completion checks under force-close (records in transit, idle-gated) ----
    @{ n = 'direct_signedsum'; m = 'MC_Direct'; e = 'violated:NoDataAtDone'; d = "Today's check, the sum of all counts. Fails even with complete counting (C1)."; o = (With $fc @{ Decide = '"signedsum"' }) }
    @{ n = 'direct_positive';  m = 'MC_Direct'; e = 'pass'; d = "timely's check, no positive count, with complete counting."; o = (With $fc @{ Decide = '"positive"' }) }
    @{ n = 'direct_exactzero'; m = 'MC_Direct'; e = 'pass'; d = "PR5's check, every count exactly zero, with complete counting."; o = (With $fc @{ Decide = '"exactzero"' }) }
    @{ n = 'egress_exactzero_keep'; m = 'MC_Direct'; e = 'pass'; d = 'Exact-zero force-close with the sink keeping its input credit until accepted.'; o = (With $fc @{ Decide = '"exactzero"'; Egress = '"keep"'; MaxSends = '1' }) }
    @{ n = 'egress_exactzero_release'; m = 'MC_Direct'; e = 'violated:CompleteMeansEmpty'; d = 'Exact-zero force-close with the sink releasing its credit when it takes the record (today, C-15): the run completes with a send still pending. With every invariant, TLC reports FrontierSafe first, because the pending record is invisible.'; o = (With $fc @{ Decide = '"exactzero"'; Egress = '"release"'; MaxSends = '1' }); inv = @('CompleteMeansEmpty', 'NoDataAtDone') }

    # ---- Today's rules, or PCP with one rule removed: must fail as named ----
    @{ n = 'chain_optin_exactzero'; m = 'MC_Chain'; e = 'violated:FrontierSafe'; d = "Today's opt-in counting (the input records no +n, C2) with PR5's exact-zero check."; o = @{ Counting = '"optin"'; Decide = '"exactzero"' } }
    @{ n = 'chain_optin_residual'; m = 'MC_Chain'; e = 'violated:NoResidual'; d = 'Opt-in counting under PCP decisions: the missing +n is reported at the seal boundary.'; o = @{ Counting = '"optin"' }; inv = @('NoResidual') }
    @{ n = 'pipe_optin'; m = 'MC_Pipe'; e = 'violated:FrontierSafe'; d = "Today's opt-in counting with the exchange wired: pipeline edges stay uncounted (H1)."; o = @{ MaxT = '0'; Counting = '"optin"'; Wired = '{"e1"}' } }
    @{ n = 'fanout_sourcekey'; m = 'MC_Fanout'; e = 'violated:FrontierSafe'; d = 'PCP, but produced is filed under the output, not the edge (H12).'; o = @{ MaxT = '0'; Counting = '"sourcekey"' } }
    @{ n = 'chain_capsfirst'; m = 'MC_Chain'; e = 'violated:FrontierSafe'; d = "PCP, but split batches send capability entries first (today's order)."; o = @{ Frag = '"capsfirst"' } }
    @{ n = 'chain_frontier_notify'; m = 'MC_Chain'; e = 'violated:FrontierSafe'; d = "PCP, but notify_at may take a capability whenever the frontier permits (P-9)."; o = @{ Justify = '"frontier"' } }
    @{ n = 'async_nocap'; m = 'MC_Async'; e = 'violated:FrontierSafe'; d = 'PCP, but async tasks hold no capability (today).'; o = @{ MaxT = '0'; AsyncCap = 'FALSE' } }
    @{ n = 'hetero_localseed'; m = 'MC_Hetero'; e = 'violated:FrontierSafe'; sym = $false; d = "Workers start knowing only their own initial capabilities (H13)."; o = @{ Seeding = '"local"' } }
    @{ n = 'hetero_workerseed'; m = 'MC_Hetero'; e = 'deadlock'; sym = $false; d = 'Initial capabilities counted once per worker instead of once per host: a hang, not a loss.'; o = @{ Seeding = '"workers"' } }
    @{ n = 'bcastsub_workersmult'; m = 'MC_BcastSub'; e = 'deadlock'; sym = $false; d = 'Broadcast counted once per worker instead of once per host: a hang, not a loss.'; o = @{ BcastMult = '"workers"' } }

    # ---- Liveness, under weak fairness (no symmetry) ----
    @{ n = 'live_direct'; m = 'MC_Direct'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('EventuallyDone'); d = 'Every worker eventually succeeds.' }
    @{ n = 'live_hetero'; m = 'MC_Hetero'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('EventuallyDone'); d = 'Every worker eventually succeeds, with heterogeneous stages.' }
    @{ n = 'live_chain_open'; m = 'MC_Chain'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('NotifyProgress'); flags = '-deadlock'; d = 'With the input held open at the last epoch, a count at an earlier epoch is still eventually notified.'; o = @{ KeepOpen = 'TRUE'; Frag = '"none"' } }
    @{ n = 'live_hetero_workerseed'; m = 'MC_Hetero'; e = 'temporal'; sym = $false; spec = 'LiveSpec'; prop = @('EventuallyDone'); flags = '-deadlock'; d = 'The wrong initial multiplicity never finishes: the liveness check catches it.'; o = @{ Seeding = '"workers"' } }

    # ---- Interim loops (PR4c until PR6a) ----
    @{ n = 'loop_interim'; m = 'MC_Loop'; e = 'pass'; d = 'A loop with an exchange inside and a count after it: the count never fires while a record can still come out of the loop.'; o = $loop; inv = $loopSafety }
    @{ n = 'loop_reach'; m = 'MC_Loop'; flags = '-simulate num=5000000 -depth 150'; e = 'violated:NeverCompletesAfterLap'; d = 'Sanity: a record goes around the loop, the count after it fires, and the run completes.'; o = $loop; inv = $loopSafety + 'NeverCompletesAfterLap' }
    @{ n = 'loop_noguard'; m = 'MC_Loop'; e = 'violated:ObserverSafe'; d = 'No guard edge (today: the feedback edge is missing from the tracker): a record on its way around the loop is invisible to the count after it.'; o = (With $loop @{ Guards = '<- MCNoGuards' }); inv = $loopSafety }
    @{ n = 'loop_body_frontier'; m = 'MC_Loop'; e = 'violated:FrontierSafe'; d = 'Why the interim claims frontiers only outside loops: inside the body, the tracker can lose sight of a record coming around the back-edge.'; o = $loop; inv = @('FrontierSafe') }
    @{ n = 'loop_inside_notify'; m = 'MC_LoopInside'; e = 'violated:NoLateData'; d = 'A count inside a loop body fires early in the interim, which is why the design rejects it until nested scopes.'; o = $loop; inv = $loopSafety }
    @{ n = 'live_loop'; m = 'MC_LoopSolo'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('EventuallyDone'); d = 'The loop dataflow eventually finishes under fair scheduling (input on w1 only).'; o = $loop; inv = $loopSafety }
    @{ n = 'live_loop_open'; m = 'MC_LoopSolo'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('NotifyProgress'); flags = '-deadlock'; d = 'With the input held open at epoch 1, the count after the loop still reports epoch 0: the guard edge does not freeze it (input on w1 only).'; o = (With $loop @{ MaxT = '1'; KeepOpen = 'TRUE' }); inv = $loopSafety }
)

foreach ($s in $scenarios) {
    $c = [ordered]@{}; foreach ($k in $base.Keys) { $c[$k] = $base[$k] }
    if ($s.o) { foreach ($k in $s.o.Keys) { $c[$k] = $s.o[$k] } }
    $workers = if ($s.m -like '*3') { @('w1', 'w2', 'w3') } else { @('w1', 'w2') }
    $lines = @("\* MODULE: $($s.m)", "\* EXPECT: $($s.e)")
    if ($s.flags) { $lines += "\* TLCFLAGS: $($s.flags)" }
    $lines += "\* $($s.d)"
    $lines += 'CONSTANTS'
    foreach ($w in $workers) { $lines += "    $w = $w" }
    foreach ($k in 'Workers', 'Ops', 'Edges', 'Kind', 'Src', 'Dst', 'Pact', 'Hosts') { $lines += "    $k <- MC$k" }
    foreach ($k in $c.Keys) {
        $v = $c[$k]
        $lines += if ($v -like '<-*') { "    $k $v" } else { "    $k = $v" }
    }
    if ($s.sym -ne $false) { $lines += 'SYMMETRY Sym' }
    $lines += "SPECIFICATION $(if ($s.spec) { $s.spec } else { 'Spec' })"
    $inv = if ($s.inv) { $s.inv } else { $safety }
    $lines += 'INVARIANTS'; foreach ($i in $inv) { $lines += "    $i" }
    if ($s.prop) { $lines += 'PROPERTIES'; foreach ($p in $s.prop) { $lines += "    $p" } }
    Set-Content -Path (Join-Path $dir "$($s.n).cfg") -Value $lines -Encoding ascii
}
"$($scenarios.Count) scenarios written to $dir"

# ---- Nested loop scopes (LoopScopes.tla, PR6a end state) ----
$scopeBase = [ordered]@{ MaxT = '0'; MaxIter = '1'; MaxSends = '2'; MaxLaps = '1'; Import = 'TRUE'; Export = '"internal"'; Frag = '"none"'; KeepOpen = 'FALSE' }
$scopeSafety = @('ScopeSafe', 'ParentSafe', 'NoLateData', 'NoDataAtDone', 'CompleteMeansEmpty', 'NoResidual')
$whole = @{ Frag = '"none"' }
$scopeScenarios = @(
    @{ n = 'scope_pcp'; e = 'pass'; d = 'Nested scope: counts inside and after the loop never fire early, nothing is lost, nothing hangs. Two records, whole commits.' }
    @{ n = 'scope_pcp_split'; e = 'pass'; d = 'Commits may be split positives-first across both levels. One record, which is all the split hazard needs.'; o = @{ Frag = '"positivesfirst"'; MaxSends = '1' } }
    @{ n = 'scope_reach'; flags = '-simulate num=5000000 -depth 150'; e = 'violated:NeverCompletesAfterLap'; d = 'Sanity: a record goes around the loop, both counts report, and the run completes.'; inv = $scopeSafety + 'NeverCompletesAfterLap' }
    @{ n = 'scope_noimport'; e = 'violated:ScopeSafe'; d = "No downward import: the scope's frontiers ignore records the parent may still send in."; o = @{ Import = 'FALSE' } }
    @{ n = 'scope_noexport'; e = 'violated:ParentSafe'; d = "No upward export: the parent's frontiers ignore records still inside the loop."; o = @{ Export = '"none"' } }
    @{ n = 'scope_parentfirst'; e = 'violated:ScopeSafe'; d = "Split commits send the parent's section first: after enter, a peer briefly loses the record's only support."; o = @{ Frag = '"parentfirst"'; MaxSends = '1' } }
    @{ n = 'scope_childfirst'; e = 'violated:ParentSafe'; d = "Split commits send the scope's section first: after leave, a peer briefly sees neither the record inside nor outside."; o = @{ Frag = '"childfirst"'; MaxSends = '1' } }
    @{ n = 'live_scope'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('EventuallyDone'); d = 'The nested-scope dataflow eventually finishes under fair scheduling.' }
    @{ n = 'live_scope_open'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('ScopeProgress'); flags = '-deadlock'; d = 'With the input held open at epoch 1, both counts still report epoch 0, including after a lap.'; o = @{ MaxT = '1'; KeepOpen = 'TRUE' } }
)
foreach ($s in $scopeScenarios) {
    $c = [ordered]@{}; foreach ($k in $scopeBase.Keys) { $c[$k] = $scopeBase[$k] }
    if ($s.o) { foreach ($k in $s.o.Keys) { $c[$k] = $s.o[$k] } }
    $lines = @('\* MODULE: LoopScopes', "\* EXPECT: $($s.e)")
    if ($s.flags) { $lines += "\* TLCFLAGS: $($s.flags)" }
    $lines += "\* $($s.d)"
    $lines += 'CONSTANTS'; $lines += '    w1 = w1'; $lines += '    w2 = w2'; $lines += '    Workers = {w1, w2}'
    foreach ($k in $c.Keys) { $lines += "    $k = $($c[$k])" }
    if ($s.sym -ne $false) { $lines += 'SYMMETRY Sym' }
    $lines += "SPECIFICATION $(if ($s.spec) { $s.spec } else { 'Spec' })"
    $inv = if ($s.inv) { $s.inv } else { $scopeSafety }
    $lines += 'INVARIANTS'; foreach ($i in $inv) { $lines += "    $i" }
    if ($s.prop) { $lines += 'PROPERTIES'; foreach ($p in $s.prop) { $lines += "    $p" } }
    Set-Content -Path (Join-Path $dir "$($s.n).cfg") -Value $lines -Encoding ascii
}
"$($scopeScenarios.Count) nested-scope scenarios written to $dir"

# ---- A loop scope on an outer loop's feedback path (NestedScopes.tla, O1) ----
$nestBase = [ordered]@{ MaxT = '0'; MaxJ = '1'; MaxK = '1'; MaxSends = '1'; LapsO = '1'; LapsI = '1'
                        Export = '"internal"'; Sat = 'FALSE'; Frag = '"none"'; KeepOpen = 'FALSE'; Track = 'FALSE' }
$nestSafety = @('InnerSafe', 'OuterSafe', 'RootSafe', 'NoLateData', 'NoDataAtDone', 'CompleteMeansEmpty', 'NoResidual', 'DerivedZero', 'CacheExact')
$nestMutSafety = $nestSafety | Where-Object { $_ -notin 'DerivedZero', 'CacheExact' }
$nestScenarios = @(
    @{ n = 'nest_pcp'; e = 'pass'; d = 'Inner loop on the outer feedback path: no frontier at any level passes outstanding work, nothing is lost, nothing hangs, and the cached links always equal a fresh recompute.' }
    @{ n = 'nest_pcp_sat'; e = 'pass'; d = 'The same with the outer feedback saturating at MaxJ (unbounded iterations): still finishes.'; o = @{ Sat = 'TRUE' } }
    @{ n = 'nest_pcp_split'; e = 'pass'; d = 'Commits may be split positives-first across all three levels. One record through both loops, no laps: the split hazard is records crossing levels, which entering and leaving both loops exercises. (Laps make the space too large to finish.)'; o = @{ Frag = '"positivesfirst"'; MaxJ = '0'; LapsO = '0'; LapsI = '0'; MaxK = '0' } }
    @{ n = 'nest_pcp_twolaps'; e = 'pass'; d = 'Two real outer laps (MaxJ = 2), so a record can re-enter the inner loop twice.'; o = @{ MaxJ = '2'; LapsO = '2'; LapsI = '0'; MaxK = '0' } }
    @{ n = 'nest_reach'; flags = '-simulate num=5000000 -depth 200'; e = 'violated:NeverCompletesAfterLaps'; d = 'Sanity: a record laps the inner loop, crosses workers on an outer lap, re-enters the inner loop at lap 1, the count reports, and the run completes.'; inv = $nestSafety + 'NeverCompletesAfterLaps'; o = @{ Sat = 'TRUE'; Track = 'TRUE' } }
    @{ n = 'nest_frontierexport'; e = 'deadlock'; d = 'The inner loop exports its frontier at the exit, import included: its export keeps supporting its own next import, and the run never finishes.'; inv = $nestMutSafety; o = @{ Export = '"frontier"'; Sat = 'TRUE'; MaxSends = '0' } }
    @{ n = 'nest_frontierexport_derived'; e = 'violated:DerivedZero'; d = 'The same mutation, caught by the R38 assertion: raw counts are zero but the cached export is not.'; inv = $nestMutSafety + 'DerivedZero'; o = @{ Export = '"frontier"'; Sat = 'TRUE'; MaxSends = '0' } }
    @{ n = 'live_nest'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('EventuallyDone'); d = 'Every run finishes under fair scheduling, with the outer feedback saturating. One outer lap through the inner loop; no inner laps.'; o = @{ Sat = 'TRUE'; LapsI = '0'; MaxK = '0' } }
    @{ n = 'live_nest_open'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('UProgress'); flags = '-deadlock'; d = 'With the input held open at epoch 1, the count after the outer loop still reports epoch 0.'; o = @{ Sat = 'TRUE'; MaxT = '1'; KeepOpen = 'TRUE'; LapsI = '0'; MaxK = '0' } }
    @{ n = 'live_nest_open_frontierexport'; e = 'temporal'; sym = $false; spec = 'LiveSpec'; prop = @('UProgress'); flags = '-deadlock'; d = 'With the frontier-export mutation, epoch 0 never reports: the inner export never drains.'; inv = $nestMutSafety; o = @{ Export = '"frontier"'; Sat = 'TRUE'; MaxT = '1'; KeepOpen = 'TRUE'; LapsI = '0'; MaxK = '0' } }
)
foreach ($s in $nestScenarios) {
    $c = [ordered]@{}; foreach ($k in $nestBase.Keys) { $c[$k] = $nestBase[$k] }
    if ($s.o) { foreach ($k in $s.o.Keys) { $c[$k] = $s.o[$k] } }
    $lines = @('\* MODULE: NestedScopes', "\* EXPECT: $($s.e)")
    if ($s.flags) { $lines += "\* TLCFLAGS: $($s.flags)" }
    $lines += "\* $($s.d)"
    $lines += 'CONSTANTS'; $lines += '    w1 = w1'; $lines += '    w2 = w2'; $lines += '    Workers = {w1, w2}'
    foreach ($k in $c.Keys) { $lines += "    $k = $($c[$k])" }
    if ($s.sym -ne $false) { $lines += 'SYMMETRY Sym' }
    $lines += "SPECIFICATION $(if ($s.spec) { $s.spec } else { 'Spec' })"
    $inv = if ($s.inv) { $s.inv } else { $nestSafety }
    $lines += 'INVARIANTS'; foreach ($i in $inv) { $lines += "    $i" }
    if ($s.prop) { $lines += 'PROPERTIES'; foreach ($p in $s.prop) { $lines += "    $p" } }
    Set-Content -Path (Join-Path $dir "$($s.n).cfg") -Value $lines -Encoding ascii
}
"$($nestScenarios.Count) outer-feedback scenarios written to $dir"

# ---- Cluster transport and teardown (ClusterTransport.tla, O16) ----
# The PR9 design, and the bounds used unless a scenario overrides them.
$ctBase = [ordered]@{
    Conns = '{1}'; Runs = '{1}'; Concurrent = 'FALSE'; Crosswise = 'FALSE'; Reuse = 'FALSE'; Faults = '{}'
    MaxFaults = '0'; FaultsRun1 = 'FALSE'; MaxStops = '0'; MaxFails = '0'; PerJob = 'TRUE'
    Pin = 'TRUE'; WriteErrClose = 'TRUE'; Split = 'TRUE'; CleanEof = '"keep"'; Linger = 'TRUE'
    RunIds = '"pr9"'; RejectCancel = 'FALSE'; Crc = 'TRUE'
}
$ctSafety = @('TypeOK', 'NoLossOk', 'NoCrossRun', 'NoCorruptAccepted', 'NoNeedlessDrop', 'NoSpuriousFail')
$allFaults = '{"reset", "writeerr", "corrupt"}'
$ctScenarios = @(
    # ---- PR9: must pass ----
    @{ n = 'ct_pr9'; e = 'pass'; d = 'PR9, healthy transport, with a requested stop: nothing lost, crossed or dropped, no run fails, nothing hangs.'; o = @{ MaxStops = '1' } }
    @{ n = 'ct_pr9_faults'; e = 'pass'; d = 'PR9 with one reset, write error or corrupted frame, and a requested stop: never Ok with lost or corrupted data.'; o = @{ Faults = $allFaults; MaxFaults = '1'; MaxStops = '1' } }
    @{ n = 'ct_pr9_concurrent'; e = 'pass'; d = 'PR9: two dataflows pinned alike to two connections, one fault: it fails only the dataflow using that connection.'; o = @{ Conns = '{1, 2}'; Runs = '{1, 2}'; Concurrent = 'TRUE'; Faults = $allFaults; MaxFaults = '1' }; inv = $ctSafety + 'NoCollateralFail' }
    @{ n = 'ct_pr9_crosswise'; e = 'pass'; d = 'PR9: two dataflows pinned crosswise (node b registered them in the other order), so each connection carries both; one reset (every fault kind breaks the whole connection), managers kept: a dataflow fails only if it used the faulted connection in either direction.'; o = @{ Conns = '{1, 2}'; Runs = '{1, 2}'; Concurrent = 'TRUE'; Crosswise = 'TRUE'; PerJob = 'FALSE'; Faults = '{"reset"}'; MaxFaults = '1' }; inv = $ctSafety + 'NoCollateralFail' }
    @{ n = 'ct_pr9_retry'; e = 'pass'; d = 'PR9: run 1 fails on one node, and the retry with a fresh DataflowId succeeds on both, never taking run 1''s late frames.'; o = @{ Runs = '{1, 2}'; PerJob = 'FALSE'; MaxFails = '1' }; inv = $ctSafety + 'FreshRetryOk' }
    @{ n = 'ct_pr9_reuse'; e = 'pass'; d = 'PR9 with the spawn guard fixed (sends nothing under an id that is another run''s): a retry that reuses the DataflowId is refused on both nodes, and no run takes another run''s frames or fails because of the refusal.'; o = @{ Runs = '{1, 2}'; Reuse = 'TRUE'; PerJob = 'FALSE'; MaxFails = '1' }; inv = @('TypeOK', 'NoLossOk', 'NoCrossRun', 'ReuseRejected', 'NoSpuriousFail') }
    @{ n = 'live_ct'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('Termination'); d = 'PR9: with one fault of any kind and a requested stop, every run ends and every connection closes on both nodes.'; o = @{ Faults = $allFaults; MaxFaults = '1'; MaxStops = '1' } }
    @{ n = 'live_ct_retry'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('Termination', 'RetryEventuallyOk'); d = 'PR9: a reset during run 1 fails it; once each node noticed the reset, its retry''s registration refills the pool and the retry succeeds on both nodes.'; o = @{ Runs = '{1, 2}'; PerJob = 'FALSE'; Faults = '{"reset"}'; MaxFaults = '1'; FaultsRun1 = 'TRUE' } }
    @{ n = 'live_ct_lostcancel'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('Termination'); d = 'PR9: a reset may hit either run, including a retry whose peer has not registered yet, so its Cancel is lost with the connection; the peer''s retry then ends by the handshake timeout, never hangs.'; o = @{ Runs = '{1, 2}'; PerJob = 'FALSE'; Faults = '{"reset"}'; MaxFaults = '1' } }
    @{ n = 'live_ct_freshretry'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('Termination', 'RetryEventuallyOk'); d = 'PR9: run 1 may fail on one node; the retry with a fresh DataflowId eventually succeeds on both nodes.'; o = @{ Runs = '{1, 2}'; PerJob = 'FALSE'; MaxFails = '1' } }
    @{ n = 'live_ct_reuse'; e = 'pass'; sym = $false; spec = 'LiveSpec'; prop = @('Termination', 'ReuseEventuallyRejected'); d = 'PR9 with R53''s rule: a retry that reuses the DataflowId is eventually refused on both nodes, and every run ends.'; o = @{ Runs = '{1, 2}'; Reuse = 'TRUE'; PerJob = 'FALSE'; MaxFails = '1' } }

    # ---- Sanity: each must be violated, which shows the behaviour is reachable ----
    @{ n = 'ct_reach_stop'; e = 'violated:NeverOkAfterStop'; d = 'A requested stop drains to success on both nodes.'; o = @{ MaxStops = '1' }; inv = $ctSafety + 'NeverOkAfterStop' }
    @{ n = 'ct_reach_retry'; e = 'violated:NeverRetryOk'; d = 'Run 1 fails, and the retry succeeds on both nodes.'; o = @{ Runs = '{1, 2}'; PerJob = 'FALSE'; MaxFails = '1' }; inv = $ctSafety + 'NeverRetryOk' }
    @{ n = 'ct_reach_contained'; e = 'violated:NeverContained'; d = 'A fault fails the dataflow on one connection while the other dataflow succeeds on both nodes.'; o = @{ Conns = '{1, 2}'; Runs = '{1, 2}'; Concurrent = 'TRUE'; Faults = '{"reset"}'; MaxFaults = '1' }; inv = $ctSafety + 'NeverContained' }

    # ---- This round's bugs: the fix removed, must fail as named ----
    @{ n = 'ct_c7'; e = 'violated:NoLossOk'; flags = '-deadlock'; d = 'Audit C7: in the earlier design, frames the bridge accepted while the reset connection was being redialled are dropped without a number; the peer completes without the record.'; o = @{ Pin = 'FALSE'; Faults = '{"reset"}'; MaxFaults = '1' }; inv = @('NoLossOk') }
    @{ n = 'ct_c7_hang'; e = 'deadlock'; d = 'The earlier design''s other silent failure: frames lost with a redialled connection, with nothing after them to show a gap, leave both runs waiting forever.'; o = @{ Pin = 'FALSE'; Faults = '{"reset"}'; MaxFaults = '1' }; inv = @('NoLossOk') }
    @{ n = 'ct_old_healthy'; e = 'pass'; d = 'Control: the earlier design with one connection and no fault loses nothing; the reconnect is what loses frames.'; o = @{ Pin = 'FALSE' } }
    @{ n = 'ct_h7'; e = 'violated:NoSpuriousFail'; d = 'Audit H7: with a healthy transport, the earlier design''s gap timer fails a run whose frames are only reordered across connections.'; o = @{ Pin = 'FALSE'; Conns = '{1, 2}' }; inv = @('NoSpuriousFail') }
    @{ n = 'ct_writeerr_halfclose'; e = 'deadlock'; d = 'Before 4be0e70: a write error drops only the write half; with split halves the peer sees nothing and its run waits forever. (PerJob off: the deadlock must be a run hang, not the model''s unbounded linger.)'; o = @{ WriteErrClose = 'FALSE'; PerJob = 'FALSE'; Faults = '{"writeerr"}'; MaxFaults = '1' } }
    @{ n = 'ct_writeerr_halfclose_owned'; e = 'pass'; d = 'Control: the same with owned halves, whose drop sends FIN, ends cleanly. Split halves are what made the bug.'; o = @{ WriteErrClose = 'FALSE'; Split = 'FALSE'; PerJob = 'FALSE'; Faults = '{"writeerr"}'; MaxFaults = '1' } }
    @{ n = 'ct_cleaneof_fails'; e = 'violated:NoNeedlessDrop'; d = 'Before 3a77274: a peer''s clean close fails the lanes pinned to the connection, so frames are dropped although the peer is still reading.'; o = @{ CleanEof = '"fail"'; MaxStops = '1' }; inv = @('NoNeedlessDrop') }
    @{ n = 'ct_nolinger'; e = 'violated:NoSpuriousFail'; d = 'A release that closes its sockets at once drops the frames still queued (or resets a connection with unread input), and the peer loses the last frames it needed.'; o = @{ Linger = 'FALSE'; MaxStops = '1' }; inv = @('NoSpuriousFail') }
    @{ n = 'ct_reuse_old'; e = 'violated:NoCrossRun'; d = 'Before 52314fb: a retry that reuses the DataflowId takes the earlier run''s frames as its own.'; o = @{ Runs = '{1, 2}'; Reuse = 'TRUE'; PerJob = 'FALSE'; MaxFails = '1'; RunIds = '"old"' }; inv = @('NoCrossRun') }
    @{ n = 'ct_nocrc'; e = 'violated:NoCorruptAccepted'; d = 'Before protocol v6 (Dedicated mode never checksummed frames): a corrupted record is accepted.'; o = @{ Crc = 'FALSE'; Faults = '{"corrupt"}'; MaxFaults = '1' }; inv = @('NoCorruptAccepted') }

    # ---- Open on main (a7c4b1c): must fail as named until fixed ----
    @{ n = 'ct_reuse_cancel'; e = 'violated:NoSpuriousFail'; d = 'a7c4b1c: a spawn refused for a reused DataflowId broadcasts a failure Cancel under that id; the peer''s run 1, which a finished with, takes it as its own and fails although nothing failed.'; o = @{ Runs = '{1, 2}'; Reuse = 'TRUE'; PerJob = 'FALSE'; RejectCancel = 'TRUE' }; inv = @('NoSpuriousFail') }
)
foreach ($s in $ctScenarios) {
    $c = [ordered]@{}; foreach ($k in $ctBase.Keys) { $c[$k] = $ctBase[$k] }
    if ($s.o) { foreach ($k in $s.o.Keys) { $c[$k] = $s.o[$k] } }
    $lines = @('\* MODULE: ClusterTransport', "\* EXPECT: $($s.e)")
    if ($s.flags) { $lines += "\* TLCFLAGS: $($s.flags)" }
    $lines += "\* $($s.d)"
    $lines += 'CONSTANTS'
    foreach ($k in $c.Keys) { $lines += "    $k = $($c[$k])" }
    $lines += "SPECIFICATION $(if ($s.spec) { $s.spec } else { 'Spec' })"
    $inv = if ($s.inv) { $s.inv } else { $ctSafety }
    $lines += 'INVARIANTS'; foreach ($i in $inv) { $lines += "    $i" }
    if ($s.prop) { $lines += 'PROPERTIES'; foreach ($p in $s.prop) { $lines += "    $p" } }
    Set-Content -Path (Join-Path $dir "$($s.n).cfg") -Value $lines -Encoding ascii
}
"$($ctScenarios.Count) cluster-transport scenarios written to $dir"
