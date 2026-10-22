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
