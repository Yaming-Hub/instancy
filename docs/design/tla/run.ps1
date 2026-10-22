<#
.SYNOPSIS
  Model-check every scenario with TLC and compare with the expected result.

.DESCRIPTION
  First regenerates models/*.cfg from the scenario table in make-models.ps1
  (the configs are generated, not committed). Each models/<name>.cfg starts
  with header comments:
    \* MODULE: <the MC_*.tla module to check>
    \* EXPECT: pass | violated:<invariant> | deadlock | temporal
    \* TLCFLAGS: <extra TLC flags>      (optional)
  A scenario matches only if TLC reports exactly the expected result.

  Requires Java 11+ and tla2tools.jar (https://github.com/tlaplus/tlaplus/releases).

.EXAMPLE
  ./run.ps1 -Jar C:\tools\tla2tools.jar
  ./run.ps1 -Filter 'chain_*' -Threads 2
#>
param(
    [string]$Filter = '*',
    [int]$Threads = 4,
    [string]$Java = $(if ($env:TLC_JAVA) { $env:TLC_JAVA } else { 'java' }),
    [string]$Jar = $(if ($env:TLA2TOOLS_JAR) { $env:TLA2TOOLS_JAR } else { 'tla2tools.jar' }),
    [string]$OutDir = $(Join-Path ([IO.Path]::GetTempPath()) 'pcp-tlc')
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
New-Item -ItemType Directory -Force $OutDir | Out-Null
# models/*.cfg are generated, not committed: create them from the scenario table.
& (Join-Path $here 'make-models.ps1') | Out-Null
$tlcVersion = (& $Java -cp $Jar tlc2.TLC -version 2>&1 | Select-String 'TLC2 Version' | Select-Object -First 1).Line
Write-Host "TLC: $tlcVersion"
$rows = @()

foreach ($cfg in Get-ChildItem (Join-Path $here 'models') -Filter "$Filter.cfg" | Sort-Object Name) {
    $name = $cfg.BaseName
    $header = Get-Content $cfg.FullName -TotalCount 4
    $module = ($header | Select-String '^\\\* MODULE: (\S+)').Matches[0].Groups[1].Value
    $expect = ($header | Select-String '^\\\* EXPECT: (\S+)').Matches[0].Groups[1].Value
    $flagLine = $header | Select-String '^\\\* TLCFLAGS: (.+)$'
    $flags = @(if ($flagLine) { -split $flagLine.Matches[0].Groups[1].Value })
    $log = Join-Path $OutDir "$name.out"
    $meta = Join-Path $OutDir "meta-$name"
    # The spec a scenario checks: the generic spec behind every MC_* module, or the module itself.
    $specFile = if ($module -like 'MC_*') { 'PortCountedProgress.tla' } else { "$module.tla" }
    $specHash = (Get-FileHash (Join-Path $here $specFile)).Hash.Substring(0, 12)

    $sw = [Diagnostics.Stopwatch]::StartNew()
    # A private temp folder: TLC unpacks its standard modules there, and
    # parallel runs sharing one folder can collide.
    $tmp = Join-Path $OutDir "tmp-$name"
    New-Item -ItemType Directory -Force $tmp | Out-Null
    Push-Location $here
    try {
        & $Java -XX:+UseParallelGC "-Djava.io.tmpdir=$tmp" -cp $Jar tlc2.TLC -workers $Threads -cleanup @flags `
            -metadir $meta -config $cfg.FullName "$module.tla" *> $log
    } finally { Pop-Location }
    $sw.Stop()

    $text = Get-Content $log -Raw
    $counts = [regex]::Matches($text, '([\d,]+) distinct states found')
    $states = if ($counts.Count) { [int64]($counts[$counts.Count - 1].Groups[1].Value -replace ',', '') } else { $null }
    $result =
        if ($text -match 'Model checking completed\. No error has been found') { 'pass' }
        elseif ($text -match 'Invariant (\w+) is violated') { "violated:$($Matches[1])" }
        elseif ($text -match 'Temporal properties were violated') { 'temporal' }
        elseif ($text -match 'Deadlock reached') { 'deadlock' }
        else { 'error' }

    $rows += [pscustomobject]@{
        Scenario = $name; Expected = $expect; Result = $result
        Match = $(if ($result -eq $expect) { 'yes' } else { 'NO' })
        States = $states; Seconds = [math]::Round($sw.Elapsed.TotalSeconds, 1)
        Config = (Get-FileHash $cfg.FullName).Hash.Substring(0, 12); Spec = "$specFile@$specHash"
    }
    $r = $rows[-1]
    Write-Host ("{0,-26} expected {1,-36} got {2,-36} {3,-4} {4,12:N0} states {5,8}s" -f `
        $r.Scenario, $r.Expected, $r.Result, $r.Match, $r.States, $r.Seconds)
}

$csv = Join-Path $OutDir 'results.csv'
$rows | Export-Csv -NoTypeInformation -Path $csv -Append
Write-Host "Results appended to $csv; logs with counterexample traces in $OutDir"
if ($rows.Match -contains 'NO') { exit 1 }
