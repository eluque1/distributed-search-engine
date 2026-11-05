param(
    [Parameter(Mandatory = $false)]
    [string]$LogPath = "",

    [Parameter(Mandatory = $false)]
    [int]$Tail = 25,

    [Parameter(Mandatory = $false)]
    [int]$RefreshSeconds = 2
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-LatestIndexerMergeLog {
    param([string[]]$SearchRoots)

    $matches = @()
    foreach ($root in $SearchRoots) {
        if ([string]::IsNullOrWhiteSpace($root)) {
            continue
        }
        if (-not (Test-Path -LiteralPath $root)) {
            continue
        }

        $files = Get-ChildItem -LiteralPath $root -File -Filter "IndexerMerge_*.log" -ErrorAction SilentlyContinue
        if ($files) {
            $matches += $files
        }
    }

    if (-not $matches) {
        return $null
    }

    return ($matches | Sort-Object LastWriteTime -Descending | Select-Object -First 1)
}

function Parse-Timestamp {
    param([string]$Line)

    if ($Line -match '^\[(?<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]') {
        return [DateTime]::ParseExact($Matches.ts, "yyyy-MM-dd HH:mm:ss", $null)
    }
    return $null
}

function Update-StateFromLine {
    param(
        [hashtable]$State,
        [string]$Line
    )

    $State.LastMessage = $Line

    $parsedTs = Parse-Timestamp -Line $Line
    if ($parsedTs) {
        if (-not $State.StartTimestamp) {
            $State.StartTimestamp = $parsedTs
        }
        $State.LastTimestamp = $parsedTs
    }

    if ($Line -match 'flatMapToPair\(\)\s+took\s+(\d+)ms') {
        $ms = [int]$Matches[1]
        $State.LastFlatMapMs = $ms
        $State.FlatMapCount += 1
        $State.FlatMapTotalMs += $ms
    }

    if ($Line -match 'foldByKey\(\)\s+took\s+(\d+)ms') {
        $ms = [int]$Matches[1]
        $State.LastFoldByKeyMs = $ms
        $State.FoldByKeyCount += 1
        $State.FoldByKeyTotalMs += $ms
    }

    if ($Line -match 'Batch\s+(\d+)\s+completed') {
        $batchId = [int]$Matches[1]
        if ($batchId -gt $State.LastBatchId) {
            $State.LastBatchId = $batchId
            $State.BatchesCompleted = $batchId + 1
        }
    }

    if ($Line -match '(IndexerMerge job completed!|--- Job Output End ---)') {
        $State.IsCompleted = $true
    }

    if ($Line -match '(?i)(error|exception|failed)') {
        $State.RecentWarnings.Add($Line) | Out-Null
        while ($State.RecentWarnings.Count -gt 5) {
            $State.RecentWarnings.RemoveAt(0)
        }
    }
}

function Format-Ms {
    param([long]$Value)
    if ($Value -lt 0) {
        return "n/a"
    }
    if ($Value -gt 1000) {
        return ("{0:N2}s" -f ($Value / 1000.0))
    }
    return "$Value ms"
}

function Render-Dashboard {
    param(
        [string]$ResolvedPath,
        [hashtable]$State,
        [string[]]$AllLines,
        [int]$TailCount
    )

    Clear-Host

    $status = if ($State.IsCompleted) { "Completed" } else { "Running" }
    $elapsed = "n/a"
    if ($State.StartTimestamp) {
        $endTs = if ($State.LastTimestamp) { $State.LastTimestamp } else { Get-Date }
        $elapsed = ([TimeSpan]($endTs - $State.StartTimestamp)).ToString()
    }

    $avgFlat = if ($State.FlatMapCount -gt 0) { [long]($State.FlatMapTotalMs / $State.FlatMapCount) } else { -1 }
    $avgFold = if ($State.FoldByKeyCount -gt 0) { [long]($State.FoldByKeyTotalMs / $State.FoldByKeyCount) } else { -1 }

    Write-Host "IndexerMerge Monitor" -ForegroundColor Cyan
    Write-Host ("Log file     : {0}" -f $ResolvedPath)
    Write-Host ("Status       : {0}" -f $status)
    Write-Host ("Elapsed      : {0}" -f $elapsed)
    Write-Host ("Batches done : {0}" -f $State.BatchesCompleted)
    Write-Host ("Last batch   : {0}" -f $(if ($State.LastBatchId -ge 0) { $State.LastBatchId } else { "n/a" }))
    Write-Host ("flatMap last : {0} | avg: {1}" -f (Format-Ms $State.LastFlatMapMs), (Format-Ms $avgFlat))
    Write-Host ("foldByKey last: {0} | avg: {1}" -f (Format-Ms $State.LastFoldByKeyMs), (Format-Ms $avgFold))
    Write-Host ("Updated      : {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))

    if ($State.RecentWarnings.Count -gt 0) {
        Write-Host ""
        Write-Host "Warnings/Errors (recent):" -ForegroundColor Yellow
        foreach ($line in $State.RecentWarnings) {
            Write-Host ("- {0}" -f $line) -ForegroundColor Yellow
        }
    }

    Write-Host ""
    Write-Host ("Recent log lines (tail {0}):" -f $TailCount) -ForegroundColor DarkCyan

    $from = [Math]::Max(0, $AllLines.Count - $TailCount)
    for ($i = $from; $i -lt $AllLines.Count; $i++) {
        Write-Host $AllLines[$i]
    }

    Write-Host ""
    Write-Host "Press Ctrl+C to stop monitoring." -ForegroundColor DarkGray
}

if ($Tail -lt 5) {
    $Tail = 5
}
if ($RefreshSeconds -lt 1) {
    $RefreshSeconds = 1
}

if ([string]::IsNullOrWhiteSpace($LogPath)) {
    $roots = @((Get-Location).Path, $PSScriptRoot)
    $parentRoot = Resolve-Path "$PSScriptRoot\.." -ErrorAction SilentlyContinue
    if ($parentRoot) {
        $roots += $parentRoot.Path
    }
    $latest = Get-LatestIndexerMergeLog -SearchRoots $roots
    if (-not $latest) {
        throw "No IndexerMerge_*.log file found. Pass -LogPath explicitly."
    }
    $resolvedLog = $latest.FullName
} else {
    $resolvedLog = (Resolve-Path -LiteralPath $LogPath).Path
}

if (-not (Test-Path -LiteralPath $resolvedLog)) {
    throw "Log file not found: $resolvedLog"
}

$state = @{
    StartTimestamp = $null
    LastTimestamp = $null
    LastMessage = ""
    IsCompleted = $false
    BatchesCompleted = 0
    LastBatchId = -1
    LastFlatMapMs = -1
    FlatMapCount = 0
    FlatMapTotalMs = 0L
    LastFoldByKeyMs = -1
    FoldByKeyCount = 0
    FoldByKeyTotalMs = 0L
    RecentWarnings = [System.Collections.Generic.List[string]]::new()
}

$lastLineCount = 0

while ($true) {
    try {
        $lines = @(Get-Content -LiteralPath $resolvedLog -ErrorAction Stop)
    } catch {
        Start-Sleep -Seconds $RefreshSeconds
        continue
    }

    if ($lines.Count -gt $lastLineCount) {
        for ($i = $lastLineCount; $i -lt $lines.Count; $i++) {
            Update-StateFromLine -State $state -Line $lines[$i]
        }
        $lastLineCount = $lines.Count
    }

    Render-Dashboard -ResolvedPath $resolvedLog -State $state -AllLines $lines -TailCount $Tail

    if ($state.IsCompleted) {
        Write-Host ""
        Write-Host "Job completion marker detected. Exiting monitor in 8 seconds..." -ForegroundColor Green
        Start-Sleep -Seconds 8
        break
    }

    Start-Sleep -Seconds $RefreshSeconds
}
