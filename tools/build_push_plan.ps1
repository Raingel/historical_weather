param(
    [string]$OutputRoot = 'reports/git_push_logs',
    [string]$SourceBranch = 'codex/rollout-data-only',
    [int]$TargetBatchMB = 250
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path 'data')) {
    throw 'data directory not found in current working tree.'
}

$timestamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$runDir = Join-Path $OutputRoot $timestamp
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

$stations = Get-ChildItem data -Directory | Sort-Object Name | ForEach-Object {
    $sum = (Get-ChildItem $_.FullName -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    [PSCustomObject]@{
        Station = $_.Name
        SizeBytes = [int64]$sum
        SizeMB = [Math]::Round($sum / 1MB, 2)
    }
}

$batches = @()
$batches += [PSCustomObject]@{
    id = '0001'
    type = 'code'
    paths = @('.github', '.gitignore', 'README.md', 'tools')
    commit_message = 'Rollout code and workflow updates'
}

$currentStations = New-Object System.Collections.Generic.List[object]
$currentBytes = [int64]0
$batchNumber = 2

foreach ($station in $stations) {
    $nextMB = ($currentBytes + $station.SizeBytes) / 1MB
    if ($currentStations.Count -gt 0 -and $nextMB -gt $TargetBatchMB) {
        $stationsInBatch = @($currentStations | ForEach-Object { $_.Station })
        $batches += [PSCustomObject]@{
            id = ('{0:D4}' -f $batchNumber)
            type = 'data'
            paths = @($stationsInBatch | ForEach-Object { 'data/' + $_ })
            stations = $stationsInBatch
            total_mb = [Math]::Round($currentBytes / 1MB, 2)
            commit_message = ('Rollout data batch ' + ('{0:D4}' -f $batchNumber))
        }
        $batchNumber += 1
        $currentStations = New-Object System.Collections.Generic.List[object]
        $currentBytes = [int64]0
    }

    $currentStations.Add($station)
    $currentBytes += $station.SizeBytes
}

if ($currentStations.Count -gt 0) {
    $stationsInBatch = @($currentStations | ForEach-Object { $_.Station })
    $batches += [PSCustomObject]@{
        id = ('{0:D4}' -f $batchNumber)
        type = 'data'
        paths = @($stationsInBatch | ForEach-Object { 'data/' + $_ })
        stations = $stationsInBatch
        total_mb = [Math]::Round($currentBytes / 1MB, 2)
        commit_message = ('Rollout data batch ' + ('{0:D4}' -f $batchNumber))
    }
}

$plan = [PSCustomObject]@{
    created_at = (Get-Date -Format s)
    source_branch = $SourceBranch
    target_batch_mb = $TargetBatchMB
    batches = $batches
}

$planPath = Join-Path $runDir 'push_plan.json'
$csvPath = Join-Path $runDir 'push_plan.csv'

$plan | ConvertTo-Json -Depth 8 | Out-File -FilePath $planPath -Encoding utf8

$rows = foreach ($batch in $batches) {
    [PSCustomObject]@{
        batch_id = $batch.id
        type = $batch.type
        item_count = if ($batch.type -eq 'code') { $batch.paths.Count } else { $batch.stations.Count }
        total_mb = if ($batch.PSObject.Properties.Name -contains 'total_mb') { $batch.total_mb } else { 0 }
        first_item = if ($batch.type -eq 'code') { $batch.paths[0] } else { $batch.stations[0] }
        last_item = if ($batch.type -eq 'code') { $batch.paths[-1] } else { $batch.stations[-1] }
    }
}

$rows | Export-Csv -NoTypeInformation -Encoding utf8 $csvPath

Write-Output ('Plan: ' + $planPath)
Write-Output ('CSV: ' + $csvPath)
Write-Output ('Batch count: ' + $batches.Count)
