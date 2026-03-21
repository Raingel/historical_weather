param(
    [string]$PythonExe = 'C:\Users\raing\anaconda3\python.exe',
    [string]$PlanCsv = 'reports\codis_full_rebuild\station_plan.csv',
    [string]$OutputRoot = 'data_codis_rebuild_full',
    [string]$ReportRoot = 'reports\codis_full_rebuild\stations',
    [string]$StatusCsv = 'reports\codis_full_rebuild\station_status.csv',
    [int]$StartIndex = 0,
    [int]$MaxStations = 0
)

$ErrorActionPreference = 'Stop'
$stations = Import-Csv $PlanCsv
if ($StartIndex -gt 0) {
    $stations = $stations | Select-Object -Skip $StartIndex
}
if ($MaxStations -gt 0) {
    $stations = $stations | Select-Object -First $MaxStations
}

if (-not (Test-Path (Split-Path $StatusCsv))) {
    New-Item -ItemType Directory -Path (Split-Path $StatusCsv) -Force | Out-Null
}
if (-not (Test-Path $ReportRoot)) {
    New-Item -ItemType Directory -Path $ReportRoot -Force | Out-Null
}

$existing = @{}
if (Test-Path $StatusCsv) {
    foreach ($row in Import-Csv $StatusCsv) {
        $existing[$row.station_id] = $row.status
    }
}

$index = $StartIndex
foreach ($station in $stations) {
    $stationId = $station.station_id
    $index += 1
    if ($existing.ContainsKey($stationId) -and $existing[$stationId] -eq 'success') {
        Write-Output "[SKIP] $index $stationId already succeeded"
        continue
    }

    $stationReport = Join-Path $ReportRoot $stationId
    New-Item -ItemType Directory -Path $stationReport -Force | Out-Null

    $args = @(
        'tools\rebuild_codis_database.py',
        '--station', $stationId,
        '--output-root', $OutputRoot,
        '--report-dir', $stationReport
    )

    $start = Get-Date
    Write-Output "[RUN]  $index $stationId"
    & $PythonExe @args
    $exitCode = $LASTEXITCODE
    $end = Get-Date
    $status = if ($exitCode -eq 0) { 'success' } else { 'failed' }

    $row = [pscustomobject]@{
        station_id = $stationId
        status = $status
        exit_code = $exitCode
        started_at = $start.ToString('s')
        finished_at = $end.ToString('s')
        report_dir = $stationReport
    }

    $allRows = @()
    if (Test-Path $StatusCsv) {
        $allRows = Import-Csv $StatusCsv | Where-Object { $_.station_id -ne $stationId }
    }
    $allRows += $row
    $allRows | Sort-Object station_id | Export-Csv $StatusCsv -NoTypeInformation -Encoding UTF8

    if ($exitCode -ne 0) {
        Write-Output "[FAIL] $index $stationId exit=$exitCode"
    } else {
        Write-Output "[OK]   $index $stationId"
    }
}
