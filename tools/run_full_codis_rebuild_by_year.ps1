param(
    [string]$PythonExe = 'C:\Users\raing\anaconda3\python.exe',
    [string]$PlanCsv = 'reports\codis_full_rebuild\station_plan.csv',
    [string]$OutputRoot = 'data_codis_rebuild_full',
    [string]$ReportRoot = 'reports\codis_full_rebuild\chunks',
    [string]$StatusCsv = 'reports\codis_full_rebuild\chunk_status.csv',
    [int]$StartIndex = 0,
    [int]$MaxStations = 0
)

$ErrorActionPreference = 'Stop'
$plans = Import-Csv $PlanCsv
if ($StartIndex -gt 0) {
    $plans = $plans | Select-Object -Skip $StartIndex
}
if ($MaxStations -gt 0) {
    $plans = $plans | Select-Object -First $MaxStations
}

New-Item -ItemType Directory -Path $ReportRoot -Force | Out-Null
New-Item -ItemType Directory -Path (Split-Path $StatusCsv) -Force | Out-Null

$done = @{}
if (Test-Path $StatusCsv) {
    foreach ($row in Import-Csv $StatusCsv) {
        if ($row.status -eq 'success') {
            $done["$($row.station_id)|$($row.granularity)|$($row.year)"] = $true
        }
    }
}

$stationCounter = $StartIndex
foreach ($plan in $plans) {
    $stationCounter += 1
    $stationId = $plan.station_id
    $startYear = [int]([datetime]$plan.planned_start).Year
    $endYear = [int]([datetime]$plan.planned_end).Year

    foreach ($granularity in @('hourly','daily','monthly')) {
        for ($year = $startYear; $year -le $endYear; $year++) {
            $key = "$stationId|$granularity|$year"
            if ($done.ContainsKey($key)) {
                Write-Output "[SKIP] $stationCounter $stationId $granularity $year"
                continue
            }

            $chunkReport = Join-Path $ReportRoot (Join-Path $stationId (Join-Path $granularity $year))
            New-Item -ItemType Directory -Path $chunkReport -Force | Out-Null
            Write-Output "[RUN]  $stationCounter $stationId $granularity $year"
            $started = Get-Date

            & $PythonExe 'tools\rebuild_codis_database.py' '--station' $stationId '--granularity' $granularity '--start-year' $year '--end-year' $year '--output-root' $OutputRoot '--report-dir' $chunkReport
            $exitCode = $LASTEXITCODE
            $finished = Get-Date
            $status = if ($exitCode -eq 0) { 'success' } else { 'failed' }

            $row = [pscustomobject]@{
                station_id = $stationId
                granularity = $granularity
                year = $year
                status = $status
                exit_code = $exitCode
                started_at = $started.ToString('s')
                finished_at = $finished.ToString('s')
                report_dir = $chunkReport
            }

            if (-not (Test-Path $StatusCsv)) {
                $row | Export-Csv $StatusCsv -NoTypeInformation -Encoding UTF8
            } else {
                $row | Export-Csv $StatusCsv -NoTypeInformation -Encoding UTF8 -Append
            }

            if ($status -eq 'success') {
                $done[$key] = $true
                Write-Output "[OK]   $stationCounter $stationId $granularity $year"
            } else {
                Write-Output "[FAIL] $stationCounter $stationId $granularity $year exit=$exitCode"
            }
        }
    }
}
