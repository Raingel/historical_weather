param(
    [Parameter(Mandatory = $true)]
    [string]$PlanPath,
    [Parameter(Mandatory = $true)]
    [string]$BatchId,
    [string]$GitExe = 'C:\Program Files\Git\cmd\git.exe',
    [string]$Remote = 'origin',
    [string]$TargetBranch = '',
    [switch]$Push,
    [switch]$List
)

$ErrorActionPreference = 'Stop'

function Ensure-Dir {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
    }
}

function Format-GitArgument {
    param([string]$Value)
    if ($null -eq $Value) {
        return '""'
    }
    if ($Value -match '[\s"]') {
        return '"' + ($Value -replace '"', '\"') + '"'
    }
    return $Value
}

function Invoke-Git {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Args,
        [string]$LogFile,
        [hashtable]$EnvironmentVariables
    )

    $saved = @{}
    if ($EnvironmentVariables) {
        foreach ($key in $EnvironmentVariables.Keys) {
            $saved[$key] = [Environment]::GetEnvironmentVariable($key, 'Process')
            [Environment]::SetEnvironmentVariable($key, $EnvironmentVariables[$key], 'Process')
        }
    }

    $stdoutPath = [System.IO.Path]::GetTempFileName()
    $stderrPath = [System.IO.Path]::GetTempFileName()

    try {
        $argLine = ($Args | ForEach-Object { Format-GitArgument $_ }) -join ' '
        $proc = Start-Process -FilePath $GitExe -ArgumentList $argLine -Wait -NoNewWindow -PassThru -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
        $stdout = if (Test-Path $stdoutPath) { Get-Content $stdoutPath } else { @() }
        $stderr = if (Test-Path $stderrPath) { Get-Content $stderrPath } else { @() }
        $allOutput = @($stdout) + @($stderr)
        if ($LogFile -and $allOutput.Count -gt 0) {
            $allOutput | Out-File -FilePath $LogFile -Append -Encoding utf8
        }
        if ($proc.ExitCode -ne 0) {
            throw "git $($Args -join ' ') failed with exit code $($proc.ExitCode)"
        }
        return $allOutput
    }
    finally {
        Remove-Item $stdoutPath, $stderrPath -ErrorAction SilentlyContinue
        if ($EnvironmentVariables) {
            foreach ($key in $EnvironmentVariables.Keys) {
                [Environment]::SetEnvironmentVariable($key, $saved[$key], 'Process')
            }
        }
    }
}

$plan = Get-Content $PlanPath -Raw | ConvertFrom-Json

if (-not $TargetBranch) {
    $TargetBranch = (& $GitExe branch --show-current).Trim()
}

if ($List) {
    $plan.batches | ForEach-Object {
        [PSCustomObject]@{
            batch_id = $_.id
            type = $_.type
            item_count = if ($_.type -eq 'code') { $_.paths.Count } else { $_.stations.Count }
            total_mb = if ($_.PSObject.Properties.Name -contains 'total_mb') { $_.total_mb } else { 0 }
            first_item = if ($_.type -eq 'code') { $_.paths[0] } else { $_.stations[0] }
            last_item = if ($_.type -eq 'code') { $_.paths[-1] } else { $_.stations[-1] }
        }
    } | Format-Table -AutoSize
    exit 0
}

$batch = @($plan.batches | Where-Object { $_.id -eq $BatchId })
if ($batch.Count -ne 1) {
    throw "Batch $BatchId not found in $PlanPath"
}
$batch = $batch[0]

$status = (& $GitExe status --porcelain --untracked-files=no)
if ($LASTEXITCODE -ne 0) {
    throw 'git status failed'
}
if ($status) {
    throw 'Working tree is not clean. Commit or stash changes before applying a batch.'
}

$logRoot = Join-Path 'reports/git_push_logs' ((Get-Date -Format 'yyyyMMdd_HHmmss') + '_' + $BatchId)
Ensure-Dir $logRoot
$batchLog = Join-Path $logRoot 'batch.log'
$traceFile = Join-Path $logRoot 'trace2.json'
$metaPath = Join-Path $logRoot 'meta.json'

[PSCustomObject]@{
    plan_path = $PlanPath
    batch_id = $batch.id
    type = $batch.type
    target_branch = $TargetBranch
    source_branch = $plan.source_branch
    created_at = (Get-Date -Format s)
    paths = $batch.paths
} | ConvertTo-Json -Depth 6 | Out-File -FilePath $metaPath -Encoding utf8

"batch $($batch.id) started $(Get-Date -Format s)" | Out-File -FilePath $batchLog -Encoding utf8
"target=$TargetBranch source=$($plan.source_branch) push=$Push" | Out-File -FilePath $batchLog -Append -Encoding utf8

Invoke-Git -Args @('switch', $TargetBranch) -LogFile $batchLog | Out-Null
Invoke-Git -Args (@('checkout', $plan.source_branch, '--') + @($batch.paths)) -LogFile $batchLog | Out-Null
Invoke-Git -Args (@('add', '--') + @($batch.paths)) -LogFile $batchLog | Out-Null

$staged = Invoke-Git -Args @('diff', '--cached', '--name-only') -LogFile $batchLog
if (-not $staged) {
    'No staged diff for this batch.' | Out-File -FilePath $batchLog -Append -Encoding utf8
    Write-Output ('No staged diff for batch ' + $batch.id)
    exit 0
}

Invoke-Git -Args @('commit', '-m', $batch.commit_message) -LogFile $batchLog | Out-Null

if ($Push) {
    Invoke-Git -Args @('push', '-u', $Remote, $TargetBranch) -LogFile $batchLog -EnvironmentVariables @{ 'GIT_TRACE2_EVENT' = $traceFile } | Out-Null
    Write-Output ('Pushed batch ' + $batch.id + ' logs=' + $logRoot)
}
else {
    Write-Output ('Committed batch ' + $batch.id + ' logs=' + $logRoot)
}
