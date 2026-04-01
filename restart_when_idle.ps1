param(
    [int]$Port = 8462,
    [int]$PollSeconds = 5,
    [int]$MaxWaitMinutes = 30,
    [int]$RequiredQuietPolls = 2,
    [string]$WorkingDirectory = "d:\Git\resilient-proxy",
    [string]$PythonwPath = "C:\Python314\pythonw.exe",
    [double]$TimeoutSeconds = 60.0,
    [double]$WatchInterval = 1.0
)

$ErrorActionPreference = "Stop"

$logPath = Join-Path $WorkingDirectory "idle-restart.log"
$runPy = Join-Path $WorkingDirectory "run.py"

function Write-Log {
    param([string]$Message)
    $stamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $logPath -Value "[$stamp] $Message" -Encoding UTF8
}

function Get-Health {
    param([string]$Path)
    $content = (Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:$Port$Path" -TimeoutSec 5).Content
    return $content | ConvertFrom-Json
}

if (-not (Test-Path -LiteralPath $runPy)) {
    Write-Log "run.py not found: $runPy"
    exit 1
}

if (-not (Test-Path -LiteralPath $PythonwPath)) {
    Write-Log "pythonw not found: $PythonwPath"
    exit 1
}

Write-Log "idle restart watcher started for port $Port"
$deadline = (Get-Date).AddMinutes($MaxWaitMinutes)
$quietPolls = 0

while ((Get-Date) -lt $deadline) {
    try {
        $health = Get-Health "/health"
        $active = [int]($health.active_sessions_count | ForEach-Object { $_ })
        $pending = [int]($health.pending_tool_waits_count | ForEach-Object { $_ })
        $startedAtMs = [int64]($health.started_at_ms | ForEach-Object { $_ })
        $supervisorPid = [int]($health.supervisor_pid | ForEach-Object { $_ })
        Write-Log "poll active=$active pending=$pending started_at_ms=$startedAtMs supervisor_pid=$supervisorPid"

        if (($active -eq 0) -and ($pending -eq 0)) {
            $quietPolls += 1
        } else {
            $quietPolls = 0
        }

        if ($quietPolls -ge $RequiredQuietPolls) {
            $args = @(
                $runPy,
                "--headless",
                "--port", "$Port",
                "--timeout", "$TimeoutSeconds",
                "--retries", "-1",
                "--watch-interval", "$WatchInterval",
                "--aggressive-autoevolve",
                "--enable-supervisor"
            )
            Start-Process -FilePath $PythonwPath -ArgumentList $args -WorkingDirectory $WorkingDirectory -WindowStyle Hidden | Out-Null
            Write-Log "quiet window found; started new headless Plunger"

            $verifyDeadline = (Get-Date).AddSeconds(45)
            $proxyRestarted = $false
            while ((Get-Date) -lt $verifyDeadline) {
                Start-Sleep -Milliseconds 600
                try {
                    $current = Get-Health "/healthz"
                    $currentStartedAtMs = [int64]($current.started_at_ms | ForEach-Object { $_ })
                    $currentSupervisorPid = [int]($current.supervisor_pid | ForEach-Object { $_ })
                    $proxyChanged = $currentStartedAtMs -ne $startedAtMs
                    $supervisorChanged = ($supervisorPid -le 0) -or ($currentSupervisorPid -gt 0 -and $currentSupervisorPid -ne $supervisorPid)
                    if ($proxyChanged -and $supervisorChanged) {
                        Write-Log "restart verified: pid=$($current.pid) started_at_ms=$currentStartedAtMs supervisor_pid=$currentSupervisorPid"
                        exit 0
                    }
                    if ($proxyChanged) {
                        $proxyRestarted = $true
                        Write-Log "proxy restarted but supervisor pid is unchanged; will wait for next quiet window"
                        $quietPolls = 0
                        break
                    }
                } catch {
                }
            }

            if ($proxyRestarted) {
                continue
            }

            Write-Log "new process started but verification timed out"
            exit 2
        }
    } catch {
        Write-Log "poll failed: $($_.Exception.Message)"
    }

    Start-Sleep -Seconds $PollSeconds
}

Write-Log "idle restart watcher timed out without finding a quiet window"
exit 3
