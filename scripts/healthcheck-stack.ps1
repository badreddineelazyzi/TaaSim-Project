$ErrorActionPreference = "Stop"

function Invoke-Check {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][scriptblock]$Action
    )

    Write-Host "--- $Name ---"
    try {
        & $Action | Out-Null
        Write-Host "  OK"
        return $true
    } catch {
        Write-Host "  FAILED: $($_.Exception.Message)"
        return $false
    }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Error "docker is not installed or not in PATH"
    exit 1
}

Write-Host "Running TaaSim stack health checks..."
Write-Host "Waiting 10 seconds for services to stabilize..."
Start-Sleep -Seconds 10

$pass = 0
$fail = 0

if (Invoke-Check -Name "[1/6] Kafka" -Action { docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list }) { $pass++ } else { $fail++ }
if (Invoke-Check -Name "[2/6] MinIO" -Action { docker run --rm --network taasim-net --entrypoint sh minio/mc:latest -c "mc alias set local http://minio:9000 minioadmin minioadmin123 >/dev/null 2>&1 && mc ls local" }) { $pass++ } else { $fail++ }
if (Invoke-Check -Name "[3/6] Cassandra" -Action { docker compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACES" }) { $pass++ } else { $fail++ }
if (Invoke-Check -Name "[4/6] Flink" -Action { docker compose exec -T flink-jobmanager flink list }) { $pass++ } else { $fail++ }
if (Invoke-Check -Name "[5/6] Spark" -Action { Invoke-RestMethod -Uri "http://localhost:8080/" -Method Get }) { $pass++ } else { $fail++ }
if (Invoke-Check -Name "[6/6] Grafana" -Action { $h = Invoke-RestMethod -Uri "http://localhost:3000/api/health" -Method Get; if ($h.database -ne "ok") { throw "Grafana health not ok" } }) { $pass++ } else { $fail++ }

Write-Host ""
Write-Host "Results: $pass passed, $fail failed out of 6 checks."

if ($fail -gt 0) {
    exit 1
}

Write-Host "All stack checks passed!"
