$ErrorActionPreference = "Stop"

function Test-Step {
    param(
        [string]$Label,
        [scriptblock]$Action
    )

    Write-Host "--- $Label ---"
    try {
        & $Action | Out-Null
        Write-Host "  OK"
        return $true
    } catch {
        Write-Host "  FAILED"
        return $false
    }
}

Write-Host "Running TaaSim stack health checks..."
Write-Host ""
Write-Host "Waiting 10 seconds for services to stabilize..."
Start-Sleep -Seconds 10

$pass = 0
$fail = 0

if (Test-Step -Label "[1/6] Kafka" -Action { docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list }) { $pass++ } else { $fail++ }
if (Test-Step -Label "[2/6] MinIO" -Action { docker run --rm --network taasim-net --entrypoint sh minio/mc:latest -c "mc alias set local http://minio:9000 minioadmin minioadmin123 >/dev/null 2>&1 && mc ls local" }) { $pass++ } else { $fail++ }
if (Test-Step -Label "[3/6] Cassandra" -Action { docker compose exec -T cassandra cqlsh -e "DESCRIBE KEYSPACES" }) { $pass++ } else { $fail++ }
if (Test-Step -Label "[4/6] Flink" -Action { docker compose exec -T flink-jobmanager flink list }) { $pass++ } else { $fail++ }
if (Test-Step -Label "[5/6] Spark" -Action { Invoke-WebRequest -UseBasicParsing -Uri "http://localhost:8080/" }) { $pass++ } else { $fail++ }
if (Test-Step -Label "[6/6] Grafana" -Action { $r = Invoke-RestMethod -Uri "http://localhost:3000/api/health"; if ($r.database -ne "ok") { throw "Grafana not healthy" } }) { $pass++ } else { $fail++ }

Write-Host ""
Write-Host "Results: $pass passed, $fail failed out of 6 checks."

if ($fail -gt 0) {
    exit 1
}

Write-Host "All stack checks passed!"
