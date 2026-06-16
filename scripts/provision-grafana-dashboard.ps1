<#
.SYNOPSIS
  Provision Grafana Cassandra datasource and TaaSim dashboard.
  Idempotent - safe to re-run.
#>

$ErrorActionPreference = "Stop"

$GRAFANA_URL  = "http://localhost:3000"
$GRAFANA_USER = "admin"
$GRAFANA_PASS = "admin123"
$BASIC_AUTH   = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${GRAFANA_USER}:${GRAFANA_PASS}"))

function Invoke-GrafanaApi {
    param($Method, $Path, $Body)
    $uri = "${GRAFANA_URL}${Path}"
    $params = @{
        Method = $Method
        Uri = $uri
        Headers = @{ Authorization = "Basic $BASIC_AUTH" }
        ContentType = "application/json"
    }
    if ($Body) {
        $params.Body = ($Body | ConvertTo-Json -Depth 10 -Compress)
    }
    try {
        $r = Invoke-RestMethod @params
        return $r
    } catch {
        Write-Warning "Grafana API error ($Method $Path): $_"
        return $null
    }
}

$dsName = "Cassandra"
$dsUid  = "efo5rnez8n2f4e"

$dsBody = @{
    uid   = $dsUid
    orgId = 1
    name  = $dsName
    type  = "hadesarchitect-cassandra-datasource"
    access = "proxy"
    url    = "cassandra:9042"
    user   = ""
    database = "taasim"
    basicAuth = $false
    jsonData = @{
        contactPoint = "cassandra"
        contactPort  = 9042
        keyspace     = "taasim"
        consistency  = "ONE"
        localDatacenter = "datacenter1"
    }
    secureJsonData = @{}
    readOnly = $false
}

Write-Host "[1/2] Creating/updating Cassandra datasource ..." -ForegroundColor Cyan
$existing = Invoke-GrafanaApi -Method GET -Path "/api/datasources/uid/$dsUid"
if ($existing) {
    Write-Host "  Datasource exists - updating"
    Invoke-GrafanaApi -Method PUT -Path "/api/datasources/uid/$dsUid" -Body $dsBody | Out-Null
} else {
    Write-Host "  Creating datasource"
    Invoke-GrafanaApi -Method POST -Path "/api/datasources" -Body $dsBody | Out-Null
}

Write-Host "[2/2] Creating/updating dashboard ..." -ForegroundColor Cyan
$dashboardJson = Get-Content -Raw -LiteralPath "C:\Users\dell\Desktop\big data project\TaaSim-Project\scripts\dashboard.json"
$params = @{
    Method = "POST"
    Uri = "${GRAFANA_URL}/api/dashboards/db"
    Headers = @{ Authorization = "Basic $BASIC_AUTH" }
    ContentType = "application/json"
    Body = $dashboardJson
}
try {
    $result = Invoke-RestMethod @params
    Write-Host "  Dashboard URL: ${GRAFANA_URL}/d/$($result.uid)/$($result.slug)" -ForegroundColor Green
    Write-Host "  Status: $($result.status)" -ForegroundColor Green
} catch {
    Write-Host "  Dashboard creation failed: $_" -ForegroundColor Red
}

Write-Host "Done." -ForegroundColor Cyan
