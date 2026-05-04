# scripts/deploy-connectors.ps1
# Helper script to deploy Kafka connectors using the REST API

$CONNECT_URL = "http://localhost:8083/connectors"
$CONFIG_DIR = "scripts/connectors"

# Wait for Kafka Connect to be ready
Write-Host "Waiting for Kafka Connect to be ready at $CONNECT_URL..." -ForegroundColor Cyan
while ($true) {
    try {
        $response = Invoke-WebRequest -Uri $CONNECT_URL -Method Get -ErrorAction Stop
        if ($response.StatusCode -eq 200) { break }
    } catch {
        Write-Host "." -NoNewline
        Start-Sleep -Seconds 2
    }
}
Write-Host "`nKafka Connect is UP!" -ForegroundColor Green

# Loop through all JSON files in the connectors directory
Get-ChildItem $CONFIG_DIR -Filter *.json | ForEach-Object {
    $file = $_.FullName
    $jsonContent = Get-Content $file -Raw
    $configObj = $jsonContent | ConvertFrom-Json
    $name = $configObj.name
    
    Write-Host "Deploying connector: $name ..." -ForegroundColor Yellow
    
    # Check if connector already exists
    try {
        $checkUrl = "$CONNECT_URL/$name"
        Invoke-WebRequest -Uri $checkUrl -Method Get -ErrorAction Stop > $null
        
        # If it exists, update it (PUT)
        $updateUrl = "$CONNECT_URL/$name/config"
        $body = $configObj.config | ConvertTo-Json -Depth 10
        Invoke-RestMethod -Uri $updateUrl -Method Put -Body $body -ContentType "application/json"
        Write-Host "Update success!" -ForegroundColor Green
    } catch {
        # If it doesn't exist, create it (POST)
        Invoke-RestMethod -Uri $CONNECT_URL -Method Post -Body $jsonContent -ContentType "application/json"
        Write-Host "Creation success!" -ForegroundColor Green
    }
}

Write-Host "`nAll connectors deployed successfully." -ForegroundColor Cyan
