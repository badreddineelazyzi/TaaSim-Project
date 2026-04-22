$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$dataDir = Join-Path $projectRoot "osrm_data"
$pbfPath = Join-Path $dataDir "morocco-latest.osm.pbf"
$downloadUrl = "https://download.geofabrik.de/africa/morocco-latest.osm.pbf"

if (-not (Test-Path $dataDir)) {
    New-Item -Path $dataDir -ItemType Directory | Out-Null
}

function Download-Pbf {
    param(
        [string]$Url,
        [string]$OutFile
    )

    Write-Host "Downloading Morocco map from Geofabrik..."
    curl.exe -L --fail --retry 5 --retry-all-errors --retry-delay 2 --output "$OutFile" "$Url"
    if ($LASTEXITCODE -ne 0) {
        throw "Download failed with exit code $LASTEXITCODE"
    }

    $size = (Get-Item $OutFile).Length
    if ($size -lt 100MB) {
        throw "Downloaded PBF looks too small ($size bytes), likely truncated"
    }
    Write-Host "Download complete: $size bytes"
}

if (-not (Test-Path $pbfPath)) {
    Download-Pbf -Url $downloadUrl -OutFile $pbfPath
} else {
    $existingSize = (Get-Item $pbfPath).Length
    if ($existingSize -lt 100MB) {
        Write-Host "Existing PBF is too small ($existingSize bytes). Re-downloading..."
        Remove-Item -Force $pbfPath
        Download-Pbf -Url $downloadUrl -OutFile $pbfPath
    } else {
        Write-Host "PBF already exists: $pbfPath ($existingSize bytes)"
    }
}

Write-Host "Running osrm-extract (car profile)..."
docker run --rm -t -v "${dataDir}:/data" osrm/osrm-backend osrm-extract -p /opt/car.lua /data/morocco-latest.osm.pbf
if ($LASTEXITCODE -ne 0) {
    Write-Host "osrm-extract failed. Removing possibly corrupted PBF and trying one fresh download..."
    if (Test-Path $pbfPath) { Remove-Item -Force $pbfPath }
    Download-Pbf -Url $downloadUrl -OutFile $pbfPath

    docker run --rm -t -v "${dataDir}:/data" osrm/osrm-backend osrm-extract -p /opt/car.lua /data/morocco-latest.osm.pbf
    if ($LASTEXITCODE -ne 0) {
        throw "osrm-extract failed after retry"
    }
}

Write-Host "Running osrm-contract (CH algorithm)..."
docker run --rm -t -v "${dataDir}:/data" osrm/osrm-backend osrm-contract /data/morocco-latest.osrm
if ($LASTEXITCODE -ne 0) {
    throw "osrm-contract failed"
}

Write-Host "OSRM data prepared in: $dataDir"
Write-Host "Now start routing service: docker compose up -d osrm-backend"
