<#
.SYNOPSIS
  Build the GPS Normalizer Flink uber-JAR using Maven inside Docker.
  No local Java or Maven installation required.
#>

$ErrorActionPreference = "Stop"
$projectDir = Join-Path $PSScriptRoot "..\flink\gps-normalizer"
$projectDir = (Resolve-Path $projectDir).Path

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " Building GPS Normalizer Flink Job"       -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Project dir: $projectDir"
Write-Host ""

# Build with Maven locally
Write-Host "[1/2] Running Maven build locally ..." -ForegroundColor Yellow
Set-Location $projectDir
mvn clean package -DskipTests -q
Set-Location $PSScriptRoot\..

if ($LASTEXITCODE -ne 0) {
    Write-Host "BUILD FAILED" -ForegroundColor Red
    exit 1
}

$jar = Join-Path $projectDir "target\gps-normalizer-1.0.jar"
if (-Not (Test-Path $jar)) {
    Write-Host "JAR not found at $jar" -ForegroundColor Red
    exit 1
}

$sizeMB = [math]::Round((Get-Item $jar).Length / 1MB, 1)
Write-Host "[2/2] Build successful! JAR size: ${sizeMB} MB" -ForegroundColor Green
Write-Host "      $jar"
Write-Host ""
Write-Host "Next: run  scripts/submit-flink-job.ps1  to deploy." -ForegroundColor Cyan
