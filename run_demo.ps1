# One-command live demo (Windows PowerShell).
#
#   .\run_demo.ps1              # full demo: stack + topics + 3 terminals
#   .\run_demo.ps1 -SkipStack   # reuse a stack that is already running
#
# Opens three PowerShell windows: consumer, DLQ inspector, producer.

param(
    [switch]$SkipStack,
    [int]$Count = 60,
    [double]$Interval = 0.4
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

if (-not $SkipStack) {
    Write-Host "==> Starting Kafka + Schema Registry..." -ForegroundColor Cyan
    docker compose -f "$root\docker-compose.yml" up -d

    Write-Host "==> Waiting for Schema Registry to answer on :8081..." -ForegroundColor Cyan
    $ready = $false
    foreach ($i in 1..60) {
        try {
            Invoke-RestMethod -Uri "http://localhost:8081/subjects" -TimeoutSec 3 | Out-Null
            $ready = $true
            break
        } catch {
            Start-Sleep -Seconds 3
        }
    }
    if (-not $ready) { throw "Schema Registry did not become ready in time." }
    Write-Host "    Schema Registry is up." -ForegroundColor Green
}

Write-Host "==> Creating topics..." -ForegroundColor Cyan
python "$root\src\create_topics.py"

Write-Host "==> Launching consumer window..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList @(
    "-NoExit", "-Command",
    "Set-Location '$root'; python src/consumer.py --from-beginning"
)

Write-Host "==> Launching DLQ inspector window..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList @(
    "-NoExit", "-Command",
    "Set-Location '$root'; python src/dlq_consumer.py --from-beginning"
)

Start-Sleep -Seconds 5

Write-Host "==> Launching producer window ($Count orders)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList @(
    "-NoExit", "-Command",
    "Set-Location '$root'; python src/producer.py -n $Count -i $Interval --corrupt-rate 0.05"
)

Write-Host ""
Write-Host "Demo running. Kafka UI: http://localhost:8080" -ForegroundColor Green
Write-Host "Press Ctrl+C in each window to stop, then: docker compose down -v" -ForegroundColor Green
