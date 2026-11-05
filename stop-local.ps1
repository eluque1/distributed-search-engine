<#
.SYNOPSIS
    Stops all local Java processes (KVS and Flame services).

.DESCRIPTION
    This script kills all Java processes running locally.
    Use this to reset/clear JVM memory before restarting services.

.EXAMPLE
    .\stop-local.ps1
#>

Write-Host "`n=== Stopping all local Java processes ===" -ForegroundColor Yellow

$javaProcesses = Get-Process -Name java -ErrorAction SilentlyContinue

if ($javaProcesses) {
    Write-Host "Found $($javaProcesses.Count) Java process(es)" -ForegroundColor Cyan
    
    foreach ($proc in $javaProcesses) {
        Write-Host "Stopping process PID $($proc.Id)..." -ForegroundColor Yellow
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    }
    
    Start-Sleep -Seconds 2
    
    # Verify they're stopped
    $remaining = Get-Process -Name java -ErrorAction SilentlyContinue
    if ($remaining) {
        Write-Host "Warning: Some processes may still be running" -ForegroundColor Red
    } else {
        Write-Host "All Java processes stopped successfully" -ForegroundColor Green
    }
} else {
    Write-Host "No Java processes found" -ForegroundColor Green
}

Write-Host "`n=== Done ===" -ForegroundColor Green

