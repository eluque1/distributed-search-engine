<#
.SYNOPSIS
    Monitor crawler progress and speed in real-time.

.DESCRIPTION
    This script compiles and runs the CrawlerMonitor tool to track:
    - Total pages crawled
    - Current crawl speed (pages/second)
    - Average crawl speed
    - Estimated time to completion
    - Progress bar

.PARAMETER KvsCoordinator
    KVS Coordinator address (default: localhost:8000)

.PARAMETER AutoStop
    Automatically stop the crawler when target is reached

.EXAMPLE
    .\monitor-crawler.ps1
    
    Monitor crawler with default settings (localhost:8000)

.EXAMPLE
    .\monitor-crawler.ps1 -KvsCoordinator "ec2-instance:8000"
    
    Monitor crawler on EC2 instance

.EXAMPLE
    .\monitor-crawler.ps1 -AutoStop
    
    Monitor and auto-stop when target pages reached
#>

param(
    [string]$KvsCoordinator = "localhost:8000",
    [switch]$AutoStop
)

Write-Host "=== Crawler Monitor Setup ===" -ForegroundColor Cyan

# Check if bin directory exists, if not compile everything first
if (-not (Test-Path "bin/cis5550/kvs/KVSClient.class")) {
    Write-Host "`nCompiling project dependencies..." -ForegroundColor Yellow
    
    # Get all Java files
    $javaFiles = Get-ChildItem -Path "src" -Filter "*.java" -Recurse | Select-Object -ExpandProperty FullName
    
    # Compile all at once
    $compileCmd = "javac -d bin -cp `"lib/*`" " + ($javaFiles -join " ")
    Invoke-Expression $compileCmd 2>&1 | Out-Null
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Project compilation failed! Please run your main build script first." -ForegroundColor Red
        exit 1
    }
    Write-Host "Project compiled successfully!" -ForegroundColor Green
} else {
    # Just compile the monitor
    Write-Host "`nCompiling CrawlerMonitor..." -ForegroundColor Yellow
    $compileOutput = javac -d bin -cp "bin;lib/*" src/cis5550/tools/CrawlerMonitor.java 2>&1

    if ($LASTEXITCODE -ne 0) {
        Write-Host "Compilation failed!" -ForegroundColor Red
        Write-Host $compileOutput
        exit 1
    }

    Write-Host "Compilation successful!" -ForegroundColor Green
}

# Build arguments
$monitorArgs = @($KvsCoordinator)
if ($AutoStop) {
    $monitorArgs += "--auto-stop"
}

# Run the monitor
Write-Host "`nStarting monitor for KVS at $KvsCoordinator..." -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop monitoring`n" -ForegroundColor Gray

java -cp "bin;lib/*" cis5550.tools.CrawlerMonitor $monitorArgs
