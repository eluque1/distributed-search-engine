<#
.SYNOPSIS
    Script to compile all files and start KVS and Flame services.

.DESCRIPTION
    This script:
    1. Compiles all Java files
    2. Starts 1 KVS Coordinator (port 8000)
    3. Starts 1 KVS Worker (port 8001)
    4. Starts 1 Flame Coordinator (port 9000)
    5. Starts 1 Flame Worker (port 9001)

.EXAMPLE
    .\start-services.ps1
#>

# Colors for output
$ErrorColor = "Red"
$SuccessColor = "Green"
$InfoColor = "Cyan"

function Write-ColorOutput {
    param([string]$Message, [string]$Color = "White")
    Write-Host $Message -ForegroundColor $Color
}

function Compile-All {
    Write-ColorOutput "`n=== Compiling all Java files ===" $InfoColor
    
    # Ensure bin directory exists
    if (-not (Test-Path "bin")) {
        New-Item -ItemType Directory -Path "bin" | Out-Null
        Write-ColorOutput "Created bin directory" $InfoColor
    }
    
    # Get all Java files recursively
    $javaFiles = Get-ChildItem -Recurse -Path "src" -Filter "*.java" | ForEach-Object { $_.FullName }
    
    if ($javaFiles.Count -eq 0) {
        Write-ColorOutput "ERROR: No Java files found in src directory" $ErrorColor
        exit 1
    }
    
    Write-ColorOutput "Found $($javaFiles.Count) Java file(s) to compile" $InfoColor
    
    # Determine classpath - check multiple possible locations
    $classpath = "bin"
    $libPaths = @("lib", "HW6\lib", ".")
    foreach ($libPath in $libPaths) {
        if (Test-Path "$libPath\webserver.jar") {
            $classpath += ";$libPath\webserver.jar"
        }
        if (Test-Path "$libPath\kvs.jar") {
            $classpath += ";$libPath\kvs.jar"
        }
        if (Test-Path "$libPath\flame.jar") {
            $classpath += ";$libPath\flame.jar"
        }
    }
    
    Write-ColorOutput "Using classpath: $classpath" $InfoColor
    
    # Compile with classpath
    $javacArgs = @("-cp", $classpath, "-d", "bin") + $javaFiles
    
    $compileOutput = & javac @javacArgs 2>&1
    foreach ($line in $compileOutput) {
        if ($line -match "error") {
            Write-ColorOutput $line $ErrorColor
        } else {
            Write-Host $line
        }
    }
    
    if ($LASTEXITCODE -ne 0) {
        Write-ColorOutput "`nERROR: Compilation failed!" $ErrorColor
        exit 1
    }
    
    Write-ColorOutput "Compilation successful!" $SuccessColor
}

function Start-KvsCoordinator {
    Write-ColorOutput "`n=== Starting KVS Coordinator on port 8000 ===" $InfoColor
    $script:kvsCoordinator = Start-Process -FilePath "java" `
        -ArgumentList "-Xmx1g", "-cp", "bin", "cis5550.kvs.Coordinator", "8000" `
        -PassThru -WindowStyle Normal
    Start-Sleep -Seconds 2
    Write-ColorOutput "KVS Coordinator started (PID: $($script:kvsCoordinator.Id))" $SuccessColor
}

function Start-KvsWorker {
    Write-ColorOutput "`n=== Starting KVS Worker on port 8001 ===" $InfoColor
    
    $port = 8001
    $workerDir = "worker1"
    
    # Create worker directory if it doesn't exist
    if (-not (Test-Path $workerDir)) {
        New-Item -ItemType Directory -Path $workerDir | Out-Null
    }
    
    Write-ColorOutput "Starting KVS Worker on port $port (directory: $workerDir)" $InfoColor
    $script:kvsWorker = Start-Process -FilePath "java" `
        -ArgumentList "-Xmx6g", "-cp", "bin", "cis5550.kvs.Worker", "$port", "$workerDir", "localhost:8000" `
        -PassThru -WindowStyle Normal
    
    if ($script:kvsWorker) {
        Write-ColorOutput "KVS Worker started (PID: $($script:kvsWorker.Id))" $SuccessColor
    } else {
        Write-ColorOutput "ERROR: Failed to start KVS Worker" $ErrorColor
    }
    Start-Sleep -Seconds 2
}

function Start-FlameCoordinator {
    Write-ColorOutput "`n=== Starting Flame Coordinator on port 9000 ===" $InfoColor
    $script:flameCoordinator = Start-Process -FilePath "java" `
        -ArgumentList "-Xmx2g", "-cp", "bin", "cis5550.flame.Coordinator", "9000", "localhost:8000" `
        -PassThru -WindowStyle Normal
    Start-Sleep -Seconds 2
    Write-ColorOutput "Flame Coordinator started (PID: $($script:flameCoordinator.Id))" $SuccessColor
}

function Start-FlameWorker {
    Write-ColorOutput "`n=== Starting Flame Worker on port 9001 ===" $InfoColor
    
    $port = 9001
    Write-ColorOutput "Starting Flame Worker on port $port" $InfoColor
    $script:flameWorker = Start-Process -FilePath "java" `
        -ArgumentList "-Xmx2g", "-cp", "bin", "cis5550.flame.Worker", "$port", "localhost:9000" `
        -PassThru -WindowStyle Normal
    
    if ($script:flameWorker) {
        Write-ColorOutput "Flame Worker started (PID: $($script:flameWorker.Id))" $SuccessColor
    } else {
        Write-ColorOutput "ERROR: Failed to start Flame Worker" $ErrorColor
    }
    Start-Sleep -Seconds 2
}

function Cleanup {
    Write-ColorOutput "`n=== Cleaning up processes ===" $InfoColor
    
    if ($script:flameWorker -and $script:flameWorker.Id -and -not $script:flameWorker.HasExited) {
        try {
            Stop-Process -Id $script:flameWorker.Id -Force -ErrorAction SilentlyContinue
            Write-ColorOutput "Stopped Flame Worker (PID: $($script:flameWorker.Id))" $InfoColor
        } catch {
            Write-ColorOutput "Error stopping Flame Worker: $($_.Exception.Message)" $ErrorColor
        }
    }
    
    if ($script:kvsWorker -and $script:kvsWorker.Id -and -not $script:kvsWorker.HasExited) {
        try {
            Stop-Process -Id $script:kvsWorker.Id -Force -ErrorAction SilentlyContinue
            Write-ColorOutput "Stopped KVS Worker (PID: $($script:kvsWorker.Id))" $InfoColor
        } catch {
            Write-ColorOutput "Error stopping KVS Worker: $($_.Exception.Message)" $ErrorColor
        }
    }
    
    if ($script:flameCoordinator -and $script:flameCoordinator.Id -and -not $script:flameCoordinator.HasExited) {
        try {
            Stop-Process -Id $script:flameCoordinator.Id -Force -ErrorAction SilentlyContinue
            Write-ColorOutput "Stopped Flame Coordinator (PID: $($script:flameCoordinator.Id))" $InfoColor
        } catch {
            Write-ColorOutput "Error stopping Flame Coordinator: $($_.Exception.Message)" $ErrorColor
        }
    }
    
    if ($script:kvsCoordinator -and $script:kvsCoordinator.Id -and -not $script:kvsCoordinator.HasExited) {
        try {
            Stop-Process -Id $script:kvsCoordinator.Id -Force -ErrorAction SilentlyContinue
            Write-ColorOutput "Stopped KVS Coordinator (PID: $($script:kvsCoordinator.Id))" $InfoColor
        } catch {
            Write-ColorOutput "Error stopping KVS Coordinator: $($_.Exception.Message)" $ErrorColor
        }
    }
    
    Write-ColorOutput "Cleanup complete" $SuccessColor
}

# Store process IDs for cleanup
$script:kvsCoordinator = $null
$script:kvsWorker = $null
$script:flameCoordinator = $null
$script:flameWorker = $null

# Main execution
Write-ColorOutput "========================================" $InfoColor
Write-ColorOutput "  Service Starter Script" $InfoColor
Write-ColorOutput "========================================" $InfoColor
Write-ColorOutput "KVS Coordinator: port 8000" $InfoColor
Write-ColorOutput "KVS Worker: port 8001" $InfoColor
Write-ColorOutput "Flame Coordinator: port 9000" $InfoColor
Write-ColorOutput "Flame Worker: port 9001" $InfoColor
Write-ColorOutput "========================================" $InfoColor

try {
    # Step 1: Compile
    Compile-All
    
    # Step 2: Start KVS Coordinator
    Start-KvsCoordinator
    
    # Step 3: Start KVS Worker
    Start-KvsWorker
    
    # Step 4: Start Flame Coordinator
    Start-FlameCoordinator
    
    # Step 5: Start Flame Worker
    Start-FlameWorker
    
    # Wait a bit for all services to initialize
    Write-ColorOutput "`nWaiting 3 seconds for services to initialize..." $InfoColor
    Start-Sleep -Seconds 3
    
    Write-ColorOutput "`n=== All Services Started ===" $SuccessColor
    Write-ColorOutput "`nProcess IDs:" $InfoColor
    if ($script:kvsCoordinator -and $script:kvsCoordinator.Id) {
        Write-ColorOutput "  KVS Coordinator: $($script:kvsCoordinator.Id)" $InfoColor
    }
    if ($script:kvsWorker -and $script:kvsWorker.Id) {
        Write-ColorOutput "  KVS Worker: $($script:kvsWorker.Id)" $InfoColor
    }
    if ($script:flameCoordinator -and $script:flameCoordinator.Id) {
        Write-ColorOutput "  Flame Coordinator: $($script:flameCoordinator.Id)" $InfoColor
    }
    if ($script:flameWorker -and $script:flameWorker.Id) {
        Write-ColorOutput "  Flame Worker: $($script:flameWorker.Id)" $InfoColor
    }
    
    # Keep script running until user presses Ctrl+C
    Write-Host "`nPress Ctrl+C to stop all services..."
    try {
        while ($true) {
            Start-Sleep -Seconds 1
        }
    } catch {
        # User pressed Ctrl+C
        Write-ColorOutput "`nShutting down..." $InfoColor
    }
    
} catch {
    Write-ColorOutput "`nERROR: $($_.Exception.Message)" $ErrorColor
    Write-ColorOutput $_.ScriptStackTrace $ErrorColor
    exit 1
} finally {
    Cleanup
}

