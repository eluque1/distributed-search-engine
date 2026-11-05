<#
.SYNOPSIS
    Automated script to compile, start services, and run CIS5550 jobs.

.DESCRIPTION
    This script automates the entire process of:
    1. Compiling all Java files
    2. Starting 1 KVS Coordinator and 1 KVS Worker
    3. Starting 1 Flame Coordinator and 2 Flame Workers
    4. Creating a JAR for the specified job
    5. Submitting the job to Flame

.NOTES
    Configuration is hardcoded:
    - 1 KVS Coordinator (port 8000)
    - 1 KVS Worker (port 8001, directory: worker1)
    - 1 Flame Coordinator (port 9000)
    - 2 Flame Workers (ports 9001-9002)
    
    To change the job, edit the $Job variable at the top of the script.
    To add job arguments, edit the $JobArgs variable.

.EXAMPLE
    .\run-job.ps1
    
    Runs the indexer job with the hardcoded configuration
#>

# Configuration - can be set via command-line argument or defaults to "indexer"
param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("crawler", "crawler_l", "pagerank", "indexer", "indexermerge", "graphbuilder")]
    [string]$Job = "crawler",

    [Parameter(Mandatory=$false)]
    [string]$JobArgs = ""
)


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
    Write-Log "=== Compiling all Java files ===" -NoConsole
    
    # Ensure bin directory exists
    if (-not (Test-Path "bin")) {
        New-Item -ItemType Directory -Path "bin" | Out-Null
        Write-ColorOutput "Created bin directory" $InfoColor
        Write-Log "Created bin directory" -NoConsole
    }
    
    # Get all Java files recursively
    $javaFiles = Get-ChildItem -Recurse -Path "src" -Filter "*.java" | ForEach-Object { $_.FullName }
    
    if ($javaFiles.Count -eq 0) {
        $errorMsg = "ERROR: No Java files found in src directory"
        Write-ColorOutput $errorMsg $ErrorColor
        Write-Log $errorMsg -NoConsole
        exit 1
    }
    
    $msg = "Found $($javaFiles.Count) Java file(s) to compile"
    Write-ColorOutput $msg $InfoColor
    Write-Log $msg -NoConsole
    
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
        # Add OpenNLP JAR
        if (Test-Path "$libPath\opennlp-tools-2.3.2.jar") {
            $classpath += ";$libPath\opennlp-tools-2.3.2.jar"
        }
    }
    
    Write-ColorOutput "Using classpath: $classpath" $InfoColor
    Write-Log "Using classpath: $classpath" -NoConsole
    
    # Compile with classpath
    $javacArgs = @("-cp", $classpath, "-d", "bin") + $javaFiles
    
    $compileOutput = & javac @javacArgs 2>&1
    foreach ($line in $compileOutput) {
        if ($line -match "error") {
            Write-ColorOutput $line $ErrorColor
            Write-Log $line -NoConsole
        } else {
            Write-Host $line
            Write-Log $line -NoConsole
        }
    }
    
    if ($LASTEXITCODE -ne 0) {
        $errorMsg = "ERROR: Compilation failed!"
        Write-ColorOutput "`n$errorMsg" $ErrorColor
        Write-Log $errorMsg -NoConsole
        exit 1
    }
    
    $successMsg = "Compilation successful!"
    Write-ColorOutput $successMsg $SuccessColor
    Write-Log $successMsg -NoConsole
}

function Start-KvsCoordinator {
    Write-ColorOutput "`n=== Starting KVS Coordinator on port 8000 ===" $InfoColor
    Write-Log "=== Starting KVS Coordinator on port 8000 ===" -NoConsole
    $script:kvsCoordinator = Start-Process -FilePath "java" `
        -ArgumentList "-Xmx2g", "-cp", "bin", "cis5550.kvs.Coordinator", "8000" `
        -PassThru -WindowStyle Normal
    Start-Sleep -Seconds 2
    $msg = "KVS Coordinator started (PID: $($script:kvsCoordinator.Id))"
    Write-ColorOutput $msg $SuccessColor
    Write-Log $msg -NoConsole
}

function Start-KvsWorkers {
    Write-ColorOutput "`n=== Starting 4 KVS Worker ===" $InfoColor
    Write-Log "=== Starting 4 KVS Worker ===" -NoConsole
    
    # Initialize ArrayList for workers
    $script:kvsWorkers = New-Object System.Collections.ArrayList
    
    $workers = @(
        @{ Port = 8001; Dir = "worker1" },
        @{ Port = 8002; Dir = "worker2" },
        @{ Port = 8003; Dir = "worker3" },
        @{ Port = 8004; Dir = "worker4" }
    )
    
    foreach ($w in $workers) {
        if (-not (Test-Path $w.Dir)) {
            New-Item -ItemType Directory -Path $w.Dir | Out-Null
        }
        
        $msg = "Starting KVS Worker on port $($w.Port) (directory: $($w.Dir))"
        Write-ColorOutput $msg $InfoColor
        Write-Log $msg -NoConsole
        
        $proc = Start-Process -FilePath "java" `
            -ArgumentList "-Xmx2g", "-cp", "bin", "cis5550.kvs.Worker", "$($w.Port)", "$($w.Dir)", "localhost:8000" `
            -PassThru -WindowStyle Normal
        
        if ($proc) {
            [void]$script:kvsWorkers.Add($proc)
            Write-Log "KVS Worker on port $($w.Port) started (PID: $($proc.Id))" -NoConsole
        } else {
            Write-Log "ERROR: Failed to start KVS Worker on port $($w.Port)" -NoConsole
        }
        
        Start-Sleep -Milliseconds 500
    }
    
    Write-ColorOutput "KVS Workers started" $SuccessColor
    Write-Log "KVS Workers started" -NoConsole
}

function Start-FlameCoordinator {
    Write-ColorOutput "`n=== Starting Flame Coordinator on port 9000 ===" $InfoColor
    Write-Log "=== Starting Flame Coordinator on port 9000 ===" -NoConsole
    $script:flameCoordinator = Start-Process -FilePath "java" `
        -ArgumentList "-Xmx2g", "-cp", "bin", "cis5550.flame.Coordinator", "9000", "localhost:8000" `
        -PassThru -WindowStyle Normal
    Start-Sleep -Seconds 2
    $msg = "Flame Coordinator started (PID: $($script:flameCoordinator.Id))"
    Write-ColorOutput $msg $SuccessColor
    Write-Log $msg -NoConsole
}

function Start-FlameWorkers {
    Write-ColorOutput "`n=== Starting 4 Flame Worker(s) ===" $InfoColor
    Write-Log "=== Starting 4 Flame Worker(s) ===" -NoConsole
    
    # Initialize ArrayList for workers
    $script:flameWorkers = New-Object System.Collections.ArrayList
    
    $workerCount = 4    
    for ($i = 1; $i -le $workerCount; $i++) {
        $port = 9000 + $i
        $msg = "Starting Flame Worker $i on port $port"
        Write-ColorOutput $msg $InfoColor
        Write-Log $msg -NoConsole
        $proc = Start-Process -FilePath "java" `
            -ArgumentList "-Xmx2g", "-cp", "bin", "cis5550.flame.Worker", "$port", "localhost:9000" `
            -PassThru -WindowStyle Normal
        
        
        if ($proc) {
            [void]$script:flameWorkers.Add($proc)
            Write-Log "Flame Worker $i started (PID: $($proc.Id))" -NoConsole
        } else {
            Write-Log "ERROR: Failed to start Flame Worker $i" -NoConsole
        }
        Start-Sleep -Seconds 1
    }
    
    Write-ColorOutput "All Flame Workers started" $SuccessColor
    Write-Log "All Flame Workers started" -NoConsole
}

function Create-JobJar {
    param([string]$JobName, [string]$ClassName)
    
    Write-ColorOutput "`n=== Creating JAR for $JobName ===" $InfoColor
    Write-Log "=== Creating JAR for $JobName ===" -NoConsole
    
    $jarName = "$JobName.jar"
    
    # Remove existing JAR if it exists
    if (Test-Path $jarName) {
        Remove-Item $jarName -Force
        Write-ColorOutput "Removed existing $jarName" $InfoColor
        Write-Log "Removed existing $jarName" -NoConsole
    }
    
    # Create JAR - include the entire jobs package and all dependencies
    $classFile = "bin\cis5550\jobs\$ClassName.class"
    if (-not (Test-Path $classFile)) {
        $errorMsg = "ERROR: Class file not found: $classFile"
        Write-ColorOutput $errorMsg $ErrorColor
        Write-ColorOutput "Make sure compilation succeeded and the class exists." $ErrorColor
        Write-Log $errorMsg -NoConsole
        exit 1
    }
    
    # Include all classes in the jobs package
    # This ensures all inner classes and dependencies are included
    $jarOutput = & jar cf $jarName -C bin "cis5550\jobs" 2>&1
    if ($jarOutput) {
        Write-Log $jarOutput -NoConsole
    }
    
    # Add OpenNLP classes to the JAR if available
    $opennlpJar = $null
    $libPaths = @("lib", "HW6\lib", ".")
    foreach ($libPath in $libPaths) {
        if (Test-Path "$libPath\opennlp-tools-2.3.2.jar") {
            $opennlpJar = "$libPath\opennlp-tools-2.3.2.jar"
            break
        }
    }
    
    if ($opennlpJar -and (Test-Path $opennlpJar)) {
        Write-ColorOutput "Including OpenNLP classes from $opennlpJar" $InfoColor
        Write-Log "Including OpenNLP classes from $opennlpJar" -NoConsole
        
        # Extract OpenNLP JAR to temp directory and add to job JAR
        $tempDir = New-TemporaryFile | ForEach-Object { Remove-Item $_; New-Item -ItemType Directory -Path $_ }
        try {
            # Extract OpenNLP JAR
            & jar xf $opennlpJar -C $tempDir.FullName 2>&1 | Out-Null
            
            # Add extracted classes to job JAR (skip META-INF to avoid conflicts)
            $opennlpClasses = Get-ChildItem -Path $tempDir.FullName -Recurse -File | Where-Object { 
                $_.FullName -notlike "*\META-INF\*" 
            }
            if ($opennlpClasses.Count -gt 0) {
                $jarOutput2 = & jar uf $jarName -C $tempDir.FullName "." 2>&1
                if ($jarOutput2) {
                    Write-Log $jarOutput2 -NoConsole
                }
                Write-ColorOutput "Added OpenNLP classes to JAR" $SuccessColor
            }
        } finally {
            Remove-Item -Path $tempDir.FullName -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
    
    if ($LASTEXITCODE -ne 0) {
        $errorMsg = "ERROR: Failed to create JAR"
        Write-ColorOutput $errorMsg $ErrorColor
        Write-Log $errorMsg -NoConsole
        exit 1
    }
    
    Write-ColorOutput "Created $jarName" $SuccessColor
    Write-Log "Created $jarName" -NoConsole
    return $jarName
}

function Delete-Table {
    param([string]$TableName)
    
    Write-ColorOutput "`n=== Deleting table: $TableName ===" $InfoColor
    Write-Log "=== Deleting table: $TableName ===" -NoConsole
    
    try {
        $deleteOutput = & java -cp bin cis5550.kvs.KVSClient localhost:8000 delete $TableName 2>&1
        
        foreach ($line in $deleteOutput) {
            Write-Log $line -NoConsole
        }
        
        if ($LASTEXITCODE -eq 0) {
            $msg = "Successfully deleted table: $TableName"
            Write-ColorOutput $msg $SuccessColor
            Write-Log $msg -NoConsole
        } else {
            $msg = "Note: Table $TableName may not exist (this is OK)"
            Write-ColorOutput $msg $InfoColor
            Write-Log $msg -NoConsole
        }
    } catch {
        $msg = "Warning: Could not delete table $TableName : $($_.Exception.Message)"
        Write-ColorOutput $msg $InfoColor
        Write-Log $msg -NoConsole
    }
}


function Submit-Job {
    param([string]$JarName, [string]$ClassName, [string[]]$ArgsArray)
    # print args array
    Write-ColorOutput "Args array: $ArgsArray" $InfoColor
    Write-Log "Args array: $ArgsArray" -NoConsole
    foreach ($arg in $ArgsArray) {
        Write-ColorOutput "Arg: $arg" $InfoColor
        Write-Log "Arg: $arg" -NoConsole
    }
    
    Write-ColorOutput "`n=== Submitting $Job job ===" $InfoColor
    Write-Log "=== Submitting $Job job ===" -NoConsole
    Write-Log "JAR: $JarName" -NoConsole
    Write-Log "Class: $ClassName" -NoConsole
    Write-Log "Args: $ArgsArray" -NoConsole
    Write-ColorOutput "Args: $ArgsArray" $InfoColor  
    
    Write-ColorOutput "JAR: $JarName" $InfoColor
    Write-ColorOutput "Class: $ClassName" $InfoColor
    
    Write-Log "--- Job Output Start ---" -NoConsole
    Write-ColorOutput "`n--- Job Output ---" $InfoColor
    
    # Build submit arguments: coordinator, jar, class, then job args
    $submitArgs = @("localhost:9000", $JarName, $ClassName)
    $allSubmitArgs = @($submitArgs + $ArgsArray)
    
    Write-Log "Full submit command: java -cp bin cis5550.flame.FlameSubmit $($allSubmitArgs -join ' ')" -NoConsole
    
    # Capture output from job submission
    $jobOutput = & java -cp bin cis5550.flame.FlameSubmit @allSubmitArgs 2>&1
    
    # Write all output to log and console
    foreach ($line in $jobOutput) {
        Write-Log $line
    }
    
    Write-Log "--- Job Output End ---" -NoConsole
    Write-ColorOutput "--- End Job Output ---`n" $InfoColor
    
    if ($LASTEXITCODE -ne 0) {
        $errorMsg = "ERROR: Job submission failed!"
        Write-ColorOutput "`n$errorMsg" $ErrorColor
        Write-Log $errorMsg -NoConsole
    } else {
        $successMsg = "Job submitted successfully!"
        Write-ColorOutput "`n$successMsg" $SuccessColor
        Write-Log $successMsg -NoConsole
    }
}

function Cleanup {
    Write-ColorOutput "`n=== Cleaning up processes ===" $InfoColor
    Write-Log "=== Cleaning up processes ===" -NoConsole
    
    if ($script:flameWorkers -and $script:flameWorkers.Count -gt 0) {
        foreach ($proc in $script:flameWorkers) {
            if ($proc -and $proc.Id -and -not $proc.HasExited) {
                try {
                    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
                    Write-Log "Stopped Flame Worker (PID: $($proc.Id))" -NoConsole
                } catch {
                    Write-Log "Error stopping Flame Worker: $($_.Exception.Message)" -NoConsole
                }
            }
        }
    }
    
    if ($script:kvsWorkers -and $script:kvsWorkers.Count -gt 0) {
        foreach ($proc in $script:kvsWorkers) {
            if ($proc -and $proc.Id -and -not $proc.HasExited) {
                try {
                    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
                    Write-Log "Stopped KVS Worker (PID: $($proc.Id))" -NoConsole
                } catch {
                    Write-Log "Error stopping KVS Worker: $($_.Exception.Message)" -NoConsole
                }
            }
        }
    }
    
    if ($script:flameCoordinator -and $script:flameCoordinator.Id -and -not $script:flameCoordinator.HasExited) {
        try {
            Stop-Process -Id $script:flameCoordinator.Id -Force -ErrorAction SilentlyContinue
            Write-Log "Stopped Flame Coordinator (PID: $($script:flameCoordinator.Id))" -NoConsole
        } catch {
            Write-Log "Error stopping Flame Coordinator: $($_.Exception.Message)" -NoConsole
        }
    }
    
    if ($script:kvsCoordinator -and $script:kvsCoordinator.Id -and -not $script:kvsCoordinator.HasExited) {
        try {
            Stop-Process -Id $script:kvsCoordinator.Id -Force -ErrorAction SilentlyContinue
            Write-Log "Stopped KVS Coordinator (PID: $($script:kvsCoordinator.Id))" -NoConsole
        } catch {
            Write-Log "Error stopping KVS Coordinator: $($_.Exception.Message)" -NoConsole
        }
    }
    
    Write-ColorOutput "Cleanup complete" $SuccessColor
    Write-Log "Cleanup complete" -NoConsole
}

# Store process IDs for cleanup
# Initialize as null, will be created as ArrayList in functions
$script:kvsCoordinator = $null
$script:kvsWorkers = $null
$script:flameCoordinator = $null
$script:flameWorkers = $null

# Logging setup
$script:logFile = $null
$script:logStream = $null

function Initialize-Log {
    param([string]$JobName)
    
    $timestamp = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
    $logFileName = "${JobName}_${timestamp}.log"
    $script:logFile = $logFileName
    
    # Create log file and write header
    $header = @"
========================================
Job Runner Log
========================================
Job: $JobName
Started: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
KVS Workers: 1
Flame Workers: 2
========================================

"@
    
    $header | Out-File -FilePath $logFileName -Encoding UTF8
    $script:logStream = [System.IO.StreamWriter]::new($logFileName, $true, [System.Text.Encoding]::UTF8)
    
    Write-ColorOutput "Log file: $logFileName" $InfoColor
}

function Write-Log {
    param([string]$Message, [switch]$NoConsole)
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] $Message"
    
    if ($script:logStream) {
        $script:logStream.WriteLine($logMessage)
        $script:logStream.Flush()
    }
    
    if (-not $NoConsole) {
        Write-Host $Message
    }
}

function Close-Log {
    if ($script:logStream) {
        $footer = @"

========================================
Job Runner Log Ended
Ended: $(Get-Date -Format "yyyy-MM-dd HH:mm:ss")
========================================
"@
        $script:logStream.WriteLine($footer)
        $script:logStream.Close()
        $script:logStream = $null
    }
}

function Split-JobArgs {
    param([string]$JobArgsText)

    if ([string]::IsNullOrWhiteSpace($JobArgsText)) {
        return @()
    }

    # Split on whitespace while preserving quoted segments.
    $matches = [regex]::Matches($JobArgsText, '("[^"]*"|''[^'']*''|\S+)')
    $result = @()

    foreach ($m in $matches) {
        $token = $m.Value
        if (
            ($token.StartsWith('"') -and $token.EndsWith('"')) -or
            ($token.StartsWith("'") -and $token.EndsWith("'"))
        ) {
            $token = $token.Substring(1, $token.Length - 2)
        }
        if ($token.Length -gt 0) {
            $result += $token
        }
    }

    return $result
}

# Main execution
Write-ColorOutput "========================================" $InfoColor
Write-ColorOutput "  Job Runner Script" $InfoColor
Write-ColorOutput "========================================" $InfoColor
Write-ColorOutput "Job: $Job" $InfoColor
Write-ColorOutput "KVS Workers: 1" $InfoColor
Write-ColorOutput "Flame Workers: 2" $InfoColor

# Debug: Check JobArgs handling
if ($JobArgs -eq $null) {
    Write-ColorOutput "Job Args: null" $InfoColor
    $JobArgs = @()
} elseif ($JobArgs -is [string]) {
    Write-ColorOutput "Job Args (single string): $JobArgs" $InfoColor
    $JobArgs = @($JobArgs)
} else {
    Write-ColorOutput "Job Args (array, count=$($JobArgs.Count)): $($JobArgs -join ', ')" $InfoColor
}

Write-ColorOutput "========================================" $InfoColor

try {
    # Initialize logging
    Initialize-Log -JobName $Job
    
    # Step 1: Compile
    Compile-All
    
    # Step 2: Start KVS Coordinator
    Start-KvsCoordinator
    
    # Step 3: Start KVS Workers
    Start-KvsWorkers
    
    # Step 4: Start Flame Coordinator
    Start-FlameCoordinator
    
    # Step 5: Start Flame Workers
    Start-FlameWorkers
    
    # Wait a bit for all services to initialize
    Write-ColorOutput "`nWaiting 5 seconds for services to initialize..." $InfoColor
    Start-Sleep -Seconds 5
    
    # Step 6: Delete pt-index table if running indexer job
    #if ($Job -eq "indexer") {
    #    Delete-Table -TableName "pt-index"
    #}
    
    # Step 7: Create JAR and submit job
    $className = switch ($Job) {
        "crawler" { "Crawler" }
        "crawler_l" { "Crawler_l" }
        "pagerank" { "PageRank" }
        "indexer" { "Indexer" }
        "indexermerge" { "IndexerMerge" }
        "graphbuilder" { "GraphBuilder" }
    }
    # print job args 
    Write-ColorOutput "Job args: $JobArgs" $InfoColor
    $ArgsArray = Split-JobArgs -JobArgsText "$JobArgs"
    # print args array
    Write-ColorOutput "Args array: $ArgsArray" $InfoColor
    Write-Log "Args array: $ArgsArray" -NoConsole
    foreach ($arg in $ArgsArray) {
        Write-ColorOutput "Arg: $arg" $InfoColor
        Write-Log "Arg: $arg" -NoConsole
    }
    $jarName = Create-JobJar -JobName $Job -ClassName $className
    
    # Step 8: Submit job
    Submit-Job -JarName $jarName -ClassName "cis5550.jobs.$className" -ArgsArray $ArgsArray
    
    Write-ColorOutput "`n=== Setup Complete ===" $SuccessColor
    Write-Log "=== Setup Complete ===" -NoConsole
    Write-ColorOutput "All services are running." $InfoColor
    Write-Log "All services are running." -NoConsole
    Write-ColorOutput "`nProcess IDs:" $InfoColor
    Write-Log "Process IDs:" -NoConsole
    if ($script:kvsCoordinator -and $script:kvsCoordinator.Id) {
        Write-ColorOutput "  KVS Coordinator: $($script:kvsCoordinator.Id)" $InfoColor
        Write-Log "  KVS Coordinator: $($script:kvsCoordinator.Id)" -NoConsole
    }
    if ($script:flameCoordinator -and $script:flameCoordinator.Id) {
        Write-ColorOutput "  Flame Coordinator: $($script:flameCoordinator.Id)" $InfoColor
        Write-Log "  Flame Coordinator: $($script:flameCoordinator.Id)" -NoConsole
    }
    Write-ColorOutput "`nKVS Workers:" $InfoColor
    Write-Log "KVS Workers:" -NoConsole
    if ($script:kvsWorkers -and $script:kvsWorkers.Count -gt 0) {
        for ($i = 0; $i -lt $script:kvsWorkers.Count; $i++) {
            $proc = $script:kvsWorkers[$i]
            if ($proc -and $proc.Id) {
                $msg = "  Worker $($i+1): $($proc.Id)"
                Write-ColorOutput $msg $InfoColor
                Write-Log $msg -NoConsole
            }
        }
    }
    Write-ColorOutput "`nFlame Workers:" $InfoColor
    Write-Log "Flame Workers:" -NoConsole
    if ($script:flameWorkers -and $script:flameWorkers.Count -gt 0) {
        for ($i = 0; $i -lt $script:flameWorkers.Count; $i++) {
            $proc = $script:flameWorkers[$i]
            if ($proc -and $proc.Id) {
                $msg = "  Worker $($i+1): $($proc.Id)"
                Write-ColorOutput $msg $InfoColor
                Write-Log $msg -NoConsole
            }
        }
    }
    
    Write-ColorOutput "`nLog file: $($script:logFile)" $InfoColor
    Write-ColorOutput "You can monitor the log file in real-time using: Get-Content $($script:logFile) -Wait -Tail 50" $InfoColor
    
    # Keep script running until user presses Ctrl+C
    Write-Host "`nPress Ctrl+C to stop all services..."
    try {
        while ($true) {
            Start-Sleep -Seconds 1
        }
    } catch {
        # User pressed Ctrl+C
        Write-ColorOutput "`nShutting down..." $InfoColor
        Write-Log "Shutting down..." -NoConsole
    }
    
} catch {
    $errorMsg = "ERROR: $($_.Exception.Message)"
    Write-ColorOutput "`n$errorMsg" $ErrorColor
    Write-ColorOutput $_.ScriptStackTrace $ErrorColor
    Write-Log $errorMsg -NoConsole
    Write-Log $_.ScriptStackTrace -NoConsole
    exit 1
} finally {
    Cleanup
    Close-Log
}

