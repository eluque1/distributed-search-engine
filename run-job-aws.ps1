# === CONFIGURATION ===

# Which job to run: "crawler", "s3loader", "indexer", "pagerank"
$JobType = "indexer"  # <-- CHANGE THIS

$Key = "kvs-coord.pem"

# KVS Coordinator
$KvsCoordPublic  = "34.239.43.135"
$KvsCoordPrivate = "172.31.33.1"

# Flame Coordinator
$FlameCoordPublic  = "52.206.47.205"
$FlameCoordPrivate = "172.31.37.2"

# Submit node
$SubmitPublic = "34.239.43.135"

$Bucket = "s3://555-project-crawl"
$SeedUrl = "https://en.wikipedia.org/wiki/Kristen_Stewart"

# Number of workers (for s3loader job)
$NumWorkers = 15

$KvsPort = 8000
$FlamePort = 9000

# === Worker definitions ===
$Workers = @(
    @{ Public="3.84.88.10";      Private="172.31.44.108"; Name="worker1" }
    @{ Public="54.198.165.68";   Private="172.31.38.13";  Name="worker2" }
    @{ Public="98.82.185.224";   Private="172.31.38.13";  Name="worker3" }
    @{ Public="100.31.81.164";   Private="172.31.46.175"; Name="worker4" }
    @{ Public="3.84.208.183";    Private="172.31.37.243"; Name="worker5" }
    @{ Public="52.23.153.87";    Private="172.31.34.246"; Name="worker6" }
    @{ Public="98.81.194.2";     Private="172.31.46.152"; Name="worker7" }
    @{ Public="54.81.193.70";    Private="172.31.37.220"; Name="worker8" }
    @{ Public="100.48.11.171";   Private="172.31.41.160"; Name="worker9" }
    @{ Public="13.223.185.74";   Private="172.31.47.133"; Name="worker10" }
    @{ Public="75.101.248.108";  Private="172.31.39.38";  Name="worker11" }
    @{ Public="18.232.70.81";    Private="172.31.44.136"; Name="worker12" }
    @{ Public="18.212.140.182";  Private="172.31.34.41";  Name="worker13" }
    @{ Public="54.84.18.84";     Private="172.31.34.138"; Name="worker14" }
    @{ Public="52.71.43.180";    Private="172.31.32.172"; Name="worker15" }
)

$KvsWorkerPort = 8001
$FlameWorkerPort = 9001

# === Helper Function ===
function Run-Remote($ip, $cmd) {
    Write-Host "==== Running on $ip ====" -ForegroundColor Cyan
    ssh -o StrictHostKeyChecking=no -i $Key ec2-user@$ip "$cmd"
}

function Run-Remote-Background($ip, $cmd) {
    Write-Host "==== Running on $ip (background) ====" -ForegroundColor Cyan
    # Run command with nohup in background, then exit immediately
    $bgCmd = "nohup bash -c '$cmd' > /tmp/deploy.log 2>&1 & echo 'Started in background'"
    ssh -o StrictHostKeyChecking=no -i $Key ec2-user@$ip "$bgCmd"
}

# === Deploy KVS Coordinator ===
Write-Host "`n=== Deploying KVS Coordinator ===" -ForegroundColor Yellow
Run-Remote-Background $KvsCoordPublic "cd Fa25-CIS5550-Project-ACAE && ./deploy-kvs-coordinator.sh"
Write-Host "KVS Coordinator started in background" -ForegroundColor Green
Start-Sleep -Seconds 5  # Give it time to start

# === Deploy KVS Workers ===
Write-Host "`n=== Deploying KVS Workers ===" -ForegroundColor Yellow
foreach ($w in $Workers) {
    Write-Host "Starting KVS Worker: $($w.Name) on $($w.Public)" -ForegroundColor Cyan
    Run-Remote-Background $w.Public "cd Fa25-CIS5550-Project-ACAE && ./deploy-kvs-worker.sh $KvsWorkerPort $($w.Name) '${KvsCoordPrivate}:$KvsPort' $Bucket"
}
Write-Host "All KVS Workers started in background" -ForegroundColor Green
Start-Sleep -Seconds 5

# === Deploy Flame Coordinator ===
Write-Host "`n=== Deploying Flame Coordinator ===" -ForegroundColor Yellow
Run-Remote-Background $FlameCoordPublic "cd Fa25-CIS5550-Project-ACAE && ./deploy-flame-coordinator.sh $FlamePort '${KvsCoordPrivate}:$KvsPort'"
Write-Host "Flame Coordinator started in background" -ForegroundColor Green
Start-Sleep -Seconds 5

# === Deploy Flame Workers ===
Write-Host "`n=== Deploying Flame Workers ===" -ForegroundColor Yellow
foreach ($w in $Workers) {
    Write-Host "Starting Flame Worker: $($w.Name) on $($w.Public)" -ForegroundColor Cyan
    Run-Remote-Background $w.Public "cd Fa25-CIS5550-Project-ACAE && ./deploy-flame-worker.sh $FlameWorkerPort '${FlameCoordPrivate}:$FlamePort' $($w.Name)"
}
Write-Host "All Flame Workers started in background" -ForegroundColor Green
Start-Sleep -Seconds 10

# === Submit Job ===
Write-Host "`n=== Submitting Job ===" -ForegroundColor Yellow

# Determine job arguments based on job type
$JobArgs = switch ($JobType) {
    "crawler"   { "`"$SeedUrl`"" }
    "s3loader"  { "`"555-project-crawl $NumWorkers`"" }
    "indexer"   { "`"`"" }  # No args
    "pagerank"  { "`"`"" }  # No args
    default     { "`"`"" }
}

Write-Host "Job Type: $JobType" -ForegroundColor Cyan
Write-Host "Job Args: $JobArgs" -ForegroundColor Cyan

Run-Remote $SubmitPublic "cd Fa25-CIS5550-Project-ACAE && ./deploy-submit-job.sh `"$JobType`" `"$JobArgs`" `"${FlameCoordPrivate}:$FlamePort`""

Write-Host "`n=== Deployment Completed Successfully 🎉 ===" -ForegroundColor Green
