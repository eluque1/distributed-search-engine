# === CONFIGURATION ===
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

Write-Host "`n=== STOPPING CLUSTER ===" -ForegroundColor Yellow

# Stop KVS Coordinator
Write-Host "==== Stopping KVS Coordinator on $KvsCoordPublic (port $KvsPort) ====" -ForegroundColor Red
ssh -o StrictHostKeyChecking=no -i $Key ec2-user@$KvsCoordPublic `
    "sudo fuser -k $KvsPort/tcp 2>/dev/null || echo 'No process on port $KvsPort'"

# Stop Flame Coordinator
Write-Host "==== Stopping Flame Coordinator on $FlameCoordPublic (port $FlamePort) ====" -ForegroundColor Red
ssh -o StrictHostKeyChecking=no -i $Key ec2-user@$FlameCoordPublic `
    "sudo fuser -k $FlamePort/tcp 2>/dev/null || echo 'No process on port $FlamePort'"

# Stop all workers (both KVS and Flame workers on each)
foreach ($w in $Workers) {
    $ip = $w.Public
    $name = $w.Name
    
    Write-Host "==== Stopping workers on $name ($ip) ====" -ForegroundColor Red
    
    # Kill both KVS worker (port 8001) and Flame worker (port 9001)
    ssh -o StrictHostKeyChecking=no -i $Key ec2-user@$ip `
        "sudo fuser -k $KvsWorkerPort/tcp 2>/dev/null || echo 'No KVS worker on port $KvsWorkerPort'; sudo fuser -k $FlameWorkerPort/tcp 2>/dev/null || echo 'No Flame worker on port $FlameWorkerPort'"
}

Write-Host "`n=== DONE ===" -ForegroundColor Green

