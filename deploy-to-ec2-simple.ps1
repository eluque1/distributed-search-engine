# Simple EC2 Deployment Script - Deploys to all instances at once
# Usage: .\deploy-to-ec2-simple.ps1

# ============================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================

# List all your EC2 instance IPs here (public IPs)
$INSTANCES = @(
    "ec2-user@98.82.141.81"   # final server
    # "ec2-user@34.239.43.135",    # KVS Coordinator
    # "ec2-user@52.206.47.205",    # Flame Coordinator
    # "ec2-user@3.84.88.10",       # Worker 1
    # "ec2-user@54.198.165.68",    # Worker 2
    # "ec2-user@98.82.185.224",    # Worker 3
    # "ec2-user@100.31.81.164",    # Worker 4
    # "ec2-user@3.84.208.183",     # Worker 5
    # "ec2-user@52.23.153.87",     # Worker 6
    # "ec2-user@98.81.194.2",      # Worker 7
    # "ec2-user@54.81.193.70",     # Worker 8
    # "ec2-user@100.48.11.171",    # Worker 9
    # "ec2-user@13.223.185.74",    # Worker 10
    # "ec2-user@75.101.248.108",   # Worker 11
    # "ec2-user@18.232.70.81",     # Worker 12
    # "ec2-user@18.212.140.182",   # Worker 13
    # "ec2-user@54.84.18.84",      # Worker 14
    # "ec2-user@52.71.43.180"      # Worker 15
)

# Path to your SSH key
$SSH_KEY = "C:\Users\anjan\Fa25-CIS5550-Project-ACAE\kvs-coord.pem"

# Remote directory
$REMOTE_DIR = "~/Fa25-CIS5550-Project-ACAE"

# ============================================
# DEPLOYMENT
# ============================================

Write-Host "Deploying to all EC2 instances..." -ForegroundColor Cyan

$count = 0
foreach ($instance in $INSTANCES) {
    if ($instance -match "<.*>") {
        Write-Host "Skipping unconfigured instance: $instance" -ForegroundColor Yellow
        continue
    }
    
    $count++
    Write-Host "`n[$count/$($INSTANCES.Count)] Deploying to: $instance" -ForegroundColor Green
    
    # Create directory
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no $instance "mkdir -p $REMOTE_DIR" 2>$null
    
    # Copy directories
    scp -i $SSH_KEY -o StrictHostKeyChecking=no -r src lib "${instance}:${REMOTE_DIR}/" 2>$null
    
    # Copy shell scripts
    scp -i $SSH_KEY -o StrictHostKeyChecking=no `
        deploy-flame-coordinator.sh deploy-flame-worker.sh `
        deploy-kvs-coordinator.sh deploy-kvs-worker.sh monitor-crawler.sh `
        deploy-submit-job.sh run-job.sh start-services.ps1 sync-worker-s3.sh kvs-coord.pem `
        "${instance}:${REMOTE_DIR}/" 2>$null
    
    # Copy properties files
    scp -i $SSH_KEY -o StrictHostKeyChecking=no *.properties "${instance}:${REMOTE_DIR}/" 2>$null
    
    # Fix line endings and make scripts executable
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no $instance `
        "cd $REMOTE_DIR && sed -i 's/\r$//' *.sh && chmod +x *.sh" 2>$null
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [SUCCESS]" -ForegroundColor Green
    } else {
        Write-Host "  [FAILED]" -ForegroundColor Red
    }
}

Write-Host "`nDeployment complete!" -ForegroundColor Green
