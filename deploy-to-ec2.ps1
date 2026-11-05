# PowerShell Script to Deploy Project to EC2 Instances
# Usage: .\deploy-to-ec2.ps1

# ============================================
# CONFIGURATION - UPDATE THESE VALUES
# ============================================

# Your EC2 instance IPs (use public IPs or private if you're on VPN/same VPC)
$KVS_COORDINATOR = "ec2-user@<KVS_COORDINATOR_PUBLIC_IP>"
$KVS_WORKER = "ec2-user@<KVS_WORKER_PUBLIC_IP>"
$FLAME_COORDINATOR = "ec2-user@<FLAME_COORDINATOR_PUBLIC_IP>"
$FLAME_WORKER = "ec2-user@<FLAME_WORKER_PUBLIC_IP>"
$SUBMIT_INSTANCE = "ec2-user@<SUBMIT_INSTANCE_PUBLIC_IP>"

# Path to your SSH key
$SSH_KEY = "~\.ssh\your-key.pem"

# Remote directory on EC2 instances (where to copy files)
$REMOTE_DIR = "~/Fa25-CIS5550-Project-ACAE"

# Local project directory (current directory)
$LOCAL_DIR = $PSScriptRoot

# ============================================
# FUNCTIONS
# ============================================

function Deploy-ToInstance {
    param(
        [string]$Instance,
        [string]$Name
    )
    
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "Deploying to: $Name ($Instance)" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    
    # Create remote directory if it doesn't exist
    Write-Host "Creating remote directory..." -ForegroundColor Yellow
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no $Instance "mkdir -p $REMOTE_DIR"
    
    # Copy files using rsync (faster and smarter than scp)
    Write-Host "Copying files..." -ForegroundColor Yellow
    
    # Use rsync if available, otherwise fall back to scp
    $rsyncAvailable = Get-Command rsync -ErrorAction SilentlyContinue
    
    if ($rsyncAvailable) {
        # rsync is more efficient - only copies changed files
        rsync -avz --progress `
            -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" `
            --exclude 'bin/' `
            --exclude 'worker*/' `
            --exclude '*.log' `
            --exclude '.git/' `
            --exclude '*.jar' `
            "$LOCAL_DIR/" "${Instance}:${REMOTE_DIR}/"
    } else {
        # Fallback to scp if rsync not available
        Write-Host "Note: rsync not found, using scp (slower). Consider installing rsync." -ForegroundColor Yellow
        scp -i $SSH_KEY -o StrictHostKeyChecking=no -r `
            "$LOCAL_DIR\src" `
            "$LOCAL_DIR\lib" `
            "$LOCAL_DIR\deploy-*.sh" `
            "$LOCAL_DIR\run-job.sh" `
            "$LOCAL_DIR\*.properties" `
            "${Instance}:${REMOTE_DIR}/"
    }
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Successfully deployed to $Name" -ForegroundColor Green
    } else {
        Write-Host "✗ Failed to deploy to $Name" -ForegroundColor Red
    }
}

function Make-ScriptsExecutable {
    param(
        [string]$Instance,
        [string]$Name
    )
    
    Write-Host "Making scripts executable on $Name..." -ForegroundColor Yellow
    ssh -i $SSH_KEY -o StrictHostKeyChecking=no $Instance "chmod +x $REMOTE_DIR/deploy-*.sh $REMOTE_DIR/run-job.sh"
}

# ============================================
# MAIN DEPLOYMENT
# ============================================

Write-Host @"
╔════════════════════════════════════════╗
║   EC2 Deployment Script                ║
║   CIS5550 Crawler Project              ║
╚════════════════════════════════════════╝
"@ -ForegroundColor Cyan

# Check if SSH key exists
if (-not (Test-Path $SSH_KEY)) {
    Write-Host "ERROR: SSH key not found at: $SSH_KEY" -ForegroundColor Red
    Write-Host "Please update the SSH_KEY variable in this script." -ForegroundColor Yellow
    exit 1
}

# Ask which instances to deploy to
Write-Host "`nSelect deployment target:" -ForegroundColor Yellow
Write-Host "  1. All instances"
Write-Host "  2. KVS Coordinator only"
Write-Host "  3. KVS Worker only"
Write-Host "  4. Flame Coordinator only"
Write-Host "  5. Flame Worker only"
Write-Host "  6. Submit Instance only"
Write-Host "  7. All KVS instances"
Write-Host "  8. All Flame instances"
Write-Host ""

$choice = Read-Host "Enter choice (1-8)"

$deployAll = $false
$deployKVSCoord = $false
$deployKVSWorker = $false
$deployFlameCoord = $false
$deployFlameWorker = $false
$deploySubmit = $false

switch ($choice) {
    "1" { $deployAll = $true }
    "2" { $deployKVSCoord = $true }
    "3" { $deployKVSWorker = $true }
    "4" { $deployFlameCoord = $true }
    "5" { $deployFlameWorker = $true }
    "6" { $deploySubmit = $true }
    "7" { $deployKVSCoord = $true; $deployKVSWorker = $true }
    "8" { $deployFlameCoord = $true; $deployFlameWorker = $true }
    default { Write-Host "Invalid choice. Exiting." -ForegroundColor Red; exit 1 }
}

# Deploy to selected instances
if ($deployAll -or $deployKVSCoord) {
    Deploy-ToInstance -Instance $KVS_COORDINATOR -Name "KVS Coordinator"
    Make-ScriptsExecutable -Instance $KVS_COORDINATOR -Name "KVS Coordinator"
}

if ($deployAll -or $deployKVSWorker) {
    Deploy-ToInstance -Instance $KVS_WORKER -Name "KVS Worker"
    Make-ScriptsExecutable -Instance $KVS_WORKER -Name "KVS Worker"
}

if ($deployAll -or $deployFlameCoord) {
    Deploy-ToInstance -Instance $FLAME_COORDINATOR -Name "Flame Coordinator"
    Make-ScriptsExecutable -Instance $FLAME_COORDINATOR -Name "Flame Coordinator"
}

if ($deployAll -or $deployFlameWorker) {
    Deploy-ToInstance -Instance $FLAME_WORKER -Name "Flame Worker"
    Make-ScriptsExecutable -Instance $FLAME_WORKER -Name "Flame Worker"
}

if ($deployAll -or $deploySubmit) {
    Deploy-ToInstance -Instance $SUBMIT_INSTANCE -Name "Submit Instance"
    Make-ScriptsExecutable -Instance $SUBMIT_INSTANCE -Name "Submit Instance"
}

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "Deployment Complete!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Host "  1. SSH into each instance"
Write-Host "  2. Run the appropriate deploy script"
Write-Host "  Example: ./deploy-kvs-coordinator.sh"
