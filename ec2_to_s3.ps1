# EC2 to S3 Sync Script
# Configuration

# anjana - dont need this anymore just send directly to s3 from ec2
$EC2_USER = "ec2-user"
$EC2_IP = "34.201.101.242"  # Replace with your EC2 instance IP
$EC2_PATH = "~/Fa25-CIS5550-Project-ACAE/worker1/pt-crawl"
$LOCAL_PATH = "$env:USERPROFILE\Fa25-CIS5550-Project-ACAE\worker1\pt-crawl"  # Change if you want a different local path
$S3_BUCKET = "s3://555-project-crawl/pt-crawl"
$SSH_KEY = "$env:USERPROFILE\Fa25-CIS5550-Project-ACAE\555final-test.pem"  # Replace with your SSH key path
$CLEANUP_AGE_MINUTES = 60


# Create local directory if it doesn't exist
if (-Not (Test-Path $LOCAL_PATH)) {
    New-Item -ItemType Directory -Path $LOCAL_PATH -Force | Out-Null
    Write-Host "Created local directory: $LOCAL_PATH" -ForegroundColor Green
}

# Step 1: Download from EC2 using SCP
Write-Host "`nStep 1: Downloading from EC2..." -ForegroundColor Cyan
Write-Host "From: ${EC2_USER}@${EC2_IP}:${EC2_PATH}" -ForegroundColor Gray
Write-Host "To: $LOCAL_PATH" -ForegroundColor Gray

scp -i $SSH_KEY -r "${EC2_USER}@${EC2_IP}:${EC2_PATH}" $LOCAL_PATH

if ($LASTEXITCODE -eq 0) {
    Write-Host "Download completed successfully!" -ForegroundColor Green
} else {
    Write-Host "Download failed with error code: $LASTEXITCODE" -ForegroundColor Red
    exit 1
}

# Step 2: Upload to S3
Write-Host "`nStep 2: Uploading to S3..." -ForegroundColor Cyan
Write-Host "From: $LOCAL_PATH" -ForegroundColor Gray
Write-Host "To: $S3_BUCKET" -ForegroundColor Gray

aws s3 sync $LOCAL_PATH $S3_BUCKET

if ($LASTEXITCODE -eq 0) {
    Write-Host "Upload completed successfully!" -ForegroundColor Green
    
    # Step 3: Clean up old files on EC2
    Write-Host "`nStep 3: Cleaning up old files on EC2..." -ForegroundColor Cyan
    Write-Host "Deleting files older than $CLEANUP_AGE_MINUTES minutes" -ForegroundColor Gray
    
    $cleanupCommand = "find ~/Fa25-CIS5550-Project-ACAE/worker1/pt-crawl -type f -mmin +$CLEANUP_AGE_MINUTES -delete"
    ssh -i $SSH_KEY "${EC2_USER}@${EC2_IP}" $cleanupCommand
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Cleanup completed successfully!" -ForegroundColor Green
    } else {
        Write-Host "Warning: Cleanup failed with error code: $LASTEXITCODE" -ForegroundColor Yellow
        Write-Host "Files were still uploaded to S3 successfully." -ForegroundColor Yellow
    }
    
    # Step 4: Clean up local files (optional)
    Write-Host "`nStep 4: Cleaning up local files..." -ForegroundColor Cyan
    Remove-Item -Path $LOCAL_PATH -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "Local cleanup completed!" -ForegroundColor Green
    
} else {
    Write-Host "Upload failed with error code: $LASTEXITCODE" -ForegroundColor Red
    Write-Host "Skipping cleanup to preserve data." -ForegroundColor Yellow
    exit 1
}

Write-Host "`nSync complete! Data transferred from EC2 to S3 and old files cleaned up." -ForegroundColor Green