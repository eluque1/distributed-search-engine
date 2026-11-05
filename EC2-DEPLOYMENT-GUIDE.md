 # Quick EC2 Deployment Guide

## Step 1: Configure Deployment Script

Edit `deploy-to-ec2-simple.ps1` and update:

```powershell
# Replace with your actual EC2 public IPs
$INSTANCES = @(
    "ec2-user@18.123.45.67",   # KVS Coordinator
    "ec2-user@18.123.45.68",   # KVS Worker
    "ec2-user@18.123.45.69",   # Flame Coordinator
    "ec2-user@18.123.45.70",   # Flame Worker
    "ec2-user@18.123.45.71"    # Submit Instance
)

# Path to your SSH key
$SSH_KEY = "~\.ssh\cis5550-key.pem"
```

## Step 2: Deploy Files to All Instances

```powershell
# From your local Windows machine (PowerShell)
.\deploy-to-ec2-simple.ps1
```

This copies all project files to all EC2 instances.

## Step 3: Run Services on Each Instance

SSH into each instance and run the appropriate script:

### Instance 1: KVS Coordinator
```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<KVS_COORD_IP>
cd Fa25-CIS5550-Project-ACAE
./deploy-kvs-coordinator.sh
```

### Instance 2: KVS Worker
```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<KVS_WORKER_IP>
cd Fa25-CIS5550-Project-ACAE
# Replace with your KVS Coordinator private IP
./deploy-kvs-worker.sh 8001 worker1 172.31.74.223:8000 s3://555-project-crawl
```

### Instance 3: Flame Coordinator
```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<FLAME_COORD_IP>
cd Fa25-CIS5550-Project-ACAE
# Replace with your KVS Coordinator private IP
./deploy-flame-coordinator.sh 9000 172.31.74.223:8000
```

### Instance 4: Flame Worker
```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<FLAME_WORKER_IP>
cd Fa25-CIS5550-Project-ACAE
# Replace with your Flame Coordinator private IP
./deploy-flame-worker.sh 9001 172.31.XX.XX:9000 1
```

### Instance 5: Submit Crawler Job
```bash
ssh -i ~/.ssh/your-key.pem ec2-user@<SUBMIT_IP>
cd Fa25-CIS5550-Project-ACAE
# Replace with your Flame Coordinator private IP
./deploy-submit-job.sh crawler "https://en.wikipedia.org/wiki/Main_Page" 172.31.XX.XX:9000
```

## Step 4: Monitor Logs

On each instance:
```bash
# View real-time logs
tail -f *.log

# Or specific log
tail -f kvs_coordinator_*.log
tail -f kvs_worker_*.log
tail -f flame_coordinator_*.log
tail -f flame_worker_*.log
```

## Step 5: Push Updates

When you make local changes:
```powershell
# From Windows PowerShell
.\deploy-to-ec2-simple.ps1
```

Then restart the affected services on their instances.

## Quick Reference

### Get Instance Private IPs
```bash
# On any EC2 instance
curl http://169.254.169.254/latest/meta-data/local-ipv4
```

### Check Running Processes
```bash
ps aux | grep java
lsof -i :8000  # Check specific port
```

### Kill Process
```bash
kill $(lsof -t -i:8000)  # Kill by port
pkill java  # Kill all Java processes
```

### S3 Sync (on KVS Worker)
```bash
# Manual sync
aws s3 sync worker1/pt-crawl s3://555-project-crawl/pt-crawl

# Check cron job
crontab -l
```

## Troubleshooting

### Connection Timeout
- Check security groups allow traffic on required ports
- Verify instances are in same VPC
- Use private IPs for inter-instance communication

### Port Already in Use
```bash
kill $(lsof -t -i:<PORT>)
```

### SSH Key Permission Issues
```powershell
# Windows - No action needed usually
# Linux/Mac - chmod 400 ~/.ssh/your-key.pem
```

### Can't SSH After Disconnect
Services run with `nohup` so they'll keep running. Just SSH back in and check logs.
