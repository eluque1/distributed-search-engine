# EC2 Deployment Guide for CIS5550 Crawler

## Instance Architecture

### 1. KVS Coordinator (t3.small or t3.medium)
- **Purpose**: Coordinate KVS workers
- **Script**: `deploy-kvs-coordinator.sh`
- **Command**: `./deploy-kvs-coordinator.sh`

### 2. KVS Worker (i3.xlarge or i3.2xlarge - Storage Optimized)
- **Purpose**: Store crawled data with high IOPS
- **Script**: `deploy-kvs-worker.sh`
- **Command**: `./deploy-kvs-worker.sh <port> <worker_id> <kvs_coord_addr> <s3_bucket>`
- **Example**: `./deploy-kvs-worker.sh 8001 worker1 10.0.1.10:8000 s3://555-project-crawl`

### 3. Flame Coordinator (t3.small or t3.medium)
- **Purpose**: Coordinate Flame workers
- **Script**: `deploy-flame-coordinator.sh`
- **Command**: `./deploy-flame-coordinator.sh <port> <kvs_coord_addr>`
- **Example**: `./deploy-flame-coordinator.sh 9000 10.0.1.10:8000`

### 4. Flame Worker (c6i.2xlarge or c6i.4xlarge - Compute Optimized)
- **Purpose**: Perform CPU-intensive crawling/processing
- **Script**: `deploy-flame-worker.sh`
- **Command**: `./deploy-flame-worker.sh <port> <flame_coord_addr> <worker_num>`
- **Example**: `./deploy-flame-worker.sh 9001 10.0.1.20:9000 1`

### 5. Job Submit (t3.micro or run from any instance)
- **Purpose**: Submit crawl jobs
- **Script**: `deploy-submit-job.sh`
- **Command**: `./deploy-submit-job.sh <job> <job_args> <flame_coord_addr>`
- **Example**: `./deploy-submit-job.sh crawler "https://example.com" 10.0.1.20:9000`

## Deployment Steps

### Prerequisites
1. Create VPC with private subnet for all instances
2. Configure security groups:
   - KVS Coordinator: Allow inbound 8000 from VPC
   - KVS Worker: Allow inbound 8001+ from VPC
   - Flame Coordinator: Allow inbound 9000 from VPC
   - Flame Worker: Allow inbound 9001+ from VPC
   - All: Allow SSH (22) from your IP
3. Create IAM role with S3 write permissions for KVS Workers
4. Upload project files to all instances

### Launch Order
1. **KVS Coordinator** (note private IP)
2. **KVS Workers** (use KVS Coordinator IP)
3. **Flame Coordinator** (use KVS Coordinator IP)
4. **Flame Workers** (use Flame Coordinator IP)
5. **Submit Job** (use Flame Coordinator IP)

### Example Full Deployment

Assuming the following private IPs:
- KVS Coordinator: `10.0.1.10`
- Flame Coordinator: `10.0.1.20`

```bash
# On KVS Coordinator instance (10.0.1.10)
./deploy-kvs-coordinator.sh

# On KVS Worker instance(s)
./deploy-kvs-worker.sh 8001 worker1 10.0.1.10:8000 s3://555-project-crawl

# On Flame Coordinator instance (10.0.1.20)
./deploy-flame-coordinator.sh 9000 10.0.1.10:8000

# On Flame Worker instance(s)
./deploy-flame-worker.sh 9001 10.0.1.20:9000 1
./deploy-flame-worker.sh 9002 10.0.1.20:9000 2  # For multiple workers on same instance

# On Submit instance (or any instance)
./deploy-submit-job.sh crawler "https://en.wikipedia.org/wiki/Main_Page" 10.0.1.20:9000
```

## S3 Data Sync

The KVS Worker script automatically sets up S3 sync:
- Syncs every 30 minutes via cron
- Final sync on graceful shutdown
- Manual sync: `aws s3 sync worker1/pt-crawl s3://555-project-crawl/pt-crawl`

## Monitoring

Check logs on each instance:
```bash
tail -f kvs_coordinator_*.log
tail -f kvs_worker_worker1_*.log
tail -f flame_coordinator_*.log
tail -f flame_worker_*_*.log
tail -f job_submit_crawler_*.log
```

## Scaling

### Add more KVS Workers:
```bash
./deploy-kvs-worker.sh 8002 worker2 10.0.1.10:8000 s3://555-project-crawl
./deploy-kvs-worker.sh 8003 worker3 10.0.1.10:8000 s3://555-project-crawl
```

### Add more Flame Workers:
```bash
# On additional c6i instances
./deploy-flame-worker.sh 9003 10.0.1.20:9000 3
./deploy-flame-worker.sh 9004 10.0.1.20:9000 4
```

## Cost Optimization

- **Spot Instances**: Use for Flame Workers (fault-tolerant compute)
- **On-Demand**: Use for KVS components (data persistence critical)
- **Reserved**: Consider for long-running coordinators
- **i3en**: If you need more storage than i3 provides

## Troubleshooting

1. **Connection refused**: Check security groups allow traffic between instances
2. **Out of memory**: Adjust `-Xmx` values in scripts
3. **Slow crawling**: Add more Flame Workers
4. **Storage full**: Add more KVS Workers or use larger i3 instances
5. **S3 sync fails**: Verify IAM role attached to instance
