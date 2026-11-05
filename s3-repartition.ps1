param(
    [string]$Bucket = "555-project-crawl",
    [string]$Prefix = "pt-crawl/",
    [int]$Workers = 15,
    [int]$Threads = 32
)

Write-Host "=== SIMPLE S3 REPARTITION START ==="

# -------------------------------------------
# Function: Split array into chunks
# -------------------------------------------
function Split-List {
    param(
        [array]$items,
        [int]$size
    )

    for ($i = 0; $i -lt $items.Count; $i += $size) {
        $end = [math]::Min($i + $size - 1, $items.Count - 1)
        ,@($items[$i..$end])
    }
}

# -------------------------------------------
# STEP 1 LIST KEYS
# -------------------------------------------
Write-Host "Listing S3 objects..."

$raw = aws s3api list-objects-v2 `
    --bucket $Bucket `
    --prefix $Prefix `
    --query "Contents[].Key" `
    --output json

if (-not $raw -or $raw -eq "null") {
    Write-Host "No objects found."
    exit
}

$keys = $raw | ConvertFrom-Json

if (-not $keys -or $keys.Count -eq 0) {
    Write-Host "No keys returned."
    exit
}

$keys = $keys | Sort-Object
$total = $keys.Count

Write-Host "Found $total files."

# -------------------------------------------
# STEP 2 ASSIGN KEYS TO WORKERS
# -------------------------------------------
$perWorker = [math]::Ceiling($total / $Workers)
$mapping = @{}

for ($i = 0; $i -lt $total; $i++) {
    $worker = [math]::Floor($i / $perWorker) + 1
    if ($worker -gt $Workers) { $worker = $Workers }
    $mapping[$keys[$i]] = $worker
}

# -------------------------------------------
# STEP 3 SPLIT WORK INTO JOBS
# -------------------------------------------
$chunks = Split-List $keys $Threads

if ($chunks.Count -eq 0) {
    Write-Host "NO CHUNKS CREATED EXITING"
    exit
}

# -------------------------------------------
# STEP 4  COPY CHUNKS IN PARALLEL
# -------------------------------------------
Write-Host "Starting copy jobs..."

$scriptBlock = {
    param($chunk, $Bucket, $Prefix, $mapping)

    $local = 0
    foreach ($key in $chunk) {

        $worker = $mapping[$key]
        $newKey = $key -replace [regex]::Escape($Prefix), "worker$worker/pt-crawl/"

        aws s3api copy-object `
            --bucket $Bucket `
            --copy-source "$Bucket/$key" `
            --key "$newKey" `
            --no-guess-mime-type | Out-Null

        $local++
    }

    return $local
}

$jobs = @()
foreach ($chunk in $chunks) {
    $jobs += Start-Job -ScriptBlock $scriptBlock -ArgumentList $chunk, $Bucket, $Prefix, $mapping
}

if ($jobs.Count -eq 0) {
    Write-Host "No jobs created."
    exit
}

# -------------------------------------------
# STEP 5 PROGRESS BAR
# -------------------------------------------
$done = 0

while ($true) {
    $finished = $jobs | Where-Object { $_.State -eq 'Completed' }

    $done = 0
    foreach ($job in $finished) {
        $done += Receive-Job $job
    }

    $pct = [math]::Round(($done / $total) * 100, 2)

    Write-Progress -Activity "Copying S3 Files" `
                   -Status "$done / $total copied" `
                   -PercentComplete $pct

    if ($finished.Count -eq $jobs.Count) { break }

    Start-Sleep 1
}

Write-Progress -Activity "Copying S3 Files" -Completed

Write-Host "Copy complete."

# Clean up
$jobs | Remove-Job -Force

Write-Host "=== DONE ==="
