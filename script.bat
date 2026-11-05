# =========================================================
#   PARALLEL COPY USING Start-Job (compatible everywhere)
# =========================================================

$counterFile = "$env:TEMP\s3_repartition_counter.txt"
Set-Content $counterFile "0"

$scriptBlock = {
    param($chunk, $Bucket, $Prefix, $mapping, $counterFile)

    foreach ($key in $chunk) {

        $worker = $mapping[$key]
        $newKey = $key -replace [regex]::Escape($Prefix), "worker$worker/pt-crawl/"

        aws s3api copy-object `
            --bucket $Bucket `
            --copy-source "$Bucket/$key" `
            --key "$newKey" `
            --no-guess-mime-type | Out-Null

        # Safe increment: update counter file
        $count = [int](Get-Content $counterFile)
        Set-Content $counterFile ($count + 1)
    }
}

# Start jobs
$jobs = @()
foreach ($chunk in $chunks) {
    $jobs += Start-Job -ScriptBlock $scriptBlock `
        -ArgumentList $chunk, $Bucket, $Prefix, $mapping, $counterFile
}

Write-Host "Copying $total files in parallel..."

# Progress loop
while ($jobs | Where-Object { $_.State -eq "Running" }) {
    $done = [int](Get-Content $counterFile)
    $pct = [math]::Round(($done / $total) * 100, 2)

    Write-Progress -Activity "Copying S3 Files" `
                   -Status "$done / $total copied" `
                   -PercentComplete $pct

    Start-Sleep 1
}

Wait-Job $jobs
Remove-Item $counterFile -ErrorAction Ignore

Write-Host "Copy stage complete."
