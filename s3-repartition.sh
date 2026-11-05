#!/bin/bash

BUCKET="555-project-crawl"
SOURCE_PREFIX="pt-crawl/"
WORKERS=4
PARALLEL_JOBS=16

LOG="repartition.log"
ERR="s3_errors.log"

echo "---- Starting S3 Repartition ----" | tee "$LOG"
date | tee -a "$LOG"
echo "" | tee -a "$LOG"

echo "Listing S3 keys from s3://$BUCKET/$SOURCE_PREFIX ..." | tee -a "$LOG"
aws s3 ls "s3://$BUCKET/$SOURCE_PREFIX" --recursive \
    | awk '{print $4}' > all_keys.txt

TOTAL=$(wc -l < all_keys.txt)
echo "Found $TOTAL objects." | tee -a "$LOG"
echo "" | tee -a "$LOG"

echo "Shuffling keys ..." | tee -a "$LOG"
shuf all_keys.txt > shuffled_keys.txt

echo "Splitting into $PARALLEL_JOBS chunks ..." | tee -a "$LOG"
split -n l/$PARALLEL_JOBS shuffled_keys.txt chunk_

echo "Starting $PARALLEL_JOBS parallel jobs..." | tee -a "$LOG"
echo "" | tee -a "$LOG"


###########################################
# START PARALLEL JOBS
###########################################

job_id=0
for chunk in chunk_*; do
(
    JOBTAG=$(basename "$chunk")
    COUNT=$(wc -l < "$chunk")
    PROCESSED=0

    echo "$(date +"%H:%M:%S") [$JOBTAG] Starting job for $COUNT files." >> "$LOG"

    while IFS= read -r key; do
        worker_id=$(( (PROCESSED % WORKERS) + 1 ))

        dest_key="${key/$SOURCE_PREFIX/worker$worker_id\/pt-crawl/}"

        src="s3://$BUCKET/$key"
        dst="s3://$BUCKET/$dest_key"

        if ! aws s3 mv "$src" "$dst" >/dev/null 2>>"$ERR"; then
            echo "$(date +"%H:%M:%S") [$JOBTAG] Error moving $key" >> "$ERR"
        fi

        PROCESSED=$((PROCESSED + 1))

        # Log update every 200 files
        if (( PROCESSED % 200 == 0 )); then
            echo "$(date +"%H:%M:%S") [$JOBTAG] $PROCESSED / $COUNT files moved." >> "$LOG"
        fi

    done < "$chunk"

    echo "$(date +"%H:%M:%S") [$JOBTAG] Finished." >> "$LOG"

) &
job_id=$((job_id + 1))
done


###########################################
# PROGRESS MONITOR
###########################################

echo "Monitoring progress..." | tee -a "$LOG"
echo "" | tee -a "$LOG"

while true; do
    RUNNING=$(jobs | grep -c "Running")

    if [[ "$RUNNING" -eq 0 ]]; then
        break
    fi

    echo "$(date +"%H:%M:%S") STILL RUNNING: $RUNNING jobs active..." | tee -a "$LOG"
    sleep 5
done


###########################################
# FINISH
###########################################

echo "" | tee -a "$LOG"
echo "All jobs finished!" | tee -a "$LOG"
date | tee -a "$LOG"
echo "Logs saved to: $LOG"
echo "Errors saved to: $ERR"
