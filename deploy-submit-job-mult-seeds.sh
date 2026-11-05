#!/bin/bash

############################################
# Multi-Seed Job Submit Script
#
# Usage:
#   ./deploy-submit-job-mult-seeds.sh JOB_NAME <seed1> <seed2> ... <seedN> FLAME_COORD
#
# Example:
#   ./deploy-submit-job-mult-seeds.sh crawler \
#       "https://en.wikipedia.org/wiki/Medical_specialty" \
#       "https://en.wikipedia.org/wiki/Sour_Patch_Kids" \
#       localhost:9000
############################################

# ----- CONFIG -----
JOB="${1:-crawler}"   # Job name: crawler, pagerank, indexer
shift                 # Remove JOB from "$@", now args = <seeds...> <coordinator>

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 JOB <seed1> <seed2> ... <seedN> FLAME_COORD"
    exit 1
fi

# Last arg = coordinator
FLAME_COORD_ADDR="${@: -1}"

# Everything before that = job args
if [ "$#" -gt 1 ]; then
    JOB_ARGS=( "${@:1:$(($#-1))}" )
else
    JOB_ARGS=()
fi

LOGFILE="job_submit_${JOB}_$(date +%Y%m%d_%H%M%S).log"

# ----- LOGGING -----
log() {
    echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOGFILE"
}

# ----- STARTUP -----
log "======================================"
log "Submitting Flame Job"
log "Job: $JOB"
log "Flame Coordinator: $FLAME_COORD_ADDR"
if [ "${#JOB_ARGS[@]}" -gt 0 ]; then
    log "Job Args (${#JOB_ARGS[@]}): ${JOB_ARGS[*]}"
else
    log "Job Args: (none)"
fi
log "Log file: $LOGFILE"
log "======================================"

# ----- COMPILE JAVA (if needed) -----
if [ ! -d "bin" ] || [ -z "$(ls -A bin 2>/dev/null)" ]; then
    log "Compiling Java files..."
    mkdir -p bin

    CLASSPATH="bin"
    for JAR in webserver.jar kvs.jar flame.jar; do
        if [ -f "lib/$JAR" ]; then
            CLASSPATH="$CLASSPATH:lib/$JAR"
        elif [ -f "$JAR" ]; then
            CLASSPATH="$CLASSPATH:$JAR"
        fi
    done

    javac -cp "$CLASSPATH" -d bin $(find src -name "*.java") 2>&1 | tee -a "$LOGFILE"
    if [ "${PIPESTATUS[0]}" -ne 0 ]; then
        log "ERROR: Compilation failed."
        exit 1
    fi

    log "Compilation successful."
fi

# Build classpath again
CLASSPATH="bin"
for JAR in webserver.jar kvs.jar flame.jar; do
    if [ -f "lib/$JAR" ]; then
        CLASSPATH="$CLASSPATH:lib/$JAR"
    elif [ -f "$JAR" ]; then
        CLASSPATH="$CLASSPATH:$JAR"
    fi
done

# ----- JOB CLASS -----
case "$JOB" in
    crawler)   JOB_CLASS="Crawler" ;;
    pagerank)  JOB_CLASS="PageRank" ;;
    indexer)   JOB_CLASS="Indexer" ;;
    *)
        log "ERROR: Invalid job: $JOB (valid: crawler, pagerank, indexer)"
        exit 1
        ;;
esac

# ----- CREATE JOB JAR -----
JARFILE="${JOB}.jar"
log "Creating job JAR: $JARFILE"

rm -f "$JARFILE"
jar cf "$JARFILE" -C bin cis5550/jobs 2>&1 | tee -a "$LOGFILE"

if [ ! -f "$JARFILE" ]; then
    log "ERROR: Failed to create JAR"
    exit 1
fi

log "JAR created successfully."

# ----- SUBMIT JOB -----
log "Submitting $JOB_CLASS with ${#JOB_ARGS[@]} args..."
java -Xmx4g -cp "$CLASSPATH" cis5550.flame.FlameSubmit \
    "$FLAME_COORD_ADDR" \
    "$JARFILE" \
    "cis5550.jobs.$JOB_CLASS" \
    "${JOB_ARGS[@]}" 2>&1 | tee -a "$LOGFILE"

log "Job submission complete."
