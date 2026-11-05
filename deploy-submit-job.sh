#!/bin/bash

############################################
# Job Submit Script
# Instance Type: t3.micro (or run from any instance)
############################################

# ----- CONFIG -----
JOB="${1:-crawler}"                    # Job name: crawler, crawler_l, pagerank, indexer
JOB_ARGS="${2:-}"                      # Job arguments (e.g., seed URL)
FLAME_COORD_ADDR="${3:-localhost:9000}" # Format: hostname:port or ip:port
LOGFILE="job_submit_${JOB}_$(date +%Y%m%d_%H%M%S).log"

# ----- LOGGING -----
log() {
    echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOGFILE"
}

# ----- STARTUP -----
log "======================================"
log "Submitting Flame Job"
log "Job: $JOB"
log "Args: $JOB_ARGS"
log "Flame Coordinator: $FLAME_COORD_ADDR"
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

# Build classpath
CLASSPATH="bin"
for JAR in webserver.jar kvs.jar flame.jar; do
    if [ -f "lib/$JAR" ]; then
        CLASSPATH="$CLASSPATH:lib/$JAR"
    elif [ -f "$JAR" ]; then
        CLASSPATH="$CLASSPATH:$JAR"
    fi
done

# ----- DETERMINE JOB CLASS -----
case "$JOB" in
    crawler)   JOB_CLASS="Crawler" ;;
    crawler_l) JOB_CLASS="Crawler_l" ;;
    crawler_fast) JOB_CLASS="CrawlerFast" ;;
    pagerank)  JOB_CLASS="PageRank" ;;
    indexer)   JOB_CLASS="Indexer" ;;   
    *) log "ERROR: Invalid job: $JOB (valid: crawler, crawler_l, crawler_fast, pagerank, indexer)"; exit 1 ;;
esac

# ----- CREATE JOB JAR -----
JARFILE="${JOB}.jar"
log "Creating job JAR: $JARFILE"
rm -f "$JARFILE"
jar cf "$JARFILE" -C bin cis5550/jobs 2>&1 | tee -a "$LOGFILE"

if [ ! -f "$JARFILE" ]; then
    log "ERROR: Failed to create JAR file"
    exit 1
fi

log "JAR created successfully."

# ----- SUBMIT JOB -----
log "Submitting job: $JOB_CLASS with args: $JOB_ARGS"
java -Xmx4g -cp "$CLASSPATH" cis5550.flame.FlameSubmit \
    "$FLAME_COORD_ADDR" \
    "$JARFILE" \
    "cis5550.jobs.$JOB_CLASS" \
    $JOB_ARGS 2>&1 | tee -a "$LOGFILE"

log "Job submission complete."
