#!/usr/bin/env bash
#
# run-job.sh
#
# Compile Java, start KVS + Flame, build a job JAR, and submit the job.
#
# Usage:
#   ./run-job.sh [crawler|pagerank|indexer|indexermerge|graphbuilder] [job-args...]
#
# If no job name is given, defaults to "indexer".
#

############################################
# Config / defaults
############################################

JOB="${1:-indexer}"
shift || true   # remaining args are job args
JOB_ARGS=("$@")

KVS_COORD_PORT=8000
KVS_WORKER_COUNT=2
KVS_WORKER_BASE_PORT=8001

FLAME_COORD_PORT=9000
FLAME_WORKER_COUNT=1
FLAME_WORKER_BASE_PORT=9001

LOG_DIR="logs"
mkdir -p "$LOG_DIR"
TS="$(date +'%Y-%m-%d_%H-%M-%S')"
LOG_FILE="$LOG_DIR/${JOB}_${TS}.log"

############################################
# Colors / logging helpers
############################################

COLOR_INFO="\033[36m"
COLOR_SUCCESS="\033[32m"
COLOR_ERROR="\033[31m"
COLOR_RESET="\033[0m"

info()    { echo -e "${COLOR_INFO}$*${COLOR_RESET}"; }
success() { echo -e "${COLOR_SUCCESS}$*${COLOR_RESET}"; }
error()   { echo -e "${COLOR_ERROR}$*${COLOR_RESET}" >&2; }

log() {
  local msg="[$(date +'%Y-%m-%d %H:%M:%S')] $*"
  echo "$msg" | tee -a "$LOG_FILE"
}

############################################
# Process tracking for cleanup
############################################

KVS_COORD_PID=""
KVS_WORKER_PIDS=()
FLAME_COORD_PID=""
FLAME_WORKER_PIDS=()

cleanup() {
  info "\n=== Cleaning up processes ==="
  log "=== Cleaning up processes ==="

  for pid in "${FLAME_WORKER_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      log "Stopped Flame Worker PID: $pid"
    fi
  done

  if [[ -n "$FLAME_COORD_PID" ]] && kill -0 "$FLAME_COORD_PID" 2>/dev/null; then
    kill "$FLAME_COORD_PID" 2>/dev/null || true
    log "Stopped Flame Coordinator PID: $FLAME_COORD_PID"
  fi

  for pid in "${KVS_WORKER_PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      log "Stopped KVS Worker PID: $pid"
    fi
  done

  if [[ -n "$KVS_COORD_PID" ]] && kill -0 "$KVS_COORD_PID" 2>/dev/null; then
    kill "$KVS_COORD_PID" 2>/dev/null || true
    log "Stopped KVS Coordinator PID: $KVS_COORD_PID"
  fi

  success "Cleanup complete"
  log "Cleanup complete"
}
trap cleanup EXIT

############################################
# Compile all Java
############################################

compile_all() {
  info "\n=== Compiling all Java files ==="
  log "=== Compiling all Java files ==="

  mkdir -p bin

  # Build classpath: bin + any jars we find
  CLASSPATH="bin"
  for LIB in "lib" "HW6/lib" "."; do
    [[ -f "$LIB/webserver.jar" ]] && CLASSPATH="$CLASSPATH:$LIB/webserver.jar"
    [[ -f "$LIB/kvs.jar"       ]] && CLASSPATH="$CLASSPATH:$LIB/kvs.jar"
    [[ -f "$LIB/flame.jar"     ]] && CLASSPATH="$CLASSPATH:$LIB/flame.jar"
    [[ -f "$LIB/opennlp-tools-2.3.2.jar" ]] && CLASSPATH="$CLASSPATH:$LIB/opennlp-tools-2.3.2.jar"
  done

  info "Using classpath: $CLASSPATH"
  log  "Using classpath: $CLASSPATH"

# Find all Java sources (portable version, no mapfile)
  JAVA_FILES=()
  while IFS= read -r file; do
    JAVA_FILES+=("$file")
  done < <(find src -name '*.java')

  if [[ ${#JAVA_FILES[@]} -eq 0 ]]; then
    error "ERROR: No Java files found in src"
    log   "ERROR: No Java files found in src"
    exit 1
  fi

  info "Found ${#JAVA_FILES[@]} Java file(s)"
  log  "Found ${#JAVA_FILES[@]} Java file(s)"

  javac -cp "$CLASSPATH" -d bin "${JAVA_FILES[@]}" 2>&1 | tee -a "$LOG_FILE"

  if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
    error "ERROR: Compilation failed"
    log   "ERROR: Compilation failed"
    exit 1
  fi

  success "Compilation successful"
  log     "Compilation successful"
}

############################################
# Start KVS / Flame
############################################

start_kvs_coord() {
  info "\n=== Starting KVS Coordinator on port $KVS_COORD_PORT ==="
  log  "=== Starting KVS Coordinator on port $KVS_COORD_PORT ==="

  java -Xmx1g -cp bin cis5550.kvs.Coordinator "$KVS_COORD_PORT" \
    >>"$LOG_FILE" 2>&1 &
  KVS_COORD_PID=$!
  sleep 2
  success "KVS Coordinator started (PID: $KVS_COORD_PID)"
  log     "KVS Coordinator started (PID: $KVS_COORD_PID)"
}

start_kvs_workers() {
  info "\n=== Starting $KVS_WORKER_COUNT KVS Worker(s) ==="
  log  "=== Starting $KVS_WORKER_COUNT KVS Worker(s) ==="

  for ((i=1; i<=KVS_WORKER_COUNT; i++)); do
    port=$((KVS_WORKER_BASE_PORT + i - 1))
    dir="worker$i"
    mkdir -p "$dir"
    info "Starting KVS Worker $i on port $port (dir: $dir)"
    log  "Starting KVS Worker $i on port $port (dir: $dir)"

    java -cp bin cis5550.kvs.Worker "$port" "$dir" "localhost:${KVS_COORD_PORT}" \
      >>"$LOG_FILE" 2>&1 &
    pid=$!
    KVS_WORKER_PIDS+=("$pid")
    log "KVS Worker $i started (PID: $pid)"
    sleep 1
  done

  success "All KVS Workers started"
  log     "All KVS Workers started"
}

start_flame_coord() {
  info "\n=== Starting Flame Coordinator on port $FLAME_COORD_PORT ==="
  log  "=== Starting Flame Coordinator on port $FLAME_COORD_PORT ==="

  java -cp bin cis5550.flame.Coordinator "$FLAME_COORD_PORT" "localhost:${KVS_COORD_PORT}" \
    >>"$LOG_FILE" 2>&1 &
  FLAME_COORD_PID=$!
  sleep 2
  success "Flame Coordinator started (PID: $FLAME_COORD_PID)"
  log     "Flame Coordinator started (PID: $FLAME_COORD_PID)"
}

start_flame_workers() {
  info "\n=== Starting $FLAME_WORKER_COUNT Flame Worker(s) ==="
  log  "=== Starting $FLAME_WORKER_COUNT Flame Worker(s) ==="

  for ((i=1; i<=FLAME_WORKER_COUNT; i++)); do
    port=$((FLAME_WORKER_BASE_PORT + i - 1))
    info "Starting Flame Worker $i on port $port"
    log  "Starting Flame Worker $i on port $port"

    java -cp bin cis5550.flame.Worker "$port" "localhost:${FLAME_COORD_PORT}" \
      >>"$LOG_FILE" 2>&1 &
    pid=$!
    FLAME_WORKER_PIDS+=("$pid")
    log "Flame Worker $i started (PID: $pid)"
    sleep 1
  done

  success "All Flame Workers started"
  log     "All Flame Workers started"
}

############################################
# KVS table delete
############################################

delete_table() {
  local table="$1"
  info "\n=== Deleting table: $table ==="
  log  "=== Deleting table: $table ==="

  java -cp bin cis5550.kvs.KVSClient "localhost:${KVS_COORD_PORT}" delete "$table" \
    >>"$LOG_FILE" 2>&1

  if [[ $? -eq 0 ]]; then
    success "Successfully deleted table: $table"
    log     "Successfully deleted table: $table"
  else
    info "Table $table may not exist (this is OK)"
    log  "Table $table may not exist (this is OK)"
  fi
}

############################################
# Create job JAR
############################################

create_job_jar() {
  local jobname="$1"
  local classname="$2"
  local jarname="${jobname}.jar"

  info "\n=== Creating JAR for $jobname ==="
  log  "=== Creating JAR for $jobname ==="

  rm -f "$jarname"

  local classfile="bin/cis5550/jobs/${classname}.class"
  if [[ ! -f "$classfile" ]]; then
    error "ERROR: Class file not found: $classfile"
    log   "ERROR: Class file not found: $classfile"
    exit 1
  fi

  # Include all classes in jobs package
  jar cf "$jarname" -C bin "cis5550/jobs" >>"$LOG_FILE" 2>&1
  if [[ $? -ne 0 ]]; then
    error "ERROR: Failed to create JAR"
    log   "ERROR: Failed to create JAR"
    exit 1
  fi

  success "Created $jarname"
  log     "Created $jarname"
  echo "$jarname"
}

############################################
# Submit job
############################################

submit_job() {
  local jarname="$1"
  local classname="$2"
  shift 2
  local args=("$@")

  info "\n=== Submitting job: $JOB ==="
  log  "=== Submitting job: $JOB ==="
  log  "JAR: $jarname"
  log  "Class: $classname"
  log  "Args: ${args[*]}"

  echo -e "${COLOR_INFO}\n--- Job Output ---${COLOR_RESET}"
  log "--- Job Output Start ---"

  java -cp bin cis5550.flame.FlameSubmit \
    "localhost:${FLAME_COORD_PORT}" \
    "$jarname" \
    "$classname" \
    "${args[@]}" \
    2>&1 | tee -a "$LOG_FILE"

  if [[ ${PIPESTATUS[0]} -ne 0 ]]; then
    error "ERROR: Job submission failed"
    log   "ERROR: Job submission failed"
  else
    success "Job submitted successfully"
    log     "Job submitted successfully"
  fi

  log "--- Job Output End ---"
  echo -e "${COLOR_INFO}--- End Job Output ---${COLOR_RESET}"
}

############################################
# Main
############################################

echo -e "${COLOR_INFO}========================================${COLOR_RESET}"
echo -e "${COLOR_INFO}  Job Runner Script (bash)${COLOR_RESET}"
echo -e "${COLOR_INFO}========================================${COLOR_RESET}"
echo -e "${COLOR_INFO}Job: $JOB${COLOR_RESET}"
echo -e "${COLOR_INFO}KVS Workers: $KVS_WORKER_COUNT${COLOR_RESET}"
echo -e "${COLOR_INFO}Flame Workers: $FLAME_WORKER_COUNT${COLOR_RESET}"
echo -e "${COLOR_INFO}Log file: $LOG_FILE${COLOR_RESET}"
echo -e "${COLOR_INFO}========================================${COLOR_RESET}"

log "Job: $JOB"
log "KVS Workers: $KVS_WORKER_COUNT"
log "Flame Workers: $FLAME_WORKER_COUNT"
log "Job args: ${JOB_ARGS[*]}"

# Map job name → class name
case "$JOB" in
  crawler)       CLASS_NAME="Crawler" ;;
  pagerank)      CLASS_NAME="PageRank" ;;
  indexer)       CLASS_NAME="Indexer" ;;
  indexermerge)  CLASS_NAME="IndexerMerge" ;;
  graphbuilder)  CLASS_NAME="GraphBuilder" ;;
  *)
    error "Unknown job: $JOB"
    log   "Unknown job: $JOB"
    exit 1
    ;;
esac

# 1) Compile
compile_all

# 2) Start services
start_kvs_coord
start_kvs_workers
start_flame_coord
start_flame_workers

info "\nWaiting 5 seconds for services to initialize..."
sleep 5

# 3) Delete table(s) for certain jobs
if [[ "$JOB" == "indexer" ]]; then
  delete_table "pt-index"
elif [[ "$JOB" == "graphbuilder" ]]; then
  delete_table "pt-pr-graph"
fi

# 4) Build JAR
create_job_jar "$JOB" "$CLASS_NAME"
JAR_NAME="${JOB}.jar"

# 5) Submit job
submit_job "$JAR_NAME" "cis5550.jobs.$CLASS_NAME" "${JOB_ARGS[@]}"


success "\n=== Setup Complete ==="
log     "=== Setup Complete ==="
info "All services are running."
log  "All services are running."
info "Process IDs:"
log  "Process IDs:"
[[ -n "$KVS_COORD_PID" ]]   && { info "  KVS Coordinator: $KVS_COORD_PID";   log "  KVS Coordinator: $KVS_COORD_PID"; }
[[ -n "$FLAME_COORD_PID" ]] && { info "  Flame Coordinator: $FLAME_COORD_PID"; log "  Flame Coordinator: $FLAME_COORD_PID"; }
for pid in "${KVS_WORKER_PIDS[@]}"; do
  info "  KVS Worker PID: $pid"
  log  "  KVS Worker PID: $pid"
done
for pid in "${FLAME_WORKER_PIDS[@]}"; do
  info "  Flame Worker PID: $pid"
  log  "  Flame Worker PID: $pid"
done

# Script exits here; trap will run cleanup when you Ctrl+C or when job exits
#info "Press Enter to clean up..."
#read
#cleanup