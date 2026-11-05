#!/bin/bash
# Monitor crawler progress and speed in real-time

KVS_COORDINATOR="${1:-localhost:8000}"
AUTO_STOP="${2:-}"

echo "=== Crawler Monitor Setup ==="

# Compile the monitor if needed
echo -e "\nCompiling Java files..."
mkdir -p bin

CLASSPATH="bin"
for JAR in lib/*.jar; do
    if [ -f "$JAR" ]; then
        CLASSPATH="$CLASSPATH:$JAR"
    fi
done

javac -cp "$CLASSPATH" -d bin $(find src -name "*.java")
if [ $? -ne 0 ]; then
    echo "Compilation failed!"
    exit 1
fi

echo "Compilation successful!"

# Build arguments
MONITOR_ARGS="$KVS_COORDINATOR"
if [ "$AUTO_STOP" = "--auto-stop" ]; then
    MONITOR_ARGS="$MONITOR_ARGS --auto-stop"
fi

# Run the monitor
echo -e "\nStarting monitor for KVS at $KVS_COORDINATOR..."
echo "Press Ctrl+C to stop monitoring"
echo ""

java -cp "bin:lib/*" cis5550.tools.CrawlerMonitor $MONITOR_ARGS
