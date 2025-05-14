#!/bin/bash

set -e  # Exit immediately on command failure

# -----------------------------------------------------------------------------
# Script to trigger multiple jobs using a fixed remote script path.
# All jobs are executed in parallel and monitored for failures.
# -----------------------------------------------------------------------------

# Configuration
LOGFILE="$(pwd)/log/error.log"
SCRIPT_PATH="path of the file”
PROCESSING_DATE="$(date '+%Y%m%d')"

# Ensure log directory exists
mkdir -p "$(dirname "$LOGFILE")"

# Validate script path exists
if [[ ! -f "$SCRIPT_PATH" ]]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Error: Script path not found: $SCRIPT_PATH" | tee -a "$LOGFILE"
    exit 1
fi

# List of job names to process
job_names=("1" "2" "3" "4" "5")

# -----------------------------------------------------------------------------
# Trigger jobs in parallel and collect PIDs
# -----------------------------------------------------------------------------
declare -a pids
for job in "${job_names[@]}"; do
    "$SCRIPT_PATH" -processingDate "$PROCESSING_DATE" -o "$job" &
    pids+=($!)
done

# -----------------------------------------------------------------------------
# Monitor running jobs, handle failures, and log errors
# -----------------------------------------------------------------------------
for pid in "${pids[@]}"; do
    if ! wait "$pid"; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - Failed: Job with PID $pid exited with failure" | tee -a "$LOGFILE"
        exit 1
    fi
done

echo "$(date '+%Y-%m-%d %H:%M:%S') - All jobs completed successfully."
