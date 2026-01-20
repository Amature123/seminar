#!/bin/bash
set -e

# Start batch jobs
echo "🚀 Starting batch jobs..."
python3 /app/static_insert/OHVLC_screener.py &
PID_BATCH=$!
wait