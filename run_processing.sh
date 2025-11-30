#!/bin/bash
set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
submit_flink_job() {
    SCRIPT="$1"
    MAX_RETRIES=3
    RETRY_COUNT=0
    
    echo -e "${YELLOW}[Flink]${NC} Submitting Flink job to cluster..."
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        echo "⏳ Waiting for Flink JobManager to be ready..."
            until curl -sf http://jobmanager:8081/overview > /dev/null; do
                echo "   Flink not ready yet, waiting..."
                sleep 2
            done
        /opt/flink/bin/flink run \
                            -m jobmanager:8081 \
                            -py "$SCRIPT" \
                            -pyExecutable /usr/bin/python3.11
        EXIT_CODE=$?
        
        if [ $EXIT_CODE -eq 0 ]; then
            echo -e "${GREEN}[Flink]${NC} Job submitted successfully!"
            
            # Verify job is running
            sleep 5
            echo -e "${YELLOW}[Flink]${NC} Verifying job status..."
            
            # Check Flink REST API
            JOB_STATUS=$(curl -s http://jobmanager:8081/jobs || echo "ERROR")
            
            if [[ "$JOB_STATUS" == *"\"status\":\"RUNNING\""* ]]; then
                echo -e "${GREEN}[Flink]${NC} Job is running on cluster ✓"
                return 0
            else
                echo -e "${YELLOW}[Flink]${NC} Job status: $JOB_STATUS"
            fi
            
            return 0
        fi
        
        RETRY_COUNT=$((RETRY_COUNT + 1))
        echo -e "${RED}[Flink]${NC} Submission failed (attempt $RETRY_COUNT/$MAX_RETRIES). Retrying in 10 seconds..."
        sleep 10
    done
    
    echo -e "${RED}[Flink]${NC} Failed to submit job after $MAX_RETRIES attempts"
    return 1
}


echo "=========================================="
echo "⚡ Submitting Flink Stream Processing Job"
echo "=========================================="

if [ -f script/flink_handler.py ]; then
    submit_flink_job ./script/flink_handler.py
    FLINK_EXIT=$?
    
    if [ $FLINK_EXIT -ne 0 ]; then
        echo -e "${RED}[ERROR]${NC} Flink job submission failed!"
        echo "   Producers will continue running, but stream processing is unavailable."
        echo "   Check logs: docker-compose logs taskmanager"
    fi
else
    echo -e "${YELLOW}[WARNING]${NC} Flink handler script not found, skipping..."
fi
