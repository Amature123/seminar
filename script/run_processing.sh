#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

submit_flink_job() {
    local SCRIPT="$1"
    local MAX_RETRIES=3
    local RETRY_COUNT=0

    echo -e "${YELLOW}[Flink] Submitting job...${NC}"

    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        echo "⏳ Waiting for JobManager..."
        until curl -sf http://jobmanager:8081/overview > /dev/null; do
            echo "   JobManager not ready, waiting..."
            sleep 2
        done

        /opt/flink/bin/flink run \
            -m jobmanager:8081 \
            -py "$SCRIPT" \
            -pyexec /usr/bin/python3.11 \
            -D execution.checkpointing.unaligned=true \
            -D state.checkpoints.dir="file:///tmp/checkpoints"
        EXIT_CODE=$?
        if [ $EXIT_CODE -eq 0 ]; then
            echo -e "${GREEN}[Flink] Job submitted successfully!${NC}"
            return 0
        fi

        RETRY_COUNT=$((RETRY_COUNT + 1))
        echo -e "${RED}[Error] submit failed ($RETRY_COUNT/$MAX_RETRIES) retrying...${NC}"
        sleep 5
    done

    echo -e "${RED}[Flink] FAILED after $MAX_RETRIES attempts${NC}"
    return 1
}


echo "⚡ Starting Flink submit process..."
    
if [ -f /opt/flink/script/flink_handler.py ]; then
    submit_flink_job /opt/flink/script/flink_handler.py
else
    echo -e "${YELLOW}⚠️ flink_handler.py NOT FOUND${NC}"
fi
