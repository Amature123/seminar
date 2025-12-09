#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

run_script_with_restart() {
    local SCRIPT="$1"
    local NAME=$(basename "$SCRIPT" .py)

    while true; do
        echo -e "${GREEN}[${NAME}] Starting...${NC}"
        python3 "$SCRIPT"
        local EXIT_CODE=$?

        if [ $EXIT_CODE -eq 0 ]; then
            echo -e "${GREEN}[${NAME}] Finished normally.${NC}"
            break
        fi

        echo -e "${RED}[${NAME}] crashed with $EXIT_CODE. Restart in 5s...${NC}"
        sleep 5
    done
}

# Wait for Kafka
echo "⏳ Waiting for Kafka..."
until python3 - <<'EOF'
from kafka import KafkaAdminClient
try:
    KafkaAdminClient(bootstrap_servers="kafka_broker:19092").close()
except:
    exit(1)
EOF
do
    echo "   Kafka not ready..."
    sleep 2
done

echo -e "${GREEN}✓ Kafka ready!${NC}"

# Start producers
run_script_with_restart /opt/flink/script/producer/production.py &
PID_PROD=$!

run_script_with_restart /opt/flink/script/producer/newsProduction.py &
PID_NEWS=$!

trap "echo 'Stopping...'; kill $PID_PROD $PID_NEWS" SIGTERM SIGINT

wait
