# #!/bin/bash
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


run_script_with_restart /app/processing/OHVLC_handler.py &
PID_OHVLC=$!

run_script_with_restart /app/processing/trading_handler.py &
PID_TRADING=$!

run_script_with_restart /app/processing/news_handler.py &
PID_NEWS=$!

run_script_with_restart /app/processing/model_handler.py &
PID_MODEL=$!

run_script_with_restart /app/processing/indicator_handler.py &
PID_IND=$!

run_script_with_restart /app/processing/signal_handler.py &
PID_SIG=$!

run_script_with_restart /app/processing/news_impact_handler.py &
PID_IMPACT=$!

trap "echo 'Stopping...'; kill $PID_OHVLC $PID_TRADING $PID_NEWS $PID_MODEL $PID_IND $PID_SIG $PID_IMPACT" SIGTERM SIGINT

wait