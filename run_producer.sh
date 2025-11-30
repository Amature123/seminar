#!/bin/bash
set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "🚀 Starting Stock Trading System"
echo "=========================================="

# ============================================================================
# Function: Run script with auto-restart (for producers)
# ============================================================================
run_script_with_restart() {
    SCRIPT="$1"
    SCRIPT_NAME=$(basename "$SCRIPT" .py)
    
    while true; do
        echo -e "${GREEN}[${SCRIPT_NAME}]${NC} Starting..."
        python3 "$SCRIPT"
        EXIT_CODE=$?
        
        if [ $EXIT_CODE -eq 0 ]; then
            echo -e "${GREEN}[${SCRIPT_NAME}]${NC} Finished successfully."
            break
        fi

        echo -e "${RED}[${SCRIPT_NAME}]${NC} Exited with code $EXIT_CODE. Restarting in 5 seconds..."
        sleep 5
    done
}

# ============================================================================
# Wait for dependencies
# ============================================================================
echo "⏳ Waiting for Kafka to be ready..."
until python3 -c "
from kafka import KafkaAdminClient
try:
    admin = KafkaAdminClient(bootstrap_servers='kafka_broker:19092', request_timeout_ms=5000)
    admin.close()
    print('Kafka is ready!')
except:
    exit(1)
" 2>/dev/null; do
    echo "   Kafka not ready yet, waiting..."
    sleep 2
done



echo -e "${GREEN}✓${NC} All dependencies ready!"
echo ""


# ============================================================================
# Start Producer Scripts (with auto-restart)
# ============================================================================
echo "=========================================="
echo "📊 Starting Data Producers"
echo "=========================================="

if [ -f script/production.py ]; then
    run_script_with_restart ./script/production.py &
    PID_PROD=$!
    echo "   • Stock producer started (PID: $PID_PROD)"
fi

if [ -f script/newsProduction.py ]; then
    run_script_with_restart ./script/newsProduction.py &
    PID_NEWS=$!
    echo "   • News producer started (PID: $PID_NEWS)"
fi

# Wait a bit for producers to start
sleep 3

# ============================================================================
# Monitor running processes
# ============================================================================
echo ""
echo "=========================================="
echo "📡 System Running"
echo "=========================================="
echo "✓ Stock producer: Running (PID: $PID_PROD)"
echo "✓ News producer: Running (PID: $PID_NEWS)"
# Keep script running to maintain background processes
wait $PID_PROD
wait $PID_NEWS