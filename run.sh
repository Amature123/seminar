#!/bin/bash

run_script() {
    SCRIPT="$1"

    while true; do
        echo "Starting $SCRIPT..."
        python3 "$SCRIPT"
        EXIT_CODE=$?
        # If success, exit loop
        if [ $EXIT_CODE -eq 0 ]; then
            echo "$SCRIPT finished successfully."
            break
        fi

        # If fails → restart after delay
        echo "$SCRIPT exited with code $EXIT_CODE. Restarting in 2 seconds..."
        sleep 2
    done
}

# Run scripts in parallel
run_script script/production.py &
PID1=$!

run_script script/newsProduction.py &
PID2=$!

# run_script script/flink_handler.py &
# PID3=$!


# Wait both
wait $PID1
wait $PID2
# wait $PID3