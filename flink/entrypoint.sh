#!/bin/bash
set -e
echo "Starting Flink JobManager..."
/docker-entrypoint.sh jobmanager &

echo "Waiting for JobManager REST API..."
until curl -s http://jobmanager:8081/overview > /dev/null; do
  sleep 2
done
echo "JobManager is ready. Submitting SQL job..."

MAX_RETRIES=5
SLEEP_TIME=5

KAFKA_HOST=kafka_broker
KAFKA_PORT=19092

echo "Waiting for Kafka at ${KAFKA_HOST}:${KAFKA_PORT}..."
until nc -z ${KAFKA_HOST} ${KAFKA_PORT}; do
  echo "Kafka not ready yet..."
  sleep 3
done
echo "Kafka is ready."


flink_job() {
  local JOB="$1"
  local SCRIPT="$2"
  RETRY_COUNT=0
  echo "Submitting ${JOB} from ${SCRIPT}"
  until [ $RETRY_COUNT -ge $MAX_RETRIES ]; do
    echo "Attempting to submit SQL job (attempt $((RETRY_COUNT + 1))/$MAX_RETRIES) for ${JOB}..."
    
    if $FLINK_HOME/bin/sql-client.sh -f "$SCRIPT"; then
      echo "${JOB} submitted successfully."
      return 0
    else
      RETRY_COUNT=$((RETRY_COUNT + 1))
      if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
        echo "Failed to submit ${JOB}. Retrying in ${SLEEP_TIME} seconds..."
        sleep 5
      else
        echo "Failed to submit ${JOB} after $MAX_RETRIES attempts."
        exit 1
      fi
    fi
  done
}
flink_job "PROCESSING" /opt/flink/job/processing.sql
wait