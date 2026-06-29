#!/bin/bash
set -e
echo "Starting Flink JobManager..."
/docker-entrypoint.sh jobmanager &

echo "Waiting for JobManager REST API..."
until curl -s http://jobmanager:8081/overview > /dev/null; do
  sleep 2
done
echo "JobManager is ready."

KAFKA_HOST=kafka_broker
KAFKA_PORT=19092
echo "Waiting for Kafka at ${KAFKA_HOST}:${KAFKA_PORT}..."
until nc -z ${KAFKA_HOST} ${KAFKA_PORT}; do
  echo "Kafka not ready yet..."
  sleep 3
done
echo "Kafka is ready."

MAX_RETRIES=5
RETRY_COUNT=0

# Submit PyFlink job (Table API + Python UDF) tới JobManager đang chạy.
submit_job() {
  echo "Submitting PyFlink job (attempt $((RETRY_COUNT + 1))/${MAX_RETRIES})..."
  $FLINK_HOME/bin/flink run -d \
    -py /opt/flink/job/job.py \
    -pyfs /opt/flink/job/udfs.py \
    -pyexec python3 \
    -pyclientexec python3
}

until [ $RETRY_COUNT -ge $MAX_RETRIES ]; do
  if submit_job; then
    echo "PyFlink job submitted successfully."
    break
  fi
  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
    echo "Failed to submit job after ${MAX_RETRIES} attempts."
  else
    echo "Submit failed. Retrying in 5s..."
    sleep 5
  fi
done

wait
