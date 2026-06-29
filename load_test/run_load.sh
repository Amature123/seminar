#!/bin/bash
# Chạy load-test: bơm ohvcl_data tốc độ cao + monitor Flink.
# Cách dùng:  ./run_load.sh [rate] [symbols] [duration]
#   rate=0 => bơm tối đa (đo throughput trần). Mặc định: 0 300 120
set -e
RATE="${1:-0}"
SYMBOLS="${2:-300}"
DURATION="${3:-120}"

# Tự dò network + image của compose
NET=$(docker network ls --format '{{.Name}}' | grep -E 'seminar.*project|project.*seminar' | head -1)
[ -z "$NET" ] && NET=$(docker inspect producer_app --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' 2>/dev/null)
IMG=$(docker inspect producer_app --format '{{.Config.Image}}' 2>/dev/null)
[ -z "$IMG" ] && IMG="seminar-producer_app"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "network=$NET image=$IMG rate=$RATE symbols=$SYMBOLS duration=${DURATION}s"
echo "Bắt đầu monitor (host) + load (container)..."

# Monitor chạy nền trên host
python "$DIR/monitor.py" --duration $((DURATION + 15)) &
MON=$!

# Load generator chạy trong network
docker run --rm --network "$NET" -v "$DIR:/lt" "$IMG" \
  python /lt/load_producer.py --rate "$RATE" --symbols "$SYMBOLS" --duration "$DURATION"

wait $MON
echo "Xong load-test."
