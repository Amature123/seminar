"""
Load generator: bơm nến OHLC tổng hợp tốc độ cao vào topic `ohvcl_data`
(bỏ qua yfinance/rate-limit) để đo throughput & điểm bão hoà của Flink.

Chạy TRONG docker network (để thấy kafka_broker:19092). Xem run_load.sh.

Ví dụ:
    python load_producer.py --rate 0     --symbols 300 --duration 120   # max throughput
    python load_producer.py --rate 20000 --symbols 300 --duration 120   # giữ 20k msg/s
"""
import argparse
import json
import random
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers",
                    default="kafka_broker:19092,kafka_broker_1:19092,kafka_broker_2:19092")
    ap.add_argument("--topic", default="ohvcl_data")
    ap.add_argument("--rate", type=float, default=0,
                    help="msg/s mục tiêu (0 = bơm tối đa)")
    ap.add_argument("--symbols", type=int, default=300,
                    help="số mã (key) — tăng để tăng song song/đa dạng key")
    ap.add_argument("--duration", type=int, default=120, help="giây")
    ap.add_argument("--minute-step", type=float, default=6.0,
                    help="số giây (sim) tiến lên mỗi vòng -> điều khiển nhịp đóng cửa sổ")
    args = ap.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.brokers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        linger_ms=20,
        batch_size=262144,
        compression_type="gzip",
        max_in_flight_requests_per_connection=5,
    )

    symbols = [f"SYM{i:04d}" for i in range(args.symbols)]
    price = {s: random.uniform(10000, 80000) for s in symbols}
    sim_t = datetime.now()                       # đồng hồ event-time mô phỏng
    interval = (1.0 / args.rate) if args.rate > 0 else 0.0

    sent = 0
    t0 = time.time()
    last_t, last_sent = t0, 0
    print(f"START rate={'MAX' if args.rate==0 else int(args.rate)} "
          f"symbols={args.symbols} duration={args.duration}s", flush=True)
    try:
        while time.time() - t0 < args.duration:
            ts = sim_t.strftime("%Y-%m-%d %H:%M:%S")
            for s in symbols:
                o = price[s]
                c = o * random.uniform(0.999, 1.001)
                price[s] = c
                producer.send(args.topic, {
                    "screener": "1m", "symbol": s, "time": ts,
                    "open": round(o, 2),
                    "high": round(max(o, c) * 1.001, 2),
                    "low": round(min(o, c) * 0.999, 2),
                    "close": round(c, 2),
                    "volume": random.randint(1000, 100000),
                })
                sent += 1
                if interval:
                    target = t0 + sent * interval
                    dt = target - time.time()
                    if dt > 0:
                        time.sleep(dt)
            sim_t += timedelta(seconds=args.minute_step)

            now = time.time()
            if now - last_t >= 5:
                r = (sent - last_sent) / (now - last_t)
                print(f"[{int(now - t0):3}s] sent={sent:>10}  achieved={r:>10.0f} msg/s",
                      flush=True)
                last_t, last_sent = now, sent
        producer.flush()
    finally:
        producer.flush()
        el = time.time() - t0
        print(f"DONE total={sent} in {el:.1f}s  avg={sent/el:.0f} msg/s", flush=True)


if __name__ == "__main__":
    main()
