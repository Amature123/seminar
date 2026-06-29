"""
Monitor hiệu năng Flink trong lúc load-test. Poll REST API mỗi `--interval` giây
và in: tốc độ nạp (source), tốc độ ghi tổng (mọi sink), busy% & backpressure% lớn
nhất (operator nghẽn nhất), watermark lag, thời gian checkpoint.

Chạy từ host:  python monitor.py --duration 120
(FLINK_URL mặc định http://localhost:8081)
"""
import argparse
import json
import os
import time
import urllib.request

FLINK = os.environ.get("FLINK_URL", "http://localhost:8081")


def get(path):
    with urllib.request.urlopen(FLINK + path, timeout=10) as r:
        return json.load(r)


def vmetric(jid, vid, keys):
    q = ",".join(keys)
    m = get(f"/jobs/{jid}/vertices/{vid}/subtasks/metrics?get={q}")
    return {x["id"]: x for x in m}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=120)
    ap.add_argument("--interval", type=int, default=5)
    args = ap.parse_args()

    jid = get("/jobs/overview")["jobs"][0]["jid"]
    job = get(f"/jobs/{jid}")
    verts = [(v["id"], v["name"]) for v in job["vertices"]]
    src_id = verts[0][0]
    print(f"job={job['name']} jid={jid} vertices={len(verts)}\n")

    hdr = (f"{'t(s)':>5} {'in/s':>9} {'out/s':>10} {'busy%':>6} {'bp%':>6} "
           f"{'wmLag(s)':>9} {'ckpt(ms)':>8}  bottleneck")
    print(hdr)
    print("-" * len(hdr))

    prev = None
    t0 = time.time()
    while time.time() - t0 < args.duration:
        job = get(f"/jobs/{jid}")
        total_out = sum(v["metrics"].get("write-records", 0) for v in job["vertices"])
        src_in = next(v["metrics"].get("read-records", 0)
                      for v in job["vertices"] if v["id"] == src_id)
        now = time.time()

        busy_max, bp_max, lag_max, bottleneck = 0.0, 0.0, 0.0, ""
        for vid, name in verts:
            try:
                md = vmetric(jid, vid, [
                    "busyTimeMsPerSecond", "backPressuredTimeMsPerSecond",
                    "currentEmitEventTimeLag"])
            except Exception:
                continue
            busy = float(md.get("busyTimeMsPerSecond", {}).get("max", 0) or 0)
            bp = float(md.get("backPressuredTimeMsPerSecond", {}).get("max", 0) or 0)
            lag = float(md.get("currentEmitEventTimeLag", {}).get("max", 0) or 0)
            if busy > busy_max:
                busy_max, bottleneck = busy, name.split(" ->")[0][:28]
            bp_max = max(bp_max, bp)
            lag_max = max(lag_max, lag)

        try:
            ck = (get(f"/jobs/{jid}/checkpoints").get("latest") or {}).get("completed") or {}
            ckdur = ck.get("end_to_end_duration", "-")
        except Exception:
            ckdur = "-"

        if prev:
            dt = now - prev[2]
            in_r = (src_in - prev[0]) / dt
            out_r = (total_out - prev[1]) / dt
            print(f"{int(now - t0):>5} {in_r:>9.0f} {out_r:>10.0f} "
                  f"{busy_max/10:>6.1f} {bp_max/10:>6.1f} {lag_max/1000:>9.1f} "
                  f"{str(ckdur):>8}  {bottleneck}")
        prev = (src_in, total_out, now)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
