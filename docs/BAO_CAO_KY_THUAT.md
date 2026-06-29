# Báo cáo kỹ thuật — Realtime Pipeline phân tích & dự đoán giá chứng khoán VN30

> Tài liệu mô tả chi tiết kiến trúc, chức năng từng service, cơ chế Kafka/Flink và
> kết quả đánh giá hiệu năng (evaluation) của hệ thống.

## Mục lục
1. [Tổng quan kiến trúc](#1-tổng-quan-kiến-trúc)
2. [Chi tiết chức năng từng service](#2-chi-tiết-chức-năng-từng-service)
3. [Đào sâu Apache Kafka](#3-đào-sâu-apache-kafka)
4. [Đào sâu Apache Flink](#4-đào-sâu-apache-flink)
5. [Đánh giá hiệu năng (Evaluation)](#5-đánh-giá-hiệu-năng-evaluation)

---

## 1. Tổng quan kiến trúc

Hệ thống là một **pipeline dữ liệu thời gian thực** theo kiến trúc streaming, gồm 6 tầng:

```
 Nguồn dữ liệu        Ingestion        Stream Processing      Lưu trữ        Phục vụ        Hiển thị
 (yfinance/mock)  →   Kafka (3 broker) →  Flink (PyFlink)  →  Cassandra  →  FastAPI   →  Streamlit
                      [decouple/buffer]   [window/CEP/join]   [time-series]  [REST API]   [dashboard]
```

**Nguyên tắc thiết kế:**
- **Decoupling qua Kafka**: producer và consumer độc lập, có thể fail/restart mà không mất dữ liệu (log-based + offset).
- **Stream processing với Flink**: mọi tính toán real-time (gộp nến đa khung, chỉ báo kỹ thuật, phát hiện mẫu hình CEP, join đa luồng) tập trung ở Flink.
- **Cassandra cho time-series**: ghi throughput cao, phân vùng theo `(symbol)` hoặc `(screener, symbol)`, clustering theo `time DESC`.
- **Containerized**: toàn bộ chạy qua Docker Compose, ~15 container.

**Luồng dữ liệu tổng quát:**
```
yfinance/mock ──> ohvcl_data ──> Flink ──> flink_computed_ohlc ──> ohlvc (Cassandra)
                                       └──> flink_indicators    ──> indicators
                                       └──> flink_signals       ──> signals
ticks_symbol ─────────────────> Flink ──> flink_news_impact    ──> news_impact
                          └────────────────────────────────────> news (consumer riêng)
trading_data ──────────────────────────────────────────────────> trading_data
ohlvc (đọc lại) ──> model TFT ──> kafka_prediction ────────────> prediction
```

---

## 2. Chi tiết chức năng từng service

Hệ thống gồm ~15 container trong `docker-compose.yml`. Phân nhóm theo vai trò:

### 2.1. Cụm Kafka (6 container)

| Service | Node ID | Vai trò | Cổng |
|---|---|---|---|
| `kafka-controller`, `-1`, `-2` | 1, 2, 3 | **Controller** (KRaft quorum) — quản lý metadata, bầu leader, không chứa dữ liệu | 9093 (nội bộ) |
| `kafka_broker`, `_1`, `_2` | 4, 5, 6 | **Broker** — lưu log message, phục vụ produce/consume | 29092, 39092, 49092 → 9092 |

- Chạy **KRaft mode** (không cần ZooKeeper). 3 controller tạo quorum (chịu lỗi 1 node).
- `KAFKA_DEFAULT_REPLICATION_FACTOR=3`, `KAFKA_NUM_PARTITIONS=3` → mỗi topic mặc định 3 partition, 3 bản sao.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` → topic tự tạo khi có message đầu tiên.
- Healthcheck `nc -z localhost 9092`, các app `depends_on: condition: service_healthy`.

### 2.2. Cụm Flink (2 container)

| Service | Vai trò |
|---|---|
| `jobmanager` | Điều phối job, REST UI **:8081**. `entrypoint.sh` đợi JM + Kafka sẵn sàng rồi submit `job.py` bằng `flink run -py` |
| `taskmanager` | Worker thực thi. `numberOfTaskSlots=8`, `memory.process.size=2048m` |

- Image build từ `flink:2.0.0`, cài thêm **PyFlink** (`apache-flink==2.0.0`) + Python để chạy Table API job và Python UDF.
- Tải sẵn 2 jar: `flink-sql-connector-kafka` và `flink-json`.
- Checkpoint EXACTLY_ONCE mỗi ~30s, lưu vào `/opt/flink/checkpoints`.

### 2.3. `producer_app` — thu thập & sinh dữ liệu

Chạy `run_producer.sh` khởi động **4 producer song song** (mỗi cái có vòng restart tự động):

| Script | Topic | Chức năng |
|---|---|---|
| `producer/OHVLC.py` | `ohvcl_data` | Nến OHLC 1 phút. **Trong giờ giao dịch**: lấy thật từ **yfinance** (ticker `.VN`); **ngoài giờ/cuối tuần**: sinh mock (random-walk quanh seed) mỗi 2s |
| `producer/trading.py` | `trading_data` | Bảng giá (bid/ask 3 mức, khối ngoại, trần/sàn/tham chiếu) — sinh tổng hợp (nguồn free không có order-book VN) |
| `producer/ticks_news.py` | `ticks_symbol` | Tin tức cổ phiếu — sinh tổng hợp với `public_date` chuẩn để Flink join |
| `producer/model_producer.py` | `kafka_prediction` | Đọc `ohlvc` từ Cassandra → chạy inference **TFT** → đẩy dự đoán nhiều bước |

> **Lưu ý nguồn dữ liệu**: đã thay `vnstock` (hỏng `symbols_by_group`, rate-limit) bằng **yfinance**. Nhưng yfinance cũng bị Yahoo trả **HTTP 429** khi fetch 30 mã liên tục, nên ngoài giờ giao dịch hệ thống dùng **mock data tổng hợp** (đúng thiết kế gốc). Danh sách VN30 được **hardcode**.

### 2.4. `processing_app` — consumer ghi vào Cassandra

Chạy `run_processing.sh` khởi động **7 consumer song song**:

| Consumer | Topic nguồn | Bảng Cassandra | Ghi chú |
|---|---|---|---|
| `OHVLC_handler.py` | `flink_computed_ohlc` | `ohlvc` | Nến đa khung do Flink tính |
| `trading_handler.py` | `trading_data` | `trading_data` | Bảng giá |
| `news_handler.py` | `ticks_symbol` | `news` | Tin tức |
| `model_handler.py` | `kafka_prediction` | `prediction` | Mở rộng mỗi dự đoán thành N bước thời gian |
| `indicator_handler.py` | `flink_indicators` | `indicators` | Nhận SMA/BB/VWAP từ Flink, **tự tính EMA/RSI/MACD** (đệ quy, Flink SQL không làm được) |
| `signal_handler.py` | `flink_signals` | `signals` | Tín hiệu CEP |
| `news_impact_handler.py` | `flink_news_impact` | `news_impact` | Tác động giá sau tin (xử lý tombstone của upsert) |

Mỗi consumer dùng `enable_auto_commit=False` + `commit()` thủ công sau khi xử lý batch → đảm bảo at-least-once.

### 2.5. `batch_app` — backfill một lần

`run_batch.sh` → `static_insert/OHVLC_screener.py`: lấy lịch sử nến 1m (yfinance, 5 ngày), roll-up thành 1m/5m/15m/30m/1H bằng `pandas.resample`, ghi vào `ohlvc` để dashboard có dữ liệu ngay từ đầu. Chạy xong **exit 0** (không phải lỗi).

### 2.6. Cassandra (2 container)

| Service | Vai trò |
|---|---|
| `cassandra` | DB chính, cổng **9042**, keyspace `market`, healthcheck `cqlsh describe keyspaces` |
| `cassandra-load-keyspace` | Chạy `jobs.cql` tạo keyspace + 7 bảng rồi exit |

**7 bảng** (keyspace `market`):

| Bảng | Khóa chính | Clustering |
|---|---|---|
| `ohlvc` | `((screener, symbol), time)` | time DESC |
| `trading_data` | `(symbol, handle_time)` | handle_time DESC |
| `news` | `(symbol, id)` | — |
| `prediction` | `((symbol), time, time_step)` | time DESC |
| `indicators` | `((screener, symbol), time)` | time DESC |
| `signals` | `((symbol), signal_time, signal_type)` | signal_time DESC |
| `news_impact` | `((symbol), news_time, id)` | news_time DESC |

Partition key gom dữ liệu cùng mã về một node; clustering `time DESC` cho truy vấn "N điểm gần nhất" cực nhanh.

### 2.7. `back_end` — FastAPI (cổng 8000)

REST API đọc Cassandra, phục vụ frontend (CORS mở):

| Endpoint | Trả về |
|---|---|
| `GET /ohlcv/{symbol}?interval=` | Nến OHLC theo khung |
| `GET /ohlcv/{symbol}/predict` | Dự đoán TFT |
| `GET /indicators/{symbol}` | Chỉ báo kỹ thuật |
| `GET /signals/{symbol}` | Tín hiệu CEP |
| `GET /news_impact/{symbol}` | Tác động tin tức |
| `GET /trading` | Bảng giá toàn VN30 |
| `GET /tick/{symbol}` | Tin tức |
| `GET /health` | Health + kết nối Cassandra |

Dùng `Depends(get_db)` mở/đóng session Cassandra mỗi request, `dict_factory` trả JSON.

### 2.8. `front_end` — Streamlit (cổng 8501)

Dashboard 5 tab (gọi API qua `host.docker.internal:8000`):
1. **Biểu đồ**: nến + overlay **SMA20/EMA12/EMA26/Bollinger** + volume (Plotly).
2. **Dữ liệu giao dịch**: bảng giá mua/bán, khối ngoại.
3. **Dự đoán**: chart dự báo TFT đa khung.
4. **Tin tức** + tác động giá sau tin.
5. **Chỉ báo & Tín hiệu**: chart RSI/MACD/VWAP + bảng tín hiệu CEP (MUA/BÁN/THEO DÕI).

> **Lưu ý kỹ thuật**: pin `pandas==2.2.3` (pandas 3.0 gây segfault Streamlit); `pd.to_datetime(..., format='ISO8601')` vì timestamp từ API lúc có `.%f` lúc không.

---

## 3. Đào sâu Apache Kafka

### 3.1. Kiến trúc KRaft (không ZooKeeper)

Kafka 4.0 chạy **KRaft** — metadata được quản lý bởi chính một quorum controller dùng giao thức Raft, thay cho ZooKeeper:

- **3 controller** (node 1–3): giữ **metadata log** (danh sách topic, partition, leader, ISR). Bầu 1 controller làm *active*; chịu lỗi 1 node (quorum 2/3).
- **3 broker** (node 4–6): chứa **dữ liệu** (partition log), phục vụ produce/consume.
- Tách controller/broker giúp scale metadata độc lập với dữ liệu.

### 3.2. Topic, partition, replication

Mọi topic mặc định **3 partition × 3 replica**:

- **Partition** = đơn vị song song. Message cùng `key` (ở đây thường không set key, phân tròn) vào cùng partition. 3 partition → tối đa 3 consumer/3 task Flink đọc song song.
- **Replication factor 3**: mỗi partition có 1 leader + 2 follower trên 3 broker khác nhau. Mất 1 broker vẫn không mất dữ liệu (ISR — In-Sync Replicas).
- `OFFSETS_TOPIC_REPLICATION_FACTOR=3` → topic nội bộ `__consumer_offsets` cũng nhân 3.

**Danh sách topic trong hệ thống:**

| Topic | Producer | Consumer | Kiểu |
|---|---|---|---|
| `ohvcl_data` | OHVLC.py | Flink `ohlc_source` | append |
| `ticks_symbol` | ticks_news.py | news_handler + Flink `news_source` | append |
| `trading_data` | trading.py | trading_handler | append |
| `flink_computed_ohlc` | Flink | OHVLC_handler | **upsert** (keyed) |
| `flink_indicators` | Flink | indicator_handler | append |
| `flink_signals` | Flink | signal_handler | append |
| `flink_news_impact` | Flink | news_impact_handler | **upsert** (keyed) |
| `kafka_prediction` | model_producer.py | model_handler | append |

### 3.3. Append vs Upsert (compacted semantics)

- **Append topic** (`format=json`): mỗi bản ghi là một sự kiện độc lập (nến mới, tín hiệu mới).
- **Upsert-kafka** (`flink_computed_ohlc`, `flink_news_impact`): dùng **key** (vd `(screener, symbol, time)`). Bản ghi cùng key ghi đè bản cũ; bản ghi `value=null` là **tombstone** (xoá). Consumer phải bỏ qua tombstone — đã xử lý trong `OHVLC_handler`/`news_impact_handler`. Phù hợp khi cần "trạng thái mới nhất" thay vì lịch sử sự kiện.

### 3.4. Consumer group & offset

Mỗi consumer đặt `group_id` riêng (`ohvcl-consumer-group`, `signal-consumer-group`, ...). Trong một group, 3 partition chia cho tối đa 3 instance. Offset commit **thủ công** sau khi ghi Cassandra thành công → nếu consumer crash giữa chừng, message được đọc lại (**at-least-once**), tránh mất dữ liệu.

### 3.5. Vai trò "buffer" chịu tải

Kafka tách tốc độ producer khỏi tốc độ consumer. Khi Flink/Cassandra chậm, dữ liệu **tồn trong log Kafka** (theo retention) chứ không mất — đây là lý do pipeline không backpressure ngược về producer (xem phần Evaluation).

---

## 4. Đào sâu Apache Flink

Toàn bộ logic stream nằm trong **một job PyFlink** (`flink/job/job.py`) — gộp 4 lệnh `INSERT` vào một `STATEMENT SET` để dùng chung phép quét nguồn. Job graph có **16 operator**, mỗi operator **parallelism = 3** (= số partition Kafka) → 48 subtask, **phân vùng theo `symbol`**.

### 4.1. Event-time & Watermark

Flink xử lý theo **event-time** (thời gian sự kiện trong dữ liệu), không phải thời gian xử lý:

- `ohlc_source`: `WATERMARK FOR time AS time - INTERVAL '10' SECONDS` → chấp nhận dữ liệu trễ tối đa 10s.
- Watermark là "đồng hồ" cho biết "đã thấy hết sự kiện đến thời điểm T" → quyết định **khi nào cửa sổ đóng và phát kết quả**, khi nào interval-join hết dữ liệu. Không có watermark thì window/join không bao giờ emit.
- `scan.watermark.idle-timeout=10s`: partition không có dữ liệu sẽ không chặn watermark toàn cục.

### 4.2. Sơ đồ 16 operator

```
[0] Source ohlc_source ──┬─> [2] WindowAggregate 1m (TUMBLE 1') = ohlc_1m (rowtime=rt)
                         │         │ (tính 1 lần, fan-out 4 hướng)
                         │         ├─> [3..6] TUMBLE 5m/15m/30m/1H ─┐
                         │         ├─> [8]  OverAggregate (SMA/BB/VWAP, RANGE 20') ─> indicators_sink
                         │         ├─> [10] Match 3 nến tăng (BUY) ─┐
                         │         ├─> [11] Match 3 nến giảm (SELL) ─┤
                         │         └─> [9]  OverAggregate volume-spike ┤
                         │                                            ├─> [12] signals_sink
                         │   [1m + 5m..1H UNION ALL] ─> [7] ohlc_kafka_sink
                         │
                         └─> [13] IntervalJoin (news ⋈ ohlc, 15') ─> [14] GroupAggregate ─> [15] news_impact_sink
[1] Source news_source ──┘
```

### 4.3. Bốn nhánh xử lý

**A. Cascading windows** — `flink_computed_ohlc`
`ohlc_source` → TUMBLE 1' tạo nến 1m sạch (`ohlc_1m`) → 4 TUMBLE **lồng** (gộp tiếp từ 1m thành 5m/15m/30m/1H, *cascading*) → `UNION ALL` đổ vào **một** sink. Cascading hiệu quả hơn gộp lại 5 lần từ raw.

**B. Technical indicators** — `flink_indicators`
`ohlc_1m` → một **OVER window** (`RANGE 20 MINUTE`) tính SMA20, Bollinger (SMA±2σ), VWAP. EMA/RSI/MACD (đệ quy) được tính ở consumer Python.

**C. CEP / tín hiệu** — `flink_signals`
3 mẫu trên `ohlc_1m`:
- `MATCH_RECOGNIZE PATTERN (A B C) DEFINE B AS B.close>A.close, C AS C.close>B.close` → 3 nến tăng liên tiếp (BUY).
- Tương tự cho 3 nến giảm (SELL).
- OVER window phát hiện volume > 3× trung bình 20' (WATCH).
- `UNION ALL` vào một sink.

**D. Multi-stream interval join** — `flink_news_impact`
`news_source ⋈ ohlc_source ON symbol AND ohlc.time BETWEEN news.time AND news.time + 15'` → `GroupAggregate` gom theo `(symbol, id)` tính giá trước/sau, biến động % → upsert sink. *Đây là nhánh sinh nhiều record nhất* (xem Evaluation).

### 4.4. Ba giới hạn của Flink streaming SQL (đã xử lý)

1. **Không hỗ trợ bounded ROWS** (`ROWS BETWEEN n PRECEDING`) → dùng **time-RANGE** (`RANGE BETWEEN INTERVAL '20' MINUTE`).
2. **Mỗi SELECT chỉ một loại OVER window**; **chain OVER qua nhiều view làm mất time-attribute** → gộp tất cả aggregate vào MỘT cửa sổ duy nhất.
3. **`PREV()` trong MATCH_RECOGNIZE** bị coi là "physical offset" → dùng biến mẫu tường minh `A B C`.
4. **Nhiều INSERT vào cùng một sink** trùng `transactional-id-prefix` → gộp `UNION ALL`; mỗi sink đặt prefix riêng + `sink.delivery-guarantee='at-least-once'`.

### 4.5. State, checkpoint & exactly-once

- **State**: mỗi window/over/match/join giữ state (theo key `symbol`) trên TaskManager — vd buffer nến trong cửa sổ, accumulator SMA, trạng thái pattern CEP.
- **Checkpoint** mỗi ~30s: chụp nhất quán toàn bộ state + offset Kafka. Đo được **<1s, state ~1.7MB** ngay cả dưới tải nặng.
- **EXACTLY_ONCE** ở mức checkpoint; sink Kafka đặt `at-least-once` cho đơn giản (đủ cho demo).
- Khi operator crash, Flink khôi phục từ checkpoint gần nhất → không mất/không nhân đôi state.

### 4.6. Song song & phân vùng

- Parallelism = 3 (khớp 3 partition Kafka). Mỗi `PARTITION BY symbol` đảm bảo cùng một mã luôn về cùng subtask → state nhất quán.
- **Scale ngang**: tăng partition Kafka → tăng `parallelism.default` → thêm TaskManager/slot. Hiện **bị chặn ở 3 vì topic chỉ 3 partition**.

---

## 5. Đánh giá hiệu năng (Evaluation)

### 5.1. Phương pháp

Vì tải thực tế quá nhỏ (mock ~17 msg/s) không thể đo công suất, ta **load-test**: bơm dữ liệu tổng hợp tốc độ cao thẳng vào `ohvcl_data` (bỏ qua yfinance) và quan sát Flink đến điểm bão hoà.

**Công cụ** (`load_test/`):
- `load_producer.py` — producer KafkaProducer (acks=1, linger 20ms, batch 256KB, gzip) sinh nến random-walk N mã, tốc độ điều chỉnh được.
- `monitor.py` — poll Flink REST mỗi 5s: throughput in/out, busy%, backpressure%, watermark-lag, checkpoint.
- `run_load.sh [rate] [symbols] [duration]`.

**3 tín hiệu bão hoà cần theo dõi:**
| Metric | Ý nghĩa khi tăng |
|---|---|
| `busyTimeMsPerSecond` → ~1000 | operator đó là bottleneck |
| `backPressuredTimeMsPerSecond` > 0 | tầng sau không theo kịp → **đây là throughput trần** |
| `currentEmitEventTimeLag` tăng dần | pipeline tụt lại sau real-time → latency phình |

Môi trường: **host 12 core**, ~33GB RAM, dùng chung cho 3 Kafka broker + Flink + producer.

### 5.2. Kết quả đo

| Đợt | Tải bơm vào | Flink consume | Backpressure | Watermark lag | Checkpoint |
|---|---|---|---|---|---|
| Baseline (mock thật) | 17 msg/s | theo kịp | 0% | 0 s | 285 ms |
| **1 producer** | **~21.000 msg/s** | theo kịp | **0%** | **0 s** | ~280 ms |
| **4 producer song song** | **~69.000 msg/s** (4,1 triệu/phút) | theo kịp (~147k record/s out) | **0%** | **0 s** | ~700 ms |

- Đợt 1: 1 producer gửi **1,67 triệu message / 80s**, đạt trung bình 21k msg/s (đỉnh 24k), chậm dần do tự gzip/json.
- Đợt 2: 4 producer × ~17.250 = **~69k msg/s**, **4,1 triệu message / 60s**.
- Flink đọc tổng cộng **~5,9 triệu record** qua 2 đợt, tiêu thụ hết.

> *Ghi chú: `busyTimeMsPerSecond` báo 0 (build Flink này không bật metric), nên kết luận dựa vào backpressure + watermark-lag + throughput in/out — các tín hiệu chuẩn xác.*

### 5.3. Phân tích "1 phút xử lý được gì"

Ở 69k msg/s đầu vào, Flink phát ra **~147k record/s** (khuếch đại ~2,1×) do mỗi nến đi qua nhiều nhánh (window + over + match + passthrough). Nhánh **news_impact khuếch đại mạnh nhất**: 1 tin × mọi nến trong cửa sổ 15' → interval-join tạo hàng trăm dòng, `GroupAggregate` tạo changelog (retract+insert) → **×2**. Trong vận hành thật, đây là operator tốn tài nguyên nhất và sẽ nghẽn đầu tiên khi scale.

### 5.4. Kết luận

1. **Công suất**: cấu hình hiện tại (parallelism 3, 1 TaskManager, 8 slot) **xử lý ≥ 4,1 triệu nến/phút mà không backpressure, không trễ** (watermark lag = 0 → bám sát real-time). Đây **chưa phải trần** — Flink chưa bị stress.
2. **Nút thắt là bộ sinh tải** (producer Python CPU-bound), không phải Flink. Trần thật của Flink cao hơn 69k/s, muốn chạm phải dùng nhiều máy / producer JVM.
3. **Dư địa khổng lồ**: tải thực tế (17 msg/s) chỉ dùng **~0,025%** công suất đã test → thừa sức cho hàng nghìn mã hoặc tick dưới giây.
4. **Độ trễ**: latency end-to-end ≈ *watermark delay (10s, chỉnh được) + vài ms tính toán*, không bị giới hạn bởi throughput.
5. **Chi phí fault-tolerance thấp**: checkpoint exactly-once vẫn <1s ngay ở 69k/s.

### 5.5. Đòn bẩy & khuyến nghị scale

- **Giới hạn cứng: parallelism = 3 vì `ohvcl_data` chỉ 3 partition.** Tăng partition Kafka (vd 12 = số core) → tăng `parallelism.default` → thêm TaskManager/slot để scale ngang.
- **Nút thắt kế tiếp** khi đẩy mạnh: Kafka broker disk I/O → Cassandra write throughput → cuối cùng mới đến tính toán Flink.
- **Data-skew theo `symbol`**: nếu vài mã quá "nóng", một subtask nghẽn dù tổng tải thấp → cân nhắc rebalance key.
- **news_impact**: cân nhắc giới hạn cửa sổ join hoặc giảm tần suất emit nếu trở thành điểm nóng.

### 5.6. Cách tái lập

```bash
# Bơm tối đa + đo
./load_test/run_load.sh 0 300 120

# Hoặc giữ tốc độ cố định (vd 30k msg/s)
./load_test/run_load.sh 30000 300 120
```

---

*Báo cáo cho seminar — ngành Toán–Tin, Trường ĐH Khoa học Tự nhiên, ĐHQG-HCM.*
