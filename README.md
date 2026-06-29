
# Realtime Pipeline for Stock Price Analytics and Prediction

## Overview
This project implements an **end-to-end real-time data pipeline** for stock market analytics and price prediction.  
The system is designed to **collect, process, predict, store, and visualize stock data** in a near real-time manner, focusing on scalability, fault tolerance, and modular architecture.

The project was developed as a **seminar report** for the Mathematics–Computer Science program at **University of Science – VNUHCM**.

---

## Project Objectives
- Build a **real-time stock data pipeline**
- Support:
  - Streaming data ingestion
  - Real-time processing
  - Multi-step time-series forecasting
  - Time-series storage
  - Near real-time visualization
- Ensure the system is:
  - Automatic
  - Parallelizable
  - Scalable
  - Fault-tolerant

---

## Data Source
- **vnstock API**
  - Open-source API for Vietnamese stock market data
  - Provides near real-time stock prices
  - Used interval: **1-minute OHLC data**
  - Limited to **~30 stock symbols** due to API request constraints

---

## Tech Stack

### Streaming & Processing
- **Apache Kafka**
  - Acts as a message broker
  - Decouples data ingestion and processing
  - Ensures fault tolerance via log-based storage and offsets

- **Apache Flink (Flink SQL)**
  - Real-time stream processing
  - Window-based aggregation (5m, 10m, 30m)
  - Data validation and normalization
  - Chosen for simplicity compared to JVM-based APIs

### Storage
- **Apache Cassandra**
  - NoSQL database optimized for time-series data
  - High write throughput
  - Horizontal scalability and fault tolerance

### Machine Learning
- **Temporal Fusion Transformer (TFT)**
  - Deep learning model for time-series forecasting
  - Supports:
    - Multi-step forecasting
    - Historical, future-known, and static features
    - Interpretability via attention mechanisms
  - Trained offline and loaded for real-time inference

### Backend & Frontend
- **FastAPI**
  - Backend API layer
  - Asynchronous, high-performance
  - Serves data from Cassandra to frontend

- **Streamlit**
  - Web-based dashboard
  - Visualizes real-time and predicted stock prices

### Deployment
- **Docker & Docker Compose**
  - Fully containerized system
  - Tested on:
    - 8 CPU cores
    - 8GB RAM

---

## System Architecture
**Pipeline Flow:**

```

vnstock API
↓
Kafka (Producer)
↓
Flink SQL (Stream Processing)
↓
Cassandra (Time-series Storage)
↓
FastAPI (Backend API)
↓
Streamlit (Dashboard)

```

Kafka acts as a buffer layer, ensuring reliability and enabling multiple consumers without data loss.

---

## Project Structure (Simplified)
```

root/
├── backend/
│   ├── model/
│   │   ├── best_model.ckpt
│   │   └── train.ipynb
│   ├── processing/
│   ├── producer/
│   ├── static_insert/
│   ├── app.py
│   └── run_*.sh
├── flink/
│   ├── job/
│   ├── sql_jobs/
│   ├── Dockerfile
│   └── entrypoint.sh
├── frontend/
│   └── main.py
├── docker-compose.yml
└── README.md

````

---

## How to Run

### 1. Prerequisites
- Install **Docker** and **Docker Compose**
- Ensure all `.sh` files use **LF** (not CRLF)

### 2. Build Containers
```bash
docker-compose build
````

> This step may take **10–15 minutes** due to large images (Kafka, Flink, Cassandra).

### 3. Run System

```bash
docker-compose up -d
```

Wait around **25–30 seconds** for services to warm up.

---

### 4. Access Services

* **Backend API**: [http://localhost:8000](http://localhost:8000)
* **Dashboard**: [http://localhost:8501](http://localhost:8501)

---

## Runtime Logic

* Kafka, Flink, Cassandra warm up first
* Producers fetch:

  * **Real data** during market hours (9:00–14:30)
  * **Mock data** outside trading hours
* Consumers process and store data
* Prediction job runs every **1–2 minutes**
* Streamlit fetches data via FastAPI

---

## Flink Analytics (Extended)

Flink không chỉ làm resampling nến mà còn đảm nhận toàn bộ tính toán real-time
trong một job PyFlink duy nhất (`flink/job/job.py`, UDF ở `flink/job/udfs.py`):

| Tính năng | Kỹ thuật Flink | Output topic → Cassandra |
|-----------|----------------|--------------------------|
| Nến đa khung 1m→5m/15m/30m/1H | **Cascading window TVF** (tính dồn từ 1m) | `flink_computed_ohlc` → `ohlvc` |
| SMA, Bollinger, VWAP, return | **OVER window** | `flink_indicators` → `indicators` |
| EMA12/26, MACD, RSI14 | **Python UDF** (`ema`, `rsi`, `macd`) trên mảng `ARRAY_AGG` | `flink_indicators` → `indicators` |
| Tín hiệu 3 nến tăng/giảm, volume spike | **CEP `MATCH_RECOGNIZE`** | `flink_signals` → `signals` |
| Tác động giá 15' sau tin | **Interval join** news ⋈ giá | `flink_news_impact` → `news_impact` |

Các INSERT được gộp bằng `EXECUTE STATEMENT SET` để dùng chung một source scan.
Dashboard có thêm overlay chỉ báo trên biểu đồ nến và tab "Chỉ báo & Tín hiệu".

> Lưu ý: image Flink nay cài thêm PyFlink (`apache-flink==2.0.0`) để chạy Python UDF;
> `taskmanager.numberOfTaskSlots` được nâng lên để chứa nhiều operator hơn.

## Current Limitations

* Limited system-wide testing
* Model shows signs of **overfitting**
* Feature set is still basic

---

## Proposed Enhancements

* Deeper exploitation of Flink capabilities
* Add technical indicators:

  * SMA, EMA, volatility, etc.
* Explore **Multi-RAG** approaches for real-time prediction
* Consider replacing Cassandra with a lighter time-series database

---

## References

* GitHub Repository: [https://github.com/Amature123/seminar](https://github.com/Amature123/seminar)
* Apache Flink SQL Documentation
* Apache Kafka Documentation
* YouTube: CodeWithYu

