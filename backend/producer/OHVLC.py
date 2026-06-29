import json
import time as t
import uuid
import schedule
import logging
import numpy as np
from datetime import datetime, time, timedelta
from kafka import KafkaProducer
import yfinance as yf
import pandas as pd
from utils import SYMBOLS
from zoneinfo import ZoneInfo
from faker import Faker
import random
import numpy as np
vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")
today = datetime.now(vietnamese_timezone)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
LAST_SESSION_DATA = {}

TOPIC = 'ohvcl_data'
BOOTSTRAP_SERVERS = ['kafka_broker:19092','kafka_broker_1:19092','kafka_broker_2:19092']
fake = Faker()


def json_serializer(data):
    if isinstance(data, (np.integer, np.floating)):
        return data.item()
    if isinstance(data, uuid.UUID):
        return str(data)
    raise TypeError(f"Type {type(data)} not serializable")

def check_trading_hour(data_time):
    if data_time.time()<datetime.time(9,30):
        last_day=data_time-datetime.timedelta(days=1)
        data_time=datetime.datetime(last_day.year,last_day.month,last_day.day,16,0,0)
        
    elif data_time.time()>datetime.time(16,0):
        data_time=datetime.datetime(data_time.year,data_time.month,data_time.day,16,0,0)
    return data_time

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(
            v, default=json_serializer
        ).encode('utf-8')
    )

def delivery_report(record_metadata):
    logger.info(
        f"Message delivered to {record_metadata.topic} "
        f"[{record_metadata.partition}] at offset {record_metadata.offset}"
    )
#----------------------Extract_data----------------------------#
def extract_stock_data(symbol):
    """Lấy nến 1m gần nhất từ yfinance (ticker <symbol>.VN)."""
    try:
        df = yf.download(
            f"{symbol}.VN",
            period="1d",
            interval="1m",
            progress=False,
            auto_adjust=False,
            timeout=10,
        )
        if df is None or df.empty:
            return None
        # yfinance trả MultiIndex (Price, Ticker) -> làm phẳng
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        last = df.iloc[-1]
        ts = df.index[-1]
        return {
            "screener": "1m",
            "symbol": symbol,
            "time": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "open": round(float(last["Open"]), 2),
            "high": round(float(last["High"]), 2),
            "low": round(float(last["Low"]), 2),
            "close": round(float(last["Close"]), 2),
            "volume": int(last["Volume"]),
        }
    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
        return None

def randomize_from_last_session(symbol, price_pct=0.005, vol_pct=0.2):
    global LAST_SESSION_DATA
    base = LAST_SESSION_DATA.get(symbol)
    if not base:
        return None
    close_price = base["close"]
    open_price = close_price
    close_price_new = close_price * random.uniform(1 - price_pct, 1 + price_pct) 
    high_price = max(open_price, close_price_new) * random.uniform(1.0, 1 + price_pct)
    low_price = min(open_price, close_price_new) * random.uniform(1 - price_pct, 1.0)
    volume = int(base["volume"] * random.uniform(1 - vol_pct, 1 + vol_pct))
    LAST_SESSION_DATA[symbol]['close'] = close_price_new

    return {
        "screener": "1m",
        "symbol": symbol,
        "time": datetime.now(vietnamese_timezone).strftime("%Y-%m-%d %H:%M:%S"),
        "open": round(open_price, 2),
        "high": round(high_price, 2),
        "low": round(low_price, 2),
        "close": round(close_price_new, 2),
        "volume": volume
    }


def synthetic_seed(symbol):
    """Seed giá tổng hợp khi yfinance không khả dụng, để mock data chạy offline."""
    base = round(random.uniform(15000, 80000), 2)
    return {
        "screener": "1m",
        "symbol": symbol,
        "time": datetime.now(vietnamese_timezone).strftime("%Y-%m-%d %H:%M:%S"),
        "open": base, "high": base, "low": base, "close": base,
        "volume": random.randint(50000, 500000),
    }


def batch_fetch_seeds(symbols, timeout=25):
    """Lấy seed cho CẢ rổ trong MỘT call yfinance (ít bị 429 hơn gọi từng mã).

    Bọc trong thread + timeout để không bao giờ treo khi Yahoo rate-limit.
    Trả dict symbol->record; mã nào thiếu sẽ được seed tổng hợp ở hàm gọi.
    """
    import concurrent.futures

    tickers = [f"{s}.VN" for s in symbols]

    def _do():
        return yf.download(
            tickers, period="1d", interval="1m",
            progress=False, auto_adjust=False,
            group_by="ticker", threads=True, timeout=10,
        )

    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        df = ex.submit(_do).result(timeout=timeout)
    except Exception as e:
        logger.warning(f"Batch yfinance seed failed/timeout: {e}")
        ex.shutdown(wait=False)   # KHÔNG chờ thread yfinance đang treo (429)
        return {}
    ex.shutdown(wait=False)

    out = {}
    if df is None or df.empty:
        return out
    for s in symbols:
        try:
            sub = df[f"{s}.VN"].dropna()
            if sub.empty:
                continue
            last = sub.iloc[-1]
            out[s] = {
                "screener": "1m", "symbol": s,
                "time": sub.index[-1].strftime("%Y-%m-%d %H:%M:%S"),
                "open": round(float(last["Open"]), 2),
                "high": round(float(last["High"]), 2),
                "low": round(float(last["Low"]), 2),
                "close": round(float(last["Close"]), 2),
                "volume": int(last["Volume"]),
            }
        except Exception:
            continue
    return out


def load_last_session_data(symbols):
    """Seed cho mock mode (ngoài giờ giao dịch) = synthetic, không gọi mạng để
    không bao giờ treo. yfinance được dùng cho phiên giao dịch thật (extract_stock_data).
    Có thể bật lại seed thật bằng batch_fetch_seeds() khi Yahoo không rate-limit."""
    global LAST_SESSION_DATA
    logger.info("Seeding session data (synthetic) ...")
    for symbol in symbols:
        LAST_SESSION_DATA[symbol] = synthetic_seed(symbol)
    logger.info(f"Seeded {len(symbols)} symbols (synthetic)")
#----------------------Producer----------------------------#
def produce_message(producer, record):
    if not record:
        return
    try:
        future = producer.send(TOPIC, value=record)
        future.add_callback(delivery_report)
        future.add_errback(
            lambda e: logger.error(f"Message delivery failed: {e}")
        )
    except Exception as e:
        logger.error(f"Error producing message: {e}")


def kafka_producer_ohvlc(symbols,producer, realtime=True):
    try:  
        for symbol in symbols:
            logger.info(f"Producing messages for symbols: {symbol}")
            record = extract_stock_data(symbol) if realtime else randomize_from_last_session(symbol)
            produce_message(producer, record)
            logger.info(f"Record to be sent: {record}")
        producer.flush()
        logger.info("All messages sent.")
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Error in producer loop: {e}")
        t.sleep(5)


if __name__ == "__main__":
    logger.info("Starting Kafka OHVLC producer")
    producer = create_producer()
    #schedule
    if today.weekday()<5:
        if today.time() < time(9,0,0) or (today.time() > time(11,30,0) and today.time() < time(13,0,0)) or today.time() > time(14,30,0):
            load_last_session_data(SYMBOLS)
            kafka_producer_ohvlc(SYMBOLS,producer,False)
            schedule.every(2).seconds.do(kafka_producer_ohvlc,SYMBOLS,producer,False)
        else:
            kafka_producer_ohvlc(SYMBOLS,producer)
            schedule.every(60).seconds.do(kafka_producer_ohvlc,SYMBOLS,producer)
    else : 
        load_last_session_data(SYMBOLS)
        kafka_producer_ohvlc(SYMBOLS,producer,False)
        schedule.every(2).seconds.do(kafka_producer_ohvlc,SYMBOLS,producer,False)
    while True:
        schedule.run_pending()