import json
import time
import uuid
import logging
import random
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from utils import SYMBOLS
from zoneinfo import ZoneInfo


vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOPIC = 'trading_data'
BOOTSTRAP_SERVERS = ['kafka_broker:19092','kafka_broker_1:19092','kafka_broker_2:19092']
def json_serializer(data):
    if isinstance(data, (np.integer, np.floating)):
        return data.item()
    if isinstance(data, uuid.UUID):
        return str(data)
    raise TypeError(f"Type {type(data)} not serializable")


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



# Giá tham chiếu giữ trạng thái giữa các vòng để bảng giá biến động liền mạch
_REF_PRICE = {}


def extract_trading_data(symbol):
    """Sinh bảng giá (bid/ask, khối ngoại) tổng hợp.

    Các nguồn miễn phí (yfinance...) không cung cấp order-book/khối ngoại của HOSE,
    nên dữ liệu bảng giá được mô phỏng quanh một mức giá tham chiếu có trạng thái.
    """
    handle_time = datetime.now(vietnamese_timezone)
    try:
        ref = _REF_PRICE.get(symbol)
        if ref is None:
            ref = round(random.uniform(15000, 80000), 0)
        # bước giá nhỏ ngẫu nhiên
        price = round(ref * random.uniform(0.99, 1.01), 0)
        _REF_PRICE[symbol] = price

        tick = max(round(price * 0.001), 50)

        def vol():
            return int(random.randint(1, 50) * 10000)

        record = {
            "handle_time": handle_time.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "ceiling": round(ref * 1.07, 0),
            "floor": round(ref * 0.93, 0),
            "reference": ref,
            "Room_foreign": float(random.randint(1, 300) * 1_000_000),
            "foreign_buy_volume": float(vol()),
            "foreign_sell_volume": float(vol()),
            "Buy": {
                "Buy_1": price - tick, "Buy_1_volume": vol(),
                "Buy_2": price - 2 * tick, "Buy_2_volume": vol(),
                "Buy_3": price - 3 * tick, "Buy_3_volume": vol(),
            },
            "Sell": {
                "Sell_1": price + tick, "Sell_1_volume": vol(),
                "Sell_2": price + 2 * tick, "Sell_2_volume": vol(),
                "Sell_3": price + 3 * tick, "Sell_3_volume": vol(),
            },
            "highest": round(price * 1.02, 0),
            "lowest": round(price * 0.98, 0),
            "average": price,
        }

        record = {
            k: (v.item() if isinstance(v, np.generic) else v)
            for k, v in record.items()
        }
        logger.info(f"Record built for {symbol}: ref={ref} price={price}")
        return record

    except Exception as e:
        logger.error(f"Error building trading data for {symbol}: {e}")
        return None


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


def run_producer(symbols, sleep_time=60):
    producer = create_producer()
    while True:
        try:
            for symbol in symbols:
                logger.info(f"Producing trading data for symbols: {symbol}")
                record = extract_trading_data(symbol)
                produce_message(producer, record)
            logger.info(f"Record to be sent: {record}")
            producer.flush()
            logger.info("All messages sent.")
            time.sleep(sleep_time)
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
            break
        except Exception as e:
            logger.error(f"Producer loop error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    logger.info("Starting Kafka Trading producer")
    run_producer(SYMBOLS, sleep_time=60)
