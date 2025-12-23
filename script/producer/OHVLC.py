import json
import time
import uuid
import logging
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from vnstock import Quote, Screener, Trading
from utils import SYMBOLS
from zoneinfo import ZoneInfo


vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


TOPIC = 'ohvcl_data'
BOOTSTRAP_SERVERS = ['kafka_broker:19092']


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

def extract_stock_data(symbol, start_date=None, end_date=None):
    today = datetime.now(vietnamese_timezone).strftime("%Y-%m-%d")
    start_date = start_date or today
    end_date = end_date or today
    screen = '1m'
    try:
        quote = Quote(symbol=symbol, source='vci')
        ohvlc = quote.history(
            start=start_date,
            end=end_date,
            interval=screen
        )

        record = {
            "screener": screen,
            "symbol": symbol,
            "time": ohvlc["time"].iloc[-1].strftime("%Y-%m-%d %H:%M:%S"),
            "open": ohvlc["open"].iloc[-1],
            "high": ohvlc["high"].iloc[-1],
            "low": ohvlc["low"].iloc[-1],
            "close": ohvlc["close"].iloc[-1],
            "volume": ohvlc["volume"].iloc[-1]
        }

        # convert numpy types
        return {
            k: (v.item() if isinstance(v, np.generic) else v)
            for k, v in record.items()
        }

    except Exception as e:
        logger.error(f"Error fetching data for {symbol}: {e}")
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


def run_producer(symbols, sleep_time=30):
    producer = create_producer()
    logger.info(f"Producing messages for symbols: {symbols}")

    while True:
        try:
            for symbol in symbols:
                record = extract_stock_data(symbol)
                produce_message(producer, record)
                logger.info(f"Record to be sent: {record}")

            producer.flush()
            logger.info("All messages sent.")
            time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
            break
        except Exception as e:
            logger.error(f"Error in producer loop: {e}")
            time.sleep(5)


if __name__ == "__main__":
    logger.info("Starting Kafka OHVLC producer")
    run_producer(SYMBOLS, sleep_time=30)
