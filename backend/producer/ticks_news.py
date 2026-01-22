import json
import time as t
import uuid
import logging
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from vnstock import Company
from utils import SYMBOLS
from zoneinfo import ZoneInfo
import schedule


vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOPIC = 'ticks_symbol'
BOOTSTRAP_SERVERS = ['kafka_broker:19092','kafka_broker_1:19092','kafka_broker_2:19092']
######-------handle--------#########
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
###-------data----------###########
def extract_news_data(symbol):
    try:
        company = Company(symbol=symbol, source='VCI')
        news_df = company.news()
        results = []
        for _, row in news_df.tail(15).iterrows():
            news_key_extract = {
                "symbol": symbol,
                "id": row.get("id", ""),
                "news_title": row.get("news_title", ""),
                "news_source_link": row.get("news_source_link", ""),
                "public_date": row.get("public_date", ""),
                "news_short_content": row.get("news_short_content", ""),
                "close_price": row.get("close_price", 0),
                "price_change_pct": row.get("price_change_pct", 0)
            }
            results.append(news_key_extract)
        return results
    except Exception as e:
        logger.error(f"No news for now: {e}")
        return None
###------kafka-----####
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


def kafka_producer_news(symbols,producer):
    try:  
        for symbol in symbols:
            logger.info(f"Producing messages for symbols: {symbol}")
            records = extract_news_data(symbol)
            if not records:
                logger.warning(f"No news records for symbol {symbol}")
                continue
            for record in records:
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
    kafka_producer_news(SYMBOLS, producer)
    #schedule
    schedule.every(120).seconds.do(kafka_producer_news,SYMBOLS,producer)
    while True:
        schedule.run_pending()