import json
from math import log
import time
import uuid
import logging
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from vnstock import Quote,Trading
import time
from utils import symbol_list
from zoneinfo import ZoneInfo

vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class KafkaOHVLCProducer:
    def __init__(self, topic='stock_data', bootstrap_servers='localhost:9092'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8')
        )
        try : 
            Trading(symbol='VN30F1M',source='vci')
            self.source = 'vci'
        except Exception:
            self.source = 'tcbs'

    def json_serializer(self, data):
        if isinstance(data, (np.integer, np.floating)):
            return data.item()
        if isinstance(data, uuid.UUID):
            return str(data)
        raise TypeError(f"Type {type(data)} not serializable")

    def delivery_report(self, record_metadata):
        logger.info(f"Message delivered to {record_metadata.topic} [{record_metadata.partition}] at offset {record_metadata.offset}")

    def extract_stock_data(self, symbol, start_date=None, end_date=None):
        today = datetime.now(vietnamese_timezone).strftime("%Y-%m-%d")
        start_date = start_date or today
        end_date = end_date or today
        try:
            quote = Quote(symbol=symbol, source='vci')
            OHVCL = quote.history(start=start_date, end=end_date, interval='1m')
            record = {
                "symbol": symbol,
                "OHVCL": {
                    "time": OHVCL["time"].iloc[-1].strftime("%Y-%m-%d %H:%M:%S"),
                    "open": OHVCL["open"].iloc[-1],
                    "high": OHVCL["high"].iloc[-1],
                    "low": OHVCL["low"].iloc[-1],
                    "close": OHVCL["close"].iloc[-1],
                    "volume": OHVCL["volume"].iloc[-1],
                }
            }
            
            record = {k: (v.item() if isinstance(v, np.generic) else v) for k, v in record.items()}
            return record
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
    def produce_messages(self, record):
        try:

            future = self.producer.send(self.topic, value=record)
            future.add_callback(self.delivery_report)
            future.add_errback(lambda e: logger.error(f"Message delivery failed: {e}"))
            
        except Exception as e:
            logger.error(f"Error producing message: {e}")

    def run(self, symbols, sleep_time=10):
        logger.info(f"Producing messages for symbols: {symbols}")
        try:
            for symbol in symbols:
                try:
                    record = self.extract_stock_data(symbol)
                    self.produce_messages(record)
                    logger.info(f"Record to be sent: {record}")
                except Exception as e:
                    logger.error(f"Error in producing loop for {symbol}: {e}")           
            self.producer.flush()
            logger.info("All messages sent.")
            time.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"Error at procedure: {e}")

if __name__ == "__main__":
    producer = KafkaOHVLCProducer(
        topic='ohvcl_data',
        bootstrap_servers='kafka_broker:19092'
    )
    
    symbols = symbol_list
    logger.info("Starting Kafka producer for symbols: %s", symbols)
    while True:
        try:
            producer.run(symbols, sleep_time=30) 
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
