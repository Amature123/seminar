import json
import time
import uuid
import logging
import numpy as np
from datetime import datetime
from confluent_kafka import Producer
from vnstock import Quote

# Logging Configuration
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class KafkaUserDataProducer:
    def __init__(self, topic='stock_data', bootstrap_servers='kafka:9092'):
        self.topic = topic
        self.producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.source = 'tcbs'
        self.checkpoint = {}

    def json_serializer(self, data):
        if isinstance(data, (np.integer, np.floating)):
            return data.item()
        if isinstance(data, uuid.UUID):
            return str(data)
        raise TypeError(f"Type {type(data)} not serializable")

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def extract_stock_data(self, symbol, start_date=None, end_date=None):
        today = datetime.now().strftime("%Y-%m-%d")
        start_date = start_date or today
        end_date = end_date or today

        try:
            quote = Quote(symbol=symbol, source=self.source)
            data = quote.history(start=start_date, end=end_date, interval='1m')
            if data.empty:
                logger.warning(f"No data returned for {symbol}")
                return None

            record = {
                "symbol": symbol,
                "time": data["time"].iloc[-1].strftime("%Y-%m-%d %H:%M:%S"),
                "open": data["open"].iloc[-1],
                "high": data["high"].iloc[-1],
                "low": data["low"].iloc[-1],
                "close": data["close"].iloc[-1],
                "volume": data["volume"].iloc[-1],
            }

            record = {k: (v.item() if isinstance(v, np.generic) else v) for k, v in record.items()}
            return record

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None

    def produce_messages(self, record):
        try:
            message = json.dumps(record, default=self.json_serializer)
            self.producer.produce(self.topic, value=message, callback=self.delivery_report)
            self.producer.poll(0.5)
        except Exception as e:
            logger.error(f"Error producing message: {e}")

    def producer_loop(self, symbols, sleep_time=10):
        logger.info(f"Producing messages for symbols: {symbols}")
        try:
            for symbol in symbols:
                try:
                    record = self.extract_stock_data(symbol)
                    last_seen = self.checkpoint.get(symbol)

                    if record and last_seen != record["time"]:
                        self.produce_messages(record)
                        logger.info(f"Record to be sent: {record}")
                        self.checkpoint[symbol] = record["time"]
                        
                        logger.info(f"Produced message for {symbol} at {record['time']}")
                    else:
                        logger.info(f"No new data for {symbol}. Last seen: {last_seen}")
                except Exception as e:
                    logger.error(f"Error in producing loop for {symbol}: {e}")
            self.producer.flush(timeout=10)
            logger.info("All messages sent.")
            time.sleep(sleep_time)
        except Exception as e:
            logger.error(f"Error at procedure :{e}")
