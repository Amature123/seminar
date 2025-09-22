import random
import json
from datetime import datetime

from confluent_kafka import Producer
import logging
import uuid

from vnstock import Vnstock, Quote

# Logging Configuration
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class KafkaUserDataProducer():
    
    def __init__(self, topic, bootstrap_servers='localhost:9092'):
        self.topic = topic
        self.config = {'bootstrap.servers': bootstrap_servers}
        self.producer = Producer(self.config)

    def _json_serializer(self, data):
        if isinstance(data, uuid.UUID):
            return str(data)
        raise TypeError(f"Type {type(data)} not serializable")

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    # def _extract_stock_data(self, symbol, source='vci'):
    #     try:
    #         stock = Vnstock().stock(symbol=symbol, source=source)
    #         in_day = stock.quote.intraday(page_size=1, show_log=False)
    #         record = {
    #         "id": in_day["id"],
    #         "symbol": symbol,
    #         "price": in_day["price"],    
    #         "volume": in_day["volume"],    
    #         "type": in_day["match_type"],            
    #         "time": in_day["time"].strftime("%Y-%m-%d %H:%M:%S"),    
    #         }
    #         return record

    #     except Exception as e:
    #         logger.error(f"Error fetching data for {symbol}: Data unvailable for now.")
    #         return None
    def _extract_stock_data(self, symbol, source='tcbs', start_date=datetime.now().date(), end_date=datetime.now().date()):
        try:
            quote = Quote(symbol=symbol, source=source)
            data = quote.history(start=start_date, end=end_date,interval = '1m')
            record = {
                "symbol": symbol,
                "time": data["time"].iloc[-1].strftime("%Y-%m-%d %H:%M:%S"),
                "open": data["open"].iloc[-1],
                "high": data["high"].iloc[-1],
                "low": data["low"].iloc[-1],
                "close": data["close"].iloc[-1],
                "volume": data["volume"].iloc[-1],
            }
            return record

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return None
        
    def produce_messages(self, symbol, source):
        record = self._extract_stock_data(symbol, source, start_date='2025-09-17', end_date='2025-09-17')
        if record is None:
            return
        try:
            message = json.dumps(record, default=self._json_serializer)
            self.producer.produce(
                topic=self.topic,
                value=message,
                callback=self._delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error producing message: {e}")
        
        self.producer.flush()
        logger.info(f"Produced 1 message to topic {self.topic}")
        return record
    def process(self,symbol, source):
        company,
        record = self.produce_messages(symbol, source)
        return record

