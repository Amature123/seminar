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

    def json_serializer(self, data):
        if isinstance(data, uuid.UUID):
            return str(data)
        raise TypeError(f"Type {type(data)} not serializable")

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


    def extract_stock_data(self, symbol, source='tcbs', start_date=datetime.now().date(), end_date=datetime.now().date()):
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
        
    def produce_messages(self, symbol, source='tcbs'):
        record = self.extract_stock_data(symbol, source)
        if record is None:
            return
        try:
            message = json.dumps(record, default=self.json_serializer)
            self.producer.produce(
                topic=self.topic,
                value=message,
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Error producing message: {e}")
        
        self.producer.flush()
        logger.info(f"Produced 1 message to topic {self.topic}")
        return record
    def process(self,symbol, source='tcbs'):
        company = ['tcb','vci','acb','ctg']
        for symbol in company:
            record = self.produce_messages(symbol, source)
            if record is not None:
                logger.info(f"Produced message for {symbol}: {record}")
            else :
                logger.warning(f"Failed to produce message for {symbol}")
        return record

