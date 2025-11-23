import json
import time
import uuid
import logging
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from vnstock import Company



logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class KafkaNewsProduct:
    def __init__(self, topic='news', bootstrap_servers='localhost:9092'):
        self.topic = topic 
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v, default=self.json_serializer).encode('utf-8')
        )
        self.source = 'tcbs'
        self.checkpoint = {}

    def json_serializer(self, data):
        if isinstance(data, (np.integer, np.floating)):
            return data.item()
        if isinstance(data, uuid.UUID):
            return str(data)
        raise TypeError(f"Type {type(data)} not serializable")


    def delivery_report(self, record_metadata):
        logger.info(f"Message delivered to {record_metadata.topic} [{record_metadata.partition}] at offset {record_metadata.offset}")

    def extract_stock_data(self, symbol, start_date=None, end_date=None):
        today = datetime.now().strftime("%Y-%m-%d")
        start_date = start_date or datetime.now().strftime("%Y-%m-%d")
        end_date = end_date or datetime.now().strftime("%Y-%m-%d")
        
        try:
            company = Company(symbol=symbol, source=self.source)
            data = company.news()
            
            if data.empty:
                logger.warning(f"No data returned for {symbol}")
                return None
            record = {
                'symbol': symbol,
                'id': data['id'].iloc[-1],
                'title': data['title'].iloc[-1],
                'publish_date': data['publish_date'].iloc[-1]
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
                    last_seen = self.checkpoint.get(symbol)
                    
                    if record and last_seen != record["id"]:
                        self.produce_messages(record)
                        logger.info(f"Record to be sent: {record}")
                        self.checkpoint[symbol] = record["id"]
                        logger.info(f"Produced message for {symbol} at {record['id']}")
                    else:
                        logger.info(f"No news for new data for {symbol}. Last seen: {last_seen}")
                        
                except Exception as e:
                    logger.error(f"Error in producing loop for {symbol}: {e}")
            
            self.producer.flush()
            logger.info("All messages sent.")
            time.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"Error at procedure: {e}")
if __name__ == "__main__":
    producer = KafkaNewsProduct(
        topic='news_data',
        bootstrap_servers='kafka_broker:19092'
    )
    symbols = ['tcb', 'vci', 'vcb', 'acb', 'tpb']
    logger.info("Starting Kafka producer for symbols: %s", symbols)
    while True:
        try:
            producer.run(symbols, sleep_time=60) 
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        finally:
            producer.producer.close()