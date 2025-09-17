import random
import json
from datetime import datetime

from vnstock import VnStock
from confluent_kafka import Producer
import logging
import uuid

# Logging Configuration
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)


class KafkaUserDataProducer:
    
    def __init__(self, bootstrap_servers='kafka:9092', topic='generated_customers'):

        self.topic = topic
        self.config = {'bootstrap.servers': bootstrap_servers}
        self.producer = Producer(self.config)
        self.vnstock = VnStock()


    def _json_serializer(self, data):
        if isinstance(data, uuid.UUID):
            return str(data)
        raise TypeError(f"Type {type(data)} not serializable")

    def _delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def _extract_stock_data(self):
        try:
            stock_data = self.vnstock.get_stock_list()
            selected_stock = random.choice(stock_data)
            return {
                "stock_code": selected_stock['stock_code'],
                "stock_name": selected_stock['stock_name'],
                "market": selected_stock['market']
            }
        except Exception as e:
            logger.error(f"Error fetching stock data: {e}")
            return {
                "stock_code": "UNKNOWN",
                "stock_name": "UNKNOWN",
                "market": "UNKNOWN"
            }
    
    
    def send_messages(self, n_customers=20):
        try:
            for _ in range(n_customers):
                payload = self._generate_fake_user()
                logger.info(f"Sending customer {payload['customer']['customer_id']} to Kafka...")
                
                self.producer.produce(
                    self.topic,
                    value=json.dumps(payload, default=self._json_serializer).encode('utf-8'),
                    callback=self._delivery_report
                )
                self.producer.poll(0)  
                logger.info(f"Produced message {payload}")

            self.producer.flush()
            logger.info("All messages sent.")
        except Exception as e:
            logger.error(f"Error sending messages to Kafka: {e}")

