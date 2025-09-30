from confluent_kafka import Consumer, KafkaError
import logging
import json
from datetime import datetime, timedelta
from vnstock import Vnstock, Quote

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)
class rawDataExtraction:
    def __init__(self, bootstrap_servers='localhost:9092', group_id='raw_data', topic='stock_data'):
        self.topic = topic
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.config)


    def _json_deserializer(self, data):
        return json.loads(data)

    def _process_message(self, message):
        try:
            data = self._json_deserializer(message.value().decode('utf-8'))
            self.logger.info(f"Received message: {data}")
            return data
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")

    def consume_messages(self):
        self.consumer.subscribe([self.topic])
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.info(f"End of partition reached {msg.topic()} [{msg.partition()}]")
                    else:
                        self.logger.error(f"Error occurred: {msg.error().str()}")
                else:
                    self._process_message(msg)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

    def insert_to_cass(self, record):
        