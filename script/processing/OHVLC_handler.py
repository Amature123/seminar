import json
import logging
import time
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
from datetime import datetime

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("stock_consumer")


class OHVLC:
    def __init__(self):
        self.bootstrap_servers = [
            'kafka_broker:19092',
            'kafka_broker_1:19092',
            'kafka_broker_2:19092'
        ]
        self.cassandra_hosts = ['cassandra']
        self.keyspace = 'market'

        self.connect_cassandra()

    def connect_cassandra(self):
        while True:
            try:
                self.cluster = Cluster(self.cassandra_hosts)
                self.session = self.cluster.connect(self.keyspace)
                logger.info("Connected to Cassandra")
                break
            except NoHostAvailable:
                logger.warning("Cassandra not ready, retrying in 5s...")
                time.sleep(5)

    def prepare_statements(self):
        self.insert_ohlvc = self.session.prepare("""
            INSERT INTO market.OHVLC (
                symbol, time, open, high, low, close, volume
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """)

    def transform_time(self, value):
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(value)

    def kafka_consumer(self):
        logger.info("Setting up Kafka consumer...")
        self.consumer = KafkaConsumer(
            'ohvcl_data',
            bootstrap_servers=self.bootstrap_servers,
            group_id='ohvcl-consumer-group',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            max_poll_records=100
        )

    def insert_data(self, record: dict):
        self.prepare_statements()
        try:
            self.session.execute(self.insert_ohlvc, (
                record['symbol'],
                self.transform_time(record['time']),
                record['open'],
                record['high'],
                record['low'],
                record['close'],
                record['volume']
            ))
            logger.info(
                f"Inserted OHVLC {record['symbol']} @ {record['time']}"
            )
        except Exception as e:
            logger.error(f"Cassandra insert error: {e}")

    def consume_data(self, wait=5):
        logger.info("Start consuming data...")
        self.kafka_consumer()
        while True:
            try:
                records = self.consumer.poll(timeout_ms=3000)

                if not records:
                    time.sleep(wait)
                    continue

                for tp, messages in records.items():
                    logger.info(
                        f"Processing {len(messages)} messages "
                        f"from {tp.topic}-{tp.partition}"
                    )

                    for message in messages:
                        self.insert_data(message.value)
                self.consumer.commit()
            except Exception as e:
                logger.error(f"Consume error: {e}")
                time.sleep(3)

if __name__ == "__main__":
    app = OHVLC()
    app.consume_data()
