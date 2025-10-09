from confluent_kafka import Consumer, KafkaError
import logging
import json
from datetime import datetime
from cassandra.cluster import Cluster
import time
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class RawDataExtraction:
    def __init__(self, bootstrap_servers='kafka:9092', group_id='raw_data', topic='stock_data', cassandra_host='cassandra'):
        self.topic = topic
        self.cassandra_host = cassandra_host
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([self.topic])
        self.cluster = None
        self.session = None

    def _json_deserializer(self, data):
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            logger.error("Invalid JSON message received.")
            return None

    def _process_message(self, message):
        data = self._json_deserializer(message.value().decode('utf-8'))
        if not data:
            return
        logger.info(f"Received message: {data}")
        try:
            logger.info("Inserting data into Cassandra...")
            self.insert_data(self.session, data)
        except Exception as e:
            logger.error(f"Error inserting to Cassandra: {e}")

    def consume_messages(self , duration=30):
        
        start = datetime.now()

        try:
            while (datetime.now() - start).seconds < duration:
                msg = self.consumer.poll(0.5)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"End of partition {msg.topic()} [{msg.partition()}]")
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                else:
                    self._process_message(msg)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted.")
        finally:
            self.consumer.close()

    def connect_cassandra(self):
        try:
            self.cluster = Cluster([self.cassandra_host], port=9042)
            self.session = self.cluster.connect()
            logger.info("Connected to Cassandra successfully.")
            return self.session
        except Exception as e:
            logger.error(f"Error connecting to Cassandra: {e}")
            raise

    def create_keyspace(self):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS stock_data
            WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
        """)
        logger.info("Keyspace ensured.")

    def create_table(self):
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS stock_data.prices (
                symbol TEXT,
                time TIMESTAMP,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                PRIMARY KEY (symbol, time)
            ) WITH CLUSTERING ORDER BY (time DESC);
        """)
        logger.info("Table ensured.")

    def insert_data(self, session, record):
        try:
            ts = record.get('time')
            if isinstance(ts, str):
                try:
                    ts = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    ts = datetime.fromisoformat(ts)
            session.execute("""
                INSERT INTO stock_data.prices (symbol, time, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (record['symbol'], ts, record['open'], record['high'], record['low'], record['close'], record['volume']))
            logger.info(f"Inserted data for {record['symbol']} at {record['time']}")
        except Exception as e:
            logger.error(f"Error inserting data: {e}")

    def consumer_stock_price(self):
        try:
            self.connect_cassandra()
            self.create_keyspace()
            self.session.set_keyspace("stock_data")
            time.sleep(2)  # wait for keyspace to be fully set
            self.create_table()
            self.consume_messages(duration =20)
            logger.info("Consumer finished processing.")
        except Exception as e:
            logger.error(f"Runtime error: {e}")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.session:
                self.session.shutdown()
            if self.cluster:
                self.cluster.shutdown()
            logger.info("Shutdown complete.")
