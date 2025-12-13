from email import message
import json
import logging
from math import log
import re
from kafka import KafkaConsumer
from cassandra.cluster import Cluster,NoHostAvailable
from cassandra.query import SimpleStatement
from datetime import datetime
import time

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("stock_consumer")

class CassandraImplement:
    def __init__(self):
        self.bootstrap_servers = ['kafka_broker:19092','kafka_broker_1:19092','kafka_broker_2:19092']
        self.cassandra_hosts = ['cassandra']
        self.keyspace = 'market'
        self.connect_cassandra()
        self.prepare_statements()

    def transform_record(self, record):
        if isinstance(record,datetime):
            return record
        try:
            return datetime.fromisoformat(record)
        except Exception as e:
            logger.error(f"Error transforming record {record}: {e}")
            return None
            
    def connect_cassandra(self):
        try:
            self.cluster = Cluster(self.cassandra_hosts)
            self.session = self.cluster.connect(self.keyspace)
            time.sleep(10) 
            logger.info("Connected to Cassandra")
        except NoHostAvailable as e:
            logger.error(f"Could not connect to Cassandra: {e}")
            raise

def prepare_statements(self):
    self.insert_trading = self.session.prepare("""
        INSERT INTO market.trading_data (
            symbol,
            handle_time,
            ceiling, floor, reference,
            room_foreign,
            foreign_buy_volume, foreign_sell_volume,
            buy_1, buy_1_volume,
            buy_2, buy_2_volume,
            buy_3, buy_3_volume,
            sell_1, sell_1_volume,
            sell_2, sell_2_volume,
            sell_3, sell_3_volume,
            highest, lowest, average
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """)
    self.insert_ohlvc = self.session.prepare("""
        INSERT INTO market.OHVLC (
            symbol, time, open, high, low, close, volume
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    self.insert_news = self.session.prepare("""
        INSERT INTO market.news (
            symbol, id, title, publish_date
        ) VALUES (?, ?, ?, ?)
    """)

    def kafka_consumer(self):
        logger.info("Setting up Kafka consumers...")
        try:
            self.consumer1 = KafkaConsumer(
                'ohvcl_data',
                bootstrap_servers=['kafka_broker:19092','kafka_broker_1:19092','kafka_broker_2:19092'],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='stock-data-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            self.consumer2= KafkaConsumer(
                'trading_data',
                bootstrap_servers=['kafka_broker:19092','kafka_broker_1:19092','kafka_broker_2:19092'],
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id='trading-data-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
        except Exception as e:
            logger.error(f"Error setting up Kafka consumer: {e}")
            raise
    def insert_data(self,record = None):
        main_session = self.session
        if record is None:
            return
        time_val = self.transform_record(record['OHVCL']['time'])
        try:
            main_session.execute(self.insert_stock, (
                record['symbol'],
                time_val,
                record['OHVCL']['open'],
                record['OHVCL']['high'],
                record['OHVCL']['low'],
                record['OHVCL']['close'],
                record['OHVCL']['volume'],
                record['trading']['ceiling'],
                record['trading']['floor'],
                record['trading']['reference'],
                record['trading']['Room_foreign'],
                record['trading']['foreign_buy_volume'],
                record['trading']['foreign_sell_volume'],
                record['trading']['Buy']['Buy_1'],
                record['trading']['Buy']['Buy_1_volume'],
                record['trading']['Buy']['Buy_2'],
                record['trading']['Buy']['Buy_2_volume'],
                record['trading']['Buy']['Buy_3'],
                record['trading']['Buy']['Buy_3_volume'],
                record['trading']['Sell']['Sell_1'],
                record['trading']['Sell']['Sell_1_volume'],
                record['trading']['Sell']['Sell_2'],
                record['trading']['Sell']['Sell_2_volume'],
                record['trading']['Sell']['Sell_3'],
                record['trading']['Sell']['Sell_3_volume'],
                record['trading']['Average'],
            ))
            logger.info(f"Inserted stock data for {record['symbol']} at {record['OHVCL']['time']}")
        except Exception as e:
            logger.error(f"Error inserting stock data: {e}")

    def _process_message(self, message):
        data = message.value
        if not data:
            return
        logger.info(f"Received message: {data}")
        try:
            logger.info("Inserting data into Cassandra...")
            self.insert_data(data)
        except Exception as e:
            logger.error(f"Error inserting to Cassandra: {e}")

    def cosume_data(self,consumer,wait = 30):
        while True:
            try:
                messages = consumer.poll(timeout_ms=3000)
                if messages is None:
                    logger.info("No messages received in this poll., waiting for next poll...")
                    time.sleep(wait)
                    continue
                for tp, msgs in messages.items():
                    logger.info(f"Processing {len(msgs)} messages from topic-partition {tp}")
                    for message in msgs:
                        self._process_message(message)
                        logger.info(f"Processed message offset {message} from partition {tp.partition}")
                
            except Exception as e:
                logger.error(f"Error consuming messages: {e}")
