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


class Trading:
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
        self.insert_trading = self.session.prepare("""
            INSERT INTO market.trading_data (
                handle_time,
                symbol,
                ceiling, 
                floor, 
                reference,
                room_foreign,
                foreign_buy_volume, 
                foreign_sell_volume,
                buy_1, 
                buy_1_volume,
                buy_2, 
                buy_2_volume,
                buy_3, 
                buy_3_volume,
                sell_1, 
                sell_1_volume,
                sell_2, 
                sell_2_volume,
                sell_3, 
                sell_3_volume,
                highest, 
                lowest, 
                average
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """)
    def transform_time(self, value):
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(value)

    def kafka_consumer(self):
        logger.info("Setting up Kafka consumer...")
        self.consumer = KafkaConsumer(
            'trading_data',
            bootstrap_servers=self.bootstrap_servers,
            group_id='trading-consumer-group',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            max_poll_records=100
        )

    def insert_data(self, record: dict):
        self.prepare_statements()
        if not record:
            logger.info(f"Empty record")
            return
        time_transform = self.transform_time(record['handle_time'])
        try:
            self.session.execute(self.insert_trading, (
                time_transform,
                record['symbol'],
                record['ceiling'],
                record['floor'],
                record['reference'],
                record['Room_foreign'],
                record['foreign_buy_volume'],
                record['foreign_sell_volume'],
                record['Buy']['Buy_1'],
                record['Buy']['Buy_1_volume'],
                record['Buy']['Buy_2'],
                record['Buy']['Buy_2_volume'],
                record['Buy']['Buy_3'],
                record['Buy']['Buy_3_volume'],
                record['Sell']['Sell_1'],
                record['Sell']['Sell_1_volume'],
                record['Sell']['Sell_2'],
                record['Sell']['Sell_2_volume'],
                record['Sell']['Sell_3'],
                record['Sell']['Sell_3_volume'],
                record['highest'],
                record['lowest'],
                record['average'],
            ))
            logger.info(
                f"Inserted trading data {record['symbol']} @ {record['handle_time']}"
            )
        except KeyError as e:
            logger.error(f"Missing field {e} in record: {record}")
        except Exception as e:
            logger.error(f"Cassandra insert error: {e}")


    def consume_data(self, wait=5):
        self.kafka_consumer()
        logger.info("Start consuming data...")
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
    app = Trading()
    app.consume_data()
