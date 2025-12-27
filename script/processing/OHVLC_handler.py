import json
import logging
import time
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
from datetime import datetime

from vnstock import Screener
from zoneinfo import ZoneInfo
vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("stock_consumer")

BOOTSTRAP_SERVERS = [
    'kafka_broker:19092',
    'kafka_broker_1:19092',
    'kafka_broker_2:19092'
]
CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'market'
TOPIC = 'flink_computed_ohlc'
GROUP_ID = 'ohvcl-consumer-group'
SCREENER = ['5m', '15m', '30m', '1h']
def connect_cassandra():
    while True:
        try:
            cluster = Cluster(CASSANDRA_HOSTS)
            session = cluster.connect(KEYSPACE)
            logger.info("Connected to Cassandra")
            return session
        except NoHostAvailable:
            logger.warning("Cassandra not ready, retrying in 5s...")
            time.sleep(5)

def prepare_statements(session):
    return session.prepare("""
        INSERT INTO market.ohlvc (
            screener, symbol, time, open, high, low, close, volume
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

def screener_check(screener: str, message: dict):
    return screener in SCREENER


def transform_time(value):
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)

def insert_data(session, insert_stmt, record: dict):
    try:
        session.execute(insert_stmt, (
            record['screener'],
            record['symbol'],
            transform_time(record['time']),
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

def create_consumer():
    logger.info("Setting up Kafka consumer...")
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        max_poll_records=100
    )

def consume_data(session, insert_stmt, wait=5):
    consumer = create_consumer()
    logger.info("Start consuming data...")

    while True:
        try:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                time.sleep(wait)
                continue
            for tp, messages in records.items():
                logger.info(
                    f"Processing {len(messages)} messages "
                    f"from {tp.topic}-{tp.partition}"
                )
                for message in messages:

                    insert_data(session, insert_stmt, message.value)
            consumer.commit()
        except Exception as e:
            logger.error(f"Consume error: {e}")
            time.sleep(3)

if __name__ == "__main__":
    session = connect_cassandra()
    insert_stmt = prepare_statements(session)
    consume_data(session, insert_stmt)
