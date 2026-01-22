import json
import logging
import time
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
from datetime import datetime
from zoneinfo import ZoneInfo
from utils import safe_json_deserializer

vietnamese_timezone = ZoneInfo("Asia/Ho_Chi_Minh")

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("news_consumer")

BOOTSTRAP_SERVERS = [
    'kafka_broker:19092',
    'kafka_broker_1:19092',
    'kafka_broker_2:19092'
]

CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'market'

TOPIC = 'ticks_symbol'
GROUP_ID = 'news-consumer-group'

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
        INSERT INTO market.news (
            symbol,
            id,
            title,
            link,
            public_date,
            s_content,
            close,
            price_change_pct
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)


def transform_time(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        if value > 1e12:   
            return datetime.fromtimestamp(value / 1000, tz=vietnamese_timezone)
        else:             
            return datetime.fromtimestamp(value, tz=vietnamese_timezone)
    return datetime.fromisoformat(value)


def insert_data(session, insert_stmt, record: dict):
    try:
        session.execute(
            insert_stmt,
            (
                record["symbol"],
                record["id"],
                record["news_title"],
                record["news_source_link"],
                transform_time(record.get("public_date")),
                record["news_short_content"],
                record["close_price"],
                record["price_change_pct"]
            )
        )

        logger.info(
            f"Inserted NEWS {record['symbol']} | "
            f"id={record['id']} | "
            f"date={record.get('public_date')}"
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
        value_deserializer=safe_json_deserializer,
        max_poll_records=100
    )

def consume_data(session, insert_stmt, wait=5):
    consumer = create_consumer()
    logger.info("Start consuming news data...")

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
                    if message.value is None:
                        logger.info(
                            f"Skip tombstone message "
                            f"key={message.key}"
                        )
                        continue
                    insert_data(session, insert_stmt, message.value)
            consumer.commit()
        except Exception as e:
            logger.error(f"Consume error: {e}")
            time.sleep(3)

if __name__ == "__main__":
    session = connect_cassandra()
    insert_stmt = prepare_statements(session)
    consume_data(session, insert_stmt)
