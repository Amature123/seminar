import logging
import time
from kafka import KafkaConsumer
from cassandra.cluster import Cluster, NoHostAvailable
from utils import safe_json_deserializer, transform_time

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("signal_consumer")

BOOTSTRAP_SERVERS = [
    'kafka_broker:19092',
    'kafka_broker_1:19092',
    'kafka_broker_2:19092'
]
CASSANDRA_HOSTS = ['cassandra']
KEYSPACE = 'market'
TOPIC = 'flink_signals'
GROUP_ID = 'signal-consumer-group'


def connect_cassandra(max_retries=10, delay=5):
    retries = 0
    while retries < max_retries:
        cluster = None
        try:
            cluster = Cluster(CASSANDRA_HOSTS)
            session = cluster.connect(KEYSPACE)
            logger.info("Connected to Cassandra")
            return session
        except NoHostAvailable:
            retries += 1
            logger.warning(
                f"Cassandra not ready (attempt {retries}/{max_retries}), retrying in {delay}s..."
            )
            time.sleep(delay)
        finally:
            if cluster and retries >= max_retries:
                cluster.shutdown()
    raise RuntimeError("Failed to connect to Cassandra after retries")


def prepare_statements(session):
    return session.prepare("""
        INSERT INTO market.signals (
            symbol, signal_time, signal_type, side, price, detail
        ) VALUES (?, ?, ?, ?, ?, ?)
    """)


def insert_data(session, insert_stmt, record: dict):
    try:
        session.execute(insert_stmt, (
            record['symbol'],
            transform_time(record['signal_time']),
            record['signal_type'],
            record.get('side'),
            float(record['price']) if record.get('price') is not None else None,
            record.get('detail')
        ))
        logger.info(
            f"Inserted SIGNAL {record['symbol']} {record['signal_type']} "
            f"({record.get('side')}) @ {record['signal_time']}"
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
    logger.info("Start consuming signals...")
    while True:
        try:
            records = consumer.poll(timeout_ms=3000)
            if not records:
                time.sleep(wait)
                continue
            for tp, messages in records.items():
                for message in messages:
                    if message.value is None:
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
